/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.ppd;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.exprNodeDesc;
import org.apache.hadoop.hive.ql.plan.exprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.filterDesc;
import org.apache.hadoop.hive.ql.plan.joinCond;
import org.apache.hadoop.hive.ql.plan.joinDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * Operator factory for predicate pushdown processing of operator graph
 * Each operator determines the pushdown predicates by walking the expression tree.
 * Each operator merges its own pushdown predicates with those of its children
 * Finally the TableScan operator gathers all the predicates and inserts a filter operator 
 * after itself.
 * TODO: Further optimizations
 *   1) Multi-insert case
 *   2) Create a filter operator for those predicates that couldn't be pushed to the previous 
 *      operators in the data flow
 *   3) Merge multiple sequential filter predicates into so that plans are more readable
 *   4) Remove predicates from filter operators that have been pushed. Currently these pushed
 *      predicates are evaluated twice.
 */
public class OpProcFactory {

  /**
   * Processor for Script Operator
   * Prevents any predicates being pushed
   */
  public static class ScriptPPD extends DefaultPPD implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.info("Processing for " +  nd.getName() + "(" + ((Operator)nd).getIdentifier() + ")");
      // script operator is a black-box to hive so no optimization here
      // assuming that nothing can be pushed above the script op
      // same with LIMIT op
      return null;
    }

  }

  /**
   * Combines predicates of its child into a single expression and adds a filter op as new child
   */
  public static class TableScanPPD extends DefaultPPD implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.info("Processing for " +  nd.getName() + "(" + ((Operator)nd).getIdentifier() + ")");
      OpWalkerInfo owi = (OpWalkerInfo)procCtx;
      TableScanOperator tsOp = (TableScanOperator)nd;
      mergeWithChildrenPred(tsOp, owi, null, null, false);
      ExprWalkerInfo pushDownPreds = owi.getPrunedPreds(tsOp);
      return createFilter(tsOp, pushDownPreds, owi);
    }

  }

  /**
   * Determines the push down predicates in its where expression and then combines it with
   * the push down predicates that are passed from its children
   */
  public static class FilterPPD extends DefaultPPD implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.info("Processing for " +  nd.getName() + "(" + ((Operator)nd).getIdentifier() + ")");
      OpWalkerInfo owi = (OpWalkerInfo)procCtx;
      Operator<? extends Serializable> op = (Operator<? extends Serializable>) nd;
      exprNodeDesc predicate = (((FilterOperator)nd).getConf()).getPredicate();
      // get pushdown predicates for this operator's predicate
      ExprWalkerInfo ewi = ExprWalkerProcFactory.extractPushdownPreds(owi, op, predicate);
      if (!ewi.isDeterministic()) {
        /* predicate is not deterministic */
        if (op.getChildren() != null && op.getChildren().size() == 1)
          createFilter(op, owi.getPrunedPreds((Operator<? extends Serializable>)(op.getChildren().get(0))), owi);

        return null;
      }

      logExpr(nd, ewi);
      owi.putPrunedPreds(op, ewi);
      // merge it with children predicates
      mergeWithChildrenPred(op, owi, ewi, null, false);
      
      return null;
    }
  }

  /**
   * Determines predicates for which alias can be pushed to it's parents.
   * See the comments for getQualifiedAliases function
   */
  public static class JoinPPD extends DefaultPPD implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.info("Processing for " +  nd.getName() + "(" + ((Operator)nd).getIdentifier() + ")");
      OpWalkerInfo owi = (OpWalkerInfo)procCtx;
      Set<String> aliases = getQualifiedAliases((JoinOperator) nd, owi.getRowResolver(nd));
      mergeWithChildrenPred(nd, owi, null, aliases, false);
      return null;
    }

    /**
     * Figures out the aliases for whom it is safe to push predicates based on ANSI SQL semantics
     * For inner join, all predicates for all aliases can be pushed
     * For full outer join, none of the predicates can be pushed as that would limit the number of 
     *   rows for join
     * For left outer join, all the predicates on the left side aliases can be pushed up
     * For right outer join, all the predicates on the right side aliases can be pushed up
     * Joins chain containing both left and right outer joins are treated as full outer join.
     * TODO: further optimization opportunity for the case a.c1 = b.c1 and b.c2 = c.c2
     *   a and b are first joined and then the result with c. But the second join op currently 
     *   treats a and b as separate aliases and thus disallowing predicate expr containing both 
     *   tables a and b (such as a.c3 +  a.c4 > 20). Such predicates also can be pushed just above
     *   the second join and below the first join
     *  
     * @param op Join Operator
     * @param rr Row resolver
     * @return set of qualified aliases 
     */
    private Set<String> getQualifiedAliases(JoinOperator op, RowResolver rr) {
      Set<String> aliases = new HashSet<String>();
      int loj = Integer.MAX_VALUE;
      int roj = -1;
      boolean oj = false;
      joinCond[] conds = op.getConf().getConds();
      Map<Integer, Set<String>> posToAliasMap = op.getPosToAliasMap();
      for(joinCond jc : conds) {
        if(jc.getType() == joinDesc.FULL_OUTER_JOIN) {
          oj = true;
          break;
        } else if(jc.getType() == joinDesc.LEFT_OUTER_JOIN) {
          if(jc.getLeft() < loj) loj = jc.getLeft();
        } else if(jc.getType() == joinDesc.RIGHT_OUTER_JOIN) {
          if(jc.getRight() > roj) roj = jc.getRight();
        }
      }
      if(oj || (loj != Integer.MAX_VALUE && roj != -1)) return aliases;
      for (Entry<Integer, Set<String>> pa : posToAliasMap.entrySet()) {
        if(loj != Integer.MAX_VALUE) {
          if(pa.getKey() <= loj) 
            aliases.addAll(pa.getValue());
        } else if(roj != -1) {
          if(pa.getKey() >= roj) 
            aliases.addAll(pa.getValue());
        } else {
            aliases.addAll(pa.getValue());
        }
      }
      Set<String> aliases2 = rr.getTableNames();
      aliases.retainAll(aliases2);
      return aliases;
    }
  }

  /**
   * Processor for ReduceSink operator.
   *
   */
  public static class ReduceSinkPPD extends DefaultPPD implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.info("Processing for " +  nd.getName() + "(" + ((Operator)nd).getIdentifier() + ")");
      OpWalkerInfo owi = (OpWalkerInfo)procCtx;
      Set<String> aliases = owi.getRowResolver(nd).getTableNames();
      boolean ignoreAliases = false;
      if(aliases.size() == 1 && aliases.contains("")) {
        // Reduce sink of group by operator
        ignoreAliases = true; 
      }
      mergeWithChildrenPred(nd, owi, null, aliases, ignoreAliases);
      return null;
    }

  }

  /**
   * Default processor which just merges its children
   */
  public static class DefaultPPD implements NodeProcessor {

    protected static final Log LOG = LogFactory.getLog(OpProcFactory.class.getName());
    
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.info("Processing for " +  nd.getName() + "(" + ((Operator)nd).getIdentifier() + ")");
      mergeWithChildrenPred(nd, (OpWalkerInfo)procCtx, null, null, false);
      return null;
    }

    /**
     * @param nd
     * @param ewi
     */
    protected void logExpr(Node nd, ExprWalkerInfo ewi) {
      for (Entry<String, List<exprNodeDesc>> e : ewi.getFinalCandidates().entrySet()) {
        LOG.info("Pushdown Predicates of " + nd.getName() + " For Alias : " + e.getKey() );
        for (exprNodeDesc n : e.getValue()) {
        LOG.info("\t" + n.getExprString());
        }
      }
    }

    /**
     * Take current operators pushdown predicates and merges them with children's pushdown predicates
     * @param nd current operator
     * @param owi operator context during this walk
     * @param ewi pushdown predicates (part of expression walker info)
     * @param aliases aliases that this operator can pushdown. null means that all aliases can be pushed down
     * @param ignoreAliases 
     * @throws SemanticException 
     */
    protected void mergeWithChildrenPred(Node nd, OpWalkerInfo owi, ExprWalkerInfo ewi, Set<String> aliases, boolean ignoreAliases) throws SemanticException {
      if(nd.getChildren() == null || nd.getChildren().size() > 1) {
        // ppd for multi-insert query is not yet implemented
        // no-op for leafs
        return;
      }
      Operator<? extends Serializable> op = (Operator<? extends Serializable>) nd;
      ExprWalkerInfo childPreds = owi.getPrunedPreds((Operator<? extends Serializable>) nd.getChildren().get(0)); 
      if(childPreds == null) {
        return;
      }
      if(ewi == null) {
        ewi = new ExprWalkerInfo();
      }
      for (Entry<String, List<exprNodeDesc>> e : childPreds.getFinalCandidates().entrySet()) {
        if(ignoreAliases || aliases == null || aliases.contains(e.getKey()) || e.getKey() == null) {
          // e.getKey() (alias) can be null in case of constant expressions. see input8.q
          ExprWalkerInfo extractPushdownPreds = ExprWalkerProcFactory.extractPushdownPreds(owi, op, e.getValue());
          ewi.merge(extractPushdownPreds);
          logExpr(nd, extractPushdownPreds);
        }
      }
      owi.putPrunedPreds((Operator<? extends Serializable>) nd, ewi);
    }
  }

  protected static Object createFilter(Operator op, ExprWalkerInfo pushDownPreds, OpWalkerInfo owi) {
    if (pushDownPreds == null 
        || pushDownPreds.getFinalCandidates() == null 
        || pushDownPreds.getFinalCandidates().size() == 0) {
      return null;
    }
      
    RowResolver inputRR = owi.getRowResolver(op);

    // combine all predicates into a single expression
    List<exprNodeDesc> preds = null;
    exprNodeDesc condn = null; 
    Iterator<List<exprNodeDesc>> iterator = pushDownPreds.getFinalCandidates().values().iterator();
    while (iterator.hasNext()) {
      preds = iterator.next();
      int i = 0;
      if (condn == null) {
        condn = preds.get(0);
        i++;
      }

      for(; i < preds.size(); i++) {
        List<exprNodeDesc> children = new ArrayList<exprNodeDesc>(2);
        children.add(condn);
        children.add((exprNodeDesc) preds.get(i));
        condn = new exprNodeGenericFuncDesc(
                                            TypeInfoFactory.booleanTypeInfo,
                                            FunctionRegistry.getGenericUDFForAnd(),
                                            children
                                            );
      }
    }

    if(condn == null)
      return null;

    // add new filter op
    List<Operator<? extends Serializable>> originalChilren = op.getChildOperators();
    op.setChildOperators(null);
    Operator<filterDesc> output = 
      OperatorFactory.getAndMakeChild(new filterDesc(condn, false),
                                      new RowSchema(inputRR.getColumnInfos()), 
                                      op);
    output.setChildOperators(originalChilren);
    for (Operator<? extends Serializable> ch : originalChilren) {
      List<Operator<? extends Serializable>> parentOperators = ch.getParentOperators();
      int pos = parentOperators.indexOf(op);
      assert pos != -1;
      parentOperators.remove(pos);
      parentOperators.add(pos, output); // add the new op as the old
    }
    OpParseContext ctx = new OpParseContext(inputRR);
    owi.put(output, ctx);
    return output;
  }
  
  public static NodeProcessor getFilterProc() {
    return new FilterPPD();
  }

  public static NodeProcessor getJoinProc() {
    return new JoinPPD();
  }

  public static NodeProcessor getRSProc() {
    return new ReduceSinkPPD();
  }

  public static NodeProcessor getTSProc() {
    return new TableScanPPD();
  }

  public static NodeProcessor getDefaultProc() {
    return new DefaultPPD();
  }

  public static NodeProcessor getSCRProc() {
    return new ScriptPPD();
  }

  public static NodeProcessor getLIMProc() {
    return new ScriptPPD();
  }

}
