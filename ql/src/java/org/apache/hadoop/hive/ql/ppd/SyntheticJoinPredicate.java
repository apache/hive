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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.PreOrderOnceWalker;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicListDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * creates synthetic predicates that represent "IN (keylist other table)"
 */
public class SyntheticJoinPredicate extends Transform {

  private static transient Logger LOG = LoggerFactory.getLogger(SyntheticJoinPredicate.class.getName());

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    boolean enabled = false;
    String queryEngine = pctx.getConf().getVar(ConfVars.HIVE_EXECUTION_ENGINE);

    if (queryEngine.equals("tez")
        && pctx.getConf().getBoolVar(ConfVars.TEZ_DYNAMIC_PARTITION_PRUNING)) {
      enabled = true;
    } else if ((queryEngine.equals("spark")
        && pctx.getConf().getBoolVar(ConfVars.SPARK_DYNAMIC_PARTITION_PRUNING))) {
      enabled = true;
    }

    if (!enabled) {
      return pctx;
    }

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", "(" +
        TableScanOperator.getOperatorName() + "%" + ".*" +
        ReduceSinkOperator.getOperatorName() + "%" +
        JoinOperator.getOperatorName() + "%)"), new JoinSynthetic());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    SyntheticContext context = new SyntheticContext(pctx);
    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, context);
    GraphWalker ogw = new PreOrderOnceWalker(disp);

    // Create a list of top op nodes
    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    return pctx;
  }

  // insert filter operator between target(child) and input(parent)
  private static Operator<FilterDesc> createFilter(Operator<?> target, Operator<?> parent,
      RowSchema parentRS, ExprNodeDesc filterExpr) {
    FilterDesc filterDesc = new FilterDesc(filterExpr, false);
    filterDesc.setSyntheticJoinPredicate(true);
    Operator<FilterDesc> filter = OperatorFactory.get(parent.getCompilationOpContext(),
        filterDesc, new RowSchema(parentRS.getSignature()));
    filter.getParentOperators().add(parent);
    filter.getChildOperators().add(target);
    parent.replaceChild(target, filter);
    target.replaceParent(parent, filter);
    return filter;
  }

  private static class SyntheticContext implements NodeProcessorCtx {

    ParseContext parseContext;

    public SyntheticContext(ParseContext pCtx) {
      parseContext = pCtx;
    }

    public ParseContext getParseContext() {
      return parseContext;
    }
  }

  private static class JoinSynthetic implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      ParseContext pCtx = ((SyntheticContext) procCtx).getParseContext();

      @SuppressWarnings("unchecked")
      CommonJoinOperator<JoinDesc> join = (CommonJoinOperator<JoinDesc>) nd;

      ReduceSinkOperator source = (ReduceSinkOperator) stack.get(stack.size() - 2);
      int srcPos = join.getParentOperators().indexOf(source);

      List<Operator<? extends OperatorDesc>> parents = join.getParentOperators();

      int[][] targets = getTargets(join);

      Operator<? extends OperatorDesc> parent = source.getParentOperators().get(0);
      RowSchema parentRS = parent.getSchema();

      // don't generate for null-safes.
      if (join.getConf().getNullSafes() != null) {
        for (boolean b : join.getConf().getNullSafes()) {
          if (b) {
            return null;
          }
        }
      }

      for (int targetPos: targets[srcPos]) {
        if (srcPos == targetPos) {
          continue;
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("Synthetic predicate: " + srcPos + " --> " + targetPos);
        }
        ReduceSinkOperator target = (ReduceSinkOperator) parents.get(targetPos);
        List<ExprNodeDesc> sourceKeys = source.getConf().getKeyCols();
        List<ExprNodeDesc> targetKeys = target.getConf().getKeyCols();

        if (sourceKeys.size() < 1) {
          continue;
        }

        ExprNodeDesc syntheticExpr = null;

        for (int i = 0; i < sourceKeys.size(); ++i) {
          List<ExprNodeDesc> inArgs = new ArrayList<ExprNodeDesc>();
          inArgs.add(sourceKeys.get(i));

          ExprNodeDynamicListDesc dynamicExpr =
              new ExprNodeDynamicListDesc(targetKeys.get(i).getTypeInfo(), target, i);

          inArgs.add(dynamicExpr);

          ExprNodeDesc syntheticInExpr =
              ExprNodeGenericFuncDesc.newInstance(FunctionRegistry.getFunctionInfo("in")
                  .getGenericUDF(), inArgs);

          if (syntheticExpr != null) {
            List<ExprNodeDesc> andArgs = new ArrayList<ExprNodeDesc>();
            andArgs.add(syntheticExpr);
            andArgs.add(syntheticInExpr);

            syntheticExpr =
                ExprNodeGenericFuncDesc.newInstance(FunctionRegistry.getFunctionInfo("and")
                    .getGenericUDF(), andArgs);
          } else {
            syntheticExpr = syntheticInExpr;
          }
        }

        Operator<FilterDesc> newFilter = createFilter(source, parent, parentRS, syntheticExpr);
        parent = newFilter;
      }

      return null;
    }

    // calculate filter propagation directions for each alias
    // L<->R for inner/semi join, L<-R for left outer join, R<-L for right outer
    // join
    private int[][] getTargets(CommonJoinOperator<JoinDesc> join) {
      JoinCondDesc[] conds = join.getConf().getConds();

      int aliases = conds.length + 1;
      Vectors vector = new Vectors(aliases);
      for (JoinCondDesc cond : conds) {
        int left = cond.getLeft();
        int right = cond.getRight();
        switch (cond.getType()) {
        case JoinDesc.INNER_JOIN:
        case JoinDesc.LEFT_SEMI_JOIN:
          vector.add(left, right);
          vector.add(right, left);
          break;
        case JoinDesc.LEFT_OUTER_JOIN:
          vector.add(right, left);
          break;
        case JoinDesc.RIGHT_OUTER_JOIN:
          vector.add(left, right);
          break;
        case JoinDesc.FULL_OUTER_JOIN:
          break;
        }
      }
      int[][] result = new int[aliases][];
      for (int pos = 0 ; pos < aliases; pos++) {
        // find all targets recursively
        result[pos] = vector.traverse(pos);
      }
      return result;
    }
  }

  private static class Vectors {

    private final Set<Integer>[] vector;

    @SuppressWarnings("unchecked")
    public Vectors(int length) {
      vector = new Set[length];
    }

    public void add(int from, int to) {
      if (vector[from] == null) {
        vector[from] = new HashSet<Integer>();
      }
      vector[from].add(to);
    }

    public int[] traverse(int pos) {
      Set<Integer> targets = new HashSet<Integer>();
      traverse(targets, pos);
      return toArray(targets);
    }

    private int[] toArray(Set<Integer> values) {
      int index = 0;
      int[] result = new int[values.size()];
      for (int value : values) {
        result[index++] = value;
      }
      return result;
    }

    private void traverse(Set<Integer> targets, int pos) {
      if (vector[pos] == null) {
        return;
      }
      for (int target : vector[pos]) {
        if (targets.add(target)) {
          traverse(targets, target);
        }
      }
    }
  }
}
