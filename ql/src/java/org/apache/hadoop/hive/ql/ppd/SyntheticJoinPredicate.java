/*
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

import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewForwardOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
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
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.PreOrderOnceWalker;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
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
    }

    if (!enabled) {
      return pctx;
    }

    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    opRules.put(new RuleRegExp("R1", "(" +
        TableScanOperator.getOperatorName() + "%" + ".*" +
        ReduceSinkOperator.getOperatorName() + "%" +
        JoinOperator.getOperatorName() + "%)"), new JoinSynthetic());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    SyntheticContext context = new SyntheticContext(pctx);
    SemanticDispatcher disp = new DefaultRuleDispatcher(null, opRules, context);
    PreOrderOnceWalker ogw = new PreOrderOnceWalker(disp);
    // The PreOrderOnceWalker traversal tries to cover all possible paths from the root to every other node. A plan
    // graph with lateral view operators has a particular structure that makes the number of paths exponentially big
    // and the traversal of such graphs prohibitively expensive. For this reason, we exclude lateral view operators
    // from the traversal and essentially disable the synthetic predicate generation for such branches.
    ogw.excludeNode(LateralViewForwardOperator.class);
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
    boolean extended;

    public SyntheticContext(ParseContext pCtx) {
      parseContext = pCtx;
      extended = parseContext.getConf().getBoolVar(ConfVars.TEZ_DYNAMIC_PARTITION_PRUNING_EXTENDED);
    }

    public ParseContext getParseContext() {
      return parseContext;
    }

    public boolean isExtended() {
      return extended;
    }
  }

  private static class JoinSynthetic implements SemanticNodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      SyntheticContext sCtx = (SyntheticContext) procCtx;

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

        ReduceSinkOperator target = (ReduceSinkOperator) parents.get(targetPos);
        List<ExprNodeDesc> sourceKeys = source.getConf().getKeyCols();
        List<ExprNodeDesc> targetKeys = target.getConf().getKeyCols();

        ExprNodeDesc syntheticExpr = null;

        if (sourceKeys.size() > 0) {
          for (int i = 0; i < sourceKeys.size(); ++i) {
            final ExprNodeDesc sourceKey = sourceKeys.get(i);

            List<ExprNodeDesc> inArgs = new ArrayList<>();
            inArgs.add(sourceKey);

            ExprNodeDynamicListDesc dynamicExpr =
              new ExprNodeDynamicListDesc(targetKeys.get(i).getTypeInfo(), target, i);

            inArgs.add(dynamicExpr);

            ExprNodeDesc syntheticInExpr =
              ExprNodeGenericFuncDesc.newInstance(FunctionRegistry.getFunctionInfo("in")
                .getGenericUDF(), inArgs);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Synthetic predicate in " + join + ": " + srcPos + " --> " + targetPos + " (" + syntheticInExpr + ")");
            }

            List<ExprNodeDesc> andArgs = new ArrayList<>();
            if (syntheticExpr != null) {
              andArgs.add(syntheticExpr);
            }
            andArgs.add(syntheticInExpr);

            if (sCtx.isExtended()) {
              // Backtrack
              List<ExprNodeDesc> newExprs = createDerivatives(target.getParentOperators().get(0), targetKeys.get(i), sourceKey);
              if (!newExprs.isEmpty()) {
                if (LOG.isDebugEnabled()) {
                  for (ExprNodeDesc expr : newExprs) {
                    LOG.debug("Additional synthetic predicate in " + join + ": " + srcPos + " --> " + targetPos + " (" + expr + ")");
                  }
                }
                andArgs.addAll(newExprs);
              }
            }

            if (andArgs.size() < 2) {
              syntheticExpr = syntheticInExpr;
            } else {
              // Create AND expression
              syntheticExpr =
                ExprNodeGenericFuncDesc.newInstance(FunctionRegistry.getFunctionInfo("and")
                  .getGenericUDF(), andArgs);
            }
          }
        }

        // Handle non-equi joins like <, <=, >, and >=
        List<ExprNodeDesc> residualFilters = join.getConf().getResidualFilterExprs();
        if (residualFilters != null && residualFilters.size() != 0 &&
          !(srcPos > 1 || targetPos > 1)) { // Either srcPos or targetPos is larger than 1, making this filter a complex one.

          for (ExprNodeDesc filter : residualFilters) {
            if (!(filter instanceof ExprNodeGenericFuncDesc)) {
              continue;
            }

            ExprNodeGenericFuncDesc funcDesc = (ExprNodeGenericFuncDesc) filter;
            // filter should be of type <, >, <= or >=
            if (getFuncText(funcDesc.getFuncText(), 1) == null) {
              // unsupported
              continue;
            }

            final ExprNodeDesc sourceChild = funcDesc.getChildren().get(srcPos);
            final ExprNodeDesc targetChild = funcDesc.getChildren().get(targetPos);
            if (!(sourceChild instanceof ExprNodeColumnDesc &&
              targetChild instanceof ExprNodeColumnDesc)) {
              continue;
            }
            // Create non-equi function.
            List<ExprNodeDesc> funcArgs = new ArrayList<>();
            ExprNodeDesc sourceKey = getRSColExprFromResidualFilter(sourceChild, join);
            funcArgs.add(sourceKey);
            final ExprNodeDynamicListDesc dynamicExpr =
              new ExprNodeDynamicListDesc(targetChild.getTypeInfo(), target, 0,
                getRSColExprFromResidualFilter(targetChild, join));
            funcArgs.add(dynamicExpr);
            ExprNodeDesc funcExpr =
              ExprNodeGenericFuncDesc.newInstance(FunctionRegistry.getFunctionInfo(getFuncText(funcDesc.getFuncText(), srcPos)).getGenericUDF(), funcArgs);

            // TODO : deduplicate the code below.
            LOG.debug(" Non-Equi Join Predicate {}", funcExpr);

            List<ExprNodeDesc> andArgs = new ArrayList<>();
            if (syntheticExpr != null) {
              andArgs.add(syntheticExpr);
            }
            andArgs.add(funcExpr);

            // TODO : HIVE-21098 : Support for extended predicates
            if (andArgs.size() < 2) {
              syntheticExpr = funcExpr;
            } else {
              syntheticExpr =
                ExprNodeGenericFuncDesc.newInstance(FunctionRegistry.getFunctionInfo("and").getGenericUDF(), andArgs);
            }
          }
        }

        if (syntheticExpr != null) {
          Operator<FilterDesc> newFilter = createFilter(source, parent, parentRS, syntheticExpr);
          parent = newFilter;
        }
      }

      return null;
    }

    private ExprNodeDesc getRSColExprFromResidualFilter(ExprNodeDesc childExpr, CommonJoinOperator<JoinDesc> join) {
      ExprNodeColumnDesc colExpr = ExprNodeDescUtils.getColumnExpr(childExpr);

      final String joinColName = colExpr.getColumn();
      // use name to get the alias pos of parent and name in parent
      final int aliasPos = join.getConf().getReversedExprs().get(joinColName);
      final ExprNodeDesc rsColExpr = join.getColumnExprMap().get(joinColName);

      // Get the correct parent
      final ReduceSinkOperator parentRS = (ReduceSinkOperator) (join.getParentOperators().get(aliasPos));

      // Fetch the colExpr from parent
      return parentRS.getColumnExprMap().get(
        ExprNodeDescUtils.extractColName(rsColExpr));
    }

    // This function serves two purposes
    // 1. As the name suggests, provides inverted function text for a given function text
    // 2. If inversion fails, it can be inferred that the given function is not supported.
    String getFuncText(String funcText, final int srcPos) {
      if (srcPos == 0) {
        return funcText;
      }

      return FunctionRegistry.invertFuncText(funcText);
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
        case JoinDesc.ANTI_JOIN:
        //TODO : In case of anti join, bloom filter can be created on left side also ("IN (keylist right table)").
        // But the filter should be "not-in" ("NOT IN (keylist right table)") as we want to select the records from
        // left side which are not present in the right side. But it may cause wrong result as
        // bloom filter may have false positive and thus simply adding not is not correct,
        // special handling is required for "NOT IN".
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

    private List<ExprNodeDesc> createDerivatives(final Operator<?> currentOp,
        final ExprNodeDesc currentNode, final ExprNodeDesc sourceKey) throws SemanticException {
      List<ExprNodeDesc> resultExprs = new ArrayList<>();
      return createDerivatives(resultExprs, currentOp, currentNode, sourceKey) ? resultExprs : new ArrayList<>();
    }

    private boolean createDerivatives(final List<ExprNodeDesc> resultExprs, final Operator<?> op,
        final ExprNodeDesc currentNode, final ExprNodeDesc sourceKey) throws SemanticException {
      // 1. Obtain join operator upstream
      Operator<?> currentOp = op;
      while (!(currentOp instanceof CommonJoinOperator)) {
        if (currentOp.getParentOperators() == null || currentOp.getParentOperators().size() != 1) {
          // Cannot backtrack
          currentOp = null;
          break;
        }
        if (!(currentOp instanceof FilterOperator) &&
            !(currentOp instanceof SelectOperator) &&
            !(currentOp instanceof ReduceSinkOperator) &&
            !(currentOp instanceof GroupByOperator)) {
          // Operator not supported
          currentOp = null;
          break;
        }
        // Move the pointer
        currentOp = currentOp.getParentOperators().get(0);
      }
      if (currentOp == null) {
        // We did not find any join, we are done
        return true;
      }
      CommonJoinOperator<JoinDesc> joinOp = (CommonJoinOperator) currentOp;

      // 2. Backtrack expression to join output
      ExprNodeDesc expr = currentNode;
      if (currentOp != op) {
        if (expr instanceof ExprNodeColumnDesc) {
          // Expression refers to output of current operator, but backtrack methods works
          // from the input columns, hence we need to make resolution for current operator
          // here. If the operator was already the join, there is nothing to do
          if (op.getColumnExprMap() != null) {
            expr = op.getColumnExprMap().get(((ExprNodeColumnDesc) expr).getColumn());
          }
        } else {
          // TODO: We can extend to other expression types
          // We are done
          return true;
        }
      }
      final ExprNodeDesc joinExprNode = ExprNodeDescUtils.backtrack(expr, op, joinOp);
      if (joinExprNode == null || !(joinExprNode instanceof ExprNodeColumnDesc)) {
        // TODO: We can extend to other expression types
        // We are done
        return true;
      }
      final String columnRefJoinInput = ((ExprNodeColumnDesc)joinExprNode).getColumn();

      // 3. Find input position in join for expression obtained
      String columnOutputName = null;
      for (Map.Entry<String, ExprNodeDesc> e : joinOp.getColumnExprMap().entrySet()) {
        if (e.getValue() == joinExprNode) {
          columnOutputName = e.getKey();
          break;
        }
      }
      if (columnOutputName == null) {
        // Maybe the join is pruning columns, though it should not.
        // In any case, we are done
        return true;
      }
      final int srcPos = joinOp.getConf().getReversedExprs().get(columnOutputName);
      final int[][] targets = getTargets(joinOp);
      final ReduceSinkOperator rsOp = (ReduceSinkOperator) joinOp.getParentOperators().get(srcPos);

      // 4. Find expression in input RS operator.
      final Operator<?> rsOpInput = rsOp.getParentOperators().get(0);
      final ExprNodeDesc rsOpInputExprNode = rsOp.getColumnExprMap().get(columnRefJoinInput);
      if (rsOpInputExprNode == null) {
        // Unexpected, we just bail out and we do not infer additional predicates
        return false;
      }
      int posInRSOpKeys = -1;
      for (int i = 0; i < rsOp.getConf().getKeyCols().size(); i++) {
        if (rsOpInputExprNode.isSame(rsOp.getConf().getKeyCols().get(i))) {
          posInRSOpKeys = i;
          break;
        }
      }

      // 5. If it is part of the key, we can create a new semijoin.
      // In addition, we can do the same for siblings
      if (posInRSOpKeys >= 0) {
        // We pass the tests, we add it to the args for the AND expression
        addParentReduceSink(resultExprs, rsOp, posInRSOpKeys, sourceKey);
        for (int targetPos: targets[srcPos]) {
          if (srcPos == targetPos) {
            continue;
          }
          final ReduceSinkOperator otherRsOp = (ReduceSinkOperator) joinOp.getParentOperators().get(targetPos);
          final Operator<?> otherRsOpInput = otherRsOp.getParentOperators().get(0);
          // We pass the tests, we add it to the args for the AND expression
          addParentReduceSink(resultExprs, otherRsOp, posInRSOpKeys, sourceKey);
          // We propagate to operator below
          boolean success = createDerivatives(
              resultExprs, otherRsOpInput, otherRsOp.getConf().getKeyCols().get(posInRSOpKeys), sourceKey);
          if (!success) {
            // Something went wrong, bail out
            return false;
          }
        }
      }

      // 6. Whether it was part of the key or of the value, if we reach here, we can at least
      // continue propagating to operators below
      boolean success = createDerivatives(
          resultExprs, rsOpInput, rsOpInputExprNode, sourceKey);
      if (!success) {
        // Something went wrong, bail out
        return false;
      }

      // 7. We are done, success
      return true;
    }

    private void addParentReduceSink(final List<ExprNodeDesc> andArgs, final ReduceSinkOperator rsOp,
        final int keyIndex, final ExprNodeDesc sourceKey) throws SemanticException {
      ExprNodeDynamicListDesc dynamicExpr =
          new ExprNodeDynamicListDesc(rsOp.getConf().getKeyCols().get(keyIndex).getTypeInfo(), rsOp, keyIndex);
      // Create synthetic IN expression
      List<ExprNodeDesc> inArgs = new ArrayList<>();
      inArgs.add(sourceKey);
      inArgs.add(dynamicExpr);
      ExprNodeDesc newNode = ExprNodeGenericFuncDesc.newInstance(
          FunctionRegistry.getFunctionInfo("in").getGenericUDF(), inArgs);
      andArgs.add(newNode);
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
