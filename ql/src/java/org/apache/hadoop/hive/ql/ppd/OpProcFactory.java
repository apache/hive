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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import javolution.util.FastBitSet;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowType;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.plan.ptf.BoundaryDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFunctionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowTableFunctionDef;
import org.apache.hadoop.hive.ql.ppd.ExprWalkerInfo.ExprInfo;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFDenseRank.GenericUDAFDenseRankEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFLead.GenericUDAFLeadEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFRank.GenericUDAFRankEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;


/**
 * Operator factory for predicate pushdown processing of operator graph Each
 * operator determines the pushdown predicates by walking the expression tree.
 * Each operator merges its own pushdown predicates with those of its children
 * Finally the TableScan operator gathers all the predicates and inserts a
 * filter operator after itself. TODO: Further optimizations 1) Multi-insert
 * case 2) Create a filter operator for those predicates that couldn't be pushed
 * to the previous operators in the data flow 3) Merge multiple sequential
 * filter predicates into so that plans are more readable 4) Remove predicates
 * from filter operators that have been pushed. Currently these pushed
 * predicates are evaluated twice.
 */
public final class OpProcFactory {

  protected static final Logger LOG = LoggerFactory.getLogger(OpProcFactory.class
    .getName());

  private static ExprWalkerInfo getChildWalkerInfo(Operator<?> current, OpWalkerInfo owi) throws SemanticException {
    if (current.getNumChild() == 0) {
      return null;
    }
    if (current.getNumChild() > 1) {
      // ppd for multi-insert query is not yet implemented
      // we assume that nothing can is pushed beyond this operator
      List<Operator<? extends OperatorDesc>> children =
              Lists.newArrayList(current.getChildOperators());
      for (Operator<?> child : children) {
        ExprWalkerInfo childInfo = owi.getPrunedPreds(child);
        createFilter(child, childInfo, owi);
      }
      return null;
    }
    return owi.getPrunedPreds(current.getChildOperators().get(0));
  }

  private static void removeCandidates(Operator<?> operator, OpWalkerInfo owi) {
    if (operator instanceof FilterOperator) {
      if (owi.getCandidateFilterOps().contains(operator)) {
        removeOperator(operator);
      }
      owi.getCandidateFilterOps().remove(operator);
    }
    if (operator.getChildOperators() != null) {
      List<Operator<? extends OperatorDesc>> children =
              Lists.newArrayList(operator.getChildOperators());
      for (Operator<?> child : children) {
        removeCandidates(child, owi);
      }
    }
  }

  private static void removeAllCandidates(OpWalkerInfo owi) {
    for (FilterOperator operator : owi.getCandidateFilterOps()) {
      removeOperator(operator);
    }
    owi.getCandidateFilterOps().clear();
  }

  private static void removeOperator(Operator<? extends OperatorDesc> operator) {
    // since removeParent/removeChild updates the childOperators and parentOperators list in place
    // we need to make a copy of list to iterator over them
    List<Operator<? extends OperatorDesc>> children = new ArrayList<>(operator.getChildOperators());
    List<Operator<? extends OperatorDesc>> parents = new ArrayList<>(operator.getParentOperators());
    for (Operator<? extends OperatorDesc> parent : parents) {
      parent.getChildOperators().addAll(children);
      parent.removeChild(operator);
    }
    for (Operator<? extends OperatorDesc> child : children) {
      child.getParentOperators().addAll(parents);
      child.removeParent(operator);
    }
  }

  /**
   * Processor for Script Operator Prevents any predicates being pushed.
   */
  public static class ScriptPPD extends DefaultPPD implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.debug("Processing for {}", nd.toString());
      // script operator is a black-box to hive so no optimization here
      // assuming that nothing can be pushed above the script op
      // same with LIMIT op
      // create a filter with all children predicates
      OpWalkerInfo owi = (OpWalkerInfo) procCtx;
      ExprWalkerInfo childInfo = getChildWalkerInfo((Operator<?>) nd, owi);
      if (childInfo != null && HiveConf.getBoolVar(owi.getParseContext().getConf(),
          HiveConf.ConfVars.HIVE_PPD_REMOVE_DUPLICATE_FILTERS)) {
        ExprWalkerInfo unpushedPreds = mergeChildrenPred(nd, owi, null, false);
        return createFilter((Operator)nd, unpushedPreds, owi);
      }
      return null;
    }

  }

  public static class PTFPPD extends ScriptPPD {

    /*
     * For WindowingTableFunction if:
     * a. there is a Rank/DenseRank function: if there are unpushedPred of the form
     *    rnkValue < Constant; then use the smallest Constant val as the 'rankLimit'
     *    on the WindowingTablFn.
     * b. If there are no Wdw Fns with an End Boundary past the current row, the
     *    condition can be pushed down as a limit pushdown(mapGroupBy=true)
     *
     * (non-Javadoc)
     * @see org.apache.hadoop.hive.ql.ppd.OpProcFactory.ScriptPPD#process(org.apache.hadoop.hive.ql.lib.Node, java.util.Stack, org.apache.hadoop.hive.ql.lib.NodeProcessorCtx, java.lang.Object[])
     */
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.debug("Processing for {}", nd.toString());
      OpWalkerInfo owi = (OpWalkerInfo) procCtx;
      PTFOperator ptfOp = (PTFOperator) nd;

      pushRankLimit(ptfOp, owi);
      return super.process(nd, stack, procCtx, nodeOutputs);
    }

    private void pushRankLimit(PTFOperator ptfOp, OpWalkerInfo owi) throws SemanticException {
      PTFDesc conf = ptfOp.getConf();

      if ( !conf.forWindowing() ) {
        return;
      }

      float threshold = owi.getParseContext().getConf().getFloatVar(HiveConf.ConfVars.HIVE_LIMIT_PUSHDOWN_MEMORY_USAGE);
      if (threshold <= 0 || threshold >= 1) {
        return;
      }

      WindowTableFunctionDef wTFn = (WindowTableFunctionDef) conf.getFuncDef();

      List<Integer> rFnIdxs = rankingFunctions(wTFn);

      if ( rFnIdxs.size() == 0 ) {
        return;
      }

      ExprWalkerInfo childInfo = getChildWalkerInfo(ptfOp, owi);

      if (childInfo == null) {
        return;
      }

      List<ExprNodeDesc> preds = new ArrayList<ExprNodeDesc>();
      Iterator<List<ExprNodeDesc>> iterator = childInfo.getFinalCandidates().values().iterator();
      while (iterator.hasNext()) {
        for (ExprNodeDesc pred : iterator.next()) {
          preds = ExprNodeDescUtils.split(pred, preds);
        }
      }

      int rLimit = -1;
      int fnIdx = -1;
      for(ExprNodeDesc pred : preds) {
        int[] pLimit = getLimit(wTFn, rFnIdxs, pred);
        if ( pLimit != null ) {
          if ( rLimit == -1 || rLimit >= pLimit[0] ) {
            rLimit = pLimit[0];
            fnIdx = pLimit[1];
          }
        }
      }

      if ( rLimit != -1 ) {
        wTFn.setRankLimit(rLimit);
        wTFn.setRankLimitFunction(fnIdx);
        if ( canPushLimitToReduceSink(wTFn)) {
          pushRankLimitToRedSink(ptfOp, owi.getParseContext().getConf(), rLimit);
        }
      }
    }

    private List<Integer> rankingFunctions(WindowTableFunctionDef wTFn) {
      List<Integer> rFns = new ArrayList<Integer>();
      for(int i=0; i < wTFn.getWindowFunctions().size(); i++ ) {
        WindowFunctionDef wFnDef = wTFn.getWindowFunctions().get(i);
        if ( (wFnDef.getWFnEval() instanceof GenericUDAFRankEvaluator) ||
            (wFnDef.getWFnEval() instanceof GenericUDAFDenseRankEvaluator )  ) {
          rFns.add(i);
        }
      }
      return rFns;
    }

    /*
     * For a predicate check if it is a candidate for pushing down as limit optimization.
     * The expression must be of the form rankFn <|<= constant.
     */
    private int[] getLimit(WindowTableFunctionDef wTFn, List<Integer> rFnIdxs, ExprNodeDesc expr) {

      if ( !(expr instanceof ExprNodeGenericFuncDesc) ) {
        return null;
      }

      ExprNodeGenericFuncDesc fExpr = (ExprNodeGenericFuncDesc) expr;

      if ( !(fExpr.getGenericUDF() instanceof GenericUDFOPLessThan) &&
          !(fExpr.getGenericUDF() instanceof GenericUDFOPEqualOrLessThan) ) {
        return null;
      }

      if ( !(fExpr.getChildren().get(0) instanceof ExprNodeColumnDesc) ) {
        return null;
      }

      if ( !(fExpr.getChildren().get(1) instanceof ExprNodeConstantDesc) ) {
        return null;
      }

      ExprNodeConstantDesc constantExpr = (ExprNodeConstantDesc) fExpr.getChildren().get(1) ;

      if ( constantExpr.getTypeInfo() != TypeInfoFactory.intTypeInfo ) {
        return null;
      }

      int limit = (Integer) constantExpr.getValue();
      if ( fExpr.getGenericUDF() instanceof GenericUDFOPEqualOrLessThan ) {
        limit = limit + 1;
      }
      String colName = ((ExprNodeColumnDesc)fExpr.getChildren().get(0)).getColumn();

      for(int i=0; i < rFnIdxs.size(); i++ ) {
        String fAlias = wTFn.getWindowFunctions().get(i).getAlias();
        if ( fAlias.equals(colName)) {
          return new int[] {limit,i};
        }
      }

      return null;
    }

    /*
     * Limit can be pushed down to Map-side if all Window Functions need access
     * to rows before the current row. This is true for:
     * 1. Rank, DenseRank and Lead Fns. (the window doesn't matter for lead fn).
     * 2. If the Window for the function is Row based and the End Boundary doesn't
     * reference rows past the Current Row.
     */
    private boolean canPushLimitToReduceSink(WindowTableFunctionDef wTFn) {
      for(WindowFunctionDef wFnDef : wTFn.getWindowFunctions() ) {
        if ( (wFnDef.getWFnEval() instanceof GenericUDAFRankEvaluator) ||
            (wFnDef.getWFnEval() instanceof GenericUDAFDenseRankEvaluator )  ||
            (wFnDef.getWFnEval() instanceof GenericUDAFLeadEvaluator ) ) {
          continue;
        }
        WindowFrameDef wdwFrame = wFnDef.getWindowFrame();
        BoundaryDef end = wdwFrame.getEnd();
        if (wdwFrame.getWindowType() == WindowType.RANGE) {
          return false;
        }
        if ( end.getDirection() == Direction.FOLLOWING ) {
          return false;
        }
      }
      return true;
    }

    private void pushRankLimitToRedSink(PTFOperator ptfOp, HiveConf conf, int rLimit) throws SemanticException {

      Operator<? extends OperatorDesc> parent = ptfOp.getParentOperators().get(0);
      Operator<? extends OperatorDesc> gP = parent == null ? null : parent.getParentOperators().get(0);

      if ( gP == null || !(gP instanceof ReduceSinkOperator )) {
        return;
      }

      float threshold = conf.getFloatVar(HiveConf.ConfVars.HIVE_LIMIT_PUSHDOWN_MEMORY_USAGE);

      ReduceSinkOperator rSink = (ReduceSinkOperator) gP;
      ReduceSinkDesc rDesc = rSink.getConf();
      rDesc.setTopN(rLimit);
      rDesc.setTopNMemoryUsage(threshold);
      rDesc.setMapGroupBy(true);
      rDesc.setPTFReduceSink(true);
    }
  }

  public static class UDTFPPD extends DefaultPPD implements SemanticNodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);
      OpWalkerInfo owi = (OpWalkerInfo) procCtx;
      ExprWalkerInfo prunedPred = owi.getPrunedPreds((Operator<? extends OperatorDesc>) nd);
      if (prunedPred == null || !prunedPred.hasAnyCandidates()) {
        return null;
      }
      Map<String, List<ExprNodeDesc>> candidates = prunedPred.getFinalCandidates();
      createFilter((Operator)nd, prunedPred, owi);
      candidates.clear();
      return null;
    }

  }

  public static class LateralViewJoinerPPD extends JoinerPPD implements SemanticNodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      Object o = super.process(nd, stack, procCtx, nodeOutputs);
      OpWalkerInfo owi = (OpWalkerInfo) procCtx;
      if (HiveConf.getBoolVar(owi.getParseContext().getConf(),
          HiveConf.ConfVars.HIVE_PPD_REMOVE_DUPLICATE_FILTERS)) {
        // The lateral view join is allowed to have a filter pushed through it.
        // We need to remove the filter candidate here once it has been applied.
        // If we do not remove it here, the candidates will be cleared out through
        // the getCandidateFilterOps().clear() method in another processor and the
        // filter candidate would not be removed.
        removeAllCandidates(owi);
      }
      return o;
    }
  }

  public static class LateralViewForwardPPD extends DefaultPPD implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.debug("Processing for {}", nd.toString());
      OpWalkerInfo owi = (OpWalkerInfo) procCtx;

      // The lateral view forward operator has 2 children, a SELECT(*) and
      // a SELECT(cols) (for the UDTF operator) The child at index 0 is the
      // SELECT(*) because that's the way that the DAG was constructed. We
      // only want to get the predicates from the SELECT(*).
      ExprWalkerInfo childPreds = owi
      .getPrunedPreds((Operator<? extends OperatorDesc>) nd.getChildren()
      .get(0));

      owi.putPrunedPreds((Operator<? extends OperatorDesc>) nd, childPreds);
      return null;
    }

  }

  /**
   * Combines predicates of its child into a single expression and adds a filter
   * op as new child.
   */
  public static class TableScanPPD extends DefaultPPD implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.debug("Processing for {}", nd.toString());
      OpWalkerInfo owi = (OpWalkerInfo) procCtx;
      TableScanOperator tsOp = (TableScanOperator) nd;
      mergeWithChildrenPred(tsOp, owi, null, null);
      if (HiveConf.getBoolVar(owi.getParseContext().getConf(),
          HiveConf.ConfVars.HIVE_PPD_REMOVE_DUPLICATE_FILTERS)) {
        // remove all the candidate filter operators
        // when we get to the TS
        removeAllCandidates(owi);
      }
      ExprWalkerInfo pushDownPreds = owi.getPrunedPreds(tsOp);
      // nonFinalCandidates predicates should be empty
      assert pushDownPreds == null || !pushDownPreds.hasNonFinalCandidates();
      return createFilter(tsOp, pushDownPreds, owi);
    }

  }

  /**
   * Determines the push down predicates in its where expression and then
   * combines it with the push down predicates that are passed from its children.
   */
  public static class FilterPPD extends DefaultPPD implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      return process(nd, stack, procCtx, false, nodeOutputs);
    }

    Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
            boolean onlySyntheticJoinPredicate, Object... nodeOutputs) throws SemanticException {
      LOG.debug("Processing for {}", nd.toString());

      OpWalkerInfo owi = (OpWalkerInfo) procCtx;
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;

      // if this filter is generated one, predicates need not to be extracted
      ExprWalkerInfo ewi = owi.getPrunedPreds(op);
      // Don't push a sampling predicate since createFilter() always creates filter
      // with isSamplePred = false. Also, the filterop with sampling pred is always
      // a child of TableScan, so there is no need to push this predicate.
      if (ewi == null && !((FilterOperator)op).getConf().getIsSamplingPred()
              && (!onlySyntheticJoinPredicate
                      || ((FilterOperator)op).getConf().isSyntheticJoinPredicate())) {
        // get pushdown predicates for this operator's predicate
        ExprNodeDesc predicate = (((FilterOperator) nd).getConf()).getPredicate();
        ewi = ExprWalkerProcFactory.extractPushdownPreds(owi, op, predicate);
        if (!ewi.isDeterministic()) {
          /* predicate is not deterministic */
          if (op.getChildren() != null && op.getChildren().size() == 1) {
            createFilter(op, owi
                .getPrunedPreds((Operator<? extends OperatorDesc>) (op
                .getChildren().get(0))), owi);
          }
          return null;
        }
        logExpr(nd, ewi);
        owi.putPrunedPreds((Operator<? extends OperatorDesc>) nd, ewi);
        if (HiveConf.getBoolVar(owi.getParseContext().getConf(),
            HiveConf.ConfVars.HIVE_PPD_REMOVE_DUPLICATE_FILTERS)) {
          // add this filter for deletion, if it does not have non-final candidates
          owi.addCandidateFilterOp((FilterOperator)op);
          Map<String, List<ExprNodeDesc>> residual = ewi.getResidualPredicates(true);
          createFilter(op, residual, owi);
        }
      }
      // merge it with children predicates
      boolean hasUnpushedPredicates = mergeWithChildrenPred(nd, owi, ewi, null);
      if (HiveConf.getBoolVar(owi.getParseContext().getConf(),
          HiveConf.ConfVars.HIVE_PPD_REMOVE_DUPLICATE_FILTERS)) {
        if (hasUnpushedPredicates) {
          ExprWalkerInfo unpushedPreds = mergeChildrenPred(nd, owi, null, false);
          return createFilter((Operator)nd, unpushedPreds, owi);
        }
      }
      return null;
    }
  }

  public static class SimpleFilterPPD extends FilterPPD implements SemanticNodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      FilterOperator filterOp = (FilterOperator) nd;
      // We try to push the full Filter predicate iff:
      // - the Filter is on top of a TableScan, or
      // - the Filter is on top of a PTF (between PTF and Filter, there might be Select operators)
      // - the Filter is on top of a LateralViewJoinOperator (the filter can be pushed through one
      //   side of the join with the base table predicate, but not the UDTF side.)
      // Otherwise, we push only the synthetic join predicates
      // Note : pushing Filter on top of PTF is necessary so the LimitPushdownOptimizer for Rank
      // functions gets enabled.
      boolean onlySyntheticJoinPredicate = false;
      if (!(filterOp.getParentOperators().get(0) instanceof TableScanOperator)) {
        onlySyntheticJoinPredicate = true;
        Operator<?> parent = filterOp;
        while (true) {
          assert parent.getParentOperators().size() == 1;
          parent = parent.getParentOperators().get(0);
          if (parent instanceof SelectOperator) {
            continue;
          } else if (parent instanceof PTFOperator) {
            onlySyntheticJoinPredicate = false;
            break;
          } else if (parent instanceof LateralViewJoinOperator) {
            onlySyntheticJoinPredicate = false;
            break;
          } else {
            break;
          }
        }
      }
      return process(nd, stack, procCtx, onlySyntheticJoinPredicate, nodeOutputs);
    }
  }

  /**
   * Determines predicates for which alias can be pushed to it's parents. See
   * the comments for getQualifiedAliases function.
   */
  public static class JoinerPPD extends DefaultPPD implements SemanticNodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.debug("Processing for {}", nd.toString());
      OpWalkerInfo owi = (OpWalkerInfo) procCtx;
      Set<String> aliases = getAliases(nd);
      // we pass null for aliases here because mergeWithChildrenPred filters
      // aliases in the children node context and we need to filter them in
      // the current JoinOperator's context
      mergeWithChildrenPred(nd, owi, null, null);
      ExprWalkerInfo prunePreds =
          owi.getPrunedPreds((Operator<? extends OperatorDesc>) nd);
      if (prunePreds != null) {
        Set<String> toRemove = new HashSet<String>();
        // we don't push down any expressions that refer to aliases that can;t
        // be pushed down per getQualifiedAliases
        for (Entry<String, List<ExprNodeDesc>> entry : prunePreds.getFinalCandidates().entrySet()) {
          String key = entry.getKey();
          List<ExprNodeDesc> value = entry.getValue();
          if (key == null && ExprNodeDescUtils.isAllConstants(value)) {
            continue;   // propagate constants
          }
          if (!aliases.contains(key)) {
            toRemove.add(key);
          }
        }
        for (String alias : toRemove) {
          for (ExprNodeDesc expr :
            prunePreds.getFinalCandidates().get(alias)) {
            // add expr to the list of predicates rejected from further pushing
            // so that we know to add it in createFilter()
            ExprInfo exprInfo;
            if (alias != null) {
              exprInfo = prunePreds.addOrGetExprInfo(expr);
              exprInfo.alias = alias;
            } else {
              exprInfo = prunePreds.getExprInfo(expr);
            }
            prunePreds.addNonFinalCandidate(exprInfo != null ? exprInfo.alias : null, expr);
          }
          prunePreds.getFinalCandidates().remove(alias);
        }
        return handlePredicates(nd, prunePreds, owi);
      }
      return null;
    }

    protected Set<String> getAliases(Node nd) throws SemanticException {
      return ((Operator)nd).getSchema().getTableNames();
    }

    protected Object handlePredicates(Node nd, ExprWalkerInfo prunePreds, OpWalkerInfo owi)
        throws SemanticException {
      if (HiveConf.getBoolVar(owi.getParseContext().getConf(),
          HiveConf.ConfVars.HIVE_PPD_REMOVE_DUPLICATE_FILTERS)) {
        return createFilter((Operator)nd, prunePreds.getResidualPredicates(true), owi);
      }
      return null;
    }
  }

  public static class JoinPPD extends JoinerPPD {

    @Override
    protected Set<String> getAliases(Node nd) {
      return getQualifiedAliases((JoinOperator) nd, ((JoinOperator)nd).getSchema());
    }

    /**
     * Figures out the aliases for whom it is safe to push predicates based on
     * ANSI SQL semantics. The join conditions are left associative so "a
     * RIGHT OUTER JOIN b LEFT OUTER JOIN c INNER JOIN d" is interpreted as
     * "((a RIGHT OUTER JOIN b) LEFT OUTER JOIN c) INNER JOIN d".  For inner
     * joins, both the left and right join subexpressions are considered for
     * pushing down aliases, for the right outer join, the right subexpression
     * is considered and the left ignored and for the left outer join, the
     * left subexpression is considered and the left ignored. Here, aliases b
     * and d are eligible to be pushed up.
     *
     * TODO: further optimization opportunity for the case a.c1 = b.c1 and b.c2
     * = c.c2 a and b are first joined and then the result with c. But the
     * second join op currently treats a and b as separate aliases and thus
     * disallowing predicate expr containing both tables a and b (such as a.c3
     * + a.c4 > 20). Such predicates also can be pushed just above the second
     * join and below the first join
     *
     * @param op
     *          Join Operator
     * @param rr
     *          Row resolver
     * @return set of qualified aliases
     */
    private Set<String> getQualifiedAliases(JoinOperator op, RowSchema rs) {
      Set<String> aliases = new HashSet<String>();
      JoinCondDesc[] conds = op.getConf().getConds();
      Map<Integer, Set<String>> posToAliasMap = op.getPosToAliasMap();
      int i;
      for (i=conds.length-1; i>=0; i--){
        if (conds[i].getType() == JoinDesc.INNER_JOIN) {
          aliases.addAll(posToAliasMap.get(i+1));
        } else if (conds[i].getType() == JoinDesc.FULL_OUTER_JOIN) {
          break;
        } else if (conds[i].getType() == JoinDesc.RIGHT_OUTER_JOIN) {
          aliases.addAll(posToAliasMap.get(i+1));
          break;
        } else if (conds[i].getType() == JoinDesc.LEFT_OUTER_JOIN) {
          continue;
        }
      }
      if(i == -1){
        aliases.addAll(posToAliasMap.get(0));
      }
      Set<String> aliases2 = rs.getTableNames();
      aliases.retainAll(aliases2);
      return aliases;
    }
  }

  public static class ReduceSinkPPD extends DefaultPPD implements SemanticNodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                          Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);
      Operator<?> operator = (Operator<?>) nd;
      OpWalkerInfo owi = (OpWalkerInfo) procCtx;
      if (operator.getNumChild() == 1 &&
          operator.getChildOperators().get(0) instanceof JoinOperator) {
        if (HiveConf.getBoolVar(owi.getParseContext().getConf(),
            HiveConf.ConfVars.HIVE_PPD_RECOGNIZE_TRANSITIVITY)) {
          JoinOperator child = (JoinOperator) operator.getChildOperators().get(0);
          int targetPos = child.getParentOperators().indexOf(operator);
          applyFilterTransitivity(child, targetPos, owi);
        }
      }
      return null;
    }

    /**
     * Adds additional pushdown predicates for a join operator by replicating
     * filters transitively over all the equijoin conditions.
     *
     * If we have a predicate "t.col=1" and the equijoin conditions
     * "t.col=s.col" and "t.col=u.col", we add the filters "s.col=1" and
     * "u.col=1". Note that this does not depend on the types of joins (ie.
     * inner, left/right/full outer) between the tables s, t and u because if
     * a predicate, eg. "t.col=1" is present in getFinalCandidates() at this
     * point, we have already verified that it can be pushed down, so any rows
     * emitted must satisfy s.col=t.col=u.col=1 and replicating the filters
     * like this is ok.
     */
    private void applyFilterTransitivity(JoinOperator join, int targetPos, OpWalkerInfo owi)
        throws SemanticException {

      ExprWalkerInfo joinPreds = owi.getPrunedPreds(join);
      if (joinPreds == null || !joinPreds.hasAnyCandidates()) {
        return;
      }
      Map<String, List<ExprNodeDesc>> oldFilters = joinPreds.getFinalCandidates();
      Map<String, List<ExprNodeDesc>> newFilters = new HashMap<String, List<ExprNodeDesc>>();

      List<Operator<? extends OperatorDesc>> parentOperators = join.getParentOperators();

      ReduceSinkOperator target = (ReduceSinkOperator) parentOperators.get(targetPos);
      List<ExprNodeDesc> targetKeys = target.getConf().getKeyCols();

      ExprWalkerInfo rsPreds = owi.getPrunedPreds(target);
      boolean recogniseColumnEqualities = HiveConf.getBoolVar(owi.getParseContext().getConf(),
              HiveConf.ConfVars.HIVE_PPD_RECOGNIZE_COLUMN_EQUALITIES);
      for (int sourcePos = 0; sourcePos < parentOperators.size(); sourcePos++) {
        ReduceSinkOperator source = (ReduceSinkOperator) parentOperators.get(sourcePos);
        List<ExprNodeDesc> sourceKeys = source.getConf().getKeyCols();
        Set<String> sourceAliases = new HashSet<String>(Arrays.asList(source.getInputAliases()));
        for (Map.Entry<String, List<ExprNodeDesc>> entry : oldFilters.entrySet()) {
          if (entry.getKey() == null && ExprNodeDescUtils.isAllConstants(entry.getValue())) {
            // propagate constants
            for (String targetAlias : target.getInputAliases()) {
              rsPreds.addPushDowns(targetAlias, entry.getValue());
            }
            continue;
          }
          if (!sourceAliases.contains(entry.getKey())) {
            continue;
          }

          Set<ExprNodeColumnDesc> columnsInPredicates = null;
          if (recogniseColumnEqualities) {
            columnsInPredicates = owi.getColumnsInPredicates().get(source);
            if (columnsInPredicates == null) {
              columnsInPredicates = collectColumnsInPredicates(entry.getValue());
              owi.getColumnsInPredicates().put(source, columnsInPredicates);
            }
          }

          for (ExprNodeDesc predicate : entry.getValue()) {
            ExprNodeDesc backtrack = ExprNodeDescUtils.backtrack(predicate, join, source);
            if (backtrack == null) {
              continue;
            }
            ExprNodeDesc replaced = ExprNodeDescUtils.replace(backtrack, sourceKeys, targetKeys);
            if (replaced == null) {
              if (!recogniseColumnEqualities) {
                continue;
              }

              Map<ExprNodeDesc, ExprNodeDesc> equalities = owi.getEqualities().get(source);
              if (equalities == null) {
                equalities = searchForEqualities(join, sourcePos, source, columnsInPredicates);
                owi.getEqualities().put(source, equalities);
              }
              if (equalities.isEmpty()) {
                continue;
              }

              ExprNodeDesc newPredicate = replaceColumnExprNodes(predicate, equalities);
              backtrack = ExprNodeDescUtils.backtrack(newPredicate, join, source);
              if (backtrack == null) {
                continue;
              }
              replaced = ExprNodeDescUtils.replace(backtrack, sourceKeys, targetKeys);
              if (replaced == null) {
                continue;
              }
            }
            for (String targetAlias : target.getInputAliases()) {
              rsPreds.addFinalCandidate(targetAlias, replaced);
            }
          }
        }
      }
    }

    /**
     * Traverse each predicate expression trees given in the predicates list and collect all ExprNodeColumnDesc.
     * @param predicates list of predicate expressions
     * @return union of ExprNodeColumnDescs referenced form the given predicates.
     */
    private Set<ExprNodeColumnDesc> collectColumnsInPredicates(List<ExprNodeDesc> predicates) {
      Set<ExprNodeColumnDesc> columnsInPredicates;
      columnsInPredicates = new HashSet<>();
      for (ExprNodeDesc predicate : predicates) {
        columnsInPredicates.addAll(ExprNodeDescUtils.findAllColumnDescs(predicate));
      }
      return columnsInPredicates;
    }

    /**
     * Traverse the operator tree and collect equal columns.
     * Traversal starts from the specified source operator to TableScan operators.
     * This method calls searchForEqualities(Operator<?> operator, Set<ExprNodeColumnDesc> exprNodeDescSet)
     * to find equalities. Since it returns a map where values are names of input columns of the join operator
     * this method maps these names to output column expression using the join operators columnExprMap.
     *
     * @param join Parent operator of source operator
     * @param sourcePos Index of source operator in parent join branches. (0 or 1).
     * @param source root of operator tree to traverse
     * @param startNodes set of columns search equal columns for
     * @return Map of equal columns: key column coming from the passed exprNodeDescSet, value column equals to the key.
     */
    private Map<ExprNodeDesc, ExprNodeDesc> searchForEqualities(
            JoinOperator join, int sourcePos, ReduceSinkOperator source, Set<ExprNodeColumnDesc> startNodes) {
      Map<ExprNodeDesc, String> equalities = searchForEqualities(source, startNodes);
      if (equalities.isEmpty()) {
        return Collections.emptyMap();
      }

      Map<ExprNodeDesc, ExprNodeDesc> replaceMap = new HashMap<>(equalities.size());
      for (Entry<ExprNodeDesc, String> eqEntry : equalities.entrySet()) {
        for (Entry<String, ExprNodeDesc> joinColMapEntry : join.getColumnExprMap().entrySet()) {
          if (join.getConf().getReversedExprs().get(joinColMapEntry.getKey()) != sourcePos) {
            continue;
          }
          if (!(joinColMapEntry.getValue() instanceof ExprNodeColumnDesc)) {
            continue;
          }
          if (((ExprNodeColumnDesc) joinColMapEntry.getValue()).getColumn().equals(eqEntry.getValue())) {
            replaceMap.put(eqEntry.getKey(), joinColMapEntry.getValue());
            break;
          }
        }
      }

      return replaceMap;
    }

    /**
     * Dispatcher method for column equality search traversal.
     *
     * @param operator root of subtree to traverse
     * @param exprNodeDescSet set of columns search equal columns for
     * @return Map of equal columns: key column coming from the passed exprNodeDescSet, value the name of equal input
     * column of current operator.
     */
    private Map<ExprNodeDesc, String> searchForEqualities(
            Operator<?> operator, Set<ExprNodeColumnDesc> exprNodeDescSet) {
      if (exprNodeDescSet.isEmpty()) {
        return Collections.emptyMap();
      }

      if (operator instanceof CommonJoinOperator) {
        return searchForEqualitiesInJoin((CommonJoinOperator<?>)operator, exprNodeDescSet);
      } else {
        return searchForEqualitiesDefault(operator, exprNodeDescSet);
      }
    }

    /**
     * Search equal columns in the join expressions for each column specified in the given set.
     *
     * @param join CommonJoinOperator which join expressions are scanned
     * @param exprNodeDescSet set of columns search equal columns for
     * @return Map of equal columns: key column coming from the passed exprNodeDescSet, value the name of equal input
     * column of current operator.
     */
    private Map<ExprNodeDesc, String> searchForEqualitiesInJoin(
            CommonJoinOperator<?> join, Set<ExprNodeColumnDesc> exprNodeDescSet) {
      Map<ExprNodeDesc, String> equalities = new HashMap<>();
      for (ExprNodeColumnDesc exprNodeDesc : exprNodeDescSet) {
        ExprNodeDesc mappedColExpr = join.getColumnExprMap().get(exprNodeDesc.getColumn());
        if (!(mappedColExpr instanceof ExprNodeColumnDesc)) {
          continue;
        }
        String mappedColName = ((ExprNodeColumnDesc)mappedColExpr).getColumn();
        int sideIndex = join.getConf().getReversedExprs().get(exprNodeDesc.getColumn());
        Operator<?> parentRSOperator = join.getParentOperators().get(sideIndex);
        for (int i = 0; i < join.getConf().getJoinKeys()[sideIndex].length; ++i) {
          ExprNodeDesc keyExpr = join.getConf().getJoinKeys()[sideIndex][i];
          if (!keyExpr.isSame(parentRSOperator.getColumnExprMap().get(mappedColName))) {
            continue;
          }

          // exprNodeDesc is join key
          // find the other key in the join expression
          Operator<?> otherParentRSOperator = join.getParentOperators().get(1 - sideIndex);
          for (Entry<String, ExprNodeDesc> joinMapEntry : join.getColumnExprMap().entrySet()) {
            if (join.getConf().getReversedExprs().get(joinMapEntry.getKey()) != 1 - sideIndex) {
              continue;
            }

            String otherColumnName = ((ExprNodeColumnDesc) joinMapEntry.getValue()).getColumn();
            ExprNodeDesc mappedOtherKeyExpr = otherParentRSOperator.getColumnExprMap().get(otherColumnName);
            ExprNodeDesc otherKeyExpr = join.getConf().getJoinKeys()[1 - sideIndex][i];
            if (mappedOtherKeyExpr != null && otherKeyExpr.isSame(mappedOtherKeyExpr)) {
              equalities.put(exprNodeDesc, joinMapEntry.getKey());
            }
          }
        }
      }

      for (Operator<?> parent : join.getParentOperators()) {
        equalities.putAll(searchForEqualities(parent, exprNodeDescSet));
      }

      return equalities;
    }

    /**
     * Default equality search method.
     * 1. Maps all columns specified in exprNodeDescSet to the input column using operators columnExprMap.
     * 2. Search for equalities in the parent operator.
     * 3. Map back the keys and values in the result equalities map using operators columnExprMap.
     * If columnExprMap is null no remapping is required.
     * @param operator operator to traverse.
     * @param exprNodeDescSet set of columns search equal columns for
     * @return Map of equal columns: key column coming from the passed exprNodeDescSet, value the name of equal input
     * column of current operator.
     */
    private Map<ExprNodeDesc, String> searchForEqualitiesDefault(
            Operator<?> operator, Set<ExprNodeColumnDesc> exprNodeDescSet) {
      Map<String, ExprNodeDesc> columnExprMap = operator.getColumnExprMap();
      // Some operators do not have columnExprMap. Example: FilterOperator.
      if (columnExprMap == null) {
        if (operator.getParentOperators().size() == 1) {
          return searchForEqualities(operator.getParentOperators().get(0), exprNodeDescSet);
        } else {
          return Collections.emptyMap();
        }
      }

      Set<ExprNodeColumnDesc> mapped = new HashSet<>(exprNodeDescSet.size());
      Map<ExprNodeDesc, ExprNodeDesc> newOldMap = new HashMap<>(exprNodeDescSet.size());
      for (ExprNodeColumnDesc exprNodeDesc : exprNodeDescSet) {
        ExprNodeDesc valueDesc = operator.getColumnExprMap().get(exprNodeDesc.getColumn());
        if (valueDesc instanceof ExprNodeColumnDesc) {
          mapped.add((ExprNodeColumnDesc) valueDesc);
          newOldMap.put(valueDesc, exprNodeDesc);
        }
      }
      if (operator.getParentOperators().size() == 1) {
        Map<ExprNodeDesc, String> equalities = searchForEqualities(operator.getParentOperators().get(0), mapped);
        Map<ExprNodeDesc, String> mappedEqualities = new HashMap<>(equalities.size());
        for (Entry<ExprNodeDesc, String> eqEntry : equalities.entrySet()) {
          for (Entry<String, ExprNodeDesc> colMapEntry : operator.getColumnExprMap().entrySet()) {
            if (!(colMapEntry.getValue() instanceof ExprNodeColumnDesc)) {
              continue;
            }
            if (((ExprNodeColumnDesc) colMapEntry.getValue()).getColumn().equals(eqEntry.getValue())) {
              mappedEqualities.put(newOldMap.get(eqEntry.getKey()), colMapEntry.getKey());
              break;
            }
          }
        }
        return mappedEqualities;
      } else {
        return Collections.emptyMap();
      }
    }

    /**
     * Replaces exprNodeDescs in the specified exprNodeDesc using replaceMap by traversing the expression tree.
     * @param exprNodeDesc expression where exprNodeDescs should be replaced.
     * @param replaceMap Map containing replacements: key exprNodeDesc should be replaced to value exprNodeDesc.
     * @return The new expression with replaced exprNodeDescs.
     */
    private ExprNodeDesc replaceColumnExprNodes(ExprNodeDesc exprNodeDesc, Map<ExprNodeDesc, ExprNodeDesc> replaceMap) {
      if (exprNodeDesc instanceof ExprNodeColumnDesc) {
        return replaceMap.getOrDefault(exprNodeDesc, exprNodeDesc);
      }
      if (exprNodeDesc instanceof ExprNodeGenericFuncDesc) {
        ExprNodeGenericFuncDesc exprNodeGenericFuncDesc = (ExprNodeGenericFuncDesc) exprNodeDesc.clone();
        List<ExprNodeDesc> replacedChildren = new ArrayList<>(exprNodeDesc.getChildren().size());
        for (ExprNodeDesc child : exprNodeDesc.getChildren()) {
          replacedChildren.add(replaceColumnExprNodes(child, replaceMap));
        }
        exprNodeGenericFuncDesc.setChildren(replacedChildren);
        return exprNodeGenericFuncDesc;
      }

      return exprNodeDesc;
    }
  }

  public static class GroupByPPD extends DefaultPPD implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      super.process(nd, stack, procCtx, nodeOutputs);
      OpWalkerInfo owi = (OpWalkerInfo) procCtx;
      GroupByDesc groupByDesc = ((GroupByOperator)nd).getConf();
      ExprWalkerInfo prunedPred = owi.getPrunedPreds((Operator<? extends OperatorDesc>) nd);
      if (prunedPred == null || !prunedPred.hasAnyCandidates() ||
          !groupByDesc.isGroupingSetsPresent()) {
        return null;
      }

      List<Long> groupingSets = groupByDesc.getListGroupingSets();
      Map<String, List<ExprNodeDesc>> candidates = prunedPred.getFinalCandidates();
      FastBitSet[] fastBitSets = new FastBitSet[groupingSets.size()];
      int groupingSetPosition = groupByDesc.getGroupingSetPosition();
      for (int pos = 0; pos < fastBitSets.length; pos ++) {
        fastBitSets[pos] = GroupByOperator.groupingSet2BitSet(groupingSets.get(pos),
            groupingSetPosition);
      }
      List<ExprNodeDesc> groupByKeys = ((GroupByOperator)nd).getConf().getKeys();
      Map<ExprNodeDesc, ExprNodeDesc> newToOldExprMap = prunedPred.getNewToOldExprMap();
      Map<String, List<ExprNodeDesc>> nonFinalCandidates = new HashMap<String, List<ExprNodeDesc>>();
      Iterator<Map.Entry<String, List<ExprNodeDesc>>> iter = candidates.entrySet().iterator();
      while (iter.hasNext()) {
        Map.Entry<String, List<ExprNodeDesc>> entry = iter.next();
        List<ExprNodeDesc> residualExprs = new ArrayList<ExprNodeDesc>();
        List<ExprNodeDesc> finalCandidates = new ArrayList<ExprNodeDesc>();
        List<ExprNodeDesc> exprs = entry.getValue();
        for (ExprNodeDesc expr : exprs) {
          if (canPredPushdown(expr, groupByKeys, fastBitSets, groupingSetPosition)) {
            finalCandidates.add(expr);
          } else {
            residualExprs.add(newToOldExprMap.get(expr));
          }
        }
        if (!residualExprs.isEmpty()) {
          nonFinalCandidates.put(entry.getKey(), residualExprs);
        }

        if (finalCandidates.isEmpty()) {
          iter.remove();
        } else {
          exprs.clear();
          exprs.addAll(finalCandidates);
        }
      }

      logExpr(nd, prunedPred);
      if (!nonFinalCandidates.isEmpty()) {
        createFilter((Operator) nd, nonFinalCandidates, owi);
      }
      return null;
    }

    private void getGBYKeyPosFromExpr(ExprNodeDesc expr, List<ExprNodeDesc> groupByKeys,
        List<Integer> gbyKeyPos) {
      for (int i = 0; i < groupByKeys.size(); i++) {
        if (groupByKeys.get(i).isSame(expr)) {
          gbyKeyPos.add(i);
          return;
        }
      }
      if (expr.getChildren() != null) {
        for (int i = 0; i < expr.getChildren().size(); i++) {
          getGBYKeyPosFromExpr(expr.getChildren().get(i), groupByKeys, gbyKeyPos);
        }
      }
    }

    private boolean canPredPushdown(ExprNodeDesc expr, List<ExprNodeDesc> groupByKeys,
        FastBitSet[] bitSets, int groupingSetPosition) {
      List<Integer> gbyKeyPos = new ArrayList<Integer>();
      getGBYKeyPosFromExpr(expr, groupByKeys, gbyKeyPos);
      // gbyKeysInExpr can be empty, maybe the expr is a boolean constant, let the expr push down
      for (Integer pos : gbyKeyPos) {
        for (FastBitSet bitset : bitSets) {
          int keyPos = bitset.nextClearBit(0);
          while (keyPos < groupingSetPosition && keyPos != pos) {
            keyPos = bitset.nextClearBit(keyPos + 1);
          }
          // If the gbyKey has not be found in grouping sets, the expr should not be pushed down
          if (keyPos != pos) {
            return false;
          }
        }
      }
      return true;
    }
  }

  /**
   * Default processor which just merges its children.
   */
  public static class DefaultPPD implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LOG.debug("Processing for {}", nd.toString());
      OpWalkerInfo owi = (OpWalkerInfo) procCtx;

      Set<String> includes = getQualifiedAliases((Operator<?>) nd, owi);
      boolean hasUnpushedPredicates = mergeWithChildrenPred(nd, owi, null, includes);
      if (hasUnpushedPredicates && HiveConf.getBoolVar(owi.getParseContext().getConf(),
          HiveConf.ConfVars.HIVE_PPD_REMOVE_DUPLICATE_FILTERS)) {
        if (includes != null || nd instanceof ReduceSinkOperator) {
          owi.getCandidateFilterOps().clear();
        } else {
          ExprWalkerInfo pruned = owi.getPrunedPreds((Operator<? extends OperatorDesc>) nd);
          Map<String, List<ExprNodeDesc>> residual = pruned.getResidualPredicates(true);
          if (residual != null && !residual.isEmpty()) {
            createFilter((Operator) nd, residual, owi);
            pruned.getNonFinalCandidates().clear();
          }
        }
      }
      return null;
    }

    // RS for join, SEL(*) for lateral view
    // SEL for union does not count (should be copied to both sides)
    private Set<String> getQualifiedAliases(Operator<?> operator, OpWalkerInfo owi) {
      if (operator.getNumChild() != 1) {
        return null;
      }
      Operator<?> child = operator.getChildOperators().get(0);
      if (!(child instanceof JoinOperator || child instanceof LateralViewJoinOperator)) {
        return null;
      }
      if (operator instanceof ReduceSinkOperator &&
          ((ReduceSinkOperator)operator).getInputAliases() != null) {
        String[] aliases = ((ReduceSinkOperator)operator).getInputAliases();
        return new HashSet<String>(Arrays.asList(aliases));
      }
      Set<String> includes = operator.getSchema().getTableNames();
      if (includes.size() == 1 && includes.contains("")) {
        // Reduce sink of group by operator
        return null;
      }
      return includes;
    }

    /**
     * @param nd
     * @param ewi
     */
    protected void logExpr(Node nd, ExprWalkerInfo ewi) {
      if (!LOG.isDebugEnabled()) {
        return;
      }
      for (Entry<String, List<ExprNodeDesc>> e : ewi.getFinalCandidates().entrySet()) {
        StringBuilder sb = new StringBuilder("Pushdown predicates of ").append(nd.getName())
            .append(" for alias ").append(e.getKey()).append(": ");
        boolean isFirst = true;
        for (ExprNodeDesc n : e.getValue()) {
          if (!isFirst) {
            sb.append("; ");
          }
          isFirst = false;
          sb.append(n.getExprString());
        }
        LOG.debug(sb.toString());
      }
    }

    /**
     * Take current operators pushdown predicates and merges them with
     * children's pushdown predicates.
     *
     * @param nd
     *          current operator
     * @param owi
     *          operator context during this walk
     * @param ewi
     *          pushdown predicates (part of expression walker info)
     * @param aliases
     *          aliases that this operator can pushdown. null means that all
     *          aliases can be pushed down
     * @throws SemanticException
     */
    protected boolean mergeWithChildrenPred(Node nd, OpWalkerInfo owi,
        ExprWalkerInfo ewi, Set<String> aliases) throws SemanticException {
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      ExprWalkerInfo childPreds = getChildWalkerInfo(op, owi);
      if (childPreds == null) {
        return false;
      }
      if (ewi == null) {
        ewi = new ExprWalkerInfo();
      }
      boolean hasUnpushedPredicates = false;
      for (Entry<String, List<ExprNodeDesc>> e : childPreds
          .getFinalCandidates().entrySet()) {
        if (aliases == null || e.getKey() == null || aliases.contains(e.getKey())) {
          // e.getKey() (alias) can be null in case of constant expressions. see
          // input8.q
          ExprWalkerInfo extractPushdownPreds = ExprWalkerProcFactory
              .extractPushdownPreds(owi, op, e.getValue());
          if (!extractPushdownPreds.getNonFinalCandidates().isEmpty()) {
            hasUnpushedPredicates = true;
          }
          ewi.merge(extractPushdownPreds);
          logExpr(nd, extractPushdownPreds);
        }
      }
      owi.putPrunedPreds((Operator<? extends OperatorDesc>) nd, ewi);
      return hasUnpushedPredicates;
    }

    protected ExprWalkerInfo mergeChildrenPred(Node nd, OpWalkerInfo owi,
        Set<String> excludedAliases, boolean ignoreAliases)
        throws SemanticException {
      if (nd.getChildren() == null) {
        return null;
      }
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>)nd;
      ExprWalkerInfo ewi = new ExprWalkerInfo();
      for (Operator<? extends OperatorDesc> child : op.getChildOperators()) {
        ExprWalkerInfo childPreds = owi.getPrunedPreds(child);
        if (childPreds == null) {
          continue;
        }
        for (Entry<String, List<ExprNodeDesc>> e : childPreds
            .getFinalCandidates().entrySet()) {
          if (ignoreAliases || excludedAliases == null ||
              !excludedAliases.contains(e.getKey()) || e.getKey() == null) {
            ewi.addPushDowns(e.getKey(), e.getValue());
            logExpr(nd, ewi);
          }
        }
      }
      return ewi;
    }
  }

  protected static Object createFilter(Operator op,
      ExprWalkerInfo pushDownPreds, OpWalkerInfo owi) throws SemanticException {
    if (pushDownPreds != null && pushDownPreds.hasAnyCandidates()) {
      return createFilter(op, pushDownPreds.getFinalCandidates(), owi);
    }
    return null;
  }

  protected static Object createFilter(Operator op,
      Map<String, List<ExprNodeDesc>> predicates, OpWalkerInfo owi) throws SemanticException {
    RowSchema inputRS = op.getSchema();

    // combine all predicates into a single expression
    List<ExprNodeDesc> preds = new ArrayList<ExprNodeDesc>();
    Iterator<List<ExprNodeDesc>> iterator = predicates.values().iterator();
    while (iterator.hasNext()) {
      for (ExprNodeDesc pred : iterator.next()) {
        preds = ExprNodeDescUtils.split(pred, preds);
      }
    }

    if (preds.isEmpty()) {
      return null;
    }

    ExprNodeDesc condn = ExprNodeDescUtils.mergePredicates(preds);

    if (op instanceof TableScanOperator && condn instanceof ExprNodeGenericFuncDesc) {
      boolean pushFilterToStorage;
      HiveConf hiveConf = owi.getParseContext().getConf();
      pushFilterToStorage =
        hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_OPT_PPD_STORAGE);
      if (pushFilterToStorage) {
        condn = pushFilterToStorageHandler(
          (TableScanOperator) op,
          (ExprNodeGenericFuncDesc)condn,
          owi,
          hiveConf);
        if (condn == null) {
          // we pushed the whole thing down
          return null;
        }
      }
    }

    // add new filter op
    List<Operator<? extends OperatorDesc>> originalChilren = op
        .getChildOperators();
    op.setChildOperators(null);
    Operator<FilterDesc> output = OperatorFactory.getAndMakeChild(
        new FilterDesc(condn, false), new RowSchema(inputRS.getSignature()), op);
    output.setChildOperators(originalChilren);
    for (Operator<? extends OperatorDesc> ch : originalChilren) {
      List<Operator<? extends OperatorDesc>> parentOperators = ch
          .getParentOperators();
      int pos = parentOperators.indexOf(op);
      assert pos != -1;
      parentOperators.remove(pos);
      parentOperators.add(pos, output); // add the new op as the old
    }

    if (HiveConf.getBoolVar(owi.getParseContext().getConf(),
        HiveConf.ConfVars.HIVE_PPD_REMOVE_DUPLICATE_FILTERS)) {
      // remove the candidate filter ops
      removeCandidates(op, owi);
    }

    // push down current ppd context to newly added filter
    ExprWalkerInfo walkerInfo = owi.getPrunedPreds(op);
    if (walkerInfo != null) {
      walkerInfo.getNonFinalCandidates().clear();
      owi.putPrunedPreds(output, walkerInfo);
    }
    return output;
  }

  /**
   * Attempts to push a predicate down into a storage handler.  For
   * native tables, this is a no-op.
   *
   * @param tableScanOp table scan against which predicate applies
   *
   * @param originalPredicate predicate to be pushed down
   *
   * @param owi object walk info
   *
   * @param hiveConf Hive configuration
   *
   * @return portion of predicate which needs to be evaluated
   * by Hive as a post-filter, or null if it was possible
   * to push down the entire predicate
   */
  private static ExprNodeGenericFuncDesc pushFilterToStorageHandler(
    TableScanOperator tableScanOp,
    ExprNodeGenericFuncDesc originalPredicate,
    OpWalkerInfo owi,
    HiveConf hiveConf) throws SemanticException {

    TableScanDesc tableScanDesc = tableScanOp.getConf();
    Table tbl = tableScanDesc.getTableMetadata();
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPT_INDEX_FILTER)) {
      // attach the original predicate to the table scan operator for index
      // optimizations that require the pushed predicate before pcr & later
      // optimizations are applied
      tableScanDesc.setFilterExpr(originalPredicate);
    }
    if (!tbl.isNonNative()) {
      return originalPredicate;
    }
    HiveStorageHandler storageHandler = tbl.getStorageHandler();
    if (!(storageHandler instanceof HiveStoragePredicateHandler)) {
      // The storage handler does not provide predicate decomposition
      // support, so we'll implement the entire filter in Hive.  However,
      // we still provide the full predicate to the storage handler in
      // case it wants to do any of its own prefiltering.
      tableScanDesc.setFilterExpr(originalPredicate);
      return originalPredicate;
    }
    HiveStoragePredicateHandler predicateHandler =
      (HiveStoragePredicateHandler) storageHandler;
    JobConf jobConf = new JobConf(owi.getParseContext().getConf());
    Utilities.setColumnNameList(jobConf, tableScanOp);
    Utilities.setColumnTypeList(jobConf, tableScanOp);

    try {
      Utilities.copyTableJobPropertiesToConf(
        Utilities.getTableDesc(tbl),
        jobConf);
    } catch (Exception e) {
      throw new SemanticException(e);
    }

    Deserializer deserializer = tbl.getDeserializer();
    HiveStoragePredicateHandler.DecomposedPredicate decomposed =
      predicateHandler.decomposePredicate(
        jobConf,
        deserializer,
        originalPredicate);
    if (decomposed == null) {
      // not able to push anything down
      if (LOG.isDebugEnabled()) {
        LOG.debug("No pushdown possible for predicate:  "
          + originalPredicate.getExprString());
      }
      return originalPredicate;
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Original predicate:  "
        + originalPredicate.getExprString());
      if (decomposed.pushedPredicate != null) {
        LOG.debug(
          "Pushed predicate:  "
          + decomposed.pushedPredicate.getExprString());
      }
      if (decomposed.residualPredicate != null) {
        LOG.debug(
            "Residual predicate:  "
                + decomposed.residualPredicate.getExprString());
      }
    }
    tableScanDesc.setFilterExpr(decomposed.pushedPredicate);
    tableScanDesc.setFilterObject(decomposed.pushedPredicateObject);

    return decomposed.residualPredicate;
  }

  public static SemanticNodeProcessor getFilterProc() {
    return new FilterPPD();
  }

  public static SemanticNodeProcessor getFilterSyntheticJoinPredicateProc() {
    return new SimpleFilterPPD();
  }

  public static SemanticNodeProcessor getJoinProc() {
    return new JoinPPD();
  }

  public static SemanticNodeProcessor getTSProc() {
    return new TableScanPPD();
  }

  public static SemanticNodeProcessor getDefaultProc() {
    return new DefaultPPD();
  }

  public static SemanticNodeProcessor getPTFProc() {
    return new PTFPPD();
  }

  public static SemanticNodeProcessor getSCRProc() {
    return new ScriptPPD();
  }

  public static SemanticNodeProcessor getLIMProc() {
    return new ScriptPPD();
  }

  public static SemanticNodeProcessor getLVFProc() {
    return new LateralViewForwardPPD();
  }

  public static SemanticNodeProcessor getUDTFProc() {
    return new UDTFPPD();
  }

  public static SemanticNodeProcessor getLVJProc() {
    return new LateralViewJoinerPPD();
  }

  public static SemanticNodeProcessor getRSProc() {
    return new ReduceSinkPPD();
  }

  public static SemanticNodeProcessor getGBYProc() {
    return new GroupByPPD();
  }

  private OpProcFactory() {
    // prevent instantiation
  }
}
