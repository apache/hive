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

package org.apache.hadoop.hive.ql.optimizer.correlation;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVECONVERTJOIN;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVECONVERTJOINNOCONDITIONALTASK;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;

/**
 * If two reducer sink operators share the same partition/sort columns and order,
 * they can be merged. This should happen after map join optimization because map
 * join optimization will remove reduce sink operators.
 *
 * This optimizer removes/replaces child-RS (not parent) which is safer way for DefaultGraphWalker.
 */
public class ReduceSinkDeDuplication implements Transform {

  private static final String RS = ReduceSinkOperator.getOperatorName();
  private static final String GBY = GroupByOperator.getOperatorName();
  private static final String JOIN = JoinOperator.getOperatorName();

  protected ParseContext pGraphContext;

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    pGraphContext = pctx;

    // generate pruned column list for all relevant operators
    ReduceSinkDeduplicateProcCtx cppCtx = new ReduceSinkDeduplicateProcCtx(pGraphContext);

    // for auto convert map-joins, it not safe to dedup in here (todo)
    boolean mergeJoins = !pctx.getConf().getBoolVar(HIVECONVERTJOIN) &&
        !pctx.getConf().getBoolVar(HIVECONVERTJOINNOCONDITIONALTASK);

    // If multiple rules can be matched with same cost, last rule will be choosen as a processor
    // see DefaultRuleDispatcher#dispatch()
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", RS + "%.*%" + RS + "%"),
        ReduceSinkDeduplicateProcFactory.getReducerReducerProc());
    opRules.put(new RuleRegExp("R2", RS + "%" + GBY + "%.*%" + RS + "%"),
        ReduceSinkDeduplicateProcFactory.getGroupbyReducerProc());
    if (mergeJoins) {
      opRules.put(new RuleRegExp("R3", JOIN + "%.*%" + RS + "%"),
          ReduceSinkDeduplicateProcFactory.getJoinReducerProc());
    }
    // TODO RS+JOIN

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(ReduceSinkDeduplicateProcFactory
        .getDefaultProc(), opRules, cppCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pGraphContext.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pGraphContext;
  }

  protected class ReduceSinkDeduplicateProcCtx extends AbstractCorrelationProcCtx {

    public ReduceSinkDeduplicateProcCtx(ParseContext pctx) {
      super(pctx);
    }
  }

  static class ReduceSinkDeduplicateProcFactory {

    public static NodeProcessor getReducerReducerProc() {
      return new ReducerReducerProc();
    }

    public static NodeProcessor getGroupbyReducerProc() {
      return new GroupbyReducerProc();
    }

    public static NodeProcessor getJoinReducerProc() {
      return new JoinReducerProc();
    }

    public static NodeProcessor getDefaultProc() {
      return new DefaultProc();
    }
  }

  /*
   * do nothing.
   */
  static class DefaultProc implements NodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      return null;
    }
  }

  public abstract static class AbsctractReducerReducerProc implements NodeProcessor {

    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ReduceSinkDeduplicateProcCtx dedupCtx = (ReduceSinkDeduplicateProcCtx) procCtx;
      if (dedupCtx.hasBeenRemoved((Operator<?>) nd)) {
        return false;
      }
      ReduceSinkOperator cRS = (ReduceSinkOperator) nd;
      Operator<?> child = CorrelationUtilities.getSingleChild(cRS);
      if (child instanceof JoinOperator) {
        return false; // not supported
      }
      if (child instanceof GroupByOperator) {
        GroupByOperator cGBY = (GroupByOperator) child;
        if (!CorrelationUtilities.hasGroupingSet(cRS) && !cGBY.getConf().isGroupingSetsPresent()) {
          return process(cRS, cGBY, dedupCtx);
        }
        return false;
      }
      if (child instanceof ExtractOperator) {
        return process(cRS, dedupCtx);
      }
      return false;
    }

    protected abstract Object process(ReduceSinkOperator cRS, ReduceSinkDeduplicateProcCtx dedupCtx)
        throws SemanticException;

    protected abstract Object process(ReduceSinkOperator cRS, GroupByOperator cGBY,
        ReduceSinkDeduplicateProcCtx dedupCtx) throws SemanticException;

    // for JOIN-RS case, it's not possible generally to merge if child has
    // more key/partition columns than parents
    protected boolean merge(ReduceSinkOperator cRS, JoinOperator pJoin, int minReducer)
        throws SemanticException {
      List<Operator<?>> parents = pJoin.getParentOperators();
      ReduceSinkOperator[] pRSs = parents.toArray(new ReduceSinkOperator[parents.size()]);
      ReduceSinkDesc cRSc = cRS.getConf();
      ReduceSinkDesc pRS0c = pRSs[0].getConf();
      if (cRSc.getKeyCols().size() > pRS0c.getKeyCols().size()) {
        return false;
      }
      if (cRSc.getPartitionCols().size() > pRS0c.getPartitionCols().size()) {
        return false;
      }
      Integer moveReducerNumTo = checkNumReducer(cRSc.getNumReducers(), pRS0c.getNumReducers());
      if (moveReducerNumTo == null ||
          moveReducerNumTo > 0 && cRSc.getNumReducers() < minReducer) {
        return false;
      }

      Integer moveRSOrderTo = checkOrder(cRSc.getOrder(), pRS0c.getOrder());
      if (moveRSOrderTo == null) {
        return false;
      }

      boolean[] sorted = CorrelationUtilities.getSortedTags(pJoin);

      int cKeySize = cRSc.getKeyCols().size();
      for (int i = 0; i < cKeySize; i++) {
        ExprNodeDesc cexpr = cRSc.getKeyCols().get(i);
        ExprNodeDesc[] pexprs = new ExprNodeDesc[pRSs.length];
        for (int tag = 0; tag < pRSs.length; tag++) {
          pexprs[tag] = pRSs[tag].getConf().getKeyCols().get(i);
        }
        int found = CorrelationUtilities.indexOf(cexpr, pexprs, cRS, pRSs, sorted);
        if (found < 0) {
          return false;
        }
      }
      int cPartSize = cRSc.getPartitionCols().size();
      for (int i = 0; i < cPartSize; i++) {
        ExprNodeDesc cexpr = cRSc.getPartitionCols().get(i);
        ExprNodeDesc[] pexprs = new ExprNodeDesc[pRSs.length];
        for (int tag = 0; tag < pRSs.length; tag++) {
          pexprs[tag] = pRSs[tag].getConf().getPartitionCols().get(i);
        }
        int found = CorrelationUtilities.indexOf(cexpr, pexprs, cRS, pRSs, sorted);
        if (found < 0) {
          return false;
        }
      }

      if (moveReducerNumTo > 0) {
        for (ReduceSinkOperator pRS : pRSs) {
          pRS.getConf().setNumReducers(cRS.getConf().getNumReducers());
        }
      }
      return true;
    }

    /**
     * Current RSDedup remove/replace child RS. For key columns,
     * sorting order, and the number of reducers, copy
     * more specific part of configurations of child RS to that of parent RS.
     * For partitioning columns, if both child RS and parent RS have been assigned
     * partitioning columns, we will choose the more general partitioning columns.
     * If parent RS has not been assigned any partitioning column, we will use
     * partitioning columns (if exist) of child RS.
     */
    protected boolean merge(ReduceSinkOperator cRS, ReduceSinkOperator pRS, int minReducer)
        throws SemanticException {
      int[] result = checkStatus(cRS, pRS, minReducer);
      if (result == null) {
        return false;
      }

      if (result[0] > 0) {
        // The sorting columns of the child RS are more specific than
        // those of the parent RS. Assign sorting columns of the child RS
        // to the parent RS.
        List<ExprNodeDesc> childKCs = cRS.getConf().getKeyCols();
        pRS.getConf().setKeyCols(ExprNodeDescUtils.backtrack(childKCs, cRS, pRS));
      }

      if (result[1] < 0) {
        // The partitioning columns of the parent RS are more specific than
        // those of the child RS.
        List<ExprNodeDesc> childPCs = cRS.getConf().getPartitionCols();
        if (childPCs != null && !childPCs.isEmpty()) {
          // If partitioning columns of the child RS are assigned,
          // assign these to the partitioning columns of the parent RS.
          pRS.getConf().setPartitionCols(ExprNodeDescUtils.backtrack(childPCs, cRS, pRS));
        }
      } else if (result[1] > 0) {
        // The partitioning columns of the child RS are more specific than
        // those of the parent RS.
        List<ExprNodeDesc> parentPCs = pRS.getConf().getPartitionCols();
        if (parentPCs == null || parentPCs.isEmpty()) {
          // If partitioning columns of the parent RS are not assigned,
          // assign partitioning columns of the child RS to the parent RS.
          ArrayList<ExprNodeDesc> childPCs = cRS.getConf().getPartitionCols();
          pRS.getConf().setPartitionCols(ExprNodeDescUtils.backtrack(childPCs, cRS, pRS));
        }
      }

      if (result[2] > 0) {
        // The sorting order of the child RS is more specific than
        // that of the parent RS. Assign the sorting order of the child RS
        // to the parent RS.
        if (result[0] <= 0) {
          // Sorting columns of the parent RS are more specific than those of the
          // child RS but Sorting order of the child RS is more specific than
          // that of the parent RS.
          throw new SemanticException("Sorting columns and order don't match. " +
              "Try set " + HiveConf.ConfVars.HIVEOPTREDUCEDEDUPLICATION + "=false;");
        }
        pRS.getConf().setOrder(cRS.getConf().getOrder());
      }

      if (result[3] > 0) {
        // The number of reducers of the child RS is more specific than
        // that of the parent RS. Assign the number of reducers of the child RS
        // to the parent RS.
        pRS.getConf().setNumReducers(cRS.getConf().getNumReducers());
      }

      return true;
    }

    /**
     * Returns merge directions between two RSs for criterias (ordering, number of reducers,
     * reducer keys, partition keys). Returns null if any of categories is not mergeable.
     *
     * Values for each index can be -1, 0, 1
     * 1. 0 means two configuration in the category is the same
     * 2. for -1, configuration of parent RS is more specific than child RS
     * 3. for 1, configuration of child RS is more specific than parent RS
     */
    private int[] checkStatus(ReduceSinkOperator cRS, ReduceSinkOperator pRS, int minReducer)
        throws SemanticException {
      ReduceSinkDesc cConf = cRS.getConf();
      ReduceSinkDesc pConf = pRS.getConf();
      Integer moveRSOrderTo = checkOrder(cConf.getOrder(), pConf.getOrder());
      if (moveRSOrderTo == null) {
        return null;
      }
      Integer moveReducerNumTo = checkNumReducer(cConf.getNumReducers(), pConf.getNumReducers());
      if (moveReducerNumTo == null ||
          moveReducerNumTo > 0 && cConf.getNumReducers() < minReducer) {
        return null;
      }
      List<ExprNodeDesc> ckeys = cConf.getKeyCols();
      List<ExprNodeDesc> pkeys = pConf.getKeyCols();
      Integer moveKeyColTo = checkExprs(ckeys, pkeys, cRS, pRS);
      if (moveKeyColTo == null) {
        return null;
      }
      List<ExprNodeDesc> cpars = cConf.getPartitionCols();
      List<ExprNodeDesc> ppars = pConf.getPartitionCols();
      Integer movePartitionColTo = checkExprs(cpars, ppars, cRS, pRS);
      if (movePartitionColTo == null) {
        return null;
      }
      return new int[] {moveKeyColTo, movePartitionColTo, moveRSOrderTo, moveReducerNumTo};
    }

    /**
     * Overlapping part of keys should be the same between parent and child.
     * And if child has more keys than parent, non-overlapping part of keys
     * should be backtrackable to parent.
     */
    private Integer checkExprs(List<ExprNodeDesc> ckeys, List<ExprNodeDesc> pkeys,
        ReduceSinkOperator cRS, ReduceSinkOperator pRS) throws SemanticException {
      Integer moveKeyColTo = 0;
      if (ckeys == null || ckeys.isEmpty()) {
        if (pkeys != null && !pkeys.isEmpty()) {
          moveKeyColTo = -1;
        }
      } else {
        if (pkeys == null || pkeys.isEmpty()) {
          for (ExprNodeDesc ckey : ckeys) {
            if (ExprNodeDescUtils.backtrack(ckey, cRS, pRS) == null) {
              // cKey is not present in parent
              return null;
            }
          }
          moveKeyColTo = 1;
        } else {
          moveKeyColTo = sameKeys(ckeys, pkeys, cRS, pRS);
        }
      }
      return moveKeyColTo;
    }

    // backtrack key exprs of child to parent and compare it with parent's
    protected Integer sameKeys(List<ExprNodeDesc> cexprs, List<ExprNodeDesc> pexprs,
        Operator<?> child, Operator<?> parent) throws SemanticException {
      int common = Math.min(cexprs.size(), pexprs.size());
      int limit = Math.max(cexprs.size(), pexprs.size());
      int i = 0;
      for (; i < common; i++) {
        ExprNodeDesc pexpr = pexprs.get(i);
        ExprNodeDesc cexpr = ExprNodeDescUtils.backtrack(cexprs.get(i), child, parent);
        if (cexpr == null || !pexpr.isSame(cexpr)) {
          return null;
        }
      }
      for (; i < limit; i++) {
        if (cexprs.size() > pexprs.size()) {
          if (ExprNodeDescUtils.backtrack(cexprs.get(i), child, parent) == null) {
            // cKey is not present in parent
            return null;
          }
        }
      }
      return Integer.valueOf(cexprs.size()).compareTo(pexprs.size());
    }

    // order of overlapping keys should be exactly the same
    protected Integer checkOrder(String corder, String porder) {
      if (corder == null || corder.trim().equals("")) {
        if (porder == null || porder.trim().equals("")) {
          return 0;
        }
        return -1;
      }
      if (porder == null || porder.trim().equals("")) {
        return 1;
      }
      corder = corder.trim();
      porder = porder.trim();
      int target = Math.min(corder.length(), porder.length());
      if (!corder.substring(0, target).equals(porder.substring(0, target))) {
        return null;
      }
      return Integer.valueOf(corder.length()).compareTo(porder.length());
    }

    /**
     * If number of reducers for RS is -1, the RS can have any number of reducers.
     * It's generally true except for order-by or forced bucketing cases.
     * if both of num-reducers are not -1, those number should be the same.
     */
    protected Integer checkNumReducer(int creduce, int preduce) {
      if (creduce < 0) {
        if (preduce < 0) {
          return 0;
        }
        return -1;
      }
      if (preduce < 0) {
        return 1;
      }
      if (creduce != preduce) {
        return null;
      }
      return 0;
    }
  }

  static class GroupbyReducerProc extends AbsctractReducerReducerProc {

    // pRS-pGBY-cRS
    @Override
    public Object process(ReduceSinkOperator cRS, ReduceSinkDeduplicateProcCtx dedupCtx)
        throws SemanticException {
      GroupByOperator pGBY =
          CorrelationUtilities.findPossibleParent(
              cRS, GroupByOperator.class, dedupCtx.trustScript());
      if (pGBY == null) {
        return false;
      }
      ReduceSinkOperator pRS =
          CorrelationUtilities.findPossibleParent(
              pGBY, ReduceSinkOperator.class, dedupCtx.trustScript());
      if (pRS != null && merge(cRS, pRS, dedupCtx.minReducer())) {
        CorrelationUtilities.replaceReduceSinkWithSelectOperator(
            cRS, dedupCtx.getPctx(), dedupCtx);
        return true;
      }
      return false;
    }

    // pRS-pGBY-cRS-cGBY
    @Override
    public Object process(ReduceSinkOperator cRS, GroupByOperator cGBY,
        ReduceSinkDeduplicateProcCtx dedupCtx)
        throws SemanticException {
      Operator<?> start = CorrelationUtilities.getStartForGroupBy(cRS);
      GroupByOperator pGBY =
          CorrelationUtilities.findPossibleParent(
              start, GroupByOperator.class, dedupCtx.trustScript());
      if (pGBY == null) {
        return false;
      }
      ReduceSinkOperator pRS =
          CorrelationUtilities.getSingleParent(pGBY, ReduceSinkOperator.class);
      if (pRS != null && merge(cRS, pRS, dedupCtx.minReducer())) {
        CorrelationUtilities.removeReduceSinkForGroupBy(
            cRS, cGBY, dedupCtx.getPctx(), dedupCtx);
        return true;
      }
      return false;
    }
  }

  static class JoinReducerProc extends AbsctractReducerReducerProc {

    // pRS-pJOIN-cRS
    @Override
    public Object process(ReduceSinkOperator cRS, ReduceSinkDeduplicateProcCtx dedupCtx)
        throws SemanticException {
      JoinOperator pJoin =
          CorrelationUtilities.findPossibleParent(cRS, JoinOperator.class, dedupCtx.trustScript());
      if (pJoin != null && merge(cRS, pJoin, dedupCtx.minReducer())) {
        pJoin.getConf().setFixedAsSorted(true);
        CorrelationUtilities.replaceReduceSinkWithSelectOperator(
            cRS, dedupCtx.getPctx(), dedupCtx);
        return true;
      }
      return false;
    }

    // pRS-pJOIN-cRS-cGBY
    @Override
    public Object process(ReduceSinkOperator cRS, GroupByOperator cGBY,
        ReduceSinkDeduplicateProcCtx dedupCtx)
        throws SemanticException {
      Operator<?> start = CorrelationUtilities.getStartForGroupBy(cRS);
      JoinOperator pJoin =
          CorrelationUtilities.findPossibleParent(
              start, JoinOperator.class, dedupCtx.trustScript());
      if (pJoin != null && merge(cRS, pJoin, dedupCtx.minReducer())) {
        pJoin.getConf().setFixedAsSorted(true);
        CorrelationUtilities.removeReduceSinkForGroupBy(
            cRS, cGBY, dedupCtx.getPctx(), dedupCtx);
        return true;
      }
      return false;
    }
  }

  static class ReducerReducerProc extends AbsctractReducerReducerProc {

    // pRS-cRS
    @Override
    public Object process(ReduceSinkOperator cRS, ReduceSinkDeduplicateProcCtx dedupCtx)
        throws SemanticException {
      ReduceSinkOperator pRS =
          CorrelationUtilities.findPossibleParent(
              cRS, ReduceSinkOperator.class, dedupCtx.trustScript());
      if (pRS != null && merge(cRS, pRS, dedupCtx.minReducer())) {
        CorrelationUtilities.replaceReduceSinkWithSelectOperator(
            cRS, dedupCtx.getPctx(), dedupCtx);
        return true;
      }
      return false;
    }

    // pRS-cRS-cGBY
    @Override
    public Object process(ReduceSinkOperator cRS, GroupByOperator cGBY,
        ReduceSinkDeduplicateProcCtx dedupCtx)
        throws SemanticException {
      Operator<?> start = CorrelationUtilities.getStartForGroupBy(cRS);
      ReduceSinkOperator pRS =
          CorrelationUtilities.findPossibleParent(
              start, ReduceSinkOperator.class, dedupCtx.trustScript());
      if (pRS != null && merge(cRS, pRS, dedupCtx.minReducer())) {
        CorrelationUtilities.removeReduceSinkForGroupBy(cRS, cGBY, dedupCtx.getPctx(), dedupCtx);
        return true;
      }
      return false;
    }
  }
}
