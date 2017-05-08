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
import java.util.Map.Entry;
import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.PTFOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
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
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * If two reducer sink operators share the same partition/sort columns and order,
 * they can be merged. This should happen after map join optimization because map
 * join optimization will remove reduce sink operators.
 *
 * This optimizer removes/replaces child-RS (not parent) which is safer way for DefaultGraphWalker.
 */
public class ReduceSinkDeDuplication extends Transform {

  protected static final Logger LOG = LoggerFactory.getLogger(ReduceSinkDeDuplication.class);

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
        !pctx.getConf().getBoolVar(HIVECONVERTJOINNOCONDITIONALTASK) &&
        !pctx.getConf().getBoolVar(ConfVars.HIVE_CONVERT_JOIN_BUCKET_MAPJOIN_TEZ) &&
        !pctx.getConf().getBoolVar(ConfVars.HIVEDYNAMICPARTITIONHASHJOIN);

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

    @Override
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
      if (child instanceof SelectOperator) {
        return process(cRS, dedupCtx);
      }
      return false;
    }

    protected abstract Object process(ReduceSinkOperator cRS, ReduceSinkDeduplicateProcCtx dedupCtx)
        throws SemanticException;

    protected abstract Object process(ReduceSinkOperator cRS, GroupByOperator cGBY,
        ReduceSinkDeduplicateProcCtx dedupCtx) throws SemanticException;

    // for JOIN-RS case, it's not possible generally to merge if child has
    // less key/partition columns than parents
    protected boolean merge(ReduceSinkOperator cRS, JoinOperator pJoin, int minReducer)
        throws SemanticException {
      List<Operator<?>> parents = pJoin.getParentOperators();
      ReduceSinkOperator[] pRSs = parents.toArray(new ReduceSinkOperator[parents.size()]);
      ReduceSinkDesc cRSc = cRS.getConf();
      for (ReduceSinkOperator pRSNs : pRSs) {
        ReduceSinkDesc pRSNc = pRSNs.getConf();
        if (cRSc.getKeyCols().size() < pRSNc.getKeyCols().size()) {
          return false;
        }
        if (cRSc.getPartitionCols().size() != pRSNc.getPartitionCols().size()) {
          return false;
        }
        Integer moveReducerNumTo = checkNumReducer(cRSc.getNumReducers(), pRSNc.getNumReducers());
        if (moveReducerNumTo == null ||
            moveReducerNumTo > 0 && cRSc.getNumReducers() < minReducer) {
          return false;
        }

        Integer moveRSOrderTo = checkOrder(true, cRSc.getOrder(), pRSNc.getOrder(),
                cRSc.getNullOrder(), pRSNc.getNullOrder());
        if (moveRSOrderTo == null) {
          return false;
        }
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
        if (found != i) {
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
        if (found != i) {
          return false;
        }
      }

      for (ReduceSinkOperator pRS : pRSs) {
        pRS.getConf().setNumReducers(cRS.getConf().getNumReducers());
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
      int[] result = extractMergeDirections(cRS, pRS, minReducer);
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
        pRS.getConf().setNullOrder(cRS.getConf().getNullOrder());
      } else {
        // The sorting order of the parent RS is more specific or they are equal.
        // We will copy the order from the child RS, and then fill in the order
        // of the rest of columns with the one taken from parent RS.
        StringBuilder order = new StringBuilder(cRS.getConf().getOrder());
        StringBuilder orderNull = new StringBuilder(cRS.getConf().getNullOrder());
        order.append(pRS.getConf().getOrder().substring(order.length()));
        orderNull.append(pRS.getConf().getNullOrder().substring(orderNull.length()));
        pRS.getConf().setOrder(order.toString());
        pRS.getConf().setNullOrder(orderNull.toString());
      }

      if (result[3] > 0) {
        // The number of reducers of the child RS is more specific than
        // that of the parent RS. Assign the number of reducers of the child RS
        // to the parent RS.
        pRS.getConf().setNumReducers(cRS.getConf().getNumReducers());
      }

      if (result[4] > 0) {
        // This case happens only when pRS key is empty in which case we can use
        // number of distribution keys and key serialization info from cRS
        if (pRS.getConf().getKeyCols() != null && pRS.getConf().getKeyCols().size() == 0
            && cRS.getConf().getKeyCols() != null && cRS.getConf().getKeyCols().size() == 0) {
          // As setNumDistributionKeys is a subset of keycols, the size should
          // be 0 too. This condition maybe too strict. We may extend it in the
          // future.
          TableDesc keyTable = PlanUtils.getReduceKeyTableDesc(new ArrayList<FieldSchema>(), pRS
              .getConf().getOrder(), pRS.getConf().getNullOrder());
          pRS.getConf().setKeySerializeInfo(keyTable);
        }
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
    private int[] extractMergeDirections(ReduceSinkOperator cRS, ReduceSinkOperator pRS, int minReducer)
        throws SemanticException {
      ReduceSinkDesc cConf = cRS.getConf();
      ReduceSinkDesc pConf = pRS.getConf();
      // If there is a PTF between cRS and pRS we cannot ignore the order direction
      final boolean checkStrictEquality = isStrictEqualityNeeded(cRS, pRS);
      Integer moveRSOrderTo = checkOrder(checkStrictEquality, cConf.getOrder(), pConf.getOrder(),
              cConf.getNullOrder(), pConf.getNullOrder());
      if (moveRSOrderTo == null) {
        return null;
      }
      // if cRS is being used for distinct - the two reduce sinks are incompatible
      if (cConf.getDistinctColumnIndices().size() >= 2) {
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
      Integer moveNumDistKeyTo = checkNumDistributionKey(cConf.getNumDistributionKeys(),
          pConf.getNumDistributionKeys());
      return new int[] {moveKeyColTo, movePartitionColTo, moveRSOrderTo,
          moveReducerNumTo, moveNumDistKeyTo};
    }

    private boolean isStrictEqualityNeeded(ReduceSinkOperator cRS, ReduceSinkOperator pRS) {
      Operator<? extends OperatorDesc> parent = cRS.getParentOperators().get(0);
      while (parent != pRS) {
        assert parent.getNumParent() == 1;
        if (parent instanceof PTFOperator) {
          return true;
        }
        parent = parent.getParentOperators().get(0);
      }
      return false;
    }

    private Integer checkNumDistributionKey(int cnd, int pnd) {
      // number of distribution keys of cRS is chosen only when numDistKeys of pRS
      // is 0 or less. In all other cases, distribution of the keys is based on
      // the pRS which is more generic than cRS.
      // Examples:
      // case 1: if pRS sort key is (a, b) and cRS sort key is (a, b, c) and number of
      // distribution keys are 2 and 3 resp. then after merge the sort keys will
      // be (a, b, c) while the number of distribution keys will be 2.
      // case 2: if pRS sort key is empty and number of distribution keys is 0
      // and if cRS sort key is (a, b) and number of distribution keys is 2 then
      // after merge new sort key will be (a, b) and number of distribution keys
      // will be 2.
      if (pnd <= 0) {
        return 1;
      }
      return 0;
    }

    /**
     * Overlapping part of keys should be the same between parent and child.
     * And if child has more keys than parent, non-overlapping part of keys
     * should be backtrackable to parent.
     */
    private Integer checkExprs(List<ExprNodeDesc> ckeys, List<ExprNodeDesc> pkeys,
        ReduceSinkOperator cRS, ReduceSinkOperator pRS) throws SemanticException {
      // If ckeys or pkeys have constant node expressions avoid the merge.
      for (ExprNodeDesc ck : ckeys) {
        if (ck instanceof ExprNodeConstantDesc) {
          return null;
        }
      }
      for (ExprNodeDesc pk : pkeys) {
        if (pk instanceof ExprNodeConstantDesc) {
          return null;
        }
      }

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

    protected Integer checkOrder(boolean checkStrictEquality, String corder, String porder,
            String cNullOrder, String pNullOrder) {
      assert corder.length() == cNullOrder.length();
      assert porder.length() == pNullOrder.length();
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
      if (checkStrictEquality) {
        // order of overlapping keys should be exactly the same
        cNullOrder = cNullOrder.trim();
        pNullOrder = pNullOrder.trim();
        int target = Math.min(corder.length(), porder.length());
        if (!corder.substring(0, target).equals(porder.substring(0, target)) ||
                !cNullOrder.substring(0, target).equals(pNullOrder.substring(0, target))) {
          return null;
        }
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

    protected boolean aggressiveDedup(ReduceSinkOperator cRS, ReduceSinkOperator pRS,
            ReduceSinkDeduplicateProcCtx dedupCtx) throws SemanticException {
      assert cRS.getNumParent() == 1;

      ReduceSinkDesc cConf = cRS.getConf();
      ReduceSinkDesc pConf = pRS.getConf();
      List<ExprNodeDesc> cKeys = cConf.getKeyCols();
      List<ExprNodeDesc> pKeys = pConf.getKeyCols();

      // Check that in the path between cRS and pRS, there are only Select operators
      // i.e. the sequence must be pRS-SEL*-cRS
      Operator<? extends OperatorDesc> parent = cRS.getParentOperators().get(0);
      while (parent != pRS) {
        assert parent.getNumParent() == 1;
        if (!(parent instanceof SelectOperator)) {
          return false;
        }
        parent = parent.getParentOperators().get(0);
      }

      // If child keys are null or empty, we bail out
      if (cKeys == null || cKeys.isEmpty()) {
        return false;
      }
      // If parent keys are null or empty, we bail out
      if (pKeys == null || pKeys.isEmpty()) {
        return false;
      }

      // Backtrack key columns of cRS to pRS
      // If we cannot backtrack any of the columns, bail out
      List<ExprNodeDesc> cKeysInParentRS = ExprNodeDescUtils.backtrack(cKeys, cRS, pRS);
      for (int i = 0; i < cKeysInParentRS.size(); i++) {
        ExprNodeDesc pexpr = cKeysInParentRS.get(i);
        if (pexpr == null) {
          // We cannot backtrack the expression, we bail out
          return false;
        }
      }
      cRS.getConf().setKeyCols(ExprNodeDescUtils.backtrack(cKeysInParentRS, cRS, pRS));

      // Backtrack partition columns of cRS to pRS
      // If we cannot backtrack any of the columns, bail out
      List<ExprNodeDesc> cPartitionInParentRS = ExprNodeDescUtils.backtrack(
              cConf.getPartitionCols(), cRS, pRS);
      for (int i = 0; i < cPartitionInParentRS.size(); i++) {
        ExprNodeDesc pexpr = cPartitionInParentRS.get(i);
        if (pexpr == null) {
          // We cannot backtrack the expression, we bail out
          return false;
        }
      }
      cRS.getConf().setPartitionCols(ExprNodeDescUtils.backtrack(cPartitionInParentRS, cRS, pRS));

      // Backtrack value columns of cRS to pRS
      // If we cannot backtrack any of the columns, bail out
      List<ExprNodeDesc> cValueInParentRS = ExprNodeDescUtils.backtrack(
              cConf.getValueCols(), cRS, pRS);
      for (int i = 0; i < cValueInParentRS.size(); i++) {
        ExprNodeDesc pexpr = cValueInParentRS.get(i);
        if (pexpr == null) {
          // We cannot backtrack the expression, we bail out
          return false;
        }
      }
      cRS.getConf().setValueCols(ExprNodeDescUtils.backtrack(cValueInParentRS, cRS, pRS));

      // Backtrack bucket columns of cRS to pRS (if any)
      // If we cannot backtrack any of the columns, bail out
      if (cConf.getBucketCols() != null) {
        List<ExprNodeDesc> cBucketInParentRS = ExprNodeDescUtils.backtrack(
                cConf.getBucketCols(), cRS, pRS);
        for (int i = 0; i < cBucketInParentRS.size(); i++) {
          ExprNodeDesc pexpr = cBucketInParentRS.get(i);
          if (pexpr == null) {
            // We cannot backtrack the expression, we bail out
            return false;
          }
        }
        cRS.getConf().setBucketCols(ExprNodeDescUtils.backtrack(cBucketInParentRS, cRS, pRS));
      }

      // Update column expression map
      for (Entry<String, ExprNodeDesc> e : cRS.getColumnExprMap().entrySet()) {
        e.setValue(ExprNodeDescUtils.backtrack(e.getValue(), cRS, pRS));
      }

      // Replace pRS with cRS and remove operator sequence from pRS to cRS
      // Recall that the sequence must be pRS-SEL*-cRS
      parent = cRS.getParentOperators().get(0);
      while (parent != pRS) {
        dedupCtx.addRemovedOperator(parent);
        parent = parent.getParentOperators().get(0);
      }
      dedupCtx.addRemovedOperator(pRS);
      cRS.getParentOperators().clear();
      for (Operator<? extends OperatorDesc> op : pRS.getParentOperators()) {
        op.replaceChild(pRS, cRS);
        cRS.getParentOperators().add(op);
      }
      pRS.getParentOperators().clear();
      pRS.getChildOperators().clear();

      return true;
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
        pRS.getConf().setDeduplicated(true);
        return true;
      }
      return false;
    }

    // pRS-pGBY-cRS-cGBY
    @Override
    public Object process(ReduceSinkOperator cRS, GroupByOperator cGBY,
        ReduceSinkDeduplicateProcCtx dedupCtx)
        throws SemanticException {
      Operator<?> start = CorrelationUtilities.getStartForGroupBy(cRS, dedupCtx);
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
        pRS.getConf().setDeduplicated(true);
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
        ReduceSinkOperator pRS =
            CorrelationUtilities.findPossibleParent(
                pJoin, ReduceSinkOperator.class, dedupCtx.trustScript());
        if (pRS != null) {
          pRS.getConf().setDeduplicated(true);
        }
        return true;
      }
      return false;
    }

    // pRS-pJOIN-cRS-cGBY
    @Override
    public Object process(ReduceSinkOperator cRS, GroupByOperator cGBY,
        ReduceSinkDeduplicateProcCtx dedupCtx)
        throws SemanticException {
      Operator<?> start = CorrelationUtilities.getStartForGroupBy(cRS, dedupCtx);
      JoinOperator pJoin =
          CorrelationUtilities.findPossibleParent(
              start, JoinOperator.class, dedupCtx.trustScript());
      if (pJoin != null && merge(cRS, pJoin, dedupCtx.minReducer())) {
        pJoin.getConf().setFixedAsSorted(true);
        CorrelationUtilities.removeReduceSinkForGroupBy(
            cRS, cGBY, dedupCtx.getPctx(), dedupCtx);
        ReduceSinkOperator pRS =
            CorrelationUtilities.findPossibleParent(
                pJoin, ReduceSinkOperator.class, dedupCtx.trustScript());
        if (pRS != null) {
          pRS.getConf().setDeduplicated(true);
        }
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
      if (pRS != null) {
        // Try extended deduplication
        if (aggressiveDedup(cRS, pRS, dedupCtx)) {
          return true;
        }
        // Normal deduplication
        if (merge(cRS, pRS, dedupCtx.minReducer())) {
          CorrelationUtilities.replaceReduceSinkWithSelectOperator(
              cRS, dedupCtx.getPctx(), dedupCtx);
          pRS.getConf().setDeduplicated(true);
          return true;
        }
      }
      return false;
    }

    // pRS-cRS-cGBY
    @Override
    public Object process(ReduceSinkOperator cRS, GroupByOperator cGBY,
        ReduceSinkDeduplicateProcCtx dedupCtx)
        throws SemanticException {
      Operator<?> start = CorrelationUtilities.getStartForGroupBy(cRS, dedupCtx);
      ReduceSinkOperator pRS =
          CorrelationUtilities.findPossibleParent(
              start, ReduceSinkOperator.class, dedupCtx.trustScript());
      if (pRS != null && merge(cRS, pRS, dedupCtx.minReducer())) {
        if (dedupCtx.getPctx().getConf().getBoolVar(HiveConf.ConfVars.HIVEGROUPBYSKEW)) {
          return false;
        }
        CorrelationUtilities.removeReduceSinkForGroupBy(cRS, cGBY, dedupCtx.getPctx(), dedupCtx);
        pRS.getConf().setDeduplicated(true);
        return true;
      }
      return false;
    }
  }
}
