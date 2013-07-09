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

package org.apache.hadoop.hive.ql.optimizer;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.ExtractOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.ForwardOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
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
import org.apache.hadoop.hive.ql.parse.OpParseContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVECONVERTJOIN;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVECONVERTJOINNOCONDITIONALTASK;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVEOPTREDUCEDEDUPLICATIONMINREDUCER;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVESCRIPTOPERATORTRUST;

/**
 * If two reducer sink operators share the same partition/sort columns and order,
 * they can be merged. This should happen after map join optimization because map
 * join optimization will remove reduce sink operators.
 *
 * This optimizer removes/replaces child-RS (not parent) which is safer way for DefaultGraphWalker.
 */
public class ReduceSinkDeDuplication implements Transform{

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

  class ReduceSinkDeduplicateProcCtx implements NodeProcessorCtx {

    ParseContext pctx;

    // For queries using script, the optimization cannot be applied without user's confirmation
    // If script preserves alias and value for columns related to keys, user can set this true
    boolean trustScript;

    // This is min number of reducer for deduped RS to avoid query executed on
    // too small number of reducers. For example, queries GroupBy+OrderBy can be executed by
    // only one reducer if this configuration does not prevents
    int minReducer;
    Set<Operator<?>> removedOps;

    public ReduceSinkDeduplicateProcCtx(ParseContext pctx) {
      removedOps = new HashSet<Operator<?>>();
      trustScript = pctx.getConf().getBoolVar(HIVESCRIPTOPERATORTRUST);
      minReducer = pctx.getConf().getIntVar(HIVEOPTREDUCEDEDUPLICATIONMINREDUCER);
      this.pctx = pctx;
    }

    public boolean contains(Operator<?> rsOp) {
      return removedOps.contains(rsOp);
    }

    public boolean addRemovedOperator(Operator<?> rsOp) {
      return removedOps.add(rsOp);
    }

    public ParseContext getPctx() {
      return pctx;
    }

    public void setPctx(ParseContext pctx) {
      this.pctx = pctx;
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

  public abstract static class AbstractReducerReducerProc implements NodeProcessor {

    ReduceSinkDeduplicateProcCtx dedupCtx;

    protected boolean trustScript() {
      return dedupCtx.trustScript;
    }

    protected int minReducer() {
      return dedupCtx.minReducer;
    }

    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      dedupCtx = (ReduceSinkDeduplicateProcCtx) procCtx;
      if (dedupCtx.contains((Operator<?>) nd)) {
        return false;
      }
      ReduceSinkOperator cRS = (ReduceSinkOperator) nd;
      Operator<?> child = getSingleChild(cRS);
      if (child instanceof JoinOperator) {
        return false; // not supported
      }
      ParseContext pctx = dedupCtx.getPctx();
      if (child instanceof GroupByOperator) {
        GroupByOperator cGBY = (GroupByOperator) child;
        if (!hasGroupingSet(cRS) && !cGBY.getConf().isGroupingSetsPresent()) {
          return process(cRS, cGBY, pctx);
        }
        return false;
      }
      if (child instanceof ExtractOperator) {
        return process(cRS, pctx);
      }
      return false;
    }

    private boolean hasGroupingSet(ReduceSinkOperator cRS) {
      GroupByOperator cGBYm = getSingleParent(cRS, GroupByOperator.class);
      if (cGBYm != null && cGBYm.getConf().isGroupingSetsPresent()) {
        return true;
      }
      return false;
    }

    protected Operator<?> getSingleParent(Operator<?> operator) {
      List<Operator<?>> parents = operator.getParentOperators();
      if (parents != null && parents.size() == 1) {
        return parents.get(0);
      }
      return null;
    }

    protected Operator<?> getSingleChild(Operator<?> operator) {
      List<Operator<?>> children = operator.getChildOperators();
      if (children != null && children.size() == 1) {
        return children.get(0);
      }
      return null;
    }

    protected <T> T getSingleParent(Operator<?> operator, Class<T> type) {
      Operator<?> parent = getSingleParent(operator);
      return type.isInstance(parent) ? (T)parent : null;
    }

    protected abstract Object process(ReduceSinkOperator cRS, ParseContext context)
        throws SemanticException;

    protected abstract Object process(ReduceSinkOperator cRS, GroupByOperator cGBY,
        ParseContext context) throws SemanticException;

    protected Operator<?> getStartForGroupBy(ReduceSinkOperator cRS) {
      Operator<? extends Serializable> parent = getSingleParent(cRS);
      return parent instanceof GroupByOperator ? parent : cRS;  // skip map-aggr GBY
    }

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

      boolean[] sorted = getSortedTags(pJoin);

      int cKeySize = cRSc.getKeyCols().size();
      for (int i = 0; i < cKeySize; i++) {
        ExprNodeDesc cexpr = cRSc.getKeyCols().get(i);
        ExprNodeDesc[] pexprs = new ExprNodeDesc[pRSs.length];
        for (int tag = 0; tag < pRSs.length; tag++) {
          pexprs[tag] = pRSs[tag].getConf().getKeyCols().get(i);
        }
        int found = indexOf(cexpr, pexprs, cRS, pRSs, sorted);
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
        int found = indexOf(cexpr, pexprs, cRS, pRSs, sorted);
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

    private boolean[] getSortedTags(JoinOperator joinOp) {
      boolean[] result = new boolean[joinOp.getParentOperators().size()];
      for (int tag = 0; tag < result.length; tag++) {
        result[tag] = isSortedTag(joinOp, tag);
      }
      return result;
    }

    // for left outer joins, left alias is sorted but right alias might be not
    // (nulls, etc.). vice versa.
    private boolean isSortedTag(JoinOperator joinOp, int tag) {
      for (JoinCondDesc cond : joinOp.getConf().getConds()) {
        switch (cond.getType()) {
          case JoinDesc.LEFT_OUTER_JOIN:
            if (cond.getRight() == tag) {
              return false;
            }
            continue;
          case JoinDesc.RIGHT_OUTER_JOIN:
            if (cond.getLeft() == tag) {
              return false;
            }
            continue;
          case JoinDesc.FULL_OUTER_JOIN:
            if (cond.getLeft() == tag || cond.getRight() == tag) {
              return false;
            }
        }
      }
      return true;
    }

    private int indexOf(ExprNodeDesc cexpr, ExprNodeDesc[] pexprs, Operator child,
        Operator[] parents, boolean[] sorted) throws SemanticException {
      for (int tag = 0; tag < parents.length; tag++) {
        if (sorted[tag] &&
            pexprs[tag].isSame(ExprNodeDescUtils.backtrack(cexpr, child, parents[tag]))) {
          return tag;
        }
      }
      return -1;
    }

    /**
     * Current RSDedup remove/replace child RS. So always copies
     * more specific part of configurations of child RS to that of parent RS.
     */
    protected boolean merge(ReduceSinkOperator cRS, ReduceSinkOperator pRS, int minReducer)
        throws SemanticException {
      int[] result = checkStatus(cRS, pRS, minReducer);
      if (result == null) {
        return false;
      }
      if (result[0] > 0) {
        ArrayList<ExprNodeDesc> childKCs = cRS.getConf().getKeyCols();
        pRS.getConf().setKeyCols(ExprNodeDescUtils.backtrack(childKCs, cRS, pRS));
      }
      if (result[1] > 0) {
        ArrayList<ExprNodeDesc> childPCs = cRS.getConf().getPartitionCols();
        pRS.getConf().setPartitionCols(ExprNodeDescUtils.backtrack(childPCs, cRS, pRS));
      }
      if (result[2] > 0) {
        pRS.getConf().setOrder(cRS.getConf().getOrder());
      }
      if (result[3] > 0) {
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
      for (;i < limit; i++) {
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

    protected <T extends Operator<?>> T findPossibleParent(Operator<?> start, Class<T> target,
        boolean trustScript) throws SemanticException {
      T[] parents = findPossibleParents(start, target, trustScript);
      return parents != null && parents.length == 1 ? parents[0] : null;
    }

    @SuppressWarnings("unchecked")
    protected <T extends Operator<?>> T[] findPossibleParents(Operator<?> start, Class<T> target,
        boolean trustScript) {
      Operator<?> cursor = getSingleParent(start);
      for (; cursor != null; cursor = getSingleParent(cursor)) {
        if (target.isAssignableFrom(cursor.getClass())) {
          T[] array = (T[]) Array.newInstance(target, 1);
          array[0] = (T) cursor;
          return array;
        }
        if (cursor instanceof JoinOperator) {
          return findParents((JoinOperator) cursor, target);
        }
        if (cursor instanceof ScriptOperator && !trustScript) {
          return null;
        }
        if (!(cursor instanceof SelectOperator
            || cursor instanceof FilterOperator
            || cursor instanceof ExtractOperator
            || cursor instanceof ForwardOperator
            || cursor instanceof ScriptOperator
            || cursor instanceof ReduceSinkOperator)) {
          return null;
        }
      }
      return null;
    }

    @SuppressWarnings("unchecked")
    private <T extends Operator<?>> T[] findParents(JoinOperator join, Class<T> target) {
      List<Operator<?>> parents = join.getParentOperators();
      T[] result = (T[]) Array.newInstance(target, parents.size());
      for (int tag = 0; tag < result.length; tag++) {
        Operator<?> cursor = parents.get(tag);
        for (; cursor != null; cursor = getSingleParent(cursor)) {
          if (target.isAssignableFrom(cursor.getClass())) {
            result[tag] = (T) cursor;
            break;
          }
        }
        if (result[tag] == null) {
          throw new IllegalStateException("failed to find " + target.getSimpleName()
              + " from " + join + " on tag " + tag);
        }
      }
      return result;
    }

    protected SelectOperator replaceReduceSinkWithSelectOperator(ReduceSinkOperator childRS,
        ParseContext context) throws SemanticException {
      SelectOperator select = replaceOperatorWithSelect(childRS, context);
      select.getConf().setOutputColumnNames(childRS.getConf().getOutputValueColumnNames());
      select.getConf().setColList(childRS.getConf().getValueCols());
      return select;
    }

    // replace the cRS to SEL operator
    // If child if cRS is EXT, EXT also should be removed
    private SelectOperator replaceOperatorWithSelect(Operator<?> operator, ParseContext context)
        throws SemanticException {
      RowResolver inputRR = context.getOpParseCtx().get(operator).getRowResolver();
      SelectDesc select = new SelectDesc(null, null);

      Operator<?> parent = getSingleParent(operator);
      Operator<?> child = getSingleChild(operator);

      parent.getChildOperators().clear();

      SelectOperator sel = (SelectOperator) putOpInsertMap(
          OperatorFactory.getAndMakeChild(select, new RowSchema(inputRR
              .getColumnInfos()), parent), inputRR, context);

      sel.setColumnExprMap(operator.getColumnExprMap());

      sel.setChildOperators(operator.getChildOperators());
      for (Operator<? extends Serializable> ch : operator.getChildOperators()) {
        ch.replaceParent(operator, sel);
      }
      if (child instanceof ExtractOperator) {
        removeOperator(child, getSingleChild(child), sel, context);
        dedupCtx.addRemovedOperator(child);
      }
      operator.setChildOperators(null);
      operator.setParentOperators(null);
      dedupCtx.addRemovedOperator(operator);
      return sel;
    }

    protected void removeReduceSinkForGroupBy(ReduceSinkOperator cRS, GroupByOperator cGBYr,
        ParseContext context) throws SemanticException {

      Operator<?> parent = getSingleParent(cRS);

      if (parent instanceof GroupByOperator) {
        // pRS-cGBYm-cRS-cGBYr (map aggregation) --> pRS-cGBYr(COMPLETE)
        // copies desc of cGBYm to cGBYr and remove cGBYm and cRS
        GroupByOperator cGBYm = (GroupByOperator) parent;

        cGBYr.getConf().setKeys(cGBYm.getConf().getKeys());
        cGBYr.getConf().setAggregators(cGBYm.getConf().getAggregators());
        for (AggregationDesc aggr : cGBYm.getConf().getAggregators()) {
          aggr.setMode(GenericUDAFEvaluator.Mode.COMPLETE);
        }
        cGBYr.setColumnExprMap(cGBYm.getColumnExprMap());
        cGBYr.setSchema(cGBYm.getSchema());
        RowResolver resolver = context.getOpParseCtx().get(cGBYm).getRowResolver();
        context.getOpParseCtx().get(cGBYr).setRowResolver(resolver);
      } else {
        // pRS-cRS-cGBYr (no map aggregation) --> pRS-cGBYr(COMPLETE)
        // revert expressions of cGBYr to that of cRS
        cGBYr.getConf().setKeys(ExprNodeDescUtils.backtrack(cGBYr.getConf().getKeys(), cGBYr, cRS));
        for (AggregationDesc aggr : cGBYr.getConf().getAggregators()) {
          aggr.setParameters(ExprNodeDescUtils.backtrack(aggr.getParameters(), cGBYr, cRS));
        }

        Map<String, ExprNodeDesc> oldMap = cGBYr.getColumnExprMap();
        RowResolver oldRR = context.getOpParseCtx().get(cGBYr).getRowResolver();

        Map<String, ExprNodeDesc> newMap = new HashMap<String, ExprNodeDesc>();
        RowResolver newRR = new RowResolver();

        List<String> outputCols = cGBYr.getConf().getOutputColumnNames();
        for (int i = 0; i < outputCols.size(); i++) {
          String colName = outputCols.get(i);
          String[] nm = oldRR.reverseLookup(colName);
          ColumnInfo colInfo = oldRR.get(nm[0], nm[1]);
          newRR.put(nm[0], nm[1], colInfo);
          ExprNodeDesc colExpr = ExprNodeDescUtils.backtrack(oldMap.get(colName), cGBYr, cRS);
          if (colExpr != null) {
            newMap.put(colInfo.getInternalName(), colExpr);
          }
        }
        cGBYr.setColumnExprMap(newMap);
        cGBYr.setSchema(new RowSchema(newRR.getColumnInfos()));
        context.getOpParseCtx().get(cGBYr).setRowResolver(newRR);
      }
      cGBYr.getConf().setMode(GroupByDesc.Mode.COMPLETE);

      removeOperator(cRS, cGBYr, parent, context);
      dedupCtx.addRemovedOperator(cRS);

      if (parent instanceof GroupByOperator) {
        removeOperator(parent, cGBYr, getSingleParent(parent), context);
        dedupCtx.addRemovedOperator(cGBYr);
      }
    }

    private void removeOperator(Operator<?> target, Operator<?> child, Operator<?> parent,
        ParseContext context) {
      for (Operator<?> aparent : target.getParentOperators()) {
        aparent.replaceChild(target, child);
      }
      for (Operator<?> achild : target.getChildOperators()) {
        achild.replaceParent(target, parent);
      }
      target.setChildOperators(null);
      target.setParentOperators(null);
      context.getOpParseCtx().remove(target);
    }

    private Operator<? extends Serializable> putOpInsertMap(Operator<?> op, RowResolver rr,
        ParseContext context) {
      OpParseContext ctx = new OpParseContext(rr);
      context.getOpParseCtx().put(op, ctx);
      return op;
    }
  }

  static class GroupbyReducerProc extends AbstractReducerReducerProc {

    // pRS-pGBY-cRS
    public Object process(ReduceSinkOperator cRS, ParseContext context)
        throws SemanticException {
      GroupByOperator pGBY = findPossibleParent(cRS, GroupByOperator.class, trustScript());
      if (pGBY == null) {
        return false;
      }
      ReduceSinkOperator pRS = findPossibleParent(pGBY, ReduceSinkOperator.class, trustScript());
      if (pRS != null && merge(cRS, pRS, minReducer())) {
        replaceReduceSinkWithSelectOperator(cRS, context);
        return true;
      }
      return false;
    }

    // pRS-pGBY-cRS-cGBY
    public Object process(ReduceSinkOperator cRS, GroupByOperator cGBY, ParseContext context)
        throws SemanticException {
      Operator<?> start = getStartForGroupBy(cRS);
      GroupByOperator pGBY = findPossibleParent(start, GroupByOperator.class, trustScript());
      if (pGBY == null) {
        return false;
      }
      ReduceSinkOperator pRS = getSingleParent(pGBY, ReduceSinkOperator.class);
      if (pRS != null && merge(cRS, pRS, minReducer())) {
        removeReduceSinkForGroupBy(cRS, cGBY, context);
        return true;
      }
      return false;
    }
  }

  static class JoinReducerProc extends AbstractReducerReducerProc {

    // pRS-pJOIN-cRS
    public Object process(ReduceSinkOperator cRS, ParseContext context)
        throws SemanticException {
      JoinOperator pJoin = findPossibleParent(cRS, JoinOperator.class, trustScript());
      if (pJoin != null && merge(cRS, pJoin, minReducer())) {
        pJoin.getConf().setFixedAsSorted(true);
        replaceReduceSinkWithSelectOperator(cRS, context);
        return true;
      }
      return false;
    }

    // pRS-pJOIN-cRS-cGBY
    public Object process(ReduceSinkOperator cRS, GroupByOperator cGBY, ParseContext context)
        throws SemanticException {
      Operator<?> start = getStartForGroupBy(cRS);
      JoinOperator pJoin = findPossibleParent(start, JoinOperator.class, trustScript());
      if (pJoin != null && merge(cRS, pJoin, minReducer())) {
        pJoin.getConf().setFixedAsSorted(true);
        removeReduceSinkForGroupBy(cRS, cGBY, context);
        return true;
      }
      return false;
    }
  }

  static class ReducerReducerProc extends AbstractReducerReducerProc {

    // pRS-cRS
    public Object process(ReduceSinkOperator cRS, ParseContext context)
        throws SemanticException {
      ReduceSinkOperator pRS = findPossibleParent(cRS, ReduceSinkOperator.class, trustScript());
      if (pRS != null && merge(cRS, pRS, minReducer())) {
        replaceReduceSinkWithSelectOperator(cRS, context);
        return true;
      }
      return false;
    }

    // pRS-cRS-cGBY
    public Object process(ReduceSinkOperator cRS, GroupByOperator cGBY, ParseContext context)
        throws SemanticException {
      Operator<?> start = getStartForGroupBy(cRS);
      ReduceSinkOperator pRS = findPossibleParent(start, ReduceSinkOperator.class, trustScript());
      if (pRS != null && merge(cRS, pRS, minReducer())) {
        removeReduceSinkForGroupBy(cRS, cGBY, context);
        return true;
      }
      return false;
    }
  }
}
