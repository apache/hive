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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.GenericUDAFInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.ExprNodeTypeCheck;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicListDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIf;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPMinus;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPMultiply;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPPlus;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFWhen;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pushes a map-side (hash) GROUP BY below an inner join, to the join input that holds every aggregation
 * argument. The pushed GROUP BY runs in {@link GroupByDesc.Mode#PARTIAL_HASH} mode in the vertex of that input,
 * before its existing shuffle (or map join). The matched GROUP BY stays above the join with the same mode and
 * output, but its aggregations merge the partial results ({@code PARTIAL2}) instead of iterating over rows. The
 * reduce-side GROUP BY that computes the final aggregate is unchanged.
 * <pre>
 *   TS - SEL - RS \
 *                  JOIN - SEL - GBY(hash) - RS - GBY(mergepartial)
 *   TS - SEL - RS /
 *
 *   =>
 *
 *   TS - SEL - GBY(partialhash) - RS \
 *                                     JOIN - GBY(hash) - RS - GBY(mergepartial)
 *   TS - SEL ------------------ RS /
 * </pre>
 * <p>Criteria must meet
 * <ul>
 *   <li>The join is a common join when this optimization applied (a {@link JoinOperator}, every input a ReduceSink).
 *   The {@link JoinOperator} must be Inner Join. If the {@link JoinOperator} is an n-way join all entry in the
 *   condition map has to be Inner Join.
 *   <li>the GROUP BY<ul>
 *     <li>is over the join through Select operators only;</li>
 *     <li>it has keys,</li>
 *     <li>no grouping sets</li>
 *     <li>no distinct aggregation,</li>
 *     <li>only SUM, $SUM0, COUNT, MIN and MAX aggregations: their partial result is a plain
 *         value, merged by SUM (counts) or the function itself</li></ul>
 *   </li>
 *   <li>every aggregate key is deterministic and references one join input at most</li>
 *   <li>every aggregation argument (examples over {@code store_sales JOIN date_dim})
 *   <ul>
 *     <li>is deterministic and references one join input at most. For example, in {@code sum(ss_sales_price)} the
 *     argument references store_sales: the input the GROUP BY is pushed to.</li>
 *     <li>or it is an {@code IF(condition, value, else)} whose condition references one join input and whose value
 *     the other. For example, in {@code sum(if(d_day_name = 'Sunday', ss_sales_price, null))}:
 *     <ul>
 *       <li>the condition is {@code d_day_name = 'Sunday'}, it references date_dim;</li>
 *       <li>the value is {@code ss_sales_price}, it references store_sales: the input the GROUP BY is pushed to;</li>
 *       <li>the else is {@code null}.</li>
 *     </ul>
 *     The value is aggregated below the join, {@code sum(ss_sales_price)}, and the condition is applied to the
 *     partial result above the join: {@code sum(if(d_day_name = 'Sunday', partial, null))};</li>
 *     <li>or it is a {@code CASE WHEN condition THEN value [ELSE else] END} with the same condition and value as
 *     the IF; a missing ELSE is NULL. For example,
 *     {@code sum(case when d_day_name = 'Sunday' then ss_sales_price else 0 end)} is aggregated below the join as
 *     {@code sum(ss_sales_price)}, and above the join as {@code sum(if(d_day_name = 'Sunday', partial, 0))}.</li>
 *   </ul>
 *   Splitting an IF or CASE is exact because every row of a pushed group has the same join key, so it joins the
 *   same rows of the other input: for each of those rows, the condition is true or false for the whole group. The
 *   value is aggregated over the rows whose condition is false too, so it must not fail on them: it is built from
 *   columns, constants, +, -, * and casts only. The else is NULL, or 0 for a SUM;</li>
 *   <li>every aggregation argument (the value, for a split IF or CASE) references the same join input, and the
 *   GROUP BY is pushed to it; otherwise the push is skipped.<br>
 *   Example: {@code sum(ss_sales_price), max(d_moy)} over {@code store_sales JOIN date_dim} is not pushed.<br>
 *   An argument without columns does not choose an input; if no argument has columns, the GROUP BY is pushed to
 *   the larger input.<br>
 *   Example: {@code count(*)}, {@code count(1)};</li>
 *   <li>the pushed aggregate's keys are going to be the original keys available on that input plus
 *   the join keys from that side and its columns used by residual join filters (non-equijoin filters)</li>
 *   <li>the push is skipped when the join expands its input by more than
 *   {@code hive.optimize.partial.aggr.join.transpose.max.join.expansion}, when the pushed keys outnumber the
 *   original keys and the columns of the IF conditions or are empty, or when a pushed key has an NDV of more than
 *   {@code hive.optimize.partial.aggr.join.transpose.max.key.ndv.ratio} x input rows </li>
 *   <li>missing statistics also skip it. Required:
 *   <ul>
 *     <li>the row count of the join;</li>
 *     <li>the row count of the join input the aggregate is pushed to;</li>
 *     <li>the column statistics of every pushed key column (both join and aggregate);</li>
 *     <li>the row counts of every join input, when no aggregation argument has columns (e.g. every aggregation is
 *     {@code count(*)} or {@code count(1)}).</li>
 *   </ul>
 *   </li>
 *   <li>the push is also skipped when the join reduces the pushed input by more than
 *   {@code hive.optimize.partial.aggr.join.transpose.max.join.reduction}: Hive has no dynamic filters for a map
 *   join, so the pushed GROUP BY would process every row the join discards.</li>
 * </ul>
 *
 * <p>When a pushed GROUP BY is matched again it can be moved further down a chain of joins. It is not kept above the
 * join it moves below: a Select projecting its keys and the partial results replaces it.
 *
 * <p>The TezCompiler annotates the statistics, which the gates above read, right before this optimization.
 * This optimization runs before the top n key optimization: a Top N Key operator pushed down to the join output
 * would sit between the join and the GROUP BY. It runs before the dynamic partition pruning: its semijoin branches
 * would hang off the join inputs and the joins,
 * so the GROUP BY could not move below them, and built afterwards they read from the pushed GROUP BY. The
 * synthetic join predicates of residual join filters that read the values of a pushed join input are moved to the
 * pushed GROUP BY output.
 */
public class PartialGroupByJoinTransposeOptimizer extends Transform {

  private static final Logger LOG = LoggerFactory.getLogger(PartialGroupByJoinTransposeOptimizer.class);

  private static final Set<String> SUPPORTED_UDAFS = Set.of("sum", "$sum0", "count", "min", "max");

  // inputOf results other than a join input tag
  private static final int NO_INPUT = -1;
  private static final int INVALID = -2;

  private HiveConf conf;
  private float minReduction;
  private float maxJoinReduction;
  private float maxJoinExpansion;
  private float maxKeyNdvRatio;
  // the value lists of synthetic join predicates from residual join filters: their target is an expression over
  // the input of the join input they read from
  private final List<ExprNodeDynamicListDesc> residualDynamicLists = new ArrayList<>();

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    conf = pctx.getConf();
    minReduction = HiveConf.getFloatVar(conf, HiveConf.ConfVars.PARTIAL_AGGR_JOIN_TRANSPOSE_MIN_REDUCTION);
    maxJoinReduction = HiveConf.getFloatVar(conf, HiveConf.ConfVars.PARTIAL_AGGR_JOIN_TRANSPOSE_MAX_JOIN_REDUCTION);
    maxJoinExpansion = HiveConf.getFloatVar(conf, HiveConf.ConfVars.PARTIAL_AGGR_JOIN_TRANSPOSE_MAX_JOIN_EXPANSION);
    maxKeyNdvRatio = HiveConf.getFloatVar(conf, HiveConf.ConfVars.PARTIAL_AGGR_JOIN_TRANSPOSE_MAX_KEY_NDV_RATIO);
    String JOIN = JoinOperator.getOperatorName() + "%";
    String SEL = SelectOperator.getOperatorName() + "%";
    String GBY = GroupByOperator.getOperatorName() + "%";

    Map<SemanticRule, SemanticNodeProcessor> opRules = Map.of(
        new RuleRegExp("R1", JOIN + "(" + SEL + ")*" + GBY), new CandidateCollector());

    // only collect the candidates during the walk and push them afterwards: pushing changes the operators around the
    // join, which the walker may still visit
    CandidateCtx ctx = new CandidateCtx();
    SemanticDispatcher semanticDispatcher = new DefaultRuleDispatcher(null, opRules, ctx);
    SemanticGraphWalker graphWalker = new DefaultGraphWalker(semanticDispatcher);
    List<Node> topNodes = new ArrayList<>(pctx.getTopOps().values());
    graphWalker.startWalking(topNodes, null);
    if (ctx.candidates.isEmpty()) {
      return pctx;
    }

    List<Operator<?>> topOps = new ArrayList<>(pctx.getTopOps().values());
    for (FilterOperator filter : OperatorUtils.findOperators(topOps, FilterOperator.class)) {
      collectResidualDynamicLists(filter.getConf().getPredicate(), residualDynamicLists);
    }

    Deque<GroupByOperator> candidates = new ArrayDeque<>(ctx.candidates);
    while (!candidates.isEmpty()) {
      GroupByOperator pushed = push(candidates.poll());
      if (pushed != null) {
        // it may move further down a chain of joins
        candidates.add(pushed);
      }
    }
    return pctx;
  }

  private static class CandidateCtx implements NodeProcessorCtx {
    private final Set<GroupByOperator> candidates = new LinkedHashSet<>();
  }

  private static class CandidateCollector implements SemanticNodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs) {
      ((CandidateCtx) procCtx).candidates.add((GroupByOperator) nd);
      return null;
    }
  }

  private static void collectResidualDynamicLists(ExprNodeDesc expr, List<ExprNodeDynamicListDesc> lists) {
    if (expr instanceof ExprNodeDynamicListDesc) {
      if (((ExprNodeDynamicListDesc) expr).getTarget() != null) {
        lists.add((ExprNodeDynamicListDesc) expr);
      }
    } else if (expr != null && expr.getChildren() != null) {
      for (ExprNodeDesc child : expr.getChildren()) {
        collectResidualDynamicLists(child, lists);
      }
    }
  }

  /**
   * Pushes the GROUP BY below the join it is over, if every criterion is met.
   *
   * @return the pushed GROUP BY, or null if the plan is unchanged
   */
  private GroupByOperator push(GroupByOperator gby) throws SemanticException {
    GroupByDesc gbyDesc = gby.getConf();
    if (gby.getNumParent() != 1 || !gbyDesc.getMode().isHash() || gbyDesc.isGroupingSetsPresent()
        || gbyDesc.getBucketGroup() || gbyDesc.isDistinct() || gbyDesc.getKeys().isEmpty()) {
      return null;
    }
    List<AggregationDesc> aggs = gbyDesc.getAggregators();
    for (AggregationDesc agg : aggs) {
      if (agg.getDistinct() || agg.getMode() != GenericUDAFEvaluator.Mode.PARTIAL1
          || !SUPPORTED_UDAFS.contains(agg.getGenericUDAFName().toLowerCase())) {
        return null;
      }
    }

    // the operator directly over the join: the lowest Select or the GROUP BY itself
    Operator<?> joinChild = gby;
    Operator<?> parent = gby.getParentOperators().getFirst();
    while (parent instanceof SelectOperator) {
      if (parent.getNumChild() != 1 || parent.getNumParent() != 1) {
        return null;
      }
      joinChild = parent;
      parent = parent.getParentOperators().getFirst();
    }
    if (!(parent instanceof JoinOperator join) || parent.getNumChild() != 1) {
      return null;
    }
    JoinDesc joinDesc = join.getConf();
    if (joinDesc.getHandleSkewJoin()) {
      return null;
    }
    for (JoinCondDesc cond : joinDesc.getConds()) {
      if (cond.getType() != JoinDesc.INNER_JOIN) {
        return null;
      }
    }
    ReduceSinkOperator[] rsByTag = new ReduceSinkOperator[join.getNumParent()];
    for (Operator<?> joinParent : join.getParentOperators()) {
      if (!(joinParent instanceof ReduceSinkOperator reduceSinkOperator) || joinParent.getNumParent() != 1
          || joinParent.getNumChild() != 1) {
        return null;
      }
      int tag = reduceSinkOperator.getConf().getTag();
      if (tag < 0 || tag >= rsByTag.length || rsByTag[tag] != null) {
        return null;
      }
      rsByTag[tag] = reduceSinkOperator;
    }
    Map<String, Byte> reversedExprs = joinDesc.getReversedExprs();
    if (reversedExprs == null || join.getColumnExprMap() == null) {
      return null;
    }
    for (String name : joinDesc.getOutputColumnNames()) {
      if (reversedExprs.get(name) == null || join.getSchema().getColumnInfo(name) == null) {
        return null;
      }
    }

    // the GBY keys and aggregation arguments, backtracked through the Selects to the join output, and the join
    // input each one references
    List<ExprNodeDesc> keys = ExprNodeDescUtils.backtrack(gbyDesc.getKeys(), gby, joinChild);
    int[] keyInputs = new int[keys.size()];
    for (int i = 0; i < keys.size(); i++) {
      keyInputs[i] = inputOf(keys.get(i), reversedExprs);
      if (keyInputs[i] == INVALID) {
        return null;
      }
    }
    List<List<ExprNodeDesc>> params = new ArrayList<>();
    List<CaseSplit> splits = new ArrayList<>();
    Set<String> conditionCols = new HashSet<>();
    int side = NO_INPUT;
    for (AggregationDesc agg : aggs) {
      List<ExprNodeDesc> aggParams = ExprNodeDescUtils.backtrack(agg.getParameters(), gby, joinChild);
      CaseSplit split = null;
      if (aggParams.size() == 1 && inputOf(aggParams.getFirst(), reversedExprs) == INVALID) {
        // the value is aggregated below the join, on the input other than the condition's
        split = splitCase(agg, aggParams.getFirst(), reversedExprs);
        if (split == null) {
          return null;
        }
        conditionCols.addAll(split.conditionCols);
        aggParams = new ArrayList<>(Arrays.asList(split.value));
      }
      splits.add(split);
      for (ExprNodeDesc param : aggParams) {
        int input = inputOf(param, reversedExprs);
        if (input == INVALID) {
          return null;
        }
        // the arguments of all the aggregations must be on the same join input, the one the GBY is pushed to
        if (input != NO_INPUT && side != NO_INPUT && input != side) {
          return null;
        }
        if (input != NO_INPUT) {
          side = input;
        }
      }
      params.add(aggParams);
    }
    if (side == NO_INPUT) {
      side = largestInput(rsByTag);
      if (side == NO_INPUT) {
        return null;
      }
    }
    // The join's filter predicates (not its residual filter predicates) on the input the GROUP BY would be pushed to
    // are evaluated on each row of that input, but it would send partial results instead of rows. Bail out. Inner
    // joins have such predicates only when the CBO return path (hive.cbo.returnpath.hiveop) is on.
    if (joinDesc.getFilters() != null && joinDesc.getFilters().get((byte) side) != null
        && !joinDesc.getFilters().get((byte) side).isEmpty()) {
      return null;
    }
    ReduceSinkOperator rs = rsByTag[side];
    ReduceSinkDesc rsDesc = rs.getConf();
    Operator<?> sideRsParent = rs.getParentOperators().getFirst();

    // pushed keys over the input of the join input: the original keys of the input, its join keys and its
    // columns used by residual filters
    List<ExprNodeDesc> pushedKeys = new ArrayList<>();
    int[] keyToPushed = new int[keys.size()];
    for (int i = 0; i < keys.size(); i++) {
      keyToPushed[i] = -1;
      if (keyInputs[i] == side) {
        ExprNodeDesc key = ExprNodeDescUtils.backtrack(keys.get(i), joinChild, rs);
        if (key == null) {
          return null;
        }
        keyToPushed[i] = addUnique(pushedKeys, key);
      }
    }
    List<ExprNodeDesc> rsKeyCols = rsDesc.getKeyCols();
    int[] rsKeyToPushed = new int[rsKeyCols.size()];
    for (int k = 0; k < rsKeyCols.size(); k++) {
      rsKeyToPushed[k] = addUnique(pushedKeys, rsKeyCols.get(k));
    }
    List<ExprNodeDesc> residualFilters = joinDesc.getResidualFilterExprs();
    Map<String, Integer> filterColToPushed = new HashMap<>();
    if (residualFilters != null) {
      for (ExprNodeDesc filter : residualFilters) {
        List<ExprNodeColumnDesc> cols = columnsOf(filter);
        if (cols == null) {
          return null;
        }
        for (ExprNodeColumnDesc col : cols) {
          Byte tag = reversedExprs.get(col.getColumn());
          if (tag == null) {
            return null;
          }
          if (tag == side && !filterColToPushed.containsKey(col.getColumn())) {
            ExprNodeDesc key = ExprNodeDescUtils.backtrack(col, joinChild, rs);
            if (key == null) {
              return null;
            }
            filterColToPushed.put(col.getColumn(), addUnique(pushedKeys, key));
          }
        }
      }
    }
    // The push is skipped when the pushed keys (the original keys on this input, its
    // join keys and its columns used by residual filters) are empty or outnumber the keys of the original GROUP BY
    // plus the columns of the split IF conditions.
    if (pushedKeys.isEmpty() || pushedKeys.size() > keys.size() + conditionCols.size()) {
      return null;
    }
    List<ExprNodeDesc> partitionCols = rsDesc.getPartitionCols();
    int[] partitionToPushed = new int[partitionCols.size()];
    for (int k = 0; k < partitionCols.size(); k++) {
      partitionToPushed[k] = ExprNodeDescUtils.indexOf(partitionCols.get(k), pushedKeys);
      if (partitionToPushed[k] < 0) {
        return null;
      }
    }
    // the synthetic join predicates of residual filters reading from the join input read from the pushed keys
    List<ExprNodeDynamicListDesc> dynamicLists = new ArrayList<>();
    List<Integer> dynamicListToPushed = new ArrayList<>();
    for (ExprNodeDynamicListDesc dynamicList : residualDynamicLists) {
      if (dynamicList.getSource() == rs) {
        int p = ExprNodeDescUtils.indexOf(dynamicList.getTarget(), pushedKeys);
        if (p < 0) {
          return null;
        }
        dynamicLists.add(dynamicList);
        dynamicListToPushed.add(p);
      }
    }

    if (!isWorthPushing(join, sideRsParent, pushedKeys)) {
      return null;
    }

    // the pushed GROUP BY: the pushed keys, then the partial results
    int numPushedKeys = pushedKeys.size();
    List<String> pushedNames = new ArrayList<>();
    List<ColumnInfo> pushedCols = new ArrayList<>();
    Map<String, ExprNodeDesc> pushedColExprMap = new HashMap<>();
    for (int i = 0; i < numPushedKeys; i++) {
      String name = SemanticAnalyzer.getColumnInternalName(i);
      pushedNames.add(name);
      pushedCols.add(new ColumnInfo(name, pushedKeys.get(i).getTypeInfo(), "", false));
      pushedColExprMap.put(name, pushedKeys.get(i));
    }
    List<AggregationDesc> pushedAggs = new ArrayList<>();
    for (int i = 0; i < aggs.size(); i++) {
      List<ExprNodeDesc> aggParams = new ArrayList<>();
      for (ExprNodeDesc param : params.get(i)) {
        ExprNodeDesc pushedParam = ExprNodeDescUtils.backtrack(param, joinChild, rs);
        if (pushedParam == null) {
          return null;
        }
        aggParams.add(pushedParam);
      }
      String udafName = aggs.get(i).getGenericUDAFName();
      GenericUDAFEvaluator evaluator = FunctionRegistry.getGenericUDAFEvaluator(udafName,
          aggParams.stream().map(ExprNodeDesc::getWritableObjectInspector).collect(Collectors.toList()),
          false, aggParams.isEmpty());
      if (evaluator == null) {
        return null;
      }
      GenericUDAFInfo udafInfo =
          SemanticAnalyzer.getGenericUDAFInfo(evaluator, GenericUDAFEvaluator.Mode.PARTIAL1, aggParams);
      pushedAggs.add(new AggregationDesc(udafName, udafInfo.genericUDAFEvaluator, udafInfo.convertedParameters,
          false, GenericUDAFEvaluator.Mode.PARTIAL1));
      String name = SemanticAnalyzer.getColumnInternalName(pushedNames.size());
      pushedNames.add(name);
      pushedCols.add(new ColumnInfo(name, udafInfo.returnType, "", false));
    }
    GroupByDesc pushedDesc = new GroupByDesc(GroupByDesc.Mode.PARTIAL_HASH, pushedNames, pushedKeys, pushedAggs,
        false, gbyDesc.getGroupByMemoryUsage(), gbyDesc.getMemoryThreshold(),
        Math.min(gbyDesc.getMinReductionHashAggr(), minReduction), gbyDesc.getMinReductionHashAggrLowerBound(),
        gbyDesc.getHashAggrFlushPercent(), null, false, -1, false);

    // the join input: the pushed keys used as join keys stay keys, the other outputs of the pushed GROUP BY
    // become values
    int[] pushedToRsKey = new int[numPushedKeys];
    Arrays.fill(pushedToRsKey, -1);
    for (int k = 0; k < rsKeyToPushed.length; k++) {
      if (pushedToRsKey[rsKeyToPushed[k]] < 0) {
        pushedToRsKey[rsKeyToPushed[k]] = k;
      }
    }
    List<String> rsKeyNames = rsDesc.getOutputKeyColumnNames();
    List<ExprNodeDesc> newKeyCols = new ArrayList<>();
    Map<String, ExprNodeDesc> rsColExprMap = new HashMap<>();
    List<ColumnInfo> rsSignature = new ArrayList<>();
    for (int k = 0; k < rsKeyToPushed.length; k++) {
      ExprNodeDesc keyCol = columnRef(pushedCols.get(rsKeyToPushed[k]));
      String field = Utilities.ReduceField.KEY + "." + rsKeyNames.get(k);
      newKeyCols.add(keyCol);
      rsColExprMap.put(field, keyCol);
      rsSignature.add(new ColumnInfo(field, keyCol.getTypeInfo(), "", false));
    }
    List<ExprNodeDesc> newPartitionCols = new ArrayList<>();
    for (int p : partitionToPushed) {
      newPartitionCols.add(columnRef(pushedCols.get(p)));
    }
    ArrayList<ExprNodeDesc> valueCols = new ArrayList<>();
    ArrayList<String> valueNames = new ArrayList<>();
    int[] valueIndex = new int[pushedCols.size()];
    String[] rsFields = new String[pushedCols.size()];
    for (int i = 0; i < pushedCols.size(); i++) {
      if (i < numPushedKeys && pushedToRsKey[i] >= 0) {
        valueIndex[i] = pushedToRsKey[i];
        rsFields[i] = Utilities.ReduceField.KEY + "." + rsKeyNames.get(pushedToRsKey[i]);
      } else {
        ExprNodeDesc valueCol = columnRef(pushedCols.get(i));
        valueIndex[i] = -valueCols.size() - 1;
        rsFields[i] = Utilities.ReduceField.VALUE + "." + pushedNames.get(i);
        valueCols.add(valueCol);
        valueNames.add(pushedNames.get(i));
        rsColExprMap.put(rsFields[i], valueCol);
        rsSignature.add(new ColumnInfo(rsFields[i], valueCol.getTypeInfo(), "", false));
      }
    }

    // The join outputs of the join output the GBY is pushed down
    boolean[] usedAbove = new boolean[pushedCols.size()];
    for (int i : keyToPushed) {
      if (i >= 0) {
        usedAbove[i] = true;
      }
    }
    for (int i : filterColToPushed.values()) {
      usedAbove[i] = true;
    }
    Arrays.fill(usedAbove, numPushedKeys, usedAbove.length, true);
    Set<String> otherInputNames = new HashSet<>();
    for (String name : joinDesc.getOutputColumnNames()) {
      if (reversedExprs.get(name) != side) {
        otherInputNames.add(name);
      }
    }
    String[] joinNames = new String[pushedCols.size()];
    List<ExprNodeDesc> sideExprs = new ArrayList<>();
    List<String> sideNames = new ArrayList<>();
    List<ColumnInfo> sideCols = new ArrayList<>();
    Map<String, ExprNodeDesc> newJoinColExprMap = new HashMap<>();
    Map<String, Byte> newReversedExprs = new HashMap<>();
    int nextName = 0;
    for (int i = 0; i < pushedCols.size(); i++) {
      if (!usedAbove[i]) {
        continue;
      }
      String name;
      do {
        name = SemanticAnalyzer.getColumnInternalName(nextName++);
      } while (otherInputNames.contains(name));
      TypeInfo type = pushedCols.get(i).getType();
      ExprNodeDesc expr = new ExprNodeColumnDesc(type, rsFields[i], null, false);
      joinNames[i] = name;
      sideExprs.add(expr);
      sideNames.add(name);
      sideCols.add(new ColumnInfo(name, type, "", false));
      newJoinColExprMap.put(name, expr);
      newReversedExprs.put(name, (byte) side);
    }
    for (Map.Entry<String, ExprNodeDesc> e : join.getColumnExprMap().entrySet()) {
      Byte tag = reversedExprs.get(e.getKey());
      if (tag != null && tag != side) {
        newJoinColExprMap.put(e.getKey(), e.getValue());
      }
    }
    // the join output concatenates the outputs of its inputs in tag order
    List<String> newOutputNames = new ArrayList<>();
    List<ColumnInfo> newJoinSignature = new ArrayList<>();
    for (byte tag : tagOrder(joinDesc, rsByTag.length)) {
      if (tag == side) {
        newOutputNames.addAll(sideNames);
        newJoinSignature.addAll(sideCols);
        continue;
      }
      for (String name : joinDesc.getOutputColumnNames()) {
        if (reversedExprs.get(name) == tag) {
          newOutputNames.add(name);
          newJoinSignature.add(join.getSchema().getColumnInfo(name));
          newReversedExprs.put(name, tag);
        }
      }
    }
    final int sideTag = side;
    Function<ExprNodeColumnDesc, ExprNodeDesc> toNewJoinOutput = col -> {
      if (reversedExprs.get(col.getColumn()) != sideTag) {
        return col;
      }
      int p = filterColToPushed.get(col.getColumn());
      return new ExprNodeColumnDesc(pushedCols.get(p).getType(), joinNames[p], col.getTabAlias(),
          col.getIsPartitionColOrVirtualCol());
    };
    List<ExprNodeDesc> newResidualFilters = null;
    if (residualFilters != null) {
      newResidualFilters = new ArrayList<>();
      for (ExprNodeDesc filter : residualFilters) {
        newResidualFilters.add(substitute(filter, toNewJoinOutput));
      }
    }

    // the keys and aggregation arguments of the operator over the join (the GROUP BY, or the Select replacing it)
    List<ExprNodeDesc> newKeys = new ArrayList<>();
    for (int i = 0; i < keys.size(); i++) {
      newKeys.add(keyToPushed[i] >= 0
          ? new ExprNodeColumnDesc(pushedCols.get(keyToPushed[i]).getType(), joinNames[keyToPushed[i]], null, false)
          : keys.get(i));
    }
    List<ExprNodeDesc> partialRefs = new ArrayList<>();
    for (int i = 0; i < aggs.size(); i++) {
      int j = numPushedKeys + i;
      ExprNodeDesc partial = new ExprNodeColumnDesc(pushedCols.get(j).getType(), joinNames[j], null, false);
      CaseSplit split = splits.get(i);
      if (split != null) {
        // a COUNT whose rows all fail the condition is 0; the other functions keep the ELSE
        TypeInfo type = partial.getTypeInfo();
        boolean count = "count".equalsIgnoreCase(aggs.get(i).getGenericUDAFName());
        ExprNodeConstantDesc otherwise = count || split.elseBranch == ElseBranch.ZERO
            ? zeroOf(type)
            : new ExprNodeConstantDesc(type, null);
        if (otherwise == null) {
          return null;
        }
        partial = ExprNodeGenericFuncDesc.newInstance(new GenericUDFIf(),
            new ArrayList<>(Arrays.asList(split.condition.clone(), partial, otherwise)));
      }
      partialRefs.add(partial);
    }

    LOG.debug("Pushing {} below {} to input {}", gby, join, side);

    // splice the pushed GROUP BY between the join input and its input
    GroupByOperator pushedGBY = (GroupByOperator) OperatorFactory.get(sideRsParent.getCompilationOpContext(),
        pushedDesc, new RowSchema(pushedCols));
    pushedGBY.setColumnExprMap(pushedColExprMap);
    sideRsParent.replaceChild(rs, pushedGBY);
    pushedGBY.setParentOperators(Utilities.makeList(sideRsParent));
    pushedGBY.setChildOperators(Utilities.makeList(rs));
    rs.replaceParent(sideRsParent, pushedGBY);

    rsDesc.setKeyCols(newKeyCols);
    rsDesc.setPartitionCols(newPartitionCols);
    rsDesc.setValueCols(valueCols);
    rsDesc.setOutputValueColumnNames(valueNames);
    rsDesc.setValueSerializeInfo(PlanUtils.getReduceValueTableDesc(
        PlanUtils.getFieldSchemasFromColumnList(valueCols, valueNames, 0, "")));
    rs.setValueIndex(valueIndex);
    rs.setColumnExprMap(rsColExprMap);
    rs.setSchema(new RowSchema(rsSignature));
    for (int i = 0; i < dynamicLists.size(); i++) {
      dynamicLists.get(i).setTarget(columnRef(pushedCols.get(dynamicListToPushed.get(i))));
    }

    Map<Byte, List<ExprNodeDesc>> joinExprs = new LinkedHashMap<>(joinDesc.getExprs());
    joinExprs.put((byte) side, sideExprs);
    joinDesc.setExprs(joinExprs);
    joinDesc.setOutputColumnNames(newOutputNames);
    joinDesc.setReversedExprs(newReversedExprs);
    if (newResidualFilters != null) {
      joinDesc.setResidualFilterExprs(newResidualFilters);
    }
    ExprNodeDesc[][] joinKeys = joinDesc.getJoinKeys();
    if (joinKeys != null && side < joinKeys.length) {
      joinKeys[side] = newKeyCols.toArray(new ExprNodeDesc[0]);
    }
    join.setColumnExprMap(newJoinColExprMap);
    join.setSchema(new RowSchema(newJoinSignature));

    if (gbyDesc.getMode() == GroupByDesc.Mode.HASH) {
      // the GROUP BY merges the partial results; the Selects between it and the join are dropped
      gbyDesc.setKeys(newKeys);
      Map<String, ExprNodeDesc> gbyColExprMap =
          gby.getColumnExprMap() == null ? new HashMap<>() : new HashMap<>(gby.getColumnExprMap());
      for (int i = 0; i < newKeys.size(); i++) {
        gbyColExprMap.put(gbyDesc.getOutputColumnNames().get(i), newKeys.get(i));
      }
      gby.setColumnExprMap(gbyColExprMap);
      for (int a = 0; a < aggs.size(); a++) {
        aggs.get(a).setParameters(new ArrayList<>(Arrays.asList(partialRefs.get(a))));
        aggs.get(a).setMode(GenericUDAFEvaluator.Mode.PARTIAL2);
      }
      if (joinChild != gby) {
        Operator<?> gbyParent = gby.getParentOperators().get(0);
        join.replaceChild(joinChild, gby);
        gby.replaceParent(gbyParent, join);
        joinChild.setParentOperators(null);
        gbyParent.setChildOperators(null);
      }
    } else {
      // this GROUP BY was pushed earlier: the partial results pass through the join to the GROUP BY above the next
      // join
      List<ExprNodeDesc> selExprs = new ArrayList<>(newKeys);
      selExprs.addAll(partialRefs);
      List<String> selNames = new ArrayList<>(gbyDesc.getOutputColumnNames());
      Map<String, ExprNodeDesc> selColExprMap = new HashMap<>();
      for (int i = 0; i < selNames.size(); i++) {
        selColExprMap.put(selNames.get(i), selExprs.get(i));
      }
      SelectOperator sel = (SelectOperator) OperatorFactory.get(join.getCompilationOpContext(),
          new SelectDesc(selExprs, selNames), new RowSchema(gby.getSchema()));
      sel.setColumnExprMap(selColExprMap);
      join.replaceChild(joinChild, sel);
      sel.setParentOperators(Utilities.makeList(join));
      sel.setChildOperators(new ArrayList<>(gby.getChildOperators()));
      for (Operator<?> child : gby.getChildOperators()) {
        child.replaceParent(gby, sel);
      }
      joinChild.setParentOperators(null);
      gby.setParentOperators(null);
      gby.setChildOperators(null);
    }
    return pushedGBY;
  }

  /**
   * Cost criteria, from the statistics of the join and of the input of the join input.
   */
  private boolean isWorthPushing(JoinOperator join, Operator<?> sideRs, List<ExprNodeDesc> pushedKeys) {
    Statistics inputStats = sideRs.getStatistics();
    Statistics joinStats = join.getStatistics();
    if (inputStats == null || joinStats == null) {
      return false;
    }
    long inputRows = inputStats.getNumRows();
    long joinRows = joinStats.getNumRows();
    if (joinRows > maxJoinExpansion * inputRows) {
      return false;
    }
    if (maxJoinReduction > 0 && joinRows * maxJoinReduction < inputRows) {
      return false;
    }
    for (ExprNodeDesc key : pushedKeys) {
      ColStatistics keyStats = StatsUtils.getColStatisticsFromExpression(conf, inputStats, key);
      if (keyStats == null || keyStats.getCountDistint() > maxKeyNdvRatio * inputRows) {
        return false;
      }
    }
    return true;
  }

  /**
   * @return the tag of the join input whose output columns the expression references, NO_INPUT if it
   *         references none, or INVALID if it cannot be pushed
   */
  private static int inputOf(ExprNodeDesc expr, Map<String, Byte> reversedExprs) {
    if (expr == null || !ExprNodeDescUtils.isDeterministic(expr)) {
      return INVALID;
    }
    List<ExprNodeColumnDesc> cols = columnsOf(expr);
    if (cols == null) {
      return INVALID;
    }
    int input = NO_INPUT;
    for (ExprNodeColumnDesc col : cols) {
      Byte tag = reversedExprs.get(col.getColumn());
      if (tag == null || input != NO_INPUT && input != tag) {
        return INVALID;
      }
      input = tag;
    }
    return input;
  }

  /**
   * An aggregation argument IF(condition, value, otherwise) split across the join: the value is aggregated below
   * the join, the condition is applied to the partial result above it. For example, over store_sales JOIN date_dim:
   * <pre>
   *   TS(store_sales) - GBY(partialhash) - RS \
   *                                            JOIN - GBY(hash)
   *   TS(date_dim) ----------------------- RS /
   *
   *   GBY(partialhash): sum(ss_sales_price)                            the value, below the join
   *   GBY(hash):        sum(if(d_day_name = 'Sunday', partial, null))  the condition, above the join
   * </pre>
   */
  private record CaseSplit(ExprNodeDesc condition, ExprNodeDesc value, ElseBranch elseBranch,
      Set<String> conditionCols) {}

  /** The else branch of a split IF or CASE: NULL (also a missing ELSE), or 0 for a SUM. */
  private enum ElseBranch { NULL, ZERO }

  /**
   * An IF or CASE argument of an aggregate that can be split across the join, or null:
   * IF(condition, value, otherwise) with the condition on one join input and the value on the other.
   * The condition is unknown below the join, so the pushed GROUP BY aggregates the value of every row, not just
   * the ones whose condition would be true. The new query can not fail even when evaluating the value of a row whose
   * condition would be false so only these expression elements are allowed:<br>
   * columns, constants, +, -, * and casts.
   */
  private static CaseSplit splitCase(AggregationDesc agg, ExprNodeDesc param, Map<String, Byte> reversedExprs)
      throws SemanticException {
    if (!(param instanceof ExprNodeGenericFuncDesc) || !ExprNodeDescUtils.isDeterministic(param)) {
      return null;
    }
    GenericUDF udf = ((ExprNodeGenericFuncDesc) param).getGenericUDF();
    List<ExprNodeDesc> children = param.getChildren();
    boolean when = udf instanceof GenericUDFWhen && (children.size() == 2 || children.size() == 3);
    if (!when && !(udf instanceof GenericUDFIf && children.size() == 3)) {
      return null;
    }
    ExprNodeDesc condition = children.get(0);
    ExprNodeDesc value = children.get(1);
    int conditionInput = inputOf(condition, reversedExprs);
    int valueInput = inputOf(value, reversedExprs);
    if (conditionInput < 0 || valueInput < 0 || conditionInput == valueInput || mayFail(value)) {
      return null;
    }
    String udafName = agg.getGenericUDAFName().toLowerCase();
    ElseBranch elseBranch = ElseBranch.NULL;
    if (children.size() == 3 && !isNull(children.get(2))) {
      if (!("sum".equals(udafName) || "$sum0".equals(udafName)) || !isZero(children.get(2))) {
        return null;
      }
      elseBranch = ElseBranch.ZERO;
    }
    // The type of the IF unifies the types of the value and the else branch, so it can be wider than the value.
    // CBO casts a value of another type (e.g. an int value with ELSE 0.0), but not a decimal of another precision or
    // scale: IF(c, value, 0) with a decimal(2,2) value is a decimal(3,2), because the 0, a decimal(1,0), needs one
    // digit before the decimal point and the value has none.
    if (!value.getTypeInfo().equals(param.getTypeInfo())) {
      if (!(param.getTypeInfo() instanceof PrimitiveTypeInfo)) {
        return null;
      }
      value = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
          .createConversionCast(value, (PrimitiveTypeInfo) param.getTypeInfo());
    }
    Set<String> conditionCols = new HashSet<>();
    for (ExprNodeColumnDesc col : columnsOf(condition)) {
      conditionCols.add(col.getColumn());
    }
    return new CaseSplit(condition, value, elseBranch, conditionCols);
  }

  /** True unless the expression is built only from columns, constants, +, -, * and casts, which cannot fail. */
  private static boolean mayFail(ExprNodeDesc expr) {
    if (expr instanceof ExprNodeColumnDesc || expr instanceof ExprNodeConstantDesc) {
      return false;
    }
    if (!(expr instanceof ExprNodeGenericFuncDesc)) {
      return true;
    }
    GenericUDF udf = ((ExprNodeGenericFuncDesc) expr).getGenericUDF();
    if (!(udf instanceof GenericUDFOPPlus || udf instanceof GenericUDFOPMinus || udf instanceof GenericUDFOPMultiply
        || FunctionRegistry.isOpNegative(expr) || FunctionRegistry.isOpCast(expr))) {
      return true;
    }
    for (ExprNodeDesc child : expr.getChildren()) {
      if (mayFail(child)) {
        return true;
      }
    }
    return false;
  }

  private static boolean isNull(ExprNodeDesc expr) {
    return expr instanceof ExprNodeConstantDesc && ((ExprNodeConstantDesc) expr).getValue() == null;
  }

  private static boolean isZero(ExprNodeDesc expr) {
    if (!(expr instanceof ExprNodeConstantDesc)) {
      return false;
    }
    Object value = ((ExprNodeConstantDesc) expr).getValue();
    if (value instanceof HiveDecimal) {
      return ((HiveDecimal) value).signum() == 0;
    }
    return value instanceof Number && ((Number) value).doubleValue() == 0;
  }

  private static ExprNodeConstantDesc zeroOf(TypeInfo type) {
    if (!(type instanceof PrimitiveTypeInfo)) {
      return null;
    }
    return switch (((PrimitiveTypeInfo) type).getPrimitiveCategory()) {
      case LONG -> new ExprNodeConstantDesc(type, 0L);
      case DOUBLE -> new ExprNodeConstantDesc(type, 0.0d);
      case DECIMAL -> new ExprNodeConstantDesc(type, HiveDecimal.ZERO);
      default -> null;
    };
  }

  /** The join input with the most rows, or NO_INPUT without statistics. */
  private static int largestInput(ReduceSinkOperator[] rsByTag) {
    int largest = NO_INPUT;
    long largestRows = -1;
    for (int tag = 0; tag < rsByTag.length; tag++) {
      Statistics stats = rsByTag[tag].getStatistics();
      if (stats == null) {
        return NO_INPUT;
      }
      if (stats.getNumRows() > largestRows) {
        largest = tag;
        largestRows = stats.getNumRows();
      }
    }
    return largest;
  }

  private static Byte[] tagOrder(JoinDesc joinDesc, int numInputs) {
    if (joinDesc.getTagOrder() != null) {
      return joinDesc.getTagOrder();
    }
    Byte[] tagOrder = new Byte[numInputs];
    for (int tag = 0; tag < numInputs; tag++) {
      tagOrder[tag] = (byte) tag;
    }
    return tagOrder;
  }

  private static int addUnique(List<ExprNodeDesc> exprs, ExprNodeDesc expr) {
    int index = ExprNodeDescUtils.indexOf(expr, exprs);
    if (index >= 0) {
      return index;
    }
    exprs.add(expr);
    return exprs.size() - 1;
  }

  private static ExprNodeDesc columnRef(ColumnInfo col) {
    return new ExprNodeColumnDesc(col.getType(), col.getInternalName(), null, false);
  }

  /** The referenced columns in the given expression, or null if the expression has an unsupported kind of node. */
  private static List<ExprNodeColumnDesc> columnsOf(ExprNodeDesc expr) {
    List<ExprNodeColumnDesc> cols = new ArrayList<>();
    return substitute(expr, col -> {
      cols.add(col);
      return col;
    }) == null ? null : cols;
  }

  /**
   * Copies the expression with its columns replaced.
   *
   * @return the new expression or null if the expression has an unsupported kind of node
   */
  private static ExprNodeDesc substitute(ExprNodeDesc expr, Function<ExprNodeColumnDesc, ExprNodeDesc> columns) {
    if (expr instanceof ExprNodeColumnDesc) {
      return columns.apply((ExprNodeColumnDesc) expr);
    }
    if (expr instanceof ExprNodeConstantDesc) {
      return expr;
    }
    if (expr instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc func = (ExprNodeGenericFuncDesc) expr.clone();
      List<ExprNodeDesc> children = new ArrayList<>();
      for (ExprNodeDesc child : func.getChildren()) {
        ExprNodeDesc newChild = substitute(child, columns);
        if (newChild == null) {
          return null;
        }
        children.add(newChild);
      }
      func.setChildren(children);
      return func;
    }
    if (expr instanceof ExprNodeFieldDesc) {
      ExprNodeFieldDesc field = (ExprNodeFieldDesc) expr.clone();
      ExprNodeDesc desc = substitute(field.getDesc(), columns);
      if (desc == null) {
        return null;
      }
      field.setDesc(desc);
      return field;
    }
    return null;
  }
}
