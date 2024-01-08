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
package org.apache.hadoop.hive.ql.optimizer;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.parse.GenTezUtils;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.RuntimeValuesInfo;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.SemiJoinBranchInfo;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.DynamicValue;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicValueDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBloomFilter.GenericUDAFBloomFilterEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMax;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMin;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFInBloomFilter;
import org.apache.hadoop.hive.ql.util.NullOrdering;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkState;
import static org.apache.hadoop.hive.ql.exec.FunctionRegistry.BLOOM_FILTER_FUNCTION;
import static org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils.and;

/**
 * A transformation that merges multiple single-column semi join reducers to one multi-column semi join reducer.
 * <p>
 * Semi join reducers are comprised from two parts:
 * <ul>
 *   <li>Filter creation from the source relation and broadcast: SOURCE - SEL - GB1 - RS1 - GB2 - RS2</li>
 *   <li>Filter application on the target relation: TS - FIL[in_bloom(col)]</li>
 * </ul>
 * <p>
 * An example of the transformation on three single column semi join reducers is shown below. The plan is simplified for
 * presentation purposes.
 * <h3>BEFORE:</h3>
 * <pre>
 *        / SEL[fname] - GB1 - RS1 - GB2 - RS2  \
 * SOURCE - SEL[lname] - GB1 - RS1 - GB2 - RS2  -&gt; TS[Author] - FIL[in_bloom(fname) ^ in_bloom(lname) ^ in_bloom(age)]
 *        \ SEL[age]   - GB1 - RS1 - GB2 - RS2  /
 * </pre>
 * <h3>AFTER:</h3>
 * <pre>
 * SOURCE - SEL[fname, lname, age] - GB1 - RS1 - GB2 - RS2 -&gt; TS[Author] - FIL[in_bloom(hash(fname,lname,age)]
 * </pre>
 */
public class SemiJoinReductionMerge extends Transform {

  public ParseContext transform(ParseContext parseContext) throws SemanticException {
    Map<ReduceSinkOperator, SemiJoinBranchInfo> allSemijoins = parseContext.getRsToSemiJoinBranchInfo();
    if (allSemijoins.isEmpty()) {
      return parseContext;
    }
    HiveConf hiveConf = parseContext.getConf();

    for (Entry<SJSourceTarget, List<ReduceSinkOperator>> sjMergeCandidate : createMergeCandidates(allSemijoins)) {
      final List<ReduceSinkOperator> sjBranches = sjMergeCandidate.getValue();
      if (sjBranches.size() < 2) {
        continue;
      }
      List<SelectOperator> selOps = new ArrayList<>(sjBranches.size());
      for (ReduceSinkOperator rs : sjBranches) {
        selOps.add(OperatorUtils.ancestor(rs, SelectOperator.class, 0, 0, 0, 0));
      }

      long sjEntriesHint = extractBloomEntriesHint(sjBranches);

      SelectOperator selectOp = mergeSelectOps(sjMergeCandidate.getKey().source, selOps);

      GroupByOperator gbPartialOp = createGroupBy(selectOp, selectOp, GroupByDesc.Mode.HASH, sjEntriesHint, hiveConf);

      ReduceSinkOperator rsPartialOp = createReduceSink(gbPartialOp, NullOrdering.defaultNullOrder(hiveConf));
      rsPartialOp.getConf().setReducerTraits(EnumSet.of(ReduceSinkDesc.ReducerTraits.QUICKSTART));

      GroupByOperator gbCompleteOp =
          createGroupBy(selectOp, rsPartialOp, GroupByDesc.Mode.FINAL, sjEntriesHint, hiveConf);

      ReduceSinkOperator rsCompleteOp = createReduceSink(gbCompleteOp, NullOrdering.defaultNullOrder(hiveConf));

      final TableScanOperator sjTargetTable = sjMergeCandidate.getKey().target;
      SemiJoinBranchInfo sjInfo = new SemiJoinBranchInfo(sjTargetTable, false);
      parseContext.getRsToSemiJoinBranchInfo().put(rsCompleteOp, sjInfo);

      // Save the info that is required at query time to resolve dynamic/runtime values.
      RuntimeValuesInfo valuesInfo = createRuntimeValuesInfo(rsCompleteOp, sjBranches, parseContext);
      parseContext.getRsToRuntimeValuesInfoMap().put(rsCompleteOp, valuesInfo);

      ExprNodeGenericFuncDesc sjPredicate = createSemiJoinPredicate(sjBranches, valuesInfo, parseContext);

      // Update filter operators with the new semi-join predicate
      for (Operator<?> op : sjTargetTable.getChildOperators()) {
        if (op instanceof FilterOperator) {
          FilterDesc filter = ((FilterOperator) op).getConf();
          filter.setPredicate(and(filter.getPredicate(), sjPredicate));
        }
      }
      // Update tableScan with the new semi-join predicate
      sjTargetTable.getConf().setFilterExpr(and(sjTargetTable.getConf().getFilterExpr(), sjPredicate));

      for (ReduceSinkOperator rs : sjBranches) {
        GenTezUtils.removeSemiJoinOperator(parseContext, rs, sjTargetTable);
        GenTezUtils.removeBranch(rs);
      }
    }
    return parseContext;
  }

  /**
   * Groups single column semijoin reducer branches together if they have the same source and target relation.
   */
  private static Collection<Map.Entry<SJSourceTarget, List<ReduceSinkOperator>>> createMergeCandidates(
      Map<ReduceSinkOperator, SemiJoinBranchInfo> semijoins) {
    // Predictable iteration helps to avoid small changes in the plans
    LinkedHashMap<SJSourceTarget, List<ReduceSinkOperator>> sjGroups = new LinkedHashMap<>();
    for (Map.Entry<ReduceSinkOperator, SemiJoinBranchInfo> smjEntry : semijoins.entrySet()) {
      TableScanOperator ts = smjEntry.getValue().getTsOp();
      // Semijoin optimization branch should look like <Parent>-SEL-GB1-RS1-GB2-RS2
      SelectOperator selOp = OperatorUtils.ancestor(smjEntry.getKey(), SelectOperator.class, 0, 0, 0, 0);
      Operator<?> source = selOp.getParentOperators().get(0);
      checkState(selOp.getParentOperators().size() == 1, "Semijoin branches should not have multiple parents");
      SJSourceTarget sjKey = new SJSourceTarget(source, ts);
      List<ReduceSinkOperator> ops = sjGroups.computeIfAbsent(sjKey, tableScanOperator -> new ArrayList<>());
      ops.add(smjEntry.getKey());
    }
    return sjGroups.entrySet();
  }

  /**
   * Extracts a hint about the number of expected entries in the composite bloom filter from the hints in the specified
   * (single column) semijoin branches.
   * <p>
   * If none of the individual semijoin branches has hints then the method returns -1, otherwise it returns the max.
   * </p>
   */
  private static long extractBloomEntriesHint(List<ReduceSinkOperator> sjBranches) {
    long bloomEntries = -1;
    for (ReduceSinkOperator rs : sjBranches) {
      GroupByOperator gbOp = OperatorUtils.ancestor(rs, GroupByOperator.class, 0, 0, 0);
      List<GenericUDAFBloomFilterEvaluator> blooms =
          FunctionUtils.extractEvaluators(gbOp.getConf().getAggregators(), GenericUDAFBloomFilterEvaluator.class);
      Preconditions.checkState(blooms.size() == 1);
      if (blooms.get(0).hasHintEntries()) {
        bloomEntries = Math.max(bloomEntries, blooms.get(0).getExpectedEntries());
      }
    }
    return bloomEntries;
  }

  /**
   * Creates the multi-column semi-join predicate that is applied on the target relation.
   *
   * Assuming that the target columns of the semi-join are fname, lname, and age, the generated predicates is:
   * <pre>
   *   fname BETWEEN ?min_fname AND ?max_fname and
   *   lname BETWEEN ?min_lname AND ?max_lname and
   *   age   BETWEEN ?min_age   AND ?max_age and
   *   IN_BLOOM_FILTER(HASH(fname,lname,age),?bloom_filter)
   * </pre>
   * where the question mark (?) indicates dynamic values bound at runtime.
   */
  private static ExprNodeGenericFuncDesc createSemiJoinPredicate(List<ReduceSinkOperator> sjBranches,
      RuntimeValuesInfo sjValueInfo, ParseContext context) {
    // Performance note: To speed-up evaluation 'BETWEEN' predicates should come before the 'IN_BLOOM_FILTER'
    Deque<String> dynamicIds = new ArrayDeque<>(sjValueInfo.getDynamicValueIDs());
    List<ExprNodeDesc> sjPredicates = new ArrayList<>();
    List<ExprNodeDesc> hashArgs = new ArrayList<>();
    for (ReduceSinkOperator rs : sjBranches) {
      RuntimeValuesInfo info = context.getRsToRuntimeValuesInfoMap().get(rs);
      checkState(info.getTargetColumns().size() == 1, "Cannot handle multi-column semijoin branches.");
      final ExprNodeDesc targetColumn = info.getTargetColumns().get(0);
      TypeInfo typeInfo = targetColumn.getTypeInfo();
      DynamicValue minDynamic = new DynamicValue(dynamicIds.poll(), typeInfo);
      DynamicValue maxDynamic = new DynamicValue(dynamicIds.poll(), typeInfo);

      List<ExprNodeDesc> betweenArgs = Arrays.asList(
          // Use false to not invert between result
          new ExprNodeConstantDesc(Boolean.FALSE),
          targetColumn,
          new ExprNodeDynamicValueDesc(minDynamic),
          new ExprNodeDynamicValueDesc(maxDynamic));
      ExprNodeDesc betweenExp =
          new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, new GenericUDFBetween(), "between", betweenArgs);
      sjPredicates.add(betweenExp);
      hashArgs.add(targetColumn);
    }

    ExprNodeDesc hashExp = ExprNodeDescUtils.murmurHash(hashArgs);

    assert dynamicIds.size() == 1 : "There should be one column left untreated the one with the bloom filter";
    DynamicValue bloomDynamic = new DynamicValue(dynamicIds.poll(), TypeInfoFactory.binaryTypeInfo);
    sjPredicates.add(
        new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, new GenericUDFInBloomFilter(), "in_bloom_filter",
            Arrays.asList(hashExp, new ExprNodeDynamicValueDesc(bloomDynamic))));
    return and(sjPredicates);
  }

  /**
   * Creates the necessary information in order to execute the multi-column semi join and perform further optimizations.
   */
  private static RuntimeValuesInfo createRuntimeValuesInfo(ReduceSinkOperator rs, List<ReduceSinkOperator> sjBranches,
      ParseContext parseContext) {
    List<ExprNodeDesc> valueCols = rs.getConf().getValueCols();
    RuntimeValuesInfo info = new RuntimeValuesInfo();
    TableDesc rsFinalTableDesc =
        PlanUtils.getReduceValueTableDesc(PlanUtils.getFieldSchemasFromColumnList(valueCols, "_col"));
    List<String> dynamicValueIDs = new ArrayList<>();
    for (ExprNodeDesc rsCol : valueCols) {
      dynamicValueIDs.add(rs.toString() + rsCol.getExprString());
    }

    info.setTableDesc(rsFinalTableDesc);
    info.setDynamicValueIDs(dynamicValueIDs);
    info.setColExprs(valueCols);
    List<ExprNodeDesc> targetTableExpressions = new ArrayList<>();
    for (ReduceSinkOperator sjBranch : sjBranches) {
      RuntimeValuesInfo sjInfo = parseContext.getRsToRuntimeValuesInfoMap().get(sjBranch);
      checkState(sjInfo.getTargetColumns().size() == 1, "Cannot handle multi-column semijoin branches.");
      targetTableExpressions.add(sjInfo.getTargetColumns().get(0));
    }
    info.setTargetColumns(targetTableExpressions);
    return info;
  }

  /**
   * Merges multiple select operators in a single one appending an additional column that is the hash of all the others.
   *
   * <pre>
   * Input: SEL[fname], SEL[lname], SEL[age]
   * Output: SEL[fname, lname, age, hash(fname, lname, age)]
   * </pre>
   */
  private static SelectOperator mergeSelectOps(Operator<?> parent, List<SelectOperator> selectOperators) {
    List<String> colNames = new ArrayList<>();
    List<ExprNodeDesc> colDescs = new ArrayList<>();
    List<ColumnInfo> columnInfos = new ArrayList<>();
    Map<String, ExprNodeDesc> selectColumnExprMap = new HashMap<>();
    for (SelectOperator sel : selectOperators) {
      checkState(sel.getConf().getColList().size() == 1);
      ExprNodeDesc col = sel.getConf().getColList().get(0);
      String colName = HiveConf.getColumnInternalName(colDescs.size());
      colNames.add(colName);
      columnInfos.add(new ColumnInfo(colName, col.getTypeInfo(), "", false));
      colDescs.add(col);
      selectColumnExprMap.put(colName, col);
    }
    ExprNodeDesc hashExp = ExprNodeDescUtils.murmurHash(colDescs);
    String hashName = HiveConf.getColumnInternalName(colDescs.size() + 1);
    colNames.add(hashName);
    columnInfos.add(new ColumnInfo(hashName, hashExp.getTypeInfo(), "", false));
    // The n-1 columns in selDescs are used as parameters to min/max aggregations
    List<ExprNodeDesc> selDescs = new ArrayList<>(colDescs);
    // The nth column in selDescs is used as parameter to the bloom_filter aggregation
    selDescs.add(hashExp);

    SelectDesc select = new SelectDesc(selDescs, colNames);
    SelectOperator selectOp =
        (SelectOperator) OperatorFactory.getAndMakeChild(select, new RowSchema(columnInfos), parent);
    selectOp.setColumnExprMap(selectColumnExprMap);
    return selectOp;
  }

  /**
   * Creates a reduce sink operator that emits all columns of the parent as values.
   */
  private static ReduceSinkOperator createReduceSink(Operator<?> parentOp, NullOrdering nullOrder)
      throws SemanticException {
    List<ExprNodeDesc> valueCols = new ArrayList<>();
    RowSchema parentSchema = parentOp.getSchema();
    List<String> outColNames = new ArrayList<>();
    for (int i = 0; i < parentSchema.getSignature().size(); i++) {
      ColumnInfo colInfo = parentSchema.getSignature().get(i);
      ExprNodeColumnDesc colExpr = new ExprNodeColumnDesc(colInfo.getType(), colInfo.getInternalName(), "", false);
      valueCols.add(colExpr);
      outColNames.add(SemanticAnalyzer.getColumnInternalName(i));
    }

    ReduceSinkDesc rsDesc = PlanUtils
        .getReduceSinkDesc(Collections.emptyList(), valueCols, outColNames, false, -1, 0, 1,
            AcidUtils.Operation.NOT_ACID, nullOrder);
    rsDesc.setColumnExprMap(Collections.emptyMap());
    return (ReduceSinkOperator) OperatorFactory.getAndMakeChild(rsDesc, new RowSchema(parentSchema), parentOp);
  }

  /**
   * Creates a group by operator with min, max, and bloomFilter aggregations for every column of the parent.
   * <p>
   * The method generates two kind of group by operators for intermediate and final aggregations respectively.
   * Intermediate aggregations require the parent operator to be a select operator while final aggregations assume that
   * the parent is a reduce sink operator.
   * </p>
   * <p>
   * Intermediate group by example.
   * <pre>
   * Input: SEL[fname, lname, age, hash(fname,lname,age)]
   * Output: GBY[min(fname),max(fname),min(lname),max(lname),min(age),max(age),bloom(hash)]
   * </pre>
   * </p>
   * <p>
   * Final group by example.
   * <pre>
   * Input: RS[fname_min,fname_max,lname_min,lname_max,age_min,age_max, hash_bloom]
   * Output: GBY[min(fname_min),max(fname_max),min(lname_min),max(lname_max),min(age_min),max(age_max),bloom(hash_bloom)]
   * </pre>
   * </p>
   */
  private static GroupByOperator createGroupBy(SelectOperator selectOp, Operator<?> parentOp, GroupByDesc.Mode gbMode,
      long bloomEntriesHint, HiveConf hiveConf) {

    final List<ExprNodeDesc> params;
    final GenericUDAFEvaluator.Mode udafMode = SemanticAnalyzer.groupByDescModeToUDAFMode(gbMode, false);
    switch (gbMode) {
    case FINAL:
      params = createGroupByAggregationParameters((ReduceSinkOperator) parentOp);
      break;
    case HASH:
      params = createGroupByAggregationParameters(selectOp);
      break;
    default:
      throw new AssertionError(gbMode.toString() + " is not supported");
    }

    List<AggregationDesc> gbAggs = new ArrayList<>();
    Deque<ExprNodeDesc> paramsCopy = new ArrayDeque<>(params);
    while (paramsCopy.size() > 1) {
      gbAggs.add(minAggregation(udafMode, paramsCopy.poll()));
      gbAggs.add(maxAggregation(udafMode, paramsCopy.poll()));
    }
    gbAggs.add(bloomFilterAggregation(udafMode, paramsCopy.poll(), selectOp, bloomEntriesHint, hiveConf));
    assert paramsCopy.size() == 0;

    List<String> gbOutputNames = new ArrayList<>(gbAggs.size());
    List<ColumnInfo> gbColInfos = new ArrayList<>(gbAggs.size());
    for (int i = 0; i < params.size(); i++) {
      String colName = HiveConf.getColumnInternalName(i);
      gbOutputNames.add(colName);
      final TypeInfo colType;
      if (i == params.size() - 1) {
        colType = TypeInfoFactory.binaryTypeInfo; // Bloom type
      } else {
        colType = params.get(i).getTypeInfo(); // Min/Max type
      }
      gbColInfos.add(new ColumnInfo(colName, colType, "", false));
    }

    float groupByMemoryUsage = HiveConf.getFloatVar(hiveConf, HiveConf.ConfVars.HIVE_MAP_AGGR_HASH_MEMORY);
    float memoryThreshold = HiveConf.getFloatVar(hiveConf, HiveConf.ConfVars.HIVE_MAP_AGGR_MEMORY_THRESHOLD);
    float minReductionHashAggr = HiveConf.getFloatVar(hiveConf, HiveConf.ConfVars.HIVE_MAP_AGGR_HASH_MIN_REDUCTION);
    float minReductionHashAggrLowerBound = HiveConf.getFloatVar(hiveConf, HiveConf.ConfVars.HIVE_MAP_AGGR_HASH_MIN_REDUCTION_LOWER_BOUND);
    GroupByDesc groupBy =
        new GroupByDesc(gbMode, gbOutputNames, Collections.emptyList(), gbAggs, false, groupByMemoryUsage,
            memoryThreshold, minReductionHashAggr, minReductionHashAggrLowerBound, null, false, -1, false);
    groupBy.setColumnExprMap(Collections.emptyMap());
    return (GroupByOperator) OperatorFactory.getAndMakeChild(groupBy, new RowSchema(gbColInfos), parentOp);
  }

  private static List<ExprNodeDesc> createGroupByAggregationParameters(SelectOperator selectOp) {
    List<ExprNodeDesc> params = new ArrayList<>();
    // The first n-1 cols are used as parameters for min & max so we need two expressions
    for (ColumnInfo c : selectOp.getSchema().getSignature()) {
      String name = c.getInternalName();
      ExprNodeColumnDesc p = new ExprNodeColumnDesc(new ColumnInfo(name, c.getType(), "", false));
      params.add(p);
      params.add(p);
    }
    // The last col is used as parameter for bloom so we need only one expression
    params.remove(params.size() - 1);
    return params;
  }

  private static List<ExprNodeDesc> createGroupByAggregationParameters(ReduceSinkOperator reduceOp) {
    List<ExprNodeDesc> params = new ArrayList<>();
    // There is a 1-1 mapping between columns and parameters for the aggregation functions min, max, bloom
    for (ColumnInfo c : reduceOp.getSchema().getSignature()) {
      String name = Utilities.ReduceField.VALUE + "." + c.getInternalName();
      params.add(new ExprNodeColumnDesc(new ColumnInfo(name, c.getType(), "", false)));
    }
    return params;
  }

  private static AggregationDesc minAggregation(GenericUDAFEvaluator.Mode mode, ExprNodeDesc col) {
    List<ExprNodeDesc> p = Collections.singletonList(col);
    return new AggregationDesc("min", new GenericUDAFMin.GenericUDAFMinEvaluator(), p, false, mode);
  }

  private static AggregationDesc maxAggregation(GenericUDAFEvaluator.Mode mode, ExprNodeDesc col) {
    List<ExprNodeDesc> p = Collections.singletonList(col);
    return new AggregationDesc("max", new GenericUDAFMax.GenericUDAFMaxEvaluator(), p, false, mode);
  }

  private static AggregationDesc bloomFilterAggregation(GenericUDAFEvaluator.Mode mode, ExprNodeDesc col,
      SelectOperator source, long numEntriesHint, HiveConf conf) {
    GenericUDAFBloomFilterEvaluator bloomFilterEval = new GenericUDAFBloomFilterEvaluator();
    bloomFilterEval.setSourceOperator(source);
    bloomFilterEval.setMaxEntries(conf.getLongVar(HiveConf.ConfVars.TEZ_MAX_BLOOM_FILTER_ENTRIES));
    bloomFilterEval.setMinEntries(conf.getLongVar(HiveConf.ConfVars.TEZ_MIN_BLOOM_FILTER_ENTRIES));
    bloomFilterEval.setFactor(conf.getFloatVar(HiveConf.ConfVars.TEZ_BLOOM_FILTER_FACTOR));
    bloomFilterEval.setHintEntries(numEntriesHint);

    List<ExprNodeDesc> params;

    // numThreads is available only for VectorUDAFBloomFilterMerge, which only supports
    // these two modes, don't add numThreads otherwise
    switch(mode) {
      case PARTIAL2:
      case FINAL:
        int numThreads = conf.getIntVar(HiveConf.ConfVars.TEZ_BLOOM_FILTER_MERGE_THREADS);
        TypeInfo intTypeInfo = TypeInfoFactory.getPrimitiveTypeInfoFromJavaPrimitive(Integer.TYPE);
        params = Arrays.asList(col, new ExprNodeConstantDesc(intTypeInfo, numThreads));
        break;
    default:
      params = Collections.singletonList(col);
      break;
    }

    AggregationDesc bloom = new AggregationDesc(BLOOM_FILTER_FUNCTION, bloomFilterEval, params, false, mode);
    // It is necessary to set the bloom filter evaluator otherwise there are runtime failures see HIVE-24018
    bloom.setGenericUDAFWritableEvaluator(bloomFilterEval);
    return bloom;
  }

  /**
   * An object that represents the source and target of a semi-join reducer.
   *
   * The objects are meant to be used as keys in maps so it is required to implement hashCode and equals methods.
   */
  private static final class SJSourceTarget {
    private final Operator<?> source;
    private final TableScanOperator target;

    public SJSourceTarget(Operator<?> source, TableScanOperator target) {
      this.source = source;
      this.target = target;
    }

    @Override public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      SJSourceTarget that = (SJSourceTarget) o;

      if (!source.equals(that.source)) {
        return false;
      }
      return target.equals(that.target);
    }

    @Override public int hashCode() {
      int result = source.hashCode();
      result = 31 * result + target.hashCode();
      return result;
    }

  }
}
