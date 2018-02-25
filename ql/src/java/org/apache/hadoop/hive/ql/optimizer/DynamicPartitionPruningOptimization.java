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

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkPartitionPruningSinkDesc;
import org.apache.hadoop.hive.ql.parse.GenTezUtils;
import org.apache.hadoop.hive.ql.parse.GenTezUtils.DynamicListContext;
import org.apache.hadoop.hive.ql.parse.GenTezUtils.DynamicPartitionPrunerContext;
import org.apache.hadoop.hive.ql.parse.OptimizeTezProcContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.RuntimeValuesInfo;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.SemiJoinBranchInfo;
import org.apache.hadoop.hive.ql.parse.SemiJoinHint;
import org.apache.hadoop.hive.ql.parse.spark.OptimizeSparkProcContext;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.DynamicPruningEventDesc;
import org.apache.hadoop.hive.ql.plan.DynamicValue;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicValueDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBloomFilter.GenericUDAFBloomFilterEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * This optimization looks for expressions of the kind "x IN (RS[n])". If such
 * an expression made it to a table scan operator and x is a partition column we
 * can use an existing join to dynamically prune partitions. This class sets up
 * the infrastructure for that.
 */
public class DynamicPartitionPruningOptimization implements NodeProcessor {

  static final private Logger LOG = LoggerFactory.getLogger(DynamicPartitionPruningOptimization.class
      .getName());

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
      throws SemanticException {
    ParseContext parseContext;
    if (procCtx instanceof OptimizeTezProcContext) {
      parseContext = ((OptimizeTezProcContext) procCtx).parseContext;
    } else if (procCtx instanceof OptimizeSparkProcContext) {
      parseContext = ((OptimizeSparkProcContext) procCtx).getParseContext();
    } else {
      throw new IllegalArgumentException("expected parseContext to be either " +
          "OptimizeTezProcContext or OptimizeSparkProcContext, but found " +
          procCtx.getClass().getName());
    }

    FilterOperator filter = (FilterOperator) nd;
    FilterDesc desc = filter.getConf();

    if (!parseContext.getConf().getBoolVar(ConfVars.TEZ_DYNAMIC_PARTITION_PRUNING) &&
        !parseContext.getConf().isSparkDPPAny()) {
      // nothing to do when the optimization is off
      return null;
    }

    TableScanOperator ts = null;

    if (filter.getParentOperators().size() == 1
        && filter.getParentOperators().get(0) instanceof TableScanOperator) {
      ts = (TableScanOperator) filter.getParentOperators().get(0);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Parent: " + filter.getParentOperators().get(0));
      LOG.debug("Filter: " + desc.getPredicateString());
      LOG.debug("TableScan: " + ts);
    }

    DynamicPartitionPrunerContext removerContext = new DynamicPartitionPrunerContext();

    // collect the dynamic pruning conditions
    removerContext.dynLists.clear();
    GenTezUtils.collectDynamicPruningConditions(desc.getPredicate(), removerContext);

    if (ts == null) {
      // Replace the synthetic predicate with true and bail out
      for (DynamicListContext ctx : removerContext) {
        ExprNodeDesc constNode =
                new ExprNodeConstantDesc(ctx.parent.getTypeInfo(), true);
        replaceExprNode(ctx, desc, constNode);
      }
      return false;
    }

    boolean semiJoin = parseContext.getConf().getBoolVar(ConfVars.TEZ_DYNAMIC_SEMIJOIN_REDUCTION);
    if (HiveConf.getVar(parseContext.getConf(), HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")) {
      //TODO HIVE-16862: Implement a similar feature like "hive.tez.dynamic.semijoin.reduction" in hive on spark
      semiJoin = false;
    }

    for (DynamicListContext ctx : removerContext) {
      String column = ExprNodeDescUtils.extractColName(ctx.parent);
      boolean semiJoinAttempted = false;

      if (column != null) {
        // Need unique IDs to refer to each min/max key value in the DynamicValueRegistry
        String keyBaseAlias = "";

        Table table = ts.getConf().getTableMetadata();

        if (table != null && table.isPartitionKey(column)) {
          String columnType = table.getPartColByName(column).getType();
          String alias = ts.getConf().getAlias();
          PrunedPartitionList plist = parseContext.getPrunedPartitions(alias, ts);
          if (LOG.isDebugEnabled()) {
            LOG.debug("alias: " + alias);
            LOG.debug("pruned partition list: ");
            if (plist != null) {
              for (Partition p : plist.getPartitions()) {
                LOG.debug(p.getCompleteName());
              }
            }
          }
          // If partKey is a constant, we can check whether the partitions
          // have been already filtered
          if (plist == null || plist.getPartitions().size() != 0) {
            LOG.info("Dynamic partitioning: " + table.getCompleteName() + "." + column);
            generateEventOperatorPlan(ctx, parseContext, ts, column, columnType);
          } else {
            // all partitions have been statically removed
            LOG.debug("No partition pruning necessary.");
          }
        } else {
          LOG.debug("Column " + column + " is not a partition column");
          if (semiJoin && ts.getConf().getFilterExpr() != null) {
            LOG.debug("Initiate semijoin reduction for " + column + " ("
                + ts.getConf().getFilterExpr().getExprString());

            StringBuilder internalColNameBuilder = new StringBuilder();
            StringBuilder colNameBuilder = new StringBuilder();

            // Apply best effort to fetch the correct table alias. If not
            // found, fallback to old logic.
            StringBuilder tabAliasBuilder = new StringBuilder();
            if (getColumnInfo(ctx, internalColNameBuilder, colNameBuilder, tabAliasBuilder)) {
              String colName = colNameBuilder.toString();
              String tableAlias;
              if (tabAliasBuilder.length() > 0) {
                tableAlias = tabAliasBuilder.toString();
              } else {
                //falling back
                Operator<?> op = ctx.generator;

                while (!(op == null || op instanceof TableScanOperator)) {
                  op = op.getParentOperators().get(0);
                }
                tableAlias = (op == null ? "" : ((TableScanOperator) op).
                        getConf().getAlias());
              }

              // Use the tableAlias to generate keyBaseAlias
              keyBaseAlias = ctx.generator.getOperatorId() + "_" + tableAlias
                      + "_" + colName;
              Map<String, List<SemiJoinHint>> hints = parseContext.getSemiJoinHints();
              if (hints != null) {
                // Create semijoin optimizations ONLY for hinted columns
                semiJoinAttempted = processSemiJoinHints(
                        parseContext, ctx, hints, tableAlias,
                        internalColNameBuilder.toString(), colName, ts,
                        keyBaseAlias);
              } else {
                // fallback to regular logic
                semiJoinAttempted = generateSemiJoinOperatorPlan(
                        ctx, parseContext, ts, keyBaseAlias,
                        internalColNameBuilder.toString(), colName, null);
              }
            }
          }
        }

        // If semijoin is attempted then replace the condition with a min-max filter
        // and bloom filter else,
        // we always remove the condition by replacing it with "true"
        if (semiJoinAttempted) {
          List<ExprNodeDesc> betweenArgs = new ArrayList<ExprNodeDesc>();
          betweenArgs.add(new ExprNodeConstantDesc(Boolean.FALSE)); // Do not invert between result
          // add column expression here
          betweenArgs.add(ctx.parent.getChildren().get(0));
          betweenArgs.add(new ExprNodeDynamicValueDesc(new DynamicValue(keyBaseAlias + "_min", ctx.desc.getTypeInfo())));
          betweenArgs.add(new ExprNodeDynamicValueDesc(new DynamicValue(keyBaseAlias + "_max", ctx.desc.getTypeInfo())));
          ExprNodeDesc betweenNode = ExprNodeGenericFuncDesc.newInstance(
                  FunctionRegistry.getFunctionInfo("between").getGenericUDF(), betweenArgs);
          // add column expression for bloom filter
          List<ExprNodeDesc> bloomFilterArgs = new ArrayList<ExprNodeDesc>();
          bloomFilterArgs.add(ctx.parent.getChildren().get(0));
          bloomFilterArgs.add(new ExprNodeDynamicValueDesc(
                  new DynamicValue(keyBaseAlias + "_bloom_filter",
                          TypeInfoFactory.binaryTypeInfo)));
          ExprNodeDesc bloomFilterNode = ExprNodeGenericFuncDesc.newInstance(
                  FunctionRegistry.getFunctionInfo("in_bloom_filter").
                          getGenericUDF(), bloomFilterArgs);
          List<ExprNodeDesc> andArgs = new ArrayList<ExprNodeDesc>();
          andArgs.add(betweenNode);
          andArgs.add(bloomFilterNode);
          ExprNodeDesc andExpr = ExprNodeGenericFuncDesc.newInstance(
              FunctionRegistry.getFunctionInfo("and").getGenericUDF(), andArgs);
          replaceExprNode(ctx, desc, andExpr);
        } else {
          ExprNodeDesc replaceNode = new ExprNodeConstantDesc(ctx.parent.getTypeInfo(), true);
          replaceExprNode(ctx, desc, replaceNode);
        }
      } else {
        ExprNodeDesc constNode =
                new ExprNodeConstantDesc(ctx.parent.getTypeInfo(), true);
        replaceExprNode(ctx, desc, constNode);
      }
    }
    // if we pushed the predicate into the table scan we need to remove the
    // synthetic conditions there.
    cleanTableScanFilters(ts);

    return false;
  }

  // Given a key, find the corresponding column name.
  private boolean getColumnInfo(DynamicListContext ctx, StringBuilder internalColName,
                                StringBuilder colName, StringBuilder tabAlias) {
    ExprNodeDesc exprNodeDesc = ctx.generator.getConf().getKeyCols().get(ctx.desc.getKeyIndex());
    ExprNodeColumnDesc colExpr = ExprNodeDescUtils.getColumnExpr(exprNodeDesc);

    if (colExpr == null) {
      return false;
    }
    internalColName.append(colExpr.getColumn());

    // fetch table ablias
    ExprNodeDescUtils.ColumnOrigin columnOrigin =
            ExprNodeDescUtils.findColumnOrigin(exprNodeDesc, ctx.generator);

    if (columnOrigin != null) {
      // get both tableAlias and column name from columnOrigin
      assert columnOrigin.op instanceof TableScanOperator;
      TableScanOperator ts = (TableScanOperator) columnOrigin.op;
      tabAlias.append(ts.getConf().getAlias());
      colName.append(
              ExprNodeDescUtils.getColumnExpr(columnOrigin.col).getColumn());
      return true;
    }

    Operator<? extends OperatorDesc> parentOfRS = ctx.generator.getParentOperators().get(0);
    if (!(parentOfRS instanceof SelectOperator)) {
      colName.append(internalColName.toString());
      return true;
    }

    exprNodeDesc = parentOfRS.getColumnExprMap().get(internalColName.toString());
    colExpr = ExprNodeDescUtils.getColumnExpr(exprNodeDesc);

    if (colExpr == null) {
      return false;
    }

    colName.append(ExprNodeDescUtils.extractColName(colExpr));
    return true;
  }

  // Handle hint based semijoin
  private boolean processSemiJoinHints(
          ParseContext pCtx, DynamicListContext ctx,
          Map<String, List<SemiJoinHint>> hints, String tableAlias,
          String internalColName, String colName, TableScanOperator ts,
          String keyBaseAlias) throws SemanticException {
    if (hints.size() == 0) {
      return false;
    }

    List<SemiJoinHint> hintList = hints.get(tableAlias);
    if (hintList == null) {
      return false;
    }

    // Iterate through the list
    for (SemiJoinHint sjHint : hintList) {
      if (!colName.equals(sjHint.getColName())) {
        continue;
      }
      if (!ts.getConf().getAlias().equals(sjHint.getTarget())) {
        continue;
      }

      // match!
      LOG.info("Creating runtime filter due to user hint: column = " + colName);
      if (generateSemiJoinOperatorPlan(ctx, pCtx, ts, keyBaseAlias,
              internalColName, colName, sjHint)) {
        return true;
      }
      throw new SemanticException("The user hint to enforce semijoin failed required conditions");
    }
    return false;
  }

  private void replaceExprNode(DynamicListContext ctx, FilterDesc desc, ExprNodeDesc node) {
    if (ctx.grandParent == null) {
      desc.setPredicate(node);
    } else {
      int i = ctx.grandParent.getChildren().indexOf(ctx.parent);
      ctx.grandParent.getChildren().remove(i);
      ctx.grandParent.getChildren().add(i, node);
    }
  }

  private void cleanTableScanFilters(TableScanOperator ts) throws SemanticException {

    if (ts == null || ts.getConf() == null || ts.getConf().getFilterExpr() == null) {
      // nothing to do
      return;
    }

    DynamicPartitionPrunerContext removerContext = new DynamicPartitionPrunerContext();

    // collect the dynamic pruning conditions
    removerContext.dynLists.clear();
    GenTezUtils.collectDynamicPruningConditions(ts.getConf().getFilterExpr(), removerContext);

    for (DynamicListContext ctx : removerContext) {
      // remove the condition by replacing it with "true"
      ExprNodeDesc constNode = new ExprNodeConstantDesc(ctx.parent.getTypeInfo(), true);
      if (ctx.grandParent == null) {
        // we're the only node, just clear out the expression
        ts.getConf().setFilterExpr(null);
      } else {
        int i = ctx.grandParent.getChildren().indexOf(ctx.parent);
        ctx.grandParent.getChildren().remove(i);
        ctx.grandParent.getChildren().add(i, constNode);
      }
    }
  }

  private void generateEventOperatorPlan(DynamicListContext ctx, ParseContext parseContext,
      TableScanOperator ts, String column, String columnType) {

    // we will put a fork in the plan at the source of the reduce sink
    Operator<? extends OperatorDesc> parentOfRS = ctx.generator.getParentOperators().get(0);

    // we need the expr that generated the key of the reduce sink
    ExprNodeDesc key = ctx.generator.getConf().getKeyCols().get(ctx.desc.getKeyIndex());

    // we also need the expr for the partitioned table
    ExprNodeDesc partKey = ctx.parent.getChildren().get(0);

    if (LOG.isDebugEnabled()) {
      LOG.debug("key expr: " + key);
      LOG.debug("partition key expr: " + partKey);
    }

    List<ExprNodeDesc> keyExprs = new ArrayList<ExprNodeDesc>();
    keyExprs.add(key);

    // group by requires "ArrayList", don't ask.
    ArrayList<String> outputNames = new ArrayList<String>();
    outputNames.add(HiveConf.getColumnInternalName(0));

    // project the relevant key column
    SelectDesc select = new SelectDesc(keyExprs, outputNames);
    SelectOperator selectOp =
        (SelectOperator) OperatorFactory.getAndMakeChild(select, parentOfRS);

    // do a group by on the list to dedup
    float groupByMemoryUsage =
        HiveConf.getFloatVar(parseContext.getConf(), HiveConf.ConfVars.HIVEMAPAGGRHASHMEMORY);
    float memoryThreshold =
        HiveConf.getFloatVar(parseContext.getConf(),
            HiveConf.ConfVars.HIVEMAPAGGRMEMORYTHRESHOLD);

    ArrayList<ExprNodeDesc> groupByExprs = new ArrayList<ExprNodeDesc>();
    ExprNodeDesc groupByExpr =
        new ExprNodeColumnDesc(key.getTypeInfo(), outputNames.get(0), null, false);
    groupByExprs.add(groupByExpr);

    GroupByDesc groupBy =
        new GroupByDesc(GroupByDesc.Mode.HASH, outputNames, groupByExprs,
            new ArrayList<AggregationDesc>(), false, groupByMemoryUsage, memoryThreshold,
            null, false, 0, true);

    GroupByOperator groupByOp = (GroupByOperator) OperatorFactory.getAndMakeChild(
        groupBy, selectOp);

    Map<String, ExprNodeDesc> colMap = new HashMap<String, ExprNodeDesc>();
    colMap.put(outputNames.get(0), groupByExpr);
    groupByOp.setColumnExprMap(colMap);

    // finally add the event broadcast operator
    if (HiveConf.getVar(parseContext.getConf(),
        ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
      DynamicPruningEventDesc eventDesc = new DynamicPruningEventDesc();
      eventDesc.setTableScan(ts);
      eventDesc.setGenerator(ctx.generator);
      eventDesc.setTable(PlanUtils.getReduceValueTableDesc(PlanUtils
          .getFieldSchemasFromColumnList(keyExprs, "key")));
      eventDesc.setTargetColumnName(column);
      eventDesc.setTargetColumnType(columnType);
      eventDesc.setPartKey(partKey);
      OperatorFactory.getAndMakeChild(eventDesc, groupByOp);
    } else {
      // Must be spark branch
      SparkPartitionPruningSinkDesc desc = new SparkPartitionPruningSinkDesc();
      desc.setTableScan(ts);
      desc.setTable(PlanUtils.getReduceValueTableDesc(PlanUtils
          .getFieldSchemasFromColumnList(keyExprs, "key")));
      desc.addTarget(column, columnType, partKey, null);
      OperatorFactory.getAndMakeChild(desc, groupByOp);
    }
  }

  // Generates plan for min/max when dynamic partition pruning is ruled out.
  private boolean generateSemiJoinOperatorPlan(DynamicListContext ctx, ParseContext parseContext,
      TableScanOperator ts, String keyBaseAlias, String internalColName,
      String colName, SemiJoinHint sjHint) throws SemanticException {

    // we will put a fork in the plan at the source of the reduce sink
    Operator<? extends OperatorDesc> parentOfRS = ctx.generator.getParentOperators().get(0);

    // we need the expr that generated the key of the reduce sink
    ExprNodeDesc key = ctx.generator.getConf().getKeyCols().get(ctx.desc.getKeyIndex());

    assert colName != null;
    // Fetch the TableScan Operator.
    Operator<?> op = parentOfRS;
    while (!(op == null || op instanceof TableScanOperator ||
             op instanceof ReduceSinkOperator)) {
      op = op.getParentOperators().get(0);
    }
    Preconditions.checkNotNull(op);

    if (op instanceof TableScanOperator) {
      Table table = ((TableScanOperator) op).getConf().getTableMetadata();
      if (table.isPartitionKey(colName)) {
        // The column is partition column, skip the optimization.
        return false;
      }
    }

    // Check if there already exists a semijoin branch
    GroupByOperator gb = parseContext.getColExprToGBMap().get(key);
    if (gb != null) {
      // Already an existing semijoin branch, reuse it
      createFinalRsForSemiJoinOp(parseContext, ts, gb, key, keyBaseAlias,
              ctx.parent.getChildren().get(0), sjHint != null);
      // done!
      return true;
    }

    List<ExprNodeDesc> keyExprs = new ArrayList<ExprNodeDesc>();
    keyExprs.add(key);

    // group by requires "ArrayList", don't ask.
    ArrayList<String> outputNames = new ArrayList<String>();
    outputNames.add(HiveConf.getColumnInternalName(0));

    // project the relevant key column
    SelectDesc select = new SelectDesc(keyExprs, outputNames);

    // Create the new RowSchema for the projected column
    ColumnInfo columnInfo = parentOfRS.getSchema().getColumnInfo(internalColName);
    ArrayList<ColumnInfo> signature = new ArrayList<ColumnInfo>();
    signature.add(columnInfo);
    RowSchema rowSchema = new RowSchema(signature);

    // Create the column expr map
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    ExprNodeDesc exprNode = null;
    if ( parentOfRS.getColumnExprMap() != null) {
      exprNode = parentOfRS.getColumnExprMap().get(internalColName).clone();
    } else {
      exprNode = new ExprNodeColumnDesc(columnInfo);
    }

    if (exprNode instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc encd = (ExprNodeColumnDesc) exprNode;
      encd.setColumn(internalColName);
    }
    colExprMap.put(internalColName, exprNode);

    // Create the Select Operator
    SelectOperator selectOp =
            (SelectOperator) OperatorFactory.getAndMakeChild(select,
                    rowSchema, colExprMap, parentOfRS);

    // do a group by to aggregate min,max and bloom filter.
    float groupByMemoryUsage =
            HiveConf.getFloatVar(parseContext.getConf(), HiveConf.ConfVars.HIVEMAPAGGRHASHMEMORY);
    float memoryThreshold =
            HiveConf.getFloatVar(parseContext.getConf(),
                    HiveConf.ConfVars.HIVEMAPAGGRMEMORYTHRESHOLD);

    // Add min/max and bloom filter aggregations
    List<ObjectInspector> aggFnOIs = new ArrayList<ObjectInspector>();
    aggFnOIs.add(key.getWritableObjectInspector());
    ArrayList<ExprNodeDesc> params = new ArrayList<ExprNodeDesc>();
    params.add(
            new ExprNodeColumnDesc(key.getTypeInfo(), outputNames.get(0),
                    "", false));

    ArrayList<AggregationDesc> aggs = new ArrayList<AggregationDesc>();
    try {
      AggregationDesc min = new AggregationDesc("min",
              FunctionRegistry.getGenericUDAFEvaluator("min", aggFnOIs, false, false),
              params, false, Mode.PARTIAL1);
      AggregationDesc max = new AggregationDesc("max",
              FunctionRegistry.getGenericUDAFEvaluator("max", aggFnOIs, false, false),
              params, false, Mode.PARTIAL1);
      AggregationDesc bloomFilter = new AggregationDesc("bloom_filter",
              FunctionRegistry.getGenericUDAFEvaluator("bloom_filter", aggFnOIs, false, false),
              params, false, Mode.PARTIAL1);
      GenericUDAFBloomFilterEvaluator bloomFilterEval =
          (GenericUDAFBloomFilterEvaluator) bloomFilter.getGenericUDAFEvaluator();
      bloomFilterEval.setSourceOperator(selectOp);

      if (sjHint != null && sjHint.getNumEntries() > 0) {
        LOG.debug("Setting size for " + keyBaseAlias + " to " + sjHint.getNumEntries() + " based on the hint");
        bloomFilterEval.setHintEntries(sjHint.getNumEntries());
      }
      bloomFilterEval.setMaxEntries(parseContext.getConf().getLongVar(ConfVars.TEZ_MAX_BLOOM_FILTER_ENTRIES));
      bloomFilterEval.setMinEntries(parseContext.getConf().getLongVar(ConfVars.TEZ_MIN_BLOOM_FILTER_ENTRIES));
      bloomFilterEval.setFactor(parseContext.getConf().getFloatVar(ConfVars.TEZ_BLOOM_FILTER_FACTOR));
      bloomFilter.setGenericUDAFWritableEvaluator(bloomFilterEval);
      aggs.add(min);
      aggs.add(max);
      aggs.add(bloomFilter);
    } catch (SemanticException e) {
      LOG.error("Error creating min/max aggregations on key", e);
      throw new IllegalStateException("Error creating min/max aggregations on key", e);
    }

    // Create the Group by Operator
    ArrayList<String> gbOutputNames = new ArrayList<String>();
    gbOutputNames.add(SemanticAnalyzer.getColumnInternalName(0));
    gbOutputNames.add(SemanticAnalyzer.getColumnInternalName(1));
    gbOutputNames.add(SemanticAnalyzer.getColumnInternalName(2));
    GroupByDesc groupBy = new GroupByDesc(GroupByDesc.Mode.HASH,
            gbOutputNames, new ArrayList<ExprNodeDesc>(), aggs, false,
            groupByMemoryUsage, memoryThreshold, null, false, 0, false);

    ArrayList<ColumnInfo> groupbyColInfos = new ArrayList<ColumnInfo>();
    groupbyColInfos.add(new ColumnInfo(gbOutputNames.get(0), key.getTypeInfo(), "", false));
    groupbyColInfos.add(new ColumnInfo(gbOutputNames.get(1), key.getTypeInfo(), "", false));
    groupbyColInfos.add(new ColumnInfo(gbOutputNames.get(2), key.getTypeInfo(), "", false));

    GroupByOperator groupByOp = (GroupByOperator)OperatorFactory.getAndMakeChild(
            groupBy, new RowSchema(groupbyColInfos), selectOp);

    groupByOp.setColumnExprMap(new HashMap<String, ExprNodeDesc>());

    // Get the column names of the aggregations for reduce sink
    int colPos = 0;
    ArrayList<ExprNodeDesc> rsValueCols = new ArrayList<ExprNodeDesc>();
    for (int i = 0; i < aggs.size() - 1; i++) {
      ExprNodeColumnDesc colExpr = new ExprNodeColumnDesc(key.getTypeInfo(),
              gbOutputNames.get(colPos++), "", false);
      rsValueCols.add(colExpr);
    }

    // Bloom Filter uses binary
    ExprNodeColumnDesc colExpr = new ExprNodeColumnDesc(TypeInfoFactory.binaryTypeInfo,
            gbOutputNames.get(colPos++), "", false);
    rsValueCols.add(colExpr);

    // Create the reduce sink operator
    ReduceSinkDesc rsDesc = PlanUtils.getReduceSinkDesc(
            new ArrayList<ExprNodeDesc>(), rsValueCols, gbOutputNames, false,
            -1, 0, 1, Operation.NOT_ACID);
    ReduceSinkOperator rsOp = (ReduceSinkOperator)OperatorFactory.getAndMakeChild(
            rsDesc, new RowSchema(groupByOp.getSchema()), groupByOp);
    Map<String, ExprNodeDesc> columnExprMap = new HashMap<String, ExprNodeDesc>();
    rsOp.setColumnExprMap(columnExprMap);

    rsOp.getConf().setReducerTraits(EnumSet.of(ReduceSinkDesc.ReducerTraits.QUICKSTART));

    // Create the final Group By Operator
    ArrayList<AggregationDesc> aggsFinal = new ArrayList<AggregationDesc>();
    try {
      List<ObjectInspector> minFinalFnOIs = new ArrayList<ObjectInspector>();
      List<ObjectInspector> maxFinalFnOIs = new ArrayList<ObjectInspector>();
      List<ObjectInspector> bloomFilterFinalFnOIs = new ArrayList<ObjectInspector>();
      ArrayList<ExprNodeDesc> minFinalParams = new ArrayList<ExprNodeDesc>();
      ArrayList<ExprNodeDesc> maxFinalParams = new ArrayList<ExprNodeDesc>();
      ArrayList<ExprNodeDesc> bloomFilterFinalParams = new ArrayList<ExprNodeDesc>();
      // Use the expressions from Reduce Sink.
      minFinalFnOIs.add(rsValueCols.get(0).getWritableObjectInspector());
      maxFinalFnOIs.add(rsValueCols.get(1).getWritableObjectInspector());
      bloomFilterFinalFnOIs.add(rsValueCols.get(2).getWritableObjectInspector());
      // Coming from a ReduceSink the aggregations would be in the form VALUE._col0, VALUE._col1
      minFinalParams.add(
              new ExprNodeColumnDesc(
                      rsValueCols.get(0).getTypeInfo(),
                      Utilities.ReduceField.VALUE + "." +
                              gbOutputNames.get(0), "", false));
      maxFinalParams.add(
              new ExprNodeColumnDesc(
                      rsValueCols.get(1).getTypeInfo(),
                      Utilities.ReduceField.VALUE + "." +
                              gbOutputNames.get(1), "", false));
      bloomFilterFinalParams.add(
              new ExprNodeColumnDesc(
                      rsValueCols.get(2).getTypeInfo(),
                      Utilities.ReduceField.VALUE + "." +
                              gbOutputNames.get(2), "", false));

      AggregationDesc min = new AggregationDesc("min",
              FunctionRegistry.getGenericUDAFEvaluator("min", minFinalFnOIs,
                      false, false),
              minFinalParams, false, Mode.FINAL);
      AggregationDesc max = new AggregationDesc("max",
              FunctionRegistry.getGenericUDAFEvaluator("max", maxFinalFnOIs,
                      false, false),
              maxFinalParams, false, Mode.FINAL);
      AggregationDesc bloomFilter = new AggregationDesc("bloom_filter",
              FunctionRegistry.getGenericUDAFEvaluator("bloom_filter", bloomFilterFinalFnOIs,
                      false, false),
              bloomFilterFinalParams, false, Mode.FINAL);
      GenericUDAFBloomFilterEvaluator bloomFilterEval = (GenericUDAFBloomFilterEvaluator) bloomFilter.getGenericUDAFEvaluator();
      bloomFilterEval.setSourceOperator(selectOp);
      if (sjHint != null && sjHint.getNumEntries() > 0) {
        bloomFilterEval.setHintEntries(sjHint.getNumEntries());
      }
      bloomFilterEval.setMaxEntries(parseContext.getConf().getLongVar(ConfVars.TEZ_MAX_BLOOM_FILTER_ENTRIES));
      bloomFilterEval.setMinEntries(parseContext.getConf().getLongVar(ConfVars.TEZ_MIN_BLOOM_FILTER_ENTRIES));
      bloomFilterEval.setFactor(parseContext.getConf().getFloatVar(ConfVars.TEZ_BLOOM_FILTER_FACTOR));
      bloomFilter.setGenericUDAFWritableEvaluator(bloomFilterEval);

      aggsFinal.add(min);
      aggsFinal.add(max);
      aggsFinal.add(bloomFilter);
    } catch (SemanticException e) {
      LOG.error("Error creating min/max aggregations on key", e);
      throw new IllegalStateException("Error creating min/max aggregations on key", e);
    }

    GroupByDesc groupByDescFinal = new GroupByDesc(GroupByDesc.Mode.FINAL,
            gbOutputNames, new ArrayList<ExprNodeDesc>(), aggsFinal, false,
            groupByMemoryUsage, memoryThreshold, null, false, 0, false);
    GroupByOperator groupByOpFinal = (GroupByOperator)OperatorFactory.getAndMakeChild(
            groupByDescFinal, new RowSchema(rsOp.getSchema()), rsOp);
    groupByOpFinal.setColumnExprMap(new HashMap<String, ExprNodeDesc>());

    createFinalRsForSemiJoinOp(parseContext, ts, groupByOpFinal, key,
            keyBaseAlias, ctx.parent.getChildren().get(0), sjHint != null);

    return true;
  }

  private void createFinalRsForSemiJoinOp(
          ParseContext parseContext, TableScanOperator ts, GroupByOperator gb,
          ExprNodeDesc key, String keyBaseAlias, ExprNodeDesc colExpr,
          boolean isHint) throws SemanticException {
    ArrayList<String> gbOutputNames = new ArrayList<>();
    // One each for min, max and bloom filter
    gbOutputNames.add(SemanticAnalyzer.getColumnInternalName(0));
    gbOutputNames.add(SemanticAnalyzer.getColumnInternalName(1));
    gbOutputNames.add(SemanticAnalyzer.getColumnInternalName(2));

    int colPos = 0;
    ArrayList<ExprNodeDesc> rsValueCols = new ArrayList<ExprNodeDesc>();
    for (int i = 0; i < gbOutputNames.size() - 1; i++) {
      ExprNodeColumnDesc expr = new ExprNodeColumnDesc(key.getTypeInfo(),
              gbOutputNames.get(colPos++), "", false);
      rsValueCols.add(expr);
    }

    // Bloom Filter uses binary
    ExprNodeColumnDesc colBFExpr = new ExprNodeColumnDesc(TypeInfoFactory.binaryTypeInfo,
            gbOutputNames.get(colPos++), "", false);
    rsValueCols.add(colBFExpr);

    // Create the final Reduce Sink Operator
    ReduceSinkDesc rsDescFinal = PlanUtils.getReduceSinkDesc(
            new ArrayList<ExprNodeDesc>(), rsValueCols, gbOutputNames, false,
            -1, 0, 1, Operation.NOT_ACID);
    ReduceSinkOperator rsOpFinal = (ReduceSinkOperator)OperatorFactory.getAndMakeChild(
            rsDescFinal, new RowSchema(gb.getSchema()), gb);
    Map<String, ExprNodeDesc> columnExprMap = new HashMap<>();
    rsOpFinal.setColumnExprMap(columnExprMap);

    LOG.debug("DynamicSemiJoinPushdown: Saving RS to TS mapping: " + rsOpFinal + ": " + ts);
    SemiJoinBranchInfo sjInfo = new SemiJoinBranchInfo(ts, isHint);
    parseContext.getRsToSemiJoinBranchInfo().put(rsOpFinal, sjInfo);

    // Save the info that is required at query time to resolve dynamic/runtime values.
    RuntimeValuesInfo runtimeValuesInfo = new RuntimeValuesInfo();
    TableDesc rsFinalTableDesc = PlanUtils.getReduceValueTableDesc(
            PlanUtils.getFieldSchemasFromColumnList(rsValueCols, "_col"));
    List<String> dynamicValueIDs = new ArrayList<String>();
    dynamicValueIDs.add(keyBaseAlias + "_min");
    dynamicValueIDs.add(keyBaseAlias + "_max");
    dynamicValueIDs.add(keyBaseAlias + "_bloom_filter");

    runtimeValuesInfo.setTableDesc(rsFinalTableDesc);
    runtimeValuesInfo.setDynamicValueIDs(dynamicValueIDs);
    runtimeValuesInfo.setColExprs(rsValueCols);
    runtimeValuesInfo.setTsColExpr(colExpr);
    parseContext.getRsToRuntimeValuesInfoMap().put(rsOpFinal, runtimeValuesInfo);
    parseContext.getColExprToGBMap().put(key, gb);
  }

}
