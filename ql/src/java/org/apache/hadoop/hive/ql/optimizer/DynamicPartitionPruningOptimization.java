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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkPartitionPruningSinkDesc;
import org.apache.hadoop.hive.ql.parse.OptimizeTezProcContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.RuntimeValuesInfo;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.spark.OptimizeSparkProcContext;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFBloomFilter.GenericUDAFBloomFilterEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This optimization looks for expressions of the kind "x IN (RS[n])". If such
 * an expression made it to a table scan operator and x is a partition column we
 * can use an existing join to dynamically prune partitions. This class sets up
 * the infrastructure for that.
 */
public class DynamicPartitionPruningOptimization implements NodeProcessor {

  static final private Logger LOG = LoggerFactory.getLogger(DynamicPartitionPruningOptimization.class
      .getName());

  private static class DynamicPartitionPrunerProc implements NodeProcessor {

    /**
     * process simply remembers all the dynamic partition pruning expressions
     * found
     */
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprNodeDynamicListDesc desc = (ExprNodeDynamicListDesc) nd;
      DynamicPartitionPrunerContext context = (DynamicPartitionPrunerContext) procCtx;

      // Rule is searching for dynamic pruning expr. There's at least an IN
      // expression wrapping it.
      ExprNodeDesc parent = (ExprNodeDesc) stack.get(stack.size() - 2);
      ExprNodeDesc grandParent = stack.size() >= 3 ? (ExprNodeDesc) stack.get(stack.size() - 3) : null;

      context.addDynamicList(desc, parent, grandParent, (ReduceSinkOperator) desc.getSource());

      return context;
    }
  }

  private static class DynamicListContext {
    public ExprNodeDynamicListDesc desc;
    public ExprNodeDesc parent;
    public ExprNodeDesc grandParent;
    public ReduceSinkOperator generator;

    public DynamicListContext(ExprNodeDynamicListDesc desc, ExprNodeDesc parent,
        ExprNodeDesc grandParent, ReduceSinkOperator generator) {
      this.desc = desc;
      this.parent = parent;
      this.grandParent = grandParent;
      this.generator = generator;
    }
  }

  private static class DynamicPartitionPrunerContext implements NodeProcessorCtx,
      Iterable<DynamicListContext> {
    public List<DynamicListContext> dynLists = new ArrayList<DynamicListContext>();

    public void addDynamicList(ExprNodeDynamicListDesc desc, ExprNodeDesc parent,
        ExprNodeDesc grandParent, ReduceSinkOperator generator) {
      dynLists.add(new DynamicListContext(desc, parent, grandParent, generator));
    }

    @Override
    public Iterator<DynamicListContext> iterator() {
      return dynLists.iterator();
    }
  }

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
        !parseContext.getConf().getBoolVar(ConfVars.SPARK_DYNAMIC_PARTITION_PRUNING)) {
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
    collectDynamicPruningConditions(desc.getPredicate(), removerContext);

    if (ts == null) {
      // Replace the synthetic predicate with true and bail out
      for (DynamicListContext ctx : removerContext) {
        ExprNodeDesc constNode =
                new ExprNodeConstantDesc(ctx.parent.getTypeInfo(), true);
        replaceExprNode(ctx, desc, constNode);
      }
      return false;
    }

    final boolean semiJoin = parseContext.getConf().getBoolVar(ConfVars.TEZ_DYNAMIC_SEMIJOIN_REDUCTION);

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
            LOG.debug("Initiate semijoin reduction for " + column);
            // Get the table name from which the min-max values will come.
            Operator<?> op = ctx.generator;
            while (!(op == null || op instanceof TableScanOperator)) {
              op = op.getParentOperators().get(0);
            }
            String tableAlias = (op == null ? "" : ((TableScanOperator) op).getConf().getAlias());
            keyBaseAlias = ctx.generator.getOperatorId() + "_" + tableAlias + "_" + column;

            semiJoinAttempted = generateSemiJoinOperatorPlan(ctx, parseContext, ts, keyBaseAlias);
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
    collectDynamicPruningConditions(ts.getConf().getFilterExpr(), removerContext);

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
      desc.setTargetColumnName(column);
      desc.setPartKey(partKey);
      OperatorFactory.getAndMakeChild(desc, groupByOp);
    }
  }

  // Generates plan for min/max when dynamic partition pruning is ruled out.
  private boolean generateSemiJoinOperatorPlan(DynamicListContext ctx, ParseContext parseContext,
      TableScanOperator ts, String keyBaseAlias) throws SemanticException {

    // we will put a fork in the plan at the source of the reduce sink
    Operator<? extends OperatorDesc> parentOfRS = ctx.generator.getParentOperators().get(0);

    // we need the expr that generated the key of the reduce sink
    ExprNodeDesc key = ctx.generator.getConf().getKeyCols().get(ctx.desc.getKeyIndex());

    String internalColName = null;
    ExprNodeDesc exprNodeDesc = key;
    // Find the ExprNodeColumnDesc
    while (!(exprNodeDesc instanceof ExprNodeColumnDesc) &&
            (exprNodeDesc.getChildren() != null)) {
      exprNodeDesc = exprNodeDesc.getChildren().get(0);
    }

    if (!(exprNodeDesc instanceof ExprNodeColumnDesc)) {
      // No column found!
      // Bail out
      return false;
    }

    internalColName = ((ExprNodeColumnDesc) exprNodeDesc).getColumn();
    if (parentOfRS instanceof SelectOperator) {
      // Make sure the semijoin branch is not on partition column.
      ExprNodeDesc expr = parentOfRS.getColumnExprMap().get(internalColName);
      while (!(expr instanceof ExprNodeColumnDesc) &&
              (expr.getChildren() != null)) {
        expr = expr.getChildren().get(0);
      }

      if (!(expr instanceof ExprNodeColumnDesc)) {
        // No column found!
        // Bail out
        return false;
      }

      ExprNodeColumnDesc colExpr = (ExprNodeColumnDesc) expr;
      String colName = ExprNodeDescUtils.extractColName(colExpr);

      // Fetch the TableScan Operator.
      Operator<?> op = parentOfRS.getParentOperators().get(0);
      while (op != null && !(op instanceof TableScanOperator)) {
        op = op.getParentOperators().get(0);
      }
      assert op != null;

      Table table = ((TableScanOperator) op).getConf().getTableMetadata();
      if (table.isPartitionKey(colName)) {
        // The column is partition column, skip the optimization.
        return false;
      }
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

    ArrayList<ExprNodeDesc> groupByExprs = new ArrayList<ExprNodeDesc>();

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
      GenericUDAFBloomFilterEvaluator bloomFilterEval = (GenericUDAFBloomFilterEvaluator) bloomFilter.getGenericUDAFEvaluator();
      bloomFilterEval.setSourceOperator(selectOp);
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

    // for explain purpose
    if (parseContext.getContext().getExplainConfig() != null
        && parseContext.getContext().getExplainConfig().isFormatted()) {
      List<String> outputOperators = new ArrayList<>();
      outputOperators.add(groupByOpFinal.getOperatorId());
      rsOp.getConf().setOutputOperators(outputOperators);
    }

    // Create the final Reduce Sink Operator
    ReduceSinkDesc rsDescFinal = PlanUtils.getReduceSinkDesc(
            new ArrayList<ExprNodeDesc>(), rsValueCols, gbOutputNames, false,
            -1, 0, 1, Operation.NOT_ACID);
    ReduceSinkOperator rsOpFinal = (ReduceSinkOperator)OperatorFactory.getAndMakeChild(
            rsDescFinal, new RowSchema(groupByOpFinal.getSchema()), groupByOpFinal);
    rsOpFinal.setColumnExprMap(columnExprMap);

    LOG.debug("DynamicMinMaxPushdown: Saving RS to TS mapping: " + rsOpFinal + ": " + ts);
    parseContext.getRsOpToTsOpMap().put(rsOpFinal, ts);

    // for explain purpose
    if (parseContext.getContext().getExplainConfig() != null
        && parseContext.getContext().getExplainConfig().isFormatted()) {
      List<String> outputOperators = new ArrayList<>();
      outputOperators.add(ts.getOperatorId());
      rsOpFinal.getConf().setOutputOperators(outputOperators);
    }

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
    runtimeValuesInfo.setTsColExpr(ctx.parent.getChildren().get(0));
    parseContext.getRsToRuntimeValuesInfoMap().put(rsOpFinal, runtimeValuesInfo);

    return true;
  }

  private Map<Node, Object> collectDynamicPruningConditions(ExprNodeDesc pred, NodeProcessorCtx ctx)
      throws SemanticException {

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<Rule, NodeProcessor> exprRules = new LinkedHashMap<Rule, NodeProcessor>();
    exprRules.put(new RuleRegExp("R1", ExprNodeDynamicListDesc.class.getName() + "%"),
        new DynamicPartitionPrunerProc());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    Dispatcher disp = new DefaultRuleDispatcher(null, exprRules, ctx);
    GraphWalker egw = new DefaultGraphWalker(disp);

    List<Node> startNodes = new ArrayList<Node>();
    startNodes.add(pred);

    HashMap<Node, Object> outputMap = new HashMap<Node, Object>();
    egw.startWalking(startNodes, outputMap);
    return outputMap;
  }

}
