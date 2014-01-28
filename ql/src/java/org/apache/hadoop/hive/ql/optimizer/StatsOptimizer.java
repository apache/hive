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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMax;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMin;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

/** There is a set of queries which can be answered entirely from statistics stored in metastore.
 * Examples of such queries are count(*), count(a), max(a), min(b) etc. Hive already collects
 * these basic statistics for query planning purposes. These same statistics can be used to
 * answer queries also.
 *
 * Optimizer looks at query plan to determine if it can answer query using statistics
 * and than change the plan to answer query entirely using statistics stored in metastore.
 */
public class StatsOptimizer implements Transform {

  private static final Log Log = LogFactory.getLog(StatsOptimizer.class);

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    if (pctx.getFetchTask() != null || !pctx.getQB().getIsQuery() ||
        pctx.getQB().isAnalyzeRewrite() || pctx.getQB().isCTAS() ||
        pctx.getLoadFileWork().size() > 1 || !pctx.getLoadTableWork().isEmpty()) {
      return pctx;
    }

    String TS = TableScanOperator.getOperatorName() + "%";
    String GBY = GroupByOperator.getOperatorName() + "%";
    String RS = ReduceSinkOperator.getOperatorName() + "%";
    String SEL = SelectOperator.getOperatorName() + "%";
    String FS = FileSinkOperator.getOperatorName() + "%";

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", TS + SEL + GBY + RS + GBY + SEL + FS),
        new MetaDataProcessor(pctx));

    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, null);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  private static class MetaDataProcessor implements NodeProcessor {

    private final ParseContext pctx;

    public MetaDataProcessor (ParseContext pctx) {
      this.pctx = pctx;
    }

    enum StatType{
      Integeral,
      Double,
      String,
      Boolean,
      Binary,
      Unsupported
    }

    private StatType getType(String origType) {
      if (serdeConstants.IntegralTypes.contains(origType)) {
        return StatType.Integeral;
      } else if (origType.equals(serdeConstants.DOUBLE_TYPE_NAME) ||
          origType.equals(serdeConstants.FLOAT_TYPE_NAME)) {
        return StatType.Double;
      } else if (origType.equals(serdeConstants.BINARY_TYPE_NAME)) {
        return StatType.Binary;
      } else if (origType.equals(serdeConstants.BOOLEAN_TYPE_NAME)) {
        return StatType.Boolean;
      } else if (origType.equals(serdeConstants.STRING_TYPE_NAME)) {
        return StatType.String;
      }
      return StatType.Unsupported;
    }

    private Long getNullcountFor(StatType type, ColumnStatisticsData statData) {

      switch(type) {
      case Integeral :
        return statData.getLongStats().getNumNulls();
      case Double:
        return statData.getDoubleStats().getNumNulls();
      case String:
        return statData.getStringStats().getNumNulls();
      case Boolean:
        return statData.getBooleanStats().getNumNulls();
      case Binary:
        return statData.getBinaryStats().getNumNulls();
      default:
        return null;
      }
    }

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      // 1. Do few checks to determine eligibility of optimization
      // 2. look at ExprNodeFuncGenericDesc in select list to see if its min, max, count etc.
      //    If it is
      // 3. Connect to metastore and get the stats
      // 4. Compose rows and add it in FetchWork
      // 5. Delete GBY - RS - GBY - SEL from the pipeline.

      TableScanOperator tsOp = (TableScanOperator) stack.get(0);
      if(tsOp.getParentOperators() != null && tsOp.getParentOperators().size() > 0) {
        // looks like a subq plan.
        return null;
      }
      SelectOperator selOp = (SelectOperator)tsOp.getChildren().get(0);
      for(ExprNodeDesc desc : selOp.getConf().getColList()) {
        if (!(desc instanceof ExprNodeColumnDesc)) {
          // Probably an expression, cant handle that
          return null;
        }
      }
      // Since we have done an exact match on TS-SEL-GBY-RS-GBY-SEL-FS
      // we need not to do any instanceof checks for following.
      GroupByOperator gbyOp = (GroupByOperator)selOp.getChildren().get(0);
      FileSinkOperator fsOp = (FileSinkOperator)(gbyOp.getChildren().get(0).
          getChildren().get(0).getChildren().get(0).getChildren().get(0));
      if (fsOp.getChildOperators() != null && fsOp.getChildOperators().size() > 0) {
        // looks like a subq plan.
        return null;
      }
      List<AggregationDesc> aggrs = gbyOp.getConf().getAggregators();

      Table tbl = pctx.getTopToTable().get(tsOp);
      List<Object> oneRow = new ArrayList<Object>();
      List<ObjectInspector> ois = new ArrayList<ObjectInspector>();
      try{
        Hive hive = Hive.get(pctx.getConf());

        for (AggregationDesc aggr : aggrs) {
          if (aggr.getGenericUDAFName().equals(GenericUDAFSum.class.getAnnotation(
              Description.class).name())) {
              if(!(aggr.getParameters().get(0) instanceof ExprNodeConstantDesc)){
                return null;
              }
              Long rowCnt = getRowCnt(pctx, tsOp, tbl);
              if(rowCnt == null) {
                return null;
              }
              oneRow.add(HiveDecimal.create(((ExprNodeConstantDesc) aggr.getParameters().get(0))
                .getValue().toString()).multiply(HiveDecimal.create(rowCnt)));
              ois.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveCategory.DECIMAL));
          }
          else if (aggr.getGenericUDAFName().equals(GenericUDAFCount.class.getAnnotation(
              Description.class).name())) {
            Long rowCnt = 0L;
            if ((aggr.getParameters().isEmpty() || aggr.getParameters().get(0) instanceof
                ExprNodeConstantDesc)) {
              // Its either count (*) or count(1) case
              rowCnt = getRowCnt(pctx, tsOp, tbl);
              if(rowCnt == null) {
                return null;
              }
            } else {
              // Its count(col) case
              if (!(aggr.getParameters().get(0) instanceof ExprNodeColumnDesc)) {
                // this is weird, we got expr or something in there, bail out
                Log.debug("Unexpected expression : " + aggr.getParameters().get(0));
                return null;
              }
              ExprNodeColumnDesc desc = (ExprNodeColumnDesc)aggr.getParameters().get(0);
              String colName = desc.getColumn();
              StatType type = getType(desc.getTypeString());
              if(!tbl.isPartitioned()) {
                if (!StatsSetupConst.areStatsUptoDate(tbl.getParameters())) {
                  Log.debug("Stats for table : " + tbl.getTableName() + " are not upto date.");
                  return null;
                  }
                  rowCnt = Long.parseLong(tbl.getProperty(StatsSetupConst.ROW_COUNT));
                if (rowCnt < 1) {
                  Log.debug("Table doesn't have upto date stats " + tbl.getTableName());
                  return null;
                }
                ColumnStatisticsData statData = hive.getMSC().getTableColumnStatistics(
                    tbl.getDbName(),tbl.getTableName(),colName).
                    getStatsObjIterator().next().getStatsData();
                Long nullCnt = getNullcountFor(type, statData);
                if (null == nullCnt) {
                  Log.debug("Unsupported type: " + desc.getTypeString() + " encountered in " +
                      "metadata optimizer for column : " + colName);
                  return null;
                } else {
                  rowCnt -= nullCnt;
                }
              } else {
                for (Partition part : pctx.getPrunedPartitions(tsOp.getConf().getAlias(), tsOp).getPartitions()) {
                  if (!StatsSetupConst.areStatsUptoDate(part.getParameters())) {
                    Log.debug("Stats for part : " + part.getSpec() + " are not upto date.");
                    return null;
                    }
                    Long partRowCnt = Long.parseLong(part.getParameters()
                      .get(StatsSetupConst.ROW_COUNT));
                  if (partRowCnt < 1) {
                    Log.debug("Partition doesn't have upto date stats " + part.getSpec());
                    return null;
                  }
                  rowCnt += partRowCnt;
                  ColumnStatisticsData statData = hive.getMSC().getPartitionColumnStatistics(
                    tbl.getDbName(), tbl.getTableName(),part.getName(), colName)
                    .getStatsObjIterator().next().getStatsData();
                  Long nullCnt = getNullcountFor(type, statData);
                  if(nullCnt == null) {
                    Log.debug("Unsupported type: " + desc.getTypeString() + " encountered in " +
                        "metadata optimizer for column : " + colName);
                    return null;
                  } else {
                    rowCnt -= nullCnt;
                  }
                }
              }
            }
            oneRow.add(rowCnt);
            ois.add(PrimitiveObjectInspectorFactory.
                getPrimitiveJavaObjectInspector(PrimitiveCategory.LONG));
          } else if (aggr.getGenericUDAFName().equals(GenericUDAFMax.class.getAnnotation(
              Description.class).name())) {
            ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc)aggr.getParameters().get(0);
            String colName = colDesc.getColumn();
            StatType type = getType(colDesc.getTypeString());
            if(!tbl.isPartitioned()) {
              if (!StatsSetupConst.areStatsUptoDate(tbl.getParameters())) {
                  Log.debug("Stats for table : " + tbl.getTableName() + " are not upto date.");
                  return null;
                  }
              ColumnStatisticsData statData = hive.getMSC().getTableColumnStatistics(
                tbl.getDbName(),tbl.getTableName(),colName).
                getStatsObjIterator().next().getStatsData();
              switch (type) {
              case Integeral:
                oneRow.add(statData.getLongStats().getHighValue());
                ois.add(PrimitiveObjectInspectorFactory.
                    getPrimitiveJavaObjectInspector(PrimitiveCategory.LONG));
                break;
              case Double:
                oneRow.add(statData.getDoubleStats().getHighValue());
                ois.add(PrimitiveObjectInspectorFactory.
                    getPrimitiveJavaObjectInspector(PrimitiveCategory.DOUBLE));
                break;
              default:
                // unsupported type
                Log.debug("Unsupported type: " + colDesc.getTypeString() + " encountered in " +
                    "metadata optimizer for column : " + colName);
                return null;
              }
            } else {
              Set<Partition> parts = pctx.getPrunedPartitions(tsOp.getConf().getAlias(), tsOp).getPartitions();
              switch(type) {
              case Integeral: {
                long maxVal = Long.MIN_VALUE;
                for (Partition part : parts) {
                  if (!StatsSetupConst.areStatsUptoDate(part.getParameters())) {
                    Log.debug("Stats for part : " + part.getSpec() + " are not upto date.");
                    return null;
                    }
                	ColumnStatisticsData statData = hive.getMSC().getPartitionColumnStatistics(
                      tbl.getDbName(),tbl.getTableName(), part.getName(), colName).
                      getStatsObjIterator().next().getStatsData();
                  maxVal = Math.max(maxVal,statData.getLongStats().getHighValue());
                }
                oneRow.add(maxVal);
                ois.add(PrimitiveObjectInspectorFactory.
                    getPrimitiveJavaObjectInspector(PrimitiveCategory.LONG));
                break;
              }
              case Double: {
                double maxVal = Double.MIN_VALUE;
                for (Partition part : parts) {
                  if (!StatsSetupConst.areStatsUptoDate(part.getParameters())) {
                    Log.debug("Stats for part : " + part.getSpec() + " are not upto date.");
                    return null;
                    }
                  ColumnStatisticsData statData = hive.getMSC().getPartitionColumnStatistics(
                    tbl.getDbName(),tbl.getTableName(), part.getName(), colName).
                    getStatsObjIterator().next().getStatsData();
                  maxVal = Math.max(maxVal,statData.getDoubleStats().getHighValue());
                }
                oneRow.add(maxVal);
                ois.add(PrimitiveObjectInspectorFactory.
                    getPrimitiveJavaObjectInspector(PrimitiveCategory.DOUBLE));
                break;
              }
              default:
                Log.debug("Unsupported type: " + colDesc.getTypeString() + " encountered in " +
                    "metadata optimizer for column : " + colName);
                return null;
              }
            }
          }  else if (aggr.getGenericUDAFName().equals(GenericUDAFMin.class.getAnnotation(
              Description.class).name())) {
            ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc)aggr.getParameters().get(0);
            String colName = colDesc.getColumn();
            StatType type = getType(colDesc.getTypeString());
            if (!tbl.isPartitioned()) {
              if (!StatsSetupConst.areStatsUptoDate(tbl.getParameters())) {
                Log.debug("Stats for table : " + tbl.getTableName() + " are not upto date.");
                return null;
                }
              ColumnStatisticsData statData = hive.getMSC().getTableColumnStatistics(
                tbl.getDbName(),tbl.getTableName(),colName).
                getStatsObjIterator().next().getStatsData();
              switch (type) {
              case Integeral:
                oneRow.add(statData.getLongStats().getLowValue());
                ois.add(PrimitiveObjectInspectorFactory.
                    getPrimitiveJavaObjectInspector(PrimitiveCategory.LONG));
                break;
              case Double:
                oneRow.add(statData.getDoubleStats().getLowValue());
                ois.add(PrimitiveObjectInspectorFactory.
                    getPrimitiveJavaObjectInspector(PrimitiveCategory.DOUBLE));
                break;
              default: // unsupported type
                Log.debug("Unsupported type: " + colDesc.getTypeString() + " encountered in " +
                    "metadata optimizer for column : " + colName);
                return null;
              }
            } else {
              Set<Partition> parts = pctx.getPrunedPartitions(tsOp.getConf().getAlias(), tsOp).getPartitions();
              switch(type) {
              case Integeral: {
                long minVal = Long.MAX_VALUE;
                for (Partition part : parts) {
                  if (!StatsSetupConst.areStatsUptoDate(part.getParameters())) {
                    Log.debug("Stats for part : " + part.getSpec() + " are not upto date.");
                    return null;
                    }
                  ColumnStatisticsData statData = hive.getMSC().getPartitionColumnStatistics(
                    tbl.getDbName(),tbl.getTableName(), part.getName(), colName).
                    getStatsObjIterator().next().getStatsData();
                  minVal = Math.min(minVal,statData.getLongStats().getLowValue());
                }
                oneRow.add(minVal);
                ois.add(PrimitiveObjectInspectorFactory.
                    getPrimitiveJavaObjectInspector(PrimitiveCategory.LONG));
                break;
              }
              case Double: {
                double minVal = Double.MAX_VALUE;
                for (Partition part : parts) {
                  if (!StatsSetupConst.areStatsUptoDate(part.getParameters())) {
                    Log.debug("Stats for part : " + part.getSpec() + " are not upto date.");
                    return null;
                    }
                  ColumnStatisticsData statData = hive.getMSC().getPartitionColumnStatistics(
                    tbl.getDbName(),tbl.getTableName(), part.getName(), colName).
                    getStatsObjIterator().next().getStatsData();
                  minVal = Math.min(minVal,statData.getDoubleStats().getLowValue());
                }
                oneRow.add(minVal);
                ois.add(PrimitiveObjectInspectorFactory.
                    getPrimitiveJavaObjectInspector(PrimitiveCategory.DOUBLE));
                break;
              }
              default: // unsupported type
                Log.debug("Unsupported type: " + colDesc.getTypeString() + " encountered in " +
                    "metadata optimizer for column : " + colName);
                return null;

              }
            }
          } else { // Unsupported aggregation.
            Log.debug("Unsupported aggregation for metadata optimizer: "
                + aggr.getGenericUDAFName());
            return null;
          }
        }
      } catch (Exception e) {
        // this is best effort optimization, bail out in error conditions and
        // try generate and execute slower plan
        Log.debug("Failed to optimize using metadata optimizer", e);
        return null;
      }

      List<List<Object>> allRows = new ArrayList<List<Object>>();
      allRows.add(oneRow);

      List<String> colNames = new ArrayList<String>();
      for (ColumnInfo colInfo: gbyOp.getSchema().getSignature()) {
        colNames.add(colInfo.getInternalName());
      }
      StandardStructObjectInspector sOI = ObjectInspectorFactory.
          getStandardStructObjectInspector(colNames, ois);
      FetchWork fWork = new FetchWork(allRows, sOI);
      FetchTask fTask = (FetchTask)TaskFactory.get(fWork, pctx.getConf());
      fWork.setLimit(allRows.size());
      pctx.setFetchTask(fTask);

      return null;
    }

    private Long getRowCnt (ParseContext pCtx, TableScanOperator tsOp, Table tbl) throws HiveException {
        Long rowCnt = 0L;
      if(tbl.isPartitioned()) {
        for (Partition part : pctx.getPrunedPartitions(tsOp.getConf().getAlias(), tsOp).getPartitions()) {
          long partRowCnt = Long.parseLong(part.getParameters().get(StatsSetupConst.ROW_COUNT));
          if (partRowCnt < 1) {
            Log.debug("Partition doesn't have upto date stats " + part.getSpec());
            return null;
          }
          rowCnt += partRowCnt;
        }
      } else { // unpartitioned table
        rowCnt = Long.parseLong(tbl.getProperty(StatsSetupConst.ROW_COUNT));
        if (rowCnt < 1) {
          // if rowCnt < 1 than its either empty table or table on which stats are not
          //  computed We assume the worse and don't attempt to optimize.
          Log.debug("Table doesn't have upto date stats " + tbl.getTableName());
          rowCnt = null;
        }
      }
      return rowCnt;
    }
  }
}
