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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
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
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMax;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMin;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;


/** There is a set of queries which can be answered entirely from statistics stored in metastore.
 * Examples of such queries are count(*), count(a), max(a), min(b) etc. Hive already collects
 * these basic statistics for query planning purposes. These same statistics can be used to
 * answer queries also.
 *
 * Optimizer looks at query plan to determine if it can answer query using statistics
 * and than change the plan to answer query entirely using statistics stored in metastore.
 */
public class StatsOptimizer extends Transform {
  // TODO: [HIVE-6289] while getting stats from metastore, we currently only get one col at
  //       a time; this could be improved - get all necessary columns in advance, then use local.
  // TODO: [HIVE-6292] aggregations could be done directly in metastore. Hive over MySQL!

  private static final Logger Logger = LoggerFactory.getLogger(StatsOptimizer.class);

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {

    if (pctx.getFetchTask() != null || !pctx.getQueryProperties().isQuery()
        || pctx.getQueryProperties().isAnalyzeRewrite() || pctx.getQueryProperties().isCTAS()
        || pctx.getLoadFileWork().size() > 1 || !pctx.getLoadTableWork().isEmpty()
        // If getNameToSplitSample is not empty, at least one of the source
        // tables is being sampled and we can not optimize.
        || !pctx.getNameToSplitSample().isEmpty()) {
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
    opRules.put(new RuleRegExp("R2", TS + SEL + GBY + RS + GBY + FS),
            new MetaDataProcessor(pctx));

    NodeProcessorCtx soProcCtx = new StatsOptimizerProcContext();
    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, soProcCtx);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  private static class StatsOptimizerProcContext implements NodeProcessorCtx {
    boolean stopProcess = false;
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

    enum LongSubType {
      BIGINT { @Override
      Object cast(long longValue) { return longValue; } },
      INT { @Override
      Object cast(long longValue) { return (int)longValue; } },
      SMALLINT { @Override
      Object cast(long longValue) { return (short)longValue; } },
      TINYINT { @Override
      Object cast(long longValue) { return (byte)longValue; } };

      abstract Object cast(long longValue);
    }

    enum DoubleSubType {
      DOUBLE { @Override
      Object cast(double doubleValue) { return doubleValue; } },
      FLOAT { @Override
      Object cast(double doubleValue) { return (float) doubleValue; } };

      abstract Object cast(double doubleValue);
    }

    enum GbyKeyType {
      NULL, CONSTANT, OTHER
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

    private GbyKeyType getGbyKeyType(GroupByOperator gbyOp) {
      GroupByDesc gbyDesc = gbyOp.getConf();
      int numCols = gbyDesc.getOutputColumnNames().size();
      int aggCols = gbyDesc.getAggregators().size();
      // If the Group by operator has null key
      if (numCols == aggCols) {
        return GbyKeyType.NULL;
      }
      // If the Gby key is a constant
      List<String> dpCols = gbyOp.getSchema().getColumnNames().subList(0, numCols - aggCols);
      for(String dpCol : dpCols) {
        ExprNodeDesc end = ExprNodeDescUtils.findConstantExprOrigin(dpCol, gbyOp);
        if (!(end instanceof ExprNodeConstantDesc)) {
          return GbyKeyType.OTHER;
        }
      }
      return GbyKeyType.CONSTANT;
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
      StatsOptimizerProcContext soProcCtx = (StatsOptimizerProcContext) procCtx;

      // If the optimization has been stopped for the reasons like being not qualified,
      // or lack of the stats data. we do not continue this process. For an example,
      // for a query select max(value) from src1 union all select max(value) from src2
      // if it has been union remove optimized, the AST tree will become
      // TS[0]->SEL[1]->GBY[2]-RS[3]->GBY[4]->FS[17]
      // TS[6]->SEL[7]->GBY[8]-RS[9]->GBY[10]->FS[18]
      // if TS[0] branch for src1 is not optimized because src1 does not have column stats
      // there is no need to continue processing TS[6] branch
      if (soProcCtx.stopProcess) {
        return null;
      }

      boolean isOptimized = false;
      try {
        TableScanOperator tsOp = (TableScanOperator) stack.get(0);
        if (tsOp.getNumParent() > 0) {
          // looks like a subq plan.
          return null;
        }
        if (tsOp.getConf().getRowLimit() != -1) {
          // table is sampled. In some situation, we really can leverage row
          // limit. In order to be safe, we do not use it now.
          return null;
        }
        Table tbl = tsOp.getConf().getTableMetadata();
        if (MetaStoreUtils.isExternalTable(tbl.getTTable())) {
          Logger.info("Table " + tbl.getTableName() + " is external. Skip StatsOptimizer.");
          return null;
        }
        if (AcidUtils.isTransactionalTable(tbl)) {
          //todo: should this be OK for MM table?
          Logger.info("Table " + tbl.getTableName() + " is ACID table. Skip StatsOptimizer.");
          return null;
        }
        Long rowCnt = getRowCnt(pctx, tsOp, tbl);
        // if we can not have correct table stats, then both the table stats and column stats are not useful.
        if (rowCnt == null) {
          return null;
        }
        SelectOperator pselOp = (SelectOperator)stack.get(1);
        for(ExprNodeDesc desc : pselOp.getConf().getColList()) {
          if (!((desc instanceof ExprNodeColumnDesc) || (desc instanceof ExprNodeConstantDesc))) {
            // Probably an expression, cant handle that
            return null;
          }
        }
        Map<String, ExprNodeDesc> exprMap = pselOp.getColumnExprMap();
        // Since we have done an exact match on TS-SEL-GBY-RS-GBY-(SEL)-FS
        // we need not to do any instanceof checks for following.
        GroupByOperator pgbyOp = (GroupByOperator)stack.get(2);
        if (getGbyKeyType(pgbyOp) == GbyKeyType.OTHER) {
          return null;
        }
        // we already check if rowCnt is null and rowCnt==0 means table is
        // empty.
        else if (getGbyKeyType(pgbyOp) == GbyKeyType.CONSTANT && rowCnt == 0) {
          return null;
        }
        ReduceSinkOperator rsOp = (ReduceSinkOperator)stack.get(3);
        if (rsOp.getConf().getDistinctColumnIndices().size() > 0) {
          // we can't handle distinct
          return null;
        }

        GroupByOperator cgbyOp = (GroupByOperator)stack.get(4);
        if (getGbyKeyType(cgbyOp) == GbyKeyType.OTHER) {
          return null;
        }
        // we already check if rowCnt is null and rowCnt==0 means table is
        // empty.
        else if (getGbyKeyType(cgbyOp) == GbyKeyType.CONSTANT && rowCnt == 0) {
          return null;
        }
        Operator<?> last = (Operator<?>) stack.get(5);
        SelectOperator cselOp = null;
        Map<Integer,Object> posToConstant = new HashMap<>();
        if (last instanceof SelectOperator) {
          cselOp = (SelectOperator) last;
          if (!cselOp.isIdentitySelect()) {
            for (int pos = 0; pos < cselOp.getConf().getColList().size(); pos++) {
              ExprNodeDesc desc = cselOp.getConf().getColList().get(pos);
              if (desc instanceof ExprNodeConstantDesc) {
                //We store the position to the constant value for later use.
                posToConstant.put(pos, ((ExprNodeConstantDesc)desc).getValue());
              } else {
                if (!(desc instanceof ExprNodeColumnDesc)) {
                  // Probably an expression, cant handle that
                  return null;
                }
              }
            }
          }
          last = (Operator<?>) stack.get(6);
        }
        FileSinkOperator fsOp = (FileSinkOperator)last;
        if (fsOp.getNumChild() > 0) {
          // looks like a subq plan.
          return null;  // todo we can collapse this part of tree into single TS
        }

        List<Object> oneRow = new ArrayList<Object>();

        Hive hive = Hive.get(pctx.getConf());

        for (AggregationDesc aggr : pgbyOp.getConf().getAggregators()) {
          if (aggr.getDistinct()) {
            // our stats for NDV is approx, not accurate.
            return null;
          }
          // Get the aggregate function matching the name in the query.
          GenericUDAFResolver udaf =
              FunctionRegistry.getGenericUDAFResolver(aggr.getGenericUDAFName());
          if (udaf instanceof GenericUDAFSum) {
            // long/double/decimal
            ExprNodeDesc desc = aggr.getParameters().get(0);
            PrimitiveCategory category = GenericUDAFSum.getReturnType(desc.getTypeInfo());
            if (category == null) {
              return null;
            }
            String constant;
            if (desc instanceof ExprNodeConstantDesc) {
              constant = ((ExprNodeConstantDesc) desc).getValue().toString();
            } else if (desc instanceof ExprNodeColumnDesc && exprMap.get(((ExprNodeColumnDesc)desc).getColumn()) instanceof ExprNodeConstantDesc) {
              constant = ((ExprNodeConstantDesc)exprMap.get(((ExprNodeColumnDesc)desc).getColumn())).getValue().toString();
            } else {
              return null;
            }
            switch (category) {
              case LONG:
                oneRow.add(Long.valueOf(constant) * rowCnt);
                break;
              case DOUBLE:
                oneRow.add(Double.valueOf(constant) * rowCnt);
                break;
              case DECIMAL:
                oneRow.add(HiveDecimal.create(constant).multiply(HiveDecimal.create(rowCnt)));
                break;
              default:
                throw new IllegalStateException("never");
            }
          }
          else if (udaf instanceof GenericUDAFCount) {
            // always long
            rowCnt = 0L;
            if (aggr.getParameters().isEmpty()) {
              // Its either count (*) or count() case
              rowCnt = getRowCnt(pctx, tsOp, tbl);
              if (rowCnt == null) {
                return null;
              }
            } else if (aggr.getParameters().get(0) instanceof ExprNodeConstantDesc) {
              if (((ExprNodeConstantDesc) aggr.getParameters().get(0)).getValue() != null) {
                // count (1)
                rowCnt = getRowCnt(pctx, tsOp, tbl);
                if (rowCnt == null) {
                  return null;
                }
              }
              // otherwise it is count(null), should directly return 0.
            } else if ((aggr.getParameters().get(0) instanceof ExprNodeColumnDesc)
                && exprMap.get(((ExprNodeColumnDesc) aggr.getParameters().get(0)).getColumn()) instanceof ExprNodeConstantDesc) {
              if (((ExprNodeConstantDesc) (exprMap.get(((ExprNodeColumnDesc) aggr.getParameters()
                  .get(0)).getColumn()))).getValue() != null) {
                rowCnt = getRowCnt(pctx, tsOp, tbl);
                if (rowCnt == null) {
                  return null;
                }
              }
            } else {
              // Its count(col) case
              ExprNodeColumnDesc desc = (ExprNodeColumnDesc) exprMap.get(((ExprNodeColumnDesc) aggr
                  .getParameters().get(0)).getColumn());
              String colName = desc.getColumn();
              StatType type = getType(desc.getTypeString());
              if (!tbl.isPartitioned()) {
                if (!StatsSetupConst.areBasicStatsUptoDate(tbl.getParameters())) {
                  Logger.debug("Stats for table : " + tbl.getTableName() + " are not up to date.");
                  return null;
                }
                rowCnt = Long.parseLong(tbl.getProperty(StatsSetupConst.ROW_COUNT));
                if (rowCnt == null) {
                  Logger.debug("Table doesn't have up to date stats " + tbl.getTableName());
                  return null;
                }
                if (!StatsSetupConst.areColumnStatsUptoDate(tbl.getParameters(), colName)) {
                  Logger.debug("Stats for table : " + tbl.getTableName() + " column " + colName
                      + " are not up to date.");
                  return null;
                }
                List<ColumnStatisticsObj> stats = hive.getMSC().getTableColumnStatistics(
                    tbl.getDbName(), tbl.getTableName(), Lists.newArrayList(colName));
                if (stats.isEmpty()) {
                  Logger.debug("No stats for " + tbl.getTableName() + " column " + colName);
                  return null;
                }
                Long nullCnt = getNullcountFor(type, stats.get(0).getStatsData());
                if (null == nullCnt) {
                  Logger.debug("Unsupported type: " + desc.getTypeString() + " encountered in "
                      + "metadata optimizer for column : " + colName);
                  return null;
                } else {
                  rowCnt -= nullCnt;
                }
              } else {
                Set<Partition> parts = pctx.getPrunedPartitions(tsOp.getConf().getAlias(), tsOp)
                    .getPartitions();
                for (Partition part : parts) {
                  if (!StatsSetupConst.areBasicStatsUptoDate(part.getParameters())) {
                    Logger.debug("Stats for part : " + part.getSpec() + " are not up to date.");
                    return null;
                  }
                  Long partRowCnt = Long.parseLong(part.getParameters().get(
                      StatsSetupConst.ROW_COUNT));
                  if (partRowCnt == null) {
                    Logger.debug("Partition doesn't have up to date stats " + part.getSpec());
                    return null;
                  }
                  rowCnt += partRowCnt;
                }
                Collection<List<ColumnStatisticsObj>> result = verifyAndGetPartColumnStats(hive,
                    tbl, colName, parts);
                if (result == null) {
                  return null; // logging inside
                }
                for (List<ColumnStatisticsObj> statObj : result) {
                  ColumnStatisticsData statData = validateSingleColStat(statObj);
                  if (statData == null)
                    return null;
                  Long nullCnt = getNullcountFor(type, statData);
                  if (nullCnt == null) {
                    Logger.debug("Unsupported type: " + desc.getTypeString() + " encountered in "
                        + "metadata optimizer for column : " + colName);
                    return null;
                  } else {
                    rowCnt -= nullCnt;
                  }
                }
              }
            }
            oneRow.add(rowCnt);
          } else if (udaf instanceof GenericUDAFMax) {
            ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc)exprMap.get(((ExprNodeColumnDesc)aggr.getParameters().get(0)).getColumn());
            String colName = colDesc.getColumn();
            StatType type = getType(colDesc.getTypeString());
            if(!tbl.isPartitioned()) {
              if (!StatsSetupConst.areColumnStatsUptoDate(tbl.getParameters(), colName)) {
                Logger.debug("Stats for table : " + tbl.getTableName() + " column " + colName
                    + " are not up to date.");
                return null;
              }
              List<ColumnStatisticsObj> stats = hive.getMSC().getTableColumnStatistics(
                  tbl.getDbName(),tbl.getTableName(), Lists.newArrayList(colName));
              if (stats.isEmpty()) {
                Logger.debug("No stats for " + tbl.getTableName() + " column " + colName);
                return null;
              }
              ColumnStatisticsData statData = stats.get(0).getStatsData();
              String name = colDesc.getTypeString().toUpperCase();
              switch (type) {
                case Integeral: {
                  LongSubType subType = LongSubType.valueOf(name);
                  LongColumnStatsData lstats = statData.getLongStats();
                  if (lstats.isSetHighValue()) {
                    oneRow.add(subType.cast(lstats.getHighValue()));
                  } else {
                    oneRow.add(null);
                  }
                  break;
                }
                case Double: {
                  DoubleSubType subType = DoubleSubType.valueOf(name);
                  DoubleColumnStatsData dstats = statData.getDoubleStats();
                  if (dstats.isSetHighValue()) {
                    oneRow.add(subType.cast(dstats.getHighValue()));
                  } else {
                    oneRow.add(null);
                  }
                  break;
                }
                default:
                  // unsupported type
                  Logger.debug("Unsupported type: " + colDesc.getTypeString() + " encountered in " +
                      "metadata optimizer for column : " + colName);
                  return null;
              }
            } else {
              Set<Partition> parts = pctx.getPrunedPartitions(
                  tsOp.getConf().getAlias(), tsOp).getPartitions();
              String name = colDesc.getTypeString().toUpperCase();
              switch (type) {
                case Integeral: {
                  LongSubType subType = LongSubType.valueOf(name);

                  Long maxVal = null;
                  Collection<List<ColumnStatisticsObj>> result =
                      verifyAndGetPartColumnStats(hive, tbl, colName, parts);
                  if (result == null) {
                    return null; // logging inside
                  }
                  for (List<ColumnStatisticsObj> statObj : result) {
                    ColumnStatisticsData statData = validateSingleColStat(statObj);
                    if (statData == null) return null;
                    LongColumnStatsData lstats = statData.getLongStats();
                    if (!lstats.isSetHighValue()) {
                      continue;
                    }
                    long curVal = lstats.getHighValue();
                    maxVal = maxVal == null ? curVal : Math.max(maxVal, curVal);
                  }
                  if (maxVal != null) {
                    oneRow.add(subType.cast(maxVal));
                  } else {
                    oneRow.add(maxVal);
                  }
                  break;
                }
                case Double: {
                  DoubleSubType subType = DoubleSubType.valueOf(name);

                  Double maxVal = null;
                  Collection<List<ColumnStatisticsObj>> result =
                      verifyAndGetPartColumnStats(hive, tbl, colName, parts);
                  if (result == null) {
                    return null; // logging inside
                  }
                  for (List<ColumnStatisticsObj> statObj : result) {
                    ColumnStatisticsData statData = validateSingleColStat(statObj);
                    if (statData == null) return null;
                    DoubleColumnStatsData dstats = statData.getDoubleStats();
                    if (!dstats.isSetHighValue()) {
                      continue;
                    }
                    double curVal = statData.getDoubleStats().getHighValue();
                    maxVal = maxVal == null ? curVal : Math.max(maxVal, curVal);
                  }
                  if (maxVal != null) {
                    oneRow.add(subType.cast(maxVal));
                  } else {
                    oneRow.add(null);
                  }
                  break;
                }
                default:
                  Logger.debug("Unsupported type: " + colDesc.getTypeString() + " encountered in " +
                      "metadata optimizer for column : " + colName);
                  return null;
              }
            }
          }  else if (udaf instanceof GenericUDAFMin) {
            ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc)exprMap.get(((ExprNodeColumnDesc)aggr.getParameters().get(0)).getColumn());
            String colName = colDesc.getColumn();
            StatType type = getType(colDesc.getTypeString());
            if (!tbl.isPartitioned()) {
              if (!StatsSetupConst.areColumnStatsUptoDate(tbl.getParameters(), colName)) {
                Logger.debug("Stats for table : " + tbl.getTableName() + " column " + colName
                    + " are not up to date.");
                return null;
              }
              ColumnStatisticsData statData = hive.getMSC().getTableColumnStatistics(
                  tbl.getDbName(), tbl.getTableName(), Lists.newArrayList(colName))
                  .get(0).getStatsData();
              String name = colDesc.getTypeString().toUpperCase();
              switch (type) {
                case Integeral: {
                  LongSubType subType = LongSubType.valueOf(name);
                  LongColumnStatsData lstats = statData.getLongStats();
                  if (lstats.isSetLowValue()) {
                    oneRow.add(subType.cast(lstats.getLowValue()));
                  } else {
                    oneRow.add(null);
                  }
                  break;
                }
                case Double: {
                  DoubleSubType subType = DoubleSubType.valueOf(name);
                  DoubleColumnStatsData dstats = statData.getDoubleStats();
                  if (dstats.isSetLowValue()) {
                    oneRow.add(subType.cast(dstats.getLowValue()));
                  } else {
                    oneRow.add(null);
                  }
                  break;
                }
                default: // unsupported type
                  Logger.debug("Unsupported type: " + colDesc.getTypeString() + " encountered in " +
                      "metadata optimizer for column : " + colName);
                  return null;
              }
            } else {
              Set<Partition> parts = pctx.getPrunedPartitions(tsOp.getConf().getAlias(), tsOp).getPartitions();
              String name = colDesc.getTypeString().toUpperCase();
              switch(type) {
                case Integeral: {
                  LongSubType subType = LongSubType.valueOf(name);

                  Long minVal = null;
                  Collection<List<ColumnStatisticsObj>> result =
                      verifyAndGetPartColumnStats(hive, tbl, colName, parts);
                  if (result == null) {
                    return null; // logging inside
                  }
                  for (List<ColumnStatisticsObj> statObj : result) {
                    ColumnStatisticsData statData = validateSingleColStat(statObj);
                    if (statData == null) return null;
                    LongColumnStatsData lstats = statData.getLongStats();
                    if (!lstats.isSetLowValue()) {
                      continue;
                    }
                    long curVal = lstats.getLowValue();
                    minVal = minVal == null ? curVal : Math.min(minVal, curVal);
                  }
                  if (minVal != null) {
                    oneRow.add(subType.cast(minVal));
                  } else {
                    oneRow.add(minVal);
                  }
                  break;
                }
                case Double: {
                  DoubleSubType subType = DoubleSubType.valueOf(name);

                  Double minVal = null;
                  Collection<List<ColumnStatisticsObj>> result =
                      verifyAndGetPartColumnStats(hive, tbl, colName, parts);
                  if (result == null) {
                    return null; // logging inside
                  }
                  for (List<ColumnStatisticsObj> statObj : result) {
                    ColumnStatisticsData statData = validateSingleColStat(statObj);
                    if (statData == null) return null;
                    DoubleColumnStatsData dstats = statData.getDoubleStats();
                    if (!dstats.isSetLowValue()) {
                      continue;
                    }
                    double curVal = statData.getDoubleStats().getLowValue();
                    minVal = minVal == null ? curVal : Math.min(minVal, curVal);
                  }
                  if (minVal != null) {
                    oneRow.add(subType.cast(minVal));
                  } else {
                    oneRow.add(minVal);
                  }
                  break;
                }
                default: // unsupported type
                  Logger.debug("Unsupported type: " + colDesc.getTypeString() + " encountered in " +
                      "metadata optimizer for column : " + colName);
                  return null;

              }
            }
          } else { // Unsupported aggregation.
            Logger.debug("Unsupported aggregation for metadata optimizer: "
                + aggr.getGenericUDAFName());
            return null;
          }
        }

        List<List<Object>> allRows = new ArrayList<List<Object>>();
        List<String> colNames = new ArrayList<String>();
        List<ObjectInspector> ois = new ArrayList<ObjectInspector>();
        if (cselOp == null) {
          allRows.add(oneRow);
          for (ColumnInfo colInfo : cgbyOp.getSchema().getSignature()) {
            colNames.add(colInfo.getInternalName());
            ois.add(TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(colInfo.getType()));
          }
        } else {
          // in return path, we may have aggr($f0), aggr($f1) in GBY
          // and then select aggr($f1), aggr($f0) in SEL.
          // Thus we need to use colExp to find out which position is
          // corresponding to which position.
          Map<String, Integer> nameToIndex = new HashMap<>();
          for (int index = 0; index < cgbyOp.getConf().getOutputColumnNames().size(); index++) {
            nameToIndex.put(cgbyOp.getConf().getOutputColumnNames().get(index), index);
          }
          List<String> outputColumnNames = cselOp.getConf().getOutputColumnNames();
          Map<Integer, Integer> cselOpTocgbyOp = new HashMap<>();
          for (int index = 0; index < outputColumnNames.size(); index++) {
            if (!posToConstant.containsKey(index)) {
              String outputColumnName = outputColumnNames.get(index);
              ExprNodeColumnDesc exprColumnNodeDesc = (ExprNodeColumnDesc) cselOp
                  .getColumnExprMap().get(outputColumnName);
              cselOpTocgbyOp.put(index, nameToIndex.get(exprColumnNodeDesc.getColumn()));
            }
          }
          List<Object> oneRowWithConstant = new ArrayList<>();
          for (int pos = 0; pos < cselOp.getSchema().getSignature().size(); pos++) {
            if (posToConstant.containsKey(pos)) {
              // This position is a constant.
              oneRowWithConstant.add(posToConstant.get(pos));
            } else {
              // This position is an aggregation.
              // As we store in oneRow only the aggregate results, we need to adjust to the correct position
              // if there are keys in the GBy operator.
              oneRowWithConstant.add(oneRow.get(cselOpTocgbyOp.get(pos) - cgbyOp.getConf().getKeys().size()));
            }
            ColumnInfo colInfo = cselOp.getSchema().getSignature().get(pos);
            colNames.add(colInfo.getInternalName());
            ois.add(TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(colInfo.getType()));
          }
          allRows.add(oneRowWithConstant);
        }

        FetchWork fWork = null;
        FetchTask fTask = pctx.getFetchTask();
        if (fTask != null) {
          fWork = fTask.getWork();
          fWork.getRowsComputedUsingStats().addAll(allRows);
        } else {
          StandardStructObjectInspector sOI = ObjectInspectorFactory.
              getStandardStructObjectInspector(colNames, ois);
          fWork = new FetchWork(allRows, sOI);
          fTask = (FetchTask)TaskFactory.get(fWork, pctx.getConf());
          pctx.setFetchTask(fTask);
        }
        fWork.setLimit(fWork.getRowsComputedUsingStats().size());
        isOptimized = true;
        return null;
      } catch (Exception e) {
        // this is best effort optimization, bail out in error conditions and
        // try generate and execute slower plan
        Logger.debug("Failed to optimize using metadata optimizer", e);
        return null;
      } finally {
        // If StatOptimization is not applied for any reason, the FetchTask should still not have been set
        if (!isOptimized) {
          soProcCtx.stopProcess = true;
          pctx.setFetchTask(null);
        }
      }
    }

    private ColumnStatisticsData validateSingleColStat(List<ColumnStatisticsObj> statObj) {
      if (statObj.size() > 1) {
        Logger.error("More than one stat for a single column!");
        return null;
      } else if (statObj.isEmpty()) {
        Logger.debug("No stats for some partition and column");
        return null;
      }
      return statObj.get(0).getStatsData();
    }

    private Collection<List<ColumnStatisticsObj>> verifyAndGetPartColumnStats(
        Hive hive, Table tbl, String colName, Set<Partition> parts) throws TException {
      List<String> partNames = new ArrayList<String>(parts.size());
      for (Partition part : parts) {
        if (!StatsSetupConst.areColumnStatsUptoDate(part.getParameters(), colName)) {
          Logger.debug("Stats for part : " + part.getSpec() + " column " + colName
              + " are not up to date.");
          return null;
        }
        partNames.add(part.getName());
      }
      Map<String, List<ColumnStatisticsObj>> result = hive.getMSC().getPartitionColumnStatistics(
          tbl.getDbName(), tbl.getTableName(), partNames, Lists.newArrayList(colName));
      if (result.size() != parts.size()) {
        Logger.debug("Received " + result.size() + " stats for " + parts.size() + " partitions");
        return null;
      }
      return result.values();
    }

    private Long getRowCnt(
        ParseContext pCtx, TableScanOperator tsOp, Table tbl) throws HiveException {
      Long rowCnt = 0L;
      if (tbl.isPartitioned()) {
        for (Partition part : pctx.getPrunedPartitions(
            tsOp.getConf().getAlias(), tsOp).getPartitions()) {
          if (!StatsSetupConst.areBasicStatsUptoDate(part.getParameters())) {
            return null;
          }
          Long partRowCnt = Long.parseLong(part.getParameters().get(StatsSetupConst.ROW_COUNT));
          if (partRowCnt == null) {
            Logger.debug("Partition doesn't have up to date stats " + part.getSpec());
            return null;
          }
          rowCnt += partRowCnt;
        }
      } else { // unpartitioned table
        if (!StatsSetupConst.areBasicStatsUptoDate(tbl.getParameters())) {
          return null;
        }
        rowCnt = Long.parseLong(tbl.getProperty(StatsSetupConst.ROW_COUNT));
        if (rowCnt == null) {
          // if rowCnt < 1 than its either empty table or table on which stats are not
          //  computed We assume the worse and don't attempt to optimize.
          Logger.debug("Table doesn't have up to date stats " + tbl.getTableName());
          rowCnt = null;
        }
      }
      return rowCnt;
    }
  }
}
