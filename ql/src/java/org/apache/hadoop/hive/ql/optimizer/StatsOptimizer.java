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

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.QueryProperties.QueryFeature;
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
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.stats.Partish;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMax;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMin;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

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

    if (pctx.getFetchTask() != null || !pctx.getQueryProperties().hasFeature(QueryFeature.QUERY)
        || pctx.getQueryProperties().hasFeature(QueryFeature.REWRITE)
        || pctx.getQueryProperties().hasFeature(QueryFeature.CTAS)
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

    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    opRules.put(new RuleRegExp("R1", TS + SEL + GBY + RS + GBY + SEL + FS),
        new MetaDataProcessor(pctx));
    opRules.put(new RuleRegExp("R2", TS + SEL + GBY + RS + GBY + FS),
            new MetaDataProcessor(pctx));

    NodeProcessorCtx soProcCtx = new StatsOptimizerProcContext();
    SemanticDispatcher disp = new DefaultRuleDispatcher(null, opRules, soProcCtx);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  private static class StatsOptimizerProcContext implements NodeProcessorCtx {
    boolean stopProcess = false;
  }

  private static class MetaDataProcessor implements SemanticNodeProcessor {

    private final ParseContext pctx;

    public MetaDataProcessor (ParseContext pctx) {
      this.pctx = pctx;
    }

    enum StatType{
      Integer,
      Double,
      String,
      Boolean,
      Binary,
      Date,
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

    enum DateSubType {
      DAYS {@Override
        Object cast(long longValue) { return (new DateWritableV2((int)longValue)).get();}
      };
      abstract Object cast(long longValue);
    }

    enum GbyKeyType {
      NULL, CONSTANT, OTHER
    }

    private StatType getType(String origType) {
      if (serdeConstants.IntegralTypes.contains(origType)) {
        return StatType.Integer;
      } else if (origType.equals(serdeConstants.DOUBLE_TYPE_NAME) ||
          origType.equals(serdeConstants.FLOAT_TYPE_NAME)) {
        return StatType.Double;
      } else if (origType.equals(serdeConstants.BINARY_TYPE_NAME)) {
        return StatType.Binary;
      } else if (origType.equals(serdeConstants.BOOLEAN_TYPE_NAME)) {
        return StatType.Boolean;
      } else if (origType.equals(serdeConstants.STRING_TYPE_NAME)) {
        return StatType.String;
      } else if (origType.equals(serdeConstants.DATE_TYPE_NAME)) {
        return StatType.Date;
      }
      return StatType.Unsupported;
    }

    private Long getNullCountFor(StatType type, ColumnStatisticsData statData) {

      switch(type) {
      case Integer :
        return statData.getLongStats().getNumNulls();
      case Double:
        return statData.getDoubleStats().getNumNulls();
      case String:
        return statData.getStringStats().getNumNulls();
      case Boolean:
        return statData.getBooleanStats().getNumNulls();
      case Binary:
        return statData.getBinaryStats().getNumNulls();
      case Date:
        return statData.getDateStats().getNumNulls();
      default:
        return null;
      }
    }

    /**
     * The statistics of no rows: nothing counted, no low or high value. The branches below already
     * fold that to the right answer - zero for a count, NULL for a min or a max.
     */
    private static ColumnStatisticsData emptyColStats(StatType type) {
      return switch (type) {
        case Integer -> ColumnStatisticsData.longStats(new LongColumnStatsData());
        case Double -> ColumnStatisticsData.doubleStats(new DoubleColumnStatsData());
        case String -> ColumnStatisticsData.stringStats(new StringColumnStatsData());
        case Boolean -> ColumnStatisticsData.booleanStats(new BooleanColumnStatsData());
        case Binary -> ColumnStatisticsData.binaryStats(new BinaryColumnStatsData());
        case Date -> ColumnStatisticsData.dateStats(new DateColumnStatsData());
        default -> null;
      };
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

        Hive hive = Hive.get(pctx.getConf());
        Table tbl = tsOp.getConf().getTableMetadata();
        boolean isTransactionalTable = AcidUtils.isTransactionalTable(tbl);

        // If the table is transactional, get stats state by calling getTable() with
        // transactional flag on to check the validity of table stats.
        if (isTransactionalTable) {
          tbl = hive.getTable(tbl.getDbName(), tbl.getTableName(), true, true);
        }

        if (!StatsUtils.checkCanProvideStats(tbl)) {
          Logger.info("Table " + tbl.getTableName() + " is external and also could not provide statistics. " +
              "Skip StatsOptimizer.");
          return null;
        }
        if (tbl.isNonNative() && !tbl.getStorageHandler().canProvideBasicStatistics()) {
          Logger.info("Table " + tbl.getTableName() + " is non Native table. Skip StatsOptimizer.");
          return null;
        }

        Long rowCnt = getRowCnt(tsOp, tbl);
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
        Map<Integer,Object> posToConstant = new LinkedHashMap<>();
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
        } else {
          // Add constants if there is no SELECT on top
          GroupByDesc gbyDesc = cgbyOp.getConf();
          int numCols = gbyDesc.getOutputColumnNames().size();
          int aggCols = gbyDesc.getAggregators().size();
          List<String> dpCols = cgbyOp.getSchema().getColumnNames().subList(0, numCols - aggCols);
          for(int i = 0; i < dpCols.size(); i++) {
            ExprNodeDesc end = ExprNodeDescUtils.findConstantExprOrigin(dpCols.get(i), cgbyOp);
            assert end instanceof ExprNodeConstantDesc;
            posToConstant.put(i, ((ExprNodeConstantDesc)end).getValue());
          }
        }
        FileSinkOperator fsOp = (FileSinkOperator)last;
        if (fsOp.getNumChild() > 0) {
          // looks like a subq plan.
          return null;  // todo we can collapse this part of tree into single TS
        }

        List<Object> oneRow = new ArrayList<Object>();

        // Every aggregate of one query asks the same partitions about a column of the same table,
        // and the statistics of one partition carry every column, so asking once for all of them
        // reads what a thousand aggregates would have read a thousand times.
        PrunedPartitionList prunedList = tbl.isPartitioned() ?
            pctx.getPrunedPartitions(tsOp.getConf().getAlias(), tsOp) : null;
        ScanColStats scanColStats =
            new ScanColStats(hive, tbl, aggregateColumns(pgbyOp, exprMap), prunedList);

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
            // add null for SUM(1), when the table is empty. Without this, category = LONG, and the result is 0
            // instead of NULL.
            if (desc instanceof ExprNodeConstantDesc && rowCnt == 0) {
              oneRow.add(null);
              continue;
            }
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
              oneRow.add(Long.parseLong(constant) * rowCnt);
              break;
            case DOUBLE:
              oneRow.add(Double.parseDouble(constant) * rowCnt);
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
              rowCnt = getRowCnt(tsOp, tbl);
              if (rowCnt == null) {
                return null;
              }
            } else if (aggr.getParameters().get(0) instanceof ExprNodeConstantDesc) {
              if (((ExprNodeConstantDesc) aggr.getParameters().get(0)).getValue() != null) {
                // count (1)
                rowCnt = getRowCnt(tsOp, tbl);
                if (rowCnt == null) {
                  return null;
                }
              }
              // otherwise it is count(null), should directly return 0.
            } else if ((aggr.getParameters().get(0) instanceof ExprNodeColumnDesc)
                && exprMap.get(((ExprNodeColumnDesc) aggr.getParameters().get(0)).getColumn()) instanceof ExprNodeConstantDesc) {
              if (((ExprNodeConstantDesc) (exprMap.get(((ExprNodeColumnDesc) aggr.getParameters()
                  .get(0)).getColumn()))).getValue() != null) {
                rowCnt = getRowCnt(tsOp, tbl);
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
                // asked of the table, not read off its parameters: those describe the current
                // snapshot, while the column statistics below answer for the one this scan reads
                Long tableRowCnt = getRowCnt(tsOp, tbl);
                if (tableRowCnt == null) {
                  Logger.debug("No exact row count for table : " + tbl.getTableName());
                  return null;
                }
                rowCnt = tableRowCnt;
                ColumnStatisticsData statData = scanColStats.statsFor(colName, type);
                if (statData == null) {
                  return null; // logging inside
                }
                Long nullCnt = getNullCountFor(type, statData);
                if (null == nullCnt) {
                  Logger.debug("Unsupported type: " + desc.getTypeString() + " encountered in "
                      + "metadata optimizer for column : " + colName);
                  return null;
                } else {
                  rowCnt -= nullCnt;
                }
              } else {
                if (tbl.isNonNative()) {
                  // a handler holds no partition parameters, so its row counts are asked of the
                  // table, which is told the partitions this scan was pruned to
                  Long handlerRowCnt = getRowCnt(tsOp, tbl);
                  if (handlerRowCnt == null) {
                    Logger.debug("No exact row count for table : " + tbl.getTableName());
                    return null;
                  }
                  rowCnt = handlerRowCnt;
                } else {
                  for (Partition part : prunedList.getPartitions()) {
                    if (!StatsUtils.areBasicStatsUptoDateForQueryAnswering(part.getTable(), part.getParameters())) {
                      Logger.debug("Stats for part : " + part.getSpec() + " are not up to date.");
                      return null;
                    }
                    long partRowCnt = Long.parseLong(part.getParameters().get(
                        StatsSetupConst.ROW_COUNT));
                    rowCnt += partRowCnt;
                  }
                }
                ColumnStatisticsData statData = scanColStats.statsFor(colName, type);
                if (statData == null) {
                  return null; // logging inside
                }
                Long nullCnt = getNullCountFor(type, statData);
                if (nullCnt == null) {
                  Logger.debug("Unsupported type: " + desc.getTypeString() + " encountered in "
                      + "metadata optimizer for column : " + colName);
                  return null;
                }
                rowCnt -= nullCnt;
              }
            }
            oneRow.add(rowCnt);
          } else if (udaf instanceof GenericUDAFMax) {
            ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc)exprMap.get(((ExprNodeColumnDesc)aggr.getParameters().get(0)).getColumn());
            String colName = colDesc.getColumn();
            StatType type = getType(colDesc.getTypeString());
            ColumnStatisticsData statData = scanColStats.statsFor(colName, type);
            if (statData == null) {
              return null; // logging inside
            }
            String name = colDesc.getTypeString().toUpperCase();
            switch (type) {
              case Integer: {
                LongColumnStatsData lstats = statData.getLongStats();
                oneRow.add(lstats.isSetHighValue() ?
                    LongSubType.valueOf(name).cast(lstats.getHighValue()) : null);
                break;
              }
              case Double: {
                DoubleColumnStatsData dstats = statData.getDoubleStats();
                oneRow.add(dstats.isSetHighValue() ?
                    DoubleSubType.valueOf(name).cast(dstats.getHighValue()) : null);
                break;
              }
              case Date: {
                DateColumnStatsData dstats = statData.getDateStats();
                oneRow.add(dstats.isSetHighValue() ?
                    DateSubType.DAYS.cast(dstats.getHighValue().getDaysSinceEpoch()) : null);
                break;
              }
              default:
                Logger.debug("Unsupported type: " + colDesc.getTypeString() + " encountered in " +
                    "metadata optimizer for column : " + colName);
                return null;
            }
          }  else if (udaf instanceof GenericUDAFMin) {
            ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc)exprMap.get(((ExprNodeColumnDesc)aggr.getParameters().get(0)).getColumn());
            String colName = colDesc.getColumn();
            StatType type = getType(colDesc.getTypeString());
            ColumnStatisticsData statData = scanColStats.statsFor(colName, type);
            if (statData == null) {
              return null; // logging inside
            }
            String name = colDesc.getTypeString().toUpperCase();
            switch (type) {
              case Integer: {
                LongColumnStatsData lstats = statData.getLongStats();
                oneRow.add(lstats.isSetLowValue() ?
                    LongSubType.valueOf(name).cast(lstats.getLowValue()) : null);
                break;
              }
              case Double: {
                DoubleColumnStatsData dstats = statData.getDoubleStats();
                oneRow.add(dstats.isSetLowValue() ?
                    DoubleSubType.valueOf(name).cast(dstats.getLowValue()) : null);
                break;
              }
              case Date: {
                DateColumnStatsData dstats = statData.getDateStats();
                oneRow.add(dstats.isSetLowValue() ?
                    DateSubType.DAYS.cast(dstats.getLowValue().getDaysSinceEpoch()) : null);
                break;
              }
              default:
                Logger.debug("Unsupported type: " + colDesc.getTypeString() + " encountered in " +
                    "metadata optimizer for column : " + colName);
                return null;
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
          List<Object> oneRowWithConstant = new ArrayList<>();
          oneRowWithConstant.addAll(posToConstant.values());
          oneRowWithConstant.addAll(oneRow);
          allRows.add(oneRowWithConstant);
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
          fTask = (FetchTask) TaskFactory.get(fWork);
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

    /** The columns the aggregates read, which are the ones statistics have to be fetched for. */
    private static List<String> aggregateColumns(GroupByOperator pgbyOp, Map<String, ExprNodeDesc> exprMap) {
      return pgbyOp.getConf().getAggregators().stream()
          .filter(aggr -> !aggr.getParameters().isEmpty())
          .map(aggr -> aggr.getParameters().get(0))
          .filter(ExprNodeColumnDesc.class::isInstance)
          .map(desc -> exprMap.get(((ExprNodeColumnDesc) desc).getColumn()))
          .filter(ExprNodeColumnDesc.class::isInstance)
          .map(desc -> ((ExprNodeColumnDesc) desc).getColumn())
          .distinct()
          .collect(Collectors.toList());
    }

    /**
     * The statistics of the columns a scan's aggregates read, fetched for every column when the
     * first of them asks. An aggregate this rewrite cannot answer declines the query before
     * asking, so a query holding one pays for nothing. Answers for a scan of a partitioned table.
     */
    private static final class ScanColStats {
      private final Hive hive;
      private final Table tbl;
      private final List<String> colNames;
      private final PrunedPartitionList prunedList;
      private Map<String, ColumnStatisticsObj> colStatsByName;
      private boolean fetched;

      ScanColStats(Hive hive, Table tbl, List<String> colNames, PrunedPartitionList prunedList) {
        this.hive = hive;
        this.tbl = tbl;
        this.colNames = colNames;
        this.prunedList = prunedList;
      }

      /**
       * One column's statistics. A scan pruned to no partitions reads no rows, and the statistics
       * of no rows are the empty ones: nothing counted, and no least or greatest to name.
       */
      ColumnStatisticsData statsFor(String colName, StatType type) throws HiveException {
        if (prunedList != null && prunedList.getPartitions().isEmpty()) {
          return emptyColStats(type);
        }
        if (!fetched) {
          fetched = true;
          colStatsByName = prunedList == null ? tableColStats() : verifyAndFetch();
        }
        ColumnStatisticsObj stat = colStatsByName == null ? null : colStatsByName.get(colName);
        if (stat == null) {
          Logger.debug("No stats for " + tbl.getTableName() + " column " + colName);
          return null;
        }
        return stat.getStatsData();
      }

      /**
       * Whether the table's own statistics answer for this scan: it keeps them for the table as
       * a whole, and the scan reads every partition. They then describe exactly the rows read.
       */
      private boolean answeredByTheTable() {
        return !StatsUtils.isPartitionStats(tbl, hive.getConf()) &&
            prunedList.getReferredPartCols().isEmpty() && !prunedList.hasUnknownPartitions();
      }

      /** The table's own statistics, taken only while they still describe it. */
      private Map<String, ColumnStatisticsObj> tableColStats() throws HiveException {
        if (!StatsUtils.areColumnStatsUptoDateForQueryAnswering(tbl, tbl.getParameters(), colNames)) {
          Logger.debug("Stats for table : " + tbl.getTableName() + " columns " + colNames
              + " are not up to date.");
          return null;
        }
        return byColumnName(hive.getTableColumnStatistics(tbl, colNames, true));
      }

      /** What the scan's partitions hold for every column asked about, or null to decline. */
      private Map<String, ColumnStatisticsObj> verifyAndFetch() throws HiveException {
        Set<Partition> parts = prunedList.getPartitions();
        List<String> partNames = new ArrayList<>(parts.size());
        // a storage handler holds no partition parameters, and one kept per partition describes no
        // partition in particular: whether each still describes itself is answered by the aggregate
        // below, which is told the partitions this query pruned to
        if (tbl.isNonNative()) {
          if (!StatsUtils.checkCanProvideColumnStats(tbl)) {
            Logger.debug("Table : " + tbl.getTableName() + " provides no column statistics.");
            return null;
          }
          if (answeredByTheTable()) {
            return tableColStats();
          }
          parts.forEach(part -> partNames.add(part.getName()));
        } else {
          for (Partition part : parts) {
            if (!StatsUtils.areColumnStatsUptoDateForQueryAnswering(
                part.getTable(), part.getParameters(), colNames)) {
              Logger.debug("Stats for part : " + part.getSpec() + " columns " + colNames
                  + " are not up to date.");
              return null;
            }
            partNames.add(part.getName());
          }
        }
        // Aggregated rather than per partition: the callers fold these with min, max or a sum, so
        // merging first gives the same answer. A handler aggregates its own statistics, which
        // the metastore cannot hold: PART_COL_STATS rows need a partition Iceberg never creates.
        AggrStats aggrStats;
        try {
          aggrStats = tbl.isNonNative()
              ? tbl.getStorageHandler().getAggrColStatsFor(tbl, colNames, partNames)
              : exactAggrColStats(partNames);
        } catch (MetaException e) {
          throw new HiveException(e);
        }
        if (aggrStats == null || aggrStats.getColStats() == null) {
          Logger.debug("No stats for " + tbl.getTableName() + " columns " + colNames);
          return null;
        }
        if (aggrStats.getPartsFound() != parts.size()) {
          // a partition whose statistics are missing would leave the answer describing a subset
          Logger.debug("Received " + aggrStats.getPartsFound() + " stats for " + parts.size() + " partitions");
          return null;
        }
        return byColumnName(aggrStats.getColStats());
      }

      /**
       * Each partition fetched and folded the way a storage handler folds its own: the
       * metastore's aggregate endpoint may serve a cached aggregate of a different partition
       * set within its variance, which estimates a plan fine but must not answer a query.
       */
      private AggrStats exactAggrColStats(List<String> partNames) throws HiveException, MetaException {
        Map<String, List<ColumnStatisticsObj>> byPartition = hive.getPartitionColumnStatistics(
            tbl.getDbName(), tbl.getTableName(), partNames, colNames, true);
        List<ColumnStatistics> partStats = new ArrayList<>();
        byPartition.forEach((partitionName, statsObjs) -> {
          // a partition counts as found only when it holds every column asked about
          if (statsObjs.size() == colNames.size()) {
            ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(false, tbl.getDbName(), tbl.getTableName());
            statsDesc.setPartName(partitionName);
            partStats.add(new ColumnStatistics(statsDesc, statsObjs));
          }
        });
        HiveConf conf = hive.getConf();
        List<ColumnStatisticsObj> aggregated = MetaStoreServerUtils.aggrPartitionStats(partStats,
            MetaStoreUtils.getDefaultCatalog(conf), tbl.getDbName(), tbl.getTableName(),
            partNames, colNames,
            partStats.size() == partNames.size(),
            MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.STATS_NDV_DENSITY_FUNCTION),
            MetastoreConf.getDoubleVar(conf, MetastoreConf.ConfVars.STATS_NDV_TUNER));
        return new AggrStats(aggregated, partStats.size());
      }

      private static Map<String, ColumnStatisticsObj> byColumnName(List<ColumnStatisticsObj> colStats) {
        return colStats.stream().collect(
            Collectors.toMap(ColumnStatisticsObj::getColName, Function.identity(), (first, second) -> second));
      }
    }

    private Long getRowCnt(TableScanOperator tsOp, Table tbl) throws HiveException {
      if (tbl.isNonNative()) {
        return getRowCntFromStorageHandler(tsOp, tbl);
      }
      final List<Partish> partishList;
      if (tbl.isPartitioned()) {
        partishList = pctx.getPrunedPartitions(tsOp.getConf().getAlias(), tsOp).getPartitions().stream()
          .map(Partish::buildFor)
          .toList();
      } else {
        partishList = Lists.newArrayList(Partish.buildFor(tbl));
      }
      long rowCnt = 0L;
      for (Partish partish : partishList) {
        Map<String, String> basicStats = partish.getPartParameters();
        if (!StatsUtils.areBasicStatsUptoDateForQueryAnswering(partish.getTable(), basicStats)) {
          return null;
        }
        rowCnt += Long.parseLong(basicStats.get(StatsSetupConst.ROW_COUNT));
      }
      return rowCnt;
    }

    private Long getRowCntFromStorageHandler(TableScanOperator tsOp, Table tbl) throws HiveException {
      if (tbl.getMetaTable() != null) {
        // metadata table scans cannot be answered from the data table's statistics
        return null;
      }
      HiveStorageHandler storageHandler = tbl.getStorageHandler();
      if (tbl.isPartitioned()) {
        PrunedPartitionList prunedList = pctx.getPrunedPartitions(tsOp.getConf().getAlias(), tsOp);
        if (!prunedList.getReferredPartCols().isEmpty()) {
          // a partition predicate may have pruned the list to a subset, so only the
          // per-partition statistics can answer exactly
          if (!StatsUtils.checkCanProvidePartitionStats(tbl)) {
            return null;
          }
          List<String> partNames = prunedList.getPartitions().stream()
              .map(Partition::getName)
              .toList();
          if (partNames.isEmpty()) {
            // the predicate matched nothing the scan would read
            return 0L;
          }
          Map<String, Long> rowCounts = storageHandler.getRowCount(tbl, partNames);
          if (rowCounts.size() != partNames.size()) {
            // the rewrite substitutes a constant, so it needs an exact count for every partition
            return null;
          }
          return rowCounts.values().stream().mapToLong(Long::longValue).sum();
        }
      }
      // unpruned or unpartitioned: the table-level statistics are exactly the aggregate of every
      // partition - answered without reading the partition stats file
      return storageHandler.getRowCount(tbl);
    }
  }
}
