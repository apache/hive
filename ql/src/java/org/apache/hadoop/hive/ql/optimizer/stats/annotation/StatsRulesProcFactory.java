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

package org.apache.hadoop.hive.ql.optimizer.stats.annotation;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualNS;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

public class StatsRulesProcFactory {

  private static final Log LOG = LogFactory.getLog(StatsRulesProcFactory.class.getName());
  private static final boolean isDebugEnabled = LOG.isDebugEnabled();

  /**
   * Collect basic statistics like number of rows, data size and column level statistics from the
   * table. Also sets the state of the available statistics. Basic and column statistics can have
   * one of the following states COMPLETE, PARTIAL, NONE. In case of partitioned table, the basic
   * and column stats are aggregated together to table level statistics. Column statistics will not
   * be collected if hive.stats.fetch.column.stats is set to false. If basic statistics is not
   * available then number of rows will be estimated from file size and average row size (computed
   * from schema).
   */
  public static class TableScanStatsRule extends DefaultStatsRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      TableScanOperator tsop = (TableScanOperator) nd;
      AnnotateStatsProcCtx aspCtx = (AnnotateStatsProcCtx) procCtx;
      PrunedPartitionList partList =
          aspCtx.getParseContext().getPrunedPartitions(tsop.getName(), tsop);
      Table table = tsop.getConf().getTableMetadata();

      try {
        // gather statistics for the first time and the attach it to table scan operator
        Statistics stats = StatsUtils.collectStatistics(aspCtx.getConf(), partList, table, tsop);
        tsop.setStatistics(stats.clone());

        if (isDebugEnabled) {
          LOG.debug("[0] STATS-" + tsop.toString() + " (" + table.getTableName() + "): " +
              stats.extendedToString());
        }
      } catch (CloneNotSupportedException e) {
        throw new SemanticException(ErrorMsg.STATISTICS_CLONING_FAILED.getMsg());
      } catch (HiveException e) {
        LOG.debug(e);
        throw new SemanticException(e);
      }
      return null;
    }
  }

  /**
   * SELECT operator doesn't change the number of rows emitted from the parent operator. It changes
   * the size of each tuple emitted. In a typical case, where only subset of columns are selected
   * the average row size will reduce as some of the columns will be pruned. In order to accurately
   * compute the average row size, column level statistics is required. Column level statistics
   * stores average size of values in column which can be used to more reliably estimate the
   * reduction in size of each tuple. In the absence of column level statistics, size of columns
   * will be based on data type. For primitive data types size from
   * {@link org.apache.hadoop.hive.ql.util.JavaDataModel} will be used and for variable length data
   * types worst case will be assumed.
   * <p>
   * <i>For more information, refer 'Estimating The Cost Of Operations' chapter in
   * "Database Systems: The Complete Book" by Garcia-Molina et. al.</i>
   * </p>
   */
  public static class SelectStatsRule extends DefaultStatsRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      SelectOperator sop = (SelectOperator) nd;
      Operator<? extends OperatorDesc> parent = sop.getParentOperators().get(0);
      Statistics parentStats = parent.getStatistics();
      AnnotateStatsProcCtx aspCtx = (AnnotateStatsProcCtx) procCtx;
      HiveConf conf = aspCtx.getConf();
      Statistics stats = null;

      if (parentStats != null) {
        try {
          stats = parentStats.clone();
        } catch (CloneNotSupportedException e) {
          throw new SemanticException(ErrorMsg.STATISTICS_CLONING_FAILED.getMsg());
        }
      }

      try {
        if (satisfyPrecondition(parentStats)) {
          // this will take care of mapping between input column names and output column names. The
          // returned column stats will have the output column names.
          List<ColStatistics> colStats = StatsUtils.getColStatisticsFromExprMap(conf, parentStats,
              sop.getColumnExprMap(), sop.getSchema());
          stats.setColumnStats(colStats);
          // in case of select(*) the data size does not change
          if (!sop.getConf().isSelectStar() && !sop.getConf().isSelStarNoCompute()) {
            long dataSize = StatsUtils.getDataSizeFromColumnStats(stats.getNumRows(), colStats);
            stats.setDataSize(dataSize);
          }
          sop.setStatistics(stats);

          if (isDebugEnabled) {
            LOG.debug("[0] STATS-" + sop.toString() + ": " + stats.extendedToString());
          }
        } else {
          if (parentStats != null) {
            sop.setStatistics(parentStats.clone());

            if (isDebugEnabled) {
              LOG.debug("[1] STATS-" + sop.toString() + ": " + parentStats.extendedToString());
            }
          }
        }
      } catch (CloneNotSupportedException e) {
        throw new SemanticException(ErrorMsg.STATISTICS_CLONING_FAILED.getMsg());
      }
      return null;
    }

  }

  /**
   * FILTER operator does not change the average row size but it does change the number of rows
   * emitted. The reduction in the number of rows emitted is dependent on the filter expression.
   * <ul>
   * <i>Notations:</i>
   * <li>T(S) - Number of tuples in relations S</li>
   * <li>V(S,A) - Number of distinct values of attribute A in relation S</li>
   * </ul>
   * <ul>
   * <i>Rules:</i> <b>
   * <li>Column equals a constant</li></b> T(S) = T(R) / V(R,A)
   * <p>
   * <b>
   * <li>Inequality conditions</li></b> T(S) = T(R) / 3
   * <p>
   * <b>
   * <li>Not equals comparison</li></b> - Simple formula T(S) = T(R)
   * <p>
   * - Alternate formula T(S) = T(R) (V(R,A) - 1) / V(R,A)
   * <p>
   * <b>
   * <li>NOT condition</li></b> T(S) = 1 - T(S'), where T(S') is the satisfying condition
   * <p>
   * <b>
   * <li>Multiple AND conditions</li></b> Cascadingly apply the rules 1 to 3 (order doesn't matter)
   * <p>
   * <b>
   * <li>Multiple OR conditions</li></b> - Simple formula is to evaluate conditions independently
   * and sum the results T(S) = m1 + m2
   * <p>
   * - Alternate formula T(S) = T(R) * ( 1 - ( 1 - m1/T(R) ) * ( 1 - m2/T(R) ))
   * <p>
   * where, m1 is the number of tuples that satisfy condition1 and m2 is the number of tuples that
   * satisfy condition2
   * </ul>
   * <p>
   * <i>Worst case:</i> If no column statistics are available, then evaluation of predicate
   * expression will assume worst case (i.e; half the input rows) for each of predicate expression.
   * <p>
   * <i>For more information, refer 'Estimating The Cost Of Operations' chapter in
   * "Database Systems: The Complete Book" by Garcia-Molina et. al.</i>
   * </p>
   */
  public static class FilterStatsRule extends DefaultStatsRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      AnnotateStatsProcCtx aspCtx = (AnnotateStatsProcCtx) procCtx;
      FilterOperator fop = (FilterOperator) nd;
      Operator<? extends OperatorDesc> parent = fop.getParentOperators().get(0);
      Statistics parentStats = parent.getStatistics();
      List<String> neededCols = null;
      if (parent instanceof TableScanOperator) {
        TableScanOperator tsop = (TableScanOperator) parent;
        neededCols = tsop.getNeededColumns();
      }

      try {
        if (parentStats != null) {
          ExprNodeDesc pred = fop.getConf().getPredicate();

          // evaluate filter expression and update statistics
          long newNumRows = evaluateExpression(parentStats, pred, aspCtx,
              neededCols, fop);
          Statistics st = parentStats.clone();

          if (satisfyPrecondition(parentStats)) {

            // update statistics based on column statistics.
            // OR conditions keeps adding the stats independently, this may
            // result in number of rows getting more than the input rows in
            // which case stats need not be updated
            if (newNumRows <= parentStats.getNumRows()) {
              updateStats(st, newNumRows, true, fop);
            }

            if (isDebugEnabled) {
              LOG.debug("[0] STATS-" + fop.toString() + ": " + st.extendedToString());
            }
          } else {

            // update only the basic statistics in the absence of column statistics
            if (newNumRows <= parentStats.getNumRows()) {
              updateStats(st, newNumRows, false, fop);
            }

            if (isDebugEnabled) {
              LOG.debug("[1] STATS-" + fop.toString() + ": " + st.extendedToString());
            }
          }
          fop.setStatistics(st);
          aspCtx.setAndExprStats(null);
        }
      } catch (CloneNotSupportedException e) {
        throw new SemanticException(ErrorMsg.STATISTICS_CLONING_FAILED.getMsg());
      }
      return null;
    }

    private long evaluateExpression(Statistics stats, ExprNodeDesc pred,
        AnnotateStatsProcCtx aspCtx, List<String> neededCols,
        FilterOperator fop) throws CloneNotSupportedException {
      long newNumRows = 0;
      Statistics andStats = null;

      if (pred instanceof ExprNodeGenericFuncDesc) {
        ExprNodeGenericFuncDesc genFunc = (ExprNodeGenericFuncDesc) pred;
        GenericUDF udf = genFunc.getGenericUDF();

        // for AND condition cascadingly update stats
        if (udf instanceof GenericUDFOPAnd) {
          andStats = stats.clone();
          aspCtx.setAndExprStats(andStats);

          // evaluate children
          for (ExprNodeDesc child : genFunc.getChildren()) {
            newNumRows = evaluateChildExpr(aspCtx.getAndExprStats(), child,
                aspCtx, neededCols, fop);
            if (satisfyPrecondition(aspCtx.getAndExprStats())) {
              updateStats(aspCtx.getAndExprStats(), newNumRows, true, fop);
            } else {
              updateStats(aspCtx.getAndExprStats(), newNumRows, false, fop);
            }
          }
        } else if (udf instanceof GenericUDFOPOr) {
          // for OR condition independently compute and update stats
          for (ExprNodeDesc child : genFunc.getChildren()) {
            newNumRows = StatsUtils.safeAdd(
                evaluateChildExpr(stats, child, aspCtx, neededCols, fop), newNumRows);
          }
        } else if (udf instanceof GenericUDFOPNot) {
          newNumRows = evaluateNotExpr(stats, pred, aspCtx, neededCols, fop);
        } else {

          // single predicate condition
          newNumRows = evaluateChildExpr(stats, pred, aspCtx, neededCols, fop);
        }
      } else if (pred instanceof ExprNodeColumnDesc) {

        // can be boolean column in which case return true count
        ExprNodeColumnDesc encd = (ExprNodeColumnDesc) pred;
        String colName = encd.getColumn();
        String tabAlias = encd.getTabAlias();
        String colType = encd.getTypeString();
        if (colType.equalsIgnoreCase(serdeConstants.BOOLEAN_TYPE_NAME)) {
          ColStatistics cs = stats.getColumnStatisticsForColumn(tabAlias, colName);
          if (cs != null) {
            return cs.getNumTrues();
          }
        }

        // if not boolean column return half the number of rows
        return stats.getNumRows() / 2;
      } else if (pred instanceof ExprNodeConstantDesc) {

        // special case for handling false constants
        ExprNodeConstantDesc encd = (ExprNodeConstantDesc) pred;
        if (encd.getValue().equals(false)) {
          return 0;
        } else {
          return stats.getNumRows();
        }
      }

      return newNumRows;
    }

    private long evaluateNotExpr(Statistics stats, ExprNodeDesc pred,
        AnnotateStatsProcCtx aspCtx, List<String> neededCols, FilterOperator fop)
        throws CloneNotSupportedException {

      long numRows = stats.getNumRows();

      // if the evaluate yields true then pass all rows else pass 0 rows
      if (pred instanceof ExprNodeGenericFuncDesc) {
        ExprNodeGenericFuncDesc genFunc = (ExprNodeGenericFuncDesc) pred;
        for (ExprNodeDesc leaf : genFunc.getChildren()) {
          if (leaf instanceof ExprNodeGenericFuncDesc) {

            // GenericUDF
            long newNumRows = 0;
            for (ExprNodeDesc child : genFunc.getChildren()) {
              newNumRows = evaluateChildExpr(stats, child, aspCtx, neededCols,
                  fop);
            }
            return numRows - newNumRows;
          } else if (leaf instanceof ExprNodeConstantDesc) {
            ExprNodeConstantDesc encd = (ExprNodeConstantDesc) leaf;
            if (encd.getValue().equals(true)) {
              return 0;
            } else {
              return numRows;
            }
          } else if (leaf instanceof ExprNodeColumnDesc) {

            // NOT on boolean columns is possible. in which case return false count.
            ExprNodeColumnDesc encd = (ExprNodeColumnDesc) leaf;
            String colName = encd.getColumn();
            String tabAlias = encd.getTabAlias();
            String colType = encd.getTypeString();
            if (colType.equalsIgnoreCase(serdeConstants.BOOLEAN_TYPE_NAME)) {
              ColStatistics cs = stats.getColumnStatisticsForColumn(tabAlias, colName);
              if (cs != null) {
                return cs.getNumFalses();
              }
            }
            // if not boolean column return half the number of rows
            return numRows / 2;
          }
        }
      }

      // worst case
      return numRows / 2;
    }

    private long evaluateColEqualsNullExpr(Statistics stats, ExprNodeDesc pred) {

      long numRows = stats.getNumRows();

      if (pred instanceof ExprNodeGenericFuncDesc) {

        ExprNodeGenericFuncDesc genFunc = (ExprNodeGenericFuncDesc) pred;
        for (ExprNodeDesc leaf : genFunc.getChildren()) {

          if (leaf instanceof ExprNodeColumnDesc) {
            ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) leaf;
            String colName = colDesc.getColumn();
            String tabAlias = colDesc.getTabAlias();
            ColStatistics cs = stats.getColumnStatisticsForColumn(tabAlias, colName);
            if (cs != null) {
              return cs.getNumNulls();
            }
          }
        }
      }

      // worst case
      return numRows / 2;
    }

    private long evaluateChildExpr(Statistics stats, ExprNodeDesc child,
        AnnotateStatsProcCtx aspCtx, List<String> neededCols,
        FilterOperator fop) throws CloneNotSupportedException {

      long numRows = stats.getNumRows();

      if (child instanceof ExprNodeGenericFuncDesc) {

        ExprNodeGenericFuncDesc genFunc = (ExprNodeGenericFuncDesc) child;
        GenericUDF udf = genFunc.getGenericUDF();

        if (udf instanceof GenericUDFOPEqual ||
            udf instanceof GenericUDFOPEqualNS) {
          String colName = null;
          String tabAlias = null;
          boolean isConst = false;
          Object prevConst = null;

          for (ExprNodeDesc leaf : genFunc.getChildren()) {
            if (leaf instanceof ExprNodeConstantDesc) {

              // constant = constant expressions. We shouldn't be getting this
              // after constant folding
              if (isConst) {

                // special case: if both constants are not equal then return 0
                if (prevConst != null &&
                    !prevConst.equals(((ExprNodeConstantDesc)leaf).getValue())) {
                  return 0;
                }
                return numRows;
              }

              // if the first argument is const then just set the flag and continue
              if (colName == null) {
                isConst = true;
                prevConst = ((ExprNodeConstantDesc) leaf).getValue();
                continue;
              }

              // if column name is not contained in needed column list then it
              // is a partition column. We do not need to evaluate partition columns
              // in filter expression since it will be taken care by partitio pruner
              if (neededCols != null && !neededCols.contains(colName)) {
                return numRows;
              }

              ColStatistics cs = stats.getColumnStatisticsForColumn(tabAlias, colName);
              if (cs != null) {
                long dvs = cs.getCountDistint();
                numRows = dvs == 0 ? numRows / 2 : numRows / dvs;
                return numRows;
              }
            } else if (leaf instanceof ExprNodeColumnDesc) {
              ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) leaf;
              colName = colDesc.getColumn();
              tabAlias = colDesc.getTabAlias();

              // if const is first argument then evaluate the result
              if (isConst) {

                // if column name is not contained in needed column list then it
                // is a partition column. We do not need to evaluate partition columns
                // in filter expression since it will be taken care by partitio pruner
                if (neededCols != null && neededCols.indexOf(colName) == -1) {
                  return numRows;
                }

                ColStatistics cs = stats.getColumnStatisticsForColumn(tabAlias, colName);
                if (cs != null) {
                  long dvs = cs.getCountDistint();
                  numRows = dvs == 0 ? numRows / 2 : numRows / dvs;
                  return numRows;
                }
              }
            }
          }
        } else if (udf instanceof GenericUDFOPNotEqual) {
          return numRows;
        } else if (udf instanceof GenericUDFOPEqualOrGreaterThan
            || udf instanceof GenericUDFOPEqualOrLessThan || udf instanceof GenericUDFOPGreaterThan
            || udf instanceof GenericUDFOPLessThan) {
          return numRows / 3;
        } else if (udf instanceof GenericUDFOPNotNull) {
          long newNumRows = evaluateColEqualsNullExpr(stats, genFunc);
          return stats.getNumRows() - newNumRows;
        } else if (udf instanceof GenericUDFOPNull) {
          return evaluateColEqualsNullExpr(stats, genFunc);
        } else if (udf instanceof GenericUDFOPAnd || udf instanceof GenericUDFOPOr
            || udf instanceof GenericUDFOPNot) {
          return evaluateExpression(stats, genFunc, aspCtx, neededCols, fop);
        }
      }

      // worst case
      return numRows / 2;
    }

  }

  /**
   * GROUPBY operator changes the number of rows. The number of rows emitted by GBY operator will be
   * atleast 1 or utmost T(R) (number of rows in relation T) based on the aggregation. A better
   * estimate can be found if we have column statistics on the columns that we are grouping on.
   * <p>
   * Suppose if we are grouping by attributes A,B,C and if statistics for columns A,B,C are
   * available then a better estimate can be found by taking the smaller of product of V(R,[A,B,C])
   * (product of distinct cardinalities of A,B,C) and T(R)/2.
   * <p>
   * T(R) = min (T(R)/2 , V(R,[A,B,C]) ---> [1]
   * <p>
   * In the presence of grouping sets, map-side GBY will emit more rows depending on the size of
   * grouping set (input rows * size of grouping set). These rows will get reduced because of
   * map-side hash aggregation. Hash aggregation is an optimization in hive to reduce the number of
   * rows shuffled between map and reduce stage. This optimization will be disabled if the memory
   * used for hash aggregation exceeds 90% of max available memory for hash aggregation. The number
   * of rows emitted from map-side will vary if hash aggregation is enabled throughout execution or
   * disabled. In the presence of grouping sets, following rules will be applied
   * <p>
   * If <b>hash-aggregation is enabled</b>, for query SELECT * FROM table GROUP BY (A,B) WITH CUBE
   * <p>
   * T(R) = min(T(R)/2, T(R, GBY(A,B)) + T(R, GBY(A)) + T(R, GBY(B)) + 1))
   * <p>
   * where, GBY(A,B), GBY(B), GBY(B) are the GBY rules mentioned above [1]
   * <p>
   * If <b>hash-aggregation is disabled</b>, apply the GBY rule [1] and then multiply the result by
   * number of elements in grouping set T(R) = T(R) * length_of_grouping_set. Since we do not know
   * if hash-aggregation is enabled or disabled during compile time, we will assume worst-case i.e,
   * hash-aggregation is disabled
   * <p>
   * NOTE: The number of rows from map-side GBY operator is dependent on map-side parallelism i.e,
   * number of mappers. The map-side parallelism is expected from hive config
   * "hive.stats.map.parallelism". If the config is not set then default parallelism of 1 will be
   * assumed.
   * <p>
   * <i>Worst case:</i> If no column statistics are available, then T(R) = T(R)/2 will be used as
   * heuristics.
   * <p>
   * <i>For more information, refer 'Estimating The Cost Of Operations' chapter in
   * "Database Systems: The Complete Book" by Garcia-Molina et. al.</i>
   * </p>
   */
  public static class GroupByStatsRule extends DefaultStatsRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {

      GroupByOperator gop = (GroupByOperator) nd;
      Operator<? extends OperatorDesc> parent = gop.getParentOperators().get(0);
      Statistics parentStats = parent.getStatistics();

      // parent stats are not populated yet
      if (parentStats == null) {
        return null;
      }

      AnnotateStatsProcCtx aspCtx = (AnnotateStatsProcCtx) procCtx;
      HiveConf conf = aspCtx.getConf();
      long maxSplitSize = HiveConf.getLongVar(conf, HiveConf.ConfVars.MAPREDMAXSPLITSIZE);
      List<AggregationDesc> aggDesc = gop.getConf().getAggregators();
      Map<String, ExprNodeDesc> colExprMap = gop.getColumnExprMap();
      RowSchema rs = gop.getSchema();
      Statistics stats = null;
      List<ColStatistics> colStats = StatsUtils.getColStatisticsFromExprMap(conf, parentStats,
          colExprMap, rs);
      long cardinality;
      long parallelism = 1L;
      boolean interReduction = false;
      boolean hashAgg = false;
      long inputSize = 1L;
      boolean containsGroupingSet = gop.getConf().isGroupingSetsPresent();
      long sizeOfGroupingSet =
          containsGroupingSet ? gop.getConf().getListGroupingSets().size() : 1L;

      // There are different cases for Group By depending on map/reduce side, hash aggregation,
      // grouping sets and column stats. If we don't have column stats, we just assume hash
      // aggregation is disabled. Following are the possible cases and rule for cardinality
      // estimation

      // INTERMEDIATE REDUCTION:
      // Case 1: NO column stats, NO hash aggregation, NO grouping sets — numRows
      // Case 2: NO column stats, NO hash aggregation, grouping sets — numRows * sizeOfGroupingSet
      // Case 3: column stats, hash aggregation, NO grouping sets — Min(numRows / 2, ndvProduct * parallelism)
      // Case 4: column stats, hash aggregation, grouping sets — Min((numRows * sizeOfGroupingSet) / 2, ndvProduct * parallelism * sizeOfGroupingSet)
      // Case 5: column stats, NO hash aggregation, NO grouping sets — numRows
      // Case 6: column stats, NO hash aggregation, grouping sets — numRows * sizeOfGroupingSet

      // FINAL REDUCTION:
      // Case 7: NO column stats — numRows / 2
      // Case 8: column stats, grouping sets — Min(numRows, ndvProduct * sizeOfGroupingSet)
      // Case 9: column stats, NO grouping sets - Min(numRows, ndvProduct)

      if (!gop.getConf().getMode().equals(GroupByDesc.Mode.MERGEPARTIAL) &&
          !gop.getConf().getMode().equals(GroupByDesc.Mode.COMPLETE) &&
          !gop.getConf().getMode().equals(GroupByDesc.Mode.FINAL)) {

        interReduction = true;

        // consider approximate map side parallelism to be table data size
        // divided by max split size
        TableScanOperator top = OperatorUtils.findSingleOperatorUpstream(gop,
            TableScanOperator.class);
        // if top is null then there are multiple parents (RS as well), hence
        // lets use parent statistics to get data size. Also maxSplitSize should
        // be updated to bytes per reducer (1GB default)
        if (top == null) {
          inputSize = parentStats.getDataSize();
          maxSplitSize = HiveConf.getLongVar(conf, HiveConf.ConfVars.BYTESPERREDUCER);
        } else {
          inputSize = top.getConf().getStatistics().getDataSize();
        }
        parallelism = (int) Math.ceil((double) inputSize / maxSplitSize);
      }

      if (isDebugEnabled) {
        LOG.debug("STATS-" + gop.toString() + ": inputSize: " + inputSize + " maxSplitSize: " +
            maxSplitSize + " parallelism: " + parallelism + " containsGroupingSet: " +
            containsGroupingSet + " sizeOfGroupingSet: " + sizeOfGroupingSet);
      }

      try {
        // satisfying precondition means column statistics is available
        if (satisfyPrecondition(parentStats)) {

          // check if map side aggregation is possible or not based on column stats
          hashAgg = checkMapSideAggregation(gop, colStats, conf);

          if (isDebugEnabled) {
            LOG.debug("STATS-" + gop.toString() + " hashAgg: " + hashAgg);
          }

          stats = parentStats.clone();
          stats.setColumnStats(colStats);
          long ndvProduct = 1;
          final long parentNumRows = stats.getNumRows();

          // compute product of distinct values of grouping columns
          for (ColStatistics cs : colStats) {
            if (cs != null) {
              long ndv = cs.getCountDistint();
              if (cs.getNumNulls() > 0) {
                ndv = StatsUtils.safeAdd(ndv, 1);
              }
              ndvProduct = StatsUtils.safeMult(ndvProduct, ndv);
            } else {
              if (parentStats.getColumnStatsState().equals(Statistics.State.COMPLETE)) {
                // the column must be an aggregate column inserted by GBY. We
                // don't have to account for this column when computing product
                // of NDVs
                continue;
              } else {
                // partial column statistics on grouping attributes case.
                // if column statistics on grouping attribute is missing, then
                // assume worst case.
                // GBY rule will emit half the number of rows if ndvProduct is 0
                ndvProduct = 0;
              }
              break;
            }
          }

          // if ndvProduct is 0 then column stats state must be partial and we are missing
          // column stats for a group by column
          if (ndvProduct == 0) {
            ndvProduct = parentNumRows / 2;

            if (isDebugEnabled) {
              LOG.debug("STATS-" + gop.toString() + ": ndvProduct became 0 as some column does not" +
                  " have stats. ndvProduct changed to: " + ndvProduct);
            }
          }

          if (interReduction) {

            if (hashAgg) {
              if (containsGroupingSet) {
                // Case 4: column stats, hash aggregation, grouping sets
                cardinality = Math.min(
                    (StatsUtils.safeMult(parentNumRows, sizeOfGroupingSet)) / 2,
                    StatsUtils.safeMult(StatsUtils.safeMult(ndvProduct, parallelism), sizeOfGroupingSet));

                if (isDebugEnabled) {
                  LOG.debug("[Case 4] STATS-" + gop.toString() + ": cardinality: " + cardinality);
                }
              } else {
                // Case 3: column stats, hash aggregation, NO grouping sets
                cardinality = Math.min(parentNumRows / 2, StatsUtils.safeMult(ndvProduct, parallelism));

                if (isDebugEnabled) {
                  LOG.debug("[Case 3] STATS-" + gop.toString() + ": cardinality: " + cardinality);
                }
              }
            } else {
              if (containsGroupingSet) {
                // Case 6: column stats, NO hash aggregation, grouping sets
                cardinality = StatsUtils.safeMult(parentNumRows, sizeOfGroupingSet);

                if (isDebugEnabled) {
                  LOG.debug("[Case 6] STATS-" + gop.toString() + ": cardinality: " + cardinality);
                }
              } else {
                // Case 5: column stats, NO hash aggregation, NO grouping sets
                cardinality = parentNumRows;

                if (isDebugEnabled) {
                  LOG.debug("[Case 5] STATS-" + gop.toString() + ": cardinality: " + cardinality);
                }
              }
            }
          } else {

            // in reduce side GBY, we don't know if the grouping set was present or not. so get it
            // from map side GBY
            GroupByOperator mGop = OperatorUtils.findSingleOperatorUpstream(parent, GroupByOperator.class);
            if (mGop != null) {
              containsGroupingSet = mGop.getConf().isGroupingSetsPresent();
              sizeOfGroupingSet = mGop.getConf().getListGroupingSets().size();
            }

            if (containsGroupingSet) {
              // Case 8: column stats, grouping sets
              cardinality = Math.min(parentNumRows, StatsUtils.safeMult(ndvProduct, sizeOfGroupingSet));

              if (isDebugEnabled) {
                LOG.debug("[Case 8] STATS-" + gop.toString() + ": cardinality: " + cardinality);
              }
            } else {
              // Case 9: column stats, NO grouping sets
              cardinality = Math.min(parentNumRows, ndvProduct);

              if (isDebugEnabled) {
                LOG.debug("[Case 9] STATS-" + gop.toString() + ": cardinality: " + cardinality);
              }
            }
          }

          // update stats, but don't update NDV as it will not change
          updateStats(stats, cardinality, true, gop, false);
        } else {

          // NO COLUMN STATS
          if (parentStats != null) {

            stats = parentStats.clone();
            final long parentNumRows = stats.getNumRows();

            // if we don't have column stats, we just assume hash aggregation is disabled
            if (interReduction) {

              if (containsGroupingSet) {
                // Case 2: NO column stats, NO hash aggregation, grouping sets
                cardinality = StatsUtils.safeMult(parentNumRows, sizeOfGroupingSet);

                if (isDebugEnabled) {
                  LOG.debug("[Case 2] STATS-" + gop.toString() + ": cardinality: " + cardinality);
                }
              } else {
                // Case 1: NO column stats, NO hash aggregation, NO grouping sets
                cardinality = parentNumRows;

                if (isDebugEnabled) {
                  LOG.debug("[Case 1] STATS-" + gop.toString() + ": cardinality: " + cardinality);
                }
              }
            } else {

              // Case 7: NO column stats
              cardinality = parentNumRows / 2;

              if (isDebugEnabled) {
                LOG.debug("[Case 7] STATS-" + gop.toString() + ": cardinality: " + cardinality);
              }
            }

            updateStats(stats, cardinality, false, gop);
          }
        }

        // if UDAFs are present, new columns needs to be added
        if (!aggDesc.isEmpty() && stats != null) {
          List<ColStatistics> aggColStats = Lists.newArrayList();
          for (ColumnInfo ci : rs.getSignature()) {

            // if the columns in row schema is not contained in column
            // expression map, then those are the aggregate columns that
            // are added GBY operator. we will estimate the column statistics
            // for those newly added columns
            if (!colExprMap.containsKey(ci.getInternalName())) {
              String colName = ci.getInternalName();
              String tabAlias = ci.getTabAlias();
              String colType = ci.getTypeName();
              ColStatistics cs = new ColStatistics(tabAlias, colName, colType);
              cs.setCountDistint(stats.getNumRows());
              cs.setNumNulls(0);
              cs.setAvgColLen(StatsUtils.getAvgColLenOfFixedLengthTypes(colType));
              aggColStats.add(cs);
            }
          }

          // add the new aggregate column and recompute data size
          if (aggColStats.size() > 0) {
            stats.addToColumnStats(aggColStats);

            // only if the column stats is available, update the data size from
            // the column stats
            if (!stats.getColumnStatsState().equals(Statistics.State.NONE)) {
              updateStats(stats, stats.getNumRows(), true, gop);
            }
          }

          // if UDAF present and if column expression map is empty then it must
          // be full aggregation query like count(*) in which case number of
          // rows will be 1
          if (colExprMap.isEmpty()) {
            stats.setNumRows(1);
            updateStats(stats, 1, true, gop);
          }
        }

        gop.setStatistics(stats);

        if (isDebugEnabled && stats != null) {
          LOG.debug("[0] STATS-" + gop.toString() + ": " + stats.extendedToString());
        }
      } catch (CloneNotSupportedException e) {
        throw new SemanticException(ErrorMsg.STATISTICS_CLONING_FAILED.getMsg());
      }
      return null;
    }

    /**
     * This method does not take into account many configs used at runtime to
     * disable hash aggregation like HIVEMAPAGGRHASHMINREDUCTION. This method
     * roughly estimates the number of rows and size of each row to see if it
     * can fit in hashtable for aggregation.
     * @param gop - group by operator
     * @param colStats - column stats for key columns
     * @param conf - hive conf
     * @return
     */
    private boolean checkMapSideAggregation(GroupByOperator gop,
        List<ColStatistics> colStats, HiveConf conf) {

      List<AggregationDesc> aggDesc = gop.getConf().getAggregators();
      GroupByDesc desc = gop.getConf();
      GroupByDesc.Mode mode = desc.getMode();

      if (mode.equals(GroupByDesc.Mode.HASH)) {
        float hashAggMem = conf.getFloatVar(HiveConf.ConfVars.HIVEMAPAGGRHASHMEMORY);
        float hashAggMaxThreshold = conf.getFloatVar(HiveConf.ConfVars.HIVEMAPAGGRMEMORYTHRESHOLD);

        // get available map memory
        long totalMemory = StatsUtils.getAvailableMemory(conf) * 1000L * 1000L;
        long maxMemHashAgg = Math.round(totalMemory * hashAggMem * hashAggMaxThreshold);

        // estimated number of rows will be product of NDVs
        long numEstimatedRows = 1;

        // estimate size of key from column statistics
        long avgKeySize = 0;
        for (ColStatistics cs : colStats) {
          if (cs != null) {
            numEstimatedRows = StatsUtils.safeMult(numEstimatedRows, cs.getCountDistint());
            avgKeySize += Math.ceil(cs.getAvgColLen());
          }
        }

        // average value size will be sum of all sizes of aggregation buffers
        long avgValSize = 0;
        // go over all aggregation buffers and see they implement estimable
        // interface if so they aggregate the size of the aggregation buffer
        GenericUDAFEvaluator[] aggregationEvaluators;
        aggregationEvaluators = new GenericUDAFEvaluator[aggDesc.size()];

        // get aggregation evaluators
        for (int i = 0; i < aggregationEvaluators.length; i++) {
          AggregationDesc agg = aggDesc.get(i);
          aggregationEvaluators[i] = agg.getGenericUDAFEvaluator();
        }

        // estimate size of aggregation buffer
        for (int i = 0; i < aggregationEvaluators.length; i++) {

          // each evaluator has constant java object overhead
          avgValSize += gop.javaObjectOverHead;
          GenericUDAFEvaluator.AggregationBuffer agg = null;
          try {
            agg = aggregationEvaluators[i].getNewAggregationBuffer();
          } catch (HiveException e) {
            // in case of exception assume unknown type (256 bytes)
            avgValSize += gop.javaSizeUnknownType;
          }

          // aggregate size from aggregation buffers
          if (agg != null) {
            if (GenericUDAFEvaluator.isEstimable(agg)) {
              avgValSize += ((GenericUDAFEvaluator.AbstractAggregationBuffer) agg)
                  .estimate();
            } else {
              // if the aggregation buffer is not estimable then get all the
              // declared fields and compute the sizes from field types
              Field[] fArr = ObjectInspectorUtils
                  .getDeclaredNonStaticFields(agg.getClass());
              for (Field f : fArr) {
                long avgSize = StatsUtils
                    .getAvgColLenOfFixedLengthTypes(f.getType().getName());
                avgValSize += avgSize == 0 ? gop.javaSizeUnknownType : avgSize;
              }
            }
          }
        }

        // total size of each hash entry
        long hashEntrySize = gop.javaHashEntryOverHead + avgKeySize + avgValSize;

        // estimated hash table size
        long estHashTableSize = StatsUtils.safeMult(numEstimatedRows, hashEntrySize);

        if (estHashTableSize < maxMemHashAgg) {
          return true;
        }
      }

      // worst-case, hash aggregation disabled
      return false;
    }

    private long applyGBYRule(long numRows, long dvProd) {
      long newNumRows = numRows;

      // to avoid divide by 2 to become 0
      if (numRows > 1) {
        if (dvProd != 0) {
          newNumRows = Math.min(numRows / 2, dvProd);
        } else {
          newNumRows = numRows / 2;
        }
      }
      return newNumRows;
    }
  }

  /**
   * JOIN operator can yield any of the following three cases <li>The values of join keys are
   * disjoint in both relations in which case T(RXS) = 0 (we need histograms for this)</li> <li>Join
   * key is primary key on relation R and foreign key on relation S in which case every tuple in S
   * will have a tuple in R T(RXS) = T(S) (we need histograms for this)</li> <li>Both R & S relation
   * have same value for join-key. Ex: bool column with all true values T(RXS) = T(R) * T(S) (we
   * need histograms for this. counDistinct = 1 and same value)</li>
   * <p>
   * In the absence of histograms, we can use the following general case
   * <p>
   * <b>2 Relations, 1 attribute</b>
   * <p>
   * T(RXS) = (T(R)*T(S))/max(V(R,Y), V(S,Y)) where Y is the join attribute
   * <p>
   * <b>2 Relations, 2 attributes</b>
   * <p>
   * T(RXS) = T(R)*T(S)/max(V(R,y1), V(S,y1)) * max(V(R,y2), V(S,y2)), where y1 and y2 are the join
   * attributes
   * <p>
   * <b>3 Relations, 1 attributes</b>
   * <p>
   * T(RXSXQ) = T(R)*T(S)*T(Q)/top2largest(V(R,y), V(S,y), V(Q,y)), where y is the join attribute
   * <p>
   * <b>3 Relations, 2 attributes</b>
   * <p>
   * T(RXSXQ) = T(R)*T(S)*T(Q)/top2largest(V(R,y1), V(S,y1), V(Q,y1)) * top2largest(V(R,y2), V(S,y2), V(Q,y2)),
   * where y1 and y2 are the join attributes
   * <p>
   * <i>Worst case:</i> If no column statistics are available, then T(RXS) = joinFactor * max(T(R),
   * T(S)) * (numParents - 1) will be used as heuristics. joinFactor is from hive.stats.join.factor
   * hive config. In the worst case, since we do not know any information about join keys (and hence
   * which of the 3 cases to use), we let it to the user to provide the join factor.
   * <p>
   * <i>For more information, refer 'Estimating The Cost Of Operations' chapter in
   * "Database Systems: The Complete Book" by Garcia-Molina et. al.</i>
   * </p>
   */
  public static class JoinStatsRule extends DefaultStatsRule implements NodeProcessor {

    private boolean pkfkInferred = false;
    private long newNumRows = 0;
    private List<Operator<? extends OperatorDesc>> parents;
    private CommonJoinOperator<? extends JoinDesc> jop;
    private int numAttr = 1;

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      jop = (CommonJoinOperator<? extends JoinDesc>) nd;
      parents = jop.getParentOperators();
      AnnotateStatsProcCtx aspCtx = (AnnotateStatsProcCtx) procCtx;
      HiveConf conf = aspCtx.getConf();
      boolean allStatsAvail = true;
      boolean allSatisfyPreCondition = true;

      for (Operator<? extends OperatorDesc> op : parents) {
        if (op.getStatistics() == null) {
          allStatsAvail = false;
        }
      }

      if (allStatsAvail) {

        for (Operator<? extends OperatorDesc> op : parents) {
          if (!satisfyPrecondition(op.getStatistics())) {
            allSatisfyPreCondition = false;
          }
        }

        if (allSatisfyPreCondition) {

          // statistics object that is combination of statistics from all
          // relations involved in JOIN
          Statistics stats = new Statistics();
          Map<String, Long> rowCountParents = new HashMap<String, Long>();
          List<Long> distinctVals = Lists.newArrayList();
          int numParent = parents.size();
          Map<String, ColStatistics> joinedColStats = Maps.newHashMap();
          Map<Integer, List<String>> joinKeys = Maps.newHashMap();
          List<Long> rowCounts = Lists.newArrayList();

          // detect if there are multiple attributes in join key
          ReduceSinkOperator rsOp = (ReduceSinkOperator) jop.getParentOperators().get(0);
          List<String> keyExprs = rsOp.getConf().getOutputKeyColumnNames();
          numAttr = keyExprs.size();

          // infer PK-FK relationship in single attribute join case
          pkfkInferred = false;
          inferPKFKRelationship();

          // get the join keys from parent ReduceSink operators
          for (int pos = 0; pos < parents.size(); pos++) {
            ReduceSinkOperator parent = (ReduceSinkOperator) jop.getParentOperators().get(pos);

            Statistics parentStats = parent.getStatistics();
            keyExprs = parent.getConf().getOutputKeyColumnNames();

            // Parent RS may have column statistics from multiple parents.
            // Populate table alias to row count map, this will be used later to
            // scale down/up column statistics based on new row count
            // NOTE: JOIN with UNION as parent of RS will not have table alias
            // propagated properly. UNION operator does not propagate the table
            // alias of subqueries properly to expression nodes. Hence union20.q
            // will have wrong number of rows.
            Set<String> tableAliases = StatsUtils.getAllTableAlias(parent.getColumnExprMap());
            for (String tabAlias : tableAliases) {
              rowCountParents.put(tabAlias, parentStats.getNumRows());
            }
            rowCounts.add(parentStats.getNumRows());

            // compute fully qualified join key column names. this name will be
            // used to quickly look-up for column statistics of join key.
            // TODO: expressions in join condition will be ignored. assign
            // internal name for expressions and estimate column statistics for expression.
            List<String> fqCols = StatsUtils.getFullyQualifedReducerKeyNames(keyExprs,
                parent.getColumnExprMap());
            joinKeys.put(pos, fqCols);

            // get column statistics for all output columns
            for (ColStatistics cs : parentStats.getColumnStats()) {
              joinedColStats.put(cs.getFullyQualifiedColName(), cs);
            }

            // since new statistics is derived from all relations involved in
            // JOIN, we need to update the state information accordingly
            stats.updateColumnStatsState(parentStats.getColumnStatsState());
          }

          // compute denominator i.e, max(V(R,Y), V(S,Y)) in case of single
          // attribute join, else max(V(R,y1), V(S,y1)) * max(V(R,y2), V(S,y2))
          // in case of multi-attribute join
          long denom = 1;
          if (numAttr > 1) {
            List<Long> perAttrDVs = Lists.newArrayList();
            for (int idx = 0; idx < numAttr; idx++) {
              for (Integer i : joinKeys.keySet()) {
                String col = joinKeys.get(i).get(idx);
                ColStatistics cs = joinedColStats.get(col);
                if (cs != null) {
                  perAttrDVs.add(cs.getCountDistint());
                }
              }

              distinctVals.add(getDenominator(perAttrDVs));
              perAttrDVs.clear();
            }

            if (numAttr > numParent) {
              // To avoid denominator getting larger and aggressively reducing
              // number of rows, we will ease out denominator.
              denom = getEasedOutDenominator(distinctVals);
            } else {
              for (Long l : distinctVals) {
                denom = StatsUtils.safeMult(denom, l);
              }
            }
          } else {
            for (List<String> jkeys : joinKeys.values()) {
              for (String jk : jkeys) {
                ColStatistics cs = joinedColStats.get(jk);
                if (cs != null) {
                  distinctVals.add(cs.getCountDistint());
                }
              }
            }
            denom = getDenominator(distinctVals);
          }

          // Update NDV of joined columns to be min(V(R,y), V(S,y))
          updateJoinColumnsNDV(joinKeys, joinedColStats, numAttr);

          // column statistics from different sources are put together and rename
          // fully qualified column names based on output schema of join operator
          Map<String, ExprNodeDesc> colExprMap = jop.getColumnExprMap();
          RowSchema rs = jop.getSchema();
          List<ColStatistics> outColStats = Lists.newArrayList();
          Map<String, String> outInTabAlias = new HashMap<String, String>();
          for (ColumnInfo ci : rs.getSignature()) {
            String key = ci.getInternalName();
            ExprNodeDesc end = colExprMap.get(key);
            if (end instanceof ExprNodeColumnDesc) {
              String colName = ((ExprNodeColumnDesc) end).getColumn();
              String tabAlias = ((ExprNodeColumnDesc) end).getTabAlias();
              String fqColName = StatsUtils.getFullyQualifiedColumnName(tabAlias, colName);
              ColStatistics cs = joinedColStats.get(fqColName);
              String outColName = key;
              String outTabAlias = ci.getTabAlias();
              outInTabAlias.put(outTabAlias, tabAlias);
              if (cs != null) {
                cs.setColumnName(outColName);
                cs.setTableAlias(outTabAlias);
              }
              outColStats.add(cs);
            }
          }

          // update join statistics
          stats.setColumnStats(outColStats);
          long newRowCount = pkfkInferred ? newNumRows : computeNewRowCount(rowCounts, denom);
          updateStatsForJoinType(stats, newRowCount, jop, rowCountParents,outInTabAlias);
          jop.setStatistics(stats);

          if (isDebugEnabled) {
            LOG.debug("[0] STATS-" + jop.toString() + ": " + stats.extendedToString());
          }
        } else {

          // worst case when there are no column statistics
          float joinFactor = HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVE_STATS_JOIN_FACTOR);
          int numParents = parents.size();
          List<Long> parentRows = Lists.newArrayList();
          List<Long> parentSizes = Lists.newArrayList();
          int maxRowIdx = 0;
          long maxRowCount = 0;
          int idx = 0;

          for (Operator<? extends OperatorDesc> op : parents) {
            Statistics ps = op.getStatistics();
            long rowCount = ps.getNumRows();
            if (rowCount > maxRowCount) {
              maxRowCount = rowCount;
              maxRowIdx = idx;
            }
            parentRows.add(rowCount);
            parentSizes.add(ps.getDataSize());
            idx++;
          }

          long maxDataSize = parentSizes.get(maxRowIdx);
          long newNumRows = StatsUtils.safeMult(StatsUtils.safeMult(maxRowCount, (numParents - 1)), joinFactor);
          long newDataSize = StatsUtils.safeMult(StatsUtils.safeMult(maxDataSize, (numParents - 1)), joinFactor);
          Statistics wcStats = new Statistics();
          wcStats.setNumRows(newNumRows);
          wcStats.setDataSize(newDataSize);
          jop.setStatistics(wcStats);
 
          if (isDebugEnabled) {
            LOG.debug("[1] STATS-" + jop.toString() + ": " + wcStats.extendedToString());
          }
        }
      }
      return null;
    }

    private void inferPKFKRelationship() {
      if (numAttr == 1) {
        List<Integer> parentsWithPK = getPrimaryKeyCandidates(parents);

        // in case of fact to many dimensional tables join, the join key in fact table will be
        // mostly foreign key which will have corresponding primary key in dimension table.
        // The selectivity of fact table in that case will be product of all selectivities of
        // dimension tables (assumes conjunctivity)
        for (Integer id : parentsWithPK) {
          ColStatistics csPK = null;
          Operator<? extends OperatorDesc> parent = parents.get(id);
          for (ColStatistics cs : parent.getStatistics().getColumnStats()) {
            if (cs.isPrimaryKey()) {
              csPK = cs;
              break;
            }
          }

          // infer foreign key candidates positions
          List<Integer> parentsWithFK = getForeignKeyCandidates(parents, csPK);
          if (parentsWithFK.size() == 1 &&
              parentsWithFK.size() + parentsWithPK.size() == parents.size()) {
            Operator<? extends OperatorDesc> parentWithFK = parents.get(parentsWithFK.get(0));
            List<Float> parentsSel = getSelectivity(parents, parentsWithPK);
            Float prodSelectivity = 1.0f;
            for (Float selectivity : parentsSel) {
              prodSelectivity *= selectivity;
            }
            newNumRows = (long) Math.ceil(
                parentWithFK.getStatistics().getNumRows() * prodSelectivity);
            pkfkInferred = true;

            // some debug information
            if (isDebugEnabled) {
              List<String> parentIds = Lists.newArrayList();

              // print primary key containing parents
              for (Integer i : parentsWithPK) {
                parentIds.add(parents.get(i).toString());
              }
              LOG.debug("STATS-" + jop.toString() + ": PK parent id(s) - " + parentIds);
              parentIds.clear();

              // print foreign key containing parents
              for (Integer i : parentsWithFK) {
                parentIds.add(parents.get(i).toString());
              }
              LOG.debug("STATS-" + jop.toString() + ": FK parent id(s) - " + parentIds);
            }
          }
        }
      }
    }

    /**
     * Get selectivity of reduce sink operators.
     * @param ops - reduce sink operators
     * @param opsWithPK - reduce sink operators with primary keys
     * @return - list of selectivity for primary key containing operators
     */
    private List<Float> getSelectivity(List<Operator<? extends OperatorDesc>> ops,
        List<Integer> opsWithPK) {
      List<Float> result = Lists.newArrayList();
      for (Integer idx : opsWithPK) {
        Operator<? extends OperatorDesc> op = ops.get(idx);
        float selectivity = getSelectivitySimpleTree(op);
        result.add(selectivity);
      }
      return result;
    }

    private float getSelectivitySimpleTree(Operator<? extends OperatorDesc> op) {
      TableScanOperator tsOp = OperatorUtils
          .findSingleOperatorUpstream(op, TableScanOperator.class);
      if (tsOp == null) {
        // complex tree with multiple parents
        return getSelectivityComplexTree(op);
      } else {
        // simple tree with single parent
        long inputRow = tsOp.getStatistics().getNumRows();
        long outputRow = op.getStatistics().getNumRows();
        return (float) outputRow / (float) inputRow;
      }
    }

    private float getSelectivityComplexTree(Operator<? extends OperatorDesc> op) {
      Operator<? extends OperatorDesc> multiParentOp = null;
      Operator<? extends OperatorDesc> currentOp = op;

      // TS-1      TS-2
      //  |          |
      // RS-1      RS-2
      //    \      /
      //      JOIN
      //        |
      //       FIL
      //        |
      //       RS-3
      //
      // For the above complex operator tree,
      // selectivity(JOIN) = selectivity(RS-1) * selectivity(RS-2) and
      // selectivity(RS-3) = numRows(RS-3)/numRows(JOIN) * selectivity(JOIN)
      while(multiParentOp == null) {
        if (op.getParentOperators().size() > 1) {
          multiParentOp = op;
        } else {
          op = op.getParentOperators().get(0);
        }
      }

      // No need for overflow checks, assume selectivity is always <= 1.0
      float selMultiParent = 1.0f;
      for(Operator<? extends OperatorDesc> parent : multiParentOp.getParentOperators()) {
        // In the above example, TS-1 -> RS-1 and TS-2 -> RS-2 are simple trees
        selMultiParent *= getSelectivitySimpleTree(parent);
      }

      float selCurrOp = ((float) currentOp.getStatistics().getNumRows() /
          (float) multiParentOp.getStatistics().getNumRows()) * selMultiParent;

      return selCurrOp;
    }

    /**
     * Returns the index of parents whose join key column statistics ranges are within the specified
     * primary key range (inferred as foreign keys).
     * @param ops - operators
     * @param csPK - column statistics of primary key
     * @return - list of foreign key containing parent ids
     */
    private List<Integer> getForeignKeyCandidates(List<Operator<? extends OperatorDesc>> ops,
        ColStatistics csPK) {
      List<Integer> result = Lists.newArrayList();
      if (csPK == null || ops == null) {
        return result;
      }

      for (int i = 0; i < ops.size(); i++) {
        Operator<? extends OperatorDesc> op = ops.get(i);
        if (op != null && op instanceof ReduceSinkOperator) {
          ReduceSinkOperator rsOp = (ReduceSinkOperator) op;
          List<String> keys = rsOp.getConf().getOutputKeyColumnNames();
          List<String> fqCols = StatsUtils.getFullyQualifedReducerKeyNames(keys,
              rsOp.getColumnExprMap());
          if (fqCols.size() == 1) {
            String joinCol = fqCols.get(0);
            if (rsOp.getStatistics() != null) {
              ColStatistics cs = rsOp.getStatistics().getColumnStatisticsFromFQColName(joinCol);
              if (cs != null && !cs.isPrimaryKey()) {
                if (StatsUtils.inferForeignKey(csPK, cs)) {
                  result.add(i);
                }
              }
            }
          }
        }
      }
      return result;
    }

    /**
     * Returns the index of parents whose join key columns are infer as primary keys
     * @param ops - operators
     * @return - list of primary key containing parent ids
     */
    private List<Integer> getPrimaryKeyCandidates(List<Operator<? extends OperatorDesc>> ops) {
      List<Integer> result = Lists.newArrayList();
      if (ops != null && !ops.isEmpty()) {
        for (int i = 0; i < ops.size(); i++) {
          Operator<? extends OperatorDesc> op = ops.get(i);
          if (op instanceof ReduceSinkOperator) {
            ReduceSinkOperator rsOp = (ReduceSinkOperator) op;
            List<String> keys = rsOp.getConf().getOutputKeyColumnNames();
            List<String> fqCols = StatsUtils.getFullyQualifedReducerKeyNames(keys,
                rsOp.getColumnExprMap());
            if (fqCols.size() == 1) {
              String joinCol = fqCols.get(0);
              if (rsOp.getStatistics() != null) {
                ColStatistics cs = rsOp.getStatistics().getColumnStatisticsFromFQColName(joinCol);
                if (cs != null && cs.isPrimaryKey()) {
                  result.add(i);
                }
              }
            }
          }
        }
      }
      return result;
    }

    private Long getEasedOutDenominator(List<Long> distinctVals) {
      // Exponential back-off for NDVs.
      // 1) Descending order sort of NDVs
      // 2) denominator = NDV1 * (NDV2 ^ (1/2)) * (NDV3 ^ (1/4))) * ....
      Collections.sort(distinctVals, Collections.reverseOrder());

      long denom = distinctVals.get(0);
      for (int i = 1; i < distinctVals.size(); i++) {
        denom = (long) (denom * Math.pow(distinctVals.get(i), 1.0 / (1 << i)));
      }

      return denom;
    }

    private void updateStatsForJoinType(Statistics stats, long newNumRows,
        CommonJoinOperator<? extends JoinDesc> jop,
        Map<String, Long> rowCountParents,
        Map<String, String> outInTabAlias) {

      if (newNumRows < 0) {
        LOG.info("STATS-" + jop.toString() + ": Overflow in number of rows."
          + newNumRows + " rows will be set to Long.MAX_VALUE");
      }
      newNumRows = StatsUtils.getMaxIfOverflow(newNumRows);
      stats.setNumRows(newNumRows);

      // scale down/up the column statistics based on the changes in number of
      // rows from each parent. For ex: If there are 2 parents for JOIN operator
      // with 1st parent having 200 rows and 2nd parent having 2000 rows. Now if
      // the new number of rows after applying join rule is 10, then the column
      // stats for columns from 1st parent should be scaled down by 200/10 = 20x
      // and stats for columns from 2nd parent should be scaled down by 200x
      List<ColStatistics> colStats = stats.getColumnStats();
      for (ColStatistics cs : colStats) {
        long oldRowCount = rowCountParents.get(outInTabAlias.get(cs.getTableAlias()));
        double ratio = (double) newNumRows / (double) oldRowCount;
        long oldDV = cs.getCountDistint();
        long newDV = oldDV;

        // if ratio is greater than 1, then number of rows increases. This can happen
        // when some operators like GROUPBY duplicates the input rows in which case
        // number of distincts should not change. Update the distinct count only when
        // the output number of rows is less than input number of rows.
        if (ratio <= 1.0) {
          newDV = (long) Math.ceil(ratio * oldDV);
        }
        // Assumes inner join
        // TODO: HIVE-5579 will handle different join types
        cs.setNumNulls(0);
        cs.setCountDistint(newDV);
      }
      stats.setColumnStats(colStats);
      long newDataSize = StatsUtils
          .getDataSizeFromColumnStats(newNumRows, colStats);
      stats.setDataSize(StatsUtils.getMaxIfOverflow(newDataSize));
    }

    private long computeNewRowCount(List<Long> rowCountParents, long denom) {
      double factor = 0.0d;
      long result = 1;
      long max = rowCountParents.get(0);
      long maxIdx = 0;

      // To avoid long overflow, we will divide the max row count by denominator
      // and use that factor to multiply with other row counts
      for (int i = 1; i < rowCountParents.size(); i++) {
        if (rowCountParents.get(i) > max) {
          max = rowCountParents.get(i);
          maxIdx = i;
        }
      }

      factor = (double) max / (double) denom;

      for (int i = 0; i < rowCountParents.size(); i++) {
        if (i != maxIdx) {
          result = StatsUtils.safeMult(result, rowCountParents.get(i));
        }
      }

      result = (long) (result * factor);

      return result;
    }

    private void updateJoinColumnsNDV(Map<Integer, List<String>> joinKeys,
        Map<String, ColStatistics> joinedColStats, int numAttr) {
      int joinColIdx = 0;
      while (numAttr > 0) {
        long minNDV = Long.MAX_VALUE;

        // find min NDV for joining columns
        for (Map.Entry<Integer, List<String>> entry : joinKeys.entrySet()) {
          String key = entry.getValue().get(joinColIdx);
          ColStatistics cs = joinedColStats.get(key);
          if (cs != null && cs.getCountDistint() < minNDV) {
            minNDV = cs.getCountDistint();
          }
        }

        // set min NDV value to both columns involved in join
        if (minNDV != Long.MAX_VALUE) {
          for (Map.Entry<Integer, List<String>> entry : joinKeys.entrySet()) {
            String key = entry.getValue().get(joinColIdx);
            ColStatistics cs = joinedColStats.get(key);
            if (cs != null) {
              cs.setCountDistint(minNDV);
            }
          }
        }

        joinColIdx++;
        numAttr--;
      }
    }

    private long getDenominator(List<Long> distinctVals) {

      if (distinctVals.isEmpty()) {

        // TODO: in union20.q the tab alias is not properly propagated down the
        // operator tree. This happens when UNION ALL is used as sub query. Hence, even
        // if column statistics are available, the tab alias will be null which will fail
        // to get proper column statistics. For now assume, worst case in which
        // denominator is 2.
        return 2;
      }

      // simple join from 2 relations: denom = max(v1, v2)
      if (distinctVals.size() <= 2) {
        return Collections.max(distinctVals);
      } else {

        // remember min value and ignore it from the denominator
        long minNDV = distinctVals.get(0);
        int minIdx = 0;

        for (int i = 1; i < distinctVals.size(); i++) {
          if (distinctVals.get(i) < minNDV) {
            minNDV = distinctVals.get(i);
            minIdx = i;
          }
        }

        // join from multiple relations:
        // denom = Product of all NDVs except the least of all
        long denom = 1;
        for (int i = 0; i < distinctVals.size(); i++) {
          if (i != minIdx) {
            denom = StatsUtils.safeMult(denom, distinctVals.get(i));
          }
        }
        return denom;
      }
    }

  }

  /**
   * LIMIT operator changes the number of rows and thereby the data size.
   */
  public static class LimitStatsRule extends DefaultStatsRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      LimitOperator lop = (LimitOperator) nd;
      Operator<? extends OperatorDesc> parent = lop.getParentOperators().get(0);
      Statistics parentStats = parent.getStatistics();

      try {
        long limit = -1;
        limit = lop.getConf().getLimit();

        if (satisfyPrecondition(parentStats)) {
          Statistics stats = parentStats.clone();

          // if limit is greater than available rows then do not update
          // statistics
          if (limit <= parentStats.getNumRows()) {
            updateStats(stats, limit, true, lop);
          }
          lop.setStatistics(stats);

          if (isDebugEnabled) {
            LOG.debug("[0] STATS-" + lop.toString() + ": " + stats.extendedToString());
          }
        } else {
          if (parentStats != null) {

            // in the absence of column statistics, compute data size based on
            // based on average row size
            Statistics wcStats = parentStats.clone();
            limit = StatsUtils.getMaxIfOverflow(limit);
            if (limit <= parentStats.getNumRows()) {
              long numRows = limit;
              long avgRowSize = parentStats.getAvgRowSize();
              long dataSize = StatsUtils.safeMult(avgRowSize, limit);
              wcStats.setNumRows(numRows);
              wcStats.setDataSize(dataSize);
            }
            lop.setStatistics(wcStats);

            if (isDebugEnabled) {
              LOG.debug("[1] STATS-" + lop.toString() + ": " + wcStats.extendedToString());
            }
          }
        }
      } catch (CloneNotSupportedException e) {
        throw new SemanticException(ErrorMsg.STATISTICS_CLONING_FAILED.getMsg());
      }
      return null;
    }

  }

  /**
   * ReduceSink operator does not change any of the statistics. But it renames
   * the column statistics from its parent based on the output key and value
   * column names to make it easy for the downstream operators. This is different
   * from the default stats which just aggregates and passes along the statistics
   * without actually renaming based on output schema of the operator.
   */
  public static class ReduceSinkStatsRule extends DefaultStatsRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ReduceSinkOperator rop = (ReduceSinkOperator) nd;
      Operator<? extends OperatorDesc> parent = rop.getParentOperators().get(0);
      Statistics parentStats = parent.getStatistics();
      if (parentStats != null) {
        AnnotateStatsProcCtx aspCtx = (AnnotateStatsProcCtx) procCtx;
        HiveConf conf = aspCtx.getConf();

        List<String> outKeyColNames = rop.getConf().getOutputKeyColumnNames();
        List<String> outValueColNames = rop.getConf().getOutputValueColumnNames();
        Map<String, ExprNodeDesc> colExprMap = rop.getColumnExprMap();
        try {
          Statistics outStats = parentStats.clone();
          if (satisfyPrecondition(parentStats)) {
            List<ColStatistics> colStats = Lists.newArrayList();
            for (String key : outKeyColNames) {
              String prefixedKey = Utilities.ReduceField.KEY.toString() + "." + key;
              ExprNodeDesc end = colExprMap.get(prefixedKey);
              if (end != null) {
                ColStatistics cs = StatsUtils
                    .getColStatisticsFromExpression(conf, parentStats, end);
                if (cs != null) {
                  cs.setColumnName(prefixedKey);
                  colStats.add(cs);
                }
              }
            }

            for (String val : outValueColNames) {
              String prefixedVal = Utilities.ReduceField.VALUE.toString() + "." + val;
              ExprNodeDesc end = colExprMap.get(prefixedVal);
              if (end != null) {
                ColStatistics cs = StatsUtils
                    .getColStatisticsFromExpression(conf, parentStats, end);
                if (cs != null) {
                  cs.setColumnName(prefixedVal);
                  colStats.add(cs);
                }
              }
            }

            outStats.setColumnStats(colStats);
          }
          rop.setStatistics(outStats);
          if (isDebugEnabled) {
            LOG.debug("[0] STATS-" + rop.toString() + ": " + outStats.extendedToString());
          }
        } catch (CloneNotSupportedException e) {
          throw new SemanticException(ErrorMsg.STATISTICS_CLONING_FAILED.getMsg());
        }
      }
      return null;
    }

  }

  /**
   * Default rule is to aggregate the statistics from all its parent operators.
   */
  public static class DefaultStatsRule implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      OperatorDesc conf = op.getConf();
      AnnotateStatsProcCtx aspCtx = (AnnotateStatsProcCtx) procCtx;
      HiveConf hconf = aspCtx.getConf();

      if (conf != null) {
        Statistics stats = conf.getStatistics();
        if (stats == null) {
          if (op.getParentOperators() != null) {

            // if parent statistics is null then that branch of the tree is not
            // walked yet. don't update the stats until all branches are walked
            if (isAllParentsContainStatistics(op)) {
              stats = new Statistics();
              for (Operator<? extends OperatorDesc> parent : op.getParentOperators()) {
                if (parent.getStatistics() != null) {
                  Statistics parentStats = parent.getStatistics();
                  stats.addToNumRows(parentStats.getNumRows());
                  stats.addToDataSize(parentStats.getDataSize());
                  stats.updateColumnStatsState(parentStats.getColumnStatsState());
                  List<ColStatistics> colStats = StatsUtils.getColStatisticsFromExprMap(hconf,
                      parentStats, op.getColumnExprMap(), op.getSchema());
                  stats.addToColumnStats(colStats);
                  op.getConf().setStatistics(stats);

                  if (isDebugEnabled) {
                    LOG.debug("[0] STATS-" + op.toString() + ": " + stats.extendedToString());
                  }
                }
              }
            }
          }
        }
      }
      return null;
    }

    // check if all parent statistics are available
    private boolean isAllParentsContainStatistics(Operator<? extends OperatorDesc> op) {
      for (Operator<? extends OperatorDesc> parent : op.getParentOperators()) {
        if (parent.getStatistics() == null) {
          return false;
        }
      }
      return true;
    }

  }

  public static NodeProcessor getTableScanRule() {
    return new TableScanStatsRule();
  }

  public static NodeProcessor getSelectRule() {
    return new SelectStatsRule();
  }

  public static NodeProcessor getFilterRule() {
    return new FilterStatsRule();
  }

  public static NodeProcessor getGroupByRule() {
    return new GroupByStatsRule();
  }

  public static NodeProcessor getJoinRule() {
    return new JoinStatsRule();
  }

  public static NodeProcessor getLimitRule() {
    return new LimitStatsRule();
  }

  public static NodeProcessor getReduceSinkRule() {
    return new ReduceSinkStatsRule();
  }

  public static NodeProcessor getDefaultRule() {
    return new DefaultStatsRule();
  }


  /**
   * Update the basic statistics of the statistics object based on the row number
   * @param stats
   *          - statistics to be updated
   * @param newNumRows
   *          - new number of rows
   * @param useColStats
   *          - use column statistics to compute data size
   */
  static void updateStats(Statistics stats, long newNumRows,
      boolean useColStats, Operator<? extends OperatorDesc> op) {
    updateStats(stats, newNumRows, useColStats, op, true);
  }

  static void updateStats(Statistics stats, long newNumRows,
      boolean useColStats, Operator<? extends OperatorDesc> op,
      boolean updateNDV) {

    if (newNumRows < 0) {
      LOG.info("STATS-" + op.toString() + ": Overflow in number of rows."
          + newNumRows + " rows will be set to Long.MAX_VALUE");
    }

    newNumRows = StatsUtils.getMaxIfOverflow(newNumRows);
    long oldRowCount = stats.getNumRows();
    double ratio = (double) newNumRows / (double) oldRowCount;
    stats.setNumRows(newNumRows);

    if (useColStats) {
      List<ColStatistics> colStats = stats.getColumnStats();
      for (ColStatistics cs : colStats) {
        long oldNumNulls = cs.getNumNulls();
        long oldDV = cs.getCountDistint();
        long newNumNulls = Math.round(ratio * oldNumNulls);
        cs.setNumNulls(newNumNulls);
        if (updateNDV) {
          long newDV = oldDV;

          // if ratio is greater than 1, then number of rows increases. This can happen
          // when some operators like GROUPBY duplicates the input rows in which case
          // number of distincts should not change. Update the distinct count only when
          // the output number of rows is less than input number of rows.
          if (ratio <= 1.0) {
            newDV = (long) Math.ceil(ratio * oldDV);
          }
          cs.setCountDistint(newDV);
        }
      }
      stats.setColumnStats(colStats);
      long newDataSize = StatsUtils.getDataSizeFromColumnStats(newNumRows, colStats);
      stats.setDataSize(StatsUtils.getMaxIfOverflow(newDataSize));
    } else {
      long newDataSize = (long) (ratio * stats.getDataSize());
      stats.setDataSize(StatsUtils.getMaxIfOverflow(newDataSize));
    }
  }

  static boolean satisfyPrecondition(Statistics stats) {
    return stats != null && stats.getBasicStatsState().equals(Statistics.State.COMPLETE)
        && !stats.getColumnStatsState().equals(Statistics.State.NONE);
  }
}
