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
import org.apache.hadoop.hive.ql.exec.AppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
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
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

public class StatsRulesProcFactory {

  private static final Log LOG = LogFactory.getLog(StatsRulesProcFactory.class.getName());

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
      Table table = aspCtx.getParseContext().getTopToTable().get(tsop);

      try {
        // gather statistics for the first time and the attach it to table scan operator
        Statistics stats = StatsUtils.collectStatistics(aspCtx.getConf(), partList, table, tsop);
        tsop.setStatistics(stats.clone());

        if (LOG.isDebugEnabled()) {
          LOG.debug("[0] STATS-" + tsop.toString() + " (" + table.getTableName()
              + "): " + stats.extendedToString());
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

      // SELECT (*) does not change the statistics. Just pass on the parent statistics
      if (sop.getConf().isSelectStar()) {
        try {
          if (parentStats != null) {
            sop.setStatistics(parentStats.clone());
          }
        } catch (CloneNotSupportedException e) {
          throw new SemanticException(ErrorMsg.STATISTICS_CLONING_FAILED.getMsg());
        }
        return null;
      }

      try {
        if (satisfyPrecondition(parentStats)) {
          Statistics stats = parentStats.clone();
          List<ColStatistics> colStats =
              StatsUtils.getColStatisticsFromExprMap(conf, parentStats, sop.getColumnExprMap(),
                  sop.getSchema());
          long dataSize = StatsUtils.getDataSizeFromColumnStats(stats.getNumRows(), colStats);
          stats.setColumnStats(colStats);
          stats.setDataSize(setMaxIfInvalid(dataSize));
          sop.setStatistics(stats);

          if (LOG.isDebugEnabled()) {
            LOG.debug("[0] STATS-" + sop.toString() + ": " + stats.extendedToString());
          }
        } else {
          if (parentStats != null) {
            sop.setStatistics(parentStats.clone());

            if (LOG.isDebugEnabled()) {
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

            if (LOG.isDebugEnabled()) {
              LOG.debug("[0] STATS-" + fop.toString() + ": " + st.extendedToString());
            }
          } else {

            // update only the basic statistics in the absence of column statistics
            if (newNumRows <= parentStats.getNumRows()) {
              updateStats(st, newNumRows, false, fop);
            }

            if (LOG.isDebugEnabled()) {
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
            newNumRows += evaluateChildExpr(stats, child, aspCtx, neededCols,
                fop);
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
      AnnotateStatsProcCtx aspCtx = (AnnotateStatsProcCtx) procCtx;
      HiveConf conf = aspCtx.getConf();
      int mapSideParallelism =
          HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_STATS_MAP_SIDE_PARALLELISM);
      List<AggregationDesc> aggDesc = gop.getConf().getAggregators();
      Map<String, ExprNodeDesc> colExprMap = gop.getColumnExprMap();
      RowSchema rs = gop.getSchema();
      Statistics stats = null;

      try {
        if (satisfyPrecondition(parentStats)) {
          stats = parentStats.clone();

          List<ColStatistics> colStats =
              StatsUtils.getColStatisticsFromExprMap(conf, parentStats, colExprMap, rs);
          stats.setColumnStats(colStats);
          long dvProd = 1;
          long newNumRows = 0;

          // compute product of distinct values of grouping columns
          for (ColStatistics cs : colStats) {
            if (cs != null) {
              long dv = cs.getCountDistint();
              if (cs.getNumNulls() > 0) {
                dv += 1;
              }
              dvProd *= dv;
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
                // GBY rule will emit half the number of rows if dvProd is 0
                dvProd = 0;
              }
              break;
            }
          }

          // map side
          if (gop.getChildOperators().get(0) instanceof ReduceSinkOperator ||
              gop.getChildOperators().get(0) instanceof AppMasterEventOperator) {

            // since we do not know if hash-aggregation will be enabled or disabled
            // at runtime we will assume that map-side group by does not do any
            // reduction.hence no group by rule will be applied

            // map-side grouping set present. if grouping set is present then
            // multiply the number of rows by number of elements in grouping set
            if (gop.getConf().isGroupingSetsPresent()) {
              int multiplier = gop.getConf().getListGroupingSets().size();

              // take into account the map-side parallelism as well, default is 1
              multiplier *= mapSideParallelism;
              newNumRows = setMaxIfInvalid(multiplier * stats.getNumRows());
              long dataSize = setMaxIfInvalid(multiplier * stats.getDataSize());
              stats.setNumRows(newNumRows);
              stats.setDataSize(dataSize);
              for (ColStatistics cs : colStats) {
                if (cs != null) {
                  long oldNumNulls = cs.getNumNulls();
                  long newNumNulls = multiplier * oldNumNulls;
                  cs.setNumNulls(newNumNulls);
                }
              }
            } else {

              // map side no grouping set
              newNumRows = stats.getNumRows() * mapSideParallelism;
              updateStats(stats, newNumRows, true, gop);
            }
          } else {

            // reduce side
            newNumRows = applyGBYRule(stats.getNumRows(), dvProd);
            updateStats(stats, newNumRows, true, gop);
          }
        } else {
          if (parentStats != null) {

            // worst case, in the absence of column statistics assume half the rows are emitted
            if (gop.getChildOperators().get(0) instanceof ReduceSinkOperator
                || gop.getChildOperators().get(0) instanceof AppMasterEventOperator) {

              // map side
              stats = parentStats.clone();
            } else {

              // reduce side
              stats = parentStats.clone();
              long newNumRows = parentStats.getNumRows() / 2;
              updateStats(stats, newNumRows, false, gop);
            }
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
              colName = StatsUtils.stripPrefixFromColumnName(colName);
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

        if (LOG.isDebugEnabled() && stats != null) {
          LOG.debug("[0] STATS-" + gop.toString() + ": " + stats.extendedToString());
        }
      } catch (CloneNotSupportedException e) {
        throw new SemanticException(ErrorMsg.STATISTICS_CLONING_FAILED.getMsg());
      }
      return null;
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

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      CommonJoinOperator<? extends JoinDesc> jop = (CommonJoinOperator<? extends JoinDesc>) nd;
      List<Operator<? extends OperatorDesc>> parents = jop.getParentOperators();
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

          // 2 relations, multiple attributes
          boolean multiAttr = false;
          int numAttr = 1;
          int numParent = parents.size();

          Map<String, ColStatistics> joinedColStats = Maps.newHashMap();
          Map<Integer, List<String>> joinKeys = Maps.newHashMap();

          // get the join keys from parent ReduceSink operators
          for (int pos = 0; pos < parents.size(); pos++) {
            ReduceSinkOperator parent = (ReduceSinkOperator) jop.getParentOperators().get(pos);

            Statistics parentStats = parent.getStatistics();
            List<ExprNodeDesc> keyExprs = parent.getConf().getKeyCols();

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

            // multi-attribute join key
            if (keyExprs.size() > 1) {
              multiAttr = true;
              numAttr = keyExprs.size();
            }

            // compute fully qualified join key column names. this name will be
            // used to quickly look-up for column statistics of join key.
            // TODO: expressions in join condition will be ignored. assign
            // internal name for expressions and estimate column statistics for expression.
            List<String> fqCols =
                StatsUtils.getFullQualifedColNameFromExprs(keyExprs, parent.getColumnExprMap());
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
          if (multiAttr) {
            List<Long> perAttrDVs = Lists.newArrayList();
            for (int idx = 0; idx < numAttr; idx++) {
              for (Integer i : joinKeys.keySet()) {
                String col = joinKeys.get(i).get(idx);
                col = StatsUtils.stripPrefixFromColumnName(col);
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
                denom *= l;
              }
            }
          } else {
            for (List<String> jkeys : joinKeys.values()) {
              for (String jk : jkeys) {
                jk = StatsUtils.stripPrefixFromColumnName(jk);
                ColStatistics cs = joinedColStats.get(jk);
                if (cs != null) {
                  distinctVals.add(cs.getCountDistint());
                }
              }
            }
            denom = getDenominator(distinctVals);
          }

          // Update NDV of joined columns to be min(V(R,y), V(S,y))
          if (multiAttr) {
            updateJoinColumnsNDV(joinKeys, joinedColStats, numAttr);
          }

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
              colName = StatsUtils.stripPrefixFromColumnName(colName);
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
          long newRowCount = computeNewRowCount(
              Lists.newArrayList(rowCountParents.values()), denom);

          updateStatsForJoinType(stats, newRowCount, jop, rowCountParents,
              outInTabAlias);
          jop.setStatistics(stats);

          if (LOG.isDebugEnabled()) {
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
          long newNumRows = (long) (joinFactor * maxRowCount * (numParents - 1));
          long newDataSize = (long) (joinFactor * maxDataSize * (numParents - 1));
          Statistics wcStats = new Statistics();
          wcStats.setNumRows(setMaxIfInvalid(newNumRows));
          wcStats.setDataSize(setMaxIfInvalid(newDataSize));
          jop.setStatistics(wcStats);

          if (LOG.isDebugEnabled()) {
            LOG.debug("[1] STATS-" + jop.toString() + ": " + wcStats.extendedToString());
          }
        }
      }
      return null;
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

      if (newNumRows <= 0) {
        LOG.info("STATS-" + jop.toString() + ": Overflow in number of rows."
          + newNumRows + " rows will be set to Long.MAX_VALUE");
      }
      newNumRows = setMaxIfInvalid(newNumRows);
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
      stats.setDataSize(setMaxIfInvalid(newDataSize));
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
          result *= rowCountParents.get(i);
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
          key = StatsUtils.stripPrefixFromColumnName(key);
          ColStatistics cs = joinedColStats.get(key);
          if (cs != null && cs.getCountDistint() < minNDV) {
            minNDV = cs.getCountDistint();
          }
        }

        // set min NDV value to both columns involved in join
        if (minNDV != Long.MAX_VALUE) {
          for (Map.Entry<Integer, List<String>> entry : joinKeys.entrySet()) {
            String key = entry.getValue().get(joinColIdx);
            key = StatsUtils.stripPrefixFromColumnName(key);
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
            denom *= distinctVals.get(i);
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

          if (LOG.isDebugEnabled()) {
            LOG.debug("[0] STATS-" + lop.toString() + ": " + stats.extendedToString());
          }
        } else {
          if (parentStats != null) {

            // in the absence of column statistics, compute data size based on
            // based on average row size
            Statistics wcStats = parentStats.clone();
            if (limit <= parentStats.getNumRows()) {
              long numRows = limit;
              long avgRowSize = parentStats.getAvgRowSize();
              long dataSize = avgRowSize * limit;
              wcStats.setNumRows(setMaxIfInvalid(numRows));
              wcStats.setDataSize(setMaxIfInvalid(dataSize));
            }
            lop.setStatistics(wcStats);

            if (LOG.isDebugEnabled()) {
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
              String prefixedKey = "KEY." + key;
              ExprNodeDesc end = colExprMap.get(prefixedKey);
              if (end != null) {
                ColStatistics cs = StatsUtils
                    .getColStatisticsFromExpression(conf, parentStats, end);
                if (cs != null) {
                  cs.setColumnName(key);
                  colStats.add(cs);
                }
              }
            }

            for (String val : outValueColNames) {
              String prefixedVal = "VALUE." + val;
              ExprNodeDesc end = colExprMap.get(prefixedVal);
              if (end != null) {
                ColStatistics cs = StatsUtils
                    .getColStatisticsFromExpression(conf, parentStats, end);
                if (cs != null) {
                  cs.setColumnName(val);
                  colStats.add(cs);
                }
              }
            }

            outStats.setColumnStats(colStats);
          }
          rop.setStatistics(outStats);
          if (LOG.isDebugEnabled()) {
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
                  stats.addToColumnStats(parentStats.getColumnStats());
                  op.getConf().setStatistics(stats);

                  if (LOG.isDebugEnabled()) {
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

    if (newNumRows <= 0) {
      LOG.info("STATS-" + op.toString() + ": Overflow in number of rows."
          + newNumRows + " rows will be set to Long.MAX_VALUE");
    }

    newNumRows = setMaxIfInvalid(newNumRows);
    long oldRowCount = stats.getNumRows();
    double ratio = (double) newNumRows / (double) oldRowCount;
    stats.setNumRows(newNumRows);

    if (useColStats) {
      List<ColStatistics> colStats = stats.getColumnStats();
      for (ColStatistics cs : colStats) {
        long oldNumNulls = cs.getNumNulls();
        long oldDV = cs.getCountDistint();
        long newNumNulls = Math.round(ratio * oldNumNulls);
        long newDV = oldDV;

        // if ratio is greater than 1, then number of rows increases. This can happen
        // when some operators like GROUPBY duplicates the input rows in which case
        // number of distincts should not change. Update the distinct count only when
        // the output number of rows is less than input number of rows.
        if (ratio <= 1.0) {
          newDV = (long) Math.ceil(ratio * oldDV);
        }
        cs.setNumNulls(newNumNulls);
        cs.setCountDistint(newDV);
      }
      stats.setColumnStats(colStats);
      long newDataSize = StatsUtils.getDataSizeFromColumnStats(newNumRows, colStats);
      stats.setDataSize(setMaxIfInvalid(newDataSize));
    } else {
      long newDataSize = (long) (ratio * stats.getDataSize());
      stats.setDataSize(setMaxIfInvalid(newDataSize));
    }
  }

  static boolean satisfyPrecondition(Statistics stats) {
    return stats != null && stats.getBasicStatsState().equals(Statistics.State.COMPLETE)
        && !stats.getColumnStatsState().equals(Statistics.State.NONE);
  }

  /**
   * negative number of rows or data sizes are invalid. It could be because of
   * long overflow in which case return Long.MAX_VALUE
   * @param val - input value
   * @return Long.MAX_VALUE if val is negative else val
   */
  static long setMaxIfInvalid(long val) {
    return val < 0 ? Long.MAX_VALUE : val;
  }
}
