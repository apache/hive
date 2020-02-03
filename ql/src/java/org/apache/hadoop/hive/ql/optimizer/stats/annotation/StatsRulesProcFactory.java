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

package org.apache.hadoop.hive.ql.optimizer.stats.annotation;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.AbstractMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UDTFOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.signature.OpTreeSignature;
import org.apache.hadoop.hive.ql.parse.ColumnStatsList;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ColStatistics.Range;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnListDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc.ExprNodeDescEqualityWrapper;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicListDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicValueDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.Statistics.State;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.apache.hadoop.hive.ql.plan.mapper.StatsSource;
import org.apache.hadoop.hive.ql.stats.OperatorStats;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFCount;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMax;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFMin;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFInBloomFilter;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFStruct;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class StatsRulesProcFactory {

  private static final Logger LOG = LoggerFactory.getLogger(StatsRulesProcFactory.class.getName());

  /**
   * Collect basic statistics like number of rows, data size and column level statistics from the
   * table. Also sets the state of the available statistics. Basic and column statistics can have
   * one of the following states COMPLETE, PARTIAL, NONE. In case of partitioned table, the basic
   * and column stats are aggregated together to table level statistics. Column statistics will not
   * be collected if hive.stats.fetch.column.stats is set to false. If basic statistics is not
   * available then number of rows will be estimated from file size and average row size (computed
   * from schema).
   */
  public static class TableScanStatsRule extends DefaultStatsRule implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      TableScanOperator tsop = (TableScanOperator) nd;
      AnnotateStatsProcCtx aspCtx = (AnnotateStatsProcCtx) procCtx;
      PrunedPartitionList partList = aspCtx.getParseContext().getPrunedPartitions(tsop);
      ColumnStatsList colStatsCached = aspCtx.getParseContext().getColStatsCached(partList);
      Table table = tsop.getConf().getTableMetadata();

      try {
        // gather statistics for the first time and the attach it to table scan operator
        Statistics stats = StatsUtils.collectStatistics(aspCtx.getConf(), partList, colStatsCached, table, tsop);

        stats = applyRuntimeStats(aspCtx.getParseContext().getContext(), stats, tsop);
        tsop.setStatistics(stats);

        if (LOG.isDebugEnabled()) {
          LOG.debug("[0] STATS-" + tsop.toString() + " (" + table.getTableName() + "): " +
              stats.extendedToString());
        }
      } catch (HiveException e) {
        LOG.debug("Failed to retrieve stats ", e);
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
  public static class SelectStatsRule extends DefaultStatsRule implements SemanticNodeProcessor {

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
        stats = parentStats.clone();
      }

      if (satisfyPrecondition(parentStats)) {
        // this will take care of mapping between input column names and output column names. The
        // returned column stats will have the output column names.
        List<ColStatistics> colStats =
            StatsUtils.getColStatisticsFromExprMap(conf, parentStats, sop.getColumnExprMap(), sop.getSchema());
        stats.setColumnStats(colStats);
        // in case of select(*) the data size does not change
        if (!sop.getConf().isSelectStar() && !sop.getConf().isSelStarNoCompute()) {
          long dataSize = StatsUtils.getDataSizeFromColumnStats(stats.getNumRows(), colStats);
          stats.setDataSize(dataSize);
        }
        stats = applyRuntimeStats(aspCtx.getParseContext().getContext(), stats, sop);
        sop.setStatistics(stats);

        if (LOG.isDebugEnabled()) {
          LOG.debug("[0] STATS-" + sop.toString() + ": " + stats.extendedToString());
        }
      } else {
        if (parentStats != null) {
          stats = applyRuntimeStats(aspCtx.getParseContext().getContext(), stats, sop);
          sop.setStatistics(stats);

          if (LOG.isDebugEnabled()) {
            LOG.debug("[1] STATS-" + sop.toString() + ": " + parentStats.extendedToString());
          }
        }
      }
      return null;
    }

  }

  /**
   * FILTER operator does not change the average row size but it does change the number of rows
   * emitted. The reduction in the number of rows emitted is dependent on the filter expression.
   * <i>Notations:</i>
   * <ul>
   * <li>T(S) - Number of tuples in relations S</li>
   * <li>V(S,A) - Number of distinct values of attribute A in relation S</li>
   * </ul>
   * <i>Rules:</i>
   * <ul>
   * <li><b>Column equals a constant</b> T(S) = T(R) / V(R,A)</li>
   * <li><b>Inequality conditions</b> T(S) = T(R) / 3</li>
   * <li><b>Not equals comparison</b> - Simple formula T(S) = T(R)</li>
   * <li>- Alternate formula T(S) = T(R) (V(R,A) - 1) / V(R,A) </li>
   * <li><b>NOT condition</b> T(S) = 1 - T(S'), where T(S') is the satisfying condition</li>
   * <li><b>Multiple AND conditions</b> Cascadingly apply the rules 1 to 3 (order doesn't matter)</li>
   * <li><b>Multiple OR conditions</b> - Simple formula is to evaluate conditions independently
   * and sum the results T(S) = m1 + m2</li>
   * <li>- Alternate formula T(S) = T(R) * ( 1 - ( 1 - m1/T(R) ) * ( 1 - m2/T(R) ))
   * <br>
   * where, m1 is the number of tuples that satisfy condition1 and m2 is the number of tuples that
   * satisfy condition2 </li>
   * </ul>
   * <i>Worst case:</i> If no column statistics are available, then evaluation of predicate
   * expression will assume worst case (i.e; half the input rows) for each of predicate expression.
   * <br>
   * <i>For more information, refer 'Estimating The Cost Of Operations' chapter in
   * "Database Systems: The Complete Book" by Garcia-Molina et. al.</i>
   * <br>
   */
  public static class FilterStatsRule extends DefaultStatsRule implements SemanticNodeProcessor {

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

      if (parentStats != null) {
        ExprNodeDesc pred = fop.getConf().getPredicate();

        // evaluate filter expression and update statistics
        aspCtx.clearAffectedColumns();
        long newNumRows = evaluateExpression(parentStats, pred, aspCtx,
            neededCols, fop, parentStats.getNumRows());
        Statistics st = parentStats.clone();

        if (satisfyPrecondition(parentStats)) {

          // update statistics based on column statistics.
          // OR conditions keeps adding the stats independently, this may
          // result in number of rows getting more than the input rows in
          // which case stats need not be updated
          if (newNumRows <= parentStats.getNumRows()) {
            StatsUtils.updateStats(st, newNumRows, true, fop, aspCtx.getAffectedColumns());
          }

          if (LOG.isDebugEnabled()) {
            LOG.debug("[0] STATS-" + fop.toString() + ": " + st.extendedToString());
          }
        } else {

          // update only the basic statistics in the absence of column statistics
          if (newNumRows <= parentStats.getNumRows()) {
            StatsUtils.updateStats(st, newNumRows, false, fop);
          }

          if (LOG.isDebugEnabled()) {
            LOG.debug("[1] STATS-" + fop.toString() + ": " + st.extendedToString());
          }
        }

        st = applyRuntimeStats(aspCtx.getParseContext().getContext(), st, fop);
        fop.setStatistics(st);

        aspCtx.setAndExprStats(null);
      }
      return null;
    }

    protected long evaluateExpression(Statistics stats, ExprNodeDesc pred,
        AnnotateStatsProcCtx aspCtx, List<String> neededCols,
        Operator<?> op, long currNumRows) throws SemanticException {
      long newNumRows = 0;
      Statistics andStats = null;

      if (currNumRows <= 1 || stats.getDataSize() <= 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Estimating row count for " + pred + " Original num rows: " + currNumRows +
              " Original data size: " + stats.getDataSize() + " New num rows: 1");
        }
        return 1;
      }

      if (pred instanceof ExprNodeGenericFuncDesc) {
        ExprNodeGenericFuncDesc genFunc = (ExprNodeGenericFuncDesc) pred;
        GenericUDF udf = genFunc.getGenericUDF();

        // for AND condition cascadingly update stats
        if (udf instanceof GenericUDFOPAnd) {
          Set<String> affectedColumns = new HashSet<>();
          andStats = stats.clone();
          aspCtx.setAndExprStats(andStats);
          aspCtx.clearAffectedColumns();

          // evaluate children
          long evaluatedRowCount = currNumRows;
          for (ExprNodeDesc child : genFunc.getChildren()) {
            evaluatedRowCount = evaluateChildExpr(aspCtx.getAndExprStats(), child,
                aspCtx, neededCols, op, evaluatedRowCount);
            newNumRows = evaluatedRowCount;
            if (satisfyPrecondition(aspCtx.getAndExprStats())) {
              // Assumption is that columns are uncorrelated.
              // Ndv is reduced in a conservative manner - only taking affected columns
              // (which might be a subset of the actual *real* affected columns due to current limitation)
              // Goal is to not let a situation in which ndv-s asre underestimated happen.
              StatsUtils.updateStats(aspCtx.getAndExprStats(), newNumRows, true, op, aspCtx.getAffectedColumns());
            } else {
              StatsUtils.updateStats(aspCtx.getAndExprStats(), newNumRows, false, op);
            }
            affectedColumns.addAll(aspCtx.getAffectedColumns());
            aspCtx.clearAffectedColumns();
          }
          aspCtx.addAffectedColumns(affectedColumns);
        } else if (udf instanceof GenericUDFOPOr) {
          // for OR condition independently compute and update stats.
          for (ExprNodeDesc child : genFunc.getChildren()) {
            newNumRows = StatsUtils.safeAdd(
                evaluateChildExpr(stats, child, aspCtx, neededCols, op, currNumRows),
                newNumRows);
          }
          // We have to clear the affected columns
          // since currently it is not possible to get a real estimate of an or expression.
          aspCtx.clearAffectedColumns();
          if (newNumRows > currNumRows) {
            newNumRows = currNumRows;
          }
        } else if (udf instanceof GenericUDFIn) {
          // for IN clause
          newNumRows = evaluateInExpr(stats, pred, currNumRows, aspCtx, neededCols, op);
        } else if (udf instanceof GenericUDFBetween) {
          // for BETWEEN clause
          newNumRows = evaluateBetweenExpr(stats, pred, currNumRows, aspCtx, neededCols, op);
        } else if (udf instanceof GenericUDFOPNot) {
          newNumRows = evaluateNotExpr(stats, pred, currNumRows, aspCtx, neededCols, op);
        } else if (udf instanceof GenericUDFOPNotNull) {
          return evaluateNotNullExpr(stats, aspCtx, genFunc, currNumRows);
        } else {
          // single predicate condition
          newNumRows = evaluateChildExpr(stats, pred, aspCtx, neededCols, op, currNumRows);
        }
      } else if (pred instanceof ExprNodeColumnDesc) {

        // can be boolean column in which case return true count
        ExprNodeColumnDesc encd = (ExprNodeColumnDesc) pred;
        aspCtx.addAffectedColumn(encd);
        String colName = encd.getColumn();
        String colType = encd.getTypeString();
        if (colType.equalsIgnoreCase(serdeConstants.BOOLEAN_TYPE_NAME)) {
          ColStatistics cs = stats.getColumnStatisticsFromColName(colName);
          if (cs != null) {
            newNumRows = cs.getNumTrues();
          } else {
            // default
            newNumRows = stats.getNumRows() / 2;
          }
        } else {
          // if not boolean column return half the number of rows
          newNumRows = stats.getNumRows() / 2;
        }
      } else if (pred instanceof ExprNodeConstantDesc) {

        // special case for handling false constants
        ExprNodeConstantDesc encd = (ExprNodeConstantDesc) pred;
        if (Boolean.FALSE.equals(encd.getValue())) {
          newNumRows = 0;
        } else {
          newNumRows = stats.getNumRows();
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Estimating row count for " + pred + " Original num rows: " + stats.getNumRows() +
            " New num rows: " + newNumRows);
      }

      return newNumRows;
    }

    private long evaluateInExpr(Statistics stats, ExprNodeDesc pred, long currNumRows, AnnotateStatsProcCtx aspCtx,
        List<String> neededCols, Operator<?> op) throws SemanticException {

      long numRows = currNumRows;

      ExprNodeGenericFuncDesc fd = (ExprNodeGenericFuncDesc) pred;

      // 1. It is an IN operator, check if it uses STRUCT
      List<ExprNodeDesc> children = fd.getChildren();
      List<ExprNodeDesc> columns = Lists.newArrayList();
      List<ColStatistics> columnStats = Lists.newArrayList();
      List<Set<ExprNodeDescEqualityWrapper>> values = Lists.newArrayList();
      ExprNodeDesc columnsChild = children.get(0);
      boolean multiColumn;
      if (columnsChild instanceof ExprNodeGenericFuncDesc &&
          ((ExprNodeGenericFuncDesc) columnsChild).getGenericUDF() instanceof GenericUDFStruct) {
        for (int j = 0; j < columnsChild.getChildren().size(); j++) {
          ExprNodeDesc columnChild = columnsChild.getChildren().get(j);
          // If column is not column reference , we bail out
          if (!(columnChild instanceof ExprNodeColumnDesc)) {
            // Default
            return numRows / 2;
          }
          columns.add(columnChild);

          // not adding column as affected; since that would rescale ndv based on the other columns
          // selectivity as well...which leads to underestimation
          // aspCtx.addAffectedColumn((ExprNodeColumnDesc) columnChild);
          final String columnName = ((ExprNodeColumnDesc) columnChild).getColumn();
          // if column name is not contained in needed column list then it
          // is a partition column. We do not need to evaluate partition columns
          // in filter expression since it will be taken care by partition pruner
          if (neededCols != null && !neededCols.contains(columnName)) {
            // Default
            return numRows / 2;
          }
          columnStats.add(stats.getColumnStatisticsFromColName(columnName));
          values.add(Sets.<ExprNodeDescEqualityWrapper> newHashSet());
        }
        multiColumn = true;
      } else {
        // If column is not column reference , we bail out
        if (!(columnsChild instanceof ExprNodeColumnDesc)) {
          // Default
          return numRows / 2;
        }
        columns.add(columnsChild);
        aspCtx.addAffectedColumn((ExprNodeColumnDesc) columnsChild);
        final String columnName = ((ExprNodeColumnDesc) columnsChild).getColumn();
        // if column name is not contained in needed column list then it
        // is a partition column. We do not need to evaluate partition columns
        // in filter expression since it will be taken care by partition pruner
        if (neededCols != null && !neededCols.contains(columnName)) {
          // Default
          return numRows / 2;
        }
        columnStats.add(stats.getColumnStatisticsFromColName(columnName));
        values.add(Sets.<ExprNodeDescEqualityWrapper> newHashSet());
        multiColumn = false;
      }

      // 2. Extract columns and values
      for (int i = 1; i < children.size(); i++) {
        ExprNodeDesc child = children.get(i);
        // If value is not a constant, we bail out
        if (!(child instanceof ExprNodeConstantDesc)) {
          // Default
          return numRows / 2;
        }
        if (multiColumn) {
          ExprNodeConstantDesc constantChild = (ExprNodeConstantDesc) child;
          List<?> items = (List<?>) constantChild.getWritableObjectInspector().getWritableConstantValue();
          List<TypeInfo> structTypes = ((StructTypeInfo) constantChild.getTypeInfo()).getAllStructFieldTypeInfos();
          for (int j = 0; j < structTypes.size(); j++) {
            ExprNodeConstantDesc constant = new ExprNodeConstantDesc(structTypes.get(j), items.get(j));
            values.get(j).add(new ExprNodeDescEqualityWrapper(constant));
          }
        } else {
          values.get(0).add(new ExprNodeDescEqualityWrapper(child));
        }
      }

      boolean allColsFilteredByStats = true;
      for (int i = 0; i < columnStats.size(); i++) {
        ValuePruner vp = new ValuePruner(columnStats.get(i));
        allColsFilteredByStats &= vp.isValid();
        Set<ExprNodeDescEqualityWrapper> newValues = Sets.newHashSet();
        for (ExprNodeDescEqualityWrapper v : values.get(i)) {
          if (vp.accept(v)) {
            newValues.add(v);
          }
        }
        values.set(i, newValues);
      }

      // 3. Calculate IN selectivity
      double factor = 1d;
      if (multiColumn) {
        // distinct value array doesn not help that much here; think (1,1),(1,2),(2,1),(2,2) as values
        // but that will look like (1,2) as column values...
        factor *= children.size() - 1;
      }
      for (int i = 0; i < columnStats.size(); i++) {
        long dvs = columnStats.get(i) == null ? 0 : columnStats.get(i).getCountDistint();
        // (num of distinct vals for col in IN clause  / num of distinct vals for col )
        double columnFactor = dvs == 0 ? 0.5d : (1.0d / dvs);
        if (!multiColumn) {
          columnFactor *=values.get(0).size();
        }
        // max can be 1, even when ndv is larger in IN clause than in column stats
        factor *= columnFactor > 1d ? 1d : columnFactor;
      }

      // Clamp at 1 to be sure that we don't get out of range.
      factor = Double.min(factor, 1.0d);
      if (!allColsFilteredByStats) {
        factor = Double.max(factor, HiveConf.getFloatVar(aspCtx.getConf(), HiveConf.ConfVars.HIVE_STATS_IN_MIN_RATIO));
      }
      float inFactor = HiveConf.getFloatVar(aspCtx.getConf(), HiveConf.ConfVars.HIVE_STATS_IN_CLAUSE_FACTOR);
      return Math.round(numRows * factor * inFactor);
    }

    static class RangeOps {

      private String colType;
      private Range range;

      public RangeOps(String colType, Range range) {
        this.colType = colType;
        this.range = range;
      }

      public static RangeOps build(String colType, Range range) {
        if (range == null || range.minValue == null || range.maxValue == null) {
          return null;
        }
        return new RangeOps(colType, range);
      }

      enum RangeResult {
        BELOW, AT_MIN, BETWEEN, AT_MAX, ABOVE;

        public static RangeResult of(boolean ltMin, boolean ltMax, boolean eqMin, boolean eqMax) {
          if (ltMin) {
            return RangeResult.BELOW;
          }
          if (eqMin) {
            return RangeResult.AT_MIN;
          }
          if (ltMax) {
            return RangeResult.BETWEEN;
          }
          if (eqMax) {
            return AT_MAX;
          }
          return ABOVE;
        }
      }

      public boolean contains(ExprNodeDesc exprNode) {
        RangeResult intersection = intersect(exprNode);
        return intersection != RangeResult.ABOVE && intersection != RangeResult.BELOW;
      }

      public RangeResult intersect(ExprNodeDesc exprNode) {
        if (!(exprNode instanceof ExprNodeConstantDesc)) {
          return null;
        }
        try {

          ExprNodeConstantDesc constantDesc = (ExprNodeConstantDesc) exprNode;

          String stringVal = constantDesc.getValue().toString();

          @Deprecated
          String boundValue = stringVal;
          switch (colType) {
          case serdeConstants.TINYINT_TYPE_NAME: {
            byte value = Byte.parseByte(stringVal);
            byte maxValue = range.maxValue.byteValue();
            byte minValue = range.minValue.byteValue();
            return RangeResult.of(value < minValue, value < maxValue, value == minValue, value == maxValue);
          }
          case serdeConstants.SMALLINT_TYPE_NAME: {
            short value = Short.parseShort(boundValue);
            short maxValue = range.maxValue.shortValue();
            short minValue = range.minValue.shortValue();
            return RangeResult.of(value < minValue, value < maxValue, value == minValue, value == maxValue);
          }
          case serdeConstants.DATE_TYPE_NAME: {
            DateWritable dateWriteable = new DateWritable(java.sql.Date.valueOf(boundValue));
            int value = dateWriteable.getDays();
            int maxValue = range.maxValue.intValue();
            int minValue = range.minValue.intValue();
            return RangeResult.of(value < minValue, value < maxValue, value == minValue, value == maxValue);
          }
          case serdeConstants.INT_TYPE_NAME: {
            int value = Integer.parseInt(boundValue);
            int maxValue = range.maxValue.intValue();
            int minValue = range.minValue.intValue();
            return RangeResult.of(value < minValue, value < maxValue, value == minValue, value == maxValue);
          }
          case serdeConstants.TIMESTAMP_TYPE_NAME: {
            TimestampWritableV2 timestampWritable = new TimestampWritableV2(Timestamp.valueOf(boundValue));
            long value = timestampWritable.getTimestamp().toEpochSecond();
            long maxValue = range.maxValue.longValue();
            long minValue = range.minValue.longValue();
            return RangeResult.of(value < minValue, value < maxValue, value == minValue, value == maxValue);
          }
          case serdeConstants.BIGINT_TYPE_NAME: {
            long value = Long.parseLong(boundValue);
            long maxValue = range.maxValue.longValue();
            long minValue = range.minValue.longValue();
            return RangeResult.of(value < minValue, value < maxValue, value == minValue, value == maxValue);
          }
          case serdeConstants.FLOAT_TYPE_NAME: {
            float value = Float.parseFloat(boundValue);
            float maxValue = range.maxValue.floatValue();
            float minValue = range.minValue.floatValue();
            return RangeResult.of(value < minValue, value < maxValue, value == minValue, value == maxValue);
          }
          case serdeConstants.DOUBLE_TYPE_NAME: {
            double value = Double.parseDouble(boundValue);
            double maxValue = range.maxValue.doubleValue();
            double minValue = range.minValue.doubleValue();
            return RangeResult.of(value < minValue, value < maxValue, value == minValue, value == maxValue);
          }
          default:
            if (colType.startsWith(serdeConstants.DECIMAL_TYPE_NAME)) {
              BigDecimal value = new BigDecimal(boundValue);
              BigDecimal maxValue = new BigDecimal(range.maxValue.toString());
              BigDecimal minValue = new BigDecimal(range.minValue.toString());
              int minComparison = value.compareTo(minValue);
              int maxComparison = value.compareTo(maxValue);
              return RangeResult.of(minComparison < 0, maxComparison < 0, minComparison == 0, maxComparison == 0);
            }
            return null;
          }
        } catch (Exception e) {
          // NumberFormatException value out of range
          // other unknown cases
          return null;
        }
      }

    }

    private static class ValuePruner {

      private boolean valid;
      private RangeOps colRange;

      ValuePruner(ColStatistics colStatistics) {
        if (colStatistics == null) {
          valid = false;
          return;
        }
        colRange = RangeOps.build(colStatistics.getColumnType(), colStatistics.getRange());
        if (colRange == null) {
          valid = false;
          return;
        }
        valid = true;
      }

      public boolean isValid() {
        return valid;
      }

      public boolean accept(ExprNodeDescEqualityWrapper e) {
        /** removes all values which are outside of the scope of the column */
        return !valid || colRange.contains(e.getExprNodeDesc());
      }
    }

    private ExprNodeDesc rewriteBetweenToIn(final ExprNodeDesc comparisonExpression, final ExprNodeDesc leftExpression,
                                            final ExprNodeDesc rightExpression, boolean invert) {
      // difference in BETWEEN values could be millions, since for each value a new ExprNodeConstantDesc is created
      // we should limit the rewrite to avoid taking too much memory
      final int REWRITE_THRESHOLD = 100;

      boolean shouldRewrite = false;
      long startVal = 0, endVal = 0;

      if (ExprNodeDescUtils.isIntegerType(comparisonExpression)
          && leftExpression instanceof ExprNodeConstantDesc
          && rightExpression instanceof ExprNodeConstantDesc) {
        Object leftValue = ((ExprNodeConstantDesc) leftExpression).getValue();
        Object rightValue = ((ExprNodeConstantDesc) rightExpression).getValue();

        startVal = ((Number)leftValue).longValue();
        endVal = ((Number)rightValue).longValue();

        // BETWEEN could be (10,0)
        if(startVal > endVal) {
          Long tmpVal = startVal;
          startVal = endVal;
          endVal = tmpVal;
        }

        if ((endVal - startVal) <= REWRITE_THRESHOLD) {
          shouldRewrite = true;
        }
      }

      if (shouldRewrite) {

        List<ExprNodeDesc> constantExprs = new ArrayList<>();
        constantExprs.add(comparisonExpression);
        //generate list of contiguous integers
        for (long i = startVal; i <= endVal; i++) {
          ExprNodeConstantDesc constExpr = new ExprNodeConstantDesc(comparisonExpression.getTypeInfo(), i);
          constantExprs.add(constExpr);
        }
        ExprNodeDesc newExpression = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
                                                                 new GenericUDFIn(), constantExprs);
        return newExpression;
      } else {
        // We transform the BETWEEN clause to AND clause (with NOT on top in invert is true).
        // This is more straightforward, as the evaluateExpression method will deal with
        // generating the final row count relying on the basic comparator evaluation methods
        final ExprNodeDesc leftComparator = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
                                                        new GenericUDFOPEqualOrGreaterThan(),
                                                        Lists.newArrayList(comparisonExpression, leftExpression));
        final ExprNodeDesc rightComparator = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
                                                        new GenericUDFOPEqualOrLessThan(),
                                                        Lists.newArrayList(comparisonExpression, rightExpression));
        ExprNodeDesc newExpression = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
                                                                 new GenericUDFOPAnd(),
                                                                 Lists.newArrayList(leftComparator, rightComparator));
        if (invert) {
          newExpression = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
                                                      new GenericUDFOPNot(), Lists.newArrayList(newExpression));
        }
        return newExpression;
      }
    }

    private long evaluateBetweenExpr(Statistics stats, ExprNodeDesc pred, long currNumRows, AnnotateStatsProcCtx aspCtx,
        List<String> neededCols, Operator<?> op) throws SemanticException {
      final ExprNodeGenericFuncDesc fd = (ExprNodeGenericFuncDesc) pred;
      final boolean invert = Boolean.TRUE.equals(
          ((ExprNodeConstantDesc) fd.getChildren().get(0)).getValue()); // boolean invert (not)
      final ExprNodeDesc comparisonExpression = fd.getChildren().get(1); // expression
      final ExprNodeDesc leftExpression = fd.getChildren().get(2); // left expression
      final ExprNodeDesc rightExpression = fd.getChildren().get(3); // right expression

      // Short circuit and return the current number of rows if this is a
      // synthetic predicate with dynamic values
      if (leftExpression instanceof ExprNodeDynamicValueDesc) {
        return currNumRows;
      }

      ExprNodeDesc newExpression = rewriteBetweenToIn(comparisonExpression, leftExpression, rightExpression, invert);

      return evaluateExpression(stats, newExpression, aspCtx, neededCols, op, currNumRows);
    }

    private long evaluateNotExpr(Statistics stats, ExprNodeDesc pred, long currNumRows,
        AnnotateStatsProcCtx aspCtx, List<String> neededCols, Operator<?> op) throws SemanticException {

      long numRows = currNumRows;

      // if the evaluate yields true then pass all rows else pass 0 rows
      if (pred instanceof ExprNodeGenericFuncDesc) {
        ExprNodeGenericFuncDesc genFunc = (ExprNodeGenericFuncDesc) pred;
        for (ExprNodeDesc leaf : genFunc.getChildren()) {
          if (leaf instanceof ExprNodeGenericFuncDesc) {

            // GenericUDF
            long newNumRows = 0;
            for (ExprNodeDesc child : genFunc.getChildren()) {
              newNumRows = evaluateChildExpr(stats, child, aspCtx, neededCols,
                  op, numRows);
            }
            return numRows - newNumRows;
          } else if (leaf instanceof ExprNodeConstantDesc) {
            ExprNodeConstantDesc encd = (ExprNodeConstantDesc) leaf;
            if (Boolean.TRUE.equals(encd.getValue())) {
              return 0;
            } else {
              return numRows;
            }
          } else if (leaf instanceof ExprNodeColumnDesc) {

            // NOT on boolean columns is possible. in which case return false count.
            ExprNodeColumnDesc encd = (ExprNodeColumnDesc) leaf;
            aspCtx.addAffectedColumn(encd);
            String colName = encd.getColumn();
            String colType = encd.getTypeString();
            if (colType.equalsIgnoreCase(serdeConstants.BOOLEAN_TYPE_NAME)) {
              ColStatistics cs = stats.getColumnStatisticsFromColName(colName);
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

    private long evaluateColEqualsNullExpr(Statistics stats, AnnotateStatsProcCtx aspCtx, ExprNodeDesc pred,
        long currNumRows) {

      long numRows = currNumRows;

      if (pred instanceof ExprNodeGenericFuncDesc) {

        ExprNodeGenericFuncDesc genFunc = (ExprNodeGenericFuncDesc) pred;
        for (ExprNodeDesc leaf : genFunc.getChildren()) {

          if (leaf instanceof ExprNodeColumnDesc) {
            ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) leaf;
            aspCtx.addAffectedColumn(colDesc);
            String colName = colDesc.getColumn();
            ColStatistics cs = stats.getColumnStatisticsFromColName(colName);
            if (cs != null) {
              return cs.getNumNulls();
            }
          }
        }
      }

      // worst case
      return numRows / 2;
    }

    private long evaluateNotNullExpr(Statistics parentStats, AnnotateStatsProcCtx aspCtx, ExprNodeGenericFuncDesc pred,
        long currNumRows) {
      long noOfNulls = getMaxNulls(parentStats, aspCtx, pred);
      long parentCardinality = currNumRows;
      long newPredCardinality = parentCardinality;

      if (parentCardinality > noOfNulls) {
        newPredCardinality = parentCardinality - noOfNulls;
      } else {
        LOG.error("Invalid column stats: No of nulls > cardinality");
      }

      return newPredCardinality;
    }

    private long getMaxNulls(Statistics stats, AnnotateStatsProcCtx aspCtx, ExprNodeDesc pred) {
      long tmpNoNulls = 0;
      long maxNoNulls = 0;

      if (pred instanceof ExprNodeColumnDesc) {
        ExprNodeColumnDesc encd = (ExprNodeColumnDesc) pred;
        ColStatistics cs = stats.getColumnStatisticsFromColName(encd.getColumn());
        if (cs != null) {
          tmpNoNulls = cs.getNumNulls();
        }
        if (cs == null || tmpNoNulls > 0) {
          aspCtx.addAffectedColumn(encd);
        }
      } else if (pred instanceof ExprNodeGenericFuncDesc || pred instanceof ExprNodeColumnListDesc) {
        long noNullsOfChild = 0;
        for (ExprNodeDesc childExpr : pred.getChildren()) {
          noNullsOfChild = getMaxNulls(stats, aspCtx, childExpr);
          if (noNullsOfChild > tmpNoNulls) {
            tmpNoNulls = noNullsOfChild;
          }
        }
      } else if (pred instanceof ExprNodeConstantDesc) {
        if (ExprNodeDescUtils.isNullConstant(pred)) {
          tmpNoNulls = stats.getNumRows();
        } else {
          tmpNoNulls = 0;
        }
      } else if (pred instanceof ExprNodeDynamicListDesc) {
        tmpNoNulls = 0;
      } else if (pred instanceof ExprNodeFieldDesc) {
        // TODO Confirm this is safe
        tmpNoNulls = getMaxNulls(stats, aspCtx, ((ExprNodeFieldDesc) pred).getDesc());
      }

      if (tmpNoNulls > maxNoNulls) {
        maxNoNulls = tmpNoNulls;
      }

      return maxNoNulls;
    }

    private long evaluateComparator(Statistics stats, AnnotateStatsProcCtx aspCtx, ExprNodeGenericFuncDesc genFunc,
        long currNumRows) {
      long numRows = currNumRows;
      GenericUDF udf = genFunc.getGenericUDF();

      ExprNodeColumnDesc columnDesc;
      ExprNodeConstantDesc constantDesc;
      boolean upperBound;
      boolean closedBound;
      String boundValue = null;
      if (genFunc.getChildren().get(0) instanceof ExprNodeColumnDesc &&
          genFunc.getChildren().get(1) instanceof ExprNodeConstantDesc) {
        columnDesc = (ExprNodeColumnDesc) genFunc.getChildren().get(0);
        constantDesc = (ExprNodeConstantDesc) genFunc.getChildren().get(1);
        aspCtx.addAffectedColumn(columnDesc);
        // Comparison to null will always return false
        if (constantDesc.getValue() == null) {
          return 0;
        }
        boundValue = constantDesc.getValue().toString();
        upperBound = udf instanceof GenericUDFOPEqualOrLessThan ||
            udf instanceof GenericUDFOPLessThan;
        closedBound =  isClosedBound(udf);
      } else if (genFunc.getChildren().get(1) instanceof ExprNodeColumnDesc &&
          genFunc.getChildren().get(0) instanceof ExprNodeConstantDesc) {
        columnDesc = (ExprNodeColumnDesc) genFunc.getChildren().get(1);
        constantDesc = (ExprNodeConstantDesc) genFunc.getChildren().get(0);
        aspCtx.addAffectedColumn(columnDesc);
        // Comparison to null will always return false
        if (constantDesc.getValue() == null) {
          return 0;
        }
        boundValue = constantDesc.getValue().toString();
        upperBound = udf instanceof GenericUDFOPEqualOrGreaterThan ||
            udf instanceof GenericUDFOPGreaterThan;
        closedBound = isClosedBound(udf);
      } else {
        // default
        return numRows / 3;
      }

      ColStatistics cs = stats.getColumnStatisticsFromColName(columnDesc.getColumn());
      if (cs != null && cs.getRange() != null &&
          cs.getRange().maxValue != null && cs.getRange().minValue != null) {
        String colTypeLowerCase = columnDesc.getTypeString().toLowerCase();
        try {
          if (colTypeLowerCase.equals(serdeConstants.TINYINT_TYPE_NAME)) {
            byte value = Byte.parseByte(boundValue);
            byte maxValue = cs.getRange().maxValue.byteValue();
            byte minValue = cs.getRange().minValue.byteValue();
            if (upperBound) {
              if (maxValue < value || maxValue == value && closedBound) {
                return numRows;
              }
              if (minValue > value || minValue == value && !closedBound) {
                return 0;
              }
              if (aspCtx.isUniformWithinRange()) {
                // Assuming uniform distribution, we can use the range to calculate
                // new estimate for the number of rows
                return Math.round(((double) (value - minValue) / (maxValue - minValue)) * numRows);
              }
            } else {
              if (minValue > value || minValue == value && closedBound) {
                return numRows;
              }
              if (maxValue < value || maxValue == value && !closedBound) {
                return 0;
              }
              if (aspCtx.isUniformWithinRange()) {
                // Assuming uniform distribution, we can use the range to calculate
                // new estimate for the number of rows
                return Math.round(((double) (maxValue - value) / (maxValue - minValue)) * numRows);
              }
            }
          } else if (colTypeLowerCase.equals(serdeConstants.SMALLINT_TYPE_NAME)) {
            short value = Short.parseShort(boundValue);
            short maxValue = cs.getRange().maxValue.shortValue();
            short minValue = cs.getRange().minValue.shortValue();
            if (upperBound) {
              if (maxValue < value || maxValue == value && closedBound) {
                return numRows;
              }
              if (minValue > value || minValue == value && !closedBound) {
                return 0;
              }
              if (aspCtx.isUniformWithinRange()) {
                // Assuming uniform distribution, we can use the range to calculate
                // new estimate for the number of rows
                return Math.round(((double) (value - minValue) / (maxValue - minValue)) * numRows);
              }
            } else {
              if (minValue > value || minValue == value && closedBound) {
                return numRows;
              }
              if (maxValue < value || maxValue == value && !closedBound) {
                return 0;
              }
              if (aspCtx.isUniformWithinRange()) {
                // Assuming uniform distribution, we can use the range to calculate
                // new estimate for the number of rows
                return Math.round(((double) (maxValue - value) / (maxValue - minValue)) * numRows);
              }
            }
          } else if (colTypeLowerCase.equals(serdeConstants.INT_TYPE_NAME) ||
              colTypeLowerCase.equals(serdeConstants.DATE_TYPE_NAME)) {
            int value;
            if (colTypeLowerCase.equals(serdeConstants.DATE_TYPE_NAME)) {
              DateWritable writableVal = new DateWritable(java.sql.Date.valueOf(boundValue));
              value = writableVal.getDays();
            } else {
              value = Integer.parseInt(boundValue);
            }
            // Date is an integer internally
            int maxValue = cs.getRange().maxValue.intValue();
            int minValue = cs.getRange().minValue.intValue();
            if (upperBound) {
              if (maxValue < value || maxValue == value && closedBound) {
                return numRows;
              }
              if (minValue > value || minValue == value && !closedBound) {
                return 0;
              }
              if (aspCtx.isUniformWithinRange()) {
                // Assuming uniform distribution, we can use the range to calculate
                // new estimate for the number of rows
                return Math.round(((double) (value - minValue) / (maxValue - minValue)) * numRows);
              }
            } else {
              if (minValue > value || minValue == value && closedBound) {
                return numRows;
              }
              if (maxValue < value || maxValue == value && !closedBound) {
                return 0;
              }
              if (aspCtx.isUniformWithinRange()) {
                // Assuming uniform distribution, we can use the range to calculate
                // new estimate for the number of rows
                return Math.round(((double) (maxValue - value) / (maxValue - minValue)) * numRows);
              }
            }
          } else if (colTypeLowerCase.equals(serdeConstants.BIGINT_TYPE_NAME) ||
              colTypeLowerCase.equals(serdeConstants.TIMESTAMP_TYPE_NAME)) {
            long value;
            if (colTypeLowerCase.equals(serdeConstants.TIMESTAMP_TYPE_NAME)) {
              TimestampWritableV2 timestampWritable = new TimestampWritableV2(Timestamp.valueOf(boundValue));
              value = timestampWritable.getTimestamp().toEpochSecond();
            } else {
              value = Long.parseLong(boundValue);
            }
            long maxValue = cs.getRange().maxValue.longValue();
            long minValue = cs.getRange().minValue.longValue();
            if (upperBound) {
              if (maxValue < value || maxValue == value && closedBound) {
                return numRows;
              }
              if (minValue > value || minValue == value && !closedBound) {
                return 0;
              }
              if (aspCtx.isUniformWithinRange()) {
                // Assuming uniform distribution, we can use the range to calculate
                // new estimate for the number of rows
                return Math.round(((double) (value - minValue) / (maxValue - minValue)) * numRows);
              }
            } else {
              if (minValue > value || minValue == value && closedBound) {
                return numRows;
              }
              if (maxValue < value || maxValue == value && !closedBound) {
                return 0;
              }
              if (aspCtx.isUniformWithinRange()) {
                // Assuming uniform distribution, we can use the range to calculate
                // new estimate for the number of rows
                return Math.round(((double) (maxValue - value) / (maxValue - minValue)) * numRows);
              }
            }
          } else if (colTypeLowerCase.equals(serdeConstants.FLOAT_TYPE_NAME)) {
            float value = Float.parseFloat(boundValue);
            float maxValue = cs.getRange().maxValue.floatValue();
            float minValue = cs.getRange().minValue.floatValue();
            if (upperBound) {
              if (maxValue < value || maxValue == value && closedBound) {
                return numRows;
              }
              if (minValue > value || minValue == value && !closedBound) {
                return 0;
              }
              if (aspCtx.isUniformWithinRange()) {
                // Assuming uniform distribution, we can use the range to calculate
                // new estimate for the number of rows
                return Math.round(((double) (value - minValue) / (maxValue - minValue)) * numRows);
              }
            } else {
              if (minValue > value || minValue == value && closedBound) {
                return numRows;
              }
              if (maxValue < value || maxValue == value && !closedBound) {
                return 0;
              }
              if (aspCtx.isUniformWithinRange()) {
                // Assuming uniform distribution, we can use the range to calculate
                // new estimate for the number of rows
                return Math.round(((double) (maxValue - value) / (maxValue - minValue)) * numRows);
              }
            }
          } else if (colTypeLowerCase.equals(serdeConstants.DOUBLE_TYPE_NAME)) {
            double value = Double.parseDouble(boundValue);
            double maxValue = cs.getRange().maxValue.doubleValue();
            double minValue = cs.getRange().minValue.doubleValue();
            if (upperBound) {
              if (maxValue < value || maxValue == value && closedBound) {
                return numRows;
              }
              if (minValue > value || minValue == value && !closedBound) {
                return 0;
              }
              if (aspCtx.isUniformWithinRange()) {
                // Assuming uniform distribution, we can use the range to calculate
                // new estimate for the number of rows
                return Math.round(((value - minValue) / (maxValue - minValue)) * numRows);
              }
            } else {
              if (minValue > value || minValue == value && closedBound) {
                return numRows;
              }
              if (maxValue < value || maxValue == value && !closedBound) {
                return 0;
              }
              if (aspCtx.isUniformWithinRange()) {
                // Assuming uniform distribution, we can use the range to calculate
                // new estimate for the number of rows
                return Math.round(((maxValue - value) / (maxValue - minValue)) * numRows);
              }
            }
          } else if (colTypeLowerCase.startsWith(serdeConstants.DECIMAL_TYPE_NAME)) {
            BigDecimal value = new BigDecimal(boundValue);
            BigDecimal maxValue = new BigDecimal(cs.getRange().maxValue.toString());
            BigDecimal minValue = new BigDecimal(cs.getRange().minValue.toString());
            int minComparison = value.compareTo(minValue);
            int maxComparison = value.compareTo(maxValue);
            if (upperBound) {
              if (maxComparison > 0 || maxComparison == 0 && closedBound) {
                return numRows;
              }
              if (minComparison < 0 || minComparison == 0 && !closedBound) {
                return 0;
              }
              if (aspCtx.isUniformWithinRange()) {
                // Assuming uniform distribution, we can use the range to calculate
                // new estimate for the number of rows
                return Math.round(
                    ((value.subtract(minValue)).divide(maxValue.subtract(minValue), RoundingMode.UP))
                        .multiply(BigDecimal.valueOf(numRows))
                        .doubleValue());
              }
            } else {
              if (minComparison < 0 || minComparison == 0 && closedBound) {
                return numRows;
              }
              if (maxComparison > 0 || maxComparison == 0 && !closedBound) {
                return 0;
              }
              if (aspCtx.isUniformWithinRange()) {
                // Assuming uniform distribution, we can use the range to calculate
                // new estimate for the number of rows
                return Math.round(
                    ((maxValue.subtract(value)).divide(maxValue.subtract(minValue), RoundingMode.UP))
                        .multiply(BigDecimal.valueOf(numRows))
                        .doubleValue());
              }
            }
          }
        } catch (NumberFormatException nfe) {
          return numRows / 3;
        }
      }
      // default
      return numRows / 3;
    }

    private boolean isClosedBound(GenericUDF udf) {
      return udf instanceof GenericUDFOPEqualOrGreaterThan ||
          udf instanceof GenericUDFOPEqualOrLessThan;
    }

    private long evaluateChildExpr(Statistics stats, ExprNodeDesc child,
        AnnotateStatsProcCtx aspCtx, List<String> neededCols,
        Operator<?> op, long currNumRows)
        throws SemanticException {

      long numRows = currNumRows;

      if (child instanceof ExprNodeGenericFuncDesc) {

        ExprNodeGenericFuncDesc genFunc = (ExprNodeGenericFuncDesc) child;
        GenericUDF udf = genFunc.getGenericUDF();

        if (udf instanceof GenericUDFOPEqual) {
          String colName = null;
          boolean isConst = false;
          Object prevConst = null;

          for (ExprNodeDesc leaf : genFunc.getChildren()) {
            if (leaf instanceof ExprNodeConstantDesc) {

              // constant = constant expressions. We shouldn't be getting this
              // after constant folding
              if (isConst) {

                // special case: if both constants are not equal then return 0
                if (prevConst != null &&
                    !prevConst.equals(((ExprNodeConstantDesc) leaf).getValue())) {
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

              ColStatistics cs = stats.getColumnStatisticsFromColName(colName);
              if (cs != null) {
                long dvs = cs.getCountDistint();
                numRows = dvs == 0 ? numRows / 2 : Math.round((double) numRows / dvs);
                return numRows;
              }
            } else if (leaf instanceof ExprNodeColumnDesc) {
              ExprNodeColumnDesc colDesc = (ExprNodeColumnDesc) leaf;
              aspCtx.addAffectedColumn(colDesc);
              colName = colDesc.getColumn();

              // if const is first argument then evaluate the result
              if (isConst) {

                // if column name is not contained in needed column list then it
                // is a partition column. We do not need to evaluate partition columns
                // in filter expression since it will be taken care by partitio pruner
                if (neededCols != null && neededCols.indexOf(colName) == -1) {
                  return numRows;
                }

                ColStatistics cs = stats.getColumnStatisticsFromColName(colName);
                if (cs != null) {
                  long dvs = cs.getCountDistint();
                  numRows = dvs == 0 ? numRows / 2 : Math.round((double) numRows / dvs);
                  return numRows;
                }
              }
            }
          }
        } else if (udf instanceof GenericUDFOPNotEqual) {
          return numRows;
        } else if (udf instanceof GenericUDFOPEqualOrGreaterThan
            || udf instanceof GenericUDFOPEqualOrLessThan
            || udf instanceof GenericUDFOPGreaterThan
            || udf instanceof GenericUDFOPLessThan) {
          return evaluateComparator(stats, aspCtx, genFunc, numRows);
        } else if (udf instanceof GenericUDFOPNotNull) {
          return evaluateNotNullExpr(stats, aspCtx, genFunc, numRows);
        } else if (udf instanceof GenericUDFOPNull) {
          return evaluateColEqualsNullExpr(stats, aspCtx, genFunc, numRows);
        } else if (udf instanceof GenericUDFOPAnd || udf instanceof GenericUDFOPOr
            || udf instanceof GenericUDFIn || udf instanceof GenericUDFBetween
            || udf instanceof GenericUDFOPNot) {
          return evaluateExpression(stats, genFunc, aspCtx, neededCols, op, numRows);
        } else if (udf instanceof GenericUDFInBloomFilter) {
          if (genFunc.getChildren().get(1) instanceof ExprNodeDynamicValueDesc) {
            // Synthetic predicates from semijoin opt should not affect stats.
            return numRows;
          }
        }
      } else if (child instanceof ExprNodeConstantDesc) {
        if (Boolean.FALSE.equals(((ExprNodeConstantDesc) child).getValue())) {
          return 0;
        } else {
          return numRows;
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
   * T(R) = min (T(R)/2 , V(R,[A,B,C]) ---&gt; [1]
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
  public static class GroupByStatsRule extends DefaultStatsRule implements SemanticNodeProcessor {

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

      if (LOG.isDebugEnabled()) {
        LOG.debug("STATS-" + gop.toString() + ": inputSize: " + inputSize + " maxSplitSize: " +
            maxSplitSize + " parallelism: " + parallelism + " containsGroupingSet: " +
            containsGroupingSet + " sizeOfGroupingSet: " + sizeOfGroupingSet);
      }

      // satisfying precondition means column statistics is available
      if (satisfyPrecondition(parentStats)) {

        // check if map side aggregation is possible or not based on column stats
        hashAgg = checkMapSideAggregation(gop, colStats, conf);

        if (LOG.isDebugEnabled()) {
          LOG.debug("STATS-" + gop.toString() + " hashAgg: " + hashAgg);
        }

        stats = parentStats.clone();
        stats.setColumnStats(colStats);
        final long parentNumRows = stats.getNumRows();

        // compute product of distinct values of grouping columns
        long ndvProduct =
            StatsUtils.computeNDVGroupingColumns(colStats, parentStats, false);
        // if ndvProduct is 0 then column stats state must be partial and we are missing
        // column stats for a group by column
        if (ndvProduct == 0) {
          ndvProduct = parentNumRows / 2;

          if (LOG.isDebugEnabled()) {
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

              if (LOG.isDebugEnabled()) {
                LOG.debug("[Case 4] STATS-" + gop.toString() + ": cardinality: " + cardinality);
              }
            } else {
              // Case 3: column stats, hash aggregation, NO grouping sets
              cardinality = Math.min(parentNumRows/2, StatsUtils.safeMult(ndvProduct, parallelism));
              long orgParentNumRows = StatsUtils.safeMult(getParentNumRows(gop, gop.getConf().getKeys(), conf),
                                                          parallelism);
              cardinality = Math.min(cardinality, orgParentNumRows);

              if (LOG.isDebugEnabled()) {
                LOG.debug("[Case 3] STATS-" + gop.toString() + ": cardinality: " + cardinality);
              }
            }
          } else {
            if (containsGroupingSet) {
              // Case 6: column stats, NO hash aggregation, grouping sets
              cardinality = StatsUtils.safeMult(parentNumRows, sizeOfGroupingSet);

              if (LOG.isDebugEnabled()) {
                LOG.debug("[Case 6] STATS-" + gop.toString() + ": cardinality: " + cardinality);
              }
            } else {
              // Case 5: column stats, NO hash aggregation, NO grouping sets
              cardinality = Math.min(parentNumRows, getParentNumRows(gop, gop.getConf().getKeys(), conf));

              if (LOG.isDebugEnabled()) {
                LOG.debug("[Case 5] STATS-" + gop.toString() + ": cardinality: " + cardinality);
              }
            }
          }
        } else {

          // in reduce side GBY, we don't know if the grouping set was present or not. so get it
          // from map side GBY
          GroupByOperator mGop = OperatorUtils.findMapSideGb(gop);
          if(mGop != null) {
            containsGroupingSet = mGop.getConf().isGroupingSetsPresent();
          }

          if (containsGroupingSet) {
            // Case 8: column stats, grouping sets
            sizeOfGroupingSet = mGop.getConf().getListGroupingSets().size();
            cardinality = Math.min(parentNumRows, StatsUtils.safeMult(ndvProduct, sizeOfGroupingSet));

            if (LOG.isDebugEnabled()) {
              LOG.debug("[Case 8] STATS-" + gop.toString() + ": cardinality: " + cardinality);
            }
          } else {
            // Case 9: column stats, NO grouping sets
            cardinality = Math.min(parentNumRows, ndvProduct);
            // to get to the source number of rows we should be using original group by
            GroupByOperator gOpStats = mGop;
            if(gOpStats == null) {
              // it could be NULL in case the plan has single group by (instead of merge and final)
              // e.g. autogather stats
              gOpStats = gop;
            }
            long orgParentNumRows = getParentNumRows(gOpStats, gOpStats.getConf().getKeys(), conf);
            cardinality = Math.min(orgParentNumRows, cardinality);

            if (LOG.isDebugEnabled()) {
              LOG.debug("[Case 9] STATS-" + gop.toString() + ": cardinality: " + cardinality);
            }
          }
        }

        // update stats, but don't update NDV as it will not change
        StatsUtils.updateStats(stats, cardinality, true, gop);
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

              if (LOG.isDebugEnabled()) {
                LOG.debug("[Case 2] STATS-" + gop.toString() + ": cardinality: " + cardinality);
              }
            } else {
              // Case 1: NO column stats, NO hash aggregation, NO grouping sets
              cardinality = parentNumRows;

              if (LOG.isDebugEnabled()) {
                LOG.debug("[Case 1] STATS-" + gop.toString() + ": cardinality: " + cardinality);
              }
            }
          } else {

            // Case 7: NO column stats
            cardinality = parentNumRows / 2;

            if (LOG.isDebugEnabled()) {
              LOG.debug("[Case 7] STATS-" + gop.toString() + ": cardinality: " + cardinality);
            }
          }

          StatsUtils.updateStats(stats, cardinality, false, gop);
        }
      }

      // if UDAFs are present, new columns needs to be added
      if (!aggDesc.isEmpty() && stats != null) {
        List<ColStatistics> aggColStats = Lists.newArrayList();
        int idx = 0;
        for (ColumnInfo ci : rs.getSignature()) {

          // if the columns in row schema is not contained in column
          // expression map, then those are the aggregate columns that
          // are added GBY operator. we will estimate the column statistics
          // for those newly added columns
          if (!colExprMap.containsKey(ci.getInternalName())) {
            String colName = ci.getInternalName();
            String colType = ci.getTypeName();
            ColStatistics cs = new ColStatistics(colName, colType);
            cs.setCountDistint(stats.getNumRows());
            cs.setNumNulls(0);
            cs.setAvgColLen(StatsUtils.getAvgColLenOf(conf, ci.getObjectInspector(), colType));
            computeAggregateColumnMinMax(cs, conf, aggDesc.get(idx++), colType, parentStats);
            aggColStats.add(cs);
          }
        }

        // add the new aggregate column and recompute data size
        if (aggColStats.size() > 0) {
          stats.addToColumnStats(aggColStats);

          // only if the column stats is available, update the data size from
          // the column stats
          if (!stats.getColumnStatsState().equals(Statistics.State.NONE)) {
            StatsUtils.updateStats(stats, stats.getNumRows(), true, gop);
          }
        }

        // if UDAF present and if column expression map is empty then it must
        // be full aggregation query like count(*) in which case number of
        // rows will be 1
        if (colExprMap.isEmpty()) {
          StatsUtils.updateStats(stats, 1, true, gop);
        }
      }

      stats = applyRuntimeStats(aspCtx.getParseContext().getContext(), stats, gop);
      gop.setStatistics(stats);

      if (LOG.isDebugEnabled() && stats != null) {
        LOG.debug("[0] STATS-" + gop.toString() + ": " + stats.extendedToString());
      }
      return null;
    }

    /**
     * If possible, sets the min / max value for the column based on the aggregate function
     * being calculated and its input.
     */
    private static void computeAggregateColumnMinMax(ColStatistics cs, HiveConf conf, AggregationDesc agg, String aggType,
        Statistics parentStats) throws SemanticException {
      if (agg.getParameters() != null && agg.getParameters().size() == 1) {
        ColStatistics parentCS = StatsUtils.getColStatisticsFromExpression(
            conf, parentStats, agg.getParameters().get(0));
        if (parentCS != null && parentCS.getRange() != null &&
            parentCS.getRange().minValue != null && parentCS.getRange().maxValue != null) {
          long valuesCount = agg.getDistinct() ?
              parentCS.getCountDistint() :
              parentStats.getNumRows() - parentCS.getNumNulls();
          Range range = parentCS.getRange();
          // Get the aggregate function matching the name in the query.
          GenericUDAFResolver udaf =
              FunctionRegistry.getGenericUDAFResolver(agg.getGenericUDAFName());
          if (udaf instanceof GenericUDAFCount) {
            cs.setRange(new Range(0, valuesCount));
          } else if (udaf instanceof GenericUDAFMax || udaf instanceof GenericUDAFMin) {
            cs.setRange(new Range(range.minValue, range.maxValue));
          } else if (udaf instanceof GenericUDAFSum) {
            switch (aggType) {
            case serdeConstants.TINYINT_TYPE_NAME:
            case serdeConstants.SMALLINT_TYPE_NAME:
            case serdeConstants.DATE_TYPE_NAME:
            case serdeConstants.INT_TYPE_NAME:
            case serdeConstants.BIGINT_TYPE_NAME:
            case serdeConstants.TIMESTAMP_TYPE_NAME:
              long maxValueLong = range.maxValue.longValue();
              long minValueLong = range.minValue.longValue();
              // If min value is less or equal to max value (legal)
              if (minValueLong <= maxValueLong && minValueLong >= 0) {
                // min = minValue, max = (minValue + maxValue) * 0.5 * parentNumRows
                cs.setRange(new Range(
                    minValueLong,
                    StatsUtils.safeMult(
                        StatsUtils.safeMult(StatsUtils.safeAdd(minValueLong, maxValueLong), 0.5),
                        valuesCount)));
              }
              break;
            case serdeConstants.FLOAT_TYPE_NAME:
            case serdeConstants.DOUBLE_TYPE_NAME:
              double maxValueDouble = range.maxValue.doubleValue();
              double minValueDouble = range.minValue.doubleValue();
              // If min value is less or equal to max value (legal)
              if (minValueDouble <= maxValueDouble && minValueDouble >= 0) {
                // min = minValue, max = (minValue + maxValue) * 0.5 * parentNumRows
                cs.setRange(new Range(
                    minValueDouble,
                    (minValueDouble + maxValueDouble) * 0.5 * valuesCount));
              }
              break;
            default:
              if (aggType.startsWith(serdeConstants.DECIMAL_TYPE_NAME)) {
                BigDecimal maxValueBD = new BigDecimal(range.maxValue.toString());
                BigDecimal minValueBD = new BigDecimal(range.minValue.toString());
                // If min value is less or equal to max value (legal)
                if (minValueBD.compareTo(maxValueBD) <= 0 && minValueBD.compareTo(BigDecimal.ZERO) >= 0) {
                  // min = minValue, max = (minValue + maxValue) * 0.5 * parentNumRows
                  cs.setRange(new Range(
                      minValueBD,
                      minValueBD.add(maxValueBD).multiply(new BigDecimal(0.5)).multiply(new BigDecimal(valuesCount))));
                }
              }
            }
          }
        }
      }
    }

    private long getParentNumRows(GroupByOperator op, List<ExprNodeDesc> gbyKeys, HiveConf conf) {
      if(gbyKeys == null || gbyKeys.isEmpty()) {
        return op.getParentOperators().get(0).getStatistics().getNumRows();
      }
      Operator<? extends OperatorDesc> parent = OperatorUtils.findSourceRS(op, gbyKeys);
      if(parent != null) {
        return parent.getStatistics().getNumRows();
      }
      return op.getParentOperators().get(0).getStatistics().getNumRows();
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
          int evaluatorEstimate = aggregationEvaluators[i].estimate();
          if (evaluatorEstimate > 0) {
            avgValSize += evaluatorEstimate;
            continue;
          }
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
  }

  /**
   * JOIN operator can yield any of the following three cases <ul><li>The values of join keys are
   * disjoint in both relations in which case T(RXS) = 0 (we need histograms for this)</li> <li>Join
   * key is primary key on relation R and foreign key on relation S in which case every tuple in S
   * will have a tuple in R T(RXS) = T(S) (we need histograms for this)</li> <li>Both R &amp; S relation
   * have same value for join-key. Ex: bool column with all true values T(RXS) = T(R) * T(S) (we
   * need histograms for this. counDistinct = 1 and same value)</li></ul>
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
  public static class JoinStatsRule extends FilterStatsRule implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      long newNumRows = 0;
      CommonJoinOperator<? extends JoinDesc> jop = (CommonJoinOperator<? extends JoinDesc>) nd;
      List<Operator<? extends OperatorDesc>> parents = jop.getParentOperators();
      int numAttr = 1;
      AnnotateStatsProcCtx aspCtx = (AnnotateStatsProcCtx) procCtx;
      HiveConf conf = aspCtx.getConf();
      boolean allSatisfyPreCondition = true;

      for (Operator<? extends OperatorDesc> op : parents) {
        if (op.getStatistics() == null) {
          return null;
        }
      }

      for (Operator<? extends OperatorDesc> op : parents) {
        if (!satisfyPrecondition(op.getStatistics())) {
          allSatisfyPreCondition = false;
          break;
        }
      }
      // there could be case where join operators input are not RS e.g.
      // map join with Spark. Since following estimation of statistics relies on join operators having it inputs as
      // reduced sink it will not work for such cases. So we should not try to estimate stats
      if (allSatisfyPreCondition) {
        for (int pos = 0; pos < parents.size(); pos++) {
          if (!(jop.getParentOperators().get(pos) instanceof ReduceSinkOperator)) {
            allSatisfyPreCondition = false;
            break;
          }
        }
      }

      if (allSatisfyPreCondition) {

        // statistics object that is combination of statistics from all
        // relations involved in JOIN
        Statistics stats = new Statistics();
        int numParent = parents.size();
        Map<Integer, Long> rowCountParents = Maps.newHashMap();
        Map<Integer, Statistics> joinStats = Maps.newHashMap();
        Map<Integer, List<String>> joinKeys = Maps.newHashMap();
        List<Long> rowCounts = Lists.newArrayList();

        // detect if there are multiple attributes in join key
        ReduceSinkOperator rsOp = (ReduceSinkOperator) jop.getParentOperators().get(0);
        List<String> keyExprs = StatsUtils.getQualifedReducerKeyNames(rsOp.getConf()
            .getOutputKeyColumnNames());
        numAttr = keyExprs.size();

        // infer PK-FK relationship in single attribute join case
        long inferredRowCount = inferPKFKRelationship(numAttr, parents, jop);
        // get the join keys from parent ReduceSink operators
        for (int pos = 0; pos < parents.size(); pos++) {
          ReduceSinkOperator parent = (ReduceSinkOperator) jop.getParentOperators().get(pos);
          Statistics parentStats;
          parentStats = parent.getStatistics().clone();
          keyExprs = StatsUtils.getQualifedReducerKeyNames(parent.getConf()
              .getOutputKeyColumnNames());

          rowCountParents.put(pos, parentStats.getNumRows());
          rowCounts.add(parentStats.getNumRows());

          // internal name for expressions and estimate column statistics for expression.
          joinKeys.put(pos, keyExprs);

          // get column statistics for all output columns
          joinStats.put(pos, parentStats);

          // since new statistics is derived from all relations involved in
          // JOIN, we need to update the state information accordingly
          stats.updateColumnStatsState(parentStats.getColumnStatsState());
        }

        if (numAttr == 0) {
          // It is a cartesian product, row count is easy to infer
          inferredRowCount = 1;
          for (int pos = 0; pos < parents.size(); pos++) {
            inferredRowCount = StatsUtils.safeMult(joinStats.get(pos).getNumRows(), inferredRowCount);
          }
        }

        List<Long> distinctVals = Lists.newArrayList();

        // these ndvs are later used to compute unmatched rows and num of nulls for outer joins
        List<Long> ndvsUnmatched= Lists.newArrayList();
        long denom = 1;
        long distinctUnmatched = 1;
        if (inferredRowCount == -1) {
          // failed to infer PK-FK relationship for row count estimation fall-back on default logic
          // compute denominator  max(V(R,y1), V(S,y1)) * max(V(R,y2), V(S,y2))
          // in case of multi-attribute join
          List<Long> perAttrDVs = Lists.newArrayList();
          // go over each predicate
          for (int idx = 0; idx < numAttr; idx++) {
            for (Integer i : joinKeys.keySet()) {
              String col = joinKeys.get(i).get(idx);
              ColStatistics cs = joinStats.get(i).getColumnStatisticsFromColName(col);
              if (cs != null) {
                perAttrDVs.add(cs.getCountDistint());
              }
            }
            distinctVals.add(getDenominator(perAttrDVs));
            ndvsUnmatched.add(getDenominatorForUnmatchedRows(perAttrDVs));
            perAttrDVs.clear();
          }

          if (numAttr > 1 && conf.getBoolVar(HiveConf.ConfVars.HIVE_STATS_CORRELATED_MULTI_KEY_JOINS)) {
            denom = Collections.max(distinctVals);
            distinctUnmatched = denom - ndvsUnmatched.get(distinctVals.indexOf(denom));
          } else {
            // To avoid denominator getting larger and aggressively reducing
            // number of rows, we will ease out denominator.
            denom = StatsUtils.addWithExpDecay(distinctVals);
            distinctUnmatched = denom - StatsUtils.addWithExpDecay(ndvsUnmatched);
          }
        }

        // Update NDV of joined columns to be min(V(R,y), V(S,y))
        updateJoinColumnsNDV(joinKeys, joinStats, numAttr);

        // column statistics from different sources are put together and
        // rename based on output schema of join operator
        Map<String, ExprNodeDesc> colExprMap = jop.getColumnExprMap();
        RowSchema rs = jop.getSchema();
        List<ColStatistics> outColStats = Lists.newArrayList();
        for (ColumnInfo ci : rs.getSignature()) {
          String key = ci.getInternalName();
          ExprNodeDesc end = colExprMap.get(key);
          if (end instanceof ExprNodeColumnDesc) {
            aspCtx.addAffectedColumn((ExprNodeColumnDesc) end);
            String colName = ((ExprNodeColumnDesc) end).getColumn();
            int pos = jop.getConf().getReversedExprs().get(key);
            ColStatistics cs = joinStats.get(pos).getColumnStatisticsFromColName(colName);
            String outColName = key;
            if (cs != null) {
              cs.setColumnName(outColName);
            }
            outColStats.add(cs);
          }
        }

        // update join statistics
        stats.setColumnStats(outColStats);

        long joinRowCount;
        long leftUnmatchedRows = 0L;
        long rightUnmatchedRows = 0L;
        if (inferredRowCount != -1) {
          joinRowCount = inferredRowCount;
        } else {
          long innerJoinRowCount = computeRowCountAssumingInnerJoin(rowCounts, denom, jop);
          // the idea is to measure unmatched rows in outer joins by figuring out how many rows didn't match
          if (jop.getConf().getConds().length == 1) {
            // TODO: Consider more than one condition
            JoinCondDesc joinCond = jop.getConf().getConds()[0];
            if (joinCond.getType() == JoinDesc.LEFT_OUTER_JOIN) {
              leftUnmatchedRows = calculateUnmatchedRowsForOuter(conf, rowCountParents.get(0), joinKeys.get(0), joinStats.get(0), distinctUnmatched);
            } else if (joinCond.getType() == JoinDesc.RIGHT_OUTER_JOIN) {
              rightUnmatchedRows = calculateUnmatchedRowsForOuter(conf, rowCountParents.get(1), joinKeys.get(1), joinStats.get(1), distinctUnmatched);
            } else if (joinCond.getType() == JoinDesc.FULL_OUTER_JOIN) {
              leftUnmatchedRows = calculateUnmatchedRowsForOuter(conf, rowCountParents.get(0), joinKeys.get(0), joinStats.get(0), distinctUnmatched);
              rightUnmatchedRows = calculateUnmatchedRowsForOuter(conf, rowCountParents.get(1), joinKeys.get(1), joinStats.get(1), distinctUnmatched);
            }
          }
          // final row computation will consider join type
          joinRowCount = computeFinalRowCount(rowCounts, StatsUtils.safeAdd(innerJoinRowCount, StatsUtils.safeAdd(leftUnmatchedRows, rightUnmatchedRows)), jop);
        }

        // update column statistics
        updateColStats(conf, stats, leftUnmatchedRows, rightUnmatchedRows, joinRowCount, jop, rowCountParents);

        // evaluate filter expression and update statistics
        if (joinRowCount != -1 && jop.getConf().getNoOuterJoin() &&
            jop.getConf().getResidualFilterExprs() != null &&
            !jop.getConf().getResidualFilterExprs().isEmpty()) {
          ExprNodeDesc pred;
          if (jop.getConf().getResidualFilterExprs().size() > 1) {
            pred = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
                FunctionRegistry.getGenericUDFForAnd(),
                jop.getConf().getResidualFilterExprs());
          } else {
            pred = jop.getConf().getResidualFilterExprs().get(0);
          }
          // evaluate filter expression and update statistics
          newNumRows = evaluateExpression(stats, pred,
              aspCtx, jop.getSchema().getColumnNames(), jop, stats.getNumRows());
          // update statistics based on column statistics.
          // OR conditions keeps adding the stats independently, this may
          // result in number of rows getting more than the input rows in
          // which case stats need not be updated
          if (newNumRows <= joinRowCount) {
            StatsUtils.updateStats(stats, newNumRows, true, jop);
          }
        }

        stats = applyRuntimeStats(aspCtx.getParseContext().getContext(), stats, jop);
        jop.setStatistics(stats);

        if (LOG.isDebugEnabled()) {
          LOG.debug("[0] STATS-" + jop.toString() + ": " + stats.extendedToString());
        }
      } else {

        // worst case when there are no column statistics
        float joinFactor = HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVE_STATS_JOIN_FACTOR);
        int numParents = parents.size();
        long crossRowCount = 1;
        long crossDataSize = 1;
        long maxRowCount = 0;
        long maxDataSize = 0;
        State statsState = State.NONE;

        for (Operator<? extends OperatorDesc> op : parents) {
          Statistics ps = op.getStatistics();
          statsState = Statistics.inferColumnStatsState(statsState, ps.getBasicStatsState());
          long rowCount = ps.getNumRows();
          long dataSize = ps.getDataSize();
          // Update cross size
          long newCrossRowCount = StatsUtils.safeMult(crossRowCount, rowCount);
          long newCrossDataSize = StatsUtils.safeAdd(
              StatsUtils.safeMult(crossDataSize, rowCount),
              StatsUtils.safeMult(dataSize, crossRowCount));
          crossRowCount = newCrossRowCount;
          crossDataSize = newCrossDataSize;
          // Update largest relation
          if (rowCount > maxRowCount) {
            maxRowCount = rowCount;
            maxDataSize = dataSize;
          }
        }

        long newDataSize;
        // detect if there are attributes in join key
        boolean cartesianProduct = false;
        if (jop.getParentOperators().get(0) instanceof ReduceSinkOperator) {
          ReduceSinkOperator rsOp = (ReduceSinkOperator) jop.getParentOperators().get(0);
          List<String> keyExprs = StatsUtils.getQualifedReducerKeyNames(rsOp.getConf()
              .getOutputKeyColumnNames());
          cartesianProduct = keyExprs.size() == 0;
        } else if (jop instanceof AbstractMapJoinOperator) {
          AbstractMapJoinOperator<? extends MapJoinDesc> mjop =
              (AbstractMapJoinOperator<? extends MapJoinDesc>) jop;
          List<ExprNodeDesc> keyExprs = mjop.getConf().getKeys().values().iterator().next();
          cartesianProduct = keyExprs.size() == 0;
        }
        if (cartesianProduct) {
          // Cartesian product
          newNumRows = crossRowCount;
          newDataSize = crossDataSize;
        } else {
          if (numParents > 1) {
            newNumRows = StatsUtils.safeMult(StatsUtils.safeMult(maxRowCount, (numParents - 1)), joinFactor);
            newDataSize = StatsUtils.safeMult(StatsUtils.safeMult(maxDataSize, (numParents - 1)), joinFactor);
          } else {
            // MUX operator with 1 parent
            newNumRows = StatsUtils.safeMult(maxRowCount, joinFactor);
            newDataSize = StatsUtils.safeMult(maxDataSize, joinFactor);
          }
        }

        Statistics wcStats = new Statistics(newNumRows, newDataSize, 0);
        wcStats.setBasicStatsState(statsState);

        // evaluate filter expression and update statistics
        if (jop.getConf().getNoOuterJoin() &&
            jop.getConf().getResidualFilterExprs() != null &&
            !jop.getConf().getResidualFilterExprs().isEmpty()) {
          long joinRowCount = newNumRows;
          ExprNodeDesc pred;
          if (jop.getConf().getResidualFilterExprs().size() > 1) {
            pred = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
                FunctionRegistry.getGenericUDFForAnd(),
                jop.getConf().getResidualFilterExprs());
          } else {
            pred = jop.getConf().getResidualFilterExprs().get(0);
          }
          // evaluate filter expression and update statistics
          newNumRows = evaluateExpression(wcStats, pred,
              aspCtx, jop.getSchema().getColumnNames(), jop, wcStats.getNumRows());
          // update only the basic statistics in the absence of column statistics
          if (newNumRows <= joinRowCount) {
            StatsUtils.updateStats(wcStats, newNumRows, false, jop);
          }
        }

        wcStats = applyRuntimeStats(aspCtx.getParseContext().getContext(), wcStats, jop);
        jop.setStatistics(wcStats);

        if (LOG.isDebugEnabled()) {
          LOG.debug("[1] STATS-" + jop.toString() + ": " + wcStats.extendedToString());
        }
      }
      return null;
    }

    private long calculateUnmatchedRowsForOuter(HiveConf conf, long inputRowCount,
        List<String> joinKeys, Statistics statistics, long distinctUnmatched) {
      // Extract the ndv from each of the columns involved in the join
      List<Long> distinctVals = new ArrayList<>();
      for (String col: joinKeys) {
        ColStatistics cs = statistics.getColumnStatisticsFromColName(col);
        if (cs != null) {
          distinctVals.add(cs.getCountDistint());
        }
      }
      // Compute the number of distinct values based on configuration property
      long distinctVal;
      if (distinctVals.isEmpty()) {
        distinctVal = 2L;
      } else {
        if (joinKeys.size() > 1 && conf.getBoolVar(HiveConf.ConfVars.HIVE_STATS_CORRELATED_MULTI_KEY_JOINS)) {
          distinctVal = Collections.max(distinctVals);
        } else {
          distinctVal = StatsUtils.addWithExpDecay(distinctVals);
        }
      }
      // If we have a greater number of unmatched values than number of distinct values,
      // we just return the number of rows in the input as we can assume there are no
      // matches
      if (distinctUnmatched >= distinctVal) {
        return inputRowCount;
      }
      // Otherwise, divide the number of input rows by the number of distinct values
      // and divide by the number of distinct values unmatched
      return StatsUtils.safeMult(inputRowCount / distinctVal, distinctUnmatched);
    }

    private long inferPKFKRelationship(int numAttr, List<Operator<? extends OperatorDesc>> parents,
        CommonJoinOperator<? extends JoinDesc> jop) {
      long newNumRows = -1;
      if (numAttr != 1) {
        return newNumRows;
      }

      // If numAttr is 1, this means we join on one single key column.
      Map<Integer, ColStatistics> parentsWithPK = getPrimaryKeyCandidates(parents);

      // We only allow one single PK.
      if (parentsWithPK.size() != 1) {
        LOG.debug("STATS-" + jop.toString() + ": detects none/multiple PK parents.");
        return newNumRows;
      }
      Integer pkPos = parentsWithPK.keySet().iterator().next();
      ColStatistics csPK = parentsWithPK.values().iterator().next();

      // infer foreign key candidates positions
      Map<Integer, ColStatistics> csFKs = getForeignKeyCandidates(parents, csPK);

      // we allow multiple foreign keys (snowflake schema)
      // csfKs.size() + 1 == parents.size() means we have a single PK and all
      // the rest ops are FKs.
      if (csFKs.size() + 1 == parents.size()) {
        newNumRows = getCardinality(parents, pkPos, csPK, csFKs, jop);

        // some debug information
        if (LOG.isDebugEnabled()) {
          List<String> parentIds = Lists.newArrayList();

          // print primary key containing parents
          for (Integer i : parentsWithPK.keySet()) {
            parentIds.add(parents.get(i).toString());
          }
          LOG.debug("STATS-" + jop.toString() + ": PK parent id(s) - " + parentIds);
          parentIds.clear();

          // print foreign key containing parents
          for (Integer i : csFKs.keySet()) {
            parentIds.add(parents.get(i).toString());
          }
          LOG.debug("STATS-" + jop.toString() + ": FK parent id(s) - " + parentIds);
        }
      }
      return newNumRows;
    }

    /**
     * Get cardinality of reduce sink operators.
     * @param csPK - ColStatistics for a single primary key
     * @param csFKs - ColStatistics for multiple foreign keys
     */
    private long getCardinality(List<Operator<? extends OperatorDesc>> ops, Integer pkPos,
        ColStatistics csPK, Map<Integer, ColStatistics> csFKs,
        CommonJoinOperator<? extends JoinDesc> jop) {
      double pkfkSelectivity = Double.MAX_VALUE;
      int fkInd = -1;
      boolean isFKIndependentFromPK = false;
      // 1. We iterate through all the operators that have candidate FKs and
      // choose the FK that has the minimum selectivity. We assume that PK and this FK
      // have the PK-FK relationship. This is heuristic and can be
      // improved later.
      for (Entry<Integer, ColStatistics> entry : csFKs.entrySet()) {
        int pos = entry.getKey();
        Operator<? extends OperatorDesc> opWithPK = ops.get(pkPos);
        Operator<? extends OperatorDesc> opWithFK = jop.getParentOperators().get(pos);
        double selectivity = getSelectivitySimpleTree(opWithPK);
        double selectivityAdjustment = StatsUtils.getScaledSelectivity(csPK, entry.getValue());
        selectivity = selectivityAdjustment * selectivity > 1 ? selectivity : selectivityAdjustment
            * selectivity;

        boolean independent =
            !entry.getValue().isFilteredColumn() && OperatorUtils.treesWithIndependentInputs(opWithFK, opWithPK);

        if (fkInd < 0 || (independent && selectivity < pkfkSelectivity)) {
          pkfkSelectivity = selectivity;
          fkInd = pos;
          isFKIndependentFromPK = independent;
        }
      }
      long newrows = 1;
      List<Long> rowCounts = Lists.newArrayList();
      List<Long> distinctVals = Lists.newArrayList();
      // 2. We then iterate through all the operators that have candidate FKs again.
      // We assume the PK is first joining with the FK that we just selected.
      // And we apply the PK-FK relationship when we compute the newrows and ndv.
      // After that, we join the result with all the other FKs.
      // We do not assume the PK-FK relationship anymore and just compute the
      // row count using the classic formula.
      for (Entry<Integer, ColStatistics> entry : csFKs.entrySet()) {
        int pos = entry.getKey();
        ColStatistics csFK = entry.getValue();
        ReduceSinkOperator parent = (ReduceSinkOperator) jop.getParentOperators().get(pos);
        Statistics parentStats = parent.getStatistics();
        if (fkInd == pos) {
          // 2.1 This is the new number of rows after PK is joining with FK
          if (!isFKIndependentFromPK) {
            // if the foreign key is filtered by some condition we may not re-scale it
            newrows = parentStats.getNumRows();
          } else {
            newrows = (long) Math.ceil(parentStats.getNumRows() * pkfkSelectivity);
          }
          rowCounts.add(newrows);

          // 2.1 The ndv is the minimum of the PK and the FK.
          distinctVals.add(Math.min(csFK.getCountDistint(), csPK.getCountDistint()));
        } else {
          // 2.2 All the other FKs.
          rowCounts.add(parentStats.getNumRows());
          distinctVals.add(csFK.getCountDistint());
        }
      }
      long newNumRows;
      if (csFKs.size() == 1) {
        // there is only one FK
        newNumRows = newrows;
      } else {
        // there is more than one FK
        newNumRows = this.computeRowCountAssumingInnerJoin(rowCounts,
            getDenominator(distinctVals), jop);
        newNumRows = this.computeFinalRowCount(rowCounts, newNumRows, jop);
      }
      return newNumRows;
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
      while (multiParentOp == null) {
        if (op.getParentOperators().size() > 1) {
          multiParentOp = op;
        } else {
          op = op.getParentOperators().get(0);
        }
      }

      // No need for overflow checks, assume selectivity is always <= 1.0
      float selMultiParent = 1.0f;

      boolean isSelComputed = false;

      // if it is two way left outer or right outer join take selectivity only for
      // corresponding branch since only that branch will factor is the reduction
      if (multiParentOp instanceof JoinOperator) {
        JoinOperator jop = ((JoinOperator) multiParentOp);
        // check for two way join
        if (jop.getConf().getConds().length == 1) {
          isSelComputed = true;
          int type = jop.getConf().getCondsList().get(0).getType();
          if (jop.getConf().getJoinKeys()[0].length == 0 || type == JoinDesc.FULL_OUTER_JOIN) {
            // This is just a cartesian product or a full outer join, we will take the max
            float selMultiParentLeft = getSelectivitySimpleTree(multiParentOp.getParentOperators().get(0));
            float selMultiParentRight = getSelectivitySimpleTree(multiParentOp.getParentOperators().get(1));
            selMultiParent = Math.max(selMultiParentLeft, selMultiParentRight);
          } else {
            switch (type) {
            case JoinDesc.LEFT_OUTER_JOIN:
              selMultiParent = getSelectivitySimpleTree(multiParentOp.getParentOperators().get(0));
              break;
            case JoinDesc.RIGHT_OUTER_JOIN:
              selMultiParent = getSelectivitySimpleTree(multiParentOp.getParentOperators().get(1));
              break;
            default:
              // for rest of the join type we will take min of the reduction.
              float selMultiParentLeft = getSelectivitySimpleTree(multiParentOp.getParentOperators().get(0));
              float selMultiParentRight = getSelectivitySimpleTree(multiParentOp.getParentOperators().get(1));
              selMultiParent = Math.min(selMultiParentLeft, selMultiParentRight);
            }
          }
        }
      }

      if (!isSelComputed) {
        for (Operator<? extends OperatorDesc> parent : multiParentOp.getParentOperators()) {
          // In the above example, TS-1 -> RS-1 and TS-2 -> RS-2 are simple trees
          selMultiParent *= getSelectivitySimpleTree(parent);
        }
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
     * @return - a map which contains position ids and the corresponding column statistics
     */
    private Map<Integer, ColStatistics> getForeignKeyCandidates(List<Operator<? extends OperatorDesc>> ops,
        ColStatistics csPK) {
      Map<Integer, ColStatistics> result = new HashMap<Integer, ColStatistics>();
      if (csPK == null || ops == null) {
        return result;
      }

      for (int i = 0; i < ops.size(); i++) {
        Operator<? extends OperatorDesc> op = ops.get(i);
        if (op != null && op instanceof ReduceSinkOperator) {
          ReduceSinkOperator rsOp = (ReduceSinkOperator) op;
          List<String> keys = StatsUtils.getQualifedReducerKeyNames(rsOp.getConf().getOutputKeyColumnNames());
          if (keys.size() == 1) {
            String joinCol = keys.get(0);
            if (rsOp.getStatistics() != null) {
              ColStatistics cs = rsOp.getStatistics().getColumnStatisticsFromColName(joinCol);
              if (cs != null && !cs.isPrimaryKey()) {
                if (StatsUtils.inferForeignKey(csPK, cs)) {
                  result.put(i, cs);
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
    private Map<Integer, ColStatistics> getPrimaryKeyCandidates(List<Operator<? extends OperatorDesc>> ops) {
      Map<Integer, ColStatistics> result = new HashMap<Integer, ColStatistics>();
      if (ops != null && !ops.isEmpty()) {
        for (int i = 0; i < ops.size(); i++) {
          Operator<? extends OperatorDesc> op = ops.get(i);
          if (op instanceof ReduceSinkOperator) {
            ReduceSinkOperator rsOp = (ReduceSinkOperator) op;
            List<String> keys = StatsUtils.getQualifedReducerKeyNames(rsOp.getConf().getOutputKeyColumnNames());
            if (keys.size() == 1) {
              String joinCol = keys.get(0);
              if (rsOp.getStatistics() != null) {
                ColStatistics cs = rsOp.getStatistics().getColumnStatisticsFromColName(joinCol);
                if (cs != null && cs.isPrimaryKey()) {
                  result.put(i, cs);
                }
              }
            }
          }
        }
      }
      return result;
    }

    private boolean isJoinKey(final String columnName,
        final ExprNodeDesc[][] joinKeys) {
      for (int i = 0; i < joinKeys.length; i++) {
        for (ExprNodeDesc expr : Arrays.asList(joinKeys[i])) {

          if (expr instanceof ExprNodeColumnDesc) {
            if (((ExprNodeColumnDesc) expr).getColumn().equals(columnName)) {
              return true;
            }
          }
        }
      }
      return false;
    }

    private void updateNumNulls(ColStatistics colStats, long leftUnmatchedRows, long rightUnmatchedRows,
        long newNumRows, long pos, CommonJoinOperator<? extends JoinDesc> jop) {

      if (!(jop.getConf().getConds().length == 1)) {
        // TODO: handle multi joins
        return;
      }

      long oldNumNulls = colStats.getNumNulls();
      long newNumNulls = Math.min(newNumRows, oldNumNulls);

      JoinCondDesc joinCond = jop.getConf().getConds()[0];
      switch (joinCond.getType()) {
      case JoinDesc.LEFT_OUTER_JOIN:
        if (pos == joinCond.getRight()) {
          if (isJoinKey(colStats.getColumnName(), jop.getConf().getJoinKeys())) {
            newNumNulls = Math.min(newNumRows, leftUnmatchedRows);
          } else {
            newNumNulls = Math.min(newNumRows, oldNumNulls + leftUnmatchedRows);
          }
        }
        break;
      case JoinDesc.RIGHT_OUTER_JOIN:
        if (pos == joinCond.getLeft()) {
          if (isJoinKey(colStats.getColumnName(), jop.getConf().getJoinKeys())) {
            newNumNulls = Math.min(newNumRows, rightUnmatchedRows);
          } else {
            newNumNulls = Math.min(newNumRows, oldNumNulls + rightUnmatchedRows);
          }
        }
        break;
      case JoinDesc.FULL_OUTER_JOIN:
        if (isJoinKey(colStats.getColumnName(), jop.getConf().getJoinKeys())) {
          newNumNulls = Math.min(newNumRows, leftUnmatchedRows + rightUnmatchedRows);
        } else {
          newNumNulls = Math.min(newNumRows, oldNumNulls + leftUnmatchedRows + rightUnmatchedRows);
        }
        break;

      case JoinDesc.INNER_JOIN:
      case JoinDesc.UNIQUE_JOIN:
      case JoinDesc.LEFT_SEMI_JOIN:
        break;
      }
      colStats.setNumNulls(newNumNulls);
    }

    private void updateColStats(HiveConf conf, Statistics stats, long leftUnmatchedRows, long rightUnmatchedRows,
        long newNumRows, CommonJoinOperator<? extends JoinDesc> jop, Map<Integer, Long> rowCountParents) {

      if (newNumRows < 0) {
        LOG.debug("STATS-" + jop.toString() + ": Overflow in number of rows. "
            + newNumRows + " rows will be set to Long.MAX_VALUE");
      }
      if (newNumRows == 0) {
        LOG.debug("STATS-" + jop.toString() + ": Equals 0 in number of rows. "
            + newNumRows + " rows will be set to 1");
        newNumRows = 1;
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
      Set<String> colNameStatsAvailable = new HashSet<>();
      for (ColStatistics cs : colStats) {
        colNameStatsAvailable.add(cs.getColumnName());
        int pos = jop.getConf().getReversedExprs().get(cs.getColumnName());
        long oldRowCount = rowCountParents.get(pos);
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

        cs.setCountDistint(newDV);
        updateNumNulls(cs, leftUnmatchedRows, rightUnmatchedRows, newNumRows, pos, jop);
      }
      stats.setColumnStats(colStats);
      long newDataSize = StatsUtils
          .getDataSizeFromColumnStats(newNumRows, colStats);
      // Add default size for columns for which stats were not available
      List<String> neededColumns = new ArrayList<>();
      for (String colName : jop.getSchema().getColumnNames()) {
        if (!colNameStatsAvailable.contains(colName)) {
          neededColumns.add(colName);
        }
      }
      if (neededColumns.size() != 0) {
        long restColumnsDefaultSize =
            StatsUtils.estimateRowSizeFromSchema(conf, jop.getSchema().getSignature(), neededColumns);
        newDataSize = StatsUtils.safeAdd(newDataSize, StatsUtils.safeMult(restColumnsDefaultSize, newNumRows));
      }
      stats.setDataSize(StatsUtils.getMaxIfOverflow(newDataSize));
      stats.setBasicStatsState(State.COMPLETE);
    }

    private long computeFinalRowCount(List<Long> rowCountParents, long interimRowCount,
        CommonJoinOperator<? extends JoinDesc> join) {
      long result = interimRowCount;
      if (join.getConf().getConds().length == 1) {
        JoinCondDesc joinCond = join.getConf().getConds()[0];
        switch (joinCond.getType()) {
        case JoinDesc.INNER_JOIN:
          // only dealing with special join types here.
          break;
        case JoinDesc.LEFT_OUTER_JOIN:
          // all rows from left side will be present in resultset
          result = Math.max(rowCountParents.get(joinCond.getLeft()), result);
          break;
        case JoinDesc.RIGHT_OUTER_JOIN:
          // all rows from right side will be present in resultset
          result = Math.max(rowCountParents.get(joinCond.getRight()), result);
          break;
        case JoinDesc.FULL_OUTER_JOIN:
          // all rows from both side will be present in resultset
          result = Math.max(StatsUtils.safeAdd(rowCountParents.get(joinCond.getRight()),
              rowCountParents.get(joinCond.getLeft())), result);
          break;
        case JoinDesc.LEFT_SEMI_JOIN:
          // max # of rows = rows from left side
          result = Math.min(rowCountParents.get(joinCond.getLeft()), result);
          break;
        default:
          LOG.debug("Unhandled join type in stats estimation: " + joinCond.getType());
          break;
        }
      }
      return result;
    }

    private long computeRowCountAssumingInnerJoin(List<Long> rowCountParents, long denom,
        CommonJoinOperator<? extends JoinDesc> join) {
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

      denom = denom == 0 ? 1 : denom;
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
        Map<Integer, Statistics> joinStats, int numAttr) {
      int joinColIdx = 0;
      while (numAttr > 0) {
        long minNDV = Long.MAX_VALUE;

        // find min NDV for joining columns
        for (Map.Entry<Integer, List<String>> entry : joinKeys.entrySet()) {
          int pos = entry.getKey();
          String key = entry.getValue().get(joinColIdx);
          ColStatistics cs = joinStats.get(pos).getColumnStatisticsFromColName(key);
          if (cs != null && cs.getCountDistint() < minNDV) {
            minNDV = cs.getCountDistint();
          }
        }

        // set min NDV value to both columns involved in join
        if (minNDV != Long.MAX_VALUE) {
          for (Map.Entry<Integer, List<String>> entry : joinKeys.entrySet()) {
            int pos = entry.getKey();
            String key = entry.getValue().get(joinColIdx);
            ColStatistics cs = joinStats.get(pos).getColumnStatisticsFromColName(key);
            if (cs != null) {
              cs.setCountDistint(minNDV);
            }
          }
        }

        joinColIdx++;
        numAttr--;
      }
    }

    private long getDenominatorForUnmatchedRows(List<Long> distinctVals) {

      if (distinctVals.isEmpty()) {
        return 2;
      }

      // simple join from 2 relations: denom = min(v1, v2)
      if (distinctVals.size() <= 2) {
        return Collections.min(distinctVals);
      } else {

        // remember max value and ignore it from the denominator
        long maxNDV = distinctVals.get(0);
        int maxIdx = 0;

        for (int i = 1; i < distinctVals.size(); i++) {
          if (distinctVals.get(i) > maxNDV) {
            maxNDV = distinctVals.get(i);
            maxIdx = i;
          }
        }

        // join from multiple relations:
        // denom = Product of all NDVs except the greatest of all
        long denom = 1;
        for (int i = 0; i < distinctVals.size(); i++) {
          if (i != maxIdx) {
            denom = StatsUtils.safeMult(denom, distinctVals.get(i));
          }
        }
        return denom;
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
  public static class LimitStatsRule extends DefaultStatsRule implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      AnnotateStatsProcCtx aspCtx = (AnnotateStatsProcCtx) procCtx;
      LimitOperator lop = (LimitOperator) nd;
      Operator<? extends OperatorDesc> parent = lop.getParentOperators().get(0);
      Statistics parentStats = parent.getStatistics();
      long limit = -1;
      limit = lop.getConf().getLimit();

      if (satisfyPrecondition(parentStats)) {
        Statistics stats = parentStats.clone();
        List<ColStatistics> colStats = StatsUtils.getColStatisticsUpdatingTableAlias(
            parentStats, lop.getSchema());
        stats.setColumnStats(colStats);

        // if limit is greater than available rows then do not update
        // statistics
        if (limit <= parentStats.getNumRows()) {
          StatsUtils.updateStats(stats, limit, true, lop);
        }
        stats = applyRuntimeStats(aspCtx.getParseContext().getContext(), stats, lop);
        lop.setStatistics(stats);

        if (LOG.isDebugEnabled()) {
          LOG.debug("[0] STATS-" + lop.toString() + ": " + stats.extendedToString());
        }
      } else {
        if (parentStats != null) {

          // in the absence of column statistics, compute data size based on
          // based on average row size
          limit = StatsUtils.getMaxIfOverflow(limit);
          Statistics wcStats = parentStats.scaleToRowCount(limit, true);
          wcStats = applyRuntimeStats(aspCtx.getParseContext().getContext(), wcStats, lop);
          lop.setStatistics(wcStats);
          if (LOG.isDebugEnabled()) {
            LOG.debug("[1] STATS-" + lop.toString() + ": " + wcStats.extendedToString());
          }
        }
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
  public static class ReduceSinkStatsRule extends DefaultStatsRule implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
        throws SemanticException {
      ReduceSinkOperator rop = (ReduceSinkOperator) nd;
      Operator<? extends OperatorDesc> parent = rop.getParentOperators().get(0);
      Statistics parentStats = parent.getStatistics();
      if (parentStats != null) {
        AnnotateStatsProcCtx aspCtx = (AnnotateStatsProcCtx) procCtx;
        HiveConf conf = aspCtx.getConf();
        List<String> outKeyColNames = rop.getConf().getOutputKeyColumnNames();
        List<String> outValueColNames = rop.getConf().getOutputValueColumnNames();
        Map<String, ExprNodeDesc> colExprMap = rop.getColumnExprMap();
        Statistics outStats = parentStats.clone();
        if (satisfyPrecondition(parentStats)) {
          List<ColStatistics> colStats = Lists.newArrayList();
          for (String key : outKeyColNames) {
            String prefixedKey = Utilities.ReduceField.KEY.toString() + "." + key;
            ExprNodeDesc end = colExprMap.get(prefixedKey);
            if (end != null) {
              ColStatistics cs = StatsUtils.getColStatisticsFromExpression(conf, parentStats, end);
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
              ColStatistics cs = StatsUtils.getColStatisticsFromExpression(conf, parentStats, end);
              if (cs != null) {
                cs.setColumnName(prefixedVal);
                colStats.add(cs);
              }
            }
          }

          outStats.setColumnStats(colStats);
        }

        outStats = applyRuntimeStats(aspCtx.getParseContext().getContext(), outStats, rop);
        rop.setStatistics(outStats);
        if (LOG.isDebugEnabled()) {
          LOG.debug("[0] STATS-" + rop.toString() + ": " + outStats.extendedToString());
        }
      }
      return null;
    }

  }

  /**
   * UDTF operator changes the number of rows and thereby the data size.
   */
  public static class UDTFStatsRule extends DefaultStatsRule implements SemanticNodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                          Object... nodeOutputs) throws SemanticException {
      AnnotateStatsProcCtx aspCtx = (AnnotateStatsProcCtx) procCtx;
      UDTFOperator uop = (UDTFOperator) nd;

      Operator<? extends OperatorDesc> parent = uop.getParentOperators().get(0);

      Statistics parentStats = parent.getStatistics();

      if (parentStats != null) {
        Statistics st = parentStats.clone();

        float udtfFactor=HiveConf.getFloatVar(aspCtx.getConf(), HiveConf.ConfVars.HIVE_STATS_UDTF_FACTOR);
        long numRows = (long) (parentStats.getNumRows() * udtfFactor);
        long dataSize = StatsUtils.safeMult(parentStats.getDataSize(), udtfFactor);
        st.setNumRows(numRows);
        st.setDataSize(dataSize);

        List<ColStatistics> colStatsList = st.getColumnStats();
        if(colStatsList != null) {
          for (ColStatistics colStats : colStatsList) {
            colStats.setNumFalses((long) (colStats.getNumFalses() * udtfFactor));
            colStats.setNumTrues((long) (colStats.getNumTrues() * udtfFactor));
            colStats.setNumNulls((long) (colStats.getNumNulls() * udtfFactor));
          }
          st.setColumnStats(colStatsList);
        }

        if (LOG.isDebugEnabled()) {
          LOG.debug("[0] STATS-" + uop.toString() + ": " + st.extendedToString());
        }

        uop.setStatistics(st);
      }
      return null;
    }
  }

  /**
   * Default rule is to aggregate the statistics from all its parent operators.
   */
  public static class DefaultStatsRule implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
      OperatorDesc conf = op.getConf();
      AnnotateStatsProcCtx aspCtx = (AnnotateStatsProcCtx) procCtx;
      HiveConf hconf = aspCtx.getConf();

      if (conf != null) {
        Statistics stats = conf.getStatistics();
        if (stats == null && op.getParentOperators() != null) {

          // if parent statistics is null then that branch of the tree is not
          // walked yet. don't update the stats until all branches are walked
          if (isAllParentsContainStatistics(op)) {

            for (Operator<? extends OperatorDesc> parent : op.getParentOperators()) {
              Statistics parentStats = parent.getStatistics();

              if (stats == null) {
                stats = parentStats.clone();
              } else {
                stats.addBasicStats(parentStats);
              }

              stats.updateColumnStatsState(parentStats.getColumnStatsState());
              List<ColStatistics> colStats =
                  StatsUtils.getColStatisticsFromExprMap(hconf, parentStats, op.getColumnExprMap(), op.getSchema());
              stats.addToColumnStats(colStats);

              if (LOG.isDebugEnabled()) {
                LOG.debug("[0] STATS-" + op.toString() + ": " + stats.extendedToString());
              }
            }
            stats = applyRuntimeStats(aspCtx.getParseContext().getContext(), stats, op);
            op.getConf().setStatistics(stats);
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

  public static SemanticNodeProcessor getTableScanRule() {
    return new TableScanStatsRule();
  }

  public static SemanticNodeProcessor getSelectRule() {
    return new SelectStatsRule();
  }

  public static SemanticNodeProcessor getFilterRule() {
    return new FilterStatsRule();
  }

  public static SemanticNodeProcessor getGroupByRule() {
    return new GroupByStatsRule();
  }

  public static SemanticNodeProcessor getJoinRule() {
    return new JoinStatsRule();
  }

  public static SemanticNodeProcessor getLimitRule() {
    return new LimitStatsRule();
  }

  public static SemanticNodeProcessor getReduceSinkRule() {
    return new ReduceSinkStatsRule();
  }

  public static SemanticNodeProcessor getUDTFRule() {
    return new UDTFStatsRule();
  }

  public static SemanticNodeProcessor getDefaultRule() {
    return new DefaultStatsRule();
  }

  static boolean satisfyPrecondition(Statistics stats) {
    return stats != null && stats.getBasicStatsState().equals(Statistics.State.COMPLETE)
        && !stats.getColumnStatsState().equals(Statistics.State.NONE);
  }

  private static Statistics applyRuntimeStats(Context context, Statistics stats, Operator<?> op) {
    if (!((HiveConf) context.getConf()).getBoolVar(ConfVars.HIVE_QUERY_REEXECUTION_ENABLED)) {
      return stats;
    }

    PlanMapper pm = context.getPlanMapper();
    OpTreeSignature treeSig = pm.getSignatureOf(op);
    pm.link(op, treeSig);

    StatsSource statsSource = context.getStatsSource();
    if (!statsSource.canProvideStatsFor(op.getClass())) {
      return stats;
    }

    Optional<OperatorStats> os = statsSource.lookup(treeSig);

    if (!os.isPresent()) {
      return stats;
    }
    LOG.debug("using runtime stats for {}; {}", op, os.get());
    Statistics outStats = stats.clone();
    outStats = outStats.scaleToRowCount(os.get().getOutputRecords(), false);
    outStats.setRuntimeStats(true);
    return outStats;
  }

}
