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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.AbstractMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
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
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

public class GroupingSetOptimizer extends Transform {
  private static final Logger LOG = LoggerFactory.getLogger(GroupingSetOptimizer.class);

  private static class GroupingSetProcessorContext implements NodeProcessorCtx {
    public final long bytesPerReducer;
    public final int maxReducers;
    public final long groupingSetThreshold;

    public GroupingSetProcessorContext(HiveConf hiveConf) {
      bytesPerReducer = hiveConf.getLongVar(HiveConf.ConfVars.BYTES_PER_REDUCER);
      maxReducers = hiveConf.getIntVar(HiveConf.ConfVars.MAX_REDUCERS);
      groupingSetThreshold = hiveConf.getLongVar(HiveConf.ConfVars.HIVE_OPTIMIZE_GROUPING_SET_THRESHOLD);
    }
  }

  private static class GroupingSetProcessor implements SemanticNodeProcessor {
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      GroupingSetProcessorContext context = (GroupingSetProcessorContext) procCtx;
      GroupByOperator gby = (GroupByOperator) nd;
      if (!isGroupByFeasible(gby, context)) {
        return null;
      }

      Operator<?> parentOp = gby.getParentOperators().get(0);
      if (!isParentOpFeasible(parentOp)) {
        return null;
      }

      String partitionCol = selectPartitionColumn(gby, parentOp);
      if (partitionCol == null) {
        return null;
      }

      LOG.info("Applying GroupingSetOptimization: partitioning the input data of {} by {}",
          gby, partitionCol);

      ReduceSinkOperator rs = createReduceSink(parentOp, partitionCol, context);

      parentOp.removeChild(gby);
      // gby.setParentOperators(Arrays.asList(rs));
      // NOTE: The above expression does not work because GBY refers to _colN
      //  while input columns are VALUE._colN. Therefore, we should either modify GBY expressions
      //  or insert a new SEL that renames columns. The following code implements the later one as it is
      //  easier to implement.

      SelectOperator sel = createSelect(parentOp.getSchema().getSignature(), partitionCol, rs);

      sel.setChildOperators(Arrays.asList(gby));
      gby.setParentOperators(Arrays.asList(sel));

      return null;
    }

    private boolean isGroupByFeasible(GroupByOperator gby, GroupingSetProcessorContext context) {
      if (!gby.getConf().isGroupingSetsPresent() || gby.getStatistics() == null) {
        return false;
      }

      if (gby.getStatistics().getNumRows() < context.groupingSetThreshold) {
        LOG.debug("Skip grouping-set optimization on a small operator: {}", gby);
        return false;
      }

      if (gby.getParentOperators().size() != 1) {
        LOG.debug("Skip grouping-set optimization on a operator with multiple parent operators: {}", gby);
        return false;
      }

      return true;
    }

    private boolean isParentOpFeasible(Operator<?> parentOp) {
      ReduceSinkOperator rs = null;

      Operator<?> curOp = parentOp;
      while (true) {
        if (curOp instanceof ReduceSinkOperator) {
          rs = (ReduceSinkOperator) curOp;
          break;
        }

        if (curOp.getParentOperators() == null) {
          break;
        }

        if (curOp.getParentOperators().size() == 1) {
          curOp = curOp.getParentOperators().get(0);
        } else if (curOp instanceof AbstractMapJoinOperator) {
          MapJoinDesc desc = ((AbstractMapJoinOperator<?>) curOp).getConf();
          curOp = curOp.getParentOperators().get(desc.getPosBigTable());
        } else {
          break;
        }
      }

      if (rs == null) {
        // There is no partitioning followed by this parentOp. Continue optimization.
        return true;
      }

      if (rs.getConf().getPartitionCols() != null && rs.getConf().getPartitionCols().size() > 0) {
        // This rs might be irrelevant to the target GroupBy operator. For example, the following query:
        //   SELECT a, b, sum(c) FROM (SELECT a, b, c FROM tbl DISTRIBUTE BY c) z GROUP BY rollup(a, b)
        // won't be optimized although 'DISTRIBUTE BY c' is irrelevant to the key columns of GroupBy.
        LOG.debug("Skip grouping-set optimization in order not to introduce possibly redundant shuffle.");
        return false;
      } else {
        // No partitioning. Continue optimization.
        return true;
      }
    }

    private String selectPartitionColumn(GroupByOperator gby, Operator<?> parentOp) {
      if (parentOp.getColumnExprMap() == null) {
        LOG.debug("Skip grouping-set optimization as the parent operator {} does not define a column " +
                        "expression mapping", parentOp);
        return null;
      }

      if (parentOp.getSchema() == null || parentOp.getSchema().getSignature() == null) {
        LOG.debug("Skip grouping-set optimization as the parent operator {} does not provide signature",
            parentOp);
        return null;
      }

      if (parentOp.getStatistics() == null ||
          parentOp.getStatistics().getNumRows() <= 0 ||
          parentOp.getStatistics().getColumnStats() == null) {
        LOG.debug("Skip grouping-set optimization as the parent operator {} does not provide statistics",
            parentOp);
        return null;
      }

      if (parentOp.getStatistics().getNumRows() > gby.getStatistics().getNumRows()) {
        LOG.debug("Skip grouping-set optimization as the parent operator {} emits more rows than {}",
            parentOp, gby);
        return null;
      }

      List<String> colNamesInSignature = new ArrayList<>();
      for (ColumnInfo pColInfo: parentOp.getSchema().getSignature()) {
        colNamesInSignature.add(pColInfo.getInternalName());
      }

      List<Integer> groupingSetKeys = listGroupingSetKeyPositions(gby.getConf().getListGroupingSets());
      Set<String> candidates = new HashSet<>();
      for (Integer groupingSetKeyPosition: groupingSetKeys) {
        ExprNodeDesc key = gby.getConf().getKeys().get(groupingSetKeyPosition);

        if (key instanceof ExprNodeColumnDesc) {
          candidates.add(((ExprNodeColumnDesc) key).getColumn());
        }
      }
      candidates.retainAll(colNamesInSignature);

      List<ColStatistics> columnStatistics =
          new ArrayList<>(parentOp.getStatistics().getColumnStats()).stream()
              .filter(cs -> cs.getCountDistint() > 0)
              .sorted(Comparator.comparingLong(ColStatistics::getCountDistint).reversed())
              .collect(Collectors.toList());

      String partitionCol = null;
      for (ColStatistics col: columnStatistics) {
        String colName = col.getColumnName();
        if (parentOp.getColumnExprMap().containsKey(colName) && candidates.contains(colName)) {
          partitionCol = colName;
          break;
        }
      }

      if (partitionCol == null) {
        LOG.debug("Skip grouping-set optimization as there is no feasible column in parent operator {}.",
            parentOp);
      }

      return partitionCol;
    }

    private ReduceSinkOperator createReduceSink(Operator<?> parentOp, String partitionColName,
        GroupingSetProcessorContext context) {
      Map<String, ExprNodeDesc> colExprMap = new HashMap<>();
      List<ExprNodeDesc> keyColumns = new ArrayList<>();
      List<String> keyColumnNames = new ArrayList<>();
      List<ExprNodeDesc> valueColumns = new ArrayList<>();
      List<String> valueColumnNames = new ArrayList<>();
      List<ColumnInfo> signature = new ArrayList<>();
      List<ExprNodeDesc> partCols = new ArrayList<>();

      for (ColumnInfo pColInfo: parentOp.getSchema().getSignature()) {
        ColumnInfo cColInfo = new ColumnInfo(pColInfo);
        String pColName = pColInfo.getInternalName();

        if (pColName.equals(partitionColName)) {
          keyColumnNames.add(pColName);

          String cColName = Utilities.ReduceField.KEY + "." + pColName;
          cColInfo.setInternalName(cColName);
          signature.add(cColInfo);

          ExprNodeDesc keyExpr = new ExprNodeColumnDesc(pColInfo);
          keyColumns.add(keyExpr);
          colExprMap.put(cColName, keyExpr);

          partCols.add(keyExpr);
        } else {
          valueColumnNames.add(pColName);

          String cColName = Utilities.ReduceField.VALUE + "." + pColName;
          cColInfo.setInternalName(cColName);
          signature.add(cColInfo);

          ExprNodeDesc valueExpr = new ExprNodeColumnDesc(pColInfo);
          valueColumns.add(valueExpr);
          colExprMap.put(cColName, valueExpr);
        }
      }

      List<FieldSchema> valueFields =
          PlanUtils.getFieldSchemasFromColumnList(valueColumns, valueColumnNames, 0, "");
      TableDesc valueTable = PlanUtils.getReduceValueTableDesc(valueFields);

      List<FieldSchema> keyFields =
          PlanUtils.getFieldSchemasFromColumnList(keyColumns, keyColumnNames, 0, "");
      TableDesc keyTable = PlanUtils.getReduceKeyTableDesc(keyFields, "+", "z");
      List<List<Integer>> distinctColumnIndices = new ArrayList<>();

      // If we run SetReducerParallelism after this optimization, then we don't have to compute numReducers.
      int numReducers = Utilities.estimateReducers(
          parentOp.getStatistics().getDataSize(), context.bytesPerReducer, context.maxReducers, false);

      ReduceSinkDesc rsConf = new ReduceSinkDesc(keyColumns, keyColumns.size(), valueColumns,
          keyColumnNames, distinctColumnIndices, valueColumnNames, -1, partCols, numReducers, keyTable,
          valueTable, AcidUtils.Operation.NOT_ACID);

      ReduceSinkOperator rs =
          (ReduceSinkOperator) OperatorFactory.getAndMakeChild(rsConf, new RowSchema(signature), parentOp);
      rs.setColumnExprMap(colExprMap);

      // If we run SetReducerParallelism after this optimization, the following code becomes unnecessary.
      rsConf.setReducerTraits(EnumSet.of(ReducerTraits.UNIFORM, ReducerTraits.AUTOPARALLEL));

      return rs;
    }

    private SelectOperator createSelect(List<ColumnInfo> signature,  String partitionColName,
        Operator<?> parentOp) {
      List<String> selColNames = new ArrayList<>();
      List<ExprNodeDesc> selColumns = new ArrayList<>();
      List<ColumnInfo> selSignature = new ArrayList<>();
      Map<String, ExprNodeDesc> colExprMap = new HashMap<>();

      for (ColumnInfo pColInfo: signature) {
        String origColName = pColInfo.getInternalName();
        String rsColName;

        if (origColName.equals(partitionColName)) {
          rsColName = Utilities.ReduceField.KEY + "." + origColName;
        } else {
          rsColName = Utilities.ReduceField.VALUE + "." + origColName;
        }

        ColumnInfo selColInfo = new ColumnInfo(pColInfo);

        ExprNodeDesc selExpr = new ExprNodeColumnDesc(pColInfo.getType(), rsColName, null, false);

        selSignature.add(selColInfo);
        selColumns.add(selExpr);
        selColNames.add(origColName);
        colExprMap.put(origColName, selExpr);
      }

      SelectDesc selConf = new SelectDesc(selColumns, selColNames);
      SelectOperator sel =
          (SelectOperator) OperatorFactory.getAndMakeChild(selConf, new RowSchema(selSignature), parentOp);
      sel.setColumnExprMap(colExprMap);

      return sel;
    }

    private List<Integer> listGroupingSetKeyPositions(List<Long> groupingSets) {
      long acc = 0L;
      for (Long groupingSet: groupingSets) {
        acc |= groupingSet;
      }

      BitSet bitset = BitSet.valueOf(new long[]{acc});
      List<Integer> ret = new ArrayList<>();
      for (int i = bitset.nextSetBit(0); i >= 0; i = bitset.nextSetBit(i + 1)) {
        ret.add(i);
      }

      return ret;
    }
  }

  @Override
  public ParseContext transform(ParseContext pCtx) throws SemanticException {
    Map<SemanticRule, SemanticNodeProcessor> testRules = new LinkedHashMap<>();
    testRules.put(new RuleRegExp("GBY", GroupByOperator.getOperatorName() + "%"),
        new GroupingSetProcessor()
    );

    SemanticDispatcher disp =
        new DefaultRuleDispatcher(null, testRules, new GroupingSetProcessorContext(pCtx.getConf()));
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pCtx.getTopOps().values());
    ogw.startWalking(topNodes, null);

    return pCtx;
  }
}

