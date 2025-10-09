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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.udf.UDFToShort;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPMultiply;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNot;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPPlus;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

/**
 * Append a filterTag computation column to ReduceSinkOperators whose child is MapJoinOperator.
 * The added column expresses JoinUtil#isFiltered in the form of ExprNode.
 * The added column should be located at the end of row as CommonJoinOperator expects it.
 *
 * This processor only affects small tables of MapJoin that run on Tez engine.
 * For big table, MapJoinOperator#process() calls CommonJoinOperator#getFilteredValue(), which adds filterTag.
 * For MapReduce engine, HashTableSinkOperator adds filterTag to every row.
 */
public class FiltertagAppenderProc implements SemanticNodeProcessor {

  private final TypeInfo shortType = TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.SMALLINT_TYPE_NAME);

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
      throws SemanticException {
    MapJoinOperator mapJoinOp = (MapJoinOperator) nd;
    MapJoinDesc mapJoinDesc = mapJoinOp.getConf();

    if (mapJoinDesc.getFilterMap() == null) {
      return null;
    }

    int[][] filterMap = mapJoinDesc.getFilterMap();

    // 1. Extend ReduceSinkoperator if it's output is filtered by MapJoinOperator.
    for (byte pos = 0; pos < filterMap.length; pos++) {
      if (pos == mapJoinDesc.getPosBigTable() || filterMap[pos] == null) {
        continue;
      }

      ExprNodeDesc filterTagExpr =
          generateFilterTagExpression(filterMap[pos], mapJoinDesc.getFilters().get(pos));

      // Note that the parent RS for the given pos is retrieved in different way in MapJoinProcessor.
      // TODO: MapJoinProcessor.convertMapJoin() fixes the order of parent operators.
      //  Does other callers also fix the order as well as MapJoinProcessor.convertMapJoin()?
      ReduceSinkOperator parent = (ReduceSinkOperator) mapJoinOp.getParentOperators().get(pos);
      ReduceSinkDesc pRsConf = parent.getConf();

      // MapJoinProcessor.getMapJoinDesc() replaces filter expressions with backtracked one if
      // adjustParentsChildren is true.
      // As of now, ConvertJoinMapJoin.convertJoinDynamicPartitionedHashJoin() is the only functions that
      // calls this method with adjustParentsChildren = false. Therefore, we backtrack filter expressions
      // only if MapJoinDesc.isDynamicPartitionHashJoin is true, which is also the unique property of
      // ConvertJoinMapJoin.convertJoinDynamicPartitionedHashJoin().
      ExprNodeDesc mapSideFilterTagExpr;
      if (mapJoinDesc.isDynamicPartitionHashJoin()) {
        mapSideFilterTagExpr = ExprNodeDescUtils.backtrack(filterTagExpr, mapJoinOp, parent);
      } else {
        mapSideFilterTagExpr = filterTagExpr;
      }
      String filterColumnName = "_filterTag";

      pRsConf.getValueCols().add(mapSideFilterTagExpr);
      pRsConf.getOutputValueColumnNames().add(filterColumnName);
      pRsConf.getColumnExprMap()
          .put(Utilities.ReduceField.VALUE + "." + filterColumnName, mapSideFilterTagExpr);

      ColumnInfo filterTagColumnInfo =
          new ColumnInfo(Utilities.ReduceField.VALUE + "." + filterColumnName, shortType, "", false);
      parent.getSchema().getSignature().add(filterTagColumnInfo);

      TableDesc newTableDesc =
          PlanUtils.getReduceValueTableDesc(
              PlanUtils.getFieldSchemasFromColumnList(pRsConf.getValueCols(), "_col"));
      pRsConf.setValueSerializeInfo(newTableDesc);
    }

    // 2. Update MapJoinOperator's valueFilteredTableDescs.
    // Unlike HashTableSinkOperator used in MR engine, Tez engine directly passes rows from RS to MapJoin.
    // Therefore, RS's writer and MapJoin's reader should have the same TableDesc. We create valueTableDesc
    // here again because it can be different from RS's valueSerializeInfo due to ColumnPruner.
    List<TableDesc> newMapJoinValueFilteredTableDescs =
        new ArrayList<>(mapJoinOp.getParentOperators().size());
    for (byte pos = 0; pos < mapJoinOp.getParentOperators().size(); pos++) {
      TableDesc tableDesc;

      if (pos == mapJoinDesc.getPosBigTable() || filterMap[pos] == null) {
        // We did not change corresponding parent operator. Use the original tableDesc.
        tableDesc = mapJoinDesc.getValueFilteredTblDescs().get(pos);
      } else {
        // Create a new TableDesc based on corresponding parent RSOperator.
        ReduceSinkOperator parent = (ReduceSinkOperator) mapJoinOp.getParentOperators().get(pos);
        ReduceSinkDesc pRsConf = parent.getConf();

        tableDesc =
            PlanUtils.getMapJoinValueTableDesc(
                PlanUtils.getFieldSchemasFromColumnList(pRsConf.getValueCols(), "mapjoinvalue"));
      }

      newMapJoinValueFilteredTableDescs.add(tableDesc);
    }
    mapJoinDesc.setValueFilteredTblDescs(newMapJoinValueFilteredTableDescs);

    return null;
  }

  /**
   * Generate an ExprNodeDesc that expresses the following method:
   * JoinUtil#isFiltered(Object, List<ExprNodeEvaluator>, List<ObjectInspector>, int[]).
   */
  private ExprNodeDesc generateFilterTagExpression(int[] filterMap, List<ExprNodeDesc> filterExprs) {
    ExprNodeDesc filterTagExpr = new ExprNodeConstantDesc(shortType, (short) 0);
    Map<Byte, ExprNodeDesc> filterExprMap = getFilterExprMap(filterMap, filterExprs);

    for (Map.Entry<Byte, ExprNodeDesc> entry: filterExprMap.entrySet()) {
      ExprNodeDesc filterTagMaskExpr = generateFilterTagMask(entry.getKey(), entry.getValue());

      if (filterTagExpr instanceof ExprNodeConstantDesc) {
        filterTagExpr = filterTagMaskExpr;
      } else {
        List<ExprNodeDesc> plusArgs = Arrays.asList(filterTagMaskExpr, filterTagExpr);
        filterTagExpr = new ExprNodeGenericFuncDesc(shortType, new GenericUDFOPPlus(), plusArgs);
      }
    }

    return filterTagExpr;
  }

  /**
   * Group filterExprs by tag and merge each of them into a single boolean ExprNodeDesc using AND operator.
   * filterInfo is repetition of tag and the length of corresponding filter expressions.
   * For example, filterInfo = {0, 2, 1, 3} means that the first 2 elements in filterExprs belong to tag 0,
   * and the remaining 3 elements belong to tag 1.
   */
  private Map<Byte, ExprNodeDesc> getFilterExprMap(int[] filterInfo, List<ExprNodeDesc> filterExprs) {
    Map<Byte, ExprNodeDesc> filterExprMap = new HashMap<>();

    int exprListOffset = 0;
    for (int idx = 0; idx < filterInfo.length; idx = idx + 2) {
      byte tag = (byte) filterInfo[idx];
      int length = filterInfo[idx + 1];

      int nextExprOffset = exprListOffset + length;
      List<ExprNodeDesc> andArgs = filterExprs.subList(exprListOffset, nextExprOffset);
      exprListOffset = nextExprOffset;

      if (andArgs.size() == 1) {
        filterExprMap.put(tag, andArgs.get(0));
      } else if (andArgs.size() > 1) {
        filterExprMap.put(tag, ExprNodeDescUtils.and(andArgs));
      }
    }

    return filterExprMap;
  }

  /**
   * Generate an ExprNodeDesc that expresses the following code:
   *   UDFToShort(!condition) * (short) (1 << tag),
   * which is logically equivalent to
   *   if (condition) { return (short) 0 } else { return (short) (1 << tag); }.
   */
  private ExprNodeDesc generateFilterTagMask(byte tag, ExprNodeDesc condition) {
    ExprNodeDesc filterMaskValue = new ExprNodeConstantDesc(shortType, (short) (1 << tag));

    List<ExprNodeDesc> negateArg = Collections.singletonList(condition);
    ExprNodeDesc negate = new ExprNodeGenericFuncDesc(
        TypeInfoFactory.getPrimitiveTypeInfo(serdeConstants.BOOLEAN_TYPE_NAME),
        new GenericUDFOPNot(),
        negateArg);

    GenericUDFBridge toShort = new GenericUDFBridge();
    toShort.setUdfClassName(UDFToShort.class.getName());
    toShort.setUdfName(UDFToShort.class.getSimpleName());

    List<ExprNodeDesc> toShortArg = Collections.singletonList(negate);
    ExprNodeDesc conditionAsShort = new ExprNodeGenericFuncDesc(shortType, toShort, toShortArg);

    List<ExprNodeDesc> multiplyArgs = Arrays.asList(conditionAsShort, filterMaskValue);
    ExprNodeDesc multiply = new ExprNodeGenericFuncDesc(shortType, new GenericUDFOPMultiply(), multiplyArgs);

    return multiply;
  }
}
