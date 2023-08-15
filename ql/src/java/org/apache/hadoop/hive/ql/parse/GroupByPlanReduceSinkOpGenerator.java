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

package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.OrderExpression;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowExpressionSpec;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class GroupByPlanReduceSinkOpGenerator {
  /**
   * Generate the ReduceSinkOperator for the Group By Query Block
   * (qb.getPartInfo().getXXX(dest)). The new ReduceSinkOperator will be a child
   * of inputOperatorInfo.
   *
   * It will put all Group By keys and the distinct field (if any) in the
   * map-reduce sort key, and all other fields in the map-reduce value.
   *
   * @param numPartitionFields
   *          the number of fields for map-reduce partitioning. This is usually
   *          the number of fields in the Group By keys.
   * @return the new ReduceSinkOperator.
   * @throws SemanticException
   */
  @SuppressWarnings("nls")
  public static Result genPlan(QB qb,
      String dest,
      Operator inputOperatorInfo,
      List<ASTNode> grpByExprs,
      int numPartitionFields,
      boolean changeNumPartitionFields,
      int numReducers,
      boolean mapAggrDone,
      boolean groupingSetsPresent,
      ReadOnlySemanticAnalyzer sa,
      Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap
      ) throws SemanticException {

    Map<Operator<? extends OperatorDesc>, OpParseContext> newOperatorMap = new HashMap<>();

    RowResolver reduceSinkInputRowResolver = operatorMap.get(inputOperatorInfo)
        .getRowResolver();
    QBParseInfo parseInfo = qb.getParseInfo();
    RowResolver reduceSinkOutputRowResolver = new RowResolver();
    reduceSinkOutputRowResolver.setIsExprResolver(true);
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    // Pre-compute group-by keys and store in reduceKeys

    List<String> outputKeyColumnNames = new ArrayList<String>();
    List<String> outputValueColumnNames = new ArrayList<String>();

    List<ExprNodeDesc> reduceKeys = SemanticAnalyzer.getReduceKeysForReduceSink(grpByExprs,
        reduceSinkInputRowResolver, reduceSinkOutputRowResolver, outputKeyColumnNames,
        colExprMap, sa.getUnparseTranslator(), sa.getConf());

    int keyLength = reduceKeys.size();
    int numOfColsRmedFromkey = grpByExprs.size() - keyLength;

    // add a key for reduce sink
    if (groupingSetsPresent) {
      // Process grouping set for the reduce sink operator
      SemanticAnalyzer.processGroupingSetReduceSinkOperator(
          reduceSinkInputRowResolver,
          reduceSinkOutputRowResolver,
          reduceKeys,
          outputKeyColumnNames,
          colExprMap);

      if (changeNumPartitionFields) {
        numPartitionFields++;
      }
    }

    List<List<Integer>> distinctColIndices = SemanticAnalyzer.getDistinctColIndicesForReduceSink(parseInfo, dest,
        reduceKeys, reduceSinkInputRowResolver, reduceSinkOutputRowResolver, outputKeyColumnNames,
        colExprMap, sa.getUnparseTranslator(), sa.getConf());

    List<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();
    Map<String, ASTNode> aggregationTrees = parseInfo.getAggregationExprsForClause(dest);

    if (!mapAggrDone) {
      SemanticAnalyzer.getReduceValuesForReduceSinkNoMapAgg(parseInfo, dest, reduceSinkInputRowResolver,
          reduceSinkOutputRowResolver, outputValueColumnNames, reduceValues, colExprMap,
          sa.getUnparseTranslator(), sa.getConf());
    } else {
      // Put partial aggregation results in reduceValues
      int inputField = reduceKeys.size() + numOfColsRmedFromkey;

      for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {

        TypeInfo type = reduceSinkInputRowResolver.getColumnInfos().get(
            inputField).getType();
        ExprNodeColumnDesc exprDesc = new ExprNodeColumnDesc(type,
            SemanticAnalyzer.getColumnInternalName(inputField), "", false);
        reduceValues.add(exprDesc);
        inputField++;
        String outputColName = SemanticAnalyzer.getColumnInternalName(reduceValues.size() - 1);
        outputValueColumnNames.add(outputColName);
        String internalName = Utilities.ReduceField.VALUE.toString() + "."
            + outputColName;
        reduceSinkOutputRowResolver.putExpression(entry.getValue(),
            new ColumnInfo(internalName, type, null, false));
        colExprMap.put(internalName, exprDesc);
      }
    }

    Operator rsOp= GenFileSinkPlan.createOperator(
        PlanUtils.getReduceSinkDesc(reduceKeys,
            groupingSetsPresent ? keyLength + 1 : keyLength,
            reduceValues, distinctColIndices,
            outputKeyColumnNames, outputValueColumnNames, true, -1, numPartitionFields,
            numReducers, AcidUtils.Operation.NOT_ACID, sa.getDefaultNullOrdering()),
        new RowSchema(reduceSinkOutputRowResolver.getColumnInfos()), inputOperatorInfo);
    newOperatorMap.put(rsOp, new OpParseContext(reduceSinkOutputRowResolver));
    rsOp.setColumnExprMap(colExprMap);
    return new Result(rsOp, newOperatorMap);
  }

  public static class Result {
    private final Operator<? extends OperatorDesc> reduceOperator;
    private final Map<Operator<? extends OperatorDesc>, OpParseContext> finalOperatorMap;

    public Result(Operator<? extends OperatorDesc> reduceOperator,
        Map<Operator<? extends OperatorDesc>, OpParseContext> finalOperatorMap) {
      this.reduceOperator = reduceOperator;
      this.finalOperatorMap = finalOperatorMap;
    }

    public Operator getOperator() {
      return reduceOperator;
    }

    public Map<Operator<? extends OperatorDesc>, OpParseContext> getOperatorMap() {
      return finalOperatorMap;
    }
  }
}
  
