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
import org.apache.hadoop.hive.ql.parse.type.TypeCheckCtx;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class CommonGroupByPlanReduceSinkOpGenerator {

  @SuppressWarnings("nls")
  public static Result genPlan(QB qb, List<String> dests,
      Operator inputOperatorInfo,
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

    // The group by keys and distinct keys should be the same for all dests, so using the first
    // one to produce these will be the same as using any other.
    String dest = dests.get(0);

    // Pre-compute group-by keys and store in reduceKeys
    List<String> outputKeyColumnNames = new ArrayList<String>();
    List<String> outputValueColumnNames = new ArrayList<String>();
    List<ASTNode> grpByExprs = SemanticAnalyzer.getGroupByForClause(parseInfo, dest, sa.isCalcitePlanner());

    List<ExprNodeDesc> reduceKeys = SemanticAnalyzer.getReduceKeysForReduceSink(grpByExprs,
        reduceSinkInputRowResolver, reduceSinkOutputRowResolver, outputKeyColumnNames,
        colExprMap, sa.getUnparseTranslator(), sa.getConf());

    int keyLength = reduceKeys.size();

    List<List<Integer>> distinctColIndices = SemanticAnalyzer.getDistinctColIndicesForReduceSink(parseInfo, dest,
        reduceKeys, reduceSinkInputRowResolver, reduceSinkOutputRowResolver, outputKeyColumnNames,
        colExprMap, sa.getUnparseTranslator(), sa.getConf());

    List<ExprNodeDesc> reduceValues = new ArrayList<ExprNodeDesc>();

    // The dests can have different non-distinct aggregations, so we have to iterate over all of
    // them
    for (String destination : dests) {

      SemanticAnalyzer.getReduceValuesForReduceSinkNoMapAgg(parseInfo, destination, reduceSinkInputRowResolver,
          reduceSinkOutputRowResolver, outputValueColumnNames, reduceValues, colExprMap,
          sa.getUnparseTranslator(), sa.getConf());

      // Need to pass all of the columns used in the where clauses as reduce values
      ASTNode whereClause = parseInfo.getWhrForClause(destination);
      if (whereClause != null) {
        assert whereClause.getChildCount() == 1;
        ASTNode predicates = (ASTNode) whereClause.getChild(0);

        TypeCheckCtx tcCtx = new TypeCheckCtx(reduceSinkInputRowResolver);
        tcCtx.setUnparseTranslator(sa.getUnparseTranslator());
        Map<ASTNode, ExprNodeDesc> nodeOutputs =
            SemanticAnalyzer.genAllExprNodeDesc(predicates, reduceSinkInputRowResolver, tcCtx, sa.getConf());
        // mutates nodeOutputs    
        SemanticAnalyzer.removeMappingForKeys(predicates, nodeOutputs, reduceKeys);

        // extract columns missing in current RS key/value
        for (Map.Entry<ASTNode, ExprNodeDesc> entry : nodeOutputs.entrySet()) {
          ASTNode parameter = entry.getKey();
          ExprNodeDesc expression = entry.getValue();
          if (!(expression instanceof ExprNodeColumnDesc)) {
            continue;
          }
          if (ExprNodeDescUtils.indexOf(expression, reduceValues) >= 0) {
            continue;
          }
          String internalName = SemanticAnalyzer.getColumnInternalName(reduceValues.size());
          String field = Utilities.ReduceField.VALUE.toString() + "." + internalName;

          reduceValues.add(expression);
          outputValueColumnNames.add(internalName);
          reduceSinkOutputRowResolver.putExpression(parameter,
              new ColumnInfo(field, expression.getTypeInfo(), null, false));
          colExprMap.put(field, expression);
        }
      }
    }
    // Optimize the scenario when there are no grouping keys - only 1 reducer is needed
    int numReducers = -1;
    if (grpByExprs.isEmpty()) {
      numReducers = 1;
    }
    ReduceSinkDesc rsDesc = PlanUtils.getReduceSinkDesc(reduceKeys, keyLength, reduceValues,
        distinctColIndices, outputKeyColumnNames, outputValueColumnNames,
        true, -1, keyLength, numReducers, AcidUtils.Operation.NOT_ACID, sa.getDefaultNullOrdering());

    Operator rsOp = GenFileSinkPlan.createOperator(rsDesc,
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
