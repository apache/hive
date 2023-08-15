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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.ErrorMsg;
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
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.GenericUDAFInfo;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.PTFDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;


public class GroupByPlanGroupByOpGenerator {
  /**
   * Generate the GroupByOperator for the Query Block (parseInfo.getXXX(dest)).
   * The new GroupByOperator will be a child of the reduceSinkOperatorInfo.
   *
   * @param mode
   *          The mode of the aggregation (PARTIAL1 or COMPLETE)
   * @param genericUDAFEvaluators
   *          If not null, this function will store the mapping from Aggregation
   *          StringTree to the genericUDAFEvaluator in this parameter, so it
   *          can be used in the next-stage GroupBy aggregations.
   * @return the new GroupByOperator
   */
  @SuppressWarnings("nls")
  public static Result genPlan(QBParseInfo parseInfo,
      String dest, Operator input, ReduceSinkOperator rs, GroupByDesc.Mode mode,
      Map<String, GenericUDAFEvaluator> genericUDAFEvaluators,
      ReadOnlySemanticAnalyzer sa,
      Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap
      ) throws SemanticException {
    Map<Operator<? extends OperatorDesc>, OpParseContext> newOperatorMap = new HashMap<>();

    RowResolver groupByInputRowResolver = operatorMap.get(input).getRowResolver();
    RowResolver groupByOutputRowResolver = new RowResolver();
    groupByOutputRowResolver.setIsExprResolver(true);
    List<ExprNodeDesc> groupByKeys = new ArrayList<ExprNodeDesc>();
    List<AggregationDesc> aggregations = new ArrayList<AggregationDesc>();
    List<String> outputColumnNames = new ArrayList<String>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<String, ExprNodeDesc>();
    List<ASTNode> grpByExprs = SemanticAnalyzer.getGroupByForClause(parseInfo, dest, sa.isCalcitePlanner());
    for (int i = 0; i < grpByExprs.size(); ++i) {
      ASTNode grpbyExpr = grpByExprs.get(i);
      ColumnInfo exprInfo = groupByInputRowResolver.getExpression(grpbyExpr);

      if (exprInfo == null) {
        throw new SemanticException(ASTErrorUtils.getMsg(
            ErrorMsg.INVALID_COLUMN.getMsg(), grpbyExpr));
      }

      groupByKeys.add(new ExprNodeColumnDesc(exprInfo.getType(), exprInfo
          .getInternalName(), "", false));
      String field = SemanticAnalyzer.getColumnInternalName(i);
      outputColumnNames.add(field);
      ColumnInfo oColInfo = new ColumnInfo(field, exprInfo.getType(), null, false);
      groupByOutputRowResolver.putExpression(grpbyExpr,
          oColInfo);
      // mutates groupByOutputRowResolver
      SemanticAnalyzer.addAlternateGByKeyMappings(grpbyExpr, oColInfo, input, groupByOutputRowResolver,
          operatorMap);
      colExprMap.put(field, groupByKeys.get(groupByKeys.size() - 1));
    }
    // For each aggregation
    Map<String, ASTNode> aggregationTrees = parseInfo.getAggregationExprsForClause(dest);
    assert (aggregationTrees != null);
    // get the last colName for the reduce KEY
    // it represents the column name corresponding to distinct aggr, if any
    String lastKeyColName = null;
    List<String> inputKeyCols = rs.getConf().getOutputKeyColumnNames();
    if (inputKeyCols.size() > 0) {
      lastKeyColName = inputKeyCols.get(inputKeyCols.size() - 1);
    }
    List<ExprNodeDesc> reduceValues = rs.getConf().getValueCols();
    int numDistinctUDFs = 0;
    for (Map.Entry<String, ASTNode> entry : aggregationTrees.entrySet()) {
      ASTNode value = entry.getValue();

      // This is the GenericUDAF name
      String aggName = BaseSemanticAnalyzer.unescapeIdentifier(value.getChild(0).getText());
      boolean isDistinct = value.getType() == HiveParser.TOK_FUNCTIONDI;
      boolean isAllColumns = value.getType() == HiveParser.TOK_FUNCTIONSTAR;

      // Convert children to aggParameters
      List<ExprNodeDesc> aggParameters = new ArrayList<ExprNodeDesc>();
      // 0 is the function name
      for (int i = 1; i < value.getChildCount(); i++) {
        ASTNode paraExpr = (ASTNode) value.getChild(i);
        ColumnInfo paraExprInfo =
            groupByInputRowResolver.getExpression(paraExpr);
        if (paraExprInfo == null) {
          throw new SemanticException(ASTErrorUtils.getMsg(
              ErrorMsg.INVALID_COLUMN.getMsg(), paraExpr));
        }

        String paraExpression = paraExprInfo.getInternalName();
        assert (paraExpression != null);
        if (isDistinct && lastKeyColName != null) {
          // if aggr is distinct, the parameter is name is constructed as
          // KEY.lastKeyColName:<tag>._colx
          paraExpression = Utilities.ReduceField.KEY.name() + "." +
              lastKeyColName + ":" + numDistinctUDFs + "." +
              SemanticAnalyzer.getColumnInternalName(i - 1);

        }

        ExprNodeDesc expr = new ExprNodeColumnDesc(paraExprInfo.getType(),
            paraExpression, paraExprInfo.getTabAlias(),
            paraExprInfo.getIsVirtualCol());
        ExprNodeDesc reduceValue = SemanticAnalyzer.isConstantParameterInAggregationParameters(
            paraExprInfo.getInternalName(), reduceValues);

        if (reduceValue != null) {
          // this parameter is a constant
          expr = reduceValue;
        }

        aggParameters.add(expr);
      }

      if (isDistinct) {
        numDistinctUDFs++;
      }
      Mode amode = SemanticAnalyzer.groupByDescModeToUDAFMode(mode, isDistinct);
      GenericUDAFEvaluator genericUDAFEvaluator = SemanticAnalyzer.getGenericUDAFEvaluator(
          aggName, aggParameters, value, isDistinct, isAllColumns);
      assert (genericUDAFEvaluator != null);
      GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo(genericUDAFEvaluator, amode, aggParameters);
      aggregations.add(new AggregationDesc(aggName.toLowerCase(),
          udaf.genericUDAFEvaluator, udaf.convertedParameters, isDistinct,
          amode));
      String field = SemanticAnalyzer.getColumnInternalName(groupByKeys.size()
          + aggregations.size() - 1);
      outputColumnNames.add(field);
      groupByOutputRowResolver.putExpression(value, new ColumnInfo(
          field, udaf.returnType, "", false));
      // Save the evaluator so that it can be used by the next-stage
      // GroupByOperators
      if (genericUDAFEvaluators != null) {
        genericUDAFEvaluators.put(entry.getKey(), genericUDAFEvaluator);
      }
    }
    float groupByMemoryUsage = HiveConf.getFloatVar(sa.getConf(), HiveConf.ConfVars.HIVEMAPAGGRHASHMEMORY);
    float memoryThreshold = HiveConf
        .getFloatVar(sa.getConf(), HiveConf.ConfVars.HIVEMAPAGGRMEMORYTHRESHOLD);
    float minReductionHashAggr = HiveConf
        .getFloatVar(sa.getConf(), HiveConf.ConfVars.HIVEMAPAGGRHASHMINREDUCTION);
    float minReductionHashAggrLowerBound = HiveConf
        .getFloatVar(sa.getConf(), ConfVars.HIVEMAPAGGRHASHMINREDUCTIONLOWERBOUND);

    Operator op = GenFileSinkPlan.createOperator(
        new GroupByDesc(mode, outputColumnNames, groupByKeys, aggregations,
            false, groupByMemoryUsage, memoryThreshold, minReductionHashAggr, minReductionHashAggrLowerBound,
                null, false, -1, numDistinctUDFs > 0),
        new RowSchema(groupByOutputRowResolver.getColumnInfos()), input);
    newOperatorMap.put(op, new OpParseContext(groupByOutputRowResolver));
    op.setColumnExprMap(colExprMap);
    return new Result(op, newOperatorMap);
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
