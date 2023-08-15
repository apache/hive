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

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.OrderExpression;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

public class ReduceSinkForWindowingGenerator {
  
  public static Result genPlan(WindowingSpec spec,
      RowResolver inputRR, Operator input,
      ReadOnlySemanticAnalyzer sa,
      Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap
      ) throws SemanticException{

    List<ExprNodeDesc> partCols = new ArrayList<ExprNodeDesc>();
    List<ExprNodeDesc> orderCols = new ArrayList<ExprNodeDesc>();
    StringBuilder order = new StringBuilder();
    StringBuilder nullOrder = new StringBuilder();

    for (PartitionExpression partCol : spec.getQueryPartitionSpec().getExpressions()) {
      ExprNodeDesc partExpr = SemanticAnalyzer.genExprNodeDesc(partCol.getExpression(), inputRR,
          true, false, sa.getUnparseTranslator(), sa.getConf());
      if (ExprNodeDescUtils.indexOf(partExpr, partCols) < 0) {
        partCols.add(partExpr);
        orderCols.add(partExpr);
        order.append('+');
        nullOrder.append('a');
      }
    }

    if (spec.getQueryOrderSpec() != null) {
      for (OrderExpression orderCol : spec.getQueryOrderSpec().getExpressions()) {
        ExprNodeDesc orderExpr = SemanticAnalyzer.genExprNodeDesc(orderCol.getExpression(), inputRR,
          true, false, sa.getUnparseTranslator(), sa.getConf());
        char orderChar = orderCol.getOrder() == PTFInvocationSpec.Order.ASC ? '+' : '-';
        char nullOrderChar = orderCol.getNullOrder() == PTFInvocationSpec.NullOrder.NULLS_FIRST ? 'a' : 'z';
        int index = ExprNodeDescUtils.indexOf(orderExpr, orderCols);
        if (index >= 0) {
          order.setCharAt(index, orderChar);
          nullOrder.setCharAt(index, nullOrderChar);
          continue;
        }
        orderCols.add(SemanticAnalyzer.genExprNodeDesc(orderCol.getExpression(), inputRR,
            true, false, sa.getUnparseTranslator(), sa.getConf()));
        order.append(orderChar);
        nullOrder.append(nullOrderChar);
      }
    }


    GenReduceSinkPlan genReduceSinkPlan = new GenReduceSinkPlan(input, partCols, orderCols,
        order.toString(), nullOrder.toString(), -1, Operation.NOT_ACID, false, sa,
        operatorMap);
    Result result = new Result(genReduceSinkPlan.getOperator(), genReduceSinkPlan.getOperatorMap());
    return result;
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

