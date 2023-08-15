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
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.OrderExpression;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowExpressionSpec;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PTFDesc;

public class WindowingPlanGenerator {

  public static Result genPlan(WindowingSpec wSpec, Operator input,
      ReadOnlySemanticAnalyzer sa,
      Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap
      ) throws SemanticException {
    Map<Operator<? extends OperatorDesc>, OpParseContext> newOperatorMap = new HashMap<>(operatorMap);

    WindowingComponentizer groups = new WindowingComponentizer(wSpec);
    RowResolver rr = operatorMap.get(input).getRowResolver();

    while (groups.hasNext()) {
      wSpec = groups.next(sa.getConf(), sa.getUnparseTranslator(), rr);
      ReduceSinkForWindowingGenerator.Result result =
          ReduceSinkForWindowingGenerator.genPlan(wSpec, rr, input, sa, operatorMap);
      newOperatorMap.putAll(result.getOperatorMap());
      input = result.getOperator();
      rr = newOperatorMap.get(input).getRowResolver();
      PTFTranslator translator = new PTFTranslator();
      PTFDesc ptfDesc = translator.translate(wSpec, sa.getConf(), rr, sa.getUnparseTranslator());
      RowResolver ptfOpRR = ptfDesc.getFuncDef().getOutputShape().getRr();

      input = GenFileSinkPlan.createOperator(ptfDesc,
          new RowSchema(ptfOpRR.getColumnInfos()), input);
      newOperatorMap.put(input, new OpParseContext(ptfOpRR));
      SelectAllDescGenerator.Result sadgResult = SelectAllDescGenerator.genPlan(input, sa, newOperatorMap);
      input = sadgResult.getOperator();
      newOperatorMap.putAll(sadgResult.getOperatorMap());
      rr = ptfOpRR;
    }

    return new Result(input, newOperatorMap);
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
