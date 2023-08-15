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
import org.apache.hadoop.hive.ql.io.AcidUtils.Operation;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.OrderExpression;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;

public class SelectAllDescGenerator {
  public static Result genPlan(Operator input,
      ReadOnlySemanticAnalyzer sa,
      Map<Operator<? extends OperatorDesc>, OpParseContext> operatorMap) {
    OpParseContext inputCtx = operatorMap.get(input);
    RowResolver inputRR = inputCtx.getRowResolver();
    List<ColumnInfo> columns = inputRR.getColumnInfos();
    List<ExprNodeDesc> colList = new ArrayList<ExprNodeDesc>();
    List<String> columnNames = new ArrayList<String>();
    Map<String, ExprNodeDesc> columnExprMap = new HashMap<String, ExprNodeDesc>();
    for (ColumnInfo col : columns) {
      colList.add(new ExprNodeColumnDesc(col, true));
      columnNames.add(col.getInternalName());
      columnExprMap.put(col.getInternalName(), new ExprNodeColumnDesc(col, true));
    }
    RowResolver outputRR = inputRR.duplicate();

    Operator output = GenFileSinkPlan.createOperator(
        new SelectDesc(colList, columnNames, true),
        outputRR.getRowSchema(), input);
    output.setColumnExprMap(columnExprMap);
    Map<Operator<? extends OperatorDesc>, OpParseContext> newOperatorMap = new HashMap<>();
    newOperatorMap.put(output, new OpParseContext(outputRR));

    return new Result(output, newOperatorMap);
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
