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

package org.apache.hadoop.hive.ql.optimizer.calcite.translator.opconventer;

import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveValues;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.opconventer.HiveOpConverter.OpAttr;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class HiveValuesVisitor extends HiveRelNodeVisitor<HiveValues> {
  HiveValuesVisitor(HiveOpConverter hiveOpConverter) {
    super(hiveOpConverter);
  }

  @Override
  OpAttr visit(HiveValues valuesRel) throws SemanticException {

    LOG.debug("Translating operator rel#{}:{} with row type: [{}]",
            valuesRel.getId(), valuesRel.getRelTypeName(), valuesRel.getRowType());

    if (!Values.isEmpty(valuesRel)) {
      LOG.error("Empty {} operator translation not supported yet in return path.",
              valuesRel.getClass().getCanonicalName());
      return null;
    }

    // 1. collect columns for project row schema
    List<String> columnNames = new ArrayList<>();
    List<ExprNodeDesc> exprNodeDescList = new ArrayList<>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<>();

    List<ColumnInfo> colInfoList = new ArrayList<>();
    for (int i = 0; i < valuesRel.getRowType().getFieldList().size(); i++) {
      RelDataTypeField typeField = valuesRel.getRowType().getFieldList().get(i);

      ColumnInfo ci = new ColumnInfo(
              typeField.getName(), TypeConverter.convert(typeField.getType()), SemanticAnalyzer.DUMMY_TABLE, false);
      colInfoList.add(ci);
      columnNames.add(typeField.getName());

      ExprNodeDesc exprNodeDesc = new ExprNodeConstantDesc(TypeConverter.convert(typeField.getType()), null);
      colExprMap.put(typeField.getName(), exprNodeDesc);
      exprNodeDescList.add(exprNodeDesc);
    }

    // 2. Create TS on dummy table
    Table metadata = hiveOpConverter.getSemanticAnalyzer().getDummyTable();
    TableScanDesc tsd = new TableScanDesc(SemanticAnalyzer.DUMMY_TABLE, Collections.emptyList(), metadata);

    TableScanOperator ts = (TableScanOperator) OperatorFactory.get(
            hiveOpConverter.getSemanticAnalyzer().getOpContext(), tsd, new RowSchema(Collections.emptyList()));

    hiveOpConverter.getTopOps().put(SemanticAnalyzer.DUMMY_TABLE, ts);

    // 3. Create Select operator
    SelectDesc sd = new SelectDesc(exprNodeDescList, columnNames);
    SelectOperator selOp = (SelectOperator) OperatorFactory.getAndMakeChild(sd, new RowSchema(colInfoList), ts);
    selOp.setColumnExprMap(colExprMap);

    // 4. Create Limit 0 operator
    int limit = 0;
    int offset = 0;
    LimitDesc limitDesc = new LimitDesc(offset, limit);
    Operator<?> resultOp = OperatorFactory.getAndMakeChild(limitDesc, new RowSchema(colInfoList), selOp);

    LOG.debug("Generated {} with row schema: [{}]", resultOp, resultOp.getSchema());

    return new OpAttr(SemanticAnalyzer.DUMMY_TABLE, Collections.emptySet(), selOp).clone(resultOp);
  }
}
