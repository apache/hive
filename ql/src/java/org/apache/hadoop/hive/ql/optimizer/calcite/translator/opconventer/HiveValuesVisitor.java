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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
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

/**
 * Converts empty {@link HiveValues} to a plan like
 * TS [DUMMY] - SEL [null, ..., null] - LIM [0]
 */
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

    Operator<?> ts = createDummyScan();
    hiveOpConverter.getTopOps().put(SemanticAnalyzer.DUMMY_TABLE, (TableScanOperator) ts);
    Operator<?> selOp = createSelect(valuesRel.getRowType(), ts);
    Operator<?> resultOp = createLimit0(selOp.getSchema().getSignature(), selOp);

    LOG.debug("Generated {} with row schema: [{}]", resultOp, resultOp.getSchema());

    return new OpAttr(SemanticAnalyzer.DUMMY_TABLE, Collections.emptySet(), selOp).clone(resultOp);
  }

  private Operator<?> createDummyScan() throws SemanticException {
    Table metadata = hiveOpConverter.getSemanticAnalyzer().getDummyTable();
    TableScanDesc tsd = new TableScanDesc(SemanticAnalyzer.DUMMY_TABLE, Collections.emptyList(), metadata);

    return OperatorFactory.get(
            hiveOpConverter.getSemanticAnalyzer().getOpContext(), tsd, new RowSchema(Collections.emptyList()));
  }

  private static Operator<?> createSelect(RelDataType relDataType, Operator<?> parentOp) {
    List<String> columnNames = new ArrayList<>();
    List<ExprNodeDesc> exprNodeDescList = new ArrayList<>();
    Map<String, ExprNodeDesc> colExprMap = new HashMap<>();
    List<ColumnInfo> colInfoList = new ArrayList<>();

    for (int i = 0; i < relDataType.getFieldList().size(); i++) {
      RelDataTypeField typeField = relDataType.getFieldList().get(i);

      ColumnInfo ci = new ColumnInfo(
              typeField.getName(), TypeConverter.convert(typeField.getType()), SemanticAnalyzer.DUMMY_TABLE, false);
      colInfoList.add(ci);
      columnNames.add(typeField.getName());

      ExprNodeDesc exprNodeDesc = new ExprNodeConstantDesc(TypeConverter.convert(typeField.getType()), null);
      colExprMap.put(typeField.getName(), exprNodeDesc);
      exprNodeDescList.add(exprNodeDesc);
    }

    SelectDesc sd = new SelectDesc(exprNodeDescList, columnNames);
    Operator<?> selOp = OperatorFactory.getAndMakeChild(sd, new RowSchema(colInfoList), parentOp);
    selOp.setColumnExprMap(colExprMap);
    return selOp;
  }

  private static Operator<?> createLimit0(List<ColumnInfo> colInfoList, Operator<?> parentOp) {
    LimitDesc limitDesc = new LimitDesc(0, 0);
    return OperatorFactory.getAndMakeChild(limitDesc, new RowSchema(colInfoList), parentOp);
  }
}
