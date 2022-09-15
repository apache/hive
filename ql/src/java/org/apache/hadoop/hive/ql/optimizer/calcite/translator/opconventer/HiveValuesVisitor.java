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
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveValues;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.opconventer.HiveOpConverter.OpAttr;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.LimitDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class HiveValuesVisitor extends HiveRelNodeVisitor<HiveValues> {
  HiveValuesVisitor(HiveOpConverter hiveOpConverter) {
    super(hiveOpConverter);
  }

  @Override
  OpAttr visit(HiveValues valuesRel) throws SemanticException {

    LOG.debug("Translating operator rel#{}:{} with row type: [{}]",
            valuesRel.getId(), valuesRel.getRelTypeName(), valuesRel.getRowType());
    LOG.debug("Operator rel#{}:{} has {} tuples.",
            valuesRel.getId(), valuesRel.getRelTypeName(), valuesRel.tuples.size());

    if (!Values.isEmpty(valuesRel)) {
      LOG.error("Empty {} operator translation not supported yet in return path.",
              valuesRel.getClass().getCanonicalName());
      return null;
    }

    List<Integer> neededColumnIDs = new ArrayList<>();

    ArrayList<ColumnInfo> colInfoList = new ArrayList<>();
    for (int i = 0; i < valuesRel.getRowType().getFieldList().size(); i++) {
      RelDataTypeField typeField = valuesRel.getRowType().getFieldList().get(i);
      ColumnInfo ci = new ColumnInfo(
              typeField.getName(), TypeConverter.convert(typeField.getType()), SemanticAnalyzer.DUMMY_TABLE, false);
      colInfoList.add(ci);
      neededColumnIDs.add(i);
    }

    Table metadata = hiveOpConverter.getSemanticAnalyzer().getDummyTable();
    TableScanDesc tsd = new TableScanDesc(SemanticAnalyzer.DUMMY_TABLE, Collections.emptyList(), metadata);
//    tsd.setPartColumns(Collections.emptyList());
    tsd.setNeededColumnIDs(neededColumnIDs);
    tsd.setNeededColumns(valuesRel.getRowType().getFieldNames());

    TableScanOperator ts = (TableScanOperator) OperatorFactory.get(
            hiveOpConverter.getSemanticAnalyzer().getOpContext(), tsd, new RowSchema(colInfoList));

    hiveOpConverter.getTopOps().put(SemanticAnalyzer.DUMMY_TABLE, ts);

    int limit = 0;
    int offset = 0;
    LimitDesc limitDesc = new LimitDesc(offset, limit);
//    ArrayList<ColumnInfo> cinfoLst = HiveOpConverterUtils.createColInfos(ts);
    Operator<?> resultOp = OperatorFactory.getAndMakeChild(limitDesc, new RowSchema(colInfoList), ts);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Generated " + resultOp + " with row schema: [" + resultOp.getSchema() + "]");
    }

    // 3. Return result
    return new OpAttr(SemanticAnalyzer.DUMMY_TABLE, Collections.emptySet(), ts).clone(resultOp);
  }
}
