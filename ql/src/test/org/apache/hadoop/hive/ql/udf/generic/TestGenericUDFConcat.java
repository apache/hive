/**
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

package org.apache.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.testutil.BaseScalarUdfTest;
import org.apache.hadoop.hive.ql.testutil.DataBuilder;
import org.apache.hadoop.hive.ql.testutil.OperatorTestUtils;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class TestGenericUDFConcat extends BaseScalarUdfTest {

  @Override
  public InspectableObject[] getBaseTable() {
    DataBuilder db = new DataBuilder();
    db.setColumnNames("a", "b", "c");
    db.setColumnTypes(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    db.addRow("one", "two", "three");
    db.addRow("four","two", "three");
    db.addRow( null, "two", "three");
    return db.createRows();
  }

  @Override
  public InspectableObject[] getExpectedResult() {
    DataBuilder db = new DataBuilder();
    db.setColumnNames("_col1", "_col2");
    db.setColumnTypes(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    db.addRow("one", "onetwo");
    db.addRow("four", "fourtwo");
    db.addRow(null, null);
    return db.createRows();
  }

  @Override
  public List<ExprNodeDesc> getExpressionList() throws UDFArgumentException {
    ExprNodeDesc expr1 = OperatorTestUtils.getStringColumn("a");
    ExprNodeDesc expr2 = OperatorTestUtils.getStringColumn("b");
    ExprNodeDesc exprDesc2 = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("concat", expr1, expr2);
    List<ExprNodeDesc> earr = new ArrayList<ExprNodeDesc>();
    earr.add(expr1);
    earr.add(exprDesc2);
    return earr;
  }

}
