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

package org.apache.hadoop.hive.ql.udf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.testutil.BaseScalarUdfTest;
import org.apache.hadoop.hive.ql.testutil.DataBuilder;
import org.apache.hadoop.hive.ql.testutil.OperatorTestUtils;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

public class TestUDFRound extends BaseScalarUdfTest {

  @Override
  public InspectableObject[] getBaseTable() {
    DataBuilder db = new DataBuilder();
    db.setColumnNames("a", "b", "c");
    db.setColumnTypes(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
        PrimitiveObjectInspectorFactory.javaDoubleObjectInspector);
    db.addRow("one", 1, new Double("1.1"));
    db.addRow( null, null, null);
    db.addRow("two", 2,  new Double("2.1"));
    return db.createRows();
  }

  @Override
  public InspectableObject[] getExpectedResult() {
    DataBuilder db = new DataBuilder();
    db.setColumnNames("_col1", "_col2", "_col3");
    db.setColumnTypes(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.writableIntObjectInspector,
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
    db.addRow(null, new IntWritable(1), new DoubleWritable(1.0));
    db.addRow(null, null, null);
    db.addRow(null, new IntWritable(2), new DoubleWritable(2.0));
    return db.createRows();
  }

  @Override
  public List<ExprNodeDesc> getExpressionList() throws UDFArgumentException {
    ExprNodeDesc expr1 = OperatorTestUtils.getStringColumn("a");
    ExprNodeDesc expr2 = OperatorTestUtils.getStringColumn("b");
    ExprNodeDesc expr3 = OperatorTestUtils.getStringColumn("c");
    ExprNodeDesc r1 = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("round", expr1);
    ExprNodeDesc r2 = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("round", expr2);
    ExprNodeDesc r3 = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("round", expr3);
    List<ExprNodeDesc> earr = new ArrayList<ExprNodeDesc>();
    earr.add(r1);
    earr.add(r2);
    earr.add(r3);
    return earr;
  }

}
