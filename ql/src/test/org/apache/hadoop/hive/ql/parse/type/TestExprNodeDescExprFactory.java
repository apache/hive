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

package org.apache.hadoop.hive.ql.parse.type;

import junit.framework.TestCase;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.HashMap;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestExprNodeDescExprFactory extends TestCase {

  public void testToExprWhenColumnIsPrimitive() throws SemanticException {
    ExprNodeDescExprFactory exprFactory = new ExprNodeDescExprFactory();
    ColumnInfo columnInfo = new ColumnInfo();
    columnInfo.setTypeName("decimal(3,2)");
    DecimalTypeInfo typeInfo = new DecimalTypeInfo(3, 2);
    columnInfo.setObjectinspector(PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        typeInfo, new HiveDecimalWritable(HiveDecimal.create(6.4))));

    ExprNodeDesc exprNodeDesc = exprFactory.toExpr(columnInfo, null, 0);

    assertThat(exprNodeDesc.getExprString(), is("6.4"));
  }

  public void testToExprWhenColumnIsPrimitiveNullValue() throws SemanticException {
    ExprNodeDescExprFactory exprFactory = new ExprNodeDescExprFactory();
    ColumnInfo columnInfo = new ColumnInfo();
    columnInfo.setTypeName("decimal(3,2)");
    DecimalTypeInfo typeInfo = new DecimalTypeInfo(3, 2);
    columnInfo.setObjectinspector(PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
        typeInfo, null));

    ExprNodeDesc exprNodeDesc = exprFactory.toExpr(columnInfo, null, 0);

    assertThat(exprNodeDesc.getExprString(), is("null"));
  }

  public void testToExprWhenColumnIsList() throws SemanticException {
    ExprNodeDescExprFactory exprFactory = new ExprNodeDescExprFactory();
    ColumnInfo columnInfo = new ColumnInfo();
    columnInfo.setTypeName("array<decimal(3,2)>");
    DecimalTypeInfo typeInfo = new DecimalTypeInfo(3, 2);
    columnInfo.setObjectinspector(ObjectInspectorFactory.getStandardConstantListObjectInspector(
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(typeInfo),
        asList(
            new HiveDecimalWritable(HiveDecimal.create(5d)),
            new HiveDecimalWritable(HiveDecimal.create(0.4)),
            null)));

    ExprNodeDesc exprNodeDesc = exprFactory.toExpr(columnInfo, null, 0);

    assertThat(exprNodeDesc.getExprString(), is("Const array<decimal(3,2)> [5, 0.4, null]"));
  }

  public void testToExprWhenColumnIsListWithNullValue() throws SemanticException {
    ExprNodeDescExprFactory exprFactory = new ExprNodeDescExprFactory();
    ColumnInfo columnInfo = new ColumnInfo();
    columnInfo.setTypeName("array<decimal(3,2)>");
    DecimalTypeInfo typeInfo = new DecimalTypeInfo(3, 2);
    columnInfo.setObjectinspector(ObjectInspectorFactory.getStandardConstantListObjectInspector(
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(typeInfo), null));

    ExprNodeDesc exprNodeDesc = exprFactory.toExpr(columnInfo, null, 0);

    assertThat(exprNodeDesc.getExprString(), is("Const array<decimal(3,2)> null"));
  }

  public void testToExprWhenColumnIsMap() throws SemanticException {
    ExprNodeDescExprFactory exprFactory = new ExprNodeDescExprFactory();
    ColumnInfo columnInfo = new ColumnInfo();
    columnInfo.setTypeName("map<int,string>");
    columnInfo.setObjectinspector(ObjectInspectorFactory.getStandardConstantMapObjectInspector(
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            PrimitiveObjectInspector.PrimitiveCategory.INT),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            PrimitiveObjectInspector.PrimitiveCategory.STRING),
        new HashMap<IntWritable, Text>() {{ put(new IntWritable(4), new Text("foo")); put(null, null); }}));

    ExprNodeDesc exprNodeDesc = exprFactory.toExpr(columnInfo, null, 0);

    assertThat(exprNodeDesc.getExprString(), is("Const map<int,string> {null=null, 4=foo}"));
  }

  public void testToExprWhenColumnIsMapWithNullValue() throws SemanticException {
    ExprNodeDescExprFactory exprFactory = new ExprNodeDescExprFactory();
    ColumnInfo columnInfo = new ColumnInfo();
    columnInfo.setTypeName("map<int,string>");
    columnInfo.setObjectinspector(ObjectInspectorFactory.getStandardConstantMapObjectInspector(
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            PrimitiveObjectInspector.PrimitiveCategory.INT),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(
            PrimitiveObjectInspector.PrimitiveCategory.STRING),
        null));

    ExprNodeDesc exprNodeDesc = exprFactory.toExpr(columnInfo, null, 0);

    assertThat(exprNodeDesc.getExprString(), is("Const map<int,string> null"));
  }

  public void testToExprWhenColumnIsStruct() throws SemanticException {
    ExprNodeDescExprFactory exprFactory = new ExprNodeDescExprFactory();
    ColumnInfo columnInfo = new ColumnInfo();
    columnInfo.setTypeName("struct<f1:int,f2:string>");
    columnInfo.setObjectinspector(ObjectInspectorFactory.getStandardConstantStructObjectInspector(
        asList("f1", "f2"),
        asList(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT),
            PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING)),
        asList(new IntWritable(4), new Text("foo"))));

    ExprNodeDesc exprNodeDesc = exprFactory.toExpr(columnInfo, null, 0);

    assertThat(exprNodeDesc.getExprString(), is("const struct(4,'foo')"));
  }

  public void testToExprWhenColumnIsStructWithNullFields() throws SemanticException {
    ExprNodeDescExprFactory exprFactory = new ExprNodeDescExprFactory();
    ColumnInfo columnInfo = new ColumnInfo();
    columnInfo.setTypeName("struct<f1:int,f2:string>");
    columnInfo.setObjectinspector(ObjectInspectorFactory.getStandardConstantStructObjectInspector(
        asList("f1", "f2"),
        asList(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT),
            PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING)),
        asList(null, null)));

    ExprNodeDesc exprNodeDesc = exprFactory.toExpr(columnInfo, null, 0);

    assertThat(exprNodeDesc.getExprString(), is("const struct(null,null)"));
  }

  public void testToExprWhenColumnIsStructWithNullValue() throws SemanticException {
    ExprNodeDescExprFactory exprFactory = new ExprNodeDescExprFactory();
    ColumnInfo columnInfo = new ColumnInfo();
    columnInfo.setTypeName("struct<f1:int,f2:string>");
    columnInfo.setObjectinspector(ObjectInspectorFactory.getStandardConstantStructObjectInspector(
        asList("f1", "f2"),
        asList(PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.INT),
            PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING)),
        null));

    ExprNodeDesc exprNodeDesc = exprFactory.toExpr(columnInfo, null, 0);

    assertThat(exprNodeDesc.getExprString(), is("null"));
  }

}
