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

package org.apache.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.parse.TypeCheckProcFactory;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.testutil.BaseScalarUdfTest;
import org.apache.hadoop.hive.ql.testutil.DataBuilder;
import org.apache.hadoop.hive.ql.testutil.OperatorTestUtils;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.InspectableObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.junit.Test;

public class TestGenericUDFRound extends BaseScalarUdfTest {
  private static final String[] cols = {"s", "i", "d", "f", "b", "sh", "l", "dec"};

  @Override
  public InspectableObject[] getBaseTable() {
    DataBuilder db = new DataBuilder();
    db.setColumnNames(cols);
    db.setColumnTypes(
        PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.javaIntObjectInspector,
        PrimitiveObjectInspectorFactory.javaDoubleObjectInspector,
        PrimitiveObjectInspectorFactory.javaFloatObjectInspector,
        PrimitiveObjectInspectorFactory.javaByteObjectInspector,
        PrimitiveObjectInspectorFactory.javaShortObjectInspector,
        PrimitiveObjectInspectorFactory.javaLongObjectInspector,
        PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(TypeInfoFactory.getDecimalTypeInfo(15, 5)));
    db.addRow("one", 170, Double.valueOf("1.1"), Float.valueOf("32.1234"), Byte.valueOf("25"), Short.valueOf("1234"),
        123456L, HiveDecimal.create("983.7235"));
    db.addRow("-234", null, null, Float.valueOf("0.347232"), Byte.valueOf("109"), Short.valueOf("551"), 923L,
        HiveDecimal.create("983723.005"));
    db.addRow("454.45", 22345, Double.valueOf("-23.00009"), Float.valueOf("-3.4"), Byte.valueOf("76"),
        Short.valueOf("2321"), 9232L, HiveDecimal.create("-932032.7"));
    return db.createRows();
  }

  @Override
  public InspectableObject[] getExpectedResult() {
    DataBuilder db = new DataBuilder();
    db.setColumnNames("_col1", "_col2", "_col3", "_col4", "_col5", "_col6", "_col7", "_col8");
    db.setColumnTypes(PrimitiveObjectInspectorFactory.javaStringObjectInspector,
        PrimitiveObjectInspectorFactory.writableIntObjectInspector,
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
        PrimitiveObjectInspectorFactory.writableByteObjectInspector,
        PrimitiveObjectInspectorFactory.writableShortObjectInspector,
        PrimitiveObjectInspectorFactory.writableLongObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector);
    db.addRow(null, new IntWritable(170), new DoubleWritable(1.1), new FloatWritable(32f),
        new ByteWritable((byte)0), new ShortWritable((short)1234), new LongWritable(123500L), new HiveDecimalWritable(HiveDecimal.create("983.724")));
    db.addRow(new DoubleWritable(-200), null, null, new FloatWritable(0f),
        new ByteWritable((byte)100), new ShortWritable((short)551), new LongWritable(900L), new HiveDecimalWritable(HiveDecimal.create("983723.005")));
    db.addRow(new DoubleWritable(500), new IntWritable(22345),  new DoubleWritable(-23.000), new FloatWritable(-3f),
        new ByteWritable((byte)100), new ShortWritable((short)2321), new LongWritable(9200L), new HiveDecimalWritable(HiveDecimal.create("-932032.7")));
    return db.createRows();
  }

  @Override
  public List<ExprNodeDesc> getExpressionList() throws UDFArgumentException {
    List<ExprNodeDesc> exprs = new ArrayList<ExprNodeDesc>(cols.length);
    for (int i = 0; i < cols.length; i++) {
      exprs.add(OperatorTestUtils.getStringColumn(cols[i]));
    }

    ExprNodeDesc[] scales = { new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, -2),
        new ExprNodeConstantDesc(TypeInfoFactory.byteTypeInfo, (byte)0),
        new ExprNodeConstantDesc(TypeInfoFactory.shortTypeInfo, (short)3),
        new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, 0),
        new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, -2L),
        new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, 0),
        new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, -2),
        new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, 3) };

    List<ExprNodeDesc> earr = new ArrayList<ExprNodeDesc>();
    for (int j = 0; j < cols.length; j++) {
      ExprNodeDesc r = TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDesc("round", exprs.get(j), scales[j]);
      earr.add(r);
    }

    return earr;
  }

  @Test
  public void testDecimalRoundingMetaData() throws UDFArgumentException {
    GenericUDFRound udf = new GenericUDFRound();
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(7, 3)),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, new IntWritable(2))
    };
    PrimitiveObjectInspector outputOI = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    DecimalTypeInfo outputTypeInfo = (DecimalTypeInfo)outputOI.getTypeInfo();
    Assert.assertEquals(TypeInfoFactory.getDecimalTypeInfo(7, 2), outputTypeInfo);
  }

  @Test
  public void testDecimalRoundingMetaData1() throws UDFArgumentException {
    GenericUDFRound udf = new GenericUDFRound();
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(7, 3)),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, new IntWritable(-2))
    };
    PrimitiveObjectInspector outputOI = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    DecimalTypeInfo outputTypeInfo = (DecimalTypeInfo)outputOI.getTypeInfo();
    Assert.assertEquals(TypeInfoFactory.getDecimalTypeInfo(5, 0), outputTypeInfo);
  }

  @Test
  public void testDecimalRoundingMetaData2() throws UDFArgumentException {
    GenericUDFRound udf = new GenericUDFRound();
    ObjectInspector[] inputOIs = {
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(TypeInfoFactory.getDecimalTypeInfo(7, 3)),
        PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.intTypeInfo, new IntWritable(5))
    };
    PrimitiveObjectInspector outputOI = (PrimitiveObjectInspector) udf.initialize(inputOIs);
    DecimalTypeInfo outputTypeInfo = (DecimalTypeInfo)outputOI.getTypeInfo();
    Assert.assertEquals(TypeInfoFactory.getDecimalTypeInfo(9, 5), outputTypeInfo);
  }

}
