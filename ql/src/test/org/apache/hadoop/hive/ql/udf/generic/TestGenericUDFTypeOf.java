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

import com.google.common.collect.ImmutableMap;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestGenericUDFTypeOf {
  private final DeferredObject[] evaluateArguments = { new DeferredJavaObject(new Text("Test")) };

  private Text executeUDF(ObjectInspector initArg) throws HiveException  {
    GenericUDFTypeOf udf = new GenericUDFTypeOf();
    ObjectInspector[] arguments = { initArg };
    udf.initialize(arguments);
    return (Text) udf.evaluate(evaluateArguments);
  }

  @Test
  public void testPrimitiveTypes() throws HiveException {
    Map<ObjectInspector, String> oiToExpectedOutput = new ImmutableMap.Builder<ObjectInspector, String>()
        .put(PrimitiveObjectInspectorFactory.writableBinaryObjectInspector, "binary")
        .put(PrimitiveObjectInspectorFactory.writableBooleanObjectInspector, "boolean")
        .put(PrimitiveObjectInspectorFactory.writableByteObjectInspector, "tinyint")
        .put(PrimitiveObjectInspectorFactory.writableDateObjectInspector, "date")
        .put(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector, "double")
        .put(PrimitiveObjectInspectorFactory.writableFloatObjectInspector, "float")
        .put(PrimitiveObjectInspectorFactory.writableHiveCharObjectInspector, "char(255)")
        .put(PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector, "decimal(38,18)")
        .put(PrimitiveObjectInspectorFactory.writableHiveIntervalDayTimeObjectInspector, "interval_day_time")
        .put(PrimitiveObjectInspectorFactory.writableHiveIntervalYearMonthObjectInspector, "interval_year_month")
        .put(PrimitiveObjectInspectorFactory.writableHiveVarcharObjectInspector, "varchar(65535)")
        .put(PrimitiveObjectInspectorFactory.writableIntObjectInspector, "int")
        .put(PrimitiveObjectInspectorFactory.writableLongObjectInspector, "bigint")
        .put(PrimitiveObjectInspectorFactory.writableShortObjectInspector, "smallint")
        .put(PrimitiveObjectInspectorFactory.writableStringObjectInspector, "string")
        .put(PrimitiveObjectInspectorFactory.writableTimestampObjectInspector, "timestamp")
        .put(PrimitiveObjectInspectorFactory.writableTimestampTZObjectInspector, "timestamp with local time zone")
        .put(PrimitiveObjectInspectorFactory.writableVoidObjectInspector, "void")
        .build();

    for (Map.Entry<ObjectInspector, String> testCase: oiToExpectedOutput.entrySet()) {
      Text retValue = executeUDF(testCase.getKey());
      assertEquals(testCase.getValue(), retValue.toString());
    }
  }

  @Test
  public void testListType() throws HiveException {
    ObjectInspector oi = ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.writableStringObjectInspector
    );
    Text retValue = executeUDF(oi);
    assertEquals("array<string>", retValue.toString());
  }

  @Test
  public void testStructType() throws HiveException {
    List<String> structFieldNames = Arrays.asList(
        "str",
        "long",
        "strArray"
    );
    List<ObjectInspector> structOIs = Arrays.asList(
        PrimitiveObjectInspectorFactory.writableStringObjectInspector,
        PrimitiveObjectInspectorFactory.writableLongObjectInspector,
        ObjectInspectorFactory.getStandardListObjectInspector(
            PrimitiveObjectInspectorFactory.writableStringObjectInspector
        ));

    ObjectInspector oi = ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structOIs);
    Text retValue = executeUDF(oi);
    assertEquals("struct<str:string,long:bigint,strarray:array<string>>", retValue.toString());
  }

  @Test
  public void testMapType() throws HiveException {
    ObjectInspector keyOI = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    ObjectInspector valueOI  = ObjectInspectorFactory.getStandardListObjectInspector(
        PrimitiveObjectInspectorFactory.writableStringObjectInspector
    );
    ObjectInspector oi = ObjectInspectorFactory.getStandardMapObjectInspector(keyOI, valueOI);
    Text retValue = executeUDF(oi);
    assertEquals("map<date,array<string>>", retValue.toString());
  }

  @Test(expected = UDFArgumentLengthException.class)
  public void testNotEnoughArguments() throws HiveException {
    GenericUDFTypeOf udf = new GenericUDFTypeOf();
    ObjectInspector[] arguments = { };
    udf.initialize(arguments);
  }

  @Test(expected = UDFArgumentLengthException.class)
  public void testTooManyArguments() throws HiveException {
    GenericUDFTypeOf udf = new GenericUDFTypeOf();
    ObjectInspector[] arguments = {
        PrimitiveObjectInspectorFactory.writableDateObjectInspector,
        PrimitiveObjectInspectorFactory.writableDateObjectInspector
    };
    udf.initialize(arguments);
  }
}
