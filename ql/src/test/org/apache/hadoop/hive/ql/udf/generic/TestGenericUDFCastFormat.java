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

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.HiveParser_IdentifiersParser;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNull;
import static org.junit.Assert.fail;

/**
 * Tests CAST (<TIMESTAMP/DATE> AS STRING/CHAR/VARCHAR FORMAT <STRING>) and
 * CAST (<STRING/CHAR/VARCHAR> AS TIMESTAMP/DATE FORMAT <STRING>).
 */
public class TestGenericUDFCastFormat {

  //type codes
  public static final int CHAR = HiveParser_IdentifiersParser.TOK_CHAR;
  public static final int VARCHAR = HiveParser_IdentifiersParser.TOK_VARCHAR;
  public static final int STRING = HiveParser_IdentifiersParser.TOK_STRING;
  public static final int DATE = HiveParser_IdentifiersParser.TOK_DATE;
  public static final int TIMESTAMP = HiveParser_IdentifiersParser.TOK_TIMESTAMP;

  @Test
  public void testDateToStringWithFormat() throws HiveException {
    ObjectInspector inputOI = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    testCast(STRING, inputOI, date("2009-07-30"), "yyyy-MM-dd", "2009-07-30");
    testCast(STRING, inputOI, date("2009-07-30"), "yyyy", "2009");
    testCast(STRING, inputOI, date("1969-07-30"), "dd", "30");

    testCast(CHAR, 3, inputOI, date("2009-07-30"), "yyyy-MM-dd", "200");
    testCast(CHAR, 3, inputOI, date("2009-07-30"), "yyyy", "200");
    testCast(CHAR, 3, inputOI, date("1969-07-30"), "dd", "30 ");

    testCast(VARCHAR, 3, inputOI, date("2009-07-30"), "yyyy-MM-dd", "200");
    testCast(VARCHAR, 3, inputOI, date("2009-07-30"), "yyyy", "200");
    testCast(VARCHAR, 3, inputOI, date("1969-07-30"), "dd", "30");
  }

  @Test public void testTimestampToStringTypesWithFormat() throws HiveException {
    ObjectInspector inputOI = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    testCast(STRING, inputOI, timestamp("2009-07-30 00:00:08"),
        "yyyy-MM-dd HH24:mi:ss", "2009-07-30 00:00:08");
    testCast(STRING, inputOI, timestamp("2009-07-30 11:02:00"),
        "MM/dd/yyyy hh24miss", "07/30/2009 110200");
    testCast(STRING, inputOI, timestamp("2009-07-30 01:02:03"), "MM", "07");
    testCast(STRING, inputOI, timestamp("1969-07-30 00:00:00"), "yy", "69");

    testCast(CHAR, 3, inputOI, timestamp("2009-07-30 00:00:08"),
        "yyyy-MM-dd HH24:mi:ss", "200");
    testCast(CHAR, 3, inputOI, timestamp("2009-07-30 11:02:00"),
        "MM/dd/yyyy hh24miss", "07/");
    testCast(CHAR, 3, inputOI, timestamp("2009-07-30 01:02:03"), "MM", "07 ");
    testCast(CHAR, 3, inputOI, timestamp("1969-07-30 00:00:00"), "yy", "69 ");

    testCast(VARCHAR, 3, inputOI, timestamp("2009-07-30 00:00:08"),
        "yyyy-MM-dd HH24:mi:ss", "200");
    testCast(VARCHAR, 3, inputOI, timestamp("2009-07-30 11:02:00"),
        "MM/dd/yyyy hh24miss", "07/");
    testCast(VARCHAR, 3, inputOI, timestamp("2009-07-30 01:02:03"), "MM", "07");
    testCast(VARCHAR, 3, inputOI, timestamp("1969-07-30 00:00:00"), "yy", "69");
  }

  @Test public void testStringTypesToDateWithFormat() throws HiveException {
    ObjectInspector inputOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    testCast(DATE, inputOI, "1969-07-30 13:00", "yyyy-MM-dd hh24:mi", "1969-07-30");
    testCast(DATE, inputOI, "307-2009", "ddmm-yyyy", "2009-07-30");
    testCast(DATE, inputOI, "307-2009", "ddd-yyyy", "2009-11-03");

    inputOI = PrimitiveObjectInspectorFactory.javaHiveCharObjectInspector;
    testCast(DATE, inputOI, new HiveChar("1969-07-30 13:00", 15), "yyyy-MM-dd hh24:mi",
        "1969-07-30");
    testCast(DATE, inputOI, new HiveChar("307-2009", 7), "ddmm-yyyy", "2200-07-30");
    testCast(DATE, inputOI, new HiveChar("307-2009", 7), "ddd-yyyy", "2200-11-03");

    inputOI = PrimitiveObjectInspectorFactory.javaHiveVarcharObjectInspector;
    testCast(DATE, inputOI, new HiveVarchar("1969-07-30 13:00", 15), "yyyy-MM-dd hh24:mi",
        "1969-07-30");
    testCast(DATE, inputOI, new HiveVarchar("307-2009", 7), "ddmm-yyyy", "2200-07-30");
    testCast(DATE, inputOI, new HiveVarchar("307-2009", 7), "ddd-yyyy", "2200-11-03");
  }

  @Test public void testStringTypesToTimestampWithFormat() throws HiveException {
    ObjectInspector inputOI = PrimitiveObjectInspectorFactory.javaStringObjectInspector;
    testCast(TIMESTAMP, inputOI, "2009-07-30 01:02:03", "yyyy-MM-dd HH24:mi:ss",
        "2009-07-30 01:02:03");
    testCast(TIMESTAMP, inputOI, "07/30/2009 11:0200", "MM/dd/yyyy hh24:miss",
        "2009-07-30 11:02:00");
    testCast(TIMESTAMP, inputOI, "969.07.30.", "yyy.MM.dd.", "2969-07-30 00:00:00");

    inputOI = PrimitiveObjectInspectorFactory.javaHiveCharObjectInspector;
    testCast(TIMESTAMP, 13, inputOI, new HiveChar("2009-07-30 01:02:03", 13), "yyyy-MM-dd HH24",
        "2009-07-30 01:00:00");
    testCast(TIMESTAMP, 18, inputOI, new HiveChar("07/30/2009 11:0200", 18), "MM/dd/yyyy hh24:miss",
        "2009-07-30 11:02:00");
    testCast(TIMESTAMP, 10, inputOI, new HiveChar("969.07.30.12:00", 10), "yyy.MM.dd.",
        "2969-07-30 00:00:00");

    inputOI = PrimitiveObjectInspectorFactory.javaHiveVarcharObjectInspector;
    testCast(TIMESTAMP, 13, inputOI, new HiveVarchar("2009-07-30 01:02:03", 13), "yyyy-MM-dd HH24",
        "2009-07-30 01:00:00");
    testCast(TIMESTAMP, 18, inputOI, new HiveVarchar("07/30/2009 11:0200", 18),
        "MM/dd/yyyy hh24:miss", "2009-07-30 11:02:00");
    testCast(TIMESTAMP, 10, inputOI, new HiveVarchar("969.07.30.12:00", 10), "yyy.MM.dd.",
        "2969-07-30 00:00:00");
  }

  private TimestampWritableV2 timestamp(String s) {
    return new TimestampWritableV2(Timestamp.valueOf(s));
  }

  private DateWritableV2 date(String s) {
    return new DateWritableV2(Date.valueOf(s));
  }

  private void testCast(int typeCode, ObjectInspector inputOI, Object input, String format,
      String expOutput) throws HiveException {
    testCast(typeCode, 0, inputOI, input, format, expOutput);
  }

  private void testCast(int typeCode, int length, ObjectInspector inputOI, Object input, String format,
      String expOutput)
      throws HiveException {
    // initialize
    GenericUDFCastFormat udf = new GenericUDFCastFormat();
    ConstantObjectInspector typeCodeOI =
        PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
            TypeInfoFactory.getPrimitiveTypeInfo("int"), new IntWritable(typeCode));
    ConstantObjectInspector formatOI =
        PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
            TypeInfoFactory.getPrimitiveTypeInfo("string"), new Text(format));
    ConstantObjectInspector lengthOI =
        PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
            TypeInfoFactory.getPrimitiveTypeInfo("int"), new IntWritable(length));
    ObjectInspector[] initArgs = {typeCodeOI, inputOI, formatOI, lengthOI};
    udf.initialize(initArgs);

    // evaluate
    GenericUDF.DeferredObject typeCodeObj = new GenericUDF.DeferredJavaObject(typeCode);
    GenericUDF.DeferredObject inputObj = new GenericUDF.DeferredJavaObject(input);
    GenericUDF.DeferredObject formatObj = new GenericUDF.DeferredJavaObject(new Text(format));
    GenericUDF.DeferredObject lengthObj = new GenericUDF.DeferredJavaObject(length);
    GenericUDF.DeferredObject[] evalArgs = {typeCodeObj, inputObj, formatObj, lengthObj};
    Object output = udf.evaluate(evalArgs);
    if (output == null) {
      fail(
          "Cast " + inputOI.getTypeName() + " \"" + input + "\" to " + GenericUDFCastFormat.OUTPUT_TYPES
              .get(typeCode) + " failed, output null");
    }
    assertEquals(
        "Cast " + inputOI.getTypeName() + " \"" + input + "\" to " + GenericUDFCastFormat.OUTPUT_TYPES.get(typeCode)
            + " failed ", expOutput, output.toString());

    // Try with null input
    GenericUDF.DeferredObject[] nullArgs =
        {typeCodeObj, new GenericUDF.DeferredJavaObject(null), formatObj, lengthObj};
    assertNull(udf.getFuncName() + " with NULL arguments failed", udf.evaluate(nullArgs));
  }
}
