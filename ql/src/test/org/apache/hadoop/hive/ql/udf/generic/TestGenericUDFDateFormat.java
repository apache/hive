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

import junit.framework.TestCase;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;

public class TestGenericUDFDateFormat extends TestCase {

  public void testDateFormatStr() throws HiveException {
    GenericUDFDateFormat udf = new GenericUDFDateFormat();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    Text fmtText = new Text("EEEE");
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, fmtText);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    // date str
    runAndVerifyStr("2015-04-05", fmtText, "Sunday", udf);
    runAndVerifyStr("2015-04-06", fmtText, "Monday", udf);
    runAndVerifyStr("2015-04-07", fmtText, "Tuesday", udf);
    runAndVerifyStr("2015-04-08", fmtText, "Wednesday", udf);
    runAndVerifyStr("2015-04-09", fmtText, "Thursday", udf);
    runAndVerifyStr("2015-04-10", fmtText, "Friday", udf);
    runAndVerifyStr("2015-04-11", fmtText, "Saturday", udf);
    runAndVerifyStr("2015-04-12", fmtText, "Sunday", udf);

    // ts str
    runAndVerifyStr("2015-04-05 10:30:45", fmtText, "Sunday", udf);
    runAndVerifyStr("2015-04-06 10:30:45", fmtText, "Monday", udf);
    runAndVerifyStr("2015-04-07 10:30:45", fmtText, "Tuesday", udf);
    runAndVerifyStr("2015-04-08 10:30:45", fmtText, "Wednesday", udf);
    runAndVerifyStr("2015-04-09 10:30", fmtText, "Thursday", udf);
    runAndVerifyStr("2015-04-10 10:30:45.123", fmtText, "Friday", udf);
    runAndVerifyStr("2015-04-11T10:30:45", fmtText, "Saturday", udf);
    runAndVerifyStr("2015-04-12 10", fmtText, "Sunday", udf);
  }

  public void testWrongDateStr() throws HiveException {
    GenericUDFDateFormat udf = new GenericUDFDateFormat();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    Text fmtText = new Text("EEEE");
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, fmtText);
    ObjectInspector[] arguments = {valueOI0, valueOI1};

    udf.initialize(arguments);
    runAndVerifyStr("2016-02-30 10:30:45", fmtText, "Tuesday", udf);
    runAndVerifyStr("2014-01-32", fmtText, "Saturday", udf);
    runAndVerifyStr("01/14/2014", fmtText, null, udf);
    runAndVerifyStr(null, fmtText, null, udf);
  }

  public void testDateFormatDate() throws HiveException {
    GenericUDFDateFormat udf = new GenericUDFDateFormat();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    Text fmtText = new Text("EEEE");
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, fmtText);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyDate("2015-04-05", fmtText, "Sunday", udf);
    runAndVerifyDate("2015-04-06", fmtText, "Monday", udf);
    runAndVerifyDate("2015-04-07", fmtText, "Tuesday", udf);
    runAndVerifyDate("2015-04-08", fmtText, "Wednesday", udf);
    runAndVerifyDate("2015-04-09", fmtText, "Thursday", udf);
    runAndVerifyDate("2015-04-10", fmtText, "Friday", udf);
    runAndVerifyDate("2015-04-11", fmtText, "Saturday", udf);
    runAndVerifyDate("2015-04-12", fmtText, "Sunday", udf);
  }

  public void testDateFormatTs() throws HiveException {
    GenericUDFDateFormat udf = new GenericUDFDateFormat();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    Text fmtText = new Text("EEEE");
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, fmtText);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyTs("2015-04-08 10:30:45", fmtText, "Wednesday", udf);
    runAndVerifyTs("2015-04-05 10:30:45", fmtText, "Sunday", udf);
    runAndVerifyTs("2015-04-06 10:30:45", fmtText, "Monday", udf);
    runAndVerifyTs("2015-04-07 10:30:45", fmtText, "Tuesday", udf);
    runAndVerifyTs("2015-04-08 10:30:45", fmtText, "Wednesday", udf);
    runAndVerifyTs("2015-04-09 10:30:45", fmtText, "Thursday", udf);
    runAndVerifyTs("2015-04-10 10:30:45.123", fmtText, "Friday", udf);
    runAndVerifyTs("2015-04-11 10:30:45.123456789", fmtText, "Saturday", udf);
    runAndVerifyTs("2015-04-12 10:30:45", fmtText, "Sunday", udf);
  }

  public void testNullFmt() throws HiveException {
    GenericUDFDateFormat udf = new GenericUDFDateFormat();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    Text fmtText = null;
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, fmtText);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyStr("2015-04-05", fmtText, null, udf);
  }

  public void testWrongFmt() throws HiveException {
    GenericUDFDateFormat udf = new GenericUDFDateFormat();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    Text fmtText = new Text("Q");
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.stringTypeInfo, fmtText);
    ObjectInspector[] arguments = { valueOI0, valueOI1 };

    udf.initialize(arguments);

    runAndVerifyStr("2015-04-05", fmtText, null, udf);
  }

  private void runAndVerifyStr(String str, Text fmtText, String expResult, GenericUDF udf)
      throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(str != null ? new Text(str) : null);
    DeferredObject valueObj1 = new DeferredJavaObject(fmtText);
    DeferredObject[] args = { valueObj0, valueObj1 };
    Text output = (Text) udf.evaluate(args);
    assertEquals("date_format() test ", expResult, output != null ? output.toString() : null);
  }

  private void runAndVerifyDate(String str, Text fmtText, String expResult, GenericUDF udf)
      throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(str != null ? new DateWritableV2(
        Date.valueOf(str)) : null);
    DeferredObject valueObj1 = new DeferredJavaObject(fmtText);
    DeferredObject[] args = { valueObj0, valueObj1 };
    Text output = (Text) udf.evaluate(args);
    assertEquals("date_format() test ", expResult, output != null ? output.toString() : null);
  }

  private void runAndVerifyTs(String str, Text fmtText, String expResult, GenericUDF udf)
      throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(str != null ? new TimestampWritableV2(
        Timestamp.valueOf(str)) : null);
    DeferredObject valueObj1 = new DeferredJavaObject(fmtText);
    DeferredObject[] args = { valueObj0, valueObj1 };
    Text output = (Text) udf.evaluate(args);
    assertEquals("date_format() test ", expResult, output != null ? output.toString() : null);
  }
}
