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
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import junit.framework.TestCase;

public class TestGenericUDFMonthsBetween extends TestCase {

  public void testMonthsBetweenForString() throws HiveException {
    // Default run
    GenericUDFMonthsBetween udf = new GenericUDFMonthsBetween();
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = { valueOI1, valueOI2 };
    udf.initialize(arguments);

    testMonthsBetweenForString(udf);

    // Run without round-off
    GenericUDFMonthsBetween udfWithoutRoundOff = new GenericUDFMonthsBetween();
    ObjectInspector vOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector vOI2 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector vOI3 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.booleanTypeInfo,
            new BooleanWritable(false));
    ObjectInspector[] args = { vOI1, vOI2, vOI3 };
    udfWithoutRoundOff.initialize(args);

    testMonthsBetweenForString(udf);
  }

  public void testWrongDateStr() throws HiveException {
    GenericUDFMonthsBetween udf = new GenericUDFMonthsBetween();
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = {valueOI1, valueOI2};
    udf.initialize(arguments);

    runTestStr("2002-03", "2002-02-24", null, udf);
    runTestStr("2002-03-24", "2002-02", null, udf);
  }

  public void testMonthsBetweenForString(GenericUDFMonthsBetween udf) throws HiveException {
    // test month diff with fraction considering time components
    runTestStr("1995-02-02", "1995-01-01", 1.03225806, udf);
    runTestStr("2003-07-17", "2005-07-06", -23.64516129, udf);
    // test the last day of month
    runTestStr("2001-06-30", "2000-05-31", 13.0, udf);
    // test the same day of month
    runTestStr("2000-06-01", "2004-07-01", -49.0, udf);
    // test February of non-leap year, 2/28
    runTestStr("2002-02-28", "2002-03-01", -0.12903226, udf);
    // test February of non-leap year, 2/31 is viewd as 3/3 due to 3 days diff
    // from 2/31 to 2/28
    runTestStr("2002-02-31", "2002-03-01", 0.06451613, udf);

    // test Feb of leap year, 2/29
    runTestStr("2012-02-29", "2012-03-01", -0.09677419, udf);
    // test february of leap year, 2/31 is viewed as 3/2 due to 2 days diff from
    // 2/31 to 2/29
    runTestStr("2012-02-31", "2012-03-01", 0.03225806, udf);

    // time part
    // test that there is no lead second adjustment
    runTestStr("1976-01-01 00:00:00", "1975-12-31 23:59:59", 0.00000037, udf);
    // test UDF considers the difference in time components date1 and date2
    runTestStr("1997-02-28 10:30:00", "1996-10-30", 3.94959677, udf);
    runTestStr("1996-10-30", "1997-02-28 10:30:00", -3.94959677, udf);

    // if both are last day of the month then time part should be ignored
    runTestStr("2002-03-31", "2002-02-28", 1.0, udf);
    runTestStr("2002-03-31", "2002-02-28 10:30:00", 1.0, udf);
    runTestStr("2002-03-31 10:30:00", "2002-02-28", 1.0, udf);
    // if the same day of the month then time part should be ignored
    runTestStr("2002-03-24", "2002-02-24", 1.0, udf);
    runTestStr("2002-03-24", "2002-02-24 10:30:00", 1.0, udf);
    runTestStr("2002-03-24 10:30:00", "2002-02-24", 1.0, udf);

    // partial time. time part will be skipped
    runTestStr("1995-02-02 10:39", "1995-01-01", 1.03225806, udf);
    runTestStr("1995-02-02", "1995-01-01 10:39", 1.03225806, udf);
    // no leading 0 for month and day should work
    runTestStr("1995-02-2", "1995-1-01", 1.03225806, udf);
    runTestStr("1995-2-02", "1995-01-1", 1.03225806, udf);
    // short year should work
    runTestStr("495-2-02", "495-01-1", 1.03225806, udf);
    runTestStr("95-2-02", "95-01-1", 1.03225806, udf);
    runTestStr("5-2-02", "5-01-1", 1.03225806, udf);

    // Test with null args
    runTestStr(null, "2002-03-01", null, udf);
    runTestStr("2002-02-28", null, null, udf);
    runTestStr(null, null, null, udf);

    runTestStr("2003-04-23", "2002-04-24", 11.96774194, udf);
  }



  public void testMonthsBetweenForTimestamp() throws HiveException {
    GenericUDFMonthsBetween udf = new GenericUDFMonthsBetween();
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    ObjectInspector[] arguments = { valueOI1, valueOI2 };
    udf.initialize(arguments);

    testMonthsBetweenForTimestamp(udf);

    // Run without round-off
    GenericUDFMonthsBetween udfWithoutRoundOff = new GenericUDFMonthsBetween();
    ObjectInspector vOI1 = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    ObjectInspector vOI2 = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    ObjectInspector vOI3 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.booleanTypeInfo,
            new BooleanWritable(false));
    ObjectInspector[] args = { vOI1, vOI2, vOI3 };
    udfWithoutRoundOff.initialize(args);

    testMonthsBetweenForTimestamp(udfWithoutRoundOff);
  }

  public void testMonthsBetweenForTimestamp(GenericUDFMonthsBetween udf) throws HiveException {
    // test month diff with fraction considering time components
    runTestTs("1995-02-02 00:00:00", "1995-01-01 00:00:00", 1.03225806, udf);
    runTestTs("2003-07-17 00:00:00", "2005-07-06 00:00:00", -23.64516129, udf);
    // test the last day of month
    runTestTs("2001-06-30 00:00:00", "2000-05-31 00:00:00", 13.0, udf);
    // test the same day of month
    runTestTs("2000-06-01 00:00:00", "2004-07-01 00:00:00", -49.0, udf);
    // test February of non-leap year, 2/28
    runTestTs("2002-02-28 00:00:00", "2002-03-01 00:00:00", -0.12903226, udf);
    // test February of non-leap year, 2/31 is viewd as 3/3 due to 3 days diff
    // from 2/31 to 2/28

    // test Feb of leap year, 2/29
    runTestTs("2012-02-29 00:00:00", "2012-03-01 00:00:00", -0.09677419, udf);

    // time part
    // test that there is no lead second adjustment
    runTestTs("1976-01-01 00:00:00", "1975-12-31 23:59:59", 0.00000037, udf);
    // test UDF considers the difference in time components date1 and date2
    runTestTs("1997-02-28 10:30:00", "1996-10-30 00:00:00", 3.94959677, udf);
    runTestTs("1996-10-30 00:00:00", "1997-02-28 10:30:00", -3.94959677, udf);

    // if both are last day of the month then time part should be ignored
    runTestTs("2002-03-31 00:00:00", "2002-02-28 00:00:00", 1.0, udf);
    runTestTs("2002-03-31 00:00:00", "2002-02-28 10:30:00", 1.0, udf);
    runTestTs("2002-03-31 10:30:00", "2002-02-28 00:00:00", 1.0, udf);
    // if the same day of the month then time part should be ignored
    runTestTs("2002-03-24 00:00:00", "2002-02-24 00:00:00", 1.0, udf);
    runTestTs("2002-03-24 00:00:00", "2002-02-24 10:30:00", 1.0, udf);
    runTestTs("2002-03-24 10:30:00", "2002-02-24 00:00:00", 1.0, udf);

    runTestTs("2003-04-23 23:59:59", "2003-03-24 00:00:00", 0.99999963, udf);
  }

  public void testMonthsBetweenForDate() throws HiveException {
    GenericUDFMonthsBetween udf = new GenericUDFMonthsBetween();
    ObjectInspector valueOI1 = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    ObjectInspector valueOI2 = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    ObjectInspector[] arguments = { valueOI1, valueOI2 };
    udf.initialize(arguments);

    testMonthsBetweenForDate(udf);

    // Run without round-off
    GenericUDFMonthsBetween udfWithoutRoundOff = new GenericUDFMonthsBetween();
    ObjectInspector vOI1 = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    ObjectInspector vOI2 = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    ObjectInspector vOI3 = PrimitiveObjectInspectorFactory
        .getPrimitiveWritableConstantObjectInspector(TypeInfoFactory.booleanTypeInfo,
            new BooleanWritable(false));
    ObjectInspector[] args = { vOI1, vOI2, vOI3 };
    udfWithoutRoundOff.initialize(args);

    testMonthsBetweenForDate(udfWithoutRoundOff);
  }

  public void testMonthsBetweenForDate(GenericUDFMonthsBetween udf) throws HiveException {
    // test month diff with fraction considering time components
    runTestDt("1995-02-02", "1995-01-01", 1.03225806, udf);
    runTestDt("2003-07-17", "2005-07-06", -23.64516129, udf);
    // test the last day of month
    runTestDt("2001-06-30", "2000-05-31", 13.0, udf);
    // test the same day of month
    runTestDt("2000-06-01", "2004-07-01", -49.0, udf);
    // test February of non-leap year, 2/28
    runTestDt("2002-02-28", "2002-03-01", -0.12903226, udf);
    // test February of non-leap year, 2/31 is viewd as 3/3 due to 3 days diff
    // from 2/31 to 2/28
    runTestDt("2002-02-31", "2002-03-01", 0.06451613, udf);

    // test Feb of leap year, 2/29
    runTestDt("2012-02-29", "2012-03-01", -0.09677419, udf);
    // test february of leap year, 2/31 is viewed as 3/2 due to 2 days diff from
    // 2/31 to 2/29
    runTestDt("2012-02-31", "2012-03-01", 0.03225806, udf);
    // Test with null args
    runTestDt(null, "2002-03-01", null, udf);
    runTestDt("2002-02-28", null, null, udf);
    runTestDt(null, null, null, udf);
  }

  protected void runTestStr(String date1, String date2, Double expDiff, GenericUDFMonthsBetween udf)
      throws HiveException {
    DeferredJavaObject valueObj1 = new DeferredJavaObject(date1 == null ? null : new Text(date1));
    DeferredJavaObject valueObj2 = new DeferredJavaObject(date2 == null ? null : new Text(date2));
    DeferredObject[] args = new DeferredObject[] { valueObj1, valueObj2 };
    DoubleWritable output = (DoubleWritable) udf.evaluate(args);
    if (expDiff == null) {
      assertNull("months_between() test for NULL STRING failed", output);
    } else {
      assertNotNull("months_between() test for NOT NULL STRING failed", output);
      assertEquals("months_between() test for STRING failed", expDiff, output.get(), 0.00000001D);
    }
  }

  protected void runTestTs(String ts1, String ts2, Double expDiff, GenericUDFMonthsBetween udf)
      throws HiveException {
    TimestampWritableV2 tsWr1 = ts1 == null ? null : new TimestampWritableV2(Timestamp.valueOf(ts1));
    TimestampWritableV2 tsWr2 = ts2 == null ? null : new TimestampWritableV2(Timestamp.valueOf(ts2));
    DeferredJavaObject valueObj1 = new DeferredJavaObject(tsWr1);
    DeferredJavaObject valueObj2 = new DeferredJavaObject(tsWr2);
    DeferredObject[] args = new DeferredObject[] { valueObj1, valueObj2 };
    DoubleWritable output = (DoubleWritable) udf.evaluate(args);
    if (expDiff == null) {
      assertNull("months_between() test for NULL TIMESTAMP failed", output);
    } else {
      assertNotNull("months_between() test for NOT NULL TIMESTAMP failed", output);
      assertEquals("months_between() test for TIMESTAMP failed", expDiff, output.get(), 0.00000001D);
    }
  }

  protected void runTestDt(String dt1, String dt2, Double expDiff, GenericUDFMonthsBetween udf)
      throws HiveException {
    DateWritableV2 dtWr1 = dt1 == null ? null : new DateWritableV2(Date.valueOf(dt1));
    DateWritableV2 dtWr2 = dt2 == null ? null : new DateWritableV2(Date.valueOf(dt2));
    DeferredJavaObject valueObj1 = new DeferredJavaObject(dtWr1);
    DeferredJavaObject valueObj2 = new DeferredJavaObject(dtWr2);
    DeferredObject[] args = new DeferredObject[] { valueObj1, valueObj2 };
    DoubleWritable output = (DoubleWritable) udf.evaluate(args);
    if (expDiff == null) {
      assertNull("months_between() test for NULL DATE failed", output);
    } else {
      assertNotNull("months_between() test for NOT NULL DATE failed", output);
      assertEquals("months_between() test for DATE failed", expDiff, output.get(), 0.00000001D);
    }
  }
}
