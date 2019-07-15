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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * TestGenericUDFQuarter.
 */
public class TestGenericUDFQuarter {

  @Test
  public void testQuarterStr() throws HiveException {
    GenericUDFQuarter udf = new GenericUDFQuarter();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = { valueOI0 };

    udf.initialize(arguments);

    // date str
    runAndVerifyStr("2014-01-10", 1, udf);
    runAndVerifyStr("2014-02-10", 1, udf);
    runAndVerifyStr("2014-03-31", 1, udf);
    runAndVerifyStr("2014-04-02", 2, udf);
    runAndVerifyStr("2014-05-28", 2, udf);
    runAndVerifyStr("2016-06-03", 2, udf);
    runAndVerifyStr("2016-07-28", 3, udf);
    runAndVerifyStr("2016-08-29", 3, udf);
    runAndVerifyStr("2016-09-29", 3, udf);
    runAndVerifyStr("2016-10-29", 4, udf);
    runAndVerifyStr("2016-11-29", 4, udf);
    runAndVerifyStr("2016-12-29", 4, udf);

    // negative Unix time
    runAndVerifyStr("1966-01-01", 1, udf);
    runAndVerifyStr("1966-03-31", 1, udf);
    runAndVerifyStr("1966-04-01", 2, udf);
    runAndVerifyStr("1966-12-31", 4, udf);

    // ts str
    runAndVerifyStr("2014-01-01 00:00:00", 1, udf);
    runAndVerifyStr("2014-02-10 15:23:00", 1, udf);
    runAndVerifyStr("2014-03-31 15:23:00", 1, udf);
    runAndVerifyStr("2014-04-02 15:23:00", 2, udf);
    runAndVerifyStr("2014-05-28 15:23:00", 2, udf);
    runAndVerifyStr("2016-06-03 15:23:00", 2, udf);
    runAndVerifyStr("2016-07-28 15:23:00", 3, udf);
    runAndVerifyStr("2016-08-29 15:23:00", 3, udf);
    runAndVerifyStr("2016-09-29 15:23:00", 3, udf);
    runAndVerifyStr("2016-10-29 15:23:00", 4, udf);
    runAndVerifyStr("2016-11-29 15:23:00", 4, udf);
    runAndVerifyStr("2016-12-31 23:59:59.999", 4, udf);

    // negative Unix time
    runAndVerifyStr("1966-01-01 00:00:00", 1, udf);
    runAndVerifyStr("1966-03-31 23:59:59.999", 1, udf);
    runAndVerifyStr("1966-04-01 00:00:00", 2, udf);
    runAndVerifyStr("1966-12-31 23:59:59.999", 4, udf);
  }

  @Test
  public void testWrongDateStr() throws HiveException {
    GenericUDFQuarter udf = new GenericUDFQuarter();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = {valueOI0};

    udf.initialize(arguments);

    runAndVerifyStr("2016-03-35", 2, udf);
    runAndVerifyStr("2014-01-32", 1, udf);
    runAndVerifyStr("01/14/2014", null, udf);
    runAndVerifyStr(null, null, udf);
  }

  @Test
  public void testQuarterDt() throws HiveException {
    GenericUDFQuarter udf = new GenericUDFQuarter();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    ObjectInspector[] arguments = { valueOI0 };

    udf.initialize(arguments);
    // positive Unix time
    runAndVerifyDt("2014-01-01", 1, udf);
    runAndVerifyDt("2014-02-10", 1, udf);
    runAndVerifyDt("2014-03-31", 1, udf);
    runAndVerifyDt("2014-04-02", 2, udf);
    runAndVerifyDt("2014-05-28", 2, udf);
    runAndVerifyDt("2016-06-03", 2, udf);
    runAndVerifyDt("2016-07-28", 3, udf);
    runAndVerifyDt("2016-08-29", 3, udf);
    runAndVerifyDt("2016-09-29", 3, udf);
    runAndVerifyDt("2016-10-29", 4, udf);
    runAndVerifyDt("2016-11-29", 4, udf);
    runAndVerifyDt("2016-12-31", 4, udf);
    // negative Unix time
    runAndVerifyDt("1966-01-01", 1, udf);
    runAndVerifyDt("1966-03-31", 1, udf);
    runAndVerifyDt("1966-04-01", 2, udf);
    runAndVerifyDt("1966-12-31", 4, udf);
  }

  @Test
  public void testQuarterTs() throws HiveException {
    GenericUDFQuarter udf = new GenericUDFQuarter();
    ObjectInspector valueOI0 = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    ObjectInspector[] arguments = { valueOI0 };

    udf.initialize(arguments);
    // positive Unix time
    runAndVerifyTs("2014-01-01 00:00:00", 1, udf);
    runAndVerifyTs("2014-02-10 15:23:00", 1, udf);
    runAndVerifyTs("2014-03-31 15:23:00", 1, udf);
    runAndVerifyTs("2014-04-02 15:23:00", 2, udf);
    runAndVerifyTs("2014-05-28 15:23:00", 2, udf);
    runAndVerifyTs("2016-06-03 15:23:00", 2, udf);
    runAndVerifyTs("2016-07-28 15:23:00", 3, udf);
    runAndVerifyTs("2016-08-29 15:23:00", 3, udf);
    runAndVerifyTs("2016-09-29 15:23:00", 3, udf);
    runAndVerifyTs("2016-10-29 15:23:00", 4, udf);
    runAndVerifyTs("2016-11-29 15:23:00", 4, udf);
    runAndVerifyTs("2016-12-31 23:59:59.999", 4, udf);
    // negative Unix time
    runAndVerifyTs("1966-01-01 00:00:00", 1, udf);
    runAndVerifyTs("1966-03-31 23:59:59", 1, udf);
    runAndVerifyTs("1966-04-01 00:00:00", 2, udf);
    runAndVerifyTs("1966-12-31 23:59:59.999", 4, udf);
  }

  private void runAndVerifyStr(String str, Integer expResult, GenericUDF udf) throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(str != null ? new Text(str) : null);
    DeferredObject[] args = { valueObj0 };
    IntWritable output = (IntWritable) udf.evaluate(args);
    if (expResult == null) {
      assertNull(output);
    } else {
      assertNotNull(output);
      assertEquals("quarter() test ", expResult.intValue(), output.get());
    }
  }

  private void runAndVerifyDt(String str, Integer expResult, GenericUDF udf) throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(str != null ? new DateWritableV2(
        Date.valueOf(str)) : null);
    DeferredObject[] args = { valueObj0 };
    IntWritable output = (IntWritable) udf.evaluate(args);
    if (expResult == null) {
      assertNull(output);
    } else {
      assertNotNull(output);
      assertEquals("quarter() test ", expResult.intValue(), output.get());
    }
  }

  private void runAndVerifyTs(String str, Integer expResult, GenericUDF udf) throws HiveException {
    DeferredObject valueObj0 = new DeferredJavaObject(str != null ? new TimestampWritableV2(
        Timestamp.valueOf(str)) : null);
    DeferredObject[] args = { valueObj0 };
    IntWritable output = (IntWritable) udf.evaluate(args);
    if (expResult == null) {
      assertNull(output);
    } else {
      assertNotNull(output);
      assertEquals("quarter() test ", expResult.intValue(), output.get());
    }
  }
}
