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

import java.time.ZoneId;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredJavaObject;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF.DeferredObject;
import org.apache.hadoop.hive.serde2.io.DateWritableV2;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import org.apache.hadoop.mapred.JobConf;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * TestGenericUDFToUnixTimestamp.
 */
public class TestGenericUDFToUnixTimestamp {

  public static void runAndVerify(GenericUDFToUnixTimeStamp udf,
      Object arg, Object expected) throws HiveException {
    DeferredObject[] args = { new DeferredJavaObject(arg) };
    Object result = udf.evaluate(args);
    if (expected == null) {
      assertNull(result);
    } else {
      assertEquals(expected.toString(), result.toString());
    }
  }

  public static void runAndVerify(GenericUDFToUnixTimeStamp udf,
      Object arg1, Object arg2, Object expected) throws HiveException {
    DeferredObject[] args = { new DeferredJavaObject(arg1), new DeferredJavaObject(arg2) };
    Object result = udf.evaluate(args);
    if (expected == null) {
      assertNull(result);
    } else {
      assertEquals(expected.toString(), result.toString());
    }
  }

  @Test
  public void testTimestamp() throws HiveException {
    GenericUDFToUnixTimeStamp udf = new GenericUDFToUnixTimeStamp();
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    ObjectInspector[] arguments = {valueOI};
    udf.initialize(arguments);
    Timestamp ts = Timestamp.valueOf("1970-01-01 00:00:00");
    TimestampTZ tstz = TimestampTZUtil.convert(ts, ZoneId.systemDefault());
    runAndVerify(udf,
        new TimestampWritableV2(ts),
        new LongWritable(tstz.getEpochSecond()));

    ts = Timestamp.valueOf("2001-02-03 01:02:03");
    tstz = TimestampTZUtil.convert(ts, ZoneId.systemDefault());
    runAndVerify(udf,
        new TimestampWritableV2(ts),
        new LongWritable(tstz.getEpochSecond()));

    // test null values
    runAndVerify(udf, null, null);
  }

  @Test
  public void testDate() throws HiveException {
    GenericUDFToUnixTimeStamp udf = new GenericUDFToUnixTimeStamp();
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    ObjectInspector[] arguments = {valueOI};
    udf.initialize(arguments);

    Date date = Date.valueOf("1970-01-01");
    TimestampTZ tstz = TimestampTZUtil.convert(date, ZoneId.systemDefault());
    runAndVerify(udf,
        new DateWritableV2(date),
        new LongWritable(tstz.getEpochSecond()));

    // test null values
    runAndVerify(udf, null, null);
  }

  @Test
  public void testStringArg1() throws HiveException {
    GenericUDFToUnixTimeStamp udf1 = new GenericUDFToUnixTimeStamp();
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = {valueOI};
    udf1.initialize(arguments);

    runAndVerify(udf1,
        new Text("2001-01-01 01:02:03"),
        new LongWritable(TimestampTZUtil.parse("2001-01-01 01:02:03", ZoneId.systemDefault()).getEpochSecond()));
    runAndVerify(udf1,
        new Text("1400-11-08 01:53:11"),
        new LongWritable(TimestampTZUtil.parse("1400-11-08 01:53:11", ZoneId.systemDefault()).getEpochSecond()));
    runAndVerify(udf1,
        new Text("1800-11-08 01:53:11"),
        new LongWritable(TimestampTZUtil.parse("1800-11-08 01:53:11", ZoneId.systemDefault()).getEpochSecond()));


    // test invalid values
    runAndVerify(udf1, null, null);
    runAndVerify(udf1, new Text("0000-00-00 00:00:00"), null);
    runAndVerify(udf1, new Text("1400-20-00 00:00:00"), null);
    runAndVerify(udf1, new Text("1400-02-34 00:00:00"), null);
    runAndVerify(udf1, new Text("1800-11-08 01:53:11 UTC"), null);
  }

  @Test
  public void testStringArg2() throws HiveException {

    GenericUDFToUnixTimeStamp udf2 = new GenericUDFToUnixTimeStamp();
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] args2 = {valueOI, valueOI};
    udf2.initialize(args2);

    runAndVerify(udf2,
        new Text("2001.07.04 AD at 12:08:56 PDT"),
        new Text("yyyy.MM.dd G 'at' HH:mm:ss z"),
        new LongWritable(TimestampTZUtil.parse("2001-07-04 12:08:56", ZoneId.systemDefault()).getEpochSecond()));
    runAndVerify(udf2,
        new Text("Wed, Jul 4, '01"),
        new Text("EEE, MMM d, ''yy"),
        new LongWritable(TimestampTZUtil.parse("2001-07-04", ZoneId.systemDefault()).getEpochSecond()));
    runAndVerify(udf2,
        new Text("2009 Mar 20 11:30:01 AM"),
        new Text("yyyy MMM dd h:mm:ss a"),
        new LongWritable(TimestampTZUtil.parse("2009-03-20 11:30:01", ZoneId.systemDefault()).getEpochSecond()));
    runAndVerify(udf2,
        new Text("2009 Mar 20 11:30:01 pm"),
        new Text("yyyy MMM dd h:mm:ss a"),
        new LongWritable(TimestampTZUtil.parse("2009-03-20 23:30:01", ZoneId.systemDefault()).getEpochSecond()));
    runAndVerify(udf2,
        new Text("1800-02-03"),
        new Text("yyyy-MM-dd"),
        new LongWritable(TimestampTZUtil.parse("1800-02-03", ZoneId.systemDefault()).getEpochSecond()));
    runAndVerify(udf2,
        new Text("1400-02-01 00:00:00 ICT"),
        new Text("yyyy-MM-dd HH:mm:ss z"),
        new LongWritable(TimestampTZUtil.parse("1400-01-31 09:00:22", ZoneId.systemDefault()).getEpochSecond()));
    runAndVerify(udf2,
        new Text("1400-02-01 00:00:00 UTC"),
        new Text("yyyy-MM-dd HH:mm:ss z"),
        new LongWritable(TimestampTZUtil.parse("1400-01-31 16:07:02", ZoneId.systemDefault()).getEpochSecond()));
    runAndVerify(udf2,
        new Text("1400-02-01 00:00:00 GMT"),
        new Text("yyyy-MM-dd HH:mm:ss z"),
        new LongWritable(TimestampTZUtil.parse("1400-01-31 16:07:02", ZoneId.systemDefault()).getEpochSecond()));

    // test invalid values
    runAndVerify(udf2, null, null, null);
    runAndVerify(udf2, null, new Text("yyyy-MM-dd"), null);
    runAndVerify(udf2, new Text("2001-01-01"), null, null);
    runAndVerify(udf2, new Text("1400-20-00 00:00:00 ICT"), new Text("yyyy-MM-dd HH:mm:ss z"),
        null);
  }
}
