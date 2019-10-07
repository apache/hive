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
    MapredContext mockContext = Mockito.mock(MapredContext.class);
    when(mockContext.getJobConf()).thenReturn(new JobConf(new HiveConf()));
    udf.configure(mockContext);
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
    MapredContext mockContext = Mockito.mock(MapredContext.class);
    when(mockContext.getJobConf()).thenReturn(new JobConf(new HiveConf()));
    udf.configure(mockContext);
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
  public void testString() throws HiveException {
    GenericUDFToUnixTimeStamp udf1 = new GenericUDFToUnixTimeStamp();
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    ObjectInspector[] arguments = {valueOI};
    MapredContext mockContext = Mockito.mock(MapredContext.class);
    when(mockContext.getJobConf()).thenReturn(new JobConf(new HiveConf()));
    udf1.configure(mockContext);
    udf1.initialize(arguments);

    String val = "2001-01-01 01:02:03";
    runAndVerify(udf1,
        new Text(val),
        new LongWritable(TimestampTZUtil.parse(val, ZoneId.systemDefault()).getEpochSecond()));

    // test null values
    runAndVerify(udf1, null, null);

    // Try 2-arg version
    GenericUDFToUnixTimeStamp udf2 = new GenericUDFToUnixTimeStamp();
    ObjectInspector[] args2 = {valueOI, valueOI};
    udf2.configure(mockContext);
    udf2.initialize(args2);

    val = "2001-01-01";
    String format = "yyyy-MM-dd";
    runAndVerify(udf2,
        new Text(val),
        new Text(format),
        new LongWritable(TimestampTZUtil.parse(val, ZoneId.systemDefault()).getEpochSecond()));

    // test null values
    runAndVerify(udf2, null, null, null);
    runAndVerify(udf2, null, new Text(format), null);
    runAndVerify(udf2, new Text(val), null, null);
  }
}
