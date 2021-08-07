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
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * TestGenericUDFFromUnixTime.
 */
public class TestGenericUDFFromUnixTime {

  public static void runAndVerify(GenericUDFFromUnixTime udf,
      Object arg, Object expected) throws HiveException {
    DeferredObject[] args = { new DeferredJavaObject(arg) };
    Object result = udf.evaluate(args);
    if (expected == null) {
      assertNull(result);
    } else {
      assertEquals(expected.toString(), result.toString());
    }
  }

  public static void runAndVerify(GenericUDFFromUnixTime udf,
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
  public void testTimestampDefaultTimezone() throws HiveException {
    ObjectInspector valueLongOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    GenericUDFFromUnixTime udf = new GenericUDFFromUnixTime();
    ObjectInspector args[] = {valueLongOI};
    udf.initialize(args);

    Timestamp ts = Timestamp.valueOf("1470-01-01 00:00:00");
    TimestampTZ tstz = TimestampTZUtil.convert(ts, ZoneId.systemDefault());

    runAndVerify(udf,
            new LongWritable(tstz.getEpochSecond()), new Text("1470-01-01 00:00:00"));

    // test null values
    runAndVerify(udf, null, null);
  }

  @Test
  public void testTimestampOtherTimezone() throws HiveException {
    ObjectInspector valueLongOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    GenericUDFFromUnixTime udf = new GenericUDFFromUnixTime();
    ObjectInspector args[] = {valueLongOI};
    udf.initialize(args);

    Timestamp ts = Timestamp.valueOf("1969-12-31 15:59:46");
    TimestampTZ tstz1 = TimestampTZUtil.convert(ts, ZoneId.of("America/Los_Angeles"));
    TimestampTZ tstz2 = TimestampTZUtil.convert(ts, ZoneId.of("America/New_York"));
    TimestampTZ tstz3 = TimestampTZUtil.convert(ts, ZoneId.of("Europe/London"));
    TimestampTZ tstz4 = TimestampTZUtil.convert(ts, ZoneId.of("Europe/Rome"));

    runAndVerify(udf,
            new LongWritable(tstz1.getEpochSecond()), new Text("1969-12-31 15:59:46"));
    runAndVerify(udf,
            new LongWritable(tstz2.getEpochSecond()), new Text("1969-12-31 12:59:46"));
    runAndVerify(udf,
            new LongWritable(tstz3.getEpochSecond()), new Text("1969-12-31 06:59:46"));
    runAndVerify(udf,
            new LongWritable(tstz4.getEpochSecond()), new Text("1969-12-31 06:59:46"));
  }

  @Test
  public void testTimestampWithArg2() throws HiveException {
    ObjectInspector valueLongOI = PrimitiveObjectInspectorFactory.writableLongObjectInspector;
    ObjectInspector valueStringOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    GenericUDFFromUnixTime udf = new GenericUDFFromUnixTime();
    ObjectInspector args[] = {valueLongOI, valueStringOI};
    udf.initialize(args);

    Timestamp ts = Timestamp.valueOf("2010-01-13 11:57:40");
    TimestampTZ tstz1 = TimestampTZUtil.convert(ts, ZoneId.systemDefault());

    runAndVerify(udf,
            new LongWritable(tstz1.getEpochSecond()), "MM/dd/yy HH:mm:ss", new Text("01/13/10 11:57:40"));
    runAndVerify(udf,
            new LongWritable(tstz1.getEpochSecond()), "EEEE", new Text("Wednesday"));
    runAndVerify(udf,
            new LongWritable(tstz1.getEpochSecond()), "yyyy-MM-dd'T'HH:mm:ssXXX", new Text("2010-01-13T11:57:40-08:00"));
    runAndVerify(udf,
            new LongWritable(tstz1.getEpochSecond()), "uuuu-MM-dd'T'HH:mm:ssXXX", new Text("2010-01-13T11:57:40-08:00"));
  }
}

