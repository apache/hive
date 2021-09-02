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
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * TestGenericUDFDatetimeLegacyHybridCalendar.
 */
public class TestGenericUDFDatetimeLegacyHybridCalendar {

  public static void runAndVerify(GenericUDF udf,
      Object arg1, Object expected) throws HiveException {
    DeferredObject[] args = { new DeferredJavaObject(arg1) };
    Object result = udf.evaluate(args);

    if (expected == null) {
      assertNull(result);
    } else {
      assertEquals(expected.toString(), result.toString());
    }
  }

  @Test
  public void testDateLegacyHybridCalendar() throws Exception {
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableDateObjectInspector;
    GenericUDFDatetimeLegacyHybridCalendar udf = new GenericUDFDatetimeLegacyHybridCalendar();
    ObjectInspector[] args2 = {valueOI, valueOI};
    udf.initialize(args2);

    runAndVerify(udf,
        new DateWritableV2(Date.valueOf("0000-12-30")),
        new DateWritableV2(Date.valueOf("0001-01-01")));

    runAndVerify(udf,
        new DateWritableV2(Date.valueOf("0601-03-07")),
        new DateWritableV2(Date.valueOf("0601-03-04")));

    runAndVerify(udf,
        new DateWritableV2(Date.valueOf("1582-10-14")),
        new DateWritableV2(Date.valueOf("1582-10-04")));

    runAndVerify(udf,
        new DateWritableV2(Date.valueOf("1582-10-15")),
        new DateWritableV2(Date.valueOf("1582-10-15")));

    runAndVerify(udf,
        new DateWritableV2(Date.valueOf("2015-03-07")),
        new DateWritableV2(Date.valueOf("2015-03-07")));
  }

  @Test
  public void testDatetimeLegacyHybridCalendar() throws Exception {
    ObjectInspector valueOI = PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
    GenericUDFDatetimeLegacyHybridCalendar udf = new GenericUDFDatetimeLegacyHybridCalendar();
    ObjectInspector[] args2 = {valueOI, valueOI};
    udf.initialize(args2);

    runAndVerify(udf,
        new TimestampWritableV2(Timestamp.valueOf("0601-03-07 17:00:00")),
        new TimestampWritableV2(Timestamp.valueOf("0601-03-04 17:00:00")));

    runAndVerify(udf,
        new TimestampWritableV2(Timestamp.valueOf("1582-10-14 09:17:22.13")),
        new TimestampWritableV2(Timestamp.valueOf("1582-10-04 09:17:22.13")));

    runAndVerify(udf,
        new TimestampWritableV2(Timestamp.valueOf("1582-10-15 11:17:22.13")),
        new TimestampWritableV2(Timestamp.valueOf("1582-10-15 11:17:22.13")));

    runAndVerify(udf,
        new TimestampWritableV2(Timestamp.valueOf("2015-03-07 17:00:00")),
        new TimestampWritableV2(Timestamp.valueOf("2015-03-07 17:00:00")));

    // Make sure nanos are preserved
    runAndVerify(udf,
        new TimestampWritableV2(Timestamp.valueOf("0601-03-07 18:00:00.123456789")),
        new TimestampWritableV2(Timestamp.valueOf("0601-03-04 18:00:00.123456789")));

    runAndVerify(udf,
        new TimestampWritableV2(Timestamp.valueOf("2018-07-07 18:00:00.123456789")),
        new TimestampWritableV2(Timestamp.valueOf("2018-07-07 18:00:00.123456789")));
  }
}
