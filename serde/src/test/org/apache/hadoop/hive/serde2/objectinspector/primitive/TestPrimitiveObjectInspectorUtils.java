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

package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.junit.Test;

import junit.framework.TestCase;

public class TestPrimitiveObjectInspectorUtils extends TestCase {

  @Test
  public void testGetPrimitiveGrouping() {
    assertEquals(PrimitiveGrouping.NUMERIC_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.BYTE));
    assertEquals(PrimitiveGrouping.NUMERIC_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.SHORT));
    assertEquals(PrimitiveGrouping.NUMERIC_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.INT));
    assertEquals(PrimitiveGrouping.NUMERIC_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.LONG));
    assertEquals(PrimitiveGrouping.NUMERIC_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.FLOAT));
    assertEquals(PrimitiveGrouping.NUMERIC_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.DOUBLE));
    assertEquals(PrimitiveGrouping.NUMERIC_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.DECIMAL));

    assertEquals(PrimitiveGrouping.STRING_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.STRING));

    assertEquals(PrimitiveGrouping.DATE_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.DATE));
    assertEquals(PrimitiveGrouping.DATE_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.TIMESTAMP));

    assertEquals(PrimitiveGrouping.BOOLEAN_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.BOOLEAN));

    assertEquals(PrimitiveGrouping.BINARY_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.BINARY));

    assertEquals(PrimitiveGrouping.UNKNOWN_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.UNKNOWN));
    assertEquals(PrimitiveGrouping.VOID_GROUP,
        PrimitiveObjectInspectorUtils.getPrimitiveGrouping(PrimitiveCategory.VOID));
  }

  @Test
  public void testgetTimestampWithMillisecondsInt() {
    DateFormat localDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    DateFormat gmtDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    gmtDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

    PrimitiveObjectInspector voidOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.VOID);
    assertEquals(null, PrimitiveObjectInspectorUtils.getTimestamp(new Object(), voidOI));

    PrimitiveObjectInspector booleanOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.BOOLEAN);
    assertEquals("1970-01-01 00:00:00.001", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(true, booleanOI)));
    assertEquals("1970-01-01 00:00:00.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(false, booleanOI)));

    PrimitiveObjectInspector byteOI = PrimitiveObjectInspectorFactory
      .getPrimitiveJavaObjectInspector(PrimitiveCategory.BYTE);
    assertEquals("1970-01-01 00:00:00.001", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((byte)1, byteOI)));
    assertEquals("1969-12-31 23:59:59.999", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((byte)-1, byteOI)));

    PrimitiveObjectInspector shortOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.SHORT);
    assertEquals("1970-01-01 00:00:00.001", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((short)1, shortOI)));
    assertEquals("1969-12-31 23:59:59.999", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((short)-1, shortOI)));

    PrimitiveObjectInspector intOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.INT);
    assertEquals("1970-01-17 11:22:01.282", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((int)1423321282, intOI)));
    assertEquals("1969-12-31 23:59:59.999", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((int)-1, intOI)));

    PrimitiveObjectInspector longOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.LONG);
    assertEquals("1970-01-17 11:22:01.282", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(1423321282L, longOI)));
    assertEquals("1969-12-31 23:59:59.999", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(-1L, longOI)));

      // Float loses some precisions
    PrimitiveObjectInspector floatOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.FLOAT);
    assertEquals("2015-02-07 15:02:24.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(1423321282.123f, floatOI)));
    assertEquals("1969-12-31 23:59:58.876", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(-1.123f, floatOI)));

    PrimitiveObjectInspector doubleOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.DOUBLE);
    assertEquals("2015-02-07 15:01:22.123", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((double)1423321282.123, doubleOI)));
    assertEquals("1969-12-31 23:59:58.877", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((double)-1.123, doubleOI)));

    PrimitiveObjectInspector decimalOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.DECIMAL);
    assertEquals("2015-02-07 15:01:22.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(HiveDecimal.create(1423321282L), decimalOI)));
    assertEquals("1969-12-31 23:59:59.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(HiveDecimal.create(-1), decimalOI)));

    PrimitiveObjectInspector stringOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
    assertEquals("2015-02-07 15:01:22.123", localDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp("2015-02-07 15:01:22.123", stringOI)));

    PrimitiveObjectInspector charOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.CHAR);
    assertEquals("2015-02-07 15:01:22.123", localDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(new HiveChar("2015-02-07 15:01:22.123", 30), charOI)));

    PrimitiveObjectInspector varcharOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.VARCHAR);
    assertEquals("2015-02-07 15:01:22.123", localDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(new HiveVarchar("2015-02-07 15:01:22.123",30), varcharOI)));

    PrimitiveObjectInspector dateOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.DATE);
    assertEquals("2015-02-07 00:00:00.000", localDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(new Date(1423321282123L), dateOI)));

    PrimitiveObjectInspector timestampOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.TIMESTAMP);
    assertEquals("2015-02-07 15:01:22.123", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(new Timestamp(1423321282123L), timestampOI)));
  }

  @Test
  public void testgetTimestampWithSecondsInt() {
    DateFormat localDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    DateFormat gmtDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    gmtDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

    PrimitiveObjectInspector voidOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.VOID);
    assertEquals(null, PrimitiveObjectInspectorUtils.getTimestamp(new Object(), voidOI));

    PrimitiveObjectInspector booleanOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.BOOLEAN);
    assertEquals("1970-01-01 00:00:01.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(true, booleanOI, true)));
    assertEquals("1970-01-01 00:00:00.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(false, booleanOI, true)));

    PrimitiveObjectInspector byteOI = PrimitiveObjectInspectorFactory
      .getPrimitiveJavaObjectInspector(PrimitiveCategory.BYTE);
    assertEquals("1970-01-01 00:00:01.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((byte)1, byteOI, true)));
    assertEquals("1969-12-31 23:59:59.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((byte)-1, byteOI, true)));

    PrimitiveObjectInspector shortOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.SHORT);
    assertEquals("1970-01-01 00:00:01.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((short)1, shortOI, true)));
    assertEquals("1969-12-31 23:59:59.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((short)-1, shortOI, true)));

    PrimitiveObjectInspector intOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.INT);
    assertEquals("2015-02-07 15:01:22.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((int)1423321282, intOI, true)));
    assertEquals("1969-12-31 23:59:59.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((int)-1, intOI, true)));

    PrimitiveObjectInspector longOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.LONG);
    assertEquals("2015-02-07 15:01:22.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(1423321282L, longOI, true)));
    assertEquals("1969-12-31 23:59:59.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(-1L, longOI, true)));

      // Float loses some precisions
    PrimitiveObjectInspector floatOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.FLOAT);
    assertEquals("2015-02-07 15:02:24.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(1423321282.123f, floatOI, true)));
    assertEquals("1969-12-31 23:59:58.876", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(-1.123f, floatOI, true)));

    PrimitiveObjectInspector doubleOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.DOUBLE);
    assertEquals("2015-02-07 15:01:22.123", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((double)1423321282.123, doubleOI, true)));
    assertEquals("1969-12-31 23:59:58.877", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((double)-1.123, doubleOI, true)));

    PrimitiveObjectInspector decimalOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.DECIMAL);
    assertEquals("2015-02-07 15:01:22.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(HiveDecimal.create(1423321282L), decimalOI, true)));
    assertEquals("1969-12-31 23:59:59.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(HiveDecimal.create(-1), decimalOI, true)));

    PrimitiveObjectInspector stringOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
    assertEquals("2015-02-07 15:01:22.123", localDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp("2015-02-07 15:01:22.123", stringOI, true)));

    PrimitiveObjectInspector charOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.CHAR);
    assertEquals("2015-02-07 15:01:22.123", localDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(new HiveChar("2015-02-07 15:01:22.123", 30), charOI, true)));

    PrimitiveObjectInspector varcharOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.VARCHAR);
    assertEquals("2015-02-07 15:01:22.123", localDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(new HiveVarchar("2015-02-07 15:01:22.123",30), varcharOI, true)));

    PrimitiveObjectInspector dateOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.DATE);
    assertEquals("2015-02-07 00:00:00.000", localDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(new Date(1423321282123L), dateOI, true)));

    PrimitiveObjectInspector timestampOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.TIMESTAMP);
    assertEquals("2015-02-07 15:01:22.123", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(new Timestamp(1423321282123L), timestampOI, true)));
  }

  @Test
  public void testGetTimestampFromString() {
    DateFormat localDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    assertEquals("2015-02-07 00:00:00.000", localDateFormat.format(PrimitiveObjectInspectorUtils.getTimestampFromString("2015-02-07")));
  }

  @Test
  public void testGetBoolean() {
    String mustEvaluateToTrue[] = { "yes", "Yes", "ON", "on", "True", "1", "ANYTHING?" };
    String mustEvaluateToFalse[] = { "", "No", "OFF", "FaLsE", "0" };

    for (String falseStr : mustEvaluateToFalse) {
      assertFalse(falseStr, PrimitiveObjectInspectorUtils.getBoolean(falseStr,
          PrimitiveObjectInspectorFactory.javaStringObjectInspector));

      byte[] b1 = ("asd"+falseStr).getBytes();
      assertFalse(falseStr, PrimitiveObjectInspectorUtils.parseBoolean(b1, 3, falseStr.length()));

    }
    for (String trueStr : mustEvaluateToTrue) {
      assertTrue(trueStr, PrimitiveObjectInspectorUtils.getBoolean(trueStr,
          PrimitiveObjectInspectorFactory.javaStringObjectInspector));
      byte[] b1 = ("asd"+trueStr).getBytes();
      assertTrue(trueStr, PrimitiveObjectInspectorUtils.parseBoolean(b1, 3, trueStr.length()));
    }
  }
}
