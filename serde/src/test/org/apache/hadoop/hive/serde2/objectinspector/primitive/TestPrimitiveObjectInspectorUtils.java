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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
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
    DateFormat gmtDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    gmtDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

    PrimitiveObjectInspector voidOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.VOID);
    assertEquals(null, PrimitiveObjectInspectorUtils.getTimestamp(new Object(), voidOI));

    PrimitiveObjectInspector booleanOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.BOOLEAN);
    assertEquals("1970-01-01 00:00:00.001", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(true, booleanOI).toSqlTimestamp()));
    assertEquals("1970-01-01 00:00:00.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(false, booleanOI).toSqlTimestamp()));

    PrimitiveObjectInspector byteOI = PrimitiveObjectInspectorFactory
      .getPrimitiveJavaObjectInspector(PrimitiveCategory.BYTE);
    assertEquals("1970-01-01 00:00:00.001", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((byte)1, byteOI).toSqlTimestamp()));
    assertEquals("1969-12-31 23:59:59.999", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((byte)-1, byteOI).toSqlTimestamp()));

    PrimitiveObjectInspector shortOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.SHORT);
    assertEquals("1970-01-01 00:00:00.001", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((short)1, shortOI).toSqlTimestamp()));
    assertEquals("1969-12-31 23:59:59.999", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((short)-1, shortOI).toSqlTimestamp()));

    PrimitiveObjectInspector intOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.INT);
    assertEquals("1970-01-17 11:22:01.282", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((int)1423321282, intOI).toSqlTimestamp()));
    assertEquals("1969-12-31 23:59:59.999", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((int)-1, intOI).toSqlTimestamp()));

    PrimitiveObjectInspector longOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.LONG);
    assertEquals("1970-01-17 11:22:01.282", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(1423321282L, longOI).toSqlTimestamp()));
    assertEquals("1969-12-31 23:59:59.999", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(-1L, longOI).toSqlTimestamp()));

      // Float loses some precisions
    PrimitiveObjectInspector floatOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.FLOAT);
    assertEquals("2015-02-07 15:02:24.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(1423321282.123f, floatOI).toSqlTimestamp()));
    assertEquals("1969-12-31 23:59:58.876", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(-1.123f, floatOI).toSqlTimestamp()));

    PrimitiveObjectInspector doubleOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.DOUBLE);
    assertEquals("2015-02-07 15:01:22.123", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((double)1423321282.123, doubleOI).toSqlTimestamp()));
    assertEquals("1969-12-31 23:59:58.877", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((double)-1.123, doubleOI).toSqlTimestamp()));

    PrimitiveObjectInspector decimalOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.DECIMAL);
    assertEquals("2015-02-07 15:01:22.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(HiveDecimal.create(1423321282L), decimalOI).toSqlTimestamp()));
    assertEquals("1969-12-31 23:59:59.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(HiveDecimal.create(-1), decimalOI).toSqlTimestamp()));

    PrimitiveObjectInspector stringOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
    assertEquals("2015-02-07 15:01:22.123", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp("2015-02-07 15:01:22.123", stringOI).toSqlTimestamp()));

    PrimitiveObjectInspector charOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.CHAR);
    assertEquals("2015-02-07 15:01:22.123", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(new HiveChar("2015-02-07 15:01:22.123", 30), charOI).toSqlTimestamp()));

    PrimitiveObjectInspector varcharOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.VARCHAR);
    assertEquals("2015-02-07 15:01:22.123", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(new HiveVarchar("2015-02-07 15:01:22.123",30), varcharOI).toSqlTimestamp()));

    PrimitiveObjectInspector dateOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.DATE);
    assertEquals("2015-02-07 00:00:00.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(Date.ofEpochMilli(1423321282123L), dateOI).toSqlTimestamp()));

    PrimitiveObjectInspector timestampOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.TIMESTAMP);
    assertEquals("2015-02-07 15:01:22.123", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(Timestamp.ofEpochMilli(1423321282123L), timestampOI).toSqlTimestamp()));
  }

  @Test
  public void testgetTimestampWithSecondsInt() {
    DateFormat gmtDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    gmtDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

    PrimitiveObjectInspector voidOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.VOID);
    assertEquals(null, PrimitiveObjectInspectorUtils.getTimestamp(new Object(), voidOI));

    PrimitiveObjectInspector booleanOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.BOOLEAN);
    assertEquals("1970-01-01 00:00:01.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(true, booleanOI, true).toSqlTimestamp()));
    assertEquals("1970-01-01 00:00:00.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(false, booleanOI, true).toSqlTimestamp()));

    PrimitiveObjectInspector byteOI = PrimitiveObjectInspectorFactory
      .getPrimitiveJavaObjectInspector(PrimitiveCategory.BYTE);
    assertEquals("1970-01-01 00:00:01.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((byte)1, byteOI, true).toSqlTimestamp()));
    assertEquals("1969-12-31 23:59:59.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((byte)-1, byteOI, true).toSqlTimestamp()));

    PrimitiveObjectInspector shortOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.SHORT);
    assertEquals("1970-01-01 00:00:01.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((short)1, shortOI, true).toSqlTimestamp()));
    assertEquals("1969-12-31 23:59:59.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((short)-1, shortOI, true).toSqlTimestamp()));

    PrimitiveObjectInspector intOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.INT);
    assertEquals("2015-02-07 15:01:22.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((int)1423321282, intOI, true).toSqlTimestamp()));
    assertEquals("1969-12-31 23:59:59.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((int)-1, intOI, true).toSqlTimestamp()));

    PrimitiveObjectInspector longOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.LONG);
    assertEquals("2015-02-07 15:01:22.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(1423321282L, longOI, true).toSqlTimestamp()));
    assertEquals("1969-12-31 23:59:59.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(-1L, longOI, true).toSqlTimestamp()));

      // Float loses some precisions
    PrimitiveObjectInspector floatOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.FLOAT);
    assertEquals("2015-02-07 15:02:24.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(1423321282.123f, floatOI, true).toSqlTimestamp()));
    assertEquals("1969-12-31 23:59:58.876", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(-1.123f, floatOI, true).toSqlTimestamp()));

    PrimitiveObjectInspector doubleOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.DOUBLE);
    assertEquals("2015-02-07 15:01:22.123", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((double)1423321282.123, doubleOI, true).toSqlTimestamp()));
    assertEquals("1969-12-31 23:59:58.877", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp((double)-1.123, doubleOI, true).toSqlTimestamp()));

    PrimitiveObjectInspector decimalOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.DECIMAL);
    assertEquals("2015-02-07 15:01:22.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(HiveDecimal.create(1423321282L), decimalOI, true).toSqlTimestamp()));
    assertEquals("1969-12-31 23:59:59.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(HiveDecimal.create(-1), decimalOI, true).toSqlTimestamp()));

    PrimitiveObjectInspector stringOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.STRING);
    assertEquals("2015-02-07 15:01:22.123", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp("2015-02-07 15:01:22.123", stringOI, true).toSqlTimestamp()));

    PrimitiveObjectInspector charOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.CHAR);
    assertEquals("2015-02-07 15:01:22.123", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(new HiveChar("2015-02-07 15:01:22.123", 30), charOI, true).toSqlTimestamp()));

    PrimitiveObjectInspector varcharOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.VARCHAR);
    assertEquals("2015-02-07 15:01:22.123", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(new HiveVarchar("2015-02-07 15:01:22.123",30), varcharOI, true).toSqlTimestamp()));

    PrimitiveObjectInspector dateOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.DATE);
    assertEquals("2015-02-07 00:00:00.000", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(Date.ofEpochMilli(1423321282123L), dateOI, true).toSqlTimestamp()));

    PrimitiveObjectInspector timestampOI = PrimitiveObjectInspectorFactory
        .getPrimitiveJavaObjectInspector(PrimitiveCategory.TIMESTAMP);
    assertEquals("2015-02-07 15:01:22.123", gmtDateFormat.format(PrimitiveObjectInspectorUtils.getTimestamp(Timestamp.ofEpochMilli(1423321282123L), timestampOI, true).toSqlTimestamp()));
  }

  @Test
  public void testGetTimestampFromString() {
    DateFormat udfDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    udfDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
    assertEquals("2015-02-07 00:00:00.000", udfDateFormat.format(
        PrimitiveObjectInspectorUtils.getTimestampFromString("2015-02-07").toSqlTimestamp()));
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
