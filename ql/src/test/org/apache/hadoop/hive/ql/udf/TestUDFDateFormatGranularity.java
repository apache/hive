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
package org.apache.hadoop.hive.ql.udf;

import java.time.Instant;
import java.time.ZoneId;

import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritableV2;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

/**
 * TestUDFDateFormatGranularity.
 */
public class TestUDFDateFormatGranularity {

  // Timestamp values are PST (timezone for tests is set to PST by default)

  @Test
  public void testTimestampToTimestampWithGranularity() throws Exception {
    // Running example
    // Friday 30th August 1985 02:47:02 AM
    final TimestampWritableV2 t = new TimestampWritableV2(Timestamp.ofEpochMilli(494243222000L));
    UDFDateFloor g;

    // Year granularity
    // Tuesday 1st January 1985 12:00:00 AM
    g = new UDFDateFloorYear();
    TimestampWritableV2 i1 = g.evaluate(t);
    assertEquals(473385600000L, i1.getTimestamp().toEpochMilli());
    
    // Quarter granularity
    // Monday 1st July 1985 12:00:00 AM
    g = new UDFDateFloorQuarter();
    TimestampWritableV2 i2 = g.evaluate(t);
    assertEquals(489024000000L, i2.getTimestamp().toEpochMilli());

    // Month granularity
    // Thursday 1st August 1985 12:00:00 AM
    g = new UDFDateFloorMonth();
    TimestampWritableV2 i3 = g.evaluate(t);
    assertEquals(491702400000L, i3.getTimestamp().toEpochMilli());

    // Week granularity
    // Monday 26th August 1985 12:00:00 AM
    g = new UDFDateFloorWeek();
    TimestampWritableV2 i4 = g.evaluate(t);
    assertEquals(493862400000L, i4.getTimestamp().toEpochMilli());

    // Day granularity
    // Friday 30th August 1985 12:00:00 AM
    g = new UDFDateFloorDay();
    TimestampWritableV2 i5 = g.evaluate(t);
    assertEquals(494208000000L, i5.getTimestamp().toEpochMilli());

    // Hour granularity
    // Friday 30th August 1985 02:00:00 AM
    g = new UDFDateFloorHour();
    TimestampWritableV2 i6 = g.evaluate(t);
    assertEquals(494240400000L, i6.getTimestamp().toEpochMilli());

    // Minute granularity
    // Friday 30th August 1985 02:47:00 AM
    g = new UDFDateFloorMinute();
    TimestampWritableV2 i7 = g.evaluate(t);
    assertEquals(494243220000L, i7.getTimestamp().toEpochMilli());

    // Second granularity
    // Friday 30th August 1985 02:47:02 AM
    g = new UDFDateFloorSecond();
    TimestampWritableV2 i8 = g.evaluate(t);
    assertEquals(494243222000L, i8.getTimestamp().toEpochMilli());
  }

  @Test
  public void testTimestampWithLocalTZGranularity() throws Exception {
    // Running example
    // Friday 30th August 1985 02:47:02 AM
    final TimestampLocalTZWritable t = new TimestampLocalTZWritable(
        new TimestampTZ(Instant.ofEpochMilli(494243222000L).atZone(ZoneId.of("America/Los_Angeles"))));
    UDFDateFloor g;

    // Year granularity
    // Tuesday 1st January 1985 12:00:00 AM
    g = new UDFDateFloorYear();
    TimestampLocalTZWritable i1 = g.evaluate(t);
    assertEquals(
        new TimestampTZ(Instant.ofEpochMilli(473414400000L).atZone(ZoneId.of("America/Los_Angeles"))),
        i1.getTimestampTZ());

    // Quarter granularity
    // Monday 1st July 1985 12:00:00 AM
    g = new UDFDateFloorQuarter();
    TimestampLocalTZWritable i2 = g.evaluate(t);
    assertEquals(
        new TimestampTZ(Instant.ofEpochMilli(489049200000L).atZone(ZoneId.of("America/Los_Angeles"))),
        i2.getTimestampTZ());

    // Month granularity
    // Thursday 1st August 1985 12:00:00 AM
    g = new UDFDateFloorMonth();
    TimestampLocalTZWritable i3 = g.evaluate(t);
    assertEquals(
        new TimestampTZ(Instant.ofEpochMilli(491727600000L).atZone(ZoneId.of("America/Los_Angeles"))),
        i3.getTimestampTZ());

    // Week granularity
    // Monday 26th August 1985 12:00:00 AM
    g = new UDFDateFloorWeek();
    TimestampLocalTZWritable i4 = g.evaluate(t);
    assertEquals(
        new TimestampTZ(Instant.ofEpochMilli(493887600000L).atZone(ZoneId.of("America/Los_Angeles"))),
        i4.getTimestampTZ());

    // Day granularity
    // Friday 30th August 1985 12:00:00 AM
    g = new UDFDateFloorDay();
    TimestampLocalTZWritable i5 = g.evaluate(t);
    assertEquals(
        new TimestampTZ(Instant.ofEpochMilli(494233200000L).atZone(ZoneId.of("America/Los_Angeles"))),
        i5.getTimestampTZ());

    // Hour granularity
    // Friday 30th August 1985 02:00:00 AM
    g = new UDFDateFloorHour();
    TimestampLocalTZWritable i6 = g.evaluate(t);
    assertEquals(
        new TimestampTZ(Instant.ofEpochMilli(494240400000L).atZone(ZoneId.of("America/Los_Angeles"))),
        i6.getTimestampTZ());

    // Minute granularity
    // Friday 30th August 1985 02:47:00 AM
    g = new UDFDateFloorMinute();
    TimestampLocalTZWritable i7 = g.evaluate(t);
    assertEquals(
        new TimestampTZ(Instant.ofEpochMilli(494243220000L).atZone(ZoneId.of("America/Los_Angeles"))),
        i7.getTimestampTZ());

    // Second granularity
    // Friday 30th August 1985 02:47:02 AM
    g = new UDFDateFloorSecond();
    TimestampLocalTZWritable i8 = g.evaluate(t);
    assertEquals(
        new TimestampTZ(Instant.ofEpochMilli(494243222000L).atZone(ZoneId.of("America/Los_Angeles"))),
        i8.getTimestampTZ());
  }

}
