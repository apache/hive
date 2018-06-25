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

import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.serde2.io.TimestampLocalTZWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.junit.Test;

import junit.framework.TestCase;

public class TestUDFDateFormatGranularity extends TestCase {

  // Timestamp values are PST (timezone for tests is set to PST by default)

  @Test
  public void testTimestampToTimestampWithGranularity() throws Exception {
    // Running example
    // Friday 30th August 1985 02:47:02 AM
    final TimestampWritable t = new TimestampWritable(new Timestamp(494243222000L));
    UDFDateFloor g;

    // Year granularity
    // Tuesday 1st January 1985 12:00:00 AM
    g = new UDFDateFloorYear();
    TimestampWritable i1 = g.evaluate(t);
    assertEquals(473414400000L, i1.getTimestamp().getTime());
    
    // Quarter granularity
    // Monday 1st July 1985 12:00:00 AM
    g = new UDFDateFloorQuarter();
    TimestampWritable i2 = g.evaluate(t);
    assertEquals(489049200000L, i2.getTimestamp().getTime());

    // Month granularity
    // Thursday 1st August 1985 12:00:00 AM
    g = new UDFDateFloorMonth();
    TimestampWritable i3 = g.evaluate(t);
    assertEquals(491727600000L, i3.getTimestamp().getTime());

    // Week granularity
    // Monday 26th August 1985 12:00:00 AM
    g = new UDFDateFloorWeek();
    TimestampWritable i4 = g.evaluate(t);
    assertEquals(493887600000L, i4.getTimestamp().getTime());

    // Day granularity
    // Friday 30th August 1985 12:00:00 AM
    g = new UDFDateFloorDay();
    TimestampWritable i5 = g.evaluate(t);
    assertEquals(494233200000L, i5.getTimestamp().getTime());

    // Hour granularity
    // Friday 30th August 1985 02:00:00 AM
    g = new UDFDateFloorHour();
    TimestampWritable i6 = g.evaluate(t);
    assertEquals(494240400000L, i6.getTimestamp().getTime());

    // Minute granularity
    // Friday 30th August 1985 02:47:00 AM
    g = new UDFDateFloorMinute();
    TimestampWritable i7 = g.evaluate(t);
    assertEquals(494243220000L, i7.getTimestamp().getTime());

    // Second granularity
    // Friday 30th August 1985 02:47:02 AM
    g = new UDFDateFloorSecond();
    TimestampWritable i8 = g.evaluate(t);
    assertEquals(494243222000L, i8.getTimestamp().getTime());
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
