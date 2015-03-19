/**
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
package org.apache.hadoop.hive.common.type;

import org.junit.*;

import static org.junit.Assert.*;
import com.google.code.tempusfugit.concurrency.annotations.*;
import com.google.code.tempusfugit.concurrency.*;

public class TestHiveIntervalDayTime {

  @Rule public ConcurrentRule concurrentRule = new ConcurrentRule();
  @Rule public RepeatingRule repeatingRule = new RepeatingRule();

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testGetters() throws Exception {
    HiveIntervalDayTime i1 = new HiveIntervalDayTime(3, 4, 5, 6, 7);

    assertEquals(3, i1.getDays());
    assertEquals(4, i1.getHours());
    assertEquals(5, i1.getMinutes());
    assertEquals(6, i1.getSeconds());
    assertEquals(7, i1.getNanos());
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testCompare() throws Exception {
    HiveIntervalDayTime i1 = new HiveIntervalDayTime(3, 4, 5, 6, 7);
    HiveIntervalDayTime i2 = new HiveIntervalDayTime(3, 4, 5, 6, 7);
    HiveIntervalDayTime i3 = new HiveIntervalDayTime(3, 4, 8, 9, 10);
    HiveIntervalDayTime i4 = new HiveIntervalDayTime(3, 4, 8, 9, 5);

    // compareTo()
    assertEquals(i1 + " compareTo " + i1, 0, i1.compareTo(i1));
    assertEquals(i1 + " compareTo " + i2, 0, i1.compareTo(i2));
    assertEquals(i2 + " compareTo " + i1, 0, i2.compareTo(i1));
    assertEquals(i3 + " compareTo " + i3, 0, i3.compareTo(i3));

    assertTrue(i1 + " compareTo " + i3, 0 > i1.compareTo(i3));
    assertTrue(i3 + " compareTo " + i1, 0 < i3.compareTo(i1));

    // equals()
    assertTrue(i1 + " equals " + i1, i1.equals(i1));
    assertTrue(i1 + " equals " + i2, i1.equals(i2));
    assertFalse(i1 + " equals " + i3, i1.equals(i3));
    assertFalse(i3 + " equals " + i1, i3.equals(i1));
    assertFalse(i3 + " equals " + i4, i3.equals(i4));

    // hashCode()
    assertEquals(i1 + " hashCode " + i1, i1.hashCode(), i1.hashCode());
    assertEquals(i1 + " hashCode " + i1, i1.hashCode(), i2.hashCode());
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testValueOf() throws Exception {
    HiveIntervalDayTime i1 = HiveIntervalDayTime.valueOf("3 04:05:06.123456");
    assertEquals(3, i1.getDays());
    assertEquals(4, i1.getHours());
    assertEquals(5, i1.getMinutes());
    assertEquals(6, i1.getSeconds());
    assertEquals(123456000, i1.getNanos());

    HiveIntervalDayTime i2 = HiveIntervalDayTime.valueOf("+3 04:05:06");
    assertEquals(3, i2.getDays());
    assertEquals(4, i2.getHours());
    assertEquals(5, i2.getMinutes());
    assertEquals(6, i2.getSeconds());
    assertEquals(0, i2.getNanos());

    HiveIntervalDayTime i3 = HiveIntervalDayTime.valueOf("-12 13:14:15.987654321");
    assertEquals(-12, i3.getDays());
    assertEquals(-13, i3.getHours());
    assertEquals(-14, i3.getMinutes());
    assertEquals(-15, i3.getSeconds());
    assertEquals(-987654321, i3.getNanos());

    HiveIntervalDayTime i4 = HiveIntervalDayTime.valueOf("-0 0:0:0.000000012");
    assertEquals(0, i4.getDays());
    assertEquals(0, i4.getHours());
    assertEquals(0, i4.getMinutes());
    assertEquals(0, i4.getSeconds());
    assertEquals(-12, i4.getNanos());

    // Invalid values
    String[] invalidValues = {
      null,
      "abc",
      "0-11",
      "0 60:0:0",
      "0 0:60:0"
    };
    for (String invalidValue : invalidValues) {
      boolean caughtException = false;
      try {
        HiveIntervalDayTime.valueOf(invalidValue);
        fail("Expected exception");
      } catch (IllegalArgumentException err) {
        caughtException = true;
      }
      assertTrue("Expected exception", caughtException);
    }
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testToString() throws Exception {
    assertEquals("0 00:00:00.000000000", HiveIntervalDayTime.valueOf("0 00:00:00").toString());
    assertEquals("3 04:05:06.123456000", HiveIntervalDayTime.valueOf("3 04:05:06.123456").toString());
    assertEquals("-3 04:05:06.123456000", HiveIntervalDayTime.valueOf("-3 04:05:06.123456").toString());
    assertEquals("1 00:00:00.000000000", HiveIntervalDayTime.valueOf("1 00:00:00").toString());
    assertEquals("-1 00:00:00.000000000", HiveIntervalDayTime.valueOf("-1 00:00:00").toString());
    assertEquals("0 00:00:00.880000000", HiveIntervalDayTime.valueOf("0 00:00:00.88").toString());
    assertEquals("-0 00:00:00.880000000", HiveIntervalDayTime.valueOf("-0 00:00:00.88").toString());

    // Mixed sign cases
    assertEquals("-3 04:05:06.000000007",
        new HiveIntervalDayTime(-3, -4, -5, -6, -7).toString());
    assertEquals("3 04:05:06.000000007",
        new HiveIntervalDayTime(3, 4, 5, 6, 7).toString());
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testNormalize() throws Exception {
    HiveIntervalDayTime i1 = new HiveIntervalDayTime(50, 48, 3, 5400, 2000000123);
    assertEquals(HiveIntervalDayTime.valueOf("52 1:33:2.000000123"), i1);
    assertEquals(52, i1.getDays());
    assertEquals(1, i1.getHours());
    assertEquals(33, i1.getMinutes());
    assertEquals(2, i1.getSeconds());
    assertEquals(123, i1.getNanos());

    assertEquals(HiveIntervalDayTime.valueOf("0 0:0:0"),
        new HiveIntervalDayTime(0, 0, 0, 0, 0));
    assertEquals(HiveIntervalDayTime.valueOf("0 0:0:0"),
        new HiveIntervalDayTime(2, -48, 0, 1, -1000000000));
    assertEquals(HiveIntervalDayTime.valueOf("0 0:0:0"),
        new HiveIntervalDayTime(-2, 48, 0, -1, 1000000000));
    assertEquals(HiveIntervalDayTime.valueOf("1 0:0:0"),
        new HiveIntervalDayTime(-1, 48, 0, 0, 0));
    assertEquals(HiveIntervalDayTime.valueOf("-1 0:0:0"),
        new HiveIntervalDayTime(1, -48, 0, 0, 0));
    assertEquals(HiveIntervalDayTime.valueOf("0 23:59:59.999999999"),
        new HiveIntervalDayTime(1, 0, 0, 0, -1));
    assertEquals(HiveIntervalDayTime.valueOf("-0 23:59:59.999999999"),
        new HiveIntervalDayTime(-1, 0, 0, 0, 1));

    // -1 day 10 hrs 11 mins 172800 secs = -1 day 10 hrs 11 mins + 2 days = 1 day 10 hrs 11 mins
    assertEquals(HiveIntervalDayTime.valueOf("1 10:11:0"),
        new HiveIntervalDayTime(-1, 10, 11, 172800, 0));

    i1 = new HiveIntervalDayTime(480, 480, 0, 5400, 2000000123);
    assertEquals(500, i1.getDays());
    assertEquals(1, i1.getHours());
    assertEquals(30, i1.getMinutes());
    assertEquals(2, i1.getSeconds());
    assertEquals(123, i1.getNanos());
  }
}
