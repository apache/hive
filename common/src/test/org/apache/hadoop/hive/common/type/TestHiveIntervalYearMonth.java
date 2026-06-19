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
package org.apache.hadoop.hive.common.type;

import org.junit.*;
import static org.junit.Assert.*;
import com.google.code.tempusfugit.concurrency.annotations.*;
import com.google.code.tempusfugit.concurrency.*;

public class TestHiveIntervalYearMonth {

  @Rule public ConcurrentRule concurrentRule = new ConcurrentRule();
  @Rule public RepeatingRule repeatingRule = new RepeatingRule();

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testGetters() throws Exception {
    HiveIntervalYearMonth i1 = new HiveIntervalYearMonth(1, 2);
    assertEquals(1, i1.getYears());
    assertEquals(2, i1.getMonths());
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testCompare() throws Exception {
    HiveIntervalYearMonth i1 = new HiveIntervalYearMonth(1, 2);
    HiveIntervalYearMonth i2 = new HiveIntervalYearMonth(1, 2);
    HiveIntervalYearMonth i3 = new HiveIntervalYearMonth(1, 3);

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

    // hashCode()
    assertEquals(i1 + " hashCode " + i1, i1.hashCode(), i1.hashCode());
    assertEquals(i1 + " hashCode " + i1, i1.hashCode(), i2.hashCode());
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testValueOf() throws Exception {
    HiveIntervalYearMonth i1 = HiveIntervalYearMonth.valueOf("1-2");
    assertEquals(1, i1.getYears());
    assertEquals(2, i1.getMonths());

    HiveIntervalYearMonth i2 = HiveIntervalYearMonth.valueOf("+8-9");
    assertEquals(8, i2.getYears());
    assertEquals(9, i2.getMonths());

    HiveIntervalYearMonth i3 = HiveIntervalYearMonth.valueOf("-10-11");
    assertEquals(-10, i3.getYears());
    assertEquals(-11, i3.getMonths());

    HiveIntervalYearMonth i4 = HiveIntervalYearMonth.valueOf("-0-0");
    assertEquals(0, i4.getYears());
    assertEquals(0, i4.getMonths());

    // Invalid values
    String[] invalidValues = {
      null,
      "abc",
      "0-12",
      "0 1:2:3"
    };
    for (String invalidValue : invalidValues) {
      boolean caughtException = false;
      try {
        HiveIntervalYearMonth.valueOf(invalidValue);
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
    assertEquals("0-0", HiveIntervalYearMonth.valueOf("0-0").toString());
    assertEquals("1-2", HiveIntervalYearMonth.valueOf("1-2").toString());
    assertEquals("-1-2", HiveIntervalYearMonth.valueOf("-1-2").toString());
    assertEquals("1-0", HiveIntervalYearMonth.valueOf("1-0").toString());
    assertEquals("-1-0", HiveIntervalYearMonth.valueOf("-1-0").toString());
    assertEquals("0-0", HiveIntervalYearMonth.valueOf("-0-0").toString());
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testNormalize() throws Exception {
    HiveIntervalYearMonth i1 = new HiveIntervalYearMonth(1, -6);
    assertEquals(HiveIntervalYearMonth.valueOf("0-6"), i1);
    assertEquals(0, i1.getYears());
    assertEquals(6, i1.getMonths());

    assertEquals(HiveIntervalYearMonth.valueOf("0-0"), new HiveIntervalYearMonth(0, 0));
    assertEquals(HiveIntervalYearMonth.valueOf("0-0"), new HiveIntervalYearMonth(-1, 12));
    assertEquals(HiveIntervalYearMonth.valueOf("0-4"), new HiveIntervalYearMonth(-1, 16));
    assertEquals(HiveIntervalYearMonth.valueOf("0-11"), new HiveIntervalYearMonth(1, -1));
    assertEquals(HiveIntervalYearMonth.valueOf("-0-11"), new HiveIntervalYearMonth(-1, 1));

    // -5 years + 121 months = -5 years + 10 years + 1 month = 5 years 1 month
    assertEquals(HiveIntervalYearMonth.valueOf("5-1"), new HiveIntervalYearMonth(-5, 121));
  }
}
