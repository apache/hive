/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.serde2.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;

import com.google.code.tempusfugit.concurrency.annotations.*;
import com.google.code.tempusfugit.concurrency.*;

import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.junit.*;

import static org.junit.Assert.*;

public class TestHiveIntervalYearMonthWritable {
  @Rule public ConcurrentRule concurrentRule = new ConcurrentRule();
  @Rule public RepeatingRule repeatingRule = new RepeatingRule();

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testConstructor() throws Exception {
    HiveIntervalYearMonth hi1 = HiveIntervalYearMonth.valueOf("1-2");
    HiveIntervalYearMonthWritable hiw1 = new HiveIntervalYearMonthWritable(hi1);
    HiveIntervalYearMonthWritable hiw2 = new HiveIntervalYearMonthWritable(hiw1);
    assertEquals(hiw1, hiw2);
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testComparison() throws Exception {
    HiveIntervalYearMonthWritable hiw0 = new HiveIntervalYearMonthWritable(
        HiveIntervalYearMonth.valueOf("2-2"));
    HiveIntervalYearMonthWritable hiw1 = new HiveIntervalYearMonthWritable(
        HiveIntervalYearMonth.valueOf("2-2"));
    HiveIntervalYearMonthWritable hiw2 = new HiveIntervalYearMonthWritable(
        HiveIntervalYearMonth.valueOf("3-2"));

    assertTrue(hiw1 + " equals " + hiw1, hiw1.equals(hiw1));
    assertTrue(hiw1 + " equals " + hiw0, hiw1.equals(hiw0));
    assertFalse(hiw1 + " equals " + hiw2, hiw1.equals(hiw2));

    assertTrue(hiw1 + " compare " + hiw1, 0 ==  hiw1.compareTo(hiw1));
    assertTrue(hiw1 + " compare " + hiw0, 0 ==  hiw1.compareTo(hiw0));
    assertTrue(hiw1 + " compare " + hiw2, 0 > hiw1.compareTo(hiw2));
    hiw2 = new HiveIntervalYearMonthWritable(
        HiveIntervalYearMonth.valueOf("1-2"));
    assertTrue(hiw1 + " compare " + hiw2, 0 < hiw1.compareTo(hiw2));

    hiw2 = new HiveIntervalYearMonthWritable(
        HiveIntervalYearMonth.valueOf("2-3"));
    assertTrue(hiw1 + " compare " + hiw2, 0 > hiw1.compareTo(hiw2));
    hiw2 = new HiveIntervalYearMonthWritable(
        HiveIntervalYearMonth.valueOf("2-1"));
    assertTrue(hiw1 + " compare " + hiw2, 0 < hiw1.compareTo(hiw2));

    // Also check hashCode()
    assertEquals(hiw0.hashCode(), hiw1.hashCode());
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testGettersSetters() throws Exception {
    HiveIntervalYearMonthWritable hiw1 = new HiveIntervalYearMonthWritable();

    hiw1.set(1, 2);
    HiveIntervalYearMonth hi1 = hiw1.getHiveIntervalYearMonth();
    assertEquals(1, hi1.getYears());
    assertEquals(2, hi1.getMonths());

    hiw1.set(new HiveIntervalYearMonth(3,4));
    hi1 = hiw1.getHiveIntervalYearMonth();
    assertEquals(3, hi1.getYears());
    assertEquals(4, hi1.getMonths());

    hiw1.set(new HiveIntervalYearMonthWritable(new HiveIntervalYearMonth(5,6)));
    hi1 = hiw1.getHiveIntervalYearMonth();
    assertEquals(5, hi1.getYears());
    assertEquals(6, hi1.getMonths());
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testWritableMethods() throws Exception {
    HiveIntervalYearMonthWritable hiw1 = new HiveIntervalYearMonthWritable();
    HiveIntervalYearMonthWritable hiw2 = new HiveIntervalYearMonthWritable();

    hiw1.set(1, 2);
    hiw2.set(7, 6);
    assertFalse(hiw1.equals(hiw2));

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(byteStream);

    hiw1.write(out);
    hiw2.readFields(new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray())));
    assertEquals(hiw1, hiw2);
  }
}
