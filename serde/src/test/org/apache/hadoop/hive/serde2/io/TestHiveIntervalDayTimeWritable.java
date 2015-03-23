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

import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.junit.*;

import static org.junit.Assert.*;

public class TestHiveIntervalDayTimeWritable {
  @Rule public ConcurrentRule concurrentRule = new ConcurrentRule();
  @Rule public RepeatingRule repeatingRule = new RepeatingRule();

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testConstructor() throws Exception {
    HiveIntervalDayTime hi1 = HiveIntervalDayTime.valueOf("3 4:5:6.12345");
    HiveIntervalDayTimeWritable hiw1 = new HiveIntervalDayTimeWritable(hi1);
    HiveIntervalDayTimeWritable hiw2 = new HiveIntervalDayTimeWritable(hiw1);
    assertEquals(hiw1, hiw2);
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testComparison() throws Exception {
    HiveIntervalDayTimeWritable hiw0 = new HiveIntervalDayTimeWritable(
        HiveIntervalDayTime.valueOf("2 2:2:2.22222"));
    HiveIntervalDayTimeWritable hiw1 = new HiveIntervalDayTimeWritable(
        HiveIntervalDayTime.valueOf("2 2:2:2.22222"));
    HiveIntervalDayTimeWritable hiw2 = new HiveIntervalDayTimeWritable(
        HiveIntervalDayTime.valueOf("3 2:2:2.22222"));

    assertTrue(hiw1 + " equals " + hiw1, hiw1.equals(hiw1));
    assertTrue(hiw1 + " equals " + hiw0, hiw1.equals(hiw0));
    assertFalse(hiw1 + " equals " + hiw2, hiw1.equals(hiw2));

    assertTrue(hiw1 + " compare " + hiw1, 0 ==  hiw1.compareTo(hiw1));
    assertTrue(hiw1 + " compare " + hiw0, 0 ==  hiw1.compareTo(hiw0));
    assertTrue(hiw1 + " compare " + hiw2, 0 > hiw1.compareTo(hiw2));

    hiw2 = new HiveIntervalDayTimeWritable(
        HiveIntervalDayTime.valueOf("3 2:2:2.22222"));
    assertTrue(hiw1 + " compare " + hiw2, 0 > hiw1.compareTo(hiw2));
    hiw2 = new HiveIntervalDayTimeWritable(
        HiveIntervalDayTime.valueOf("1 2:2:2.22222"));
    assertTrue(hiw1 + " compare " + hiw2, 0 < hiw1.compareTo(hiw2));

    hiw2 = new HiveIntervalDayTimeWritable(
        HiveIntervalDayTime.valueOf("2 3:2:2.22222"));
    assertTrue(hiw1 + " compare " + hiw2, 0 > hiw1.compareTo(hiw2));
    hiw2 = new HiveIntervalDayTimeWritable(
        HiveIntervalDayTime.valueOf("2 1:2:2.22222"));
    assertTrue(hiw1 + " compare " + hiw2, 0 < hiw1.compareTo(hiw2));

    hiw2 = new HiveIntervalDayTimeWritable(
        HiveIntervalDayTime.valueOf("2 2:3:2.22222"));
    assertTrue(hiw1 + " compare " + hiw2, 0 > hiw1.compareTo(hiw2));
    hiw2 = new HiveIntervalDayTimeWritable(
        HiveIntervalDayTime.valueOf("2 2:1:2.22222"));
    assertTrue(hiw1 + " compare " + hiw2, 0 < hiw1.compareTo(hiw2));

    hiw2 = new HiveIntervalDayTimeWritable(
        HiveIntervalDayTime.valueOf("2 2:2:3.22222"));
    assertTrue(hiw1 + " compare " + hiw2, 0 > hiw1.compareTo(hiw2));
    hiw2 = new HiveIntervalDayTimeWritable(
        HiveIntervalDayTime.valueOf("2 2:2:1.22222"));
    assertTrue(hiw1 + " compare " + hiw2, 0 < hiw1.compareTo(hiw2));

    hiw2 = new HiveIntervalDayTimeWritable(
        HiveIntervalDayTime.valueOf("2 2:2:2.33333"));
    assertTrue(hiw1 + " compare " + hiw2, 0 > hiw1.compareTo(hiw2));
    hiw2 = new HiveIntervalDayTimeWritable(
        HiveIntervalDayTime.valueOf("2 2:2:2.11111"));
    assertTrue(hiw1 + " compare " + hiw2, 0 < hiw1.compareTo(hiw2));

    // Also check hashCode()
    assertEquals(hiw0.hashCode(), hiw1.hashCode());
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testGettersSetters() throws Exception {
    HiveIntervalDayTimeWritable hiw1 = new HiveIntervalDayTimeWritable();

    hiw1.set(3, 4, 5, 6, 7);

    HiveIntervalDayTime hi1 = hiw1.getHiveIntervalDayTime();
    assertEquals(3, hi1.getDays());
    assertEquals(4, hi1.getHours());
    assertEquals(5, hi1.getMinutes());
    assertEquals(6, hi1.getSeconds());
    assertEquals(7, hi1.getNanos());
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testWritableMethods() throws Exception {
    HiveIntervalDayTimeWritable hiw1 = new HiveIntervalDayTimeWritable();
    HiveIntervalDayTimeWritable hiw2 = new HiveIntervalDayTimeWritable();

    hiw1.set(3, 4, 5, 6, 7);
    hiw2.set(5, 4, 3, 2, 1);
    assertFalse(hiw1.equals(hiw2));

    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutput out = new DataOutputStream(byteStream);

    hiw1.write(out);
    hiw2.readFields(new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray())));
    assertEquals(hiw1, hiw2);
  }
}
