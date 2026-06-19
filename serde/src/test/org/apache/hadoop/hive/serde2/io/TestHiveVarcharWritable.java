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
package org.apache.hadoop.hive.serde2.io;

import com.google.code.tempusfugit.concurrency.annotations.*;
import com.google.code.tempusfugit.concurrency.*;
import org.junit.*;
import static org.junit.Assert.*;

import org.apache.hadoop.hive.common.type.HiveVarchar;
import java.io.*;

public class TestHiveVarcharWritable {
  @Rule public ConcurrentRule concurrentRule = new ConcurrentRule();
  @Rule public RepeatingRule repeatingRule = new RepeatingRule();

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testStringLength() throws Exception {
    HiveVarcharWritable vc1 = new HiveVarcharWritable(new HiveVarchar("0123456789", 10));
    assertEquals(10, vc1.getCharacterLength());

    // Changing string value; getCharacterLength() should update accordingly
    vc1.set("012345678901234");
    assertEquals(15, vc1.getCharacterLength());

    vc1.set(new HiveVarcharWritable(new HiveVarchar("01234", -1)));
    assertEquals(5, vc1.getCharacterLength());

    vc1.set(new HiveVarchar("012345", -1));
    assertEquals(6, vc1.getCharacterLength());

    vc1.set("0123456", -1);
    assertEquals(7, vc1.getCharacterLength());

    vc1.set(new HiveVarcharWritable(new HiveVarchar("01234567", -1)), -1);
    assertEquals(8, vc1.getCharacterLength());

    // string length should work after enforceMaxLength()
    vc1.enforceMaxLength(3);
    assertEquals(3, vc1.getCharacterLength());

    // string length should work after readFields()
    ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
    HiveVarcharWritable vc2 = new HiveVarcharWritable(new HiveVarchar("abcdef", -1));
    vc2.write(new DataOutputStream(outputBytes));
    vc1.readFields(new DataInputStream(new ByteArrayInputStream(outputBytes.toByteArray())));
    assertEquals(6, vc1.getCharacterLength());
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testEnforceLength() throws Exception {
    HiveVarcharWritable vc1 = new HiveVarcharWritable(new HiveVarchar("0123456789", 10));
    assertEquals(10, vc1.getCharacterLength());

    vc1.enforceMaxLength(20);
    assertEquals(10, vc1.getCharacterLength());

    vc1.enforceMaxLength(10);
    assertEquals(10, vc1.getCharacterLength());

    vc1.enforceMaxLength(8);
    assertEquals(8, vc1.getCharacterLength());
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testComparison() throws Exception {
    HiveVarcharWritable hc1 = new HiveVarcharWritable(new HiveVarchar("abcd", 20));
    HiveVarcharWritable hc2 = new HiveVarcharWritable(new HiveVarchar("abcd", 20));

    // Identical strings should be equal
    assertTrue(hc1.equals(hc2));
    assertTrue(hc2.equals(hc1));
    assertEquals(0, hc1.compareTo(hc2));
    assertEquals(0, hc2.compareTo(hc1));

    // Unequal strings
    hc2 = new HiveVarcharWritable(new HiveVarchar("abcde", 20));
    assertFalse(hc1.equals(hc2));
    assertFalse(hc2.equals(hc1));
    assertFalse(0 == hc1.compareTo(hc2));
    assertFalse(0 == hc2.compareTo(hc1));

    // Trailing spaces are significant
    hc2 = new HiveVarcharWritable(new HiveVarchar("abcd  ", 30));

    assertFalse(hc1.equals(hc2));
    assertFalse(hc2.equals(hc1));
    assertFalse(0 == hc1.compareTo(hc2));
    assertFalse(0 == hc2.compareTo(hc1));

    // Leading spaces are significant
    hc2 = new HiveVarcharWritable(new HiveVarchar("  abcd", 20));
    assertFalse(hc1.equals(hc2));
    assertFalse(hc2.equals(hc1));
    assertFalse(0 == hc1.compareTo(hc2));
    assertFalse(0 == hc2.compareTo(hc1));
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testStringValue() throws Exception {
    HiveVarcharWritable vc1 = new HiveVarcharWritable(new HiveVarchar("abcde", 20));
    assertEquals("abcde", vc1.toString());
    assertEquals("abcde", vc1.getHiveVarchar().toString());
  }
}
