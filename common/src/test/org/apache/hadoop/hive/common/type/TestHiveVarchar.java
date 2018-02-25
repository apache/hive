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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.junit.Rule;
import org.junit.Test;

import com.google.code.tempusfugit.concurrency.ConcurrentRule;
import com.google.code.tempusfugit.concurrency.RepeatingRule;
import com.google.code.tempusfugit.concurrency.annotations.Concurrent;
import com.google.code.tempusfugit.concurrency.annotations.Repeating;

public class TestHiveVarchar {
  @Rule public ConcurrentRule concurrentRule = new ConcurrentRule();
  @Rule public RepeatingRule repeatingRule = new RepeatingRule();

  public TestHiveVarchar() {
    super();
  }

  static Random rnd = new Random();

  public static int getRandomSupplementaryChar() {
    int lowSurrogate = 0xDC00 + rnd.nextInt(1024);
    //return 0xD8000000 + lowSurrogate;
    int highSurrogate = 0xD800;
    return Character.toCodePoint((char)highSurrogate, (char)lowSurrogate);
  }

  public static int getRandomCodePoint() {
    int codePoint;
    if (rnd.nextDouble() < 0.50) {
      codePoint = 32 + rnd.nextInt(90);
    } else {
      codePoint = getRandomSupplementaryChar();
    }
    if (!Character.isValidCodePoint(codePoint)) {
      System.out.println(Integer.toHexString(codePoint) + " is not a valid code point");
    }
    return codePoint;
  }

  public static int getRandomCodePoint(int excludeChar) {
    while (true) {
      int codePoint = getRandomCodePoint();
      if (codePoint != excludeChar) {
        return codePoint;
      }
    }
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testStringLength() throws Exception {
    int strLen = 20;
    int[] lengths = { 15, 20, 25 };
    // Try with supplementary characters
    for (int idx1 = 0; idx1 < lengths.length; ++idx1) {
      // Create random test string
      StringBuilder sb = new StringBuilder();
      int curLen = lengths[idx1];
      for (int idx2 = 0; idx2 < curLen; ++idx2) {
        sb.appendCodePoint(getRandomCodePoint(' '));
      }
      String testString = sb.toString();
      assertEquals(curLen, testString.codePointCount(0, testString.length()));
      String enforcedString = HiveBaseChar.enforceMaxLength(testString, strLen);
      if (curLen <= strLen) {
        // No truncation needed
        assertEquals(testString, enforcedString);
      } else {
        // String should have been truncated.
        assertEquals(strLen, enforcedString.codePointCount(0, enforcedString.length()));
      }
    }

    // Try with ascii chars
    String[] testStrings = {
        "abcdefg",
        "abcdefghijklmnopqrst",
        "abcdefghijklmnopqrstuvwxyz"
    };
    for (String testString : testStrings) {
      int curLen = testString.length();
      assertEquals(curLen, testString.codePointCount(0, testString.length()));
      String enforcedString = HiveBaseChar.enforceMaxLength(testString, strLen);
      if (curLen <= strLen) {
        // No truncation needed
        assertEquals(testString, enforcedString);
      } else {
        // String should have been truncated.
        assertEquals(strLen, enforcedString.codePointCount(0, enforcedString.length()));
      }
    }

    HiveVarchar vc1 = new HiveVarchar("0123456789", 10);
    assertEquals(10, vc1.getCharacterLength());

    // Changing string value; getCharacterLength() should update accordingly
    vc1.setValue("012345678901234");
    assertEquals(15, vc1.getCharacterLength());

    vc1.setValue("01234", -1);
    assertEquals(5, vc1.getCharacterLength());

    vc1.setValue(new HiveVarchar("0123456789", -1));
    assertEquals(10, vc1.getCharacterLength());

    vc1.setValue(new HiveVarchar("01234", -1), -1);
    assertEquals(5, vc1.getCharacterLength());
  }

  @Test
  @Concurrent(count=4)
  @Repeating(repetition=100)
  public void testComparison() throws Exception {
    HiveVarchar hc1 = new HiveVarchar("abcd", 20);
    HiveVarchar hc2 = new HiveVarchar("abcd", 20);

    // Identical strings should be equal
    assertTrue(hc1.equals(hc2));
    assertTrue(hc2.equals(hc1));
    assertEquals(0, hc1.compareTo(hc2));
    assertEquals(0, hc2.compareTo(hc1));

    // Unequal strings
    hc2 = new HiveVarchar("abcde", 20);
    assertFalse(hc1.equals(hc2));
    assertFalse(hc2.equals(hc1));
    assertFalse(0 == hc1.compareTo(hc2));
    assertFalse(0 == hc2.compareTo(hc1));

    // Trailing spaces are significant
    hc2 = new HiveVarchar("abcd  ", 30);

    assertFalse(hc1.equals(hc2));
    assertFalse(hc2.equals(hc1));
    assertFalse(0 == hc1.compareTo(hc2));
    assertFalse(0 == hc2.compareTo(hc1));

    // Leading spaces are significant
    hc2 = new HiveVarchar("  abcd", 20);
    assertFalse(hc1.equals(hc2));
    assertFalse(hc2.equals(hc1));
    assertFalse(0 == hc1.compareTo(hc2));
    assertFalse(0 == hc2.compareTo(hc1));
  }
}
