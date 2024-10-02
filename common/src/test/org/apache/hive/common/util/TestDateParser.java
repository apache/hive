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
package org.apache.hive.common.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.common.type.Date;
import org.junit.Test;

public class TestDateParser {
  private Date date = new Date();

  void checkValidCase(String strValue, Date expected) {
    Date dateValue = DateParser.parseDate(strValue);
    assertEquals(expected, dateValue);

    assertTrue(DateParser.parseDate(strValue, date));
    assertEquals(expected, date);
  }

  void checkInvalidCase(String strValue) {
    Date dateValue = DateParser.parseDate(strValue);
    assertNull(dateValue);

    assertFalse(DateParser.parseDate(strValue, date));
  }

  @Test
  public void testValidCases() throws Exception {
    checkValidCase("1945-12-31", Date.of(1945,12,31));
    checkValidCase("1946-01-01", Date.of(1946,1,1));
    checkValidCase("2001-11-12", Date.of(2001,11,12));
    checkValidCase("0004-05-06", Date.of(4,5,6));
    checkValidCase("1678-09-10", Date.of(1678,9,10));
    checkValidCase("9999-10-11", Date.of(9999,10,11));

    // Timestamp strings should parse ok
    checkValidCase("2001-11-12 01:02:03", Date.of(2001,11,12));

    // Leading spaces
    checkValidCase(" 1946-01-01", Date.of(1946,01,01));
    checkValidCase(" 2001-11-12 01:02:03", Date.of(2001,11,12));
  }

  @Test
  public void testParseDateFromTimestampWithCommonTimeDelimiter() {
    for (String d : new String[] { "T", " ", "-", ".", "_" }) {
      String ts = "2023-08-03" + d + "01:02:03";
      assertEquals("Parsing " + ts, Date.of(2023, 8, 3), DateParser.parseDate(ts));
    }
  }

  @Test
  public void testInvalidCases() throws Exception {
    checkInvalidCase("2001");
    checkInvalidCase("2001-01");
    checkInvalidCase("abc");
    checkInvalidCase(" 2001 ");
    checkInvalidCase("a2001-01-01");
    checkInvalidCase("0000-00-00");
    checkInvalidCase("2001-13-12");
    checkInvalidCase("2001-11-31");
    checkInvalidCase("19999-10-11");
    checkInvalidCase("08-08-2023");
    checkInvalidCase("2023-08-0800");
  }
}
