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

import static org.junit.Assert.*;
import org.junit.Test;

import java.sql.Date;

public class TestDateParser {
  DateParser parser = new DateParser();
  Date date = new Date(0);

  void checkValidCase(String strValue, Date expected) {
    Date dateValue = parser.parseDate(strValue);
    assertEquals(expected, dateValue);

    assertTrue(parser.parseDate(strValue, date));
    assertEquals(expected, date);
  }

  void checkInvalidCase(String strValue) {
    Date dateValue = parser.parseDate(strValue);
    assertNull(dateValue);

    assertFalse(parser.parseDate(strValue, date));
  }

  @Test
  public void testValidCases() throws Exception {
    checkValidCase("1945-12-31", Date.valueOf("1945-12-31"));
    checkValidCase("1946-01-01", Date.valueOf("1946-01-01"));
    checkValidCase("2001-11-12", Date.valueOf("2001-11-12"));
    checkValidCase("0004-05-06", Date.valueOf("0004-05-06"));
    checkValidCase("1678-09-10", Date.valueOf("1678-09-10"));
    checkValidCase("9999-10-11", Date.valueOf("9999-10-11"));

    // Timestamp strings should parse ok
    checkValidCase("2001-11-12 01:02:03", Date.valueOf("2001-11-12"));

    // Leading spaces
    checkValidCase(" 1946-01-01", Date.valueOf("1946-01-01"));
    checkValidCase(" 2001-11-12 01:02:03", Date.valueOf("2001-11-12"));

    // Current date parsing is lenient
    checkValidCase("2001-13-12", Date.valueOf("2002-01-12"));
    checkValidCase("2001-11-31", Date.valueOf("2001-12-01"));
  }

  @Test
  public void testInvalidCases() throws Exception {
    checkInvalidCase("2001");
    checkInvalidCase("2001-01");
    checkInvalidCase("abc");
    checkInvalidCase(" 2001 ");
    checkInvalidCase("a2001-01-01");
  }
}
