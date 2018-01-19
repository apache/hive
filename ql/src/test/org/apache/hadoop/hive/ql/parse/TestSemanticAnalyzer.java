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
package org.apache.hadoop.hive.ql.parse;

import static org.junit.Assert.*;

import java.sql.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.junit.Test;

public class TestSemanticAnalyzer {

  @Test
  public void testNormalizeColSpec() throws Exception {
    // Hive normalizes partition spec for dates to yyyy-mm-dd format. Some versions of Java will
    // accept other formats for Date.valueOf, e.g. yyyy-m-d, and who knows what else in the future;
    // some will not accept other formats, so we cannot test normalization with them - type check
    // will fail before it can ever happen. Thus, test in isolation.
    checkNormalization("date", "2010-01-01", "2010-01-01", Date.valueOf("2010-01-01"));
    checkNormalization("date", "2010-1-01", "2010-01-01", Date.valueOf("2010-01-01"));
    checkNormalization("date", "2010-1-1", "2010-01-01", Date.valueOf("2010-01-01"));
    checkNormalization("string", "2010-1-1", "2010-1-1", "2010-1-1");

    try {
      checkNormalization("date", "foo", "", "foo"); // Bad format.
      fail("should throw");
    } catch (SemanticException ex) {
    }

    try {
      checkNormalization("date", "2010-01-01", "2010-01-01", "2010-01-01"); // Bad value type.
      fail("should throw");
    } catch (SemanticException ex) {
    }
  }


  public void checkNormalization(String colType, String originalColSpec,
      String result, Object colValue) throws SemanticException {
    final String colName = "col";
    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put(colName, originalColSpec);
    BaseSemanticAnalyzer.normalizeColSpec(partSpec, colName, colType, originalColSpec, colValue);
    assertEquals(result, partSpec.get(colName));
    if (colValue instanceof Date) {
      DateWritable dw = new DateWritable((Date)colValue);
      BaseSemanticAnalyzer.normalizeColSpec(partSpec, colName, colType, originalColSpec, dw);
      assertEquals(result, partSpec.get(colName));
    }
  }

  @Test
  public void testUnescapeSQLString() {
    assertEquals("abcdefg", BaseSemanticAnalyzer.unescapeSQLString("\"abcdefg\""));

    // String enclosed by single quotes.
    assertEquals("C0FFEE", BaseSemanticAnalyzer.unescapeSQLString("\'C0FFEE\'"));

    // Strings including single escaped characters.
    assertEquals("\u0000", BaseSemanticAnalyzer.unescapeSQLString("'\\0'"));
    assertEquals("\'", BaseSemanticAnalyzer.unescapeSQLString("\"\\'\""));
    assertEquals("\"", BaseSemanticAnalyzer.unescapeSQLString("'\\\"'"));
    assertEquals("\b", BaseSemanticAnalyzer.unescapeSQLString("\"\\b\""));
    assertEquals("\n", BaseSemanticAnalyzer.unescapeSQLString("'\\n'"));
    assertEquals("\r", BaseSemanticAnalyzer.unescapeSQLString("\"\\r\""));
    assertEquals("\t", BaseSemanticAnalyzer.unescapeSQLString("'\\t'"));
    assertEquals("\u001A", BaseSemanticAnalyzer.unescapeSQLString("\"\\Z\""));
    assertEquals("\\", BaseSemanticAnalyzer.unescapeSQLString("'\\\\'"));
    assertEquals("\\%", BaseSemanticAnalyzer.unescapeSQLString("\"\\%\""));
    assertEquals("\\_", BaseSemanticAnalyzer.unescapeSQLString("'\\_'"));

    // String including '\000' style literal characters.
    assertEquals("3 + 5 = \u0038", BaseSemanticAnalyzer.unescapeSQLString("'3 + 5 = \\070'"));
    assertEquals("\u0000", BaseSemanticAnalyzer.unescapeSQLString("\"\\000\""));

    // String including invalid '\000' style literal characters.
    assertEquals("256", BaseSemanticAnalyzer.unescapeSQLString("\"\\256\""));

    // String including a '\u0000' style literal characters (\u732B is a cat in Kanji).
    assertEquals("How cute \u732B are",
      BaseSemanticAnalyzer.unescapeSQLString("\"How cute \\u732B are\""));

    // String including a surrogate pair character
    // (\uD867\uDE3D is Okhotsk atka mackerel in Kanji).
    assertEquals("\uD867\uDE3D is a fish",
      BaseSemanticAnalyzer.unescapeSQLString("\"\\uD867\uDE3D is a fish\""));
  }
}
