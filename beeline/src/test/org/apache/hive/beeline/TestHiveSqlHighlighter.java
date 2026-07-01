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
package org.apache.hive.beeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;
import org.junit.Test;

/**
 * Unit tests for {@link HiveSqlHighlighter}.
 */
public class TestHiveSqlHighlighter {

  private final HiveSqlHighlighter on = new HiveSqlHighlighter(() -> true);

  /** Style at character offset {@code idx} when highlighting {@code sql}. */
  private AttributedStyle styleAt(String sql, int idx) {
    return on.highlight(sql).styleAt(idx);
  }

  @Test
  public void testKeyword() {
    assertEquals(HiveSqlHighlighter.KEYWORD_STYLE, styleAt("SELECT", 0));
    assertEquals(HiveSqlHighlighter.KEYWORD_STYLE, styleAt("INSERT OVERWRITE", 0));
    assertEquals(HiveSqlHighlighter.KEYWORD_STYLE, styleAt("INSERT OVERWRITE", 7));
  }

  @Test
  public void testKeywordCaseInsensitive() {
    assertEquals(HiveSqlHighlighter.KEYWORD_STYLE, styleAt("select", 0));
    assertEquals(HiveSqlHighlighter.KEYWORD_STYLE, styleAt("SeLeCt", 0));
    assertEquals(HiveSqlHighlighter.KEYWORD_STYLE, styleAt("where", 0));
  }

  @Test
  public void testDataType() {
    assertEquals(HiveSqlHighlighter.TYPE_STYLE, styleAt("INT", 0));
    assertEquals(HiveSqlHighlighter.TYPE_STYLE, styleAt("string", 0));
    assertEquals(HiveSqlHighlighter.TYPE_STYLE, styleAt("ARRAY", 0));
    // a type must not be reported as a generic keyword
    assertTrue(HiveSqlHighlighter.TYPES.contains("INT"));
    assertTrue(!HiveSqlHighlighter.KEYWORDS.contains("INT"));
  }

  @Test
  public void testConstant() {
    assertEquals(HiveSqlHighlighter.CONSTANT_STYLE, styleAt("NULL", 0));
    assertEquals(HiveSqlHighlighter.CONSTANT_STYLE, styleAt("true", 0));
    assertEquals(HiveSqlHighlighter.CONSTANT_STYLE, styleAt("FALSE", 0));
  }

  @Test
  public void testStringLiteral() {
    assertEquals(HiveSqlHighlighter.STRING_STYLE, styleAt("'abc'", 0));
    assertEquals(HiveSqlHighlighter.STRING_STYLE, styleAt("'abc'", 2));
    assertEquals(HiveSqlHighlighter.STRING_STYLE, styleAt("\"abc\"", 0));
    // an escaped quote does not end the string
    assertEquals(HiveSqlHighlighter.STRING_STYLE, styleAt("'a\\'b'", 4));
  }

  @Test
  public void testUnterminatedStringColorsToEnd() {
    String sql = "x = 'abc";
    assertEquals(HiveSqlHighlighter.STRING_STYLE, styleAt(sql, sql.length() - 1));
  }

  @Test
  public void testNumberLiteral() {
    assertEquals(HiveSqlHighlighter.NUMBER_STYLE, styleAt("42", 0));
    assertEquals(HiveSqlHighlighter.NUMBER_STYLE, styleAt("3.14", 0));
    assertEquals(HiveSqlHighlighter.NUMBER_STYLE, styleAt("1e9", 0));
    // a number embedded in an identifier is part of the (column) identifier
    assertEquals(HiveSqlHighlighter.COLUMN_STYLE, styleAt("col1", 3));
  }

  @Test
  public void testLineComment() {
    assertEquals(HiveSqlHighlighter.COMMENT_STYLE, styleAt("-- a comment", 0));
    assertEquals(HiveSqlHighlighter.COMMENT_STYLE, styleAt("-- a comment", 5));
    // comment ends at newline; the next line is highlighted again
    String sql = "-- c\nSELECT";
    assertEquals(HiveSqlHighlighter.KEYWORD_STYLE, styleAt(sql, 5));
  }

  @Test
  public void testBlockComment() {
    assertEquals(HiveSqlHighlighter.COMMENT_STYLE, styleAt("/* x */", 0));
    assertEquals(HiveSqlHighlighter.COMMENT_STYLE, styleAt("/* x */", 3));
  }

  @Test
  public void testFunctionCall() {
    // count is not a Hive grammar keyword; followed by '(' -> function
    assertEquals(HiveSqlHighlighter.FUNCTION_STYLE, styleAt("count(*)", 0));
    assertEquals(HiveSqlHighlighter.FUNCTION_STYLE, styleAt("my_udf (x)", 0));
    // a bare identifier (no FROM context, no '(') is a column
    assertEquals(HiveSqlHighlighter.COLUMN_STYLE, styleAt("mytable", 0));
  }

  @Test
  public void testTableVsColumn() {
    String sql = "SELECT a, b FROM sales";
    assertEquals(HiveSqlHighlighter.COLUMN_STYLE, styleAt(sql, sql.indexOf("a,")));
    assertEquals(HiveSqlHighlighter.COLUMN_STYLE, styleAt(sql, sql.indexOf("b ")));
    assertEquals(HiveSqlHighlighter.TABLE_STYLE, styleAt(sql, sql.indexOf("sales")));

    // both relations in a join are tables
    String j = "FROM t1 JOIN t2";
    assertEquals(HiveSqlHighlighter.TABLE_STYLE, styleAt(j, j.indexOf("t1")));
    assertEquals(HiveSqlHighlighter.TABLE_STYLE, styleAt(j, j.indexOf("t2")));

    // CREATE TABLE name -> table; the column inside the parens -> column
    String c = "CREATE TABLE foo (id INT)";
    assertEquals(HiveSqlHighlighter.TABLE_STYLE, styleAt(c, c.indexOf("foo")));
    assertEquals(HiveSqlHighlighter.COLUMN_STYLE, styleAt(c, c.indexOf("id")));

    // qualifier in alias.column -> alias is table-colored, the field is a column
    String d = "WHERE t.amount > 0";
    assertEquals(HiveSqlHighlighter.TABLE_STYLE, styleAt(d, d.indexOf("t.")));
    assertEquals(HiveSqlHighlighter.COLUMN_STYLE, styleAt(d, d.indexOf("amount")));
  }

  @Test
  public void testMixedStatement() {
    String sql = "SELECT count(*) FROM t1 WHERE x = 1";
    assertEquals(HiveSqlHighlighter.KEYWORD_STYLE, styleAt(sql, sql.indexOf("SELECT")));
    assertEquals(HiveSqlHighlighter.FUNCTION_STYLE, styleAt(sql, sql.indexOf("count")));
    assertEquals(HiveSqlHighlighter.KEYWORD_STYLE, styleAt(sql, sql.indexOf("FROM")));
    assertEquals(HiveSqlHighlighter.TABLE_STYLE, styleAt(sql, sql.indexOf("t1")));
    assertEquals(HiveSqlHighlighter.KEYWORD_STYLE, styleAt(sql, sql.indexOf("WHERE")));
    assertEquals(HiveSqlHighlighter.COLUMN_STYLE, styleAt(sql, sql.indexOf("x ")));
    assertEquals(HiveSqlHighlighter.NUMBER_STYLE, styleAt(sql, sql.length() - 1));
  }

  @Test
  public void testDisabledReturnsPlainText() {
    HiveSqlHighlighter off = new HiveSqlHighlighter(() -> false);
    AttributedString out = off.highlight((LineReader) null, "SELECT * FROM t");
    assertEquals("SELECT * FROM t", out.toString());
    assertEquals(AttributedStyle.DEFAULT, out.styleAt(0));
  }

  @Test
  public void testEnabledThroughPublicApi() {
    AttributedString out = on.highlight((LineReader) null, "SELECT");
    assertEquals(HiveSqlHighlighter.KEYWORD_STYLE, out.styleAt(0));
    assertEquals("SELECT", out.toString());
  }

  @Test
  public void testNullBufferIsSafe() {
    assertEquals("", on.highlight((LineReader) null, null).toString());
  }
}
