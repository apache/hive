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

import java.util.Arrays;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class TestReservedWords {
  @Parameters(name = "{0}")
  public static Collection<String> data() {
    return Arrays.asList(
        "ALL",
        "ALTER",
        "AND",
        "ANY",
        "ARRAY",
        "AS",
        "AUTHORIZATION",
        "BETWEEN",
        "BIGINT",
        "BINARY",
        "BOOLEAN",
        "BOTH",
        "BY",
        "CASE",
        "CAST",
        "CHAR",
        "COLUMN",
        "COMMIT",
        "CONF", // Hive specific reserved keyword as of 1.2.0
        "CONSTRAINT",
        "CONVERT",
        "CREATE",
        "CROSS",
        "CUBE",
        "CURRENT",
        "CURRENT_DATE",
        "CURRENT_TIMESTAMP",
        "CURSOR",
        "DATABASE", // Hive specific reserved keyword as of 1.2.0
        "DATE",
        "DECIMAL",
        "DELETE",
        "DESCRIBE",
        "DISTINCT",
        "DOUBLE",
        "DROP",
        "ELSE",
        "END",
        "EXCEPT",
        "EXCHANGE", // Hive specific reserved keyword as of 1.2.0
        "EXISTS",
        "EXTENDED", // Hive specific reserved keyword as of 1.2.0
        "EXTERNAL",
        "EXTRACT",
        "FALSE",
        "FETCH",
        "FLOAT",
        "FLOOR",
        "FOLLOWING", // Hive specific reserved keyword as of 1.2.0
        "FOR",
        "FOREIGN",
        "FROM",
        "FULL",
        "FUNCTION",
        "GRANT",
        "GROUP",
        "GROUPING",
        "HAVING",
        "IMPORT",
        "IF", // Hive specific reserved keyword as of 1.2.0
        "IN",
        "INNER",
        "INSERT",
        "INT",
        "INTERSECT",
        "INTERVAL",
        "INTO",
        "IS",
        "JOIN",
        "LATERAL",
        "LEADING",
        "LEFT",
        "LESS", // Hive specific reserved keyword as of 1.2.0
        "LIKE",
        "LOCAL",
        "MACRO", // Hive specific reserved keyword as of 1.2.0
        "MAP", // Hive specific reserved keyword as of 1.2.0
        "MERGE",
        "MINUS", // HIVE-12765: Hive specific reserved keyword since 2.2.0
        "MORE", // Hive specific reserved keyword as of 1.2.0
        "NONE",
        "NOT",
        "NULL",
        "OF",
        "ON",
        "ONLY",
        "OR",
        "ORDER",
        "OUT",
        "OUTER",
        "OVER",
        "PARTITION",
        "PERCENT",
        "PRECEDING", // Hive specific reserved keyword as of 1.2.0
        "PRECISION",
        "PREPARE",
        "PRESERVE",
        "PRIMARY",
        "PROCEDURE",
        "QUALIFY", // HIVE-25589: Not a part of the ANSI standard, but it should be reserved
        "RANGE",
        "READS",
        "REAL",
        "REDUCE", // Hive specific reserved keyword as of 1.2.0
        "REFERENCES",
        "REGEXP", // HIVE-11600: Hive specific reserved keyword since 2.0.0
        "REVOKE",
        "RIGHT",
        "RLIKE", // HIVE-11600: Hive specific reserved keyword since 2.0.0
        "ROLLBACK",
        "ROLLUP",
        "ROW",
        "ROWS",
        "SELECT",
        "SET",
        "SMALLINT",
        "SOME",
        "START",
        "SYNC", // HIVE-17824: Hive specific reserved keyword since 3.0.0
        "TABLE",
        "TABLESAMPLE",
        "THEN",
        "TIME",
        "TIMESTAMP",
        "TO",
        "TRAILING",
        "TRANSFORM", // Hive specific reserved keyword as of 1.2.0
        "TRIGGER",
        "TRUE",
        "TRUNCATE",
        "UNBOUNDED", // Hive specific reserved keyword as of 1.2.0
        "UNION",
        "UNIQUE",
        "UNIQUEJOIN", // Hive specific reserved keyword as of 1.2.0
        "UPDATE",
        "USER",
        "USING",
        "VALUES",
        "VARCHAR",
        "WHEN",
        "WHERE",
        "WINDOW",
        "WITH",
        "VARIANT",
        "TIMESTAMP_NS",
        "TIMESTAMPTZ_NS",
        "NANOSECOND"
    );
  }

  private static final Configuration conf = new Configuration();
  private static final ParseDriver pd = new ParseDriver();

  private final String keyword;

  public TestReservedWords(String keyword) {
    this.keyword = keyword;
  }

  @Test
  public void testReservedWords() {
    try {
      String query = String.format("CREATE TABLE %s (col STRING)", keyword);
      pd.parse(query, conf);
      Assert.fail("Expected ParseException");
    } catch (ParseException e) {
      if (keyword.equals("IF")) {
        String expected = "line 1:16 mismatched input '(' expecting NOT near 'IF' in if not exists clause";
        Assert.assertEquals("Failure didn't match.", expected, e.getMessage());
        return;
      }
      String expected = String.format("line 1:13 cannot recognize input near '%s' '(' 'col' in table name", keyword);
      Assert.assertEquals("Failure didn't match.", expected, e.getMessage());
    }
  }
}
