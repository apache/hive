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
        "APPLICATION",
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
        "COMPACTIONID",
        "CONF",
        "CONNECTOR",
        "CONNECTORS",
        "CONSTRAINT",
        "CONVERT",
        "CREATE",
        "CROSS",
        "CUBE",
        "CURRENT",
        "CURRENT_DATE",
        "CURRENT_TIMESTAMP",
        "CURSOR",
        "DATABASE",
        "DATE",
        "DDL",
        "DECIMAL",
        "DELETE",
        "DESCRIBE",
        "DISTINCT",
        "DOUBLE",
        "DROP",
        "ELSE",
        "END",
        "EXCEPT",
        "EXCHANGE",
        "EXISTS",
        "EXTENDED",
        "EXTERNAL",
        "EXTRACT",
        "FALSE",
        "FETCH",
        "FLOAT",
        "FLOOR",
        "FOLLOWING",
        "FOR",
        "FORCE",
        "FOREIGN",
        "FROM",
        "FULL",
        "FUNCTION",
        "GRANT",
        "GROUP",
        "GROUPING",
        "HAVING",
        "IF",
        "IMPORT",
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
        "LESS",
        "LIKE",
        "LOCAL",
        "MACRO",
        "MAP",
        "MERGE",
        "MINUS",
        "MORE",
        "NONE",
        "NOT",
        "NULL",
        "OF",
        "OLDER",
        "ON",
        "ONLY",
        "OR",
        "ORDER",
        "OUT",
        "OUTER",
        "OVER",
        "PARTITION",
        "PERCENT",
        "PKFK_JOIN",
        "PRECEDING",
        "PRECISION",
        "PREPARE",
        "PRESERVE",
        "PRIMARY",
        "PROCEDURE",
        "QUALIFY",
        "RANGE",
        "READS",
        "REAL",
        "REDUCE",
        "REFERENCES",
        "REGEXP",
        "REVOKE",
        "RIGHT",
        "RLIKE",
        "ROLLBACK",
        "ROLLUP",
        "ROW",
        "ROWS",
        "SELECT",
        "SET",
        "SMALLINT",
        "SOME",
        "START",
        "SYNC",
        "TABLE",
        "TABLESAMPLE",
        "THAN",
        "THEN",
        "TIME",
        "TIMESTAMP",
        "TIMESTAMPLOCALTZ",
        "TO",
        "TRAILING",
        "TRANSFORM",
        "TRIGGER",
        "TRUE",
        "TRUNCATE",
        "UNBOUNDED",
        "UNION",
        "UNIQUE",
        "UNIQUEJOIN",
        "UPDATE",
        "USER",
        "USING",
        "VALUES",
        "VARCHAR",
        "WHEN",
        "WHERE",
        "WINDOW",
        "WITH"
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
