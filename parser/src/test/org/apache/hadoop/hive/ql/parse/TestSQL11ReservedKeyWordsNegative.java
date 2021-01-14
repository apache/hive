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
import org.junit.experimental.runners.Enclosed;

/**
 * Parser tests for SQL11 Reserved KeyWords. Please find more information in HIVE-6617.
 */
@RunWith(Enclosed.class)
public class TestSQL11ReservedKeyWordsNegative {
  private static Configuration conf = new Configuration();
  private static ParseDriver pd = new ParseDriver();

  private static ASTNode parse(String query) throws ParseException {
    ASTNode nd = null;
    nd = pd.parse(query, conf).getTree();
    return (ASTNode) nd.getChild(0);
  }

  public static class TestSQL11ReservedKeyWordsNegativeMisc {

    @Test
    public void testSQL11ReservedKeyWords_KILL() {
      try {
        parse("CREATE TABLE KILL QUERY (col STRING)");
        Assert.fail("Expected ParseException");
      } catch (ParseException ex) {
        Assert.assertEquals("Failure didn't match.",
            "line 1:18 cannot recognize input near 'QUERY' '(' 'col' in create table statement",
            ex.getMessage());
      }
    }
  }

  @RunWith(Parameterized.class)
  public static class TestSQL11ReservedKeyWordsNegativeParametrized {

    @Parameters(name = "{0}")
    public static Collection<String[]> data() {
      return Arrays.asList(new String[][] { { "ALL" }, { "ALTER" }, { "ARRAY" }, { "AS" },
          { "AUTHORIZATION" }, { "BETWEEN" }, { "BIGINT" }, { "BINARY" }, { "BOOLEAN" }, { "BOTH" },
          { "BY" }, { "CONSTRAINT" }, { "CREATE" }, { "CUBE" }, { "CURRENT_DATE" },
          { "CURRENT_TIMESTAMP" }, { "CURSOR" }, { "DATE" }, { "DECIMAL" }, { "DELETE" },
          { "DESCRIBE" }, { "DOUBLE" }, { "DROP" }, { "EXISTS" }, { "EXTERNAL" }, { "FALSE" },
          { "FETCH" }, { "FLOAT" }, { "REAL" }, { "FOR" }, { "FOREIGN" }, { "FULL" }, { "GRANT" },
          { "GROUP" }, { "GROUPING" }, { "IMPORT" }, { "IN" }, { "INNER" }, { "INSERT" }, { "INT" },
          { "INTERSECT" }, { "INTO" }, { "IS" }, { "LATERAL" }, { "LEFT" }, { "LIKE" }, { "LOCAL" },
          { "MINUS" }, { "NONE" }, { "NULL" }, { "OF" }, { "ORDER" }, { "OUT" }, { "OUTER" },
          { "PARTITION" }, { "PERCENT" }, { "PRECISION" }, { "PRIMARY" }, { "PROCEDURE" },
          { "RANGE" }, { "READS" }, { "REFERENCES" }, { "REGEXP" }, { "REVOKE" }, { "RIGHT" },
          { "RLIKE" }, { "ROLLUP" }, { "ROW" }, { "ROWS" }, { "SET" }, { "SMALLINT" }, { "TABLE" },
          { "TIME" }, { "TIMESTAMP" }, { "TO" }, { "TRIGGER" }, { "TRUE" }, { "TRUNCATE" },
          { "UNION" }, { "UNIQUE" }, { "UPDATE" }, { "USER" }, { "USING" },
          { "VALUES" }, { "WITH" }, { "SOME" }, { "ANY" }, { "ALL" }, {"PREPARE"},
          {"LEADING"}, {"TRAILING"} });
    }

    private String keyword;

    public TestSQL11ReservedKeyWordsNegativeParametrized(String keyword) {
      this.keyword = keyword;
    }

    @Test
    public void testNegative() {
      try {
        parse(String.format("CREATE TABLE %s (col STRING)", keyword));
        Assert.fail("Expected ParseException");
      } catch (ParseException ex) {
        Assert.assertEquals("Failure didn't match.", String
            .format("line 1:13 cannot recognize input near '%s' '(' 'col' in table name", keyword),
            ex.getMessage());
      }
    }
  }
}
