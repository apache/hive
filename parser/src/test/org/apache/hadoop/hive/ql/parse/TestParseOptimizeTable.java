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

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class TestParseOptimizeTable {
  ParseDriver parseDriver = new ParseDriver();
  
  @Test
  public void testOptimizeTableWithWhere() throws Exception {

    String expectedWhereFilter = "\n" +
        "nil\n" +
        "   TOK_ALTERTABLE\n" +
        "      TOK_TABNAME\n" +
        "         tbl0\n" +
        "      TOK_ALTERTABLE_COMPACT\n" +
        "         'SMART_OPTIMIZE'\n" +
        "         TOK_WHERE\n" +
        "            and\n" +
        "               TOK_FUNCTION\n" +
        "                  in\n" +
        "                  TOK_TABLE_OR_COL\n" +
        "                     col01\n" +
        "                  'A'\n" +
        "                  'B'\n" +
        "               <\n" +
        "                  TOK_TABLE_OR_COL\n" +
        "                     col02\n" +
        "                  '2024-09-17 00:00:00'\n" +
        "   <EOF>\n";

    ASTNode tree = parseDriver.parse(
        " optimize table tbl0 rewrite data where (col01 in ('A', 'B') and col02 < '2024-09-17 00:00:00')", null).getTree();
    assertThat(tree.dump(), is(expectedWhereFilter));
  }

  @Test
  public void testOptimizeTableWithOrderBy() throws Exception {

    String expectedOrderBy = "\n" +
        "nil\n" +
        "   TOK_ALTERTABLE\n" +
        "      TOK_TABNAME\n" +
        "         tbl0\n" +
        "      TOK_ALTERTABLE_COMPACT\n" +
        "         'SMART_OPTIMIZE'\n" +
        "         TOK_ORDERBY\n" +
        "            TOK_TABSORTCOLNAMEDESC\n" +
        "               TOK_NULLS_FIRST\n" +
        "                  TOK_TABLE_OR_COL\n" +
        "                     col01\n" +
        "   <EOF>\n";
    
    ASTNode tree = parseDriver.parse(
        " optimize table tbl0 rewrite data order by col01 desc", null).getTree();
    assertThat(tree.dump(), is(expectedOrderBy));
  }

  @Test
  public void testOptimizeTableWithPool() throws Exception {

    String expectedWithCompactPool = "\n" +
        "nil\n" +
        "   TOK_ALTERTABLE\n" +
        "      TOK_TABNAME\n" +
        "         tbl0\n" +
        "      TOK_ALTERTABLE_COMPACT\n" +
        "         'SMART_OPTIMIZE'\n" +
        "         TOK_COMPACT_POOL\n" +
        "            'iceberg'\n" +
        "   <EOF>\n";

    ASTNode tree = parseDriver.parse(
        " optimize table tbl0 rewrite data pool 'iceberg'", null).getTree();
    assertThat(tree.dump(), is(expectedWithCompactPool));
  }
}
