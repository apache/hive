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

public class TestParseShowCompactions {
    ParseDriver parseDriver = new ParseDriver();

    @Test
    public void testShowCompactions() throws Exception {
        ASTNode tree = parseDriver.parse(
                "SHOW COMPACTIONS", null).getTree();

        assertThat(tree.toStringTree(), is("tok_show_compactions <eof>"));
    }

    @Test
    public void testShowCompactionsFilterDb() throws Exception {
        ASTNode tree = parseDriver.parse(
                "SHOW COMPACTIONS DATABASE db1", null).getTree();

        assertThat(tree.toStringTree(), is("(tok_show_compactions db1) <eof>"));
    }

    private static final String EXPECTED_WHEN_FILTER_BY_DB_AND_ALL_AND_ORDER_BY = "\n" +
            "nil\n" +
            "   TOK_SHOW_COMPACTIONS\n" +
            "      db1\n" +
            "      TOK_COMPACT_POOL\n" +
            "         'pool0'\n" +
            "      TOK_COMPACTION_TYPE\n" +
            "         'minor'\n" +
            "      TOK_COMPACTION_STATUS\n" +
            "         'ready for clean'\n" +
            "      TOK_ORDERBY\n" +
            "         TOK_TABSORTCOLNAMEDESC\n" +
            "            TOK_NULLS_FIRST\n" +
            "               TOK_TABLE_OR_COL\n" +
            "                  cq_table\n" +
            "         TOK_TABSORTCOLNAMEASC\n" +
            "            TOK_NULLS_LAST\n" +
            "               TOK_TABLE_OR_COL\n" +
            "                  cq_state\n" +
            "      TOK_LIMIT\n" +
            "         42\n" +
            "   <EOF>\n";

    @Test
    public void testShowCompactionsFilterDbAndAllAndOrder() throws Exception {
        ASTNode tree = parseDriver.parse(
                "SHOW COMPACTIONS DATABASE db1 POOL 'pool0' TYPE 'minor' STATUS 'ready for clean' ORDER BY cq_table DESC, cq_state LIMIT 42", null).getTree();

        assertThat(tree.dump(), is(EXPECTED_WHEN_FILTER_BY_DB_AND_ALL_AND_ORDER_BY));
    }

    @Test
    public void testShowCompactionsFilterSchema() throws Exception {
        ASTNode tree = parseDriver.parse(
                "SHOW COMPACTIONS SCHEMA db1", null).getTree();

        assertThat(tree.toStringTree(), is("(tok_show_compactions db1) <eof>"));
    }

    @Test
    public void testShowCompactionsFilterTable() throws Exception {
        ASTNode tree = parseDriver.parse(
                "SHOW COMPACTIONS tbl0", null).getTree();

        assertThat(tree.toStringTree(), is("(tok_show_compactions (tok_tabtype tbl0)) <eof>"));
    }

    @Test
    public void testShowCompactionsFilterID() throws Exception {
        ASTNode tree = parseDriver.parse(
                "SHOW COMPACTIONS compactionid =1", null).getTree();

        assertThat(tree.toStringTree(), is("(tok_show_compactions (tok_compact_id 1)) <eof>"));
    }
    @Test
    public void testShowCompactionsFilterQualifiedTable() throws Exception {
        ASTNode tree = parseDriver.parse(
                "SHOW COMPACTIONS db1.tbl0", null).getTree();

        assertThat(tree.toStringTree(), is("(tok_show_compactions (tok_tabtype (. db1 tbl0))) <eof>"));
    }

    @Test
    public void testShowCompactionsFilterPool() throws Exception {
        ASTNode tree = parseDriver.parse(
                "SHOW COMPACTIONS POOL 'pool0'", null).getTree();

        assertThat(tree.toStringTree(), is("(tok_show_compactions (tok_compact_pool 'pool0')) <eof>"));
    }


    private static final String EXPECTED_WHEN_FILTER_BY_TABLE_AND_ALL_AND_ORDER_BY = "\n" +
            "nil\n" +
            "   TOK_SHOW_COMPACTIONS\n" +
            "      TOK_TABTYPE\n" +
            "         .\n" +
            "            db1\n" +
            "            tbl0\n" +
            "         TOK_PARTSPEC\n" +
            "            TOK_PARTVAL\n" +
            "               p\n" +
            "               101\n" +
            "            TOK_PARTVAL\n" +
            "               day\n" +
            "               'Monday'\n" +
            "      TOK_COMPACT_POOL\n" +
            "         'pool0'\n" +
            "      TOK_COMPACTION_TYPE\n" +
            "         'minor'\n" +
            "      TOK_COMPACTION_STATUS\n" +
            "         'ready for clean'\n" +
            "      TOK_ORDERBY\n" +
            "         TOK_TABSORTCOLNAMEDESC\n" +
            "            TOK_NULLS_FIRST\n" +
            "               TOK_TABLE_OR_COL\n" +
            "                  cq_table\n" +
            "         TOK_TABSORTCOLNAMEASC\n" +
            "            TOK_NULLS_LAST\n" +
            "               TOK_TABLE_OR_COL\n" +
            "                  cq_state\n" +
            "      TOK_LIMIT\n" +
            "         42\n" +
            "   <EOF>\n";

    @Test
    public void testShowCompactionsFilterTableAndAllAndOrderBy() throws Exception {
        ASTNode tree = parseDriver.parse(
                "SHOW COMPACTIONS db1.tbl0 PARTITION (p=101,day='Monday') POOL 'pool0' TYPE 'minor' STATUS 'ready for clean' ORDER BY cq_table DESC, cq_state LIMIT 42", null).getTree();

        assertThat(tree.dump(), is(EXPECTED_WHEN_FILTER_BY_TABLE_AND_ALL_AND_ORDER_BY));
    }
}