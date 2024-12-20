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

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestParseDefault {
  ParseDriver parseDriver = new ParseDriver();

  @Test
  public void testParseDefaultKeywordInInsert() throws Exception {
    ASTNode tree = parseDriver.parse(
            "INSERT INTO TABLE t1 values(DEFAULT, deFaUlt)", null).getTree();

    assertTrue(tree.dump(), tree.toStringTree().contains(
            "(tok_table_or_col tok_default_value) (tok_table_or_col tok_default_value)"));
  }

  @Test
  public void testParseDefaultKeywordInUpdate() throws Exception {
    ASTNode tree = parseDriver.parse(
            "update t1 set b = default", null).getTree();

    assertTrue(tree.dump(), tree.toStringTree().contains(
            "(tok_table_or_col tok_default_value)"));

  }

  @Test
  public void testParseDefaultKeywordInUpdateWithWhere() throws Exception {
    ASTNode tree = parseDriver.parse(
            "update t1 set b = default where a = 10", null).getTree();

    assertTrue(tree.dump(), tree.toStringTree().contains(
            "(tok_table_or_col tok_default_value)"));

  }

  @Test
  public void testParseStructFieldNamedDefaultInSetClause() throws Exception {
    ASTNode tree = parseDriver.parse(
            "update t1 set b = default.field0\n", null).getTree();

    assertFalse(tree.dump(), tree.toStringTree().contains("tok_default_value"));
  }

  @Test
  public void testParseStructFieldNamedDefaultInBeginningOdSetClause() throws Exception {
    ASTNode tree = parseDriver.parse(
            "update t1 set b = default.field0, a = 10\n", null).getTree();

    assertFalse(tree.dump(), tree.toStringTree().contains("tok_default_value"));
  }

  @Test
  public void testParseDefaultKeywordInMerge() throws Exception {
    ASTNode tree = parseDriver.parse(
            "MERGE INTO t1 AS t USING t2 as s ON t.a = s.a\n" +
                    "WHEN MATCHED THEN UPDATE SET b = defauLt " +
                    "WHEN NOT MATCHED THEN INSERT VALUES (s.a, DEFAuLT, DEFAULT)", null).getTree();

    assertEquals(tree.dump(), 3, StringUtils.countMatches(tree.toStringTree(), "(tok_table_or_col tok_default_value)"));
  }

  @Test
  public void testParseStructNamedDefault() throws Exception {
    ASTNode tree = parseDriver.parse(
            "select default.src.`end`.key from s_n1\n", null).getTree();

    assertFalse(tree.dump(), tree.toStringTree().contains("tok_default_value"));
  }

  @Test
  public void testParseStructFieldNamedDefault() throws Exception {
    ASTNode tree = parseDriver.parse(
            "select col0.default.key from s_n1\n", null).getTree();

    assertFalse(tree.dump(), tree.toStringTree().contains("tok_default_value"));
  }

  @Test
  public void testSelectColumNamedDefault() throws Exception {
    ASTNode tree = parseDriver.parse(
            "select default from s_n1\n", null).getTree();

    assertFalse(tree.dump(), tree.toStringTree().contains("tok_default_value"));
  }

  @Test
  public void testCompact() throws Exception {
    ASTNode tree = parseDriver.parse(
            "alter table t1 compact 'minor' pool 'pool'", null).getTree();

    assertTrue(tree.dump(), tree.toStringTree().contains("tok_compact_pool 'pool'"));
    assertTrue(tree.dump(), tree.toStringTree().contains("tok_altertable_compact 'minor'"));
  }

  @Test
  public void testShowCompactions() throws Exception {
    ASTNode tree = parseDriver.parse(
        "show compactions pool 'pool'", null).getTree();
    assertTrue(tree.dump(), tree.toStringTree().contains("tok_compact_pool 'pool'"));
  }

}
