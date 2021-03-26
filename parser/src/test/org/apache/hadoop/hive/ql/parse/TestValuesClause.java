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

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test cases for parse WITHIN GROUP clause syntax.
 * function(expression) WITHIN GROUP (ORDER BY sort_expression)
 */
public class TestValuesClause {
  ParseDriver parseDriver = new ParseDriver();

  @Test
  public void testParseSelect() throws Exception {
    ASTNode tree = parseDriver.parse(
            "select 1 a, 2 as b, 3", null).getTree();

    System.out.println(tree.dump());
  }

  @Test
  public void testParseInsertInto() throws Exception {
    ASTNode tree = parseDriver.parse(
            "insert into t1(a,b,c) values (1,2,3),(4,5,6)", null).getTree();

    System.out.println(tree.dump());
  }
  @Test
  public void testParseInsert() throws Exception {
    ASTNode tree = parseDriver.parse(
            "insert into t1 select 1 a, 2 b, 3", null).getTree();

    System.out.println(tree.dump());
  }

  @Test
  public void testParseValues() throws Exception {
    ASTNode tree = parseDriver.parse(
            "VALUES(1,2,3),(4,5,6)", null).getTree();

    ASTNode queryNode = (ASTNode) tree.getChild(0);
    Assert.assertEquals(EXPECTED_VALUES_CLAUSE_TREE, queryNode.dump());
  }

  @Test
  public void testParseValuesAsSubQuery() throws Exception {
    ASTNode tree = parseDriver.parse("SELECT * FROM (VALUES(1,2,3),(4,5,6)) as foo", null).getTree();

    ASTNode queryNode = (ASTNode) tree.getChild(0);
    ASTNode fromNode = (ASTNode) queryNode.getChild(0);
    ASTNode subQueryNode = (ASTNode) fromNode.getChild(0);
    ASTNode subQueryQueryNode = (ASTNode) subQueryNode.getChild(0);

    assertEquals(EXPECTED_VALUES_CLAUSE_TREE, subQueryQueryNode.dump());
  }

  @Test
  public void testParseValuesAsSubQueryWhenJoined() throws Exception {
    ASTNode tree = parseDriver.parse("SELECT * FROM (VALUES(1,2,3),(4,5,6)) as foo\n" +
            "JOIN (VALUES(1,'a'),(4,'b')) as bar ON foo.col1 = bar.col1", null).getTree();

    ASTNode queryNode = (ASTNode) tree.getChild(0);
    ASTNode fromNode = (ASTNode) queryNode.getChild(0);
    ASTNode joinNode = (ASTNode) fromNode.getChild(0);
    ASTNode subQueryNode0 = (ASTNode) joinNode.getChild(0);
    ASTNode subQueryQueryNode = (ASTNode) subQueryNode0.getChild(0);

    assertEquals(EXPECTED_VALUES_CLAUSE_TREE, subQueryQueryNode.dump());
  }

  public static final String EXPECTED_VALUES_CLAUSE_TREE = "\n" +
          "TOK_QUERY\n" +
          "   TOK_INSERT\n" +
          "      TOK_DESTINATION\n" +
          "         TOK_DIR\n" +
          "            TOK_TMP_FILE\n" +
          "      TOK_SELECT\n" +
          "         TOK_SELEXPR\n" +
          "            TOK_FUNCTION\n" +
          "               inline\n" +
          "               TOK_FUNCTION\n" +
          "                  array\n" +
          "                  TOK_FUNCTION\n" +
          "                     struct\n" +
          "                     1\n" +
          "                     2\n" +
          "                     3\n" +
          "                  TOK_FUNCTION\n" +
          "                     struct\n" +
          "                     4\n" +
          "                     5\n" +
          "                     6\n";

  @Test
  public void testParseValuesInUnion() throws Exception {
    parseDriver.parse("values(1,2,3),(4,5,6)\n" +
            "union all\n" +
            "values(1,2,3),(4,5,6)", null);
  }

  @Test
  public void testParseInsertValues() throws Exception {
    parseDriver.parse("INSERT INTO t1(a, b) VALUES (1,2),(3,4)", null);
  }

  @Test
  public void testParseInsertFromValuesAsSubQuery() throws Exception {
    parseDriver.parse("insert into table FOO select a,b from (values(1,2),(3,4)) as BAR", null);
  }

  @Test
  public void testParseValuesWithOneCol() throws Exception {
    ASTNode tree = parseDriver.parse(
            "VALUES(1),(4)", null).getTree();

    assertArrayOfStructsEquals("\n" +
            "TOK_FUNCTION\n" +
            "   array\n" +
            "   TOK_FUNCTION\n" +
            "      struct\n" +
            "      1\n" +
            "   TOK_FUNCTION\n" +
            "      struct\n" +
            "      4\n", tree);
  }

  @Test
  public void testParseValuesWithOneColAndAlias() throws Exception {
    ASTNode tree = parseDriver.parse(
            "VALUES(1 a),(4)", null).getTree();

    assertArrayOfStructsEquals("\n" +
            "TOK_FUNCTION\n" +
            "   array\n" +
            "   TOK_FUNCTION\n" +
            "      struct\n" +
            "      1\n" +
            "      TOK_ALIAS\n" +
            "         a\n" +
            "   TOK_FUNCTION\n" +
            "      struct\n" +
            "      4\n", tree);
  }

  @Test
  public void testParseValuesWithMultipleColumnsAndFirstColHasNoAliases() throws Exception {
    ASTNode tree = parseDriver.parse(
            "VALUES(1, 2 b, 3),(4, 5, 6)", null).getTree();

    assertArrayOfStructsEquals("\n" +
            "TOK_FUNCTION\n" +
            "   array\n" +
            "   TOK_FUNCTION\n" +
            "      struct\n" +
            "      1\n" +
            "      TOK_ALIAS\n" +
            "         col1\n" +
            "      2\n" +
            "      TOK_ALIAS\n" +
            "         b\n" +
            "      3\n" +
            "      TOK_ALIAS\n" +
            "         col3\n" +
            "   TOK_FUNCTION\n" +
            "      struct\n" +
            "      4\n" +
            "      5\n" +
            "      6\n", tree);
  }

  @Test
  public void testParseValuesWithMultipleColumnsAndFirstColHasAlias() throws Exception {
    ASTNode tree = parseDriver.parse(
            "VALUES(1 a, 2 b, 3),(4, 5, 6)", null).getTree();

    assertArrayOfStructsEquals("\n" +
            "TOK_FUNCTION\n" +
            "   array\n" +
            "   TOK_FUNCTION\n" +
            "      struct\n" +
            "      1\n" +
            "      TOK_ALIAS\n" +
            "         a\n" +
            "      2\n" +
            "      TOK_ALIAS\n" +
            "         b\n" +
            "      3\n" +
            "      TOK_ALIAS\n" +
            "         col3\n" +
            "   TOK_FUNCTION\n" +
            "      struct\n" +
            "      4\n" +
            "      5\n" +
            "      6\n", tree);
  }

  private void assertArrayOfStructsEquals(String expected, ASTNode tree) {
    ASTNode queryNode = (ASTNode) tree.getChild(0);
    ASTNode insertNode = (ASTNode) queryNode.getChild(0);
    ASTNode selectNode = (ASTNode) insertNode.getChild(1);
    ASTNode selectExprNode = (ASTNode) selectNode.getChild(0);
    ASTNode inlineFuncNode = (ASTNode) selectExprNode.getChild(0);
    ASTNode arrayFuncNode = (ASTNode) inlineFuncNode.getChild(1);

    assertEquals(expected, arrayFuncNode.dump());
  }

}