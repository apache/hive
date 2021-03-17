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

import static org.junit.Assert.assertEquals;

/**
 * Test cases for parse WITHIN GROUP clause syntax.
 * function(expression) WITHIN GROUP (ORDER BY sort_expression)
 */
public class TestNullTreatment {
  ParseDriver parseDriver = new ParseDriver();

  @Test
  public void testParseRespectNulls() throws Exception {
    ASTNode tree = parseDriver.parse(
            "select last_value(b) respect nulls over(partition by a) from t1", null).getTree();

    Assert.assertEquals(String.format(EXPECTED_RESULT_BASE, "TOK_RESPECT_NULLS"),
        ((ASTNode)tree.getChild(0)).dump());
  }

  @Test
  public void testParseRespectNullsInFunctionParameterList() throws Exception {
    ASTNode tree = parseDriver.parse(
            "select last_value(b respect nulls) over(partition by a) from t1", null).getTree();

    Assert.assertEquals(String.format(EXPECTED_RESULT_BASE, "TOK_RESPECT_NULLS"),
        ((ASTNode)tree.getChild(0)).dump());
  }

  @Test
  public void testParseIgnoreNulls() throws Exception {
    ASTNode tree = parseDriver.parse(
            "select last_value(b) ignore nulls over(partition by a) from t1", null).getTree();

    Assert.assertEquals(String.format(EXPECTED_RESULT_BASE, "TOK_IGNORE_NULLS"),
            ((ASTNode)tree.getChild(0)).dump());
  }

  @Test
  public void testParseIgnoreNullsInFunctionParameterList() throws Exception {
    ASTNode tree = parseDriver.parse(
        "select last_value(b ignore nulls) over(partition by a) from t1", null).getTree();

    Assert.assertEquals(String.format(EXPECTED_RESULT_BASE, "TOK_IGNORE_NULLS"),
        ((ASTNode)tree.getChild(0)).dump());
  }

  private final static String EXPECTED_RESULT_BASE = "\n" +
          "TOK_QUERY\n" +
          "   TOK_FROM\n" +
          "      TOK_TABREF\n" +
          "         TOK_TABNAME\n" +
          "            t1\n" +
          "   TOK_INSERT\n" +
          "      TOK_DESTINATION\n" +
          "         TOK_DIR\n" +
          "            TOK_TMP_FILE\n" +
          "      TOK_SELECT\n" +
          "         TOK_SELEXPR\n" +
          "            TOK_FUNCTION\n" +
          "               last_value\n" +
          "               TOK_TABLE_OR_COL\n" +
          "                  b\n" +
          "               TOK_WINDOWSPEC\n" +
          "                  TOK_PARTITIONINGSPEC\n" +
          "                     TOK_DISTRIBUTEBY\n" +
          "                        TOK_TABLE_OR_COL\n" +
          "                           a\n" +
          "                  %s\n";
}
