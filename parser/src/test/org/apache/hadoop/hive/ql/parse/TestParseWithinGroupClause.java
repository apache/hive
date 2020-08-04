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

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Test cases for parse WITHIN GROUP clause syntax.
 * function(expression) WITHIN GROUP (ORDER BY sort_expression)
 */
public class TestParseWithinGroupClause {
  ParseDriver parseDriver = new ParseDriver();

  @Test
  public void testParsePercentileCont() throws Exception {
    ASTNode tree = parseDriver.parseSelect(
        "SELECT percentile_cont(0.4) WITHIN GROUP (ORDER BY val) FROM src", null).getTree();

    assertEquals(1, tree.getChildCount());
    ASTNode selExprNode = (ASTNode) tree.getChild(0);
    assertEquals(1, selExprNode.getChildCount());
    ASTNode functionNode = (ASTNode) selExprNode.getChild(0);
    assertEquals(HiveParser.TOK_FUNCTION, functionNode.getType());
    assertEquals(3, functionNode.getChildCount());

    ASTNode functionNameNode = (ASTNode) functionNode.getChild(0);
    assertEquals(HiveParser.Identifier, functionNameNode.getType());
    assertEquals("percentile_cont", functionNameNode.getText());

    ASTNode fractionNode = (ASTNode) functionNode.getChild(1);
    assertEquals(HiveParser.Number, fractionNode.getType());
    assertEquals("0.4", fractionNode.getText());

    ASTNode withinGroupNode = (ASTNode) functionNode.getChild(2);
    assertEquals(HiveParser.TOK_WITHIN_GROUP, withinGroupNode.getType());

    ASTNode orderByNode = (ASTNode) withinGroupNode.getChild(0);
    assertEquals(HiveParser.TOK_ORDERBY, orderByNode.getType());

    ASTNode tabSortColNameNode = (ASTNode) orderByNode.getChild(0);
    assertEquals(HiveParser.TOK_TABSORTCOLNAMEASC, tabSortColNameNode.getType());
  }

  @Test
  public void testParseMultipleColumnRefs() throws Exception {
    ASTNode tree = parseDriver.parseSelect(
            "SELECT rank(3, 4) WITHIN GROUP (ORDER BY val, val2) FROM src", null).getTree();
    ASTNode selExprNode = (ASTNode) tree.getChild(0);
    ASTNode functionNode = (ASTNode) selExprNode.getChild(0);
    ASTNode withinGroupNode = (ASTNode) functionNode.getChild(3);
    ASTNode orderByNode = (ASTNode) withinGroupNode.getChild(0);
    assertEquals(2, orderByNode.getChildCount());
  }
}
