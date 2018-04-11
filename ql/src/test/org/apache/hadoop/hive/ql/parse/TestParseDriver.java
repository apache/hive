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


public class TestParseDriver {
  ParseDriver parseDriver = new ParseDriver();

  @Test
  public void testParse() throws Exception {
    String selectStr = "select field1, field2, sum(field3+field4)";
    String whereStr = "field5=1 and field6 in ('a', 'b')";
    String havingStr = "sum(field7) > 11";
    ASTNode tree = parseDriver.parse(selectStr + " from table1 where " + whereStr
      + " group by field1, field2 having  " + havingStr);
    assertEquals(tree.getType(), 0);
    assertEquals(tree.getChildCount(), 2);
    ASTNode queryTree = (ASTNode) tree.getChild(0);
    assertEquals(tree.getChild(1).getType(), HiveParser.EOF);
    assertEquals(queryTree.getChildCount(), 2);
    ASTNode fromAST = (ASTNode) queryTree.getChild(0);
    ASTNode insertAST = (ASTNode) queryTree.getChild(1);
    assertEquals(fromAST.getType(), HiveParser.TOK_FROM);
    assertEquals(fromAST.getChildCount(), 1);
    assertEquals(fromAST.getChild(0).getType(), HiveParser.TOK_TABREF);
    assertEquals(fromAST.getChild(0).getChildCount(), 1);
    assertEquals(fromAST.getChild(0).getChild(0).getType(), HiveParser.TOK_TABNAME);
    assertEquals(fromAST.getChild(0).getChild(0).getChildCount(), 1);
    assertEquals(fromAST.getChild(0).getChild(0).getChild(0).getType(), HiveParser.Identifier);
    assertEquals(fromAST.getChild(0).getChild(0).getChild(0).getText(), "table1");
    assertEquals(insertAST.getChildCount(), 5);
    assertEquals(insertAST.getChild(0).getType(), HiveParser.TOK_DESTINATION);
    assertTree((ASTNode) insertAST.getChild(1), parseDriver.parseSelect(selectStr, null));
    assertEquals(insertAST.getChild(2).getType(), HiveParser.TOK_WHERE);
    assertTree((ASTNode) insertAST.getChild(2).getChild(0), parseDriver.parseExpression(whereStr));
    assertEquals(insertAST.getChild(3).getType(), HiveParser.TOK_GROUPBY);
    assertEquals(insertAST.getChild(3).getChildCount(), 2);
    for (int i = 0; i < 2; i++) {
      assertEquals(insertAST.getChild(3).getChild(i).getType(), HiveParser.TOK_TABLE_OR_COL);
      assertEquals(insertAST.getChild(3).getChild(i).getChild(0).getType(), HiveParser.Identifier);
      assertEquals(insertAST.getChild(3).getChild(i).getChild(0).getText(), "field" + (i + 1));
    }
    assertEquals(insertAST.getChild(4).getType(), HiveParser.TOK_HAVING);
    assertEquals(insertAST.getChild(4).getChildCount(), 1);
    assertTree((ASTNode) insertAST.getChild(4).getChild(0), parseDriver.parseExpression(havingStr));
  }

  @Test
  public void testParseSelect() throws Exception {
    ASTNode tree = parseDriver.parseSelect("select field1, field2, sum(field3+field4)", null);
    assertEquals(tree.getType(), HiveParser.TOK_SELECT);
    assertEquals(tree.getChildCount(), 3);
    for (int i = 0; i < 3; i++) {
      assertEquals(((ASTNode) tree.getChild(i)).getType(), HiveParser.TOK_SELEXPR);
    }
    assertTree((ASTNode) tree.getChild(0).getChild(0), parseDriver.parseExpression("field1"));
    assertTree((ASTNode) tree.getChild(1).getChild(0), parseDriver.parseExpression("field2"));
    assertTree((ASTNode) tree.getChild(2).getChild(0), parseDriver.parseExpression("sum(field3+field4)"));
  }

  @Test
  public void testParseExpression() throws Exception {
    ASTNode plusNode = parseDriver.parseExpression("field3 + field4");
    assertEquals(plusNode.getType(), HiveParser.PLUS);
    assertEquals(plusNode.getChildCount(), 2);
    for (int i = 0; i < 2; i++) {
      assertEquals(plusNode.getChild(i).getType(), HiveParser.TOK_TABLE_OR_COL);
      assertEquals(plusNode.getChild(i).getChildCount(), 1);
      assertEquals(plusNode.getChild(i).getChild(0).getType(), HiveParser.Identifier);
      assertEquals(plusNode.getChild(i).getChild(0).getText(), "field" + (i + 3));
    }

    ASTNode sumNode = parseDriver.parseExpression("sum(field3 + field4)");
    assertEquals(sumNode.getType(), HiveParser.TOK_FUNCTION);
    assertEquals(sumNode.getChildCount(), 2);
    assertEquals(sumNode.getChild(0).getType(), HiveParser.Identifier);
    assertEquals(sumNode.getChild(0).getText(), "sum");
    assertTree((ASTNode) sumNode.getChild(1), plusNode);

    ASTNode tree = parseDriver.parseExpression("case when field1 = 1 then sum(field3 + field4) when field1 != 2 then " +
      "sum(field3-field4) else sum(field3 * field4) end");
    assertEquals(tree.getChildCount(), 6);
    assertEquals(tree.getChild(0).getType(), HiveParser.KW_WHEN);
    assertEquals(tree.getChild(1).getType(), HiveParser.EQUAL);
    assertTree((ASTNode) tree.getChild(2), sumNode);
    assertEquals(tree.getChild(3).getType(), HiveParser.NOTEQUAL);
    assertTree((ASTNode) tree.getChild(4), parseDriver.parseExpression("sum(field3-field4)"));
    assertTree((ASTNode) tree.getChild(5), parseDriver.parseExpression("sum(field3*field4)"));
  }

  private void assertTree(ASTNode astNode1, ASTNode astNode2) {
    assertEquals(astNode1.getType(), astNode2.getType());
    assertEquals(astNode1.getChildCount(), astNode2.getChildCount());
    for (int i = 0; i < astNode1.getChildCount(); i++) {
      assertTree((ASTNode) astNode1.getChild(i), (ASTNode) astNode2.getChild(i));
    }
  }
}