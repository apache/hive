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
public class TestParseTrim {
  ParseDriver parseDriver = new ParseDriver();

//  TOK_SELECT
//    TOK_SELEXPR
//      TOK_FUNCTION
//        trim
//        'foo  bar'
//        'f'
  @Test
  public void testParseTrim() throws Exception {
    ASTNode tree = parseDriver.parseSelect(
        "select trim('f' from 'foo  bar')", null).getTree();

    assertTrimFunction(tree, "trim", "'foo  bar'", "'f'");
  }

  @Test
  public void testParseLTrim() throws Exception {
    ASTNode tree = parseDriver.parseSelect(
        "select trim(leading 'fo' from 'foo  bar')", null).getTree();

    assertTrimFunction(tree, "ltrim", "'foo  bar'", "'fo'");
  }

  @Test
  public void testParseRTrim() throws Exception {
    ASTNode tree = parseDriver.parseSelect(
        "select trim(trailing 'fo' from 'foo  bar')", null).getTree();

    assertTrimFunction(tree, "rtrim", "'foo  bar'", "'fo'");
  }

  @Test
  public void testParseTrimBoth() throws Exception {
    ASTNode tree = parseDriver.parseSelect(
        "select trim(both 'rfo' from 'foo  bar')", null).getTree();

    assertTrimFunction(tree, "trim", "'foo  bar'", "'rfo'");
  }

  @Test
  public void testParseTrimWithOneParameter() throws Exception {
    ASTNode tree = parseDriver.parseSelect(
            "select trim('  foo  bar ')", null).getTree();

    assertTrimFunction(tree, "trim", "'  foo  bar '");
  }

  private void assertTrimFunction(
          ASTNode tree, String expectedFunctionName, String expectedStringToTrim) {
    assertTrimFunction(tree, expectedFunctionName, expectedStringToTrim, 2);
  }

  private void assertTrimFunction(
          ASTNode tree, String expectedFunctionName, String expectedStringToTrim, String expectedTrimChars) {
    ASTNode functionNode = assertTrimFunction(tree, expectedFunctionName, expectedStringToTrim, 3);

    ASTNode trimCharacters = (ASTNode) functionNode.getChild(2);
    assertEquals(HiveParser.StringLiteral, trimCharacters.getType());
    assertEquals(expectedTrimChars, trimCharacters.getText());
  }

  private ASTNode assertTrimFunction(
          ASTNode tree, String expectedFunctionName, String expectedStringToTrim, int functionNodeChildCount) {
    assertEquals(1, tree.getChildCount());
    ASTNode selExprNode = (ASTNode) tree.getChild(0);
    assertEquals(1, selExprNode.getChildCount());
    ASTNode functionNode = (ASTNode) selExprNode.getChild(0);
    assertEquals(HiveParser.TOK_FUNCTION, functionNode.getType());
    assertEquals(functionNodeChildCount, functionNode.getChildCount());

    ASTNode functionNameNode = (ASTNode) functionNode.getChild(0);
    assertEquals(HiveParser.Identifier, functionNameNode.getType());
    assertEquals(expectedFunctionName, functionNameNode.getText());

    ASTNode strToTrimNode = (ASTNode) functionNode.getChild(1);
    assertEquals(HiveParser.StringLiteral, strToTrimNode.getType());
    assertEquals(expectedStringToTrim, strToTrimNode.getText());

    return functionNode;
  }

  @Test
  public void testParseWhenFunctionHasTrimLikeStructure() {
    try {
      parseDriver.parse("select upper(both 'rfo' from 'foo  bar')", null);
      Assert.fail("Expected ParseException");
    } catch (ParseException ex) {
      Assert.assertEquals("Failure didn't match.",
              "line 1:13 cannot recognize input near 'both' ''rfo'' 'from' in function specification",
              ex.getMessage());
    }
  }
}
