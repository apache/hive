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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.parse.type;

import java.util.Map;

import org.antlr.runtime.CommonToken;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.JoinType;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the ambiguity checks in JoinCondTypeCheckProcFactory (HIVE-29580): a join condition
 * referencing a duplicate-named column that escaped a subquery boundary must be rejected, both
 * through the qualified (JoinCondDefaultExprProcessor) and the unqualified
 * (JoinCondColumnExprProcessor) resolution paths. The expressions are type checked through the
 * same walker entry point the planner uses.
 */
public class TestJoinCondTypeCheckProcFactory {

  private static ASTNode node(int type, String text) {
    return new ASTNode(new CommonToken(type, text));
  }

  private static ASTNode unqualifiedRef(String col) {
    ASTNode tableOrCol = node(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL");
    tableOrCol.addChild(node(HiveParser.Identifier, col));
    return tableOrCol;
  }

  private static ASTNode qualifiedRef(String tab, String col) {
    ASTNode dot = node(HiveParser.DOT, ".");
    dot.addChild(unqualifiedRef(tab));
    dot.addChild(node(HiveParser.Identifier, col));
    return dot;
  }

  private static RowResolver singleColRR(String tab, String col, boolean markedAmbiguous) {
    RowResolver rr = new RowResolver();
    ColumnInfo colInfo = new ColumnInfo(tab + "_" + col, TypeInfoFactory.stringTypeInfo, tab, false);
    colInfo.setAlias(col);
    colInfo.setAmbiguousName(markedAmbiguous);
    rr.put(tab, col, colInfo);
    return rr;
  }

  private static Map<ASTNode, RexNode> typeCheck(ASTNode expr, boolean leftColumnMarked)
      throws SemanticException {
    RowResolver leftRR = singleColRR("t", "c", leftColumnMarked);
    RowResolver rightRR = singleColRR("u", "e", false);
    RexBuilder rexBuilder = new RexBuilder(new JavaTypeFactoryImpl(new HiveTypeSystemImpl()));
    JoinTypeCheckCtx ctx = new JoinTypeCheckCtx(leftRR, rightRR, rexBuilder, JoinType.INNER);
    return RexNodeTypeCheck.genExprNodeJoinCond(expr, ctx, rexBuilder);
  }

  private static void assertThrowsAmbiguous(ASTNode expr) {
    SemanticException e = Assert.assertThrows(SemanticException.class, () -> typeCheck(expr, true));
    Assert.assertTrue(e.getMessage(), e.getMessage().contains("Ambiguous column reference c in t"));
  }

  @Test
  public void testQualifiedRefToMarkedColumnThrows() {
    assertThrowsAmbiguous(qualifiedRef("t", "c"));
  }

  @Test
  public void testUnqualifiedRefToMarkedColumnThrows() {
    assertThrowsAmbiguous(unqualifiedRef("c"));
  }

  @Test
  public void testQualifiedRefToCleanColumnResolves() throws SemanticException {
    ASTNode expr = qualifiedRef("t", "c");
    Assert.assertNotNull(typeCheck(expr, false).get(expr));
  }

  @Test
  public void testUnqualifiedRefToCleanColumnResolves() throws SemanticException {
    ASTNode expr = unqualifiedRef("c");
    Assert.assertNotNull(typeCheck(expr, false).get(expr));
  }
}
