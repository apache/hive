/**
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

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestQBJoinTreeApplyPredicate {

  static HiveConf conf;

  SemanticAnalyzer sA;

  @BeforeClass
  public static void initialize() {
    conf = new HiveConf(SemanticAnalyzer.class);
    SessionState.start(conf);
  }

  @Before
  public void setup() throws SemanticException {
    sA = new SemanticAnalyzer(conf);
  }

  static ASTNode constructIdentifier(String nm) {
    return (ASTNode) ParseDriver.adaptor.create(HiveParser.Identifier, nm);
  }

  static ASTNode constructTabRef(String tblNm) {
    ASTNode table = (ASTNode)
        ParseDriver.adaptor.create(HiveParser.TOK_TABLE_OR_COL, "TOK_TABLE_OR_COL");
    ASTNode id = constructIdentifier(tblNm);
    table.addChild(id);
    return table;
  }

  static ASTNode constructColRef(String tblNm, String colNm) {
    ASTNode table = constructTabRef(tblNm);
    ASTNode col = constructIdentifier(colNm);
    ASTNode dot = (ASTNode) ParseDriver.adaptor.create(HiveParser.DOT, ".");
    dot.addChild(table);
    dot.addChild(col);
    return dot;
  }

  static ASTNode constructEqualityCond(String lTbl, String lCol, String rTbl, String rCol) {
    ASTNode lRef = constructColRef(lTbl, lCol);
    ASTNode rRef = constructColRef(rTbl, rCol);
    ASTNode eq = (ASTNode) ParseDriver.adaptor.create(HiveParser.EQUAL, "=");
    eq.addChild(lRef);
    eq.addChild(rRef);
    return eq;
  }

  QBJoinTree createJoinTree(JoinType type,
      String leftAlias,
      QBJoinTree leftTree,
      String rightAlias) {
    QBJoinTree jT = new QBJoinTree();
    JoinCond[] condn = new JoinCond[1];
    condn[0] = new JoinCond(0, 1, type);
    if ( leftTree == null ) {
      jT.setLeftAlias(leftAlias);
      String[] leftAliases = new String[1];
      leftAliases[0] = leftAlias;
      jT.setLeftAliases(leftAliases);
    } else {
      jT.setJoinSrc(leftTree);
      String[] leftChildAliases = leftTree.getLeftAliases();
      String leftAliases[] = new String[leftChildAliases.length + 1];
      for (int i = 0; i < leftChildAliases.length; i++) {
        leftAliases[i] = leftChildAliases[i];
      }
      leftAliases[leftChildAliases.length] = leftTree.getRightAliases()[0];
      jT.setLeftAliases(leftAliases);
    }
    String[] rightAliases = new String[1];
    rightAliases[0] = rightAlias;
    jT.setRightAliases(rightAliases);
    String[] children = new String[2];
    children[0] = leftAlias;
    children[1] = rightAlias;
    jT.setBaseSrc(children);
    ArrayList<ArrayList<ASTNode>> expressions = new ArrayList<ArrayList<ASTNode>>();
    expressions.add(new ArrayList<ASTNode>());
    expressions.add(new ArrayList<ASTNode>());
    jT.setExpressions(expressions);

    ArrayList<Boolean> nullsafes = new ArrayList<Boolean>();
    jT.setNullSafes(nullsafes);

    ArrayList<ArrayList<ASTNode>> filters = new ArrayList<ArrayList<ASTNode>>();
    filters.add(new ArrayList<ASTNode>());
    filters.add(new ArrayList<ASTNode>());
    jT.setFilters(filters);
    jT.setFilterMap(new int[2][]);

    ArrayList<ArrayList<ASTNode>> filtersForPushing =
        new ArrayList<ArrayList<ASTNode>>();
    filtersForPushing.add(new ArrayList<ASTNode>());
    filtersForPushing.add(new ArrayList<ASTNode>());
    jT.setFiltersForPushing(filtersForPushing);

    return jT;
  }

  ASTNode applyEqPredicate(QBJoinTree jT,
      String lTbl, String lCol,
      String rTbl, String rCol) throws SemanticException {
    ASTNode joinCond = constructEqualityCond(lTbl, lCol, rTbl, rCol);

    ASTNode leftCondn = (ASTNode) joinCond.getChild(0);
    ASTNode rightCondn = (ASTNode) joinCond.getChild(1);

    List<String> leftSrc = new ArrayList<String>();
    ArrayList<String> leftCondAl1 = new ArrayList<String>();
    ArrayList<String> leftCondAl2 = new ArrayList<String>();
    ArrayList<String> rightCondAl1 = new ArrayList<String>();
    ArrayList<String> rightCondAl2 = new ArrayList<String>();

    sA.parseJoinCondPopulateAlias(jT, leftCondn, leftCondAl1, leftCondAl2, null, null);
    sA.parseJoinCondPopulateAlias(jT, rightCondn, rightCondAl1, rightCondAl2, null, null);

    sA.applyEqualityPredicateToQBJoinTree(jT, JoinType.INNER, leftSrc, joinCond,
        leftCondn,
        rightCondn,
        leftCondAl1, leftCondAl2, rightCondAl1, rightCondAl2);
    return joinCond;
  }

  @Test
  public void testSimpleCondn() throws SemanticException {
    QBJoinTree jT = createJoinTree(JoinType.INNER, "a", null, "b");
    ASTNode joinCond = applyEqPredicate(jT, "a", "x", "b", "y");
    Assert.assertEquals(jT.getExpressions().get(0).get(0), joinCond.getChild(0));
    Assert.assertEquals(jT.getExpressions().get(1).get(0), joinCond.getChild(1));
  }

  @Test
  public void test3WayJoin() throws SemanticException {
    QBJoinTree jT1 = createJoinTree(JoinType.INNER, "a", null, "b");
    QBJoinTree jT = createJoinTree(JoinType.INNER, "b", jT1, "c");
    ASTNode joinCond1 = applyEqPredicate(jT, "a", "x", "b", "y");
    ASTNode joinCond2 = applyEqPredicate(jT, "b", "y", "c", "z");
    Assert.assertEquals(jT1.getExpressions().get(0).get(0), joinCond1.getChild(0));
    Assert.assertEquals(jT1.getExpressions().get(1).get(0), joinCond1.getChild(1));
    Assert.assertEquals(jT.getExpressions().get(0).get(0), joinCond2.getChild(0));
    Assert.assertEquals(jT.getExpressions().get(1).get(0), joinCond2.getChild(1));
  }

  @Test
  public void test3WayJoinSwitched() throws SemanticException {
    QBJoinTree jT1 = createJoinTree(JoinType.INNER, "a", null, "b");
    QBJoinTree jT = createJoinTree(JoinType.INNER, "b", jT1, "c");
    ASTNode joinCond1 = applyEqPredicate(jT, "b", "y", "a", "x");
    ASTNode joinCond2 = applyEqPredicate(jT, "b", "y", "c", "z");
    Assert.assertEquals(jT1.getExpressions().get(0).get(0), joinCond1.getChild(1));
    Assert.assertEquals(jT1.getExpressions().get(1).get(0), joinCond1.getChild(0));
    Assert.assertEquals(jT.getExpressions().get(0).get(0), joinCond2.getChild(0));
    Assert.assertEquals(jT.getExpressions().get(1).get(0), joinCond2.getChild(1));
  }

  @Test
  public void test4WayJoin() throws SemanticException {
    QBJoinTree jT1 = createJoinTree(JoinType.INNER, "a", null, "b");
    QBJoinTree jT2 = createJoinTree(JoinType.INNER, "b", jT1, "c");
    QBJoinTree jT = createJoinTree(JoinType.INNER, "c", jT2, "d");
    ASTNode joinCond1 = applyEqPredicate(jT, "a", "x", "b", "y");
    ASTNode joinCond2 = applyEqPredicate(jT, "b", "y", "c", "z");
    ASTNode joinCond3 = applyEqPredicate(jT, "a", "x", "c", "z");
    Assert.assertEquals(jT1.getExpressions().get(0).get(0), joinCond1.getChild(0));
    Assert.assertEquals(jT1.getExpressions().get(1).get(0), joinCond1.getChild(1));
    Assert.assertEquals(jT2.getExpressions().get(0).get(0), joinCond2.getChild(0));
    Assert.assertEquals(jT2.getExpressions().get(1).get(0), joinCond2.getChild(1));
    Assert.assertEquals(jT2.getExpressions().get(0).get(1), joinCond3.getChild(0));
    Assert.assertEquals(jT2.getExpressions().get(1).get(1), joinCond3.getChild(1));
  }

  @Test
  public void test4WayJoinSwitched() throws SemanticException {
    QBJoinTree jT1 = createJoinTree(JoinType.INNER, "a", null, "b");
    QBJoinTree jT2 = createJoinTree(JoinType.INNER, "b", jT1, "c");
    QBJoinTree jT = createJoinTree(JoinType.INNER, "c", jT2, "d");
    ASTNode joinCond1 = applyEqPredicate(jT, "b", "y", "a", "x");
    ASTNode joinCond2 = applyEqPredicate(jT, "b", "y", "c", "z");
    ASTNode joinCond3 = applyEqPredicate(jT, "c", "z", "a", "x");
    Assert.assertEquals(jT1.getExpressions().get(0).get(0), joinCond1.getChild(1));
    Assert.assertEquals(jT1.getExpressions().get(1).get(0), joinCond1.getChild(0));
    Assert.assertEquals(jT2.getExpressions().get(0).get(0), joinCond2.getChild(0));
    Assert.assertEquals(jT2.getExpressions().get(1).get(0), joinCond2.getChild(1));
    Assert.assertEquals(jT2.getExpressions().get(0).get(1), joinCond3.getChild(1));
    Assert.assertEquals(jT2.getExpressions().get(1).get(1), joinCond3.getChild(0));
  }
}