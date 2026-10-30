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

package org.apache.hadoop.hive.ql.optimizer.calcite.translator;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestExprNodeConverter {

  private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl(new HiveTypeSystemImpl());
  private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
  private final RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
  private final RelDataType rowType = typeFactory.builder().add("a", intType).add("b", intType).build();

  private ExprNodeDesc convert(RexNode node) {
    return node.accept(new ExprNodeConverter("t", rowType, Collections.emptySet(), typeFactory));
  }

  private RexNode intLiteral(int value) {
    return rexBuilder.makeExactLiteral(BigDecimal.valueOf(value), intType);
  }

  private RexNode equalsCall(RexNode left, RexNode right) {
    return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, left, right);
  }

  @Test
  public void testAndWithDuplicateConstantOperandsIsUnwrapped() {
    RexNode eq = equalsCall(intLiteral(1), intLiteral(1));
    ExprNodeDesc result = convert(rexBuilder.makeCall(SqlStdOperatorTable.AND, eq, eq));

    List<ExprNodeDesc> resultNodes = result.getChildren();
    assertEquals(2, resultNodes.size());
    assertEquals("Const int 1", resultNodes.get(0).toString());
    assertEquals("Const int 1", resultNodes.get(1).toString());
  }

  @Test
  public void testAndWithDuplicateColumnOperandsIsUnwrapped() {
    RexNode eq = equalsCall(rexBuilder.makeInputRef(intType, 0), intLiteral(5));
    ExprNodeDesc result = convert(rexBuilder.makeCall(SqlStdOperatorTable.AND, eq, eq));

    List<ExprNodeDesc> resultNodes = result.getChildren();
    assertEquals(2, resultNodes.size());
    assertEquals("Column[a]", resultNodes.get(0).toString());
    assertEquals("Const int 5", resultNodes.get(1).toString());
  }

  @Test
  public void testOrWithDuplicateConstantOperandsIsUnwrapped() {
    RexNode eq = equalsCall(intLiteral(5), intLiteral(5));
    ExprNodeDesc result = convert(rexBuilder.makeCall(SqlStdOperatorTable.OR, eq, eq));

    List<ExprNodeDesc> resultNodes = result.getChildren();
    assertEquals(2, resultNodes.size());
    assertEquals("Const int 5", resultNodes.get(0).toString());
    assertEquals("Const int 5", resultNodes.get(1).toString());
  }

  @Test
  public void testOrWithDuplicateColumnOperandsIsUnwrapped() {
    RexNode eq = equalsCall(rexBuilder.makeInputRef(intType, 0), intLiteral(5));
    ExprNodeDesc result = convert(rexBuilder.makeCall(SqlStdOperatorTable.OR, eq, eq));

    List<ExprNodeDesc> resultNodes = result.getChildren();
    assertEquals(2, resultNodes.size());
    assertEquals("Column[a]", resultNodes.get(0).toString());
    assertEquals("Const int 5", resultNodes.get(1).toString());
  }

  @Test
  public void testAndWithDistinctOperandsIsKept() {
    RexNode eqA = equalsCall(rexBuilder.makeInputRef(intType, 0), intLiteral(5));
    RexNode eqB = equalsCall(rexBuilder.makeInputRef(intType, 1), intLiteral(6));
    ExprNodeDesc result = convert(rexBuilder.makeCall(SqlStdOperatorTable.AND, eqA, eqB, eqA));

    List<ExprNodeDesc> resultNodes = result.getChildren();
    assertEquals(2, resultNodes.size());
    assertEquals("GenericUDFOPEqual(Column[a], Const int 5)", resultNodes.get(0).toString());
    assertEquals("GenericUDFOPEqual(Column[b], Const int 6)", resultNodes.get(1).toString());
  }

  @Test
  public void testOrWithDistinctOperandsIsKept() {
    RexNode eqA = equalsCall(rexBuilder.makeInputRef(intType, 0), intLiteral(5));
    RexNode eqB = equalsCall(rexBuilder.makeInputRef(intType, 1), intLiteral(6));
    ExprNodeDesc result = convert(rexBuilder.makeCall(SqlStdOperatorTable.OR, eqA, eqB, eqA));

    List<ExprNodeDesc> resultNodes = result.getChildren();
    assertEquals(2, resultNodes.size());
    assertEquals("GenericUDFOPEqual(Column[a], Const int 5)", resultNodes.get(0).toString());
    assertEquals("GenericUDFOPEqual(Column[b], Const int 6)", resultNodes.get(1).toString());
  }
}
