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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRexExecutorImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

    assertUdf(GenericUDFOPEqual.class, 2, result);
  }

  @Test
  public void testAndWithDuplicateColumnOperandsIsUnwrapped() {
    RexNode eq = equalsCall(rexBuilder.makeInputRef(intType, 0), intLiteral(5));
    ExprNodeDesc result = convert(rexBuilder.makeCall(SqlStdOperatorTable.AND, eq, eq));

    assertUdf(GenericUDFOPEqual.class, 2, result);
  }

  @Test
  public void testOrWithDuplicateConstantOperandsIsUnwrapped() {
    RexNode eq = equalsCall(intLiteral(5), intLiteral(5));
    ExprNodeDesc result = convert(rexBuilder.makeCall(SqlStdOperatorTable.OR, eq, eq));

    assertUdf(GenericUDFOPEqual.class, 2, result);
  }

  @Test
  public void testOrWithDuplicateColumnOperandsIsUnwrapped() {
    RexNode eq = equalsCall(rexBuilder.makeInputRef(intType, 0), intLiteral(5));
    ExprNodeDesc result = convert(rexBuilder.makeCall(SqlStdOperatorTable.OR, eq, eq));

    assertUdf(GenericUDFOPEqual.class, 2, result);
  }

  @Test
  public void testAndWithDistinctOperandsIsKept() {
    RexNode eqA = equalsCall(rexBuilder.makeInputRef(intType, 0), intLiteral(5));
    RexNode eqB = equalsCall(rexBuilder.makeInputRef(intType, 1), intLiteral(6));
    ExprNodeDesc result = convert(rexBuilder.makeCall(SqlStdOperatorTable.AND, eqA, eqB, eqA));

    assertUdf(GenericUDFOPAnd.class, 2, result);
  }

  @Test
  public void testOrWithDistinctOperandsIsKept() {
    RexNode eqA = equalsCall(rexBuilder.makeInputRef(intType, 0), intLiteral(5));
    RexNode eqB = equalsCall(rexBuilder.makeInputRef(intType, 1), intLiteral(6));
    ExprNodeDesc result = convert(rexBuilder.makeCall(SqlStdOperatorTable.OR, eqA, eqB, eqA));

    assertUdf(GenericUDFOPOr.class, 2, result);
  }

  @Test
  public void testNestedANDWithDistinctOperandsIsFlattened() {
    RexNode eqA = equalsCall(rexBuilder.makeInputRef(intType, 0), intLiteral(5));
    RexNode eqB = equalsCall(rexBuilder.makeInputRef(intType, 1), intLiteral(6));
    RexNode eqC = equalsCall(rexBuilder.makeInputRef(intType, 0), intLiteral(7));
    RexNode nestedOr = rexBuilder.makeCall(SqlStdOperatorTable.AND, eqB, eqC);
    ExprNodeDesc result = convert(rexBuilder.makeCall(SqlStdOperatorTable.AND, eqA, nestedOr));

    assertUdf(GenericUDFOPAnd.class, 3, result);
  }

  @Test
  public void testNestedOrWithDistinctOperandsIsFlattened() {
    RexNode eqA = equalsCall(rexBuilder.makeInputRef(intType, 0), intLiteral(5));
    RexNode eqB = equalsCall(rexBuilder.makeInputRef(intType, 1), intLiteral(6));
    RexNode eqC = equalsCall(rexBuilder.makeInputRef(intType, 0), intLiteral(7));
    RexNode nestedOr = rexBuilder.makeCall(SqlStdOperatorTable.OR, eqB, eqC);
    ExprNodeDesc result = convert(rexBuilder.makeCall(SqlStdOperatorTable.OR, eqA, nestedOr));

    assertUdf(GenericUDFOPOr.class, 3, result);
  }

  @Test
  public void testReduceAndWithDuplicateConstantOperands() {
    assertSpecifiedOperatorWithDuplicateConstantOperands(SqlStdOperatorTable.AND);
  }

  @Test
  public void testReduceOrWithDuplicateConstantOperands() {
    assertSpecifiedOperatorWithDuplicateConstantOperands(SqlStdOperatorTable.OR);
  }

  private void assertSpecifiedOperatorWithDuplicateConstantOperands(SqlBinaryOperator operator) {
    RexNode eq = equalsCall(intLiteral(1), intLiteral(1));
    RexNode and = rexBuilder.makeCall(operator, eq, eq);
    List<RexNode> reduced = new ArrayList<>();

    new HiveRexExecutorImpl().reduce(rexBuilder, Collections.singletonList(and), reduced);

    assertEquals(1, reduced.size());
    assertTrue(reduced.get(0) instanceof RexLiteral);
    assertEquals(Boolean.TRUE, ((RexLiteral) reduced.get(0)).getValueAs(Boolean.class));
  }

  private static void assertUdf(Class<?> expectedUdf, int expectedChildren, ExprNodeDesc actual) {
    assertTrue(actual.getExprString(), actual instanceof ExprNodeGenericFuncDesc);
    assertEquals(actual.getExprString(), expectedUdf, ((ExprNodeGenericFuncDesc) actual).getGenericUDF().getClass());
    assertEquals(actual.getExprString(), expectedChildren, actual.getChildren().size());
  }
}
