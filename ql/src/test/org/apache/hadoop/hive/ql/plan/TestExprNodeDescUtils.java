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

package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class TestExprNodeDescUtils {

  private static ExprNodeDesc intConst(int value) {
    return new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, value);
  }

  private static ExprNodeDesc intColumn(String name) {
    return new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, name, "t", false);
  }

  private static ExprNodeDesc equalsExpr(ExprNodeDesc left, ExprNodeDesc right) {
    return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, new GenericUDFOPEqual(), "=",
        Arrays.asList(left, right));
  }

  private static ExprNodeDesc funcExpr(GenericUDF udf, ExprNodeDesc... children) {
    return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo, udf, Arrays.asList(children));
  }

  @Test
  public void testAndWithDuplicateConstantOperandsReturnsOperand() {
    ExprNodeDesc eq = equalsExpr(intConst(1), intConst(1));

    ExprNodeDesc result = ExprNodeDescUtils.and(eq, eq);

    assertEquals("GenericUDFOPEqual(Const int 1, Const int 1)", result.toString());
  }

  @Test
  public void testAndWithDuplicateColumnOperandsReturnsOperand() {
    ExprNodeDesc eq = equalsExpr(intColumn("a"), intConst(5));

    ExprNodeDesc result = ExprNodeDescUtils.and(Arrays.asList(eq, eq));

    assertEquals("GenericUDFOPEqual(Column[a], Const int 5)", result.toString());
  }

  @Test
  public void testAndWithNestedDuplicateOperandsReturnsOperand() {
    ExprNodeDesc eq = equalsExpr(intColumn("a"), intConst(5));
    ExprNodeDesc and = funcExpr(new GenericUDFOPAnd(), eq, eq);

    ExprNodeDesc result = ExprNodeDescUtils.and(eq, and);

    assertEquals("GenericUDFOPEqual(Column[a], Const int 5)", result.toString());
  }

  @Test
  public void testAndWithSingleOperandReturnsOperand() {
    ExprNodeDesc eq = equalsExpr(intColumn("a"), intConst(5));

    ExprNodeDesc result = ExprNodeDescUtils.and(Collections.singletonList(eq));

    assertEquals("GenericUDFOPEqual(Column[a], Const int 5)", result.toString());
  }

  @Test
  public void testAndWithDistinctOperandsIsFlattenedAndDeduplicated() {
    ExprNodeDesc eqA = equalsExpr(intColumn("a"), intConst(5));
    ExprNodeDesc eqB = equalsExpr(intColumn("b"), intConst(6));
    ExprNodeDesc eqC = equalsExpr(intColumn("a"), intConst(7));
    ExprNodeDesc and = funcExpr(new GenericUDFOPAnd(), eqB, eqC);

    ExprNodeDesc result = ExprNodeDescUtils.and(Arrays.asList(eqA, and, eqA));

    assertEquals(
        "GenericUDFOPAnd(GenericUDFOPEqual(Column[a], Const int 5), GenericUDFOPEqual(Column[b], Const int 6), "
            + "GenericUDFOPEqual(Column[a], Const int 7))",
        result.toString());
  }

  @Test
  public void testAndDoesNotFlattenNestedOr() {
    ExprNodeDesc eqA = equalsExpr(intColumn("a"), intConst(5));
    ExprNodeDesc eqB = equalsExpr(intColumn("b"), intConst(6));
    ExprNodeDesc or = funcExpr(new GenericUDFOPOr(), eqA, eqB);

    ExprNodeDesc result = ExprNodeDescUtils.and(eqA, or);

    assertEquals(
        "GenericUDFOPAnd(GenericUDFOPEqual(Column[a], Const int 5), "
            + "GenericUDFOPOr(GenericUDFOPEqual(Column[a], Const int 5),"
            + " GenericUDFOPEqual(Column[b], Const int 6)))",
        result.toString());
  }

  @Test
  public void testOrWithDuplicateConstantOperandsReturnsOperand() {
    ExprNodeDesc eq = equalsExpr(intConst(1), intConst(1));

    ExprNodeDesc result = ExprNodeDescUtils.or(Arrays.asList(eq, eq));

    assertEquals("GenericUDFOPEqual(Const int 1, Const int 1)", result.toString());
  }

  @Test
  public void testOrWithDuplicateColumnOperandsReturnsOperand() {
    ExprNodeDesc eq = equalsExpr(intColumn("a"), intConst(5));

    ExprNodeDesc result = ExprNodeDescUtils.or(Arrays.asList(eq, eq));

    assertEquals("GenericUDFOPEqual(Column[a], Const int 5)", result.toString());
  }

  @Test
  public void testOrWithNestedDuplicateOperandsReturnsOperand() {
    ExprNodeDesc eq = equalsExpr(intColumn("a"), intConst(5));
    ExprNodeDesc or = funcExpr(new GenericUDFOPOr(), eq, eq);

    ExprNodeDesc result = ExprNodeDescUtils.or(Arrays.asList(eq, or));

    assertEquals("GenericUDFOPEqual(Column[a], Const int 5)", result.toString());
  }

  @Test
  public void testOrWithSingleOperandReturnsOperand() {
    ExprNodeDesc eq = equalsExpr(intColumn("a"), intConst(5));

    ExprNodeDesc result = ExprNodeDescUtils.or(Collections.singletonList(eq));

    assertEquals("GenericUDFOPEqual(Column[a], Const int 5)", result.toString());
  }

  @Test
  public void testOrWithDistinctOperandsIsFlattenedAndDeduplicated() {
    ExprNodeDesc eqA = equalsExpr(intColumn("a"), intConst(5));
    ExprNodeDesc eqB = equalsExpr(intColumn("b"), intConst(6));
    ExprNodeDesc eqC = equalsExpr(intColumn("a"), intConst(7));
    ExprNodeDesc or = funcExpr(new GenericUDFOPOr(), eqB, eqC);

    ExprNodeDesc result = ExprNodeDescUtils.or(Arrays.asList(eqA, or, eqA));

    assertEquals(
        "GenericUDFOPOr(GenericUDFOPEqual(Column[a], Const int 5), GenericUDFOPEqual(Column[b], Const int 6),"
            + " GenericUDFOPEqual(Column[a], Const int 7))",
        result.toString());
  }

  @Test
  public void testOrDoesNotFlattenNestedAnd() {
    ExprNodeDesc eqA = equalsExpr(intColumn("a"), intConst(5));
    ExprNodeDesc eqB = equalsExpr(intColumn("b"), intConst(6));
    ExprNodeDesc and = funcExpr(new GenericUDFOPAnd(), eqA, eqB);

    ExprNodeDesc result = ExprNodeDescUtils.or(Arrays.asList(eqA, and));

    assertEquals(
        "GenericUDFOPOr(GenericUDFOPEqual(Column[a], Const int 5), GenericUDFOPAnd(GenericUDFOPEqual(Column[a],"
            + " Const int 5), GenericUDFOPEqual(Column[b], Const int 6)))",
        result.toString());
  }
}
