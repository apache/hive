/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.ppr;

import java.util.Arrays;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNull;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public final class TestPositivePartitionPrunerCompactExpr {

  private final ExprNodeDesc expression;
  private final String expected;

  public TestPositivePartitionPrunerCompactExpr(ExprNodeDesc expression, String expected) {
    this.expression = expression;
    this.expected = expected;
  }

  @Parameterized.Parameters(name = "{index}: {0} => {1}")
  public static Iterable<Object[]> data() {
    ExprNodeDesc trueExpr = new ExprNodeConstantDesc(Boolean.TRUE);
    ExprNodeDesc falseExpr = new ExprNodeConstantDesc(Boolean.FALSE);
    ExprNodeDesc col1Expr = new ExprNodeColumnDesc(TypeInfoFactory.booleanTypeInfo, "col1", "t1", true);
    ExprNodeDesc col2Expr = new ExprNodeColumnDesc(TypeInfoFactory.booleanTypeInfo, "col2", "t1", true);
    ExprNodeDesc udf1Expr = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPNull(), Arrays.<ExprNodeDesc>asList(col1Expr));
    ExprNodeDesc udf2Expr = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPNull(), Arrays.<ExprNodeDesc>asList(col2Expr));

    return Arrays.asList(new Object[][]{
      {null, null},
      {and(null, null), null},
      {and(falseExpr, null), "false"},
      {and(null, falseExpr), "false"},
      {and(trueExpr, null), null},
      {and(null, trueExpr), null},
      {and(udf1Expr, null), "col1 is null"},
      {and(null, udf2Expr), "col2 is null"},
      {and(udf1Expr, udf2Expr), "(col1 is null and col2 is null)"},
      {and(falseExpr, falseExpr), "false"},
      {and(trueExpr, falseExpr), "false"},
      {and(falseExpr, trueExpr), "false"},
      {and(udf1Expr, falseExpr), "false"},
      {and(falseExpr, udf2Expr), "false"},
      {and(trueExpr, trueExpr), "true"},
      {and(udf1Expr, trueExpr), "col1 is null"},
      {and(trueExpr, udf2Expr), "col2 is null"},
      {or(null, null), null},
      {or(falseExpr, null), null},
      {or(null, falseExpr), null},
      {or(trueExpr, null), "true"},
      {or(null, trueExpr), "true"},
      {or(udf1Expr, null), null},
      {or(null, udf2Expr), null},
      {or(udf1Expr, udf2Expr), "(col1 is null or col2 is null)"},
      {or(falseExpr, falseExpr), "false"},
      {or(trueExpr, falseExpr), "true"},
      {or(falseExpr, trueExpr), "true"},
      {or(udf1Expr, falseExpr), "col1 is null"},
      {or(falseExpr, udf2Expr), "col2 is null"},
      {or(trueExpr, trueExpr), "true"},
      {or(udf1Expr, trueExpr), "true"},
      {or(trueExpr, udf2Expr), "true"},
      {or(and(udf1Expr, udf2Expr), udf2Expr), "((col1 is null and col2 is null) or col2 is null)"},
      {and(or(udf1Expr, udf2Expr), udf2Expr), "((col1 is null or col2 is null) and col2 is null)"},
    });
  }

  @Test
  public void testCompactExpr() {
    ExprNodeDesc actual = PartitionPruner.compactExpr(expression);
    if (expected == null) {
      assertNull(actual);
    } else {
      assertNotNull("Expected not NULL expression", actual);
      assertNotNull("Expected not NULL expression string", actual.getExprString());
      assertEquals(expected, actual.getExprString());
    }
  }

  private static ExprNodeDesc or(ExprNodeDesc left, ExprNodeDesc right) {
    return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPOr(), Arrays.<ExprNodeDesc>asList(left, right));
  }

  private static ExprNodeDesc and(ExprNodeDesc left, ExprNodeDesc right) {
    return new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPAnd(), Arrays.<ExprNodeDesc>asList(left, right));
  }
}
