/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.variant;

import java.util.List;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFVariantGet;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.junit.Assert;
import org.junit.Test;

public class TestVariantFilterRewriter {

  private static final TypeInfo VARIANT_STRUCT =
      TypeInfoUtils.getTypeInfoFromTypeString("struct<metadata:binary,value:binary>");

  @Test
  public void testStripExtractPredicateBecomesAlwaysTrue() {
    Expression expr = Expressions.equal(
        Expressions.extract("payload", "$.tier", "string"),
        "gold");

    Assert.assertSame(
        Expressions.alwaysTrue(),
        VariantFilterRewriter.stripVariantExtractPredicates(expr));
  }

  @Test
  public void testStripTypedValueReferenceBecomesAlwaysTrue() {
    Expression expr = Expressions.equal("payload.typed_value.tier", "gold");

    Assert.assertSame(
        Expressions.alwaysTrue(),
        VariantFilterRewriter.stripVariantExtractPredicates(expr));
  }

  @Test
  public void testStripExtractFromAndKeepsOtherConjunct() {
    Expression expr = Expressions.and(
        Expressions.equal(
            Expressions.extract("payload", "$.tier", "string"),
            "gold"),
        Expressions.greaterThanOrEqual("id", 5));

    Expression stripped = VariantFilterRewriter.stripVariantExtractPredicates(expr);
    Assert.assertTrue(stripped instanceof UnboundPredicate);

    UnboundPredicate<?> predicate = (UnboundPredicate<?>) stripped;
    Assert.assertEquals(Expression.Operation.GT_EQ, predicate.op());
    Assert.assertEquals("id", predicate.ref().name());
  }

  @Test
  public void testStripExtractFromOrBecomesAlwaysTrue() {
    Expression expr = Expressions.or(
        Expressions.equal(
            Expressions.extract("payload", "$.tier", "string"),
            "gold"),
        Expressions.lessThan("id", 10));

    Assert.assertSame(
        Expressions.alwaysTrue(),
        VariantFilterRewriter.stripVariantExtractPredicates(expr));
  }

  @Test
  public void testStripExtractUnderNotBecomesAlwaysTrue() {
    Expression expr = Expressions.not(
        Expressions.equal(
            Expressions.extract("payload", "$.tier", "string"),
            "gold"));

    Assert.assertSame(
        Expressions.alwaysTrue(),
        VariantFilterRewriter.stripVariantExtractPredicates(expr));
  }

  @Test
  public void testRewritesVariantGetToShreddedColumn() throws Exception {
    ExprNodeGenericFuncDesc predicate = equals(
        variantGet("payload", "$.tier"),
        stringConst("gold"));

    ExprNodeGenericFuncDesc rewritten = VariantFilterRewriter.rewriteForShredding(predicate);
    Assert.assertNotNull(rewritten);

    // original must remain unchanged (rewriteForShredding clones)
    Assert.assertTrue(predicate.getChildren().getFirst() instanceof ExprNodeGenericFuncDesc);

    ExprNodeDesc lhs = rewritten.getChildren().getFirst();
    Assert.assertTrue(lhs instanceof ExprNodeColumnDesc);

    ExprNodeColumnDesc col = (ExprNodeColumnDesc) lhs;
    Assert.assertEquals("payload.typed_value.tier", col.getColumn());
    Assert.assertEquals("t", col.getTabAlias());
    Assert.assertEquals(predicate.getChildren().getFirst().getTypeInfo(), col.getTypeInfo());
  }

  @Test
  public void testRewritesNestedObjectPath() throws Exception {
    ExprNodeGenericFuncDesc predicate = equals(
        variantGet("payload", "$.a.b"),
        stringConst("gold"));

    ExprNodeGenericFuncDesc rewritten = VariantFilterRewriter.rewriteForShredding(predicate);

    ExprNodeDesc lhs = rewritten.getChildren().getFirst();
    Assert.assertTrue(lhs instanceof ExprNodeColumnDesc);
    Assert.assertEquals("payload.typed_value.a.b", ((ExprNodeColumnDesc) lhs).getColumn());
  }

  @Test
  public void testDoesNotRewriteArrayPath() throws Exception {
    ExprNodeGenericFuncDesc predicate = equals(
        variantGet("payload", "$[0]"),
        stringConst("gold"));

    ExprNodeGenericFuncDesc rewritten = VariantFilterRewriter.rewriteForShredding(predicate);

    ExprNodeDesc lhs = rewritten.getChildren().getFirst();
    Assert.assertTrue(lhs instanceof ExprNodeGenericFuncDesc);
    Assert.assertTrue(((ExprNodeGenericFuncDesc) lhs).getGenericUDF() instanceof GenericUDFVariantGet);
  }

  @Test
  public void testDoesNotRewriteWhenPathIsNotConstant() throws Exception {
    ExprNodeColumnDesc payload = new ExprNodeColumnDesc(
        VARIANT_STRUCT, "payload", "t", false);
    ExprNodeColumnDesc path = new ExprNodeColumnDesc(
        TypeInfoFactory.stringTypeInfo, "path", "t", false);
    ExprNodeGenericFuncDesc udf = ExprNodeGenericFuncDesc.newInstance(
        new GenericUDFVariantGet(), List.of(payload, path));
    ExprNodeGenericFuncDesc predicate = equals(udf, stringConst("gold"));

    ExprNodeGenericFuncDesc rewritten = VariantFilterRewriter.rewriteForShredding(predicate);

    ExprNodeDesc lhs = rewritten.getChildren().getFirst();
    Assert.assertTrue(lhs instanceof ExprNodeGenericFuncDesc);
    Assert.assertTrue(((ExprNodeGenericFuncDesc) lhs).getGenericUDF() instanceof GenericUDFVariantGet);
  }

  private static ExprNodeGenericFuncDesc equals(ExprNodeDesc left, ExprNodeDesc right) throws Exception {
    return ExprNodeGenericFuncDesc.newInstance(
        new GenericUDFOPEqual(), List.of(left, right));
  }

  private static ExprNodeConstantDesc stringConst(String value) {
    return new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, value);
  }

  private static ExprNodeGenericFuncDesc variantGet(String column, String jsonPath) throws Exception {
    ExprNodeColumnDesc payload = new ExprNodeColumnDesc(VARIANT_STRUCT, column, "t", false);
    ExprNodeConstantDesc path = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, jsonPath);
    return ExprNodeGenericFuncDesc.newInstance(
        new GenericUDFVariantGet(), List.of(payload, path));
  }
}
