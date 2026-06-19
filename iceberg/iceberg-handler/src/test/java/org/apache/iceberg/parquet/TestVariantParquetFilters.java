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

package org.apache.iceberg.parquet;

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.junit.Assert;
import org.junit.Test;

public class TestVariantParquetFilters {

  private static final MessageType VARIANT_MESSAGE =
      MessageTypeParser.parseMessageType(
          "message root {" +
              "  optional group payload {" +
              "    required binary metadata;" +
              "    optional binary value;" +
              "    optional group typed_value {" +
              "      required group tier {" +
              "        optional binary value;" +
              "        optional binary typed_value (UTF8);" +
              "      }" +
              "    }" +
              "  }" +
              "}");

  private static final MessageType VARIANT_MESSAGE_WITH_LONG =
      MessageTypeParser.parseMessageType(
          "message root {" +
              "  optional group payload {" +
              "    required binary metadata;" +
              "    optional binary value;" +
              "    optional group typed_value {" +
              "      required group age {" +
              "        optional binary value;" +
              "        optional int64 typed_value;" +
              "      }" +
              "    }" +
              "  }" +
              "}");

  private static final MessageType VARIANT_MESSAGE_NESTED =
      MessageTypeParser.parseMessageType(
          "message root {" +
              "  optional group payload {" +
              "    required binary metadata;" +
              "    optional binary value;" +
              "    optional group typed_value {" +
              "      required group a {" +
              "        required group b {" +
              "          optional binary value;" +
              "          optional binary typed_value (UTF8);" +
              "        }" +
              "      }" +
              "    }" +
              "  }" +
              "}");

  private static final MessageType VARIANT_MESSAGE_STRUCT_NESTED =
      MessageTypeParser.parseMessageType(
          "message root {" +
              "  optional group payload {" +
              "    optional group info {" +
              "      required binary metadata;" +
              "      optional binary value;" +
              "      optional group typed_value {" +
              "        required group city {" +
              "          optional binary value;" +
              "          optional binary typed_value (UTF8);" +
              "        }" +
              "      }" +
              "    }" +
              "  }" +
              "}");

  private static final MessageType VARIANT_MESSAGE_MULTI_TYPE =
      MessageTypeParser.parseMessageType(
          "message root {" +
              "  optional group payload {" +
              "    required binary metadata;" +
              "    optional binary value;" +
              "    optional group typed_value {" +
              "      required group int_field {" +
              "        optional binary value;" +
              "        optional int32 typed_value;" +
              "      }" +
              "      required group float_field {" +
              "        optional binary value;" +
              "        optional float typed_value;" +
              "      }" +
              "      required group double_field {" +
              "        optional binary value;" +
              "        optional double typed_value;" +
              "      }" +
              "      required group bool_field {" +
              "        optional binary value;" +
              "        optional boolean typed_value;" +
              "      }" +
              "    }" +
              "  }" +
              "}");

  @Test
  public void testEqualsPredicateConversion() {
    assertPredicateConversion(
        Expressions.equal("payload.typed_value.tier", "gold"),
        VARIANT_MESSAGE,
        FilterApi.eq(
            FilterApi.binaryColumn("payload.typed_value.tier.typed_value"),
            Binary.fromString("gold")));
  }

  @Test
  public void testUnknownColumn() {
    Expression expr = Expressions.equal("payload.typed_value.unknown", "gold");
    FilterCompat.Filter filter = VariantParquetFilters.toParquetFilter(VARIANT_MESSAGE, expr);
    Assert.assertNull(filter);
  }

  @Test
  public void testLongPredicateConversion() {
    assertPredicateConversion(
        Expressions.greaterThanOrEqual(
            Expressions.extract("payload", "$.age", "long"),
            30L),
        VARIANT_MESSAGE_WITH_LONG,
        FilterApi.gtEq(
            FilterApi.longColumn("payload.typed_value.age.typed_value"),
            30L));
  }

  @Test
  public void testNestedPathConversion() {
    assertPredicateConversion(
        Expressions.equal(
            Expressions.extract("payload", "$.a.b", "string"),
            "gold"),
        VARIANT_MESSAGE_NESTED,
        FilterApi.eq(
            FilterApi.binaryColumn("payload.typed_value.a.b.typed_value"),
            Binary.fromString("gold")));
  }

  @Test
  public void testStructNestedVariantConversion() {
    assertPredicateConversion(
        Expressions.equal(
            Expressions.extract("payload.info", "$.city", "string"),
            "Seattle"),
        VARIANT_MESSAGE_STRUCT_NESTED,
        FilterApi.eq(
            FilterApi.binaryColumn("payload.info.typed_value.city.typed_value"),
            Binary.fromString("Seattle")));
  }

  @Test
  public void testIsNullPredicateConversion() {
    assertNullPredicateConversion(
        Expressions.isNull(
            Expressions.extract("payload", "$.tier", "string")),
        VARIANT_MESSAGE,
        "payload.typed_value.tier.typed_value",
        true);
  }

  @Test
  public void testNotNullPredicateConversion() {
    assertNullPredicateConversion(
        Expressions.notNull(
            Expressions.extract("payload", "$.tier", "string")),
        VARIANT_MESSAGE,
        "payload.typed_value.tier.typed_value",
        false);
  }

  @Test
  public void testIsNullOnNestedPath() {
    assertNullPredicateConversion(
        Expressions.isNull(
            Expressions.extract("payload", "$.a.b", "string")),
        VARIANT_MESSAGE_NESTED,
        "payload.typed_value.a.b.typed_value",
        true);
  }

  @Test
  public void testNotEqualPredicateConversion() {
    assertPredicateConversion(
        Expressions.notEqual("payload.typed_value.tier", "gold"),
        VARIANT_MESSAGE,
        FilterApi.notEq(
            FilterApi.binaryColumn("payload.typed_value.tier.typed_value"),
            Binary.fromString("gold")));
  }

  @Test
  public void testGreaterThanPredicateConversion() {
    assertPredicateConversion(
        Expressions.greaterThan(
            Expressions.extract("payload", "$.age", "long"),
            25L),
        VARIANT_MESSAGE_WITH_LONG,
        FilterApi.gt(
            FilterApi.longColumn("payload.typed_value.age.typed_value"),
            25L));
  }

  @Test
  public void testLessThanOrEqualPredicateConversion() {
    assertPredicateConversion(
        Expressions.lessThanOrEqual(
            Expressions.extract("payload", "$.age", "long"),
            40L),
        VARIANT_MESSAGE_WITH_LONG,
        FilterApi.ltEq(
            FilterApi.longColumn("payload.typed_value.age.typed_value"),
            40L));
  }

  @Test
  public void testInt32PredicateConversion() {
    assertPredicateConversion(
        Expressions.equal(
            Expressions.extract("payload", "$.int_field", "int"),
            100),
        VARIANT_MESSAGE_MULTI_TYPE,
        FilterApi.eq(
            FilterApi.intColumn("payload.typed_value.int_field.typed_value"),
            100));
  }

  @Test
  public void testFloatPredicateConversion() {
    assertPredicateConversion(
        Expressions.greaterThan(
            Expressions.extract("payload", "$.float_field", "float"),
            3.14f),
        VARIANT_MESSAGE_MULTI_TYPE,
        FilterApi.gt(
            FilterApi.floatColumn("payload.typed_value.float_field.typed_value"),
            3.14f));
  }

  @Test
  public void testDoublePredicateConversion() {
    assertPredicateConversion(
        Expressions.lessThan(
            Expressions.extract("payload", "$.double_field", "double"),
            100.5),
        VARIANT_MESSAGE_MULTI_TYPE,
        FilterApi.lt(
            FilterApi.doubleColumn("payload.typed_value.double_field.typed_value"),
            100.5));
  }

  @Test
  public void testBooleanPredicateConversion() {
    assertPredicateConversion(
        Expressions.equal(
            Expressions.extract("payload", "$.bool_field", "boolean"),
            true),
        VARIANT_MESSAGE_MULTI_TYPE,
        FilterApi.eq(
            FilterApi.booleanColumn("payload.typed_value.bool_field.typed_value"),
            true));
  }

  @Test
  public void testOrPredicateConvertible() {
    // OR with both sides convertible: tier = 'gold' OR tier = 'silver'
    Expression expr = Expressions.or(
        Expressions.equal("payload.typed_value.tier", "gold"),
        Expressions.equal("payload.typed_value.tier", "silver"));

    FilterPredicate expected = FilterApi.or(
        FilterApi.eq(
            FilterApi.binaryColumn("payload.typed_value.tier.typed_value"),
            Binary.fromString("gold")),
        FilterApi.eq(
            FilterApi.binaryColumn("payload.typed_value.tier.typed_value"),
            Binary.fromString("silver")));

    assertPredicateConversion(expr, VARIANT_MESSAGE, expected);
  }

  @Test
  public void testOrPredicateUnconvertible() {
    // OR where one side is unconvertible (unknown column)
    // Should return null because OR requires both sides to be convertible
    Expression expr = Expressions.or(
        Expressions.equal("payload.typed_value.tier", "gold"),
        Expressions.equal("payload.typed_value.unknown", "test"));

    FilterCompat.Filter filter = VariantParquetFilters.toParquetFilter(VARIANT_MESSAGE, expr);
    Assert.assertNull("OR with unconvertible side should return null", filter);
  }

  @Test
  public void testAndPredicateConvertible() {
    // AND with both sides convertible
    Expression expr = Expressions.and(
        Expressions.equal("payload.typed_value.tier", "gold"),
        Expressions.notEqual("payload.typed_value.tier", "bronze"));

    FilterPredicate expected = FilterApi.and(
        FilterApi.eq(
            FilterApi.binaryColumn("payload.typed_value.tier.typed_value"),
            Binary.fromString("gold")),
        FilterApi.notEq(
            FilterApi.binaryColumn("payload.typed_value.tier.typed_value"),
            Binary.fromString("bronze")));

    assertPredicateConversion(expr, VARIANT_MESSAGE, expected);
  }

  @Test
  public void testAndPredicateUnconvertible() {
    // AND where one side is unconvertible (unknown column)
    // Should return the convertible side (AND can drop unconvertible predicates)
    Expression expr = Expressions.and(
        Expressions.equal("payload.typed_value.tier", "gold"),
        Expressions.equal("payload.typed_value.unknown", "test"));

    FilterPredicate expected = FilterApi.eq(
        FilterApi.binaryColumn("payload.typed_value.tier.typed_value"),
        Binary.fromString("gold"));

    assertPredicateConversion(expr, VARIANT_MESSAGE, expected);
  }

  private void assertPredicateConversion(
      Expression expr, MessageType schema, FilterPredicate expected) {
    FilterCompat.Filter filter = VariantParquetFilters.toParquetFilter(schema, expr);

    Assert.assertTrue(filter instanceof FilterCompat.FilterPredicateCompat);
    FilterPredicate predicate = ((FilterCompat.FilterPredicateCompat) filter).getFilterPredicate();

    Assert.assertEquals(expected, predicate);
  }

  private void assertNullPredicateConversion(
      Expression expr, MessageType schema, String expectedColumnPath, boolean isNull) {
    FilterPredicate expected = isNull ?
        FilterApi.eq(
            FilterApi.binaryColumn(expectedColumnPath),
            null) :
        FilterApi.notEq(
            FilterApi.binaryColumn(expectedColumnPath),
            null);

    assertPredicateConversion(expr, schema, expected);
  }
}
