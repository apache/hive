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

  @Test
  public void testEqualsPredicateConversion() {
    Expression expr = Expressions.equal("payload.typed_value.tier", "gold");

    FilterCompat.Filter filter = VariantParquetFilters.convert(VARIANT_MESSAGE, expr);

    Assert.assertTrue(filter instanceof FilterCompat.FilterPredicateCompat);
    FilterPredicate predicate =
        ((FilterCompat.FilterPredicateCompat) filter).getFilterPredicate();
    FilterPredicate expected =
        FilterApi.eq(
            FilterApi.binaryColumn("payload.typed_value.tier.typed_value"),
            Binary.fromString("gold"));
    Assert.assertEquals(expected, predicate);
  }

  @Test
  public void testUnknownColumn() {
    Expression expr = Expressions.equal("payload.typed_value.unknown", "gold");

    FilterCompat.Filter filter = VariantParquetFilters.convert(VARIANT_MESSAGE, expr);

    Assert.assertNull(filter);
  }
}
