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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.PathUtil;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.ByteBuffers;
import org.apache.iceberg.variants.ShreddedObject;
import org.apache.iceberg.variants.Variant;
import org.apache.iceberg.variants.VariantMetadata;
import org.apache.iceberg.variants.Variants;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;
import static org.assertj.core.api.Assertions.assertThat;

public class TestVariantFilePruner {

  private static final Schema SCHEMA = new Schema(
      required(1, "id", Types.IntegerType.get()),
      optional(2, "payload", Types.VariantType.get()));

  private static final Expression EVENT_ID_15 =
      Expressions.equal(Expressions.extract("payload", "$.event_id", "long"), 15L);

  @Test
  public void testNoPrunerWithoutTopLevelExtractConjunct() {
    assertThat(VariantFilePruner.of(SCHEMA, null, true)).isNull();
    assertThat(VariantFilePruner.of(SCHEMA, Expressions.equal("id", 5), true)).isNull();
    assertThat(VariantFilePruner.of(SCHEMA, Expressions.or(EVENT_ID_15, Expressions.equal("id", 5)), true)).isNull();
    assertThat(VariantFilePruner.of(SCHEMA, Expressions.not(Expressions.and(EVENT_ID_15, EVENT_ID_15)), true))
        .isNull();
    assertThat(VariantFilePruner.of(SCHEMA, Expressions.not(Expressions.or(EVENT_ID_15, EVENT_ID_15)), true))
        .isNull();
    assertThat(VariantFilePruner.of(SCHEMA, Expressions.not(Expressions.equal("id", 5)), true)).isNull();
    // DECIMAL fallback of HiveIcebergFilterFactory, cannot bind to the table schema
    assertThat(VariantFilePruner.of(SCHEMA, Expressions.equal("payload.typed_value.price", 5), true)).isNull();
    // a time-travel scan before the variant column was added
    assertThat(VariantFilePruner.of(SCHEMA.select("id"), EVENT_ID_15, true)).isNull();
  }

  @Test
  public void testNoPrunerForExtractOnNonVariantColumn() {
    // variant_get accepts any struct column
    Schema schema = new Schema(
        required(1, "id", Types.IntegerType.get()),
        optional(2, "s", Types.StructType.of(
            optional(3, "metadata", Types.BinaryType.get()),
            optional(4, "value", Types.BinaryType.get()))));
    Expression onStruct = Expressions.equal(Expressions.extract("s", "$.x", "long"), 1L);

    assertThat(VariantFilePruner.of(schema, onStruct, true)).isNull();
    assertThat(VariantFilePruner.of(schema, Expressions.not(onStruct), true)).isNull();
  }

  @Test
  public void testCaseInsensitiveColumn() {
    Expression upperCase = Expressions.equal(Expressions.extract("PAYLOAD", "$.event_id", "long"), 15L);
    assertThat(VariantFilePruner.of(SCHEMA, upperCase, true)).isNull();
    assertThat(VariantFilePruner.of(SCHEMA, upperCase, false).variantColumns()).containsExactly("payload");
  }

  @Test
  public void testPrunesWithManifestBounds() {
    ByteBuffer lower = manifestBound(1);
    ByteBuffer upper = manifestBound(10);
    FileScanTask task = task(lower, upper);

    VariantFilePruner pruner =
        VariantFilePruner.of(SCHEMA, Expressions.and(Expressions.equal("id", 5), EVENT_ID_15), true);
    assertThat(pruner.variantColumns()).containsExactly("payload");
    assertThat(pruner.mayMatch(task)).isFalse();

    Expression eventId5 = Expressions.equal(Expressions.extract("payload", "$.event_id", "long"), 5L);
    assertThat(VariantFilePruner.of(SCHEMA, eventId5, true).mayMatch(task)).isTrue();

    assertThat(lower.order()).isEqualTo(ByteOrder.BIG_ENDIAN);
    assertThat(lower.position()).isZero();
    assertThat(task.file().lowerBounds().get(2)).isSameAs(lower);
  }

  @Test
  public void testNegatedExtractPredicate() {
    FileScanTask task = task(manifestBound(1), manifestBound(10));
    Expression notAtMost10 =
        Expressions.not(Expressions.lessThanOrEqual(Expressions.extract("payload", "$.event_id", "long"), 10L));
    Expression notAtMost9 =
        Expressions.not(Expressions.lessThanOrEqual(Expressions.extract("payload", "$.event_id", "long"), 9L));

    assertThat(VariantFilePruner.of(SCHEMA, notAtMost10, true).mayMatch(task)).isFalse();
    assertThat(VariantFilePruner.of(SCHEMA, notAtMost9, true).mayMatch(task)).isTrue();
  }

  @Test
  public void testKeepsFileWithoutVariantBounds() {
    assertThat(VariantFilePruner.of(SCHEMA, EVENT_ID_15, true).mayMatch(task(null, null))).isTrue();
  }

  /**
   * A variant bound for {@code $.event_id} as a manifest returns it: in a big-endian buffer.
   */
  private static ByteBuffer manifestBound(int eventId) {
    String path = PathUtil.toNormalizedPath(List.of("event_id"));
    VariantMetadata metadata = Variants.metadata(path);
    ShreddedObject bound = Variants.object(metadata);
    bound.put(path, Variants.of((byte) eventId));
    ByteBuffer serialized = Conversions.toByteBuffer(Types.VariantType.get(), Variant.of(metadata, bound));
    return ByteBuffer.wrap(ByteBuffers.toByteArray(serialized));
  }

  private static FileScanTask task(ByteBuffer lower, ByteBuffer upper) {
    DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath("file:/tmp/data.parquet")
        .withFormat(FileFormat.PARQUET)
        .withFileSizeInBytes(100L)
        .withMetrics(new Metrics(10L, null, Map.of(2, 10L), Map.of(2, 0L), null,
            lower == null ? null : Map.of(2, lower), upper == null ? null : Map.of(2, upper)))
        .build();
    FileScanTask task = Mockito.mock(FileScanTask.class);
    Mockito.when(task.file()).thenReturn(file);
    Mockito.when(task.spec()).thenReturn(PartitionSpec.unpartitioned());
    return task;
  }
}
