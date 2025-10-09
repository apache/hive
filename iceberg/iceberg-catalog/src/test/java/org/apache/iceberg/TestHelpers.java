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

package org.apache.iceberg;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.BoundSetPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionVisitors;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.junit.jupiter.api.Assertions;

import static org.assertj.core.api.Assertions.assertThat;

public class TestHelpers {

  private TestHelpers() {
  }

  public static <T> T assertAndUnwrap(Expression expr, Class<T> expected) {
    Assertions.assertTrue(
        expected.isInstance(expr), "Expression should have expected type: " + expected);
    return expected.cast(expr);
  }

  @SuppressWarnings("unchecked")
  public static <T> BoundPredicate<T> assertAndUnwrap(Expression expr) {
    Assertions.assertTrue(
        expr instanceof BoundPredicate, "Expression should be a bound predicate: " + expr);
    return (BoundPredicate<T>) expr;
  }

  @SuppressWarnings("unchecked")
  public static <T> BoundSetPredicate<T> assertAndUnwrapBoundSet(Expression expr) {
    Assertions.assertTrue(
        expr instanceof BoundSetPredicate, "Expression should be a bound set predicate: " + expr);
    return (BoundSetPredicate<T>) expr;
  }

  @SuppressWarnings("unchecked")
  public static <T> UnboundPredicate<T> assertAndUnwrapUnbound(Expression expr) {
    Assertions.assertTrue(
        expr instanceof UnboundPredicate, "Expression should be an unbound predicate: " + expr);
    return (UnboundPredicate<T>) expr;
  }

  public static void assertAllReferencesBound(String message, Expression expr) {
    ExpressionVisitors.visit(expr, new CheckReferencesBound(message));
  }

  @SuppressWarnings("unchecked")
  public static <T> T roundTripSerialize(T type) throws IOException, ClassNotFoundException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(type);
    }

    try (ObjectInputStream in = new ObjectInputStream(
        new ByteArrayInputStream(bytes.toByteArray()))) {
      return (T) in.readObject();
    }
  }

  public static void assertSameSchemaList(List<Schema> list1, List<Schema> list2) {
    assertThat(list1)
        .as("Should have same number of schemas in both lists")
        .hasSameSizeAs(list2);

    IntStream.range(0, list1.size())
        .forEach(
            index -> {
              Schema schema1 = list1.get(index);
              Schema schema2 = list2.get(index);
              Assertions.assertEquals(schema1.schemaId(), schema2.schemaId(),
                  "Should have matching schema id");
              Assertions.assertEquals(schema1.asStruct(), schema2.asStruct(),
                  "Should have matching schema struct");
            });
  }

  public static void assertSerializedMetadata(Table expected, Table actual) {
    Assertions.assertEquals(expected.name(), actual.name(), "Name must match");
    Assertions.assertEquals(expected.location(), actual.location(), "Location must match");
    Assertions.assertEquals(expected.properties(), actual.properties(), "Props must match");
    Assertions.assertEquals(expected.schema().asStruct(), actual.schema().asStruct(),
        "Schema must match");
    Assertions.assertEquals(expected.spec(), actual.spec(), "Spec must match");
    Assertions.assertEquals(expected.sortOrder(), actual.sortOrder(), "Sort order must match");
  }

  public static void assertSerializedAndLoadedMetadata(Table expected, Table actual) {
    assertSerializedMetadata(expected, actual);
    Assertions.assertEquals(expected.specs(), actual.specs(), "Specs must match");
    Assertions.assertEquals(expected.sortOrders(), actual.sortOrders(), "Sort orders must match");
    Assertions.assertEquals(expected.currentSnapshot(), actual.currentSnapshot(),
        "Current snapshot must match");
    Assertions.assertEquals(expected.snapshots(), actual.snapshots(), "Snapshots must match");
    Assertions.assertEquals(expected.history(), actual.history(), "History must match");
  }

  public static void assertSameSchemaMap(Map<Integer, Schema> map1, Map<Integer, Schema> map2) {
    assertThat(map1)
        .as("Should have same number of schemas in both maps")
        .hasSameSizeAs(map2);

    map1.forEach(
        (schemaId, schema1) -> {
          Schema schema2 = map2.get(schemaId);
          Assertions.assertNotNull(
              schema2, String.format("Schema ID %s does not exist in map: %s", schemaId, map2));

          Assertions.assertEquals(schema1.schemaId(), schema2.schemaId(),
              "Should have matching schema id");
          Assertions.assertTrue(
              schema1.sameSchema(schema2), String.format(
                  "Should be the same schema. Schema 1: %s, schema 2: %s", schema1, schema2));
        });
  }

  private static class CheckReferencesBound extends ExpressionVisitors.ExpressionVisitor<Void> {
    private final String message;

    CheckReferencesBound(String message) {
      this.message = message;
    }

    @Override
    public <T> Void predicate(UnboundPredicate<T> pred) {
      Assertions.fail(message + ": Found unbound predicate: " + pred);
      return null;
    }
  }

  /**
   * Implements {@link StructLike#get} for passing data in tests.
   */
  public static class Row implements StructLike {
    public static Row of(Object... values) {
      return new Row(values);
    }

    private final Object[] values;

    private Row(Object... values) {
      this.values = values;
    }

    @Override
    public int size() {
      return values.length;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T get(int pos, Class<T> javaClass) {
      return javaClass.cast(values[pos]);
    }

    @Override
    public <T> void set(int pos, T value) {
      throw new UnsupportedOperationException("Setting values is not supported");
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }

      Row that = (Row) other;

      return Arrays.equals(values, that.values);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(values);
    }
  }

  public static class TestFieldSummary implements ManifestFile.PartitionFieldSummary {
    private final boolean containsNull;
    private final Boolean containsNaN;
    private final ByteBuffer lowerBound;
    private final ByteBuffer upperBound;

    public TestFieldSummary(boolean containsNull, ByteBuffer lowerBound, ByteBuffer upperBound) {
      this(containsNull, null, lowerBound, upperBound);
    }

    public TestFieldSummary(boolean containsNull, Boolean containsNaN, ByteBuffer lowerBound, ByteBuffer upperBound) {
      this.containsNull = containsNull;
      this.containsNaN = containsNaN;
      this.lowerBound = lowerBound;
      this.upperBound = upperBound;
    }

    @Override
    public boolean containsNull() {
      return containsNull;
    }

    // commented out because this method was only added to ManifestFile.PartitionFieldSummary after the 0.11.0 release
//    @Override
//    public Boolean containsNaN() {
//      return containsNaN;
//    }

    @Override
    public ByteBuffer lowerBound() {
      return lowerBound;
    }

    @Override
    public ByteBuffer upperBound() {
      return upperBound;
    }

    @Override
    public ManifestFile.PartitionFieldSummary copy() {
      return this;
    }
  }

  public static class TestManifestFile implements ManifestFile {
    private final String path;
    private final long length;
    private final int specId;
    private final ManifestContent content;
    private final Long snapshotId;
    private final Integer addedFiles;
    private final Long addedRows;
    private final Integer existingFiles;
    private final Long existingRows;
    private final Integer deletedFiles;
    private final Long deletedRows;
    private final List<PartitionFieldSummary> partitions;

    public TestManifestFile(String path, long length, int specId, Long snapshotId,
                            Integer addedFiles, Integer existingFiles, Integer deletedFiles,
                            List<PartitionFieldSummary> partitions) {
      this.path = path;
      this.length = length;
      this.specId = specId;
      this.content = ManifestContent.DATA;
      this.snapshotId = snapshotId;
      this.addedFiles = addedFiles;
      this.addedRows = null;
      this.existingFiles = existingFiles;
      this.existingRows = null;
      this.deletedFiles = deletedFiles;
      this.deletedRows = null;
      this.partitions = partitions;
    }

    public TestManifestFile(String path, long length, int specId, ManifestContent content, Long snapshotId,
                            Integer addedFiles, Long addedRows, Integer existingFiles,
                            Long existingRows, Integer deletedFiles, Long deletedRows,
                            List<PartitionFieldSummary> partitions) {
      this.path = path;
      this.length = length;
      this.specId = specId;
      this.content = content;
      this.snapshotId = snapshotId;
      this.addedFiles = addedFiles;
      this.addedRows = addedRows;
      this.existingFiles = existingFiles;
      this.existingRows = existingRows;
      this.deletedFiles = deletedFiles;
      this.deletedRows = deletedRows;
      this.partitions = partitions;
    }

    @Override
    public String path() {
      return path;
    }

    @Override
    public long length() {
      return length;
    }

    @Override
    public int partitionSpecId() {
      return specId;
    }

    @Override
    public ManifestContent content() {
      return content;
    }

    @Override
    public long sequenceNumber() {
      return 0;
    }

    @Override
    public long minSequenceNumber() {
      return 0;
    }

    @Override
    public Long snapshotId() {
      return snapshotId;
    }

    @Override
    public Integer addedFilesCount() {
      return addedFiles;
    }

    @Override
    public Long addedRowsCount() {
      return addedRows;
    }

    @Override
    public Integer existingFilesCount() {
      return existingFiles;
    }

    @Override
    public Long existingRowsCount() {
      return existingRows;
    }

    @Override
    public Integer deletedFilesCount() {
      return deletedFiles;
    }

    @Override
    public Long deletedRowsCount() {
      return deletedRows;
    }

    @Override
    public List<PartitionFieldSummary> partitions() {
      return partitions;
    }

    @Override
    public ManifestFile copy() {
      return this;
    }
  }

  public static class TestDataFile implements DataFile {
    private final String path;
    private final StructLike partition;
    private final long recordCount;
    private final Map<Integer, Long> valueCounts;
    private final Map<Integer, Long> nullValueCounts;
    private final Map<Integer, Long> nanValueCounts;
    private final Map<Integer, ByteBuffer> lowerBounds;
    private final Map<Integer, ByteBuffer> upperBounds;

    public TestDataFile(String path, StructLike partition, long recordCount) {
      this(path, partition, recordCount, null, null, null, null, null);
    }

    public TestDataFile(String path, StructLike partition, long recordCount,
                        Map<Integer, Long> valueCounts,
                        Map<Integer, Long> nullValueCounts,
                        Map<Integer, Long> nanValueCounts,
                        Map<Integer, ByteBuffer> lowerBounds,
                        Map<Integer, ByteBuffer> upperBounds) {
      this.path = path;
      this.partition = partition;
      this.recordCount = recordCount;
      this.valueCounts = valueCounts;
      this.nullValueCounts = nullValueCounts;
      this.nanValueCounts = nanValueCounts;
      this.lowerBounds = lowerBounds;
      this.upperBounds = upperBounds;
    }

    @Override
    public Long pos() {
      return null;
    }

    @Override
    public int specId() {
      return 0;
    }

    @Override
    public CharSequence path() {
      return path;
    }

    @Override
    public FileFormat format() {
      return FileFormat.fromFileName(path());
    }

    @Override
    public StructLike partition() {
      return partition;
    }

    @Override
    public long recordCount() {
      return recordCount;
    }

    @Override
    public long fileSizeInBytes() {
      return 0;
    }

    @Override
    public Map<Integer, Long> columnSizes() {
      return null;
    }

    @Override
    public Map<Integer, Long> valueCounts() {
      return valueCounts;
    }

    @Override
    public Map<Integer, Long> nullValueCounts() {
      return nullValueCounts;
    }

    @Override
    public Map<Integer, Long> nanValueCounts() {
      return nanValueCounts;
    }

    @Override
    public Map<Integer, ByteBuffer> lowerBounds() {
      return lowerBounds;
    }

    @Override
    public Map<Integer, ByteBuffer> upperBounds() {
      return upperBounds;
    }

    @Override
    public ByteBuffer keyMetadata() {
      return null;
    }

    @Override
    public DataFile copy() {
      return this;
    }

    @Override
    public DataFile copyWithoutStats() {
      return this;
    }

    @Override
    public List<Long> splitOffsets() {
      return null;
    }
  }
}
