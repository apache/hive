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

package org.apache.iceberg.mr.hive;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.FileMetadata;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.RandomGenericData;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.PositionDelete;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Comparators;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestIcebergAcidUtil {

  private static final Schema SCHEMA = new Schema(
      optional(1, "int_col", Types.IntegerType.get()),
      optional(2, "long_col", Types.LongType.get()),
      optional(3, "float_col", Types.FloatType.get()),
      optional(4, "double_col", Types.DoubleType.get()),
      optional(5, "boolean_col", Types.BooleanType.get()),
      optional(6, "string_col", Types.StringType.get()),
      optional(7, "binary_col", Types.BinaryType.get()),
      optional(8, "fixed_col", Types.FixedType.ofLength(5)),
      optional(9, "decimal_col", Types.DecimalType.of(20, 4)),
      optional(10, "date_col", Types.DateType.get()),
      optional(11, "time_col", Types.TimeType.get()),
      optional(12, "timestamp_col", Types.TimestampType.withoutZone()),
      optional(13, "timestamptz_col", Types.TimestampType.withZone()),
      optional(14, "timestamp_ns_col", Types.TimestampNanoType.withoutZone()),
      optional(15, "timestamptz_ns_col", Types.TimestampNanoType.withZone()),
      optional(16, "uuid_col", Types.UUIDType.get()));

  private static final Set<Type.TypeID> UNBUCKETED = EnumSet.of(Type.TypeID.BOOLEAN, Type.TypeID.FLOAT,
      Type.TypeID.DOUBLE);
  private static final Set<Type.TypeID> TRUNCATED = EnumSet.of(Type.TypeID.INTEGER, Type.TypeID.LONG,
      Type.TypeID.DECIMAL, Type.TypeID.STRING, Type.TypeID.BINARY);
  private static final Set<Type.TypeID> HOURED = EnumSet.of(Type.TypeID.TIMESTAMP, Type.TypeID.TIMESTAMP_NANO);
  private static final Set<Type.TypeID> DATED = EnumSet.of(Type.TypeID.DATE, Type.TypeID.TIMESTAMP,
      Type.TypeID.TIMESTAMP_NANO);

  @Test
  public void testPartitionRoundTrip() {
    List<Record> rows = rows();
    for (PartitionSpec spec : specs()) {
      PartitionKey partition = new PartitionKey(spec, SCHEMA);
      InternalRecordWrapper wrapper = new InternalRecordWrapper(SCHEMA.asStruct());
      for (Record row : rows) {
        partition.partition(wrapper.wrap(row));
        StructLike parsed = IcebergAcidUtil.deserializePartition(
            ByteBuffer.wrap(IcebergAcidUtil.serializePartition(partition, spec)), spec);
        Assert.assertEquals(spec.toString(),
            0, Comparators.forType(spec.partitionType()).compare(partition, parsed));
      }
    }
  }

  @Test
  public void testStringPartitionsStayDistinct() {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("string_col").truncate("string_col", 1).build();
    PartitionKey partition = new PartitionKey(spec, SCHEMA);
    Record row = GenericRecord.create(SCHEMA);
    Set<ByteBuffer> serialized = Sets.newHashSet();
    for (String value : Arrays.asList(null, "", "null", " ")) {
      row.setField("string_col", value);
      partition.partition(row);
      ByteBuffer bytes = ByteBuffer.wrap(IcebergAcidUtil.serializePartition(partition, spec));
      Assert.assertTrue(value, serialized.add(bytes));
      StructLike parsed = IcebergAcidUtil.deserializePartition(bytes, spec);
      Assert.assertEquals(value, parsed.get(0, String.class));
      Assert.assertEquals(value == null ? null : value.substring(0, Math.min(1, value.length())),
          parsed.get(1, String.class));
    }
  }

  @Test
  public void testPartitionSize() {
    Assert.assertEquals(0, serializedSize(PartitionSpec.unpartitioned()));
    Assert.assertEquals(1, serializedSize(PartitionSpec.builderFor(SCHEMA).alwaysNull("string_col").build(),
        (Object) null));
    Assert.assertEquals(5, serializedSize(PartitionSpec.builderFor(SCHEMA).identity("date_col").build(), 19_000));
    Assert.assertEquals(10,
        serializedSize(PartitionSpec.builderFor(SCHEMA).day("timestamp_col").bucket("int_col", 16).build(), 19_000, 7));
    Assert.assertEquals(6, serializedSize(PartitionSpec.builderFor(SCHEMA).identity("string_col").build(), "Smith"));
    Assert.assertEquals(4, serializedSize(PartitionSpec.builderFor(SCHEMA).truncate("decimal_col", 100).build(),
        new BigDecimal("12.3400")));
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).identity("string_col").day("timestamp_col")
        .bucket("int_col", 16).truncate("string_col", 3).build();
    Assert.assertEquals(17, serializedSize(spec, "eu", 19_000, 7, "abc"));
    Assert.assertEquals(15, serializedSize(spec, null, 19_000, 7, "abc"));
    Assert.assertEquals(10,
        serializedSize(PartitionSpec.builderFor(SCHEMA).bucket("int_col", 8).truncate("int_col", 100).build(), 3, 100));
    Assert.assertEquals(10, serializedSize(
        PartitionSpec.builderFor(SCHEMA).year("timestamp_col").bucket("timestamp_col", 4).build(), 56, 1));
    Assert.assertEquals(10, serializedSize(
        PartitionSpec.builderFor(SCHEMA).identity("string_col").alwaysNull("int_col").day("date_col").build(),
        "abc", null, 19_000));
  }

  @Test
  public void testMergeTaskPartition() throws IOException {
    PartitionSpec spec = PartitionSpec.builderFor(SCHEMA).withSpecId(3).identity("string_col").day("timestamp_col")
        .truncate("decimal_col", 100).build();
    PartitionKey partition = new PartitionKey(spec, SCHEMA);
    partition.partition(new InternalRecordWrapper(SCHEMA.asStruct()).wrap(rows().get(0)));
    DeleteFile deleteFile = FileMetadata.deleteFileBuilder(spec).ofPositionDeletes().withPath("/delete.parquet")
        .withFormat(FileFormat.PARQUET).withFileSizeInBytes(1).withRecordCount(1).withPartition(partition).build();
    Record delete = GenericRecord.create(new Schema(MetadataColumns.DELETE_FILE_PATH, MetadataColumns.DELETE_FILE_POS));
    delete.set(0, "/data.parquet");
    delete.set(1, 7L);

    // a merge task builds the record of an ordinary delete from the delete file
    try (CloseableIterator<Record> records = new IcebergAcidUtil.MergeTaskVirtualColumnAwareIterator<>(
        CloseableIterator.withClose(List.of(delete).iterator()), IcebergAcidUtil.createSerdeSchemaForDelete(), spec,
        deleteFile)) {
      Record rec = records.next();
      Assert.assertEquals(3, IcebergAcidUtil.parseSpecId(rec));
      Assert.assertEquals(ByteBuffer.wrap(IcebergAcidUtil.serializePartition(partition, spec)),
          IcebergAcidUtil.parseSerializedPartition(rec));
      StructLike parsed = IcebergAcidUtil.deserializePartition(IcebergAcidUtil.parseSerializedPartition(rec), spec);
      Assert.assertEquals(0, Comparators.forType(spec.partitionType()).compare(partition, parsed));
      PositionDelete<Record> positionDelete = IcebergAcidUtil.getPositionDelete(rec);
      Assert.assertEquals("/data.parquet", positionDelete.path());
      Assert.assertEquals(7L, positionDelete.pos());
    }
  }

  private static List<Record> rows() {
    Schema randomSchema = new Schema(SCHEMA.columns().stream()
        .filter(column -> column.type().typeId() != Type.TypeID.TIMESTAMP_NANO)
        .toList());
    List<Record> rows = Lists.newArrayList();
    long nanos = 0;
    for (Record random : RandomGenericData.generate(randomSchema, 20, 0L)) {
      Record row = GenericRecord.create(SCHEMA);
      randomSchema.columns().forEach(column -> row.setField(column.name(), random.getField(column.name())));
      nanos += 86_400_123_456_789L;
      row.setField("timestamp_ns_col", LocalDateTime.of(1990, 1, 1, 0, 0).plusNanos(nanos));
      row.setField("timestamptz_ns_col", OffsetDateTime.of(1990, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).plusNanos(nanos));
      rows.add(row);
    }
    // a null in one field only
    Record row = rows.get(0).copy();
    row.setField("string_col", null);
    rows.add(row);
    rows.add(GenericRecord.create(SCHEMA));
    return rows;
  }

  private static int serializedSize(PartitionSpec spec, Object... values) {
    PartitionKey partition = new PartitionKey(spec, SCHEMA);
    for (int pos = 0; pos < values.length; pos++) {
      partition.set(pos, values[pos]);
    }
    return IcebergAcidUtil.serializePartition(partition, spec).length;
  }

  // every transform of every type that supports it, in one spec per transform
  private static List<PartitionSpec> specs() {
    List<PartitionSpec> specs = Lists.newArrayList(PartitionSpec.unpartitioned());
    for (Types.NestedField column : SCHEMA.columns()) {
      String name = column.name();
      Type.TypeID type = column.type().typeId();
      add(specs, builder -> builder.identity(name));
      add(specs, builder -> builder.alwaysNull(name));
      if (!UNBUCKETED.contains(type)) {
        add(specs, builder -> builder.bucket(name, 7));
      }
      if (TRUNCATED.contains(type)) {
        add(specs, builder -> builder.truncate(name, 3));
      }
      if (DATED.contains(type)) {
        add(specs, builder -> builder.year(name));
        add(specs, builder -> builder.month(name));
        add(specs, builder -> builder.day(name));
      }
      if (HOURED.contains(type)) {
        add(specs, builder -> builder.hour(name));
      }
    }
    // specs of several fields: of different transforms, of the same source, with a void field
    add(specs, builder -> builder.identity("string_col").day("timestamp_col").bucket("int_col", 16)
        .truncate("string_col", 3));
    add(specs, builder -> builder.bucket("int_col", 8).truncate("int_col", 100));
    add(specs, builder -> builder.year("timestamp_col").bucket("timestamp_col", 4));
    add(specs, builder -> builder.identity("string_col").alwaysNull("int_col").day("date_col"));
    add(specs, builder -> builder.identity("string_col").day("timestamptz_col").bucket("uuid_col", 4)
        .truncate("decimal_col", 10).identity("boolean_col").identity("binary_col"));
    return specs;
  }

  private static void add(List<PartitionSpec> specs, Consumer<PartitionSpec.Builder> fields) {
    PartitionSpec.Builder builder = PartitionSpec.builderFor(SCHEMA);
    fields.accept(builder);
    specs.add(builder.build());
  }
}
