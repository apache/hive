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

package org.apache.iceberg.mr.hive.writer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hive.ql.Context;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics2;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.hive.IcebergAcidUtil;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestHiveIcebergDeleteWriter extends HiveIcebergWriterTestBase {
  private static final Set<Integer> DELETED_IDS = Sets.newHashSet(29, 61, 89, 100, 122);

  @Test
  public void testDelete() throws IOException {
    HiveIcebergWriter testWriter = deleteWriter();

    List<GenericRecord> deleteRecords = deleteRecords(table, DELETED_IDS);

    Collections.sort(deleteRecords,
        Comparator.comparing(a -> a.getField(MetadataColumns.FILE_PATH.name()).toString()));

    CharSequenceSet expectedDataFiles = CharSequenceSet.empty();
    Container<Record> container = new Container<>();
    for (Record deleteRecord : deleteRecords) {
      container.set(deleteRecord);
      testWriter.write(container);
      expectedDataFiles.add((String) deleteRecord.getField(MetadataColumns.FILE_PATH.name()));
    }

    testWriter.close(false);

    RowDelta rowDelta = table.newRowDelta();
    testWriter.files().deleteFiles().forEach(rowDelta::addDeletes);
    Collection<CharSequence> actualDataFiles = testWriter.files().referencedDataFiles();
    rowDelta.commit();

    Assert.assertTrue("Actual :" + actualDataFiles + " Expected: " + expectedDataFiles,
        actualDataFiles.containsAll(expectedDataFiles));

    StructLikeSet expected = rowSetWithoutIds(RECORDS, DELETED_IDS);
    StructLikeSet actual = actualRowSet(table);

    Assert.assertEquals("Table should contain expected rows", expected, actual);
  }

  @Test
  public void testDeleteWithNestedPartitionSource() throws IOException {
    // the source of the new partition field is a field of a struct column
    table.updateSchema()
        .addColumn("address", Types.StructType.of(Types.NestedField.optional(0, "city", Types.StringType.get())))
        .commit();
    table.updateSpec().addField(Expressions.truncate("address.city", 1)).commit();
    Types.StructType addressType = table.schema().findType("address").asStructType();
    List<Record> records = Lists.newArrayList();
    for (int id = 200; id < 206; id++) {
      Record address = GenericRecord.create(addressType);
      address.setField("city", id % 2 == 0 ? "x" : "y");
      Record record = GenericRecord.create(table.schema());
      record.setField("id", id);
      record.setField("data", "d" + id);
      record.setField("address", address);
      records.add(record);
    }
    helper.appendToTable(records);
    Set<Integer> deletedIds = Sets.newHashSet(29, 61, 200, 201, 204);
    StructLikeSet expected = actualRowSet(table);
    expected.removeIf(row -> deletedIds.contains(row.get(0, Integer.class)));

    HiveIcebergWriter testWriter = deleteWriter();
    Container<Record> container = new Container<>();
    for (Record deleteRecord : deleteRecords(table, deletedIds)) {
      container.set(deleteRecord);
      testWriter.write(container);
    }
    testWriter.close(false);
    RowDelta rowDelta = table.newRowDelta();
    testWriter.files().deleteFiles().forEach(rowDelta::addDeletes);
    rowDelta.commit();

    Assert.assertEquals("Table should contain expected rows", expected, actualRowSet(table));
    // a partition-filtered read finds the deletes, which carry the partitions of the deleted records
    List<Integer> filtered = Lists.newArrayList();
    try (CloseableIterable<Record> reader =
             IcebergGenerics2.read(table).where(Expressions.equal("address.city", "x")).build()) {
      reader.forEach(record -> filtered.add((Integer) record.getField("id")));
    }
    Assert.assertEquals(List.of(202), filtered);
  }

  private static List<GenericRecord> deleteRecords(Table table, Set<Integer> idsToRemove)
      throws IOException {
    // the readers carry the partition of the data file of a record
    Map<String, ByteBuffer> partitions = Maps.newHashMap();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      tasks.forEach(task -> partitions.put(task.file().location(),
          ByteBuffer.wrap(IcebergAcidUtil.serializePartition(task.file().partition(), task.spec()))));
    }
    List<GenericRecord> deleteRecords = Lists.newArrayListWithExpectedSize(idsToRemove.size());
    Schema deleteSchema = IcebergAcidUtil.createSerdeSchemaForDelete();
    for (GenericRecord record : readRecords(table, schemaWithMeta(table))) {
      if (!idsToRemove.contains(record.getField("id"))) {
        continue;
      }

      GenericRecord deleteRecord = GenericRecord.create(deleteSchema);
      deleteSchema.columns().forEach(field -> deleteRecord.setField(field.name(), record.getField(field.name())));
      deleteRecord.setField(MetadataColumns.PARTITION_COLUMN_NAME,
          partitions.get(record.getField(MetadataColumns.FILE_PATH.name())));
      deleteRecords.add(deleteRecord);
    }
    return deleteRecords;
  }

  private HiveIcebergWriter deleteWriter() {
    return writerBuilder.operation(Context.Operation.DELETE).build();
  }
}
