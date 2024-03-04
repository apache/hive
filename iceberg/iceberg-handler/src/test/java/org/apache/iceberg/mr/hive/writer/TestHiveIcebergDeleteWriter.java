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
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hive.ql.Context;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.hive.IcebergAcidUtil;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
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
        Comparator.comparing(a -> a.getField(MetadataColumns.PARTITION_COLUMN_NAME).toString()));

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

  private static List<GenericRecord> deleteRecords(Table table, Set<Integer> idsToRemove)
      throws IOException {
    List<GenericRecord> deleteRecords = Lists.newArrayListWithExpectedSize(idsToRemove.size());
    for (GenericRecord record : readRecords(table, schemaWithMeta(table))) {
      if (!idsToRemove.contains(record.getField("id"))) {
        continue;
      }

      GenericRecord deleteRecord = GenericRecord.create(IcebergAcidUtil.createSerdeSchemaForDelete(SCHEMA.columns()));
      int specId = (Integer) record.getField(MetadataColumns.SPEC_ID.name());
      deleteRecord.setField(MetadataColumns.SPEC_ID.name(), specId);
      PartitionKey partitionKey = new PartitionKey(table.specs().get(specId), table.schema());
      partitionKey.partition(record);
      deleteRecord.setField(MetadataColumns.PARTITION_COLUMN_NAME, partitionKey);
      deleteRecord.setField(MetadataColumns.FILE_PATH.name(), record.getField(MetadataColumns.FILE_PATH.name()));
      deleteRecord.setField(MetadataColumns.ROW_POSITION.name(), record.getField(MetadataColumns.ROW_POSITION.name()));

      SCHEMA.columns().forEach(field -> deleteRecord.setField(field.name(), record.getField(field.name())));
      deleteRecords.add(deleteRecord);
    }
    return deleteRecords;
  }

  private HiveIcebergWriter deleteWriter() {
    return writerBuilder.operation(Context.Operation.DELETE).build();
  }
}
