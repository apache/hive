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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestHiveIcebergUpdateWriter extends HiveIcebergWriterTestBase {
  private static final long TARGET_FILE_SIZE = 128 * 1024 * 1024;
  private static final JobID JOB_ID = new JobID("test", 0);
  private static final TaskAttemptID TASK_ATTEMPT_ID =
      new TaskAttemptID(JOB_ID.getJtIdentifier(), JOB_ID.getId(), TaskType.MAP, 0, 0);
  private static final Map<Integer, GenericRecord> UPDATED_RECORDS = ImmutableMap.of(
      29, record(29, "d"),
      61, record(61, "h"),
      89, record(81, "a"),
      100, record(132, "e"),
      122, record(142, "k"));

  /**
   * This test just runs sends the data through the DeleteWriter. Here we make sure that the correct rows are removed.
   * @throws IOException If here is an error
   */
  @Test
  public void testDelete() throws IOException {
    HiveIcebergWriter testWriter = new HiveIcebergBufferedDeleteWriter(table.schema(), table.specs(), fileFormat,
        hiveFileWriterFactory(), outputFileFactory(), table.io(), TARGET_FILE_SIZE, new HiveConf());

    update(table, testWriter);

    StructLikeSet expected = rowSetWithoutIds(RECORDS, UPDATED_RECORDS.keySet());
    StructLikeSet actual = actualRowSet(table);

    Assert.assertEquals("Table should contain expected rows", expected, actual);
  }

  /**
   * This test uses the UpdateWriter to check that the values are correctly updated.
   * @throws IOException If there is an error
   */
  @Test
  public void testUpdate() throws IOException {
    HiveIcebergWriter testWriter = new HiveIcebergUpdateWriter(table.schema(), table.specs(), table.spec().specId(),
        fileFormat, hiveFileWriterFactory(), outputFileFactory(), table.io(), TARGET_FILE_SIZE, TASK_ATTEMPT_ID,
        "table_name", new HiveConf());

    update(table, testWriter);

    StructLikeSet expected = rowSetWithoutIds(RECORDS, UPDATED_RECORDS.keySet());
    expected.addAll(UPDATED_RECORDS.values());
    StructLikeSet actual = actualRowSet(table);

    Assert.assertEquals("Table should contain expected rows", expected, actual);
  }

  private OutputFileFactory outputFileFactory() {
    return OutputFileFactory.builderFor(table, 1, 2)
        .format(fileFormat)
        .operationId("3")
        .build();
  }

  private HiveFileWriterFactory hiveFileWriterFactory() {
    return  new HiveFileWriterFactory(table, fileFormat, SCHEMA, null, fileFormat, null, null,
        null, null);
  }

  private static void update(Table table, HiveIcebergWriter testWriter) throws IOException {
    List<GenericRecord> updateRecords = updateRecords(table, UPDATED_RECORDS);

    Collections.sort(updateRecords, Comparator.comparing(a -> a.getField("data").toString()));

    Container<Record> container = new Container<>();
    for (Record deleteRecord : updateRecords) {
      container.set(deleteRecord);
      testWriter.write(container);
    }

    testWriter.close(false);

    RowDelta rowDelta = table.newRowDelta();
    testWriter.files().deleteFiles().forEach(rowDelta::addDeletes);
    testWriter.files().dataFiles().forEach(rowDelta::addRows);
    rowDelta.commit();
  }

  private static List<GenericRecord> updateRecords(Table table, Map<Integer, GenericRecord> updated)
      throws IOException {
    List<GenericRecord> updateRecords = Lists.newArrayListWithExpectedSize(updated.size());
    for (GenericRecord record : readRecords(table, schemaWithMeta(table))) {
      if (!updated.keySet().contains(record.getField("id"))) {
        continue;
      }

      GenericRecord updateRecord = GenericRecord.create(IcebergAcidUtil.createSerdeSchemaForUpdate(SCHEMA.columns()));
      int specId = (Integer) record.getField(MetadataColumns.SPEC_ID.name());
      updateRecord.setField(MetadataColumns.SPEC_ID.name(), specId);
      PartitionKey partitionKey = new PartitionKey(table.specs().get(specId), table.schema());
      partitionKey.partition(record);
      updateRecord.setField(MetadataColumns.PARTITION_COLUMN_NAME, partitionKey);
      updateRecord.setField(MetadataColumns.FILE_PATH.name(), record.getField(MetadataColumns.FILE_PATH.name()));
      updateRecord.setField(MetadataColumns.ROW_POSITION.name(), record.getField(MetadataColumns.ROW_POSITION.name()));

      SCHEMA.columns().forEach(field -> {
        updateRecord.setField(field.name(), updated.get(record.getField("id")).getField(field.name()));
        updateRecord.setField("__old_value_for_" + field.name(), record.getField(field.name()));
      });
      updateRecords.add(updateRecord);
    }
    return updateRecords;
  }

  private static GenericRecord record(Integer id, String data) {
    GenericRecord record = GenericRecord.create(SCHEMA);
    record.setField("id", id);
    record.setField("data", data);
    return record;
  }
}
