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

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Files;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.SerializableTable;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.FileHelpers;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.Catalogs;
import org.apache.iceberg.mr.hive.IcebergAcidUtil;
import org.apache.iceberg.mr.mapred.Container;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.util.CharSequenceSet;
import org.apache.iceberg.util.DeleteFileSet;
import org.apache.iceberg.util.Pair;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class TestHiveIcebergDeleteWriter extends HiveIcebergWriterTestBase {
  private static final Set<Integer> DELETED_IDS = Sets.newHashSet(29, 61, 89, 100, 122);
  private static final String DV_WRITER_QUERY_ID = "dv_writer_query";

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

      GenericRecord deleteRecord =
          GenericRecord.create(IcebergAcidUtil.createSerdeSchemaForDelete(SCHEMA.columns(), false));
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

  /**
   * A position delete file of a V2 table applies to every data file of its partition. After an upgrade to V3, a DV
   * writer merges it into the DVs of all of them, and decodes it once for the query.
   */
  @Test
  public void testDVWriterDecodesAPositionDeleteFileOncePerQuery() throws IOException, HiveException {
    Assume.assumeFalse(partitioned);
    helper.appendToTable(RECORDS);
    List<String> dataFiles = Lists.newArrayList();
    try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
      tasks.forEach(task -> dataFiles.add(task.file().location()));
    }
    Assert.assertEquals(2, dataFiles.size());
    // the first record of each data file
    DeleteFile positionDeletes = FileHelpers.writeDeleteFile(table,
        Files.localOutput(new File(temp.newFolder(), "pos-deletes")),
        dataFiles.stream().map(file -> Pair.<CharSequence, Long>of(file, 0L)).toList()).first();
    table.newRowDelta().addDeletes(positionDeletes).commit();
    TableOperations ops = ((BaseTable) table).operations();
    ops.commit(ops.current(), ops.current().upgradeToFormatVersion(3));

    // the writers of a Hive job find the format version in the properties of their table
    Table writerTable = SerializableTable.copyOf(table);
    writerTable.properties().put(TableProperties.FORMAT_VERSION, "3");
    Map<String, DeleteFileSet> rewritableDeletes = Maps.newHashMap();
    for (String dataFile : dataFiles) {
      DeleteFileSet deletes = DeleteFileSet.create();
      deletes.add(positionDeletes);
      rewritableDeletes.put(dataFile, deletes);
    }
    JobConf jc = new JobConf();
    jc.set(Catalogs.NAME, "dummy");
    HiveConf.setVar(jc, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    HiveConf.setVar(jc, HiveConf.ConfVars.HIVE_QUERY_ID, DV_WRITER_QUERY_ID);

    LlapProxy.setDaemon(true);
    try {
      HiveIcebergWriter writer = WriterBuilder.builderFor(writerTable, jc)
          .attemptID(new TaskAttemptID("test", 0, TaskType.MAP, 0, 0))
          .queryId(DV_WRITER_QUERY_ID)
          .operation(Context.Operation.DELETE)
          .rewritableDeletes(() -> rewritableDeletes)
          .build();
      Container<Record> container = new Container<>();
      for (Record deleteRecord : deleteRecords(table, Sets.newHashSet(43))) {
        container.set(deleteRecord);
        writer.write(container);
      }
      writer.close(false);

      Collection<DeleteFile> dvs = writer.files().deleteFiles();
      Assert.assertEquals("a DV per data file", 2, dvs.size());
      // the deleted record and the previous delete of the data file
      dvs.forEach(dv -> Assert.assertEquals(2, dv.recordCount()));
      boolean[] decodedAgain = { false };
      ObjectCacheFactory.getCache(jc, DV_WRITER_QUERY_ID, false).retrieve(positionDeletes.location(), () -> {
        decodedAgain[0] = true;
        return Boolean.TRUE;
      });
      Assert.assertFalse("the decoded position delete file is cached for the query", decodedAgain[0]);
    } finally {
      LlapProxy.setDaemon(false);
      ObjectCacheFactory.removeLlapQueryCache(DV_WRITER_QUERY_ID);
    }
  }
}
