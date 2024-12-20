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
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics2;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeSet;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;

public class HiveIcebergWriterTestBase {
  // Schema passed to create tables
  public static final Schema SCHEMA = new Schema(
      Types.NestedField.required(1, "id", Types.IntegerType.get()),
      Types.NestedField.required(2, "data", Types.StringType.get())
  );

  public static final List<Record> RECORDS = TestHelper.RecordsBuilder.newInstance(SCHEMA)
      .add(29, "a")
      .add(43, "b")
      .add(61, "c")
      .add(89, "d")
      .add(100, "e")
      .add(121, "f")
      .add(122, "g")
      .build();

  private final HadoopTables tables = new HadoopTables(new HiveConf());
  private TestHelper helper;
  protected Table table;
  protected WriterBuilder writerBuilder;

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  @Parameterized.Parameter(0)
  public FileFormat fileFormat;

  @Parameterized.Parameter(1)
  public boolean partitioned;

  @Parameterized.Parameter(2)
  public boolean skipRowData;

  @Parameterized.Parameters(name = "fileFormat={0}, partitioned={1}, skipRowData={2}")
  public static Collection<Object[]> parameters() {
    return Lists.newArrayList(new Object[][] {
        { FileFormat.PARQUET, true, true },
        { FileFormat.ORC, true, true },
        { FileFormat.AVRO, true, true },
        { FileFormat.PARQUET, false, true },
        { FileFormat.PARQUET, true, false },
        { FileFormat.ORC, true, false },
        { FileFormat.AVRO, true, false },
        { FileFormat.PARQUET, false, false },
// Skip this until the ORC reader is fixed - test only issue
//        { FileFormat.ORC, false },
        { FileFormat.AVRO, false, true },
        { FileFormat.AVRO, false, false }
    });
  }

  @Before
  public void init() throws IOException {
    File location = temp.newFolder(fileFormat.name());
    Assert.assertTrue(location.delete());

    PartitionSpec spec = !partitioned ? PartitionSpec.unpartitioned() :
        PartitionSpec.builderFor(SCHEMA)
            .bucket("data", 3)
            .build();
    this.helper = new TestHelper(new HiveConf(), tables, location.toString(), SCHEMA, spec, fileFormat,
        Collections.singletonMap(WriterBuilder.ICEBERG_DELETE_SKIPROWDATA, String.valueOf(skipRowData)), temp);
    this.table = helper.createTable();
    helper.appendToTable(RECORDS);

    TableOperations ops = ((BaseTable) table).operations();
    TableMetadata meta = ops.current();
    ops.commit(meta, meta.upgradeToFormatVersion(2));

    JobID jobId = new JobID("test", 0);
    TaskAttemptID taskAttemptID =
        new TaskAttemptID(jobId.getJtIdentifier(), jobId.getId(), TaskType.MAP, 0, 0);
    writerBuilder = WriterBuilder.builderFor(table)
        .attemptID(taskAttemptID)
        .queryId("Q_ID")
        .tableName("dummy");
  }

  @After
  public void cleanUp() {
    tables.dropTable(helper.table().location());
  }

  protected StructLikeSet rowSetWithoutIds(List<Record> records, Set<Integer> idToDelete) {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    records.stream()
        .filter(row -> !idToDelete.contains(row.getField("id")))
        .forEach(set::add);
    return set;
  }

  protected static List<GenericRecord> readRecords(Table table, Schema schema) throws IOException {
    List<GenericRecord> records = Lists.newArrayList();
    try (CloseableIterable<Record> reader = IcebergGenerics2.read(table).project(schema).build()) {
      // For these tests we can be sure that the records are GenericRecords, and we need it to easily access fields
      reader.forEach(record -> records.add((GenericRecord) record));
    }

    return records;
  }

  protected static StructLikeSet actualRowSet(Table table) throws IOException {
    StructLikeSet set = StructLikeSet.create(table.schema().asStruct());
    readRecords(table, table.schema()).forEach(set::add);
    return set;
  }

  protected static Schema schemaWithMeta(Table table) {
    List<Types.NestedField> cols = Lists.newArrayListWithCapacity(table.schema().columns().size() + 4);
    cols.addAll(table.schema().columns());
    cols.add(MetadataColumns.ROW_POSITION);
    cols.add(MetadataColumns.FILE_PATH);
    cols.add(MetadataColumns.SPEC_ID);
    cols.add(MetadataColumns.metadataColumn(table, MetadataColumns.PARTITION_COLUMN_NAME));
    return new Schema(cols);
  }
}
