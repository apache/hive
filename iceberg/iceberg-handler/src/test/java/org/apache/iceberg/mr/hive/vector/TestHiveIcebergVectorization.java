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

package org.apache.iceberg.mr.hive.vector;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.orc.VectorizedOrcInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.mr.hive.HiveIcebergStorageHandlerWithEngineBase;
import org.apache.iceberg.mr.hive.TestTables;
import org.apache.iceberg.mr.hive.serde.objectinspector.IcebergObjectInspector;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.junit.Assume.assumeTrue;

public class TestHiveIcebergVectorization extends HiveIcebergStorageHandlerWithEngineBase {

  /**
   * Tests the row iterator implementation (HiveRow, HiveBatchContext.RowIterator) along with HiveValueConverter by
   * reading in values from all supported types via VRBs, and iterating on its records 1-by-1 while comparing with the
   * expected Iceberg record instances.
   * @throws Exception any test failure
   */
  @Test
  public void testRowIterator() throws Exception {
    assumeTrue("Tests a format-independent feature", isVectorized && FileFormat.ORC.equals(fileFormat));

    // Create a table with sample data with all supported types, those unsupported for vectorization are commented out
    Schema allSchema = new Schema(
        optional(1, "binary_col", Types.BinaryType.get()),
        optional(2, "boolean_col", Types.BooleanType.get()),
        optional(3, "date_col", Types.DateType.get()),
        optional(4, "decimal_col", Types.DecimalType.of(6, 4)),
        optional(5, "double_col", Types.DoubleType.get()),
        optional(6, "fixed_col", Types.FixedType.ofLength(4)),
        optional(7, "float_col", Types.FloatType.get()),
        optional(8, "int_col", Types.IntegerType.get()),
        optional(9, "long_col", Types.LongType.get()),
        optional(10, "string_col", Types.StringType.get()),
//        optional(11, "uuid_col", Types.UUIDType.get()),
        optional(12, "timestamp_col", Types.TimestampType.withoutZone())
//        optional(13, "timestamp_with_tz_col", Types.TimestampType.withZone()),
//        optional(14, "time_col", Types.TimeType.get())
    );

    // Generate 10 records for all column types into our test table
    List<Record> records = TestHelper.generateRandomRecords(allSchema, 10, 0L);
    Table table = testTables.createTable(shell, "temptable", allSchema, fileFormat, records);

    // Identify data file location - expected to be 1 file exactly
    Path dataFilePath = new Path(Lists.newArrayList(Lists.newArrayList(table.newScan().planTasks().iterator()).get(0)
        .files().iterator()).get(0).file().path().toString());

    // Generate a mock vectorized read job
    JobConf jobConf = prepareMockJob(allSchema, dataFilePath);

    // Simulates HiveVectorizedReader creating an ORC record reader (implementation inside Hive QL code)
    VectorizedOrcInputFormat inputFormat = new VectorizedOrcInputFormat();
    RecordReader<NullWritable, VectorizedRowBatch> internalVectorizedRecordReader =
        inputFormat.getRecordReader(new FileSplit(dataFilePath, 0L, Long.MAX_VALUE, new String[]{}), jobConf,
            new MockReporter());
    HiveBatchIterator hiveBatchIterator = new HiveBatchIterator(
        internalVectorizedRecordReader, jobConf, null, null, null);

    // Expected to be one batch exactly
    HiveBatchContext hiveBatchContext = hiveBatchIterator.next();
    CloseableIterator<HiveRow> hiveRowIterator = hiveBatchContext.rowIterator();

    // Iterator for the expected records
    Iterator<Record> genericRowIterator = records.iterator();

    // Compare record data provided by Hive with those provided by GenericRecord implementation of Iceberg
    while (hiveRowIterator.hasNext() && genericRowIterator.hasNext()) {
      HiveRow hiveRow = hiveRowIterator.next();
      Record hiveRecord = HiveValueConverter.convert(allSchema, hiveRow);
      Record genericRecord = genericRowIterator.next();

      // Will do a deep comparison on values
      Assert.assertEquals(genericRecord, hiveRecord);
    }

    // The two iterators both should be at the end by now
    Assert.assertEquals(genericRowIterator.hasNext(), hiveRowIterator.hasNext());
  }

  /**
   * Tests HiveDeleteFilter implementation correctly filtering rows from VRBs.
   */
  @Test
  public void testHiveDeleteFilterWithEmptyBatches() {
    Map<String, String> props = Maps.newHashMap();
    props.put("parquet.block.size", "8192");
    props.put("parquet.page.row.count.limit", "20");
    testVectorizedReadWithDeleteFilter(props);
  }

  @Test
  public void testHiveDeleteFilter() {
    testVectorizedReadWithDeleteFilter(Collections.emptyMap());
  }

  private void testVectorizedReadWithDeleteFilter(Map<String, String> props) {
    // The Avro "vectorized" case should actually serve as compareTo scenario to non-vectorized reading, because
    // there's no vectorization for Avro and it falls back to the non-vectorized implementation
    Assume.assumeTrue(isVectorized && testTableType == TestTables.TestTableType.HIVE_CATALOG);

    // Minimal schema to minimize resource footprint of what's coming next...
    Schema schema = new Schema(
        optional(1, "customer_id", Types.LongType.get()),
        optional(2, "customer_age", Types.IntegerType.get())
    );

    // Generate 106000 records so that we end up with multiple (104) batches to work with during the read.
    // Sadly there's no way to control DeleteFilter.DEFAULT_SET_FILTER_THRESHOLD from this test, so we need two
    // select statements: one where we have less than DEFAULT_SET_FILTER_THRESHOLD deletes, & one where there's more.
    // This should test both in-memory and streaming delete application implementations of Iceberg.
    List<Record> records = TestHelper.generateRandomRecords(schema, 106000, 0L);

    // Fill id column with deterministic values
    for (int i = 0; i < records.size(); ++i) {
      records.get(i).setField("customer_id", (long) i);
    }

    testTables.createTable(shell, "vectordelete", schema, PartitionSpec.unpartitioned(), fileFormat, records, 2, props);

    // Delete every odd row until 6000
    shell.executeStatement("DELETE FROM vectordelete WHERE customer_id % 2 = 1 and customer_id < 6000");

    // Delete a whole batch's worth of data overlapping into the previous and next partial batches (batch size is 1024)
    shell.executeStatement("DELETE FROM vectordelete WHERE 1000 < customer_id and customer_id < 3000");

    Function<Integer, Void> validation = expectedCount -> {
      List<Object[]> result = shell.executeStatement("select * from vectordelete where customer_id < 6000");
      Assert.assertEquals(expectedCount.intValue(), result.size());

      for (Object[] row : result) {
        long id = (long) row[0];
        Assert.assertTrue("Found row with odd customer_id", id % 2 == 0);
        Assert.assertTrue("Found a row with customer_id between 1000 and 3000 (both exclusive)",
            id <= 1000 || 3000 <= id);
        Assert.assertTrue("Found a row with customer_id >= 6000, i.e. where clause is not in effect.", id < 6000);
      }

      return null;
    };

    // Test with all deletes loaded into memory (DeletePositionIndex), as we have only 3999 deletes in 2 delete files
    validation.apply(2001);

    // Test with streamed reading of deletes and data rows as we have 104499 deletes in 3 delete files with:
    shell.executeStatement("DELETE FROM vectordelete WHERE customer_id >= 5000");
    // 500 fewer rows as the above statement removed all even rows between 5000 and 6000 that were there previously
    validation.apply(1501);
  }

  @Test
  public void testHiveDeleteFilterWithFilteredParquetBlock() {
    Assume.assumeTrue(
        isVectorized && testTableType == TestTables.TestTableType.HIVE_CATALOG && fileFormat == FileFormat.PARQUET);

    Schema schema = new Schema(
        optional(1, "customer_id", Types.LongType.get()),
        optional(2, "customer_age", Types.IntegerType.get()),
        optional(3, "date_col", Types.DateType.get())
    );

    // Generate 10600 records so that we end up with multiple batches to work with during the read.
    List<Record> records = TestHelper.generateRandomRecords(schema, 10600, 0L);

    // Fill id and date column with deterministic values
    for (int i = 0; i < records.size(); ++i) {
      records.get(i).setField("customer_id", (long) i);
      if (i % 3 == 0) {
        records.get(i).setField("date_col", Date.valueOf("2022-04-28"));
      } else if (i % 3 == 1) {
        records.get(i).setField("date_col", Date.valueOf("2022-04-29"));
      } else {
        records.get(i).setField("date_col", Date.valueOf("2022-04-30"));
      }
    }
    Map<String, String> props = Maps.newHashMap();
    props.put("parquet.block.size", "8192");
    testTables.createTable(shell, "vectordelete", schema, PartitionSpec.unpartitioned(), fileFormat, records, 2, props);

    // Check there is some rows before we do an update
    List<Object[]> results = shell.executeStatement("select * from vectordelete where date_col=date'2022-04-29'");

    Assert.assertNotEquals(0, results.size());

    // Capture the number of entries with both column, to validate after update value
    List<Object[]> postUpdateResult = shell.executeStatement(
        "select * from vectordelete where date_col=date'2022-04-29' OR date_col=date'2022-04-30'");

    Assert.assertNotEquals(0, postUpdateResult.size());

    // Do an update on the column, and check if the count is 0, since we changed the value for that column
    shell.executeStatement("update vectordelete set date_col=date'2022-04-30' where date_col=date'2022-04-29'");
    results = shell.executeStatement("select * from vectordelete where date_col=date'2022-04-29'");
    Assert.assertEquals(0, results.size());

    results = shell.executeStatement("select * from vectordelete where date_col=date'2022-04-30'");
    Assert.assertEquals(postUpdateResult.size(), results.size());
  }

  /**
   * Creates a mock vectorized ORC read job for a particular data file and a read schema (projecting on all columns)
   * @param schema readSchema
   * @param dataFilePath data file path
   * @return JobConf instance
   * @throws HiveException any failure during job creation
   */
  static JobConf prepareMockJob(Schema schema, Path dataFilePath) throws HiveException {
    StructObjectInspector oi = (StructObjectInspector) IcebergObjectInspector.create(schema);
    String hiveColumnNames = String.join(",", oi.getAllStructFieldRefs().stream()
        .map(sf -> sf.getFieldName()).collect(Collectors.toList()));
    String hiveTypeInfoNames = String.join(",", oi.getAllStructFieldRefs().stream()
        .map(sf -> sf.getFieldObjectInspector().getTypeName()).collect(Collectors.toList()));

    // facepalm: getTypeName returns detailed info for decimal type.. :/
    hiveTypeInfoNames = hiveTypeInfoNames.replaceAll("decimal\\(\\d+,\\d+\\)", "decimal");

    Configuration conf = new Configuration();
    conf.set(IOConstants.COLUMNS, hiveColumnNames);
    conf.set(IOConstants.COLUMNS_TYPES, hiveTypeInfoNames);
    conf.setBoolean(ColumnProjectionUtils.READ_ALL_COLUMNS, true);

    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    HiveConf.setVar(conf, HiveConf.ConfVars.PLAN, "//tmp");
    JobConf vectorJob = new JobConf(conf);

    VectorizedOrcInputFormat.setInputPaths(vectorJob, dataFilePath);

    MapWork mapWork = new MapWork();
    VectorizedRowBatchCtx rbCtx = new VectorizedRowBatchCtx();
    rbCtx.init(oi, new String[0]);
    mapWork.setVectorMode(true);
    mapWork.setVectorizedRowBatchCtx(rbCtx);
    mapWork.deriveLlap(conf, false);
    Utilities.setMapWork(vectorJob, mapWork);
    return vectorJob;
  }

  private static class MockReporter implements Reporter {

    @Override
    public void setStatus(String s) {
    }

    @Override
    public Counters.Counter getCounter(Enum<?> anEnum) {
      return null;
    }

    @Override
    public Counters.Counter getCounter(String s, String s1) {
      return null;
    }

    @Override
    public void incrCounter(Enum<?> anEnum, long l) {
    }

    @Override
    public void incrCounter(String s, String s1, long l) {
    }

    @Override
    public InputSplit getInputSplit() throws UnsupportedOperationException {
      return null;
    }

    @Override
    public float getProgress() {
      return 0;
    }

    @Override
    public void progress() {
    }
  }
}
