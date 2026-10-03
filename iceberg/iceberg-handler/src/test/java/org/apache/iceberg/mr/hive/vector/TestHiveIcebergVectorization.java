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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.orc.VectorizedOrcInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mr.TestHelper;
import org.apache.iceberg.mr.hive.HiveIcebergStorageHandlerWithEngineBase;
import org.apache.iceberg.mr.hive.serde.objectinspector.IcebergObjectInspector;
import org.apache.iceberg.mr.hive.test.TestTables.TestTableType;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.iceberg.types.Types.NestedField.optional;

public class TestHiveIcebergVectorization extends HiveIcebergStorageHandlerWithEngineBase {

  @Parameters(name = "fileFormat={0}, catalog={1}, isVectorized={2}, formatVersion={3}")
  public static Collection<Object[]> parameters() {
    return HiveIcebergStorageHandlerWithEngineBase.getParameters(p ->
        p.testTableType() == TestTableType.HIVE_CATALOG &&
        p.isVectorized() &&
        p.formatVersion() >= 2);
  }

  /**
   * Tests HiveDeleteFilter implementation correctly filtering rows from VRBs.
   */
  @Test
  public void testHiveDeleteFilterWithEmptyBatches() {
    testVectorizedReadWithDeleteFilter(
        ImmutableMap.of(
            "parquet.block.size", "8192",
            "parquet.page.row.count.limit", "20")
    );
  }

  @Test
  public void testHiveDeleteFilter() {
    testVectorizedReadWithDeleteFilter(
        ImmutableMap.of()
    );
  }

  private void testVectorizedReadWithDeleteFilter(Map<String, String> props) {
    // The Avro "vectorized" case should actually serve as compareTo scenario to non-vectorized reading, because
    // there's no vectorization for Avro and it falls back to the non-vectorized implementation

    // Minimal schema to minimize resource footprint of what's coming next...
    Schema schema = new Schema(
        optional(1, "customer_id", Types.LongType.get()),
        optional(2, "customer_age", Types.IntegerType.get())
    );

    // Generate 106000 records so that we end up with multiple (104) batches to work with during the read.
    List<Record> records = TestHelper.generateRandomRecords(schema, 106000, 0L);

    // Fill id column with deterministic values
    for (int i = 0; i < records.size(); ++i) {
      records.get(i).setField("customer_id", (long) i);
    }

    testTables.createTable(shell, "vectordelete", schema, PartitionSpec.unpartitioned(), fileFormat, records,
        formatVersion, props);

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

    // 3999 deleted rows, scattered and in a range spanning whole batches
    validation.apply(2001);

    // 104499 deleted rows, most batches fully deleted
    shell.executeStatement("DELETE FROM vectordelete WHERE customer_id >= 5000");
    // 500 fewer rows as the above statement removed all even rows between 5000 and 6000 that were there previously
    validation.apply(1501);
  }

  /**
   * ORC can filter the rows of a batch by the pushed down predicate; row positions and deletes must still apply to
   * the right rows.
   */
  @Test
  public void testHiveDeleteFilterWithOrcRowFiltering() {
    Assume.assumeTrue(fileFormat == FileFormat.ORC);

    Schema schema = new Schema(optional(1, "customer_id", Types.LongType.get()));
    List<Record> records = TestHelper.generateRandomRecords(schema, 30000, 0L);
    for (int i = 0; i < records.size(); ++i) {
      records.get(i).setField("customer_id", (long) i);
    }
    testTables.createTable(shell, "vectordelete", schema, PartitionSpec.unpartitioned(), fileFormat, records, 2);
    shell.executeStatement("DELETE FROM vectordelete WHERE customer_id = 25000");

    shell.setHiveSessionValue("orc.sarg.to.filter", true);
    shell.setHiveSessionValue("orc.filter.use.selected", true);
    List<Object[]> result = shell.executeStatement(
        "select customer_id, ROW__POSITION from vectordelete where customer_id in (25000, 25001)");

    Assert.assertEquals(1, result.size());
    Assert.assertArrayEquals(new Object[] { 25001L, 25001L }, result.get(0));
  }

  @Test
  public void testHiveDeleteFilterWithFilteredBlock() {
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
    testTables.createTable(shell, "vectordelete", schema, PartitionSpec.unpartitioned(), fileFormat, records,
        formatVersion, props);

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
}
