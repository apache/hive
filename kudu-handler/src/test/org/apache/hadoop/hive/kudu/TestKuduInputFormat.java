/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.kudu;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.kudu.KuduInputFormat.KuduInputSplit;
import org.apache.hadoop.hive.kudu.KuduInputFormat.KuduRecordReader;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hive.kudu.KuduHiveUtils.toHiveType;
import static org.apache.hadoop.hive.kudu.KuduStorageHandler.KUDU_MASTER_ADDRS_KEY;
import static org.apache.hadoop.hive.kudu.KuduStorageHandler.KUDU_TABLE_NAME_KEY;
import static org.apache.hadoop.hive.kudu.KuduTestUtils.getAllTypesSchema;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the KuduInputFormat implementation.
 */
public class TestKuduInputFormat {

  private static final String TABLE_NAME = "default.TestKuduInputFormat";

  private static final Schema SCHEMA = getAllTypesSchema();

  private static final Configuration BASE_CONF = new Configuration();

  private static final long NOW_MS = System.currentTimeMillis();

  private static final PartialRow ROW;
  static {
    ROW = SCHEMA.newPartialRow();
    ROW.addByte("key", (byte) 1);
    ROW.addShort("int16", (short) 1);
    ROW.addInt("int32", 1);
    ROW.addLong("int64", 1L);
    ROW.addBoolean("bool", true);
    ROW.addFloat("float", 1.1f);
    ROW.addDouble("double", 1.1d);
    ROW.addString("string", "one");
    ROW.addBinary("binary", "one".getBytes(UTF_8));
    ROW.addTimestamp("timestamp", new Timestamp(NOW_MS));
    ROW.addDecimal("decimal", new BigDecimal("1.111"));
    ROW.setNull("null");
    // Not setting the "default" column.
  }

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() throws Exception {
    // Set the base configuration values.
    BASE_CONF.set(KUDU_MASTER_ADDRS_KEY, harness.getMasterAddressesAsString());
    BASE_CONF.set(KUDU_TABLE_NAME_KEY, TABLE_NAME);
    BASE_CONF.set(FileInputFormat.INPUT_DIR, "dummy");

    // Create the test Kudu table.
    CreateTableOptions options = new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of("key"));
    harness.getClient().createTable(TABLE_NAME, SCHEMA, options);

    // Insert a test row.
    KuduTable table = harness.getClient().openTable(TABLE_NAME);
    KuduSession session = harness.getClient().newSession();
    Insert insert = table.newInsert();
    PartialRow insertRow = insert.getRow();
    // Use KuduWritable, to populate the insert row.
    new KuduWritable(ROW).populateRow(insertRow);
    session.apply(insert);
    session.close();
  }

  @Test
  public void testAllColumns() throws Exception {
    KuduInputFormat input = new KuduInputFormat();

    JobConf jobConf = new JobConf(BASE_CONF);
    String columnsStr = SCHEMA.getColumns().stream()
        .map(ColumnSchema::getName)
        .collect(Collectors.joining(","));
    jobConf.set(serdeConstants.LIST_COLUMNS, columnsStr);

    InputSplit[] splits = input.getSplits(jobConf, 1);
    assertEquals(1, splits.length);
    KuduInputSplit split = (KuduInputSplit) splits[0];

    KuduRecordReader reader =
        (KuduRecordReader) input.getRecordReader(split, jobConf, null);
    assertTrue(reader.nextKeyValue());
    RowResult value = reader.getCurrentValue().getRowResult();
    verifyRow(value);
    assertFalse(reader.nextKeyValue());
  }

  @Test
  public void testProjection() throws Exception {
    KuduInputFormat input = new KuduInputFormat();


    JobConf jobConf = new JobConf(BASE_CONF);
    jobConf.set(serdeConstants.LIST_COLUMNS, "bool,key");

    InputSplit[] splits = input.getSplits(jobConf, 1);
    assertEquals(1, splits.length);
    KuduInputSplit split = (KuduInputSplit) splits[0];

    KuduRecordReader reader =
        (KuduRecordReader) input.getRecordReader(split, jobConf, null);
    assertTrue(reader.nextKeyValue());
    RowResult value = reader.getCurrentValue().getRowResult();
    assertEquals(2, value.getSchema().getColumnCount());
    assertTrue(value.getBoolean(0));
    assertEquals((byte) 1, value.getByte(1));
    assertFalse(reader.nextKeyValue());
  }

  @Test
  public void testMissingTable() throws Exception {
    KuduInputFormat input = new KuduInputFormat();

    JobConf jobConf = new JobConf(BASE_CONF);
    jobConf.unset(KUDU_TABLE_NAME_KEY);
    jobConf.set(serdeConstants.LIST_COLUMNS, "key");

    try {
      input.getSplits(jobConf, 1);
      fail("Should fail on missing master addresses");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage(), containsString("kudu.table_name is not set"));
    }
  }

  @Test
  public void testBadTable() throws Exception {
    KuduInputFormat input = new KuduInputFormat();

    JobConf jobConf = new JobConf(BASE_CONF);
    jobConf.set(KUDU_TABLE_NAME_KEY, "default.notatable");
    jobConf.set(serdeConstants.LIST_COLUMNS, "key");

    try {
      input.getSplits(jobConf, 1);
      fail("Should fail on a bad table");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage(),
          containsString("Kudu table does not exist: default.notatable"));
    }
  }

  @Test
  public void testMissingColumn() throws Exception {
    KuduInputFormat input = new KuduInputFormat();

    JobConf jobConf = new JobConf(BASE_CONF);
    jobConf.set(serdeConstants.LIST_COLUMNS, "missing");

    try {
      input.getSplits(jobConf, 1);
      fail("Should fail on missing column");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage(), containsString("Unknown column: missing"));
    }
  }

  @Test
  public void testMultipleSplits() throws Exception {
    String tableName = "default.twoPartitionTable";
    Schema schema = new Schema(Arrays.asList(
        new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("string", Type.STRING).build()
    ));
    CreateTableOptions options = new CreateTableOptions()
        .addHashPartitions(Collections.singletonList("key"), 2);
    harness.getClient().createTable(tableName, schema, options);

    // Insert multiple test rows.
    KuduTable table = harness.getClient().openTable(tableName);
    KuduSession session = harness.getClient().newSession();
    Insert insert1 = table.newInsert();
    PartialRow row1 = insert1.getRow();
    row1.addInt("key", 1);
    row1.addString("string", "one");
    session.apply(insert1);
    Insert insert2 = table.newInsert();
    PartialRow row2 = insert2.getRow();
    row2.addInt("key", 2);
    row2.addString("string", "two");
    session.apply(insert2);
    session.close();

    KuduInputFormat input = new KuduInputFormat();

    JobConf jobConf = new JobConf(BASE_CONF);
    jobConf.set(KUDU_TABLE_NAME_KEY, tableName);
    jobConf.set(serdeConstants.LIST_COLUMNS, "key");

    InputSplit[] splits = input.getSplits(jobConf, 1);
    assertEquals(2, splits.length);
  }

  @Test
  public void testPredicate() throws Exception {
    // Insert a second test row that will be filtered out.
    KuduTable table = harness.getClient().openTable(TABLE_NAME);
    KuduSession session = harness.getClient().newSession();
    Insert insert = table.newInsert();
    PartialRow row = insert.getRow();
    row.addByte("key", (byte) 2);
    row.addShort("int16", (short) 2);
    row.addInt("int32", 2);
    row.addLong("int64", 2L);
    row.addBoolean("bool", false);
    row.addFloat("float", 2.2f);
    row.addDouble("double", 2.2d);
    row.addString("string", "two");
    row.addBinary("binary", "two".getBytes(UTF_8));
    row.addTimestamp("timestamp", new Timestamp(NOW_MS + 1));
    row.addDecimal("decimal", new BigDecimal("2.222"));
    row.setNull("null");
    // Not setting the "default" column.
    session.apply(insert);
    session.close();

    KuduInputFormat input = new KuduInputFormat();

    // Test an equality predicate for each column.
    for (ColumnSchema col : SCHEMA.getColumns()) {
      // Skip null and default columns because they don't have a value to use.
      // Skip binary columns because binary predicates are not supported. (HIVE-11370)
      if (col.getName().equals("null") || col.getName().equals("default") ||
          col.getName().equals("binary")) {
        continue;
      }

      JobConf jobConf = new JobConf(BASE_CONF);
      String columnsStr = SCHEMA.getColumns().stream()
          .map(ColumnSchema::getName)
          .collect(Collectors.joining(","));
      jobConf.set(serdeConstants.LIST_COLUMNS, columnsStr);

      PrimitiveTypeInfo typeInfo = toHiveType(col.getType(), col.getTypeAttributes());
      ExprNodeDesc colExpr =  new ExprNodeColumnDesc(typeInfo, col.getName(), null, false);
      ExprNodeDesc constExpr = new ExprNodeConstantDesc(typeInfo, ROW.getObject(col.getName()));
      List<ExprNodeDesc> children = Lists.newArrayList();
      children.add(colExpr);
      children.add(constExpr);
      ExprNodeGenericFuncDesc predicateExpr =
          new ExprNodeGenericFuncDesc(typeInfo, new GenericUDFOPEqual(), children);

      String filterExpr = SerializationUtilities.serializeExpression(predicateExpr);
      jobConf.set(TableScanDesc.FILTER_EXPR_CONF_STR, filterExpr);

      InputSplit[] splits = input.getSplits(jobConf, 1);
      assertEquals(1, splits.length);
      KuduInputSplit split = (KuduInputSplit) splits[0];

      KuduRecordReader reader =
          (KuduRecordReader) input.getRecordReader(split, jobConf, null);
      assertTrue(reader.nextKeyValue());
      RowResult value = reader.getCurrentValue().getRowResult();
      verifyRow(value);
      assertFalse("Extra row on column: " + col.getName(), reader.nextKeyValue());
    }
  }
  private void verifyRow(RowResult value) {
    assertEquals(SCHEMA.getColumnCount(), value.getSchema().getColumnCount());
    assertEquals(ROW.getByte(0), value.getByte(0));
    assertEquals(ROW.getShort(1), value.getShort(1));
    assertEquals(ROW.getInt(2), value.getInt(2));
    assertEquals(ROW.getLong(3), value.getLong(3));
    assertEquals(ROW.getBoolean(4), value.getBoolean(4));
    assertEquals(ROW.getFloat(5), value.getFloat(5), 0);
    assertEquals(ROW.getDouble(6), value.getDouble(6), 0);
    assertEquals(ROW.getString(7), value.getString(7));
    assertArrayEquals(ROW.getBinaryCopy(8), value.getBinaryCopy(8));
    assertEquals(ROW.getTimestamp(9), value.getTimestamp(9));
    assertEquals(ROW.getDecimal(10), value.getDecimal(10));
    assertTrue(value.isNull(11));
    assertEquals(1, value.getInt(12)); // default.
  }
}
