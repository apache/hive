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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.kudu.KuduOutputFormat.KuduRecordWriter;
import org.apache.hadoop.mapred.JobConf;
import org.apache.kudu.Schema;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hive.kudu.KuduStorageHandler.KUDU_MASTER_ADDRS_KEY;
import static org.apache.hadoop.hive.kudu.KuduStorageHandler.KUDU_TABLE_NAME_KEY;
import static org.apache.hadoop.hive.kudu.KuduTestUtils.getAllTypesSchema;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the KuduOutputFormat implementation.
 */
public class TestKuduOutputFormat {

  private static final String TABLE_NAME = "default.TestKuduOutputFormat";

  private static final Schema SCHEMA = getAllTypesSchema();

  private static final Configuration BASE_CONF = new Configuration();

  private static final Properties TBL_PROPS = new Properties();

  private static final long NOW_MS = System.currentTimeMillis();

  @Rule
  public KuduTestHarness harness = new KuduTestHarness();

  @Before
  public void setUp() throws Exception {
    // Set the base configuration values.
    BASE_CONF.set(KUDU_MASTER_ADDRS_KEY, harness.getMasterAddressesAsString());
    TBL_PROPS.setProperty(KUDU_TABLE_NAME_KEY, TABLE_NAME);

    // Create the test Kudu table.
    CreateTableOptions options = new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of("key"));
    harness.getClient().createTable(TABLE_NAME, SCHEMA, options);
  }

  @Test
  public void testGoodRow() throws Exception {
    KuduOutputFormat outputFormat = new KuduOutputFormat();

    KuduRecordWriter writer =
        (KuduRecordWriter) outputFormat.getHiveRecordWriter(new JobConf(BASE_CONF),
            null, null, false, TBL_PROPS, null);

    // Write a good row.
    try {
      PartialRow row = SCHEMA.newPartialRow();
      row.addByte("key", (byte) 1);
      row.addShort("int16", (short) 1);
      row.addInt("int32", 1);
      row.addLong("int64", 1L);
      row.addBoolean("bool", true);
      row.addFloat("float", 1.1f);
      row.addDouble("double", 1.1d);
      row.addString("string", "one");
      row.addBinary("binary", "one".getBytes(UTF_8));
      row.addTimestamp("timestamp", new Timestamp(NOW_MS));
      row.addDecimal("decimal", new BigDecimal("1.111"));
      row.setNull("null");
      // Not setting the "default" column.
      KuduWritable writable = new KuduWritable(row);
      writer.write(writable);
    } finally {
      writer.close(false);
    }

    // Verify the written row.
    KuduClient client = harness.getClient();
    KuduTable table = client.openTable(TABLE_NAME);
    KuduScanner scanner = client.newScannerBuilder(table).build();

    List<RowResult> results = new ArrayList<>();
    for (RowResult result : scanner) {
      results.add(result);
    }
    assertEquals(1, results.size());
    RowResult result = results.get(0);

    assertEquals((byte) 1, result.getByte(0));
    assertEquals((short) 1, result.getShort(1));
    assertEquals(1, result.getInt(2));
    assertEquals(1L, result.getLong(3));
    assertTrue(result.getBoolean(4));
    assertEquals(1.1f, result.getFloat(5), 0);
    assertEquals(1.1d, result.getDouble(6), 0);
    assertEquals("one", result.getString(7));
    assertEquals("one", new String(result.getBinaryCopy(8), UTF_8));
    assertEquals(NOW_MS, result.getTimestamp(9).getTime());
    assertEquals(new BigDecimal("1.111"), result.getDecimal(10));
    assertTrue(result.isNull(11));
    assertEquals(1, result.getInt(12)); // default.
  }

  @Test
  public void testBadRow() throws Exception {
    KuduOutputFormat outputFormat = new KuduOutputFormat();

    KuduRecordWriter writer =
        (KuduRecordWriter) outputFormat.getHiveRecordWriter(new JobConf(BASE_CONF),
            null, null, false, TBL_PROPS, null);

    // Write an empty row.
    try {
      PartialRow row = SCHEMA.newPartialRow();
      KuduWritable writable = new KuduWritable(row);
      writer.write(writable);
    } catch (KuduException ex) {
      assertThat(ex.getMessage(), containsString("Primary key column key is not set"));
    } finally {
      writer.close(false);
    }
  }

  @Test
  public void testMissingTable() throws Exception {
    KuduOutputFormat outputFormat = new KuduOutputFormat();

    Properties tblProps = new Properties();

    try {
      outputFormat.getHiveRecordWriter(new JobConf(BASE_CONF),
          null, null, false, tblProps, null);
      fail("Should fail on missing table");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage(), containsString("kudu.table_name is not set"));
    }
  }

  @Test
  public void testBadTable() throws Exception {
    KuduOutputFormat outputFormat = new KuduOutputFormat();

    Properties tblProps = new Properties();
    tblProps.setProperty(KUDU_TABLE_NAME_KEY, "default.notatable");

    try {
      outputFormat.getHiveRecordWriter(new JobConf(BASE_CONF),
          null, null, false, tblProps, null);
      fail("Should fail on a bad table");
    } catch (IllegalArgumentException ex) {
      assertThat(ex.getMessage(),
          containsString("Kudu table does not exist: default.notatable"));
    }
  }
}
