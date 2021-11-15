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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.test.KuduTestHarness;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hive.kudu.KuduStorageHandler.KUDU_MASTER_ADDRS_KEY;
import static org.apache.hadoop.hive.kudu.KuduStorageHandler.KUDU_TABLE_NAME_KEY;
import static org.apache.hadoop.hive.kudu.KuduTestUtils.getAllTypesSchema;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests for the KuduSerDe implementation.
 */
public class TestKuduSerDe {

  private static final String TABLE_NAME = "default.TestKuduSerDe";

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
  public void testSerDeRoundTrip() throws Exception {
    KuduSerDe serDe = new KuduSerDe();
    serDe.initialize(BASE_CONF, TBL_PROPS, null);

    PartialRow before = SCHEMA.newPartialRow();
    before.addByte("key", (byte) 1);
    before.addShort("int16", (short) 1);
    before.addInt("int32", 1);
    before.addLong("int64", 1L);
    before.addBoolean("bool", true);
    before.addFloat("float", 1.1f);
    before.addDouble("double", 1.1d);
    before.addString("string", "one");
    before.addBinary("binary", "one".getBytes(UTF_8));
    before.addTimestamp("timestamp", new Timestamp(NOW_MS));
    before.addDecimal("decimal", new BigDecimal("1.111"));
    before.setNull("null");
    before.addInt("default", 1);
    KuduWritable beforeWritable = new KuduWritable(before);
    Object object = serDe.deserialize(beforeWritable);


    // Capitalized `key` field to check for field case insensitivity.
    List<String> fieldNames = Arrays.asList("KEY", "int16", "int32", "int64", "bool", "float",
        "double", "string", "binary", "timestamp", "decimal", "null", "default");
    List<ObjectInspector> ois = Arrays.asList(
        PrimitiveObjectInspectorFactory.writableByteObjectInspector,
        PrimitiveObjectInspectorFactory.writableShortObjectInspector,
        PrimitiveObjectInspectorFactory.writableIntObjectInspector,
        PrimitiveObjectInspectorFactory.writableLongObjectInspector,
        PrimitiveObjectInspectorFactory.writableBooleanObjectInspector,
        PrimitiveObjectInspectorFactory.writableFloatObjectInspector,
        PrimitiveObjectInspectorFactory.writableDoubleObjectInspector,
        PrimitiveObjectInspectorFactory.writableStringObjectInspector,
        PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector,
        PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector,
        PrimitiveObjectInspectorFactory.writableStringObjectInspector,
        PrimitiveObjectInspectorFactory.writableIntObjectInspector
        // the "default" column is not set.
    );
    StandardStructObjectInspector objectInspector =
        ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, ois);
    KuduWritable afterWritable = serDe.serialize(object, objectInspector);
    PartialRow after = afterWritable.getPartialRow();

    for (int i = 0; i < SCHEMA.getColumnCount(); i++) {

      if (SCHEMA.getColumnByIndex(i).getType() == Type.BINARY) {
        assertArrayEquals("Columns not equal at index: " + i,
            before.getBinaryCopy(i), after.getBinaryCopy(i));
      } else {
        assertEquals("Columns not equal at index: " + i,
            before.getObject(i), after.getObject(i));
      }
    }
  }

  @Test
  public void testMissingMasters() throws Exception {
    KuduSerDe serDe = new KuduSerDe();

    Configuration conf = new Configuration(BASE_CONF);
    conf.set(HiveConf.ConfVars.HIVE_KUDU_MASTER_ADDRESSES_DEFAULT.varname, "");
    conf.unset(KUDU_MASTER_ADDRS_KEY);

    try {
      serDe.initialize(conf, TBL_PROPS, null);
      fail("Should fail on missing table");
    } catch (SerDeException ex) {
      assertThat(ex.getMessage(),
          containsString("Kudu master addresses are not specified in the table property"));
    }
  }

  @Test
  public void testMissingTable() throws Exception {
    KuduSerDe serDe = new KuduSerDe();

    Properties tblProps = new Properties();

    try {
      serDe.initialize(BASE_CONF, tblProps, null);
      fail("Should fail on missing table");
    } catch (SerDeException ex) {
      assertThat(ex.getMessage(), containsString("kudu.table_name is not set"));
    }
  }

  @Test
  public void testBadTable() throws Exception {
    KuduSerDe serDe = new KuduSerDe();

    Properties tblProps = new Properties();
    tblProps.setProperty(KUDU_TABLE_NAME_KEY, "default.notatable");

    try {
      serDe.initialize(BASE_CONF, tblProps, null);
      fail("Should fail on a bad table");
    } catch (SerDeException ex) {
      assertThat(ex.getMessage(),
          containsString("Kudu table does not exist: default.notatable"));
    }
  }
}
