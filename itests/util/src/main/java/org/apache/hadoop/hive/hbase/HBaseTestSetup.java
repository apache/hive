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

package org.apache.hadoop.hive.hbase;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QTestMiniClusters.QTestSetup;
import org.apache.hadoop.mapred.JobConf;
import org.apache.zookeeper.Watcher;

/**
 * HBaseTestSetup defines HBase-specific test fixtures which are
 * reused across testcases.
 */
public class HBaseTestSetup extends QTestSetup {

  private MiniHBaseCluster hbaseCluster;
  private HBaseTestingUtility util;
  private int zooKeeperPort;
  private Connection hbaseConn;

  private static final int NUM_REGIONSERVERS = 1;

  public Connection getConnection() {
    return this.hbaseConn;
  }

  @Override
  public void preTest(HiveConf conf) throws Exception {
    super.preTest(conf);

    setUpFixtures(conf);

    // Set some properties since HiveConf gets recreated for the new query
    Path hbaseRoot = util.getDefaultRootDirPath();
    conf.set(HConstants.HBASE_DIR, hbaseRoot.toUri().toString());

    String auxJars = conf.getAuxJars();
    auxJars = (StringUtils.isBlank(auxJars) ? "" : (auxJars + ",")) + "file://"
      + new JobConf(conf, HBaseConfiguration.class).getJar();
    auxJars += ",file://" + new JobConf(conf, HBaseSerDe.class).getJar();
    auxJars += ",file://" + new JobConf(conf, Watcher.class).getJar();
    conf.setAuxJars(auxJars);
  }

  private void setUpFixtures(HiveConf conf) throws Exception {
    /* We are not starting zookeeper server here because
     * QTestUtil already starts it.
     */
    int zkPort = conf.getInt("hive.zookeeper.client.port", -1);
    conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(zkPort));
    if ((zkPort == zooKeeperPort) && (hbaseCluster != null)) {
      return;
    }
    zooKeeperPort = zkPort;
    this.tearDown();

    // Fix needed due to dependency for hbase-mapreduce module
    System.setProperty("org.apache.hadoop.hbase.shaded.io.netty.packagePrefix",
        "org.apache.hadoop.hbase.shaded.");

    Configuration hbaseConf = HBaseConfiguration.create(conf);
    util = new HBaseTestingUtility(hbaseConf);

    util.startMiniDFSCluster(1);
    hbaseCluster = util.startMiniHBaseCluster(1, NUM_REGIONSERVERS);
    hbaseConn = util.getConnection();

    // opening the META table ensures that cluster is running
    Table meta = null;
    try {
      meta = hbaseConn.getTable(TableName.META_TABLE_NAME);
    } finally {
      if (meta != null) meta.close();
    }
    createHBaseTable();
    createAvroTable();
  }

  private void createHBaseTable() throws IOException {
    final String HBASE_TABLE_NAME = "HiveExternalTable";
    HTableDescriptor htableDesc = new HTableDescriptor(TableName.valueOf(HBASE_TABLE_NAME));
    HColumnDescriptor hcolDesc = new HColumnDescriptor("cf".getBytes());
    htableDesc.addFamily(hcolDesc);

    boolean [] booleans = new boolean [] { true, false, true };
    byte [] bytes = new byte [] { Byte.MIN_VALUE, -1, Byte.MAX_VALUE };
    short [] shorts = new short [] { Short.MIN_VALUE, -1, Short.MAX_VALUE };
    int [] ints = new int [] { Integer.MIN_VALUE, -1, Integer.MAX_VALUE };
    long [] longs = new long [] { Long.MIN_VALUE, -1, Long.MAX_VALUE };
    String [] strings = new String [] { "Hadoop, HBase,", "Hive", "Test Strings" };
    float [] floats = new float [] { Float.MIN_VALUE, -1.0F, Float.MAX_VALUE };
    double [] doubles = new double [] { Double.MIN_VALUE, -1.0, Double.MAX_VALUE };

    Admin hbaseAdmin = null;
    Table htable = null;
    try {
      hbaseAdmin = hbaseConn.getAdmin();
      if (Arrays.asList(hbaseAdmin.listTables()).contains(htableDesc)) {
        // if table is already in there, don't recreate.
        return;
      }
      hbaseAdmin.createTable(htableDesc);
      htable = hbaseConn.getTable(TableName.valueOf(HBASE_TABLE_NAME));

      // data
      Put[] puts = new Put[]{
        new Put("key-1".getBytes()), new Put("key-2".getBytes()), new Put("key-3".getBytes())};

      // store data
      for (int i = 0; i < puts.length; i++) {
        puts[i].addColumn("cf".getBytes(), "cq-boolean".getBytes(), Bytes.toBytes(booleans[i]));
        puts[i].addColumn("cf".getBytes(), "cq-byte".getBytes(), new byte[]{bytes[i]});
        puts[i].addColumn("cf".getBytes(), "cq-short".getBytes(), Bytes.toBytes(shorts[i]));
        puts[i].addColumn("cf".getBytes(), "cq-int".getBytes(), Bytes.toBytes(ints[i]));
        puts[i].addColumn("cf".getBytes(), "cq-long".getBytes(), Bytes.toBytes(longs[i]));
        puts[i].addColumn("cf".getBytes(), "cq-string".getBytes(), Bytes.toBytes(strings[i]));
        puts[i].addColumn("cf".getBytes(), "cq-float".getBytes(), Bytes.toBytes(floats[i]));
        puts[i].addColumn("cf".getBytes(), "cq-double".getBytes(), Bytes.toBytes(doubles[i]));

        htable.put(puts[i]);
      }
    } finally {
      if (htable != null) htable.close();
      if (hbaseAdmin != null) hbaseAdmin.close();
    }
  }

  private static byte[] createAvroRecordWithNestedTimestamp() throws IOException {
    File schemaFile = Paths.get(System.getProperty("test.data.dir"), "nested_ts.avsc").toFile();
    Schema schema = new Schema.Parser().parse(schemaFile);
    GenericData.Record rootRecord = new GenericData.Record(schema);
    rootRecord.put("id", "X338092");
    GenericData.Record dateRecord = new GenericData.Record(schema.getField("dischargedate").schema());
    final LocalDateTime _2022_07_05 = LocalDate.of(2022, 7, 5).atStartOfDay();
    // Store in UTC as required per Avro specification and as done by Hive in other parts of the system
    dateRecord.put("value", _2022_07_05.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli());
    rootRecord.put("dischargedate", dateRecord);

    try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
      try (DataFileWriter<GenericRecord> dataFileWriter
             = new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema))) {
        dataFileWriter.create(schema, out);
        dataFileWriter.append(rootRecord);
      }
      return out.toByteArray();
    }
  }

  private void createAvroTable() throws IOException {
    final TableName hbaseTable = TableName.valueOf("HiveAvroTable");
    HTableDescriptor htableDesc = new HTableDescriptor(hbaseTable);
    htableDesc.addFamily(new HColumnDescriptor("data".getBytes()));

    try (Admin hbaseAdmin = hbaseConn.getAdmin()) {
      hbaseAdmin.createTable(htableDesc);
      try (Table table = hbaseConn.getTable(hbaseTable)) {
        Put p = new Put("1".getBytes());
        p.add(new KeyValue("1".getBytes(), "data".getBytes(), "frV4".getBytes(),
          createAvroRecordWithNestedTimestamp()));
        table.put(p);
      }
    }
  }

  @Override
  public void tearDown() throws Exception {
    if (hbaseCluster != null) {
      util.shutdownMiniCluster();
      hbaseCluster = null;
    }
  }
}
