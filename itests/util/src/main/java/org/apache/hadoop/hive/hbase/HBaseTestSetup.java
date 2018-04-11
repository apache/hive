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

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapred.JobConf;
import org.apache.zookeeper.Watcher;

/**
 * HBaseTestSetup defines HBase-specific test fixtures which are
 * reused across testcases.
 */
public class HBaseTestSetup {

  private MiniHBaseCluster hbaseCluster;
  private int zooKeeperPort;
  private String hbaseRoot;
  private Connection hbaseConn;

  private static final int NUM_REGIONSERVERS = 1;

  public Connection getConnection() {
    return this.hbaseConn;
  }

  void preTest(HiveConf conf) throws Exception {

    setUpFixtures(conf);

    conf.set("hbase.rootdir", hbaseRoot);
    conf.set("hbase.master", hbaseCluster.getMaster().getServerName().getHostAndPort());
    conf.set("hbase.zookeeper.property.clientPort", Integer.toString(zooKeeperPort));
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
    if ((zkPort == zooKeeperPort) && (hbaseCluster != null)) {
      return;
    }
    zooKeeperPort = zkPort;
    String tmpdir =  System.getProperty("test.tmp.dir");
    this.tearDown();
    conf.set("hbase.master", "local");

    hbaseRoot = "file:///" + tmpdir + "/hbase";
    conf.set("hbase.rootdir", hbaseRoot);

    conf.set("hbase.zookeeper.property.clientPort",
      Integer.toString(zooKeeperPort));
    Configuration hbaseConf = HBaseConfiguration.create(conf);
    hbaseConf.setInt("hbase.master.port", findFreePort());
    hbaseConf.setInt("hbase.master.info.port", -1);
    hbaseConf.setInt("hbase.regionserver.port", findFreePort());
    hbaseConf.setInt("hbase.regionserver.info.port", -1);
    // Fix needed due to dependency for hbase-mapreduce module
    System.setProperty("org.apache.hadoop.hbase.shaded.io.netty.packagePrefix",
        "org.apache.hadoop.hbase.shaded.");
    hbaseCluster = new MiniHBaseCluster(hbaseConf, NUM_REGIONSERVERS);
    conf.set("hbase.master", hbaseCluster.getMaster().getServerName().getHostAndPort());
    hbaseConn = ConnectionFactory.createConnection(hbaseConf);

    // opening the META table ensures that cluster is running
    Table meta = null;
    try {
      meta = hbaseConn.getTable(TableName.META_TABLE_NAME);
    } finally {
      if (meta != null) meta.close();
    }
    createHBaseTable();
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

  private static int findFreePort() throws IOException {
    ServerSocket server = new ServerSocket(0);
    int port = server.getLocalPort();
    server.close();
    return port;
  }

  public void tearDown() throws Exception {
    if (hbaseConn != null) {
      hbaseConn.close();
      hbaseConn = null;
    }
    if (hbaseCluster != null) {
      hbaseCluster.shutdown();
      hbaseCluster = null;
    }
  }
}
