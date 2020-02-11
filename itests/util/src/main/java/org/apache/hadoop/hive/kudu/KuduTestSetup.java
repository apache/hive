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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.QTestMiniClusters;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.test.cluster.MiniKuduCluster;

import com.google.common.collect.ImmutableList;

import java.io.File;
import java.util.Arrays;

/**
 * Start and stop a Kudu MiniCluster for testing purposes.
 */
public class KuduTestSetup extends QTestMiniClusters.QTestSetup {

  public static final String KV_TABLE_NAME = "default.kudu_kv";
  public static final String ALL_TYPES_TABLE_NAME = "default.kudu_all_types";

  public static final Schema ALL_TYPES_SCHEMA = KuduTestUtils.getAllTypesSchema();
  public static final Schema KV_SCHEMA = new Schema(Arrays.asList(
      new ColumnSchema.ColumnSchemaBuilder("key", Type.INT32).key(true).build(),
        new ColumnSchema.ColumnSchemaBuilder("value", Type.STRING).build())
  );

  private MiniKuduCluster miniCluster;

  public KuduTestSetup() {
  }

  @Override
  public void preTest(HiveConf conf) throws Exception {
    super.preTest(conf);
    setupWithHiveConf(conf);
    createKuduTables();
  }

  @Override
  public void postTest(HiveConf conf) throws Exception {
    dropKuduTables();
    super.postTest(conf);
  }

  @Override
  public void tearDown() throws Exception {
    if (null != miniCluster) {
      miniCluster.shutdown();
      miniCluster = null;
    }
    super.tearDown();
  }

  private void setupWithHiveConf(HiveConf conf) throws Exception {
    if (null == miniCluster) {
      String testTmpDir = System.getProperty("test.tmp.dir");
      File tmpDir = new File(testTmpDir, "kudu");

      if (tmpDir.exists()) {
        FileUtils.deleteDirectory(tmpDir);
      }

      miniCluster = new MiniKuduCluster.MiniKuduClusterBuilder()
          .numMasterServers(3)
          .numTabletServers(3)
          .build();
    }

    updateConf(conf);
  }

  /**
   * Update hiveConf with the Kudu specific parameters.
   * @param conf The hiveconf to update
   */
  private void updateConf(HiveConf conf) {
    if (miniCluster != null) {
      HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_KUDU_MASTER_ADDRESSES_DEFAULT,
          miniCluster.getMasterAddressesAsString());
    }
  }

  private void createKuduTables() throws KuduException {
    if (null != miniCluster) {
      String masterAddresses = miniCluster.getMasterAddressesAsString();
      try (KuduClient client = new KuduClient.KuduClientBuilder(masterAddresses).build()) {
        createKVTable(client);
        createAllTypesTable(client);
      }
    }
  }

  private void dropKuduTables() throws KuduException  {
    if (null != miniCluster) {
      String masterAddresses = miniCluster.getMasterAddressesAsString();
      try (KuduClient client = new KuduClient.KuduClientBuilder(masterAddresses).build()) {
        dropKVTable(client);
        dropAllTypesTable(client);
      }
    }
  }

  public void createKVTable(KuduClient client) throws KuduException {
    dropKVTable(client);
    CreateTableOptions options = new CreateTableOptions()
        .setRangePartitionColumns(ImmutableList.of("key"));
    client.createTable(KV_TABLE_NAME, KV_SCHEMA, options);
  }

  public void dropKVTable(KuduClient client ) throws KuduException {
    if (client.tableExists(KV_TABLE_NAME)) {
      client.deleteTable(KV_TABLE_NAME);
    }
  }

  public void createAllTypesTable(KuduClient client) throws KuduException {
    dropAllTypesTable(client);
    CreateTableOptions options = new CreateTableOptions()
        .addHashPartitions(Arrays.asList("key"), 4);
    client.createTable(ALL_TYPES_TABLE_NAME, ALL_TYPES_SCHEMA, options);
  }

  public void dropAllTypesTable(KuduClient client ) throws KuduException {
    if (client.tableExists(ALL_TYPES_TABLE_NAME)) {
      client.deleteTable(ALL_TYPES_TABLE_NAME);
    }
  }
}
