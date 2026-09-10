/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.search.testutil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.messaging.json.JSONMessageEncoder;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hive.hcatalog.listener.DbNotificationListener;

/** Starts an in-process HMS backed by Derby for integration tests. */
public final class RealMetastoreServer implements AutoCloseable {
  private static RealMetastoreServer instance;

  private final Configuration conf;
  private final int port;
  private final HiveMetaStoreClient client;

  private RealMetastoreServer(Configuration conf, int port, HiveMetaStoreClient client) {
    this.conf = conf;
    this.port = port;
    this.client = client;
  }

  public static synchronized RealMetastoreServer start() throws Exception {
    if (instance != null) {
      return instance;
    }
    Configuration conf = MetastoreConf.newMetastoreConf();
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_IN_TEST, true);
    MetastoreConf.setBoolVar(conf, ConfVars.CAPABILITY_CHECK, false);
    MetastoreConf.setVar(conf, ConfVars.TRANSACTIONAL_EVENT_LISTENERS,
        DbNotificationListener.class.getName());
    MetastoreConf.setVar(conf, ConfVars.EVENT_MESSAGE_FACTORY, JSONMessageEncoder.class.getName());
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    int port = MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
    HiveMetaStoreClient client = new HiveMetaStoreClient(conf);
    instance = new RealMetastoreServer(conf, port, client);
    return instance;
  }

  public static synchronized RealMetastoreServer get() {
    if (instance == null) {
      throw new IllegalStateException("Call RealMetastoreServer.start() first");
    }
    return instance;
  }

  public Configuration conf() {
    return new Configuration(conf);
  }

  public IMetaStoreClient client() {
    return client;
  }

  public void createDatabase(String dbName) throws Exception {
    new DatabaseBuilder().setName(dbName).create(client, conf);
  }

  public void createTable(String dbName, String tableName, String comment) throws Exception {
    new TableBuilder()
        .setDbName(dbName)
        .setTableName(tableName)
        .addCol("id", "int")
        .addTableParam("comment", comment)
        .create(client, conf);
  }

  public void dropTable(String dbName, String tableName) throws Exception {
    client.dropTable(dbName, tableName);
  }

  @Override
  public synchronized void close() {
    if (instance == null) {
      return;
    }
    client.close();
    MetaStoreTestUtils.close(port);
    instance = null;
  }
}
