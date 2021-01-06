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

package org.apache.hive.service.testutil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.RetryingHMSHandler;
import org.apache.hadoop.hive.metastore.TSetIpAddressProcessor;
import org.apache.hadoop.hive.metastore.tools.schematool.MetastoreSchemaTool;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.file.Files.createTempDirectory;
import static java.nio.file.attribute.PosixFilePermissions.asFileAttribute;
import static java.nio.file.attribute.PosixFilePermissions.fromString;

public class TestHiveMetastore {

  private static final String DEFAULT_DATABASE_NAME = "default";
  private static final int DEFAULT_POOL_SIZE = 5;

  private File hiveLocalDir;
  private HiveConf hiveConf;
  private ExecutorService executorService;
  private TServer server;
  private HiveMetaStore.HMSHandler baseHandler;
  private HiveMetaStoreClient client;

  /**
   * Starts a TestHiveMetastore with the default connection pool size (5).
   */
  public void start() {
    start(DEFAULT_POOL_SIZE);
  }

  /**
   * Starts a TestHiveMetastore with a provided connection pool size.
   * @param poolSize The number of threads in the executor pool
   */
  public void start(int poolSize) {
    try {
      this.hiveLocalDir = createTempDirectory("hive", asFileAttribute(fromString("rwxrwxrwx"))).toFile();
      File derbyLogFile = new File(hiveLocalDir, "derby.log");
      System.setProperty("derby.stream.error.file", derbyLogFile.getAbsolutePath());

      TServerSocket socket = new TServerSocket(0);
      int port = socket.getServerSocket().getLocalPort();
      this.hiveConf = newHiveConf(port);

      setupMetastoreDB("jdbc:derby:" + getDerbyPath() + ";create=true");
      this.server = newThriftServer(socket, poolSize, hiveConf);
      this.executorService = Executors.newSingleThreadExecutor();
      this.executorService.submit(() -> server.serve());

      // setting this as a system prop ensures that it will be picked up whenever a new HiveConf is created
      System.setProperty(HiveConf.ConfVars.METASTOREURIS.varname, hiveConf.getVar(HiveConf.ConfVars.METASTOREURIS));

      this.client = new HiveMetaStoreClient(hiveConf);
    } catch (Exception e) {
      throw new RuntimeException("Cannot start TestHiveMetastore", e);
    }
  }

  public void stop() {
    if (client != null) {
      client.close();
    }
    if (server != null) {
      server.stop();
    }
    if (executorService != null) {
      executorService.shutdown();
    }
    if (hiveLocalDir != null) {
      hiveLocalDir.delete();
    }
    if (baseHandler != null) {
      baseHandler.shutdown();
    }
  }

  public HiveConf hiveConf() {
    return hiveConf;
  }

  public void reset() throws Exception {
    for (String dbName : client.getAllDatabases()) {
      for (String tblName : client.getAllTables(dbName)) {
        client.dropTable(dbName, tblName, true, true, true);
      }
      if (!DEFAULT_DATABASE_NAME.equals(dbName)) {
        // Drop cascade, functions dropped by cascade
        client.dropDatabase(dbName, true, true, true);
      }
    }

    Path warehouseRoot = new Path(hiveLocalDir.getAbsolutePath());
    FileSystem fs = warehouseRoot.getFileSystem(hiveConf);
    for (FileStatus fileStatus : fs.listStatus(warehouseRoot)) {
      if (!fileStatus.getPath().getName().equals("derby.log") &&
          !fileStatus.getPath().getName().equals("metastore_db")) {
        fs.delete(fileStatus.getPath(), true);
      }
    }
  }

  private TServer newThriftServer(TServerSocket socket, int poolSize, HiveConf conf) throws Exception {
    HiveConf serverConf = new HiveConf(conf);
    serverConf.set(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname, "jdbc:derby:" + getDerbyPath() + ";create=true");
    baseHandler = new HiveMetaStore.HMSHandler("new db based metaserver", serverConf);
    IHMSHandler handler = RetryingHMSHandler.getProxy(serverConf, baseHandler, false);

    TThreadPoolServer.Args args = new TThreadPoolServer.Args(socket)
        .processor(new TSetIpAddressProcessor<IHMSHandler>(handler))
        .transportFactory(new TTransportFactory())
        .protocolFactory(new TBinaryProtocol.Factory())
        .minWorkerThreads(poolSize)
        .maxWorkerThreads(poolSize);

    return new TThreadPoolServer(args);
  }

  private HiveConf newHiveConf(int port) {
    HiveConf newHiveConf = new HiveConf(new Configuration(), TestHiveMetastore.class);
    newHiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, "thrift://localhost:" + port);
    newHiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, "file:" + hiveLocalDir.getAbsolutePath());
    newHiveConf.set(HiveConf.ConfVars.METASTORE_TRY_DIRECT_SQL.varname, "false");
    newHiveConf.set(HiveConf.ConfVars.METASTORE_DISALLOW_INCOMPATIBLE_COL_TYPE_CHANGES.varname, "false");
    return newHiveConf;
  }

  private void setupMetastoreDB(String dbURL) throws Exception {
    MetastoreSchemaTool schemaTool = new MetastoreSchemaTool();
    schemaTool.setUserName("hive");
    schemaTool.setPassWord("hive");

    // below usage of SchemaTool is a bit hacky, but it works for our purposes to apply the metastore DB script
    hiveConf.set("javax.jdo.option.ConnectionURL", dbURL);
    Field conf = schemaTool.getClass().getDeclaredField("conf");
    conf.setAccessible(true);
    conf.set(schemaTool, hiveConf);

    Method execSql = schemaTool.getClass().getDeclaredMethod("execSql", String.class);
    execSql.setAccessible(true);
    execSql.invoke(schemaTool, "../standalone-metastore/metastore-server/src/main/sql/derby/hive-schema-4.0.0.derby.sql");
  }

  private String getDerbyPath() {
    File metastoreDB = new File(hiveLocalDir, "metastore_db");
    return metastoreDB.getPath();
  }
}
