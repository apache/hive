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
package org.apache.hadoop.hive.metastore;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.events.EventCleanerTask;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;

public class MetaStoreTestUtils {
  private static final Logger LOG = LoggerFactory.getLogger(MetaStoreTestUtils.class);
  private static final String TMP_DIR = System.getProperty("test.tmp.dir");
  public static final int RETRY_COUNT = 10;

  /**
   * Starts a MetaStore instance on get given port with the given configuration and Thrift bridge.
   * Use it only it the port is definitely free. For tests use startMetaStoreWithRetry instead so
   * the MetaStore will find an emtpy port eventually, so the different tests can be run on the
   * same machine.
   * @param port The port to start on
   * @param bridge The bridge to use
   * @param conf The configuration to use
   * @throws Exception
   */
  public static void startMetaStore(final int port,
      final HadoopThriftAuthBridge bridge, Configuration conf)
      throws Exception{
    if (conf == null) {
      conf = MetastoreConf.newMetastoreConf();
    }
    final Configuration finalConf = conf;
    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          HiveMetaStore.startMetaStore(port, bridge, finalConf);
        } catch (Throwable e) {
          LOG.error("Metastore Thrift Server threw an exception...", e);
        }
      }
    }, "MetaStoreThread-" + port);
    thread.setDaemon(true);
    thread.start();
    MetaStoreTestUtils.loopUntilHMSReady(port);
  }

  public static int startMetaStoreWithRetry(final HadoopThriftAuthBridge bridge) throws Exception {
    return MetaStoreTestUtils.startMetaStoreWithRetry(bridge, MetastoreConf.newMetastoreConf());
  }

  public static int startMetaStoreWithRetry(Configuration conf) throws Exception {
    return MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
  }

  public static int startMetaStoreWithRetry(Configuration conf, boolean keepJdbcUri)
      throws Exception {
    return MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf,
        keepJdbcUri);
  }

  public static int startMetaStoreWithRetry() throws Exception {
    return MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(),
        MetastoreConf.newMetastoreConf());
  }

  public static int startMetaStoreWithRetry(HadoopThriftAuthBridge bridge,
                                            Configuration conf) throws Exception {
    return MetaStoreTestUtils.startMetaStoreWithRetry(bridge, conf, false);
  }

  /**
   * Starts a MetaStore instance with the given configuration and given bridge.
   * Tries to find a free port, and use it. If failed tries another port so the tests will not
   * fail if run parallel. Also adds the port to the warehouse dir, so the multiple MetaStore
   * instances will use different warehouse directories.
   * @param bridge The Thrift bridge to uses
   * @param conf The configuration to use
   * @param keepJdbcUri If set to true, then the JDBC url is not changed
   * @return The port on which the MetaStore finally started
   * @throws Exception
   */
  public static int startMetaStoreWithRetry(HadoopThriftAuthBridge bridge,
      Configuration conf, boolean keepJdbcUri) throws Exception {
    Exception metaStoreException = null;
    String warehouseDir = MetastoreConf.getVar(conf, ConfVars.WAREHOUSE);

    for (int tryCount = 0; tryCount < MetaStoreTestUtils.RETRY_COUNT; tryCount++) {
      try {
        int metaStorePort = findFreePort();
        // Setting metastore instance specific warehouse directory, postfixing with port
        Path postfixedWarehouseDir = new Path(warehouseDir, String.valueOf(metaStorePort));
        MetastoreConf.setVar(conf, ConfVars.WAREHOUSE, postfixedWarehouseDir.toString());

        String jdbcUrl = MetastoreConf.getVar(conf, ConfVars.CONNECT_URL_KEY);
        if (!keepJdbcUri) {
          // Setting metastore instance specific jdbc url postfixed with port
          jdbcUrl = "jdbc:derby:;databaseName=" + TMP_DIR + File.separator
              + "junit_metastore_db_" + metaStorePort + ";create=true";
          MetastoreConf.setVar(conf, ConfVars.CONNECT_URL_KEY, jdbcUrl);
        }

        // Setting metastore instance specific metastore uri
        MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "thrift://localhost:" + metaStorePort);
        MetaStoreTestUtils.startMetaStore(metaStorePort, bridge, conf);

        // Creating warehouse dir, if not exists
        Warehouse wh = new Warehouse(conf);
        if (!wh.isDir(wh.getWhRoot())) {
          FileSystem fs = wh.getWhRoot().getFileSystem(conf);
          fs.mkdirs(wh.getWhRoot());
          fs.setPermission(wh.getWhRoot(),
              new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
          LOG.info("MetaStore warehouse root dir ({}) is created", postfixedWarehouseDir);
        }

        LOG.info("MetaStore Thrift Server started on port: {} with warehouse dir: {} with " +
            "jdbcUrl: {}", metaStorePort, postfixedWarehouseDir, jdbcUrl);
        return metaStorePort;
      } catch (ConnectException ce) {
        metaStoreException = ce;
      }
    }
    throw metaStoreException;
  }

  /**
   * A simple connect test to make sure that the metastore is up
   * @throws Exception
   */
  private static void loopUntilHMSReady(int port) throws Exception {
    int retries = 0;
    Exception exc = null;
    while (true) {
      try {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(port), 5000);
        socket.close();
        return;
      } catch (Exception e) {
        if (retries++ > 60) { //give up
          exc = e;
          break;
        }
        Thread.sleep(1000);
      }
    }
    // something is preventing metastore from starting
    // print the stack from all threads for debugging purposes
    LOG.error("Unable to connect to metastore server: " + exc.getMessage());
    LOG.info("Printing all thread stack traces for debugging before throwing exception.");
    LOG.info(MetaStoreTestUtils.getAllThreadStacksAsString());
    throw exc;
  }

  private static String getAllThreadStacksAsString() {
    Map<Thread, StackTraceElement[]> threadStacks = Thread.getAllStackTraces();
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<Thread, StackTraceElement[]> entry : threadStacks.entrySet()) {
      Thread t = entry.getKey();
      sb.append(System.lineSeparator());
      sb.append("Name: ").append(t.getName()).append(" State: ").append(t.getState());
      MetaStoreTestUtils.addStackString(entry.getValue(), sb);
    }
    return sb.toString();
  }

  private static void addStackString(StackTraceElement[] stackElems, StringBuilder sb) {
    sb.append(System.lineSeparator());
    for (StackTraceElement stackElem : stackElems) {
      sb.append(stackElem).append(System.lineSeparator());
    }
  }

  /**
   * Finds a free port on the machine.
   *
   * @return
   * @throws IOException
   */
  public static int findFreePort() throws IOException {
    ServerSocket socket= new ServerSocket(0);
    int port = socket.getLocalPort();
    socket.close();
    return port;
  }

  /**
   * Finds a free port on the machine, but allow the
   * ability to specify a port number to not use, no matter what.
   */
  public static int findFreePortExcepting(int portToExclude) throws IOException {
    ServerSocket socket1 = null;
    ServerSocket socket2 = null;
    try {
      socket1 = new ServerSocket(0);
      socket2 = new ServerSocket(0);
      if (socket1.getLocalPort() != portToExclude) {
        return socket1.getLocalPort();
      }
      // If we're here, then socket1.getLocalPort was the port to exclude
      // Since both sockets were open together at a point in time, we're
      // guaranteed that socket2.getLocalPort() is not the same.
      return socket2.getLocalPort();
    } finally {
      if (socket1 != null){
        socket1.close();
      }
      if (socket2 != null){
        socket2.close();
      }
    }
  }

  /**
   * Setup a configuration file for standalone mode.  There are a few config variables that have
   * defaults that require parts of Hive that aren't present in standalone mode.  This method
   * sets them to something that will work without the rest of Hive.  It only changes them if
   * they have not already been set, to avoid clobbering intentional changes.
   * @param conf Configuration object
   */
  public static void setConfForStandloneMode(Configuration conf) {
    if (MetastoreConf.getVar(conf, ConfVars.TASK_THREADS_ALWAYS).equals(
        ConfVars.TASK_THREADS_ALWAYS.getDefaultVal())) {
      MetastoreConf.setVar(conf, ConfVars.TASK_THREADS_ALWAYS,
          EventCleanerTask.class.getName());
    }
    if (MetastoreConf.getVar(conf, ConfVars.EXPRESSION_PROXY_CLASS).equals(
        ConfVars.EXPRESSION_PROXY_CLASS.getDefaultVal())) {
      MetastoreConf.setClass(conf, ConfVars.EXPRESSION_PROXY_CLASS,
          DefaultPartitionExpressionProxy.class, PartitionExpressionProxy.class);
    }
  }


  public static String getTestWarehouseDir(String name) {
    File dir = new File(System.getProperty("java.io.tmpdir"), name);
    dir.deleteOnExit();
    return dir.getAbsolutePath();
  }

  /**
   * There is no cascade option for dropping a catalog for security reasons.  But this in
   * inconvenient in tests, so this method does it.
   * @param client metastore client
   * @param catName catalog to drop, cannot be the default catalog
   * @throws TException from underlying client calls
   */
  public static void dropCatalogCascade(IMetaStoreClient client, String catName) throws TException {
    if (catName != null && !catName.equals(DEFAULT_CATALOG_NAME)) {
      List<String> databases = client.getAllDatabases(catName);
      for (String db : databases) {
        client.dropDatabase(catName, db, true, false, true);
      }
      client.dropCatalog(catName);
    }
  }
}
