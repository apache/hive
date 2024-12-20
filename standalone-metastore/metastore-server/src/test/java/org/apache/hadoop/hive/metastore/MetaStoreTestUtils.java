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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.ZooKeeperHiveHelper;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.events.EventCleanerTask;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;

public class MetaStoreTestUtils {
  private static Map<Integer, Thread> map = new HashMap<>();

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
   * @param withHouseKeepingThreads Start the housekeeping background threads in the metastore
   * @param waitForHouseKeepers Wait until the background threads are scheduled
   * @throws Exception Timeout after 60 seconds
   */
  public static void startMetaStore(final int port, final HadoopThriftAuthBridge bridge, Configuration conf,
      boolean withHouseKeepingThreads, boolean waitForHouseKeepers) throws Exception {
    if (conf == null) {
      conf = MetastoreConf.newMetastoreConf();
    }
    final Configuration finalConf = conf;
    final AtomicBoolean startedBackgroundThreads =
        (withHouseKeepingThreads && waitForHouseKeepers) ? new AtomicBoolean() : null;
    Thread thread = new Thread(() -> {
      try {
        HiveMetaStore.startMetaStore(port, bridge, finalConf, withHouseKeepingThreads, startedBackgroundThreads);
      } catch (Throwable e) {
        LOG.error("Metastore Thrift Server threw an exception...", e);
      }
    }, "MetaStoreThread-" + port);
    thread.setDaemon(true);
    thread.start();
    map.put(port,thread);
    String msHost = MetastoreConf.getVar(conf, ConfVars.THRIFT_BIND_HOST);
    MetaStoreTestUtils.loopUntilHMSReady(msHost, port, startedBackgroundThreads);
    String serviceDiscMode = MetastoreConf.getVar(conf, ConfVars.THRIFT_SERVICE_DISCOVERY_MODE);
    if (serviceDiscMode != null && serviceDiscMode.equalsIgnoreCase("zookeeper")) {
      MetaStoreTestUtils.loopUntilZKReady(conf, msHost, port);
    }
  }

  @SuppressWarnings("deprecation")
  public static void close(final int port){
    Thread thread = map.get(port);
    if(thread != null){
      thread.stop();
    }
  }

  public static int startMetaStoreWithRetry(final HadoopThriftAuthBridge bridge) throws Exception {
    return MetaStoreTestUtils.startMetaStoreWithRetry(bridge, MetastoreConf.newMetastoreConf());
  }

  public static int startMetaStoreWithRetry(Configuration conf) throws Exception {
    return MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
  }

  public static int startMetaStoreWithRetry(Configuration conf, boolean keepWarehousePath) throws Exception {
    return MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf, false, keepWarehousePath, false, false);
  }

  public static int startMetaStoreWithRetry(Configuration conf, boolean keepJdbcUri,
                                            boolean keepWarehousePath)
      throws Exception {
    return MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf,
        keepJdbcUri, keepWarehousePath, false, false);
  }

  public static int startMetaStoreWithRetry() throws Exception {
    return MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(),
        MetastoreConf.newMetastoreConf());
  }

  public static int startMetaStoreWithRetry(HadoopThriftAuthBridge bridge,
                                            Configuration conf) throws Exception {
    return MetaStoreTestUtils.startMetaStoreWithRetry(bridge, conf, false, false, false, false);
  }

  public static int startMetaStoreWithRetry(HadoopThriftAuthBridge bridge,
                                            Configuration conf, boolean withHouseKeepingThreads)
          throws Exception {
    return MetaStoreTestUtils.startMetaStoreWithRetry(bridge, conf, false, false, withHouseKeepingThreads, false);
  }

  public static int startMetaStoreWithRetry(HadoopThriftAuthBridge bridge, Configuration conf, boolean keepJdbcUri,
                                            boolean keepWarehousePath, boolean withHouseKeepingThreads,
                                            boolean waitForHouseKeepers) throws Exception {
    return startMetaStoreWithRetry(bridge, conf, keepJdbcUri, keepWarehousePath, withHouseKeepingThreads,
            waitForHouseKeepers, true);
  }

  /**
   * Starts a MetaStore instance with the given configuration and given bridge.
   * Tries to find a free port, and use it. If failed tries another port so the tests will not
   * fail if run parallel. Also adds the port to the warehouse dir, so the multiple MetaStore
   * instances will use different warehouse directories.
   * @param bridge The Thrift bridge to uses
   * @param conf The configuration to use
   * @param keepJdbcUri If set to true, then the JDBC url is not changed
   * @param keepWarehousePath If set to true, then the Warehouse directory is not changed
   * @param withHouseKeepingThreads Start the housekeeping background threads in the metastore
   * @param waitForHouseKeepers Wait until the background threads are scheduled
   * @return The port on which the MetaStore finally started
   * @throws Exception Timeout after 60 seconds
   */
  public static int startMetaStoreWithRetry(HadoopThriftAuthBridge bridge, Configuration conf, boolean keepJdbcUri,
      boolean keepWarehousePath, boolean withHouseKeepingThreads, boolean waitForHouseKeepers,
                                            boolean createTransactionalTables) throws Exception {
    Exception metaStoreException = null;
    String warehouseDir = MetastoreConf.getVar(conf, ConfVars.WAREHOUSE);

    for (int tryCount = 0; tryCount < MetaStoreTestUtils.RETRY_COUNT; tryCount++) {
      try {
        int metaStorePort = findFreePort();
        if (!keepWarehousePath) {
          // Setting metastore instance specific warehouse directory, postfixing with port
          Path postfixedWarehouseDir = new Path(warehouseDir, String.valueOf(metaStorePort));
          MetastoreConf.setVar(conf, ConfVars.WAREHOUSE, postfixedWarehouseDir.toString());
          warehouseDir = postfixedWarehouseDir.toString();
        }

        String jdbcUrl = MetastoreConf.getVar(conf, ConfVars.CONNECT_URL_KEY);
        if (!keepJdbcUri) {
          // Setting metastore instance specific jdbc url postfixed with port
          jdbcUrl = "jdbc:derby:memory:" + TMP_DIR + File.separator
              + MetaStoreServerUtils.JUNIT_DATABASE_PREFIX + "_" + metaStorePort + ";create=true";
          MetastoreConf.setVar(conf, ConfVars.CONNECT_URL_KEY, jdbcUrl);
        }

        // Setting metastore instance specific metastore uri, if dynamic metastore service
        // discovery is not enabled.
        if (MetastoreConf.getVar(conf, ConfVars.THRIFT_SERVICE_DISCOVERY_MODE).trim().isEmpty()) {
          MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "thrift://localhost:" + metaStorePort);
        }

        if (createTransactionalTables) {
          // Some tests may have dummy txn manager. There is no harm in creating the transactional tables but
          // we should not change the test config in case they purposefully do not use transactions
          Configuration txnInitConf = new Configuration(conf);
          TestTxnDbUtil.setConfValues(txnInitConf);
          TestTxnDbUtil.prepDb(txnInitConf);
        }

        MetaStoreTestUtils.startMetaStore(metaStorePort, bridge, conf, withHouseKeepingThreads, waitForHouseKeepers);

        // Creating warehouse dir, if not exists
        Warehouse wh = new Warehouse(conf);
        if (!wh.isDir(wh.getWhRoot())) {
          FileSystem fs = wh.getWhRoot().getFileSystem(conf);
          fs.mkdirs(wh.getWhRoot());
          fs.setPermission(wh.getWhRoot(),
              new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL));
          LOG.info("MetaStore warehouse root dir ({}) is created", warehouseDir);
        }

        LOG.info("MetaStore Thrift Server started on port: {} with warehouse dir: {} with " +
            "jdbcUrl: {}", metaStorePort, warehouseDir, jdbcUrl);

        return metaStorePort;
      } catch (ConnectException ce) {
        metaStoreException = ce;
      }
    }
    throw metaStoreException;
  }

  /**
   * A simple connect test to make sure that the metastore is up
   * @throws Exception Timeout after 60 seconds
   */
  private static void loopUntilHMSReady(String msHost, int port, AtomicBoolean houseKeepingStarted) throws Exception {
    int retries = 0;
    Exception exc = null;
    while (retries++ < 60) {
      try {
        Socket socket = new Socket();
        SocketAddress sockAddr;
        if (msHost == null) {
          sockAddr = new InetSocketAddress(port);
        } else {
          sockAddr = new InetSocketAddress(msHost, port);
        }
        socket.connect(sockAddr, 5000);
        socket.close();
        if (houseKeepingStarted == null || houseKeepingStarted.get()) {
          return;
        } else {
          LOG.info("HMS started, waiting for housekeeper threads to start.");
        }
      } catch (Exception e) {
        LOG.info("Waiting the HMS to start.");
        exc = e;
      }
      Thread.sleep(1000);
    }
    if (exc == null) {
      exc = new IllegalStateException("HMS started, but housekeeping threads were not started until 60 sec.");
    }
    // something is preventing metastore from starting
    // print the stack from all threads for debugging purposes
    LOG.error("Unable to connect to metastore server: " + exc.getMessage());
    LOG.info("Printing all thread stack traces for debugging before throwing exception.");
    LOG.info(MetaStoreTestUtils.getAllThreadStacksAsString());
    throw exc;
  }

  /**
   * A simple connect test to make sure that the metastore URI is available in the ZooKeeper
   * @throws Exception Timeout after 60 seconds
   */
  private static void loopUntilZKReady(Configuration conf, String msHost, int port)
          throws Exception {
    ZooKeeperHiveHelper zkHelper = MetastoreConf.getZKConfig(conf);
    String uri;
    if (msHost != null && !msHost.trim().isEmpty()) {
      uri = msHost;
    } else {
      uri = InetAddress.getLocalHost().getHostName();
    }
    uri = uri + ":" + port;
    int retries = 0;
    while (true) {
      try {
        List<String> serverUris = zkHelper.getServerUris();
        // URI of the metastore server should be same as expected.
        if (!serverUris.equals(Collections.singletonList(uri))) {
            throw new Exception("Expected metastore URI " + uri + " but got " + serverUris);
        }
        return;
      } catch (Exception e) {
        if (retries++ > 60) { //give up
          // Metastore URI is not visible from the ZooKeeper yet.
          LOG.error("Unable to get metastore URI from the ZooKeeper: " + e.getMessage());
          throw e;
        }
        Thread.sleep(1000);
      }
    }
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
   * @return The allocated port
   * @throws IOException -
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
    try (ServerSocket socket1 = new ServerSocket(0); ServerSocket socket2 = new ServerSocket(0)) {
      if (socket1.getLocalPort() != portToExclude) {
        return socket1.getLocalPort();
      }
      // If we're here, then socket1.getLocalPort was the port to exclude
      // Since both sockets were open together at a point in time, we're
      // guaranteed that socket2.getLocalPort() is not the same.
      return socket2.getLocalPort();
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

  /**
   * This method can run an assertion wrapped in to a runnable, and keep retrying it for a certain amount of time.
   * It can be useful when the assertion doesn't necessarily pass immediately, and it would be hard
   * to mock into production code in order to wait for some conditions properly. Instead, it just
   * waits, but not like a constant time before a single attempt (which is easy, but errorprone).
   * @param assertionContext
   * @param runnable
   * @param msBetweenAssertionAttempts
   * @param msOverallTimeout
   * @throws Exception
   */
  public static void waitForAssertion(String assertionContext, Runnable assertionRunnable,
      int msBetweenAssertionAttempts, int msOverallTimeout) throws Exception {
    if (msOverallTimeout <= 0) {
      msOverallTimeout = Integer.MAX_VALUE;
    }
    long start = System.currentTimeMillis();
    LOG.info("Waiting for assertion: " + assertionContext);
    while (true) {
      try {
        assertionRunnable.run();
        LOG.info("waitForAssertion passed in {} ms", System.currentTimeMillis() - start);
        return;
      } catch (AssertionError e) {
        LOG.info("AssertionError: " + e.getMessage());
        long elapsedMs = System.currentTimeMillis() - start;
        if (elapsedMs > msOverallTimeout) {
          LOG.info("waitForAssertion failed in {} ms", elapsedMs);
          String message = e.getMessage();
          throw new AssertionError(message + " (waitForAssertion timeout: " + elapsedMs + "ms)", e);
        }
        Thread.sleep(msBetweenAssertionAttempts);
      }
    }
  }
}
