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

package org.apache.iceberg.mr.hive;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hive.service.cli.CLIService;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.HiveSession;
import org.apache.hive.service.server.HiveServer2;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.apache.iceberg.relocated.com.google.common.base.Joiner;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;

/**
 * Test class for running HiveQL queries, essentially acting like a Beeline shell in tests.
 *
 * It spins up both an HS2 and a Metastore instance to work with. The shell will only accept
 * queries if it has been previously initialized via {@link #start()}, and a session has been opened via
 * {@link #openSession()}. Prior to calling {@link #start()}, the shell should first be configured with props that apply
 * across all test cases by calling {@link #setHiveConfValue(String, String)} ()}. On the other hand, session-level conf
 * can be applied anytime via {@link #setHiveSessionValue(String, String)} ()}, once we've opened an active session.
 */
public class TestHiveShell {

  private final TestHiveMetastore metastore;
  private final HiveServer2 hs2;
  private final HiveConf hs2Conf;
  private CLIService client;
  private boolean started;

  private ThreadLocal<HiveSession> session = ThreadLocal.withInitial(this::openSession);

  public TestHiveShell() {
    metastore = new TestHiveMetastore();
    hs2Conf = initializeConf();
    hs2 = new HiveServer2();
  }

  public void setHiveConfValue(String key, String value) {
    Preconditions.checkState(!started, "TestHiveShell has already been started. Cannot set Hive conf anymore.");
    hs2Conf.verifyAndSet(key, value);
  }

  public void setHiveSessionValue(String key, String value) {
    Preconditions.checkState(session.get() != null, "There is no open session for setting variables.");
    try {
      session.get().getSessionConf().set(key, value);
    } catch (Exception e) {
      throw new RuntimeException("Unable to set Hive session variable: ", e);
    }
  }

  public void setHiveSessionValue(String key, boolean value) {
    setHiveSessionValue(key, Boolean.toString(value));
  }

  public void start() {
    // Create a copy of the HiveConf for the metastore
    metastore.start(new HiveConf(hs2Conf), 20);
    hs2Conf.setVar(HiveConf.ConfVars.METASTORE_URIS, metastore.hiveConf().getVar(HiveConf.ConfVars.METASTORE_URIS));
    hs2Conf.setVar(HiveConf.ConfVars.METASTORE_WAREHOUSE,
        metastore.hiveConf().getVar(HiveConf.ConfVars.METASTORE_WAREHOUSE));
    hs2Conf.setVar(HiveConf.ConfVars.HIVE_METASTORE_WAREHOUSE_EXTERNAL,
        metastore.hiveConf().getVar(HiveConf.ConfVars.HIVE_METASTORE_WAREHOUSE_EXTERNAL));

    // Initializing RpcMetrics in a single JVM multiple times can cause issues
    DefaultMetricsSystem.setMiniClusterMode(true);

    hs2.init(hs2Conf);
    hs2.start();
    client = hs2.getServices().stream()
            .filter(CLIService.class::isInstance)
            .findFirst()
            .map(CLIService.class::cast)
            .get();
    started = true;
  }

  public void stop() throws Exception {
    if (client != null) {
      client.stop();
    }
    hs2.stop();
    metastore.stop();
    started = false;
  }

  public TestHiveMetastore metastore() {
    return metastore;
  }

  private HiveSession openSession() {
    Preconditions.checkState(started, "You have to start TestHiveShell first, before opening a session.");
    try {
      SessionHandle sessionHandle = client.getSessionManager().openSession(
          CLIService.SERVER_VERSION, "", "", "127.0.0.1", Collections.emptyMap());
      return client.getSessionManager().getSession(sessionHandle);
    } catch (Exception e) {
      throw new RuntimeException("Unable to open new Hive session: ", e);
    }
  }

  public void closeSession() {
    Preconditions.checkState(session.get() != null, "There is no open session to be closed.");
    try {
      session.get().close();
      session.remove();
    } catch (Exception e) {
      throw new RuntimeException("Unable to close Hive session: ", e);
    }
  }

  public List<Object[]> executeStatement(String statement) {
    Preconditions.checkState(session.get() != null,
            "You have to start TestHiveShell and open a session first, before running a query.");
    try {
      OperationHandle handle = client.executeStatement(session.get().getSessionHandle(), statement,
          Collections.emptyMap());
      List<Object[]> resultSet = Lists.newArrayList();
      if (handle.hasResultSet()) {
        RowSet rowSet;
        // keep fetching results until we can
        while ((rowSet = client.fetchResults(handle)) != null && rowSet.numRows() > 0) {
          for (Object[] row : rowSet) {
            resultSet.add(row.clone());
          }
        }
      }
      return resultSet;
    } catch (HiveSQLException e) {
      throw new IllegalArgumentException("Failed to execute Hive query '" + statement + "': " + e.getMessage(), e);
    }
  }

  /**
   * Used for debugging. Please do not remove even if unused in the codebase.
   * @param statement The statement to execute
   * @return The formatted statement output in a single String which is IDE friendly for viewing
   */
  public String executeAndStringify(String statement) {
    List<Object[]> objects = executeStatement(statement);
    return objects.stream()
        .map(o -> Joiner.on("\t").useForNull("NULL").join(o))
        .collect(Collectors.joining("\n"));
  }

  public Configuration getHiveConf() {
    if (session != null) {
      return session.get().getHiveConf();
    } else {
      return hs2Conf;
    }
  }

  public HiveSession getSession() {
    return session.get();
  }

  private HiveConf initializeConf() {
    HiveConf hiveConf = new HiveConf();

    // Use ephemeral port to enable running tests in parallel
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_PORT, 0);
    // Disable the web UI
    hiveConf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_WEBUI_PORT, -1);

    // Switch off optimizers in order to contain the map reduction within this JVM
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_CBO_ENABLED, true);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_INFER_BUCKET_SORT, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_METADATA_ONLY_QUERIES, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_OPT_INDEX_FILTER, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_CONVERT_JOIN, false);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SKEW_JOIN, false);

    // Speed up test execution
    hiveConf.setLongVar(HiveConf.ConfVars.HIVE_COUNTERS_PULL_INTERVAL, 1L);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_STATS_AUTOGATHER, false);

    // Resource configuration
    hiveConf.setInt("mapreduce.map.memory.mb", 1024);

    // Tez configuration
    hiveConf.setBoolean("tez.local.mode", true);
    hiveConf.setBoolean("tez.local.mode.without.network", true);

    // Disable vectorization for HiveIcebergInputFormat
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, false);

    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
    hiveConf.set(HiveConf.ConfVars.HIVE_TXN_MANAGER.varname, DbTxnManager.class.getName());
    hiveConf.set(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER.varname, SQLStdHiveAuthorizerFactory.class.getName());

    // set to false so that TxnManager#checkLock does not throw exception when using UNSET data type operation
    // in the requested lock component
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST, false);
    // set to true so that the Tez session will create an empty jar for localization
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST_IDE, true);

    // set lifecycle hooks
    hiveConf.setVar(HiveConf.ConfVars.HIVE_QUERY_LIFETIME_HOOKS, HiveIcebergQueryLifeTimeHook.class.getName());

    return hiveConf;
  }
}
