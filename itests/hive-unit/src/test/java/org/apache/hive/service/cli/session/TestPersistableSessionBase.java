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

package org.apache.hive.service.cli.session;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.session.store.HiveSessionSnapshot;
import org.apache.hive.service.cli.session.store.SessionStateStore;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Abstract integration test for Persistable Sessions feature.
 * Tests that session state (configs, temp tables, data) is persisted to a shared
 * store and recovered by another HS2 instance on failover.
 *
 * Subclasses provide the store implementation class and infrastructure setup
 * (e.g. ZooKeeper TestingServer, Redis container) via {@link #getStoreClassName()}
 * and {@link #configureStore(HiveConf)}.
 */
public abstract class TestPersistableSessionBase {

  protected MiniHS2 miniHs2First;
  protected MiniHS2 miniHs2Second;
  protected HiveConf hiveConf1;
  protected HiveConf hiveConf2;

  protected abstract String getStoreClassName();

  protected abstract void configureStore(HiveConf conf);

  protected abstract SessionStateStore createVerifyStore() throws Exception;

  @Before
  public void setUp() throws Exception {
    hiveConf1 = new HiveConf();
    hiveConf1.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    configurePersistableSession(hiveConf1);
    miniHs2First = new MiniHS2.Builder().withConf(hiveConf1).withHTTPTransport()
        .cleanupLocalDirOnStartup(false).build();

    hiveConf2 = new HiveConf();
    hiveConf2.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    configurePersistableSession(hiveConf2);
    miniHs2Second = new MiniHS2.Builder().withConf(hiveConf2).withHTTPTransport()
        .cleanupLocalDirOnStartup(false).build();
  }

  @After
  public void tearDown() {
    if (miniHs2First != null && miniHs2First.isStarted()) {
      miniHs2First.stop();
    }
    if (miniHs2Second != null && miniHs2Second.isStarted()) {
      miniHs2Second.stop();
    }
  }

  private void executeStatementAndWait(CLIServiceClient client, SessionHandle sessHandle,
      String statement, Map<String, String> confOverlay) throws Exception {
    OperationHandle opHandle = client.executeStatementAsync(sessHandle, statement, confOverlay);
    long timeout = System.currentTimeMillis() + 60000;
    while (true) {
      OperationState state = client.getOperationStatus(opHandle, false).getState();
      if (state == OperationState.FINISHED) {
        break;
      }
      if (state == OperationState.ERROR || state == OperationState.CANCELED) {
        fail("Operation failed with state: " + state + " for statement: " + statement);
      }
      if (System.currentTimeMillis() > timeout) {
        fail("Timed out waiting for: " + statement);
      }
      Thread.sleep(200);
    }
  }

  /**
   * Polls the session state store until the snapshot satisfies the given condition or times out.
   * Needed because the snapshot save runs asynchronously on the background thread after the
   * operation state becomes FINISHED (there is a small window between state visibility and
   * save completion).
   */
  private HiveSessionSnapshot waitForSnapshotCondition(SessionStateStore store,
      String storeKey, java.util.function.Predicate<HiveSessionSnapshot> condition,
      long timeoutMs) throws Exception {
    long deadline = System.currentTimeMillis() + timeoutMs;
    HiveSessionSnapshot snapshot = null;
    while (System.currentTimeMillis() < deadline) {
      snapshot = store.getSnapshot(storeKey);
      if (snapshot != null && condition.test(snapshot)) {
        return snapshot;
      }
      Thread.sleep(100);
    }
    return snapshot;
  }

  private void configurePersistableSession(HiveConf conf) {
    conf.setVar(ConfVars.HIVE_SERVER2_SESSION_STATE_STORE_CLASS, getStoreClassName());
    conf.setVar(ConfVars.HIVE_SERVER2_SESSION_STATE_STORE_FETCH_STRATEGY, "FETCH_WHEN_MISSING");
    conf.setVar(ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    conf.setBoolean("tez.local.mode", true);
    conf.setBoolean("tez.local.mode.without.network", true);
    conf.setVar(ConfVars.HIVE_JAR_DIRECTORY, System.getProperty("java.io.tmpdir"));
    configureStore(conf);
  }

  @Test(timeout = 120000)
  public void testSessionRecoveryOnFailover() throws Exception {
    Map<String, String> confOverlay = new HashMap<>();
    miniHs2First.start(confOverlay);

    CLIServiceClient client1 = miniHs2First.getServiceClient();
    SessionHandle sessHandle = client1.openSession("foo", "bar");
    executeStatementAndWait(client1, sessHandle, "SET hive.exec.dynamic.partition=true", confOverlay);
    executeStatementAndWait(client1, sessHandle, "SET hive.exec.dynamic.partition.mode=nonstrict", confOverlay);
    executeStatementAndWait(client1, sessHandle,
        "CREATE TEMPORARY TABLE tmp_failover_test (id INT, name STRING)", confOverlay);
    executeStatementAndWait(client1, sessHandle,
        "INSERT INTO tmp_failover_test VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')", confOverlay);

    miniHs2First.stop();
    miniHs2Second.start(confOverlay);

    CLIServiceClient client2 = miniHs2Second.getServiceClient();

    // Present the SAME session handle — triggers recovery from shared store
    OperationHandle opHandle = client2.executeStatement(sessHandle, "SELECT 1", confOverlay);
    RowSet rowSet = client2.fetchResults(opHandle);
    assertEquals(1, rowSet.numRows());

    // Verify configs recovered
    opHandle = client2.executeStatement(sessHandle, "SET hive.exec.dynamic.partition", confOverlay);
    rowSet = client2.fetchResults(opHandle);
    assertTrue(rowSet.numRows() > 0);
    assertTrue(rowSet.iterator().next()[0].toString().contains("true"));

    opHandle = client2.executeStatement(sessHandle,
        "SET hive.exec.dynamic.partition.mode", confOverlay);
    rowSet = client2.fetchResults(opHandle);
    assertTrue(rowSet.numRows() > 0);
    assertTrue(rowSet.iterator().next()[0].toString().contains("nonstrict"));

    // Verify temp table data is recovered (shared filesystem, LOCATION preserved)
    opHandle = client2.executeStatement(sessHandle,
        "SELECT id FROM tmp_failover_test ORDER BY id", confOverlay);
    rowSet = client2.fetchResults(opHandle);
    assertEquals(3, rowSet.numRows());

    client2.closeSession(sessHandle);
  }

  @Test(timeout = 120000)
  public void testFetchStrategyNeverNoRecovery() throws Exception {
    hiveConf1.setVar(ConfVars.HIVE_SERVER2_SESSION_STATE_STORE_FETCH_STRATEGY, "NEVER");
    miniHs2First = new MiniHS2.Builder().withConf(hiveConf1).withHTTPTransport()
        .cleanupLocalDirOnStartup(false).build();

    hiveConf2.setVar(ConfVars.HIVE_SERVER2_SESSION_STATE_STORE_FETCH_STRATEGY, "NEVER");
    miniHs2Second = new MiniHS2.Builder().withConf(hiveConf2).withHTTPTransport()
        .cleanupLocalDirOnStartup(false).build();

    Map<String, String> confOverlay = new HashMap<>();
    miniHs2First.start(confOverlay);

    CLIServiceClient client1 = miniHs2First.getServiceClient();
    SessionHandle sessHandle = client1.openSession("foo", "bar");
    executeStatementAndWait(client1, sessHandle, "SET hive.exec.dynamic.partition=true", confOverlay);

    miniHs2First.stop();
    miniHs2Second.start(confOverlay);

    CLIServiceClient client2 = miniHs2Second.getServiceClient();
    try {
      client2.executeStatement(sessHandle, "SELECT 1", confOverlay);
      fail("Expected HiveSQLException for invalid session handle");
    } catch (HiveSQLException e) {
      assertTrue("Expected 'Invalid SessionHandle' error, got: " + e.getMessage(),
          e.getMessage().contains("Invalid SessionHandle"));
    }
  }

  @Test(timeout = 120000)
  public void testConfigsRecoveredAfterFailover() throws Exception {
    Map<String, String> confOverlay = new HashMap<>();
    miniHs2First.start(confOverlay);

    CLIServiceClient client1 = miniHs2First.getServiceClient();
    SessionHandle sessHandle = client1.openSession("foo", "bar");
    executeStatementAndWait(client1, sessHandle, "SET hive.exec.dynamic.partition=true", confOverlay);
    executeStatementAndWait(client1, sessHandle, "SET hive.exec.dynamic.partition.mode=nonstrict", confOverlay);
    executeStatementAndWait(client1, sessHandle, "SET hive.mapred.mode=strict", confOverlay);

    miniHs2First.stop();
    miniHs2Second.start(confOverlay);

    CLIServiceClient client2 = miniHs2Second.getServiceClient();

    OperationHandle opHandle = client2.executeStatement(sessHandle,
        "SET hive.exec.dynamic.partition", confOverlay);
    RowSet rowSet = client2.fetchResults(opHandle);
    assertTrue(rowSet.numRows() > 0);
    assertTrue(rowSet.iterator().next()[0].toString().contains("true"));

    opHandle = client2.executeStatement(sessHandle,
        "SET hive.exec.dynamic.partition.mode", confOverlay);
    rowSet = client2.fetchResults(opHandle);
    assertTrue(rowSet.numRows() > 0);
    assertTrue(rowSet.iterator().next()[0].toString().contains("nonstrict"));

    opHandle = client2.executeStatement(sessHandle, "SET hive.mapred.mode", confOverlay);
    rowSet = client2.fetchResults(opHandle);
    assertTrue(rowSet.numRows() > 0);
    assertTrue(rowSet.iterator().next()[0].toString().contains("strict"));

    client2.closeSession(sessHandle);
  }

  @Test(timeout = 120000)
  public void testTempTableWithDataRecoveredAfterFailover() throws Exception {
    Map<String, String> confOverlay = new HashMap<>();
    miniHs2First.start(confOverlay);

    CLIServiceClient client1 = miniHs2First.getServiceClient();
    SessionHandle sessHandle = client1.openSession("foo", "bar");
    executeStatementAndWait(client1, sessHandle,
        "CREATE TEMPORARY TABLE tmp_data_test (id INT, name STRING)", confOverlay);
    executeStatementAndWait(client1, sessHandle,
        "INSERT INTO tmp_data_test VALUES (10, 'hive'), (20, 'hadoop')", confOverlay);

    miniHs2First.stop();
    miniHs2Second.start(confOverlay);

    CLIServiceClient client2 = miniHs2Second.getServiceClient();

    // Verify temp table schema is recovered (table exists after failover)
    OperationHandle opHandle = client2.executeStatement(sessHandle,
        "DESCRIBE tmp_data_test", confOverlay);
    RowSet rowSet = client2.fetchResults(opHandle);
    assertTrue("Temp table schema should be recovered", rowSet.numRows() > 0);

    // Verify the data is recovered (shared filesystem, LOCATION preserved in DDL)
    opHandle = client2.executeStatement(sessHandle,
        "SELECT id, name FROM tmp_data_test ORDER BY id", confOverlay);
    rowSet = client2.fetchResults(opHandle);
    assertEquals(2, rowSet.numRows());
    Iterator<Object[]> it = rowSet.iterator();
    Object[] row1 = it.next();
    assertEquals("10", row1[0].toString());
    assertEquals("hive", row1[1].toString());
    Object[] row2 = it.next();
    assertEquals("20", row2[0].toString());
    assertEquals("hadoop", row2[1].toString());

    client2.closeSession(sessHandle);
  }

  @Test(timeout = 120000)
  public void testQueryRetrySucceedsOnSecondHS2AfterCrash() throws Exception {
    Map<String, String> confOverlay = new HashMap<>();
    miniHs2First.start(confOverlay);

    CLIServiceClient client1 = miniHs2First.getServiceClient();
    SessionHandle sessHandle = client1.openSession("foo", "bar");

    // Create a regular table and insert data on HS2-1
    client1.executeStatement(sessHandle,
        "CREATE TABLE IF NOT EXISTS retry_test (id INT, val STRING)", confOverlay);
    client1.executeStatement(sessHandle,
        "INSERT INTO retry_test VALUES (1, 'one'), (2, 'two'), (3, 'three')", confOverlay);

    // Verify query works on HS2-1
    OperationHandle opHandle = client1.executeStatement(sessHandle,
        "SELECT count(*) FROM retry_test", confOverlay);
    RowSet rowSet = client1.fetchResults(opHandle);
    assertEquals(3L, Long.parseLong(rowSet.iterator().next()[0].toString()));

    // HS2-1 crashes
    miniHs2First.stop();

    // HS2-2 comes up — same shared store
    miniHs2Second.start(confOverlay);
    CLIServiceClient client2 = miniHs2Second.getServiceClient();

    // Retry the query with the SAME session handle on HS2-2
    // Without persistable sessions this would throw "Invalid SessionHandle"
    opHandle = client2.executeStatement(sessHandle,
        "SELECT count(*) FROM retry_test", confOverlay);
    rowSet = client2.fetchResults(opHandle);
    assertTrue(rowSet.numRows() > 0);
    assertEquals(3L, Long.parseLong(rowSet.iterator().next()[0].toString()));

    // Further queries on the recovered session also work
    opHandle = client2.executeStatement(sessHandle,
        "SELECT val FROM retry_test WHERE id = 2", confOverlay);
    rowSet = client2.fetchResults(opHandle);
    assertTrue(rowSet.numRows() > 0);
    assertEquals("two", rowSet.iterator().next()[0].toString());

    // Cleanup
    client2.executeStatement(sessHandle, "DROP TABLE retry_test", confOverlay);
    client2.closeSession(sessHandle);
  }

  @Test(timeout = 120000)
  public void testSnapshotWipedOnSessionClose() throws Exception {
    Map<String, String> confOverlay = new HashMap<>();
    miniHs2First.start(confOverlay);

    CLIServiceClient client1 = miniHs2First.getServiceClient();
    SessionHandle sessHandle = client1.openSession("foo", "bar");
    executeStatementAndWait(client1, sessHandle, "SET hive.exec.dynamic.partition=true", confOverlay);

    SessionStateStore verifyStore = createVerifyStore();
    String storeKey = sessHandle.getHandleIdentifier().getPublicId().toString() + ":"
        + sessHandle.getHandleIdentifier().getSecretId().toString();
    assertNotNull("Snapshot should exist before close", verifyStore.getSnapshot(storeKey));

    client1.closeSession(sessHandle);

    assertNull("Snapshot should be wiped after session close", verifyStore.getSnapshot(storeKey));
    verifyStore.close();
  }

  @Test(timeout = 120000)
  public void testTempTablesAcrossDifferentDatabasesRecovered() throws Exception {
    Map<String, String> confOverlay = new HashMap<>();
    miniHs2First.start(confOverlay);

    CLIServiceClient client1 = miniHs2First.getServiceClient();
    SessionHandle sessHandle = client1.openSession("foo", "bar");

    // Create two databases and a temp table in each
    executeStatementAndWait(client1, sessHandle, "CREATE DATABASE IF NOT EXISTS db_alpha", confOverlay);
    executeStatementAndWait(client1, sessHandle, "CREATE DATABASE IF NOT EXISTS db_beta", confOverlay);

    executeStatementAndWait(client1, sessHandle, "USE db_alpha", confOverlay);
    executeStatementAndWait(client1, sessHandle,
        "CREATE TEMPORARY TABLE tmp_cross_db (id INT, label STRING)", confOverlay);
    executeStatementAndWait(client1, sessHandle,
        "INSERT INTO tmp_cross_db VALUES (1, 'alpha_row')", confOverlay);

    executeStatementAndWait(client1, sessHandle, "USE db_beta", confOverlay);
    executeStatementAndWait(client1, sessHandle,
        "CREATE TEMPORARY TABLE tmp_cross_db (id INT, label STRING)", confOverlay);
    executeStatementAndWait(client1, sessHandle,
        "INSERT INTO tmp_cross_db VALUES (2, 'beta_row')", confOverlay);

    // Failover
    miniHs2First.stop();
    miniHs2Second.start(confOverlay);

    CLIServiceClient client2 = miniHs2Second.getServiceClient();

    // Verify the temp table in db_alpha has the correct data
    client2.executeStatement(sessHandle, "USE db_alpha", confOverlay);
    OperationHandle opHandle = client2.executeStatement(sessHandle,
        "SELECT id, label FROM tmp_cross_db", confOverlay);
    RowSet rowSet = client2.fetchResults(opHandle);
    assertEquals(1, rowSet.numRows());
    Object[] row = rowSet.iterator().next();
    assertEquals("1", row[0].toString());
    assertEquals("alpha_row", row[1].toString());

    // Verify the temp table in db_beta has the correct data
    client2.executeStatement(sessHandle, "USE db_beta", confOverlay);
    opHandle = client2.executeStatement(sessHandle,
        "SELECT id, label FROM tmp_cross_db", confOverlay);
    rowSet = client2.fetchResults(opHandle);
    assertEquals(1, rowSet.numRows());
    row = rowSet.iterator().next();
    assertEquals("2", row[0].toString());
    assertEquals("beta_row", row[1].toString());

    // Verify the current database is restored to what it was before failover (db_beta)
    opHandle = client2.executeStatement(sessHandle, "SELECT current_database()", confOverlay);
    rowSet = client2.fetchResults(opHandle);
    assertEquals("db_beta", rowSet.iterator().next()[0].toString());

    client2.closeSession(sessHandle);
  }

  @Test(timeout = 120000)
  public void testTempTableWithPartitionsAndComplexTypesRecovered() throws Exception {
    Map<String, String> confOverlay = new HashMap<>();
    miniHs2First.start(confOverlay);

    CLIServiceClient client1 = miniHs2First.getServiceClient();
    SessionHandle sessHandle = client1.openSession("foo", "bar");

    executeStatementAndWait(client1, sessHandle,
        "CREATE TEMPORARY TABLE tmp_complex ("
            + "id INT, "
            + "info STRUCT<name:STRING,age:INT>, "
            + "tags ARRAY<STRING>, "
            + "metadata MAP<STRING,STRING>"
            + ") PARTITIONED BY (dt STRING) "
            + "TBLPROPERTIES ('custom.key'='custom.value', 'transient_lastDdlTime'='0')",
        confOverlay);
    executeStatementAndWait(client1, sessHandle,
        "INSERT INTO tmp_complex PARTITION(dt='2024-01-01') "
            + "VALUES (1, named_struct('name','alice','age',30), "
            + "array('tag1','tag2'), map('k1','v1'))",
        confOverlay);

    // Verify partition metadata is captured in the snapshot after INSERT
    SessionStateStore verifyStore = createVerifyStore();
    String storeKey = sessHandle.getHandleIdentifier().getPublicId().toString() + ":"
        + sessHandle.getHandleIdentifier().getSecretId().toString();
    HiveSessionSnapshot snapshot = waitForSnapshotCondition(verifyStore, storeKey,
        s -> s.getTempTablePartitionDefinitions() != null
            && !s.getTempTablePartitionDefinitions().isEmpty(),
        10000);
    assertNotNull("Snapshot should exist after INSERT into partitioned temp table", snapshot);
    assertTrue("tempTablePartitionDefinitions should contain partition metadata for tmp_complex",
        snapshot.getTempTablePartitionDefinitions().values().stream()
            .flatMap(List::stream)
            .anyMatch(p -> p.getValues().contains("2024-01-01")
                && p.getLocation() != null && !p.getLocation().isEmpty()));
    verifyStore.close();

    miniHs2First.stop();
    miniHs2Second.start(confOverlay);

    CLIServiceClient client2 = miniHs2Second.getServiceClient();

    // Verify table schema is recovered with complex types and partitions
    OperationHandle opHandle = client2.executeStatement(sessHandle,
        "DESCRIBE tmp_complex", confOverlay);
    RowSet rowSet = client2.fetchResults(opHandle);
    assertTrue("Partitioned temp table with complex types should be recovered",
        rowSet.numRows() > 0);

    // Verify pre-failover data is queryable (partition metadata + LOCATION restored)
    opHandle = client2.executeStatement(sessHandle,
        "SELECT id, info.name, tags[0], metadata['k1'], dt FROM tmp_complex "
            + "WHERE dt='2024-01-01'", confOverlay);
    rowSet = client2.fetchResults(opHandle);
    assertEquals(1, rowSet.numRows());
    Object[] row = rowSet.iterator().next();
    assertEquals("1", row[0].toString());
    assertEquals("alice", row[1].toString());
    assertEquals("tag1", row[2].toString());
    assertEquals("v1", row[3].toString());
    assertEquals("2024-01-01", row[4].toString());

    // Verify new inserts still work after recovery
    executeStatementAndWait(client2, sessHandle,
        "INSERT INTO tmp_complex PARTITION(dt='2024-02-01') "
            + "VALUES (2, named_struct('name','bob','age',25), "
            + "array('x'), map('k2','v2'))",
        confOverlay);
    opHandle = client2.executeStatement(sessHandle,
        "SELECT id, info.name, tags[0], metadata['k2'], dt FROM tmp_complex "
            + "WHERE dt='2024-02-01'", confOverlay);
    rowSet = client2.fetchResults(opHandle);
    assertEquals(1, rowSet.numRows());
    row = rowSet.iterator().next();
    assertEquals("2", row[0].toString());
    assertEquals("bob", row[1].toString());
    assertEquals("x", row[2].toString());
    assertEquals("v2", row[3].toString());
    assertEquals("2024-02-01", row[4].toString());

    client2.closeSession(sessHandle);
  }

  @Test(timeout = 120000)
  public void testAddedFilesRecoveredAfterFailover() throws Exception {
    Map<String, String> confOverlay = new HashMap<>();
    miniHs2First.start(confOverlay);

    CLIServiceClient client1 = miniHs2First.getServiceClient();
    SessionHandle sessHandle = client1.openSession("foo", "bar");

    // Create the file before ADD FILE
    executeStatementAndWait(client1, sessHandle,
        "CREATE TEMPORARY TABLE tmp_file_helper (line STRING)", confOverlay);
    executeStatementAndWait(client1, sessHandle,
        "INSERT INTO tmp_file_helper VALUES ('hello')", confOverlay);

    executeStatementAndWait(client1, sessHandle,
        "INSERT OVERWRITE LOCAL DIRECTORY '/tmp/test_persistable_file_dir' "
            + "SELECT 'test_content' FROM tmp_file_helper LIMIT 1", confOverlay);
    executeStatementAndWait(client1, sessHandle,
        "ADD FILE /tmp/test_persistable_file_dir/000000_0", confOverlay);

    // Verify the snapshot contains the file — poll briefly because the snapshot save
    // runs on the background thread after the operation state becomes FINISHED
    SessionStateStore verifyStore = createVerifyStore();
    String storeKey = sessHandle.getHandleIdentifier().getPublicId().toString() + ":"
        + sessHandle.getHandleIdentifier().getSecretId().toString();
    HiveSessionSnapshot snapshot = waitForSnapshotCondition(verifyStore, storeKey,
        s -> !s.getAddedFiles().isEmpty(), 5000);
    assertNotNull("Snapshot should exist", snapshot);
    assertTrue("addedFiles should contain the file",
        snapshot.getAddedFiles().stream()
            .anyMatch(f -> f.contains("000000_0")));

    // Failover to second HS2
    miniHs2First.stop();
    miniHs2Second.start(confOverlay);

    CLIServiceClient client2 = miniHs2Second.getServiceClient();

    // Verify session is recovered and file resource is available
    OperationHandle opHandle = client2.executeStatement(sessHandle,
        "SELECT 1", confOverlay);
    RowSet rowSet = client2.fetchResults(opHandle);
    assertEquals(1, rowSet.numRows());

    // Verify the recovered snapshot on the second HS2 also has the file
    HiveSessionSnapshot recoveredSnapshot = verifyStore.getSnapshot(storeKey);
    assertNotNull("Snapshot should exist after recovery", recoveredSnapshot);
    assertTrue("addedFiles should be preserved after recovery",
        recoveredSnapshot.getAddedFiles().stream()
            .anyMatch(f -> f.contains("000000_0")));

    client2.closeSession(sessHandle);
    verifyStore.close();
  }

  @Test(timeout = 120000)
  public void testTempFunctionRecoveredAfterFailover() throws Exception {
    Map<String, String> confOverlay = new HashMap<>();
    miniHs2First.start(confOverlay);

    CLIServiceClient client1 = miniHs2First.getServiceClient();
    SessionHandle sessHandle = client1.openSession("foo", "bar");

    // Register a temporary function using GenericUDFUpper (always on HS2 classpath)
    executeStatementAndWait(client1, sessHandle,
        "CREATE TEMPORARY FUNCTION tmp_my_upper AS "
            + "'org.apache.hadoop.hive.ql.udf.generic.GenericUDFUpper'",
        confOverlay);

    // Verify snapshot has the function captured — poll briefly because the snapshot save
    // runs on the background thread after the operation state becomes FINISHED
    SessionStateStore verifyStore = createVerifyStore();
    String storeKey = sessHandle.getHandleIdentifier().getPublicId().toString() + ":"
        + sessHandle.getHandleIdentifier().getSecretId().toString();
    HiveSessionSnapshot snapshot = waitForSnapshotCondition(verifyStore, storeKey,
        s -> !s.getTempFunctionDefinitions().isEmpty(), 5000);
    assertNotNull("Snapshot should exist after CREATE TEMPORARY FUNCTION", snapshot);
    assertTrue("tempFunctionDefinitions should contain tmp_my_upper, but got: "
            + snapshot.getTempFunctionDefinitions(),
        snapshot.getTempFunctionDefinitions().stream()
            .anyMatch(d -> d.contains("tmp_my_upper")));
    verifyStore.close();

    // Verify it works before failover
    OperationHandle opHandle = client1.executeStatement(sessHandle,
        "SELECT tmp_my_upper('hello')", confOverlay);
    RowSet rowSet = client1.fetchResults(opHandle);
    assertEquals(1, rowSet.numRows());
    assertEquals("HELLO", rowSet.iterator().next()[0].toString());

    // Failover
    miniHs2First.stop();
    miniHs2Second.start(confOverlay);

    CLIServiceClient client2 = miniHs2Second.getServiceClient();

    // Verify the temp function is usable after recovery
    opHandle = client2.executeStatement(sessHandle,
        "SELECT tmp_my_upper('recovered')", confOverlay);
    rowSet = client2.fetchResults(opHandle);
    assertEquals(1, rowSet.numRows());
    assertEquals("RECOVERED", rowSet.iterator().next()[0].toString());

    client2.closeSession(sessHandle);
  }

  @Test(timeout = 120000)
  public void testAlwaysStrategySyncsFromRemoteWhenStale() throws Exception {
    hiveConf1.setVar(ConfVars.HIVE_SERVER2_SESSION_STATE_STORE_FETCH_STRATEGY, "ALWAYS");
    miniHs2First = new MiniHS2.Builder().withConf(hiveConf1).withHTTPTransport()
        .cleanupLocalDirOnStartup(false).build();

    Map<String, String> confOverlay = new HashMap<>();
    miniHs2First.start(confOverlay);

    CLIServiceClient client1 = miniHs2First.getServiceClient();
    SessionHandle sessHandle = client1.openSession("foo", "bar");
    executeStatementAndWait(client1, sessHandle, "SET hive.exec.dynamic.partition=true", confOverlay);

    // Now simulate another HS2 updating the store with a newer snapshot
    // that has an additional config and a later lastAccessTime
    SessionStateStore verifyStore = createVerifyStore();
    String storeKey = sessHandle.getHandleIdentifier().getPublicId().toString() + ":"
        + sessHandle.getHandleIdentifier().getSecretId().toString();
    HiveSessionSnapshot current = verifyStore.getSnapshot(storeKey);
    assertNotNull(current);

    Map<String, String> updatedConfigs = new HashMap<>();
    if (current.getOverriddenConfigurations() != null) {
      updatedConfigs.putAll(current.getOverriddenConfigurations());
    }
    updatedConfigs.put("hive.mapred.mode", "strict");

    HiveSessionSnapshot newerSnapshot = HiveSessionSnapshot.builder()
        .sessionHandleId(current.getSessionHandleId())
        .username(current.getUsername())
        .ipAddress(current.getIpAddress())
        .currentDatabase(current.getCurrentDatabase())
        .overriddenConfigurations(updatedConfigs)
        .addedJars(current.getAddedJars() != null ? current.getAddedJars() : new ArrayList<>())
        .addedFiles(current.getAddedFiles() != null ? current.getAddedFiles() : new ArrayList<>())
        .tempTableDefinitions(current.getTempTableDefinitions())
        .tempTablePartitionDefinitions(current.getTempTablePartitionDefinitions())
        .tempFunctionDefinitions(current.getTempFunctionDefinitions() != null
            ? current.getTempFunctionDefinitions() : new ArrayList<>())
        .protocolVersion(current.getProtocolVersion())
        .creationTime(current.getCreationTime())
        .lastAccessTime(System.currentTimeMillis() + 60000)
        .build();
    verifyStore.saveSnapshot(storeKey, newerSnapshot);

    // Access the session again — ALWAYS strategy should detect the remote is newer and re-hydrate
    OperationHandle opHandle = client1.executeStatement(sessHandle,
        "SET hive.mapred.mode", confOverlay);
    RowSet rowSet = client1.fetchResults(opHandle);
    assertTrue(rowSet.numRows() > 0);
    assertTrue("ALWAYS strategy should have synced hive.mapred.mode=strict from remote",
        rowSet.iterator().next()[0].toString().contains("strict"));

    client1.closeSession(sessHandle);
    verifyStore.close();
  }
}
