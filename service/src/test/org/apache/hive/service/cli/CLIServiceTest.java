/**
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

package org.apache.hive.service.cli;

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryDisplay;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.plan.api.StageType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.thrift.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * CLIServiceTest.
 *
 */
public abstract class CLIServiceTest {
  private static final Logger LOG = LoggerFactory.getLogger(CLIServiceTest.class);

  protected static CLIServiceClient client;

  /**
   * @throws java.lang.Exception
   */
  @Before
  public void setUp() throws Exception {
  }

  /**
   * @throws java.lang.Exception
   */
  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void testOpenSession() throws Exception {
    SessionHandle sessionHandle = client.openSession(
        "tom", "password", Collections.<String, String>emptyMap());
    assertNotNull(sessionHandle);
    client.closeSession(sessionHandle);

    sessionHandle = client.openSession("tom", "password");
    assertNotNull(sessionHandle);
    client.closeSession(sessionHandle);
  }

  @Test
  public void testGetFunctions() throws Exception {
    SessionHandle sessionHandle = client.openSession("tom", "password");
    assertNotNull(sessionHandle);

    OperationHandle opHandle = client.getFunctions(sessionHandle, null, null, "*");
    TableSchema schema = client.getResultSetMetadata(opHandle);

    ColumnDescriptor columnDesc = schema.getColumnDescriptorAt(0);
    assertEquals("FUNCTION_CAT", columnDesc.getName());
    assertEquals(Type.STRING_TYPE, columnDesc.getType());

    columnDesc = schema.getColumnDescriptorAt(1);
    assertEquals("FUNCTION_SCHEM", columnDesc.getName());
    assertEquals(Type.STRING_TYPE, columnDesc.getType());

    columnDesc = schema.getColumnDescriptorAt(2);
    assertEquals("FUNCTION_NAME", columnDesc.getName());
    assertEquals(Type.STRING_TYPE, columnDesc.getType());

    columnDesc = schema.getColumnDescriptorAt(3);
    assertEquals("REMARKS", columnDesc.getName());
    assertEquals(Type.STRING_TYPE, columnDesc.getType());

    columnDesc = schema.getColumnDescriptorAt(4);
    assertEquals("FUNCTION_TYPE", columnDesc.getName());
    assertEquals(Type.INT_TYPE, columnDesc.getType());

    columnDesc = schema.getColumnDescriptorAt(5);
    assertEquals("SPECIFIC_NAME", columnDesc.getName());
    assertEquals(Type.STRING_TYPE, columnDesc.getType());

    // Cleanup
    client.closeOperation(opHandle);
    client.closeSession(sessionHandle);
  }

  @Test
  public void testGetInfo() throws Exception {
    SessionHandle sessionHandle = client.openSession(
        "tom", "password", Collections.<String, String>emptyMap());
    assertNotNull(sessionHandle);

    GetInfoValue value = client.getInfo(sessionHandle, GetInfoType.CLI_DBMS_NAME);
    System.out.println(value.getStringValue());

    value = client.getInfo(sessionHandle, GetInfoType.CLI_SERVER_NAME);
    System.out.println(value.getStringValue());

    value = client.getInfo(sessionHandle, GetInfoType.CLI_DBMS_VER);
    System.out.println(value.getStringValue());

    client.closeSession(sessionHandle);
  }

  /**
   * Test the blocking execution of a query
   * @throws Exception
   */
  @Test
  public void testExecuteStatement() throws Exception {
    HashMap<String, String> confOverlay = new HashMap<String, String>();
    SessionHandle sessionHandle = client.openSession(
        "tom", "password", new HashMap<String, String>());
    assertNotNull(sessionHandle);

    OperationHandle opHandle;

    String queryString = "SET " + HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname
        + " = false";
    opHandle = client.executeStatement(sessionHandle, queryString, confOverlay);
    client.closeOperation(opHandle);

    queryString = "DROP TABLE IF EXISTS TEST_EXEC";
    opHandle = client.executeStatement(sessionHandle, queryString, confOverlay);
    client.closeOperation(opHandle);

    // Create a test table
    queryString = "CREATE TABLE TEST_EXEC(ID STRING)";
    opHandle = client.executeStatement(sessionHandle, queryString, confOverlay);
    client.closeOperation(opHandle);

    // Blocking execute
    queryString = "SELECT ID+1 FROM TEST_EXEC";
    opHandle = client.executeStatement(sessionHandle, queryString, confOverlay);

    OperationStatus opStatus = client.getOperationStatus(opHandle, false);
    checkOperationTimes(opHandle, opStatus);
    // Expect query to be completed now
    assertEquals("Query should be finished",
        OperationState.FINISHED, client.getOperationStatus(opHandle, false).getState());
    client.closeOperation(opHandle);

    // Cleanup
    queryString = "DROP TABLE IF EXISTS TEST_EXEC";
    opHandle = client.executeStatement(sessionHandle, queryString, confOverlay);
    client.closeOperation(opHandle);
    client.closeSession(sessionHandle);
  }

  /**
   * Test async execution of a well-formed and a malformed query with different long polling durations
   * - Test malformed query with default long polling timeout
   * - Test well-formed query with default long polling timeout
   * - Test well-formed query with long polling timeout set to 0
   * - Test well-formed query with long polling timeout set to 500 millis
   * - Test well-formed query cancellation
   * @throws Exception
   */
  @Test
  public void testExecuteStatementAsync() throws Exception {
    Map<String, String> confOverlay = new HashMap<String, String>();
    String tableName = "TEST_EXEC_ASYNC";
    String columnDefinitions = "(ID STRING)";
    String queryString;

    // Open a session and set up the test data
    SessionHandle sessionHandle = setupTestData(tableName, columnDefinitions, confOverlay);
    assertNotNull(sessionHandle);

    OperationState state = null;
    OperationHandle opHandle;
    OperationStatus opStatus = null;

    // Change lock manager, otherwise unit-test doesn't go through
    queryString = "SET " + HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname
        + " = false";
    opHandle = client.executeStatement(sessionHandle, queryString, confOverlay);
    client.closeOperation(opHandle);

    // Set longPollingTimeout to a custom value for different test cases
    long longPollingTimeout;

    /**
     * Execute a malformed async query with default config,
     * to give a compile time error.
     * (compilation is done synchronous as of now)
     */
    longPollingTimeout = HiveConf.getTimeVar(new HiveConf(),
        HiveConf.ConfVars.HIVE_SERVER2_LONG_POLLING_TIMEOUT, TimeUnit.MILLISECONDS);
    queryString = "SELECT NON_EXISTING_COLUMN FROM " + tableName;
    try {
      runAsyncAndWait(sessionHandle, queryString, confOverlay, OperationState.ERROR, longPollingTimeout);
    }
    catch (HiveSQLException e) {
      // expected error
    }

    /**
     * Execute a malformed async query with default config,
     * to give a runtime time error.
     * Also check that the sqlState and errorCode should be set
     */
    queryString = "CREATE TABLE NON_EXISTING_TAB (ID STRING) location 'invalid://localhost:10000/a/b/c'";
    opStatus = runAsyncAndWait(sessionHandle, queryString, confOverlay, OperationState.ERROR, longPollingTimeout);
    // sqlState, errorCode should be set
    assertEquals(opStatus.getOperationException().getSQLState(), "08S01");
    assertEquals(opStatus.getOperationException().getErrorCode(), 1);
    /**
     * Execute an async query with default config
     */
    queryString = "SELECT ID+1 FROM " + tableName;
    runAsyncAndWait(sessionHandle, queryString, confOverlay, OperationState.FINISHED, longPollingTimeout);

    /**
     * Execute an async query with long polling timeout set to 0
     */
    longPollingTimeout = 0;
    queryString = "SELECT ID+1 FROM " + tableName;
    runAsyncAndWait(sessionHandle, queryString, confOverlay, OperationState.FINISHED, longPollingTimeout);

    /**
     * Execute an async query with long polling timeout set to 500 millis
     */
    longPollingTimeout = 500;
    queryString = "SELECT ID+1 FROM " + tableName;
    runAsyncAndWait(sessionHandle, queryString, confOverlay, OperationState.FINISHED, longPollingTimeout);

    /**
     * Cancellation test
     */
    queryString = "SELECT ID+1 FROM " + tableName;
    opHandle = client.executeStatementAsync(sessionHandle, queryString, confOverlay);
    System.out.println("Cancelling " + opHandle);
    client.cancelOperation(opHandle);

    OperationStatus operationStatus = client.getOperationStatus(opHandle, false);
    checkOperationTimes(opHandle, operationStatus);

    state = client.getOperationStatus(opHandle, false).getState();
    System.out.println(opHandle + " after cancelling, state= " + state);
    assertEquals("Query should be cancelled", OperationState.CANCELED, state);

    // Cleanup
    queryString = "DROP TABLE " + tableName;
    client.executeStatement(sessionHandle, queryString, confOverlay);
    client.closeSession(sessionHandle);
  }


  private void syncThreadStart(final CountDownLatch cdlIn, final CountDownLatch cdlOut) {
    cdlIn.countDown();
    try {
      cdlOut.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testExecuteStatementParallel() throws Exception {
    Map<String, String> confOverlay = new HashMap<String, String>();
    String tableName = "TEST_EXEC_PARALLEL";
    String columnDefinitions = "(ID STRING)";

    // Open a session and set up the test data
    SessionHandle sessionHandle = setupTestData(tableName, columnDefinitions, confOverlay);
    assertNotNull(sessionHandle);

    long longPollingTimeout = HiveConf.getTimeVar(new HiveConf(),
        HiveConf.ConfVars.HIVE_SERVER2_LONG_POLLING_TIMEOUT, TimeUnit.MILLISECONDS);
    confOverlay.put(
        HiveConf.ConfVars.HIVE_SERVER2_LONG_POLLING_TIMEOUT.varname, longPollingTimeout + "ms");

    int THREAD_COUNT = 10, QUERY_COUNT = 10;
    // TODO: refactor this into an utility, LLAP tests use this pattern a lot
    ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
    CountDownLatch cdlIn = new CountDownLatch(THREAD_COUNT), cdlOut = new CountDownLatch(1);
    @SuppressWarnings("unchecked")
    Callable<Void>[] cs = new Callable[3];
    // Create callables with different queries.
    String query = "SELECT ID + %1$d FROM " + tableName;
    cs[0] = createQueryCallable(
        query, confOverlay, longPollingTimeout, QUERY_COUNT, OperationState.FINISHED, true, cdlIn, cdlOut);
    query = "SELECT t1.ID, SUM(t2.ID) + %1$d FROM  " + tableName + " t1 CROSS JOIN "
        + tableName + " t2 GROUP BY t1.ID HAVING t1.ID > 1";
    cs[1] = createQueryCallable(
        query, confOverlay, longPollingTimeout, QUERY_COUNT, OperationState.FINISHED, true, cdlIn, cdlOut);
    query = "SELECT b.a FROM (SELECT (t1.ID + %1$d) as a , t2.* FROM  " + tableName
        + " t1 INNER JOIN " + tableName + " t2 ON t1.ID = t2.ID WHERE t2.ID > 2) b";
    cs[2] = createQueryCallable(
        query, confOverlay, longPollingTimeout, QUERY_COUNT, OperationState.FINISHED, true, cdlIn, cdlOut);

    @SuppressWarnings("unchecked")
    FutureTask<Void>[] tasks = new FutureTask[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; ++i) {
      tasks[i] = new FutureTask<Void>(cs[i % cs.length]);
      executor.execute(tasks[i]);
    }
    try {
      cdlIn.await(); // Wait for all threads to be ready.
      cdlOut.countDown(); // Release them at the same time.
      for (int i = 0; i < THREAD_COUNT; ++i) {
        tasks[i].get();
      }
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }

    // Cleanup
    client.executeStatement(sessionHandle, "DROP TABLE " + tableName, confOverlay);
    client.closeSession(sessionHandle);
  }

  public static class CompileLockTestSleepHook implements HiveSemanticAnalyzerHook {
    @Override
    public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context,
      ASTNode ast) throws SemanticException {
      try {
        Thread.sleep(20 * 1000);
      } catch (Throwable t) {
        // do nothing
      }
      return ast;
    }

    @Override
    public void postAnalyze(HiveSemanticAnalyzerHookContext context,
      List<Task<? extends Serializable>> rootTasks) throws SemanticException {
    }
  }

  @Test
  public void testGlobalCompileLockTimeout() throws Exception {
    String tableName = "TEST_COMPILE_LOCK_TIMEOUT";
    String columnDefinitions = "(ID STRING)";

    // Open a session and set up the test data
    SessionHandle sessionHandle = setupTestData(tableName, columnDefinitions,
        new HashMap<String, String>());
    assertNotNull(sessionHandle);

    int THREAD_COUNT = 3;
    @SuppressWarnings("unchecked")
    FutureTask<Void>[] tasks = (FutureTask<Void>[])new FutureTask[THREAD_COUNT];
    long longPollingTimeoutMs = 10 * 60 * 1000; // Larger than max compile duration used in test

    // 1st query acquires the lock and takes 20 secs to compile
    Map<String, String> confOverlay = getConfOverlay(0, longPollingTimeoutMs);
    confOverlay.put(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK.varname,
        CompileLockTestSleepHook.class.getName());
    String query = "SELECT 0 FROM " + tableName;
    tasks[0] = new FutureTask<Void>(
        createQueryCallable(query, confOverlay, longPollingTimeoutMs, 1,
            OperationState.FINISHED, false, null, null));
    new Thread(tasks[0]).start();
    Thread.sleep(5 * 1000);

    // 2nd query's session has compile lock timeout of 1 sec, so it should
    // not be able to acquire the lock within that time period
    confOverlay = getConfOverlay(1, longPollingTimeoutMs);
    query = "SELECT 1 FROM " + tableName;
    tasks[1] = new FutureTask<Void>(
        createQueryCallable(query, confOverlay, longPollingTimeoutMs, 1,
            OperationState.ERROR, false, null, null));
    new Thread(tasks[1]).start();

    // 3rd query's session has compile lock timeout of 100 secs, so it should
    // be able to acquire the lock and finish successfully
    confOverlay = getConfOverlay(100, longPollingTimeoutMs);
    query = "SELECT 2 FROM " + tableName;
    tasks[2] = new FutureTask<Void>(
        createQueryCallable(query, confOverlay, longPollingTimeoutMs, 1,
            OperationState.FINISHED, false, null, null));
    new Thread(tasks[2]).start();

    boolean foundExpectedException = false;
    for (int i = 0; i < THREAD_COUNT; ++i) {
      try {
        tasks[i].get();
      } catch (Throwable t) {
        if (i == 1) {
          assertTrue(t.getMessage().contains(
              ErrorMsg.COMPILE_LOCK_TIMED_OUT.getMsg()));
          foundExpectedException = true;
        } else {
          throw new RuntimeException(t);
        }
      }
    }
    assertTrue(foundExpectedException);

    // Cleanup
    client.executeStatement(sessionHandle, "DROP TABLE " + tableName,
        getConfOverlay(0, longPollingTimeoutMs));
    client.closeSession(sessionHandle);
  }

  private Map<String, String> getConfOverlay(long compileLockTimeoutSecs,
    long longPollingTimeoutMs) {
    Map<String, String> confOverlay = new HashMap<String, String>();
    confOverlay.put(
        HiveConf.ConfVars.HIVE_SERVER2_PARALLEL_COMPILATION.varname, "false");
    confOverlay.put(
        HiveConf.ConfVars.HIVE_SERVER2_LONG_POLLING_TIMEOUT.varname,
        longPollingTimeoutMs + "ms");
    if (compileLockTimeoutSecs > 0) {
      confOverlay.put(
          HiveConf.ConfVars.HIVE_SERVER2_COMPILE_LOCK_TIMEOUT.varname,
          compileLockTimeoutSecs + "s");
    }
    return confOverlay;
  }

  private Callable<Void> createQueryCallable(final String queryStringFormat,
      final Map<String, String> confOverlay, final long longPollingTimeout,
      final int queryCount, final OperationState expectedOperationState,
      final boolean syncThreadStart, final CountDownLatch cdlIn,
      final CountDownLatch cdlOut) {
    return new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        if (syncThreadStart) {
          syncThreadStart(cdlIn, cdlOut);
        }

        SessionHandle sessionHandle = openSession(confOverlay);
        OperationHandle[] hs  = new OperationHandle[queryCount];
        for (int i = 0; i < hs.length; ++i) {
          String queryString = String.format(queryStringFormat, i);
          LOG.info("Submitting " + i);
          hs[i] = client.executeStatementAsync(sessionHandle, queryString, confOverlay);
        }
        for (int i = hs.length - 1; i >= 0; --i) {
          waitForAsyncQuery(hs[i], expectedOperationState, longPollingTimeout);
        }
        return null;
      }
    };
  }

  /**
   * Sets up a test specific table with the given column definitions and config
   * @param tableName
   * @param columnDefinitions
   * @param confOverlay
   * @throws Exception
   */
  private SessionHandle setupTestData(String tableName, String columnDefinitions,
      Map<String, String> confOverlay) throws Exception {
    SessionHandle sessionHandle = openSession(confOverlay);
    createTestTable(tableName, columnDefinitions, confOverlay, sessionHandle);
    return sessionHandle;
  }

  private SessionHandle openSession(Map<String, String> confOverlay)
      throws HiveSQLException {
    SessionHandle sessionHandle = client.openSession("tom", "password", confOverlay);
    assertNotNull(sessionHandle);
    SessionState.get().setIsHiveServerQuery(true); // Pretend we are in HS2.

    String queryString = "SET " + HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname
      + " = false";
    client.executeStatement(sessionHandle, queryString, confOverlay);
    return sessionHandle;
  }

  private void createTestTable(String tableName, String columnDefinitions,
      Map<String, String> confOverlay, SessionHandle sessionHandle)
      throws HiveSQLException {
    String queryString;
    // Drop the table if it exists
    queryString = "DROP TABLE IF EXISTS " + tableName;
    client.executeStatement(sessionHandle, queryString, confOverlay);

    // Create a test table
    queryString = "CREATE TABLE " + tableName + columnDefinitions;
    client.executeStatement(sessionHandle, queryString, confOverlay);
  }

  private OperationStatus runAsyncAndWait(SessionHandle sessionHandle, String queryString,
      Map<String, String> confOverlay, OperationState expectedState,
      long longPollingTimeout) throws HiveSQLException {
    // Timeout for the iteration in case of asynchronous execute
    confOverlay.put(
        HiveConf.ConfVars.HIVE_SERVER2_LONG_POLLING_TIMEOUT.varname, longPollingTimeout + "ms");
    OperationHandle h = client.executeStatementAsync(sessionHandle, queryString, confOverlay);
    return waitForAsyncQuery(h, expectedState, longPollingTimeout);
  }

  private OperationStatus waitForAsyncQuery(OperationHandle opHandle,
      OperationState expectedState, long maxLongPollingTimeout) throws HiveSQLException {
    long testIterationTimeout = System.currentTimeMillis() + 100000;
    long longPollingStart;
    long longPollingEnd;
    long longPollingTimeDelta;
    OperationStatus opStatus = null;
    OperationState state = null;
    int count = 0;    
    long start = System.currentTimeMillis();
    while (true) {
      // Break if iteration times out
      if (System.currentTimeMillis() > testIterationTimeout) {
        System.out.println("Polling timed out");
        break;
      }
      longPollingStart = System.currentTimeMillis();
      System.out.println("Long polling starts at: " + longPollingStart);
      opStatus = client.getOperationStatus(opHandle, false);
      state = opStatus.getState();
      longPollingEnd = System.currentTimeMillis();
      System.out.println("Long polling ends at: " + longPollingEnd);

      System.out.println("Polling: " + opHandle + " count=" + (++count)
          + " state=" + state);

      if (state == OperationState.CANCELED ||
          state == OperationState.CLOSED ||
          state == OperationState.FINISHED ||
          state == OperationState.ERROR) {
        break;
      } else {
        // Verify that getOperationStatus returned only after the long polling timeout
        longPollingTimeDelta = longPollingEnd - longPollingStart;
        // Calculate the expected timeout based on the elapsed time between waiting start time and polling start time
        long elapsed = longPollingStart - start;
        long expectedTimeout = Math.min(maxLongPollingTimeout, (elapsed / TimeUnit.SECONDS.toMillis(10) + 1) * 500);
        // Scale down by a factor of 0.9 to account for approximate values
        assertTrue(longPollingTimeDelta - 0.9*expectedTimeout > 0);
      }
    }
    assertEquals(expectedState, client.getOperationStatus(opHandle, false).getState());
    client.closeOperation(opHandle);
    return opStatus;
  }

  /**
   * Test per statement configuration overlay.
   * Create a table using hiveconf: var substitution, with the conf var passed
   * via confOverlay.Verify the confOverlay works for the query and does set the
   * value in the session configuration
   * @throws Exception
   */
  @Test
  public void testConfOverlay() throws Exception {
    SessionHandle sessionHandle = client.openSession("tom", "password", new HashMap<String, String>());
    assertNotNull(sessionHandle);
    String tabName = "TEST_CONF_EXEC";
    String tabNameVar = "tabNameVar";

    String setLockMgr = "SET " + HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname
        + " = false";
    OperationHandle opHandle = client.executeStatement(sessionHandle, setLockMgr, null);
    client.closeOperation(opHandle);

    String dropTable = "DROP TABLE IF EXISTS " + tabName;
    opHandle = client.executeStatement(sessionHandle, dropTable, null);
    client.closeOperation(opHandle);

    // set a pass a property to operation and check if its set the query config
    Map <String, String> confOverlay = new HashMap<String, String>();
    confOverlay.put(tabNameVar, tabName);

    // execute statement with the conf overlay
    String createTab = "CREATE TABLE ${hiveconf:" + tabNameVar + "} (id int)";
    opHandle = client.executeStatement(sessionHandle, createTab, confOverlay);
    assertNotNull(opHandle);
    // query should pass and create the table
    assertEquals("Query should be finished",
        OperationState.FINISHED, client.getOperationStatus(opHandle, false).getState());
    client.closeOperation(opHandle);

    // select from  the new table should pass
    String selectTab = "SELECT * FROM " + tabName;
    opHandle = client.executeStatement(sessionHandle, selectTab, null);
    assertNotNull(opHandle);
    // query should pass and create the table
    assertEquals("Query should be finished",
        OperationState.FINISHED, client.getOperationStatus(opHandle, false).getState());
    client.closeOperation(opHandle);

    // the settings in conf overlay should not be part of session config
    // another query referring that property with the conf overlay should fail
    selectTab = "SELECT * FROM ${hiveconf:" + tabNameVar + "}";
    try {
      opHandle = client.executeStatement(sessionHandle, selectTab, null);
      fail("Query should fail");
    } catch (HiveSQLException e) {
      // Expected exception
    }

    // cleanup
    dropTable = "DROP TABLE IF EXISTS " + tabName;
    opHandle = client.executeStatement(sessionHandle, dropTable, null);
    client.closeOperation(opHandle);
    client.closeSession(sessionHandle);
  }

  @Test
  public void testTaskStatus() throws Exception {
    HashMap<String, String> confOverlay = new HashMap<String, String>();
    String tableName = "TEST_EXEC_ASYNC";
    String columnDefinitions = "(ID STRING)";

    // Open a session and set up the test data
    SessionHandle sessionHandle = setupTestData(tableName, columnDefinitions, confOverlay);
    assertNotNull(sessionHandle);
    // nonblocking execute
    String select = "select a.id, b.id from (SELECT ID + ' ' `ID` FROM TEST_EXEC_ASYNC) a full outer join "
      + "(SELECT ID + ' ' `ID` FROM TEST_EXEC_ASYNC) b on a.ID=b.ID";
    OperationHandle ophandle =
      client.executeStatementAsync(sessionHandle, select, confOverlay);

    OperationStatus status = null;
    int count = 0;
    while (true) {
      status = client.getOperationStatus(ophandle, false);
      checkOperationTimes(ophandle, status);
      OperationState state = status.getState();
      System.out.println("Polling: " + ophandle + " count=" + (++count)
        + " state=" + state);

      String jsonTaskStatus = status.getTaskStatus();
      assertNotNull(jsonTaskStatus);
      ObjectMapper mapper = new ObjectMapper();
      ByteArrayInputStream in = new ByteArrayInputStream(jsonTaskStatus.getBytes("UTF-8"));
      List<QueryDisplay.TaskDisplay> taskStatuses =
        mapper.readValue(in, new TypeReference<List<QueryDisplay.TaskDisplay>>(){});
      System.out.println("task statuses: " + jsonTaskStatus); // TaskDisplay doesn't have a toString, using json
      checkTaskStatuses(taskStatuses);
      if (OperationState.CANCELED == state || state == OperationState.CLOSED
        || state == OperationState.FINISHED
        || state == OperationState.ERROR) {
        for (QueryDisplay.TaskDisplay display: taskStatuses) {
          assertNotNull(display.getReturnValue());
        }
        break;
      }
      Thread.sleep(1000);
    }
  }

  private void checkTaskStatuses(List<QueryDisplay.TaskDisplay> taskDisplays) {
    assertNotNull(taskDisplays);
    for (QueryDisplay.TaskDisplay taskDisplay: taskDisplays) {
      switch (taskDisplay.taskState) {
        case INITIALIZED:
        case QUEUED:
          assertNull(taskDisplay.getExternalHandle());
          assertNull(taskDisplay.getBeginTime());
          assertNull(taskDisplay.getEndTime());
          assertNull(taskDisplay.getElapsedTime());
          assertNull(taskDisplay.getErrorMsg());
          assertNull(taskDisplay.getReturnValue());
          break;
        case RUNNING:
          assertNotNull(taskDisplay.getBeginTime());
          assertNull(taskDisplay.getEndTime());
          assertNotNull(taskDisplay.getElapsedTime());
          assertNull(taskDisplay.getErrorMsg());
          assertNull(taskDisplay.getReturnValue());
          break;
        case FINISHED:
          if (taskDisplay.getTaskType() == StageType.MAPRED || taskDisplay.getTaskType() == StageType.MAPREDLOCAL) {
            assertNotNull(taskDisplay.getExternalHandle());
            assertNotNull(taskDisplay.getStatusMessage());
          }
          assertNotNull(taskDisplay.getBeginTime());
          assertNotNull(taskDisplay.getEndTime());
          assertNotNull(taskDisplay.getElapsedTime());
          break;
        case UNKNOWN:
        default:
          fail("unknown task status: " + taskDisplay);
      }
    }
  }


  private void checkOperationTimes(OperationHandle operationHandle, OperationStatus status) {
    OperationState state = status.getState();
    assertFalse(status.getOperationStarted() ==  0);
    if (OperationState.CANCELED == state || state == OperationState.CLOSED
      || state == OperationState.FINISHED || state == OperationState.ERROR) {
      System.out.println("##OP " + operationHandle.getHandleIdentifier() + " STATE:" + status.getState()
        +" START:" + status.getOperationStarted()
        + " END:" + status.getOperationCompleted());
      assertFalse(status.getOperationCompleted() ==  0);
      assertTrue(status.getOperationCompleted() - status.getOperationStarted() >= 0);
    }
  }
}
