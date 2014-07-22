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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * CLIServiceTest.
 *
 */
public abstract class CLIServiceTest {

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
    queryString = "SELECT ID FROM TEST_EXEC";
    opHandle = client.executeStatement(sessionHandle, queryString, confOverlay);
    // Expect query to be completed now
    assertEquals("Query should be finished",
        OperationState.FINISHED, client.getOperationStatus(opHandle).getState());
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
    longPollingTimeout = new HiveConf().getLongVar(ConfVars.HIVE_SERVER2_LONG_POLLING_TIMEOUT);
    queryString = "SELECT NON_EXISTING_COLUMN FROM " + tableName;
    try {
      runQueryAsync(sessionHandle, queryString, confOverlay, OperationState.ERROR, longPollingTimeout);
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
    opStatus = runQueryAsync(sessionHandle, queryString, confOverlay, OperationState.ERROR, longPollingTimeout);
    // sqlState, errorCode should be set
    assertEquals(opStatus.getOperationException().getSQLState(), "08S01");
    assertEquals(opStatus.getOperationException().getErrorCode(), 1);
    /**
     * Execute an async query with default config
     */
    queryString = "SELECT ID FROM " + tableName;
    runQueryAsync(sessionHandle, queryString, confOverlay, OperationState.FINISHED, longPollingTimeout);

    /**
     * Execute an async query with long polling timeout set to 0
     */
    longPollingTimeout = 0;
    queryString = "SELECT ID FROM " + tableName;
    runQueryAsync(sessionHandle, queryString, confOverlay, OperationState.FINISHED, longPollingTimeout);

    /**
     * Execute an async query with long polling timeout set to 500 millis
     */
    longPollingTimeout = 500;
    queryString = "SELECT ID FROM " + tableName;
    runQueryAsync(sessionHandle, queryString, confOverlay, OperationState.FINISHED, longPollingTimeout);

    /**
     * Cancellation test
     */
    queryString = "SELECT ID FROM " + tableName;
    opHandle = client.executeStatementAsync(sessionHandle, queryString, confOverlay);
    System.out.println("Cancelling " + opHandle);
    client.cancelOperation(opHandle);
    state = client.getOperationStatus(opHandle).getState();
    System.out.println(opHandle + " after cancelling, state= " + state);
    assertEquals("Query should be cancelled", OperationState.CANCELED, state);

    // Cleanup
    queryString = "DROP TABLE " + tableName;
    client.executeStatement(sessionHandle, queryString, confOverlay);
    client.closeSession(sessionHandle);
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
    SessionHandle sessionHandle = client.openSession("tom", "password", confOverlay);
    assertNotNull(sessionHandle);

    String queryString = "SET " + HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname
        + " = false";
    client.executeStatement(sessionHandle, queryString, confOverlay);

    // Drop the table if it exists
    queryString = "DROP TABLE IF EXISTS " + tableName;
    client.executeStatement(sessionHandle, queryString, confOverlay);

    // Create a test table
    queryString = "CREATE TABLE " + tableName + columnDefinitions;
    client.executeStatement(sessionHandle, queryString, confOverlay);

    return sessionHandle;
  }

  private OperationStatus runQueryAsync(SessionHandle sessionHandle, String queryString,
      Map<String, String> confOverlay, OperationState expectedState,
      long longPollingTimeout) throws HiveSQLException {
    // Timeout for the iteration in case of asynchronous execute
    long testIterationTimeout = System.currentTimeMillis() + 100000;
    long longPollingStart;
    long longPollingEnd;
    long longPollingTimeDelta;
    OperationStatus opStatus = null;
    OperationState state = null;
    confOverlay.put(HiveConf.ConfVars.HIVE_SERVER2_LONG_POLLING_TIMEOUT.varname, String.valueOf(longPollingTimeout));
    OperationHandle opHandle = client.executeStatementAsync(sessionHandle, queryString, confOverlay);
    int count = 0;
    while (true) {
      // Break if iteration times out
      if (System.currentTimeMillis() > testIterationTimeout) {
        System.out.println("Polling timed out");
        break;
      }
      longPollingStart = System.currentTimeMillis();
      System.out.println("Long polling starts at: " + longPollingStart);
      opStatus = client.getOperationStatus(opHandle);
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
        // Scale down by a factor of 0.9 to account for approximate values
        assertTrue(longPollingTimeDelta - 0.9*longPollingTimeout > 0);
      }
    }
    assertEquals(expectedState, client.getOperationStatus(opHandle).getState());
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
        OperationState.FINISHED, client.getOperationStatus(opHandle).getState());
    client.closeOperation(opHandle);

    // select from  the new table should pass
    String selectTab = "SELECT * FROM " + tabName;
    opHandle = client.executeStatement(sessionHandle, selectTab, null);
    assertNotNull(opHandle);
    // query should pass and create the table
    assertEquals("Query should be finished",
        OperationState.FINISHED, client.getOperationStatus(opHandle).getState());
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
}
