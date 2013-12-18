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
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
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
  public void openSessionTest() throws Exception {
    SessionHandle sessionHandle = client.openSession(
        "tom", "password", Collections.<String, String>emptyMap());
    assertNotNull(sessionHandle);
    client.closeSession(sessionHandle);

    sessionHandle = client.openSession("tom", "password");
    assertNotNull(sessionHandle);
    client.closeSession(sessionHandle);
  }

  @Test
  public void getFunctionsTest() throws Exception {
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
  public void getInfoTest() throws Exception {
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

  @Test
  public void testExecuteStatementAsync() throws Exception {
    HashMap<String, String> confOverlay = new HashMap<String, String>();
    SessionHandle sessionHandle = client.openSession("tom", "password",
        new HashMap<String, String>());
    // Timeout for the poll in case of asynchronous execute
    long pollTimeout = System.currentTimeMillis() + 100000;
    assertNotNull(sessionHandle);
    OperationState state = null;
    OperationHandle opHandle;
    OperationStatus opStatus = null;

    // Change lock manager, otherwise unit-test doesn't go through
    String queryString = "SET " + HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname
        + " = false";
    opHandle = client.executeStatement(sessionHandle, queryString, confOverlay);
    client.closeOperation(opHandle);

    // Drop the table if it exists
    queryString = "DROP TABLE IF EXISTS TEST_EXEC_ASYNC";
    opHandle = client.executeStatement(sessionHandle, queryString, confOverlay);
    client.closeOperation(opHandle);

    // Create a test table
    queryString = "CREATE TABLE TEST_EXEC_ASYNC(ID STRING)";
    opHandle = client.executeStatement(sessionHandle, queryString, confOverlay);
    client.closeOperation(opHandle);

    // Test async execution response when query is malformed
    // Compile time error
    // This query will error out during compilation (which is done synchronous as of now)
    String wrongQueryString = "SELECT NON_EXISTANT_COLUMN FROM TEST_EXEC_ASYNC";
    try {
      opHandle = client.executeStatementAsync(sessionHandle, wrongQueryString, confOverlay);
      fail("Async syntax excution should fail");
    } catch (HiveSQLException e) {
      // expected error
    }
    

    // Runtime error
    wrongQueryString = "CREATE TABLE NON_EXISTING_TAB (ID STRING) location 'hdfs://fooNN:10000/a/b/c'";
    opHandle = client.executeStatementAsync(sessionHandle, wrongQueryString, confOverlay);

    int count = 0;
    while (true) {
      // Break if polling times out
      if (System.currentTimeMillis() > pollTimeout) {
        System.out.println("Polling timed out");
        break;
      }
      opStatus = client.getOperationStatus(opHandle);
      state = opStatus.getState();
      System.out.println("Polling: " + opHandle + " count=" + (++count)
          + " state=" + state);

      if (state == OperationState.CANCELED || state == OperationState.CLOSED
          || state == OperationState.FINISHED || state == OperationState.ERROR) {
        break;
      }
      Thread.sleep(1000);
    }
    assertEquals("Operation should be in error state", OperationState.ERROR, state);
    // sqlState, errorCode should be set
    assertEquals(opStatus.getOperationException().getSQLState(), "08S01");
    assertEquals(opStatus.getOperationException().getErrorCode(), 1);
    client.closeOperation(opHandle);
    
    // Test async execution when query is well formed
    queryString = "SELECT ID FROM TEST_EXEC_ASYNC";
    opHandle = client.executeStatementAsync(sessionHandle, queryString, confOverlay);
    assertTrue(opHandle.hasResultSet());
    
    count = 0;
    while (true) {
      // Break if polling times out
      if (System.currentTimeMillis() > pollTimeout) {
        System.out.println("Polling timed out");
        break;
      }
      opStatus = client.getOperationStatus(opHandle);
      state = opStatus.getState();
      System.out.println("Polling: " + opHandle + " count=" + (++count)
          + " state=" + state);

      if (state == OperationState.CANCELED || state == OperationState.CLOSED
          || state == OperationState.FINISHED || state == OperationState.ERROR) {
        break;
      }
      Thread.sleep(1000);
    }
    assertEquals("Query should be finished", OperationState.FINISHED, state);
    client.closeOperation(opHandle);

    // Cancellation test
    opHandle = client.executeStatementAsync(sessionHandle, queryString, confOverlay);
    System.out.println("cancelling " + opHandle);
    client.cancelOperation(opHandle);
    state = client.getOperationStatus(opHandle).getState();
    System.out.println(opHandle + " after cancelling, state= " + state);
    assertEquals("Query should be cancelled", OperationState.CANCELED, state);

    // Cleanup
    queryString = "DROP TABLE IF EXISTS TEST_EXEC_ASYNC";
    opHandle = client.executeStatement(sessionHandle, queryString, confOverlay);
    client.closeOperation(opHandle);
    client.closeSession(sessionHandle);
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
