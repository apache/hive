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
import static org.junit.Assert.fail;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
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
    SessionHandle sessionHandle = client
        .openSession("tom", "password", Collections.<String, String>emptyMap());
    assertNotNull(sessionHandle);
    client.closeSession(sessionHandle);

    sessionHandle = client.openSession("tom", "password");
    assertNotNull(sessionHandle);
    client.closeSession(sessionHandle);
  }

  @Test
  public void getFunctionsTest() throws Exception {
    SessionHandle sessionHandle = client.openSession("tom", "password", new HashMap<String, String>());
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

    client.closeOperation(opHandle);
    client.closeSession(sessionHandle);
  }

  @Test
  public void getInfoTest() throws Exception {
    SessionHandle sessionHandle = client.openSession("tom", "password", new HashMap<String, String>());
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
    SessionHandle sessionHandle = client.openSession("tom", "password",
        new HashMap<String, String>());
    assertNotNull(sessionHandle);

    // Change lock manager, otherwise unit-test doesn't go through
    String queryString = "SET hive.lock.manager=" +
        "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager";
    client.executeStatement(sessionHandle, queryString, confOverlay);

    // Drop the table if it exists
    queryString = "DROP TABLE IF EXISTS TEST_EXEC";
    client.executeStatement(sessionHandle, queryString, confOverlay);

    // Create a test table
    queryString = "CREATE TABLE TEST_EXEC(ID STRING)";
    client.executeStatement(sessionHandle, queryString, confOverlay);

    // Blocking execute
    queryString = "SELECT ID FROM TEST_EXEC";
    OperationHandle ophandle = client.executeStatement(sessionHandle, queryString, confOverlay);

    // Expect query to be completed now
    assertEquals("Query should be finished",
        OperationState.FINISHED, client.getOperationStatus(ophandle));
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
    OperationHandle ophandle;

    // Change lock manager, otherwise unit-test doesn't go through
    String queryString = "SET hive.lock.manager=" +
        "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager";
    client.executeStatement(sessionHandle, queryString, confOverlay);

    // Drop the table if it exists
    queryString = "DROP TABLE IF EXISTS TEST_EXEC_ASYNC";
    client.executeStatement(sessionHandle, queryString, confOverlay);

    // Create a test table
    queryString = "CREATE TABLE TEST_EXEC_ASYNC(ID STRING)";
    client.executeStatement(sessionHandle, queryString, confOverlay);

    // Test async execution response when query is malformed
    String wrongQueryString = "SELECT NAME FROM TEST_EXEC";
    ophandle = client.executeStatementAsync(sessionHandle, wrongQueryString, confOverlay);

    int count = 0;
    while (true) {
      // Break if polling times out
      if (System.currentTimeMillis() > pollTimeout) {
        System.out.println("Polling timed out");
        break;
      }
      state = client.getOperationStatus(ophandle);
      System.out.println("Polling: " + ophandle + " count=" + (++count)
          + " state=" + state);

      if (OperationState.CANCELED == state || state == OperationState.CLOSED
          || state == OperationState.FINISHED || state == OperationState.ERROR) {
        break;
      }
      Thread.sleep(1000);
    }
    assertEquals("Query should return an error state",
        OperationState.ERROR, client.getOperationStatus(ophandle));

    // Test async execution when query is well formed
    queryString = "SELECT ID FROM TEST_EXEC_ASYNC";
    ophandle =
        client.executeStatementAsync(sessionHandle, queryString, confOverlay);

    count = 0;
    while (true) {
      // Break if polling times out
      if (System.currentTimeMillis() > pollTimeout) {
        System.out.println("Polling timed out");
        break;
      }
      state = client.getOperationStatus(ophandle);
      System.out.println("Polling: " + ophandle + " count=" + (++count)
          + " state=" + state);

      if (OperationState.CANCELED == state || state == OperationState.CLOSED
          || state == OperationState.FINISHED || state == OperationState.ERROR) {
        break;
      }
      Thread.sleep(1000);
    }
    assertEquals("Query should be finished",
        OperationState.FINISHED, client.getOperationStatus(ophandle));

    // Cancellation test
    ophandle = client.executeStatementAsync(sessionHandle, queryString, confOverlay);
    System.out.println("cancelling " + ophandle);
    client.cancelOperation(ophandle);
    state = client.getOperationStatus(ophandle);
    System.out.println(ophandle + " after cancelling, state= " + state);
    assertEquals("Query should be cancelled", OperationState.CANCELED, state);
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
        OperationState.FINISHED, client.getOperationStatus(opHandle));
    client.closeOperation(opHandle);

    // select from  the new table should pass
    String selectTab = "SELECT * FROM " + tabName;
    opHandle = client.executeStatement(sessionHandle, selectTab, null);
    assertNotNull(opHandle);
    // query should pass and create the table
    assertEquals("Query should be finished",
        OperationState.FINISHED, client.getOperationStatus(opHandle));
    client.closeOperation(opHandle);

    // the settings in confoverly should not be part of session config
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
