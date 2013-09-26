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

import java.util.Collections;
import java.util.HashMap;

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
    String setLockMgr = "SET hive.lock.manager=" +
        "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager";
    client.executeStatement(sessionHandle, setLockMgr, confOverlay);

    String createTable = "CREATE TABLE TEST_EXEC(ID STRING)";
    client.executeStatement(sessionHandle, createTable, confOverlay);

    // blocking execute
    String select = "SELECT ID FROM TEST_EXEC";
    OperationHandle ophandle = client.executeStatement(sessionHandle, select, confOverlay);

    // expect query to be completed now
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
    String setLockMgr = "SET hive.lock.manager=" +
        "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager";
    client.executeStatement(sessionHandle, setLockMgr, confOverlay);

    String createTable = "CREATE TABLE TEST_EXEC_ASYNC(ID STRING)";
    client.executeStatementAsync(sessionHandle, createTable, confOverlay);

    // Test async execution response when query is malformed
    String wrongQuery = "SELECT NAME FROM TEST_EXEC";
    ophandle = client.executeStatementAsync(sessionHandle, wrongQuery, confOverlay);

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
    String select = "SELECT ID FROM TEST_EXEC_ASYNC";
    ophandle =
        client.executeStatementAsync(sessionHandle, select, confOverlay);

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
    ophandle = client.executeStatementAsync(sessionHandle, select, confOverlay);
    System.out.println("cancelling " + ophandle);
    client.cancelOperation(ophandle);
    state = client.getOperationStatus(ophandle);
    System.out.println(ophandle + " after cancelling, state= " + state);
    assertEquals("Query should be cancelled", OperationState.CANCELED, state);
  }
}
