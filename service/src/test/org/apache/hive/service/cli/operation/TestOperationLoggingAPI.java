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
package org.apache.hive.service.cli.operation;

import java.io.File;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.FetchType;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.hive.service.cli.thrift.EmbeddedThriftBinaryCLIService;
import org.apache.hive.service.cli.thrift.ThriftCLIServiceClient;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * TestOperationLoggingAPI
 * Test the FetchResults of TFetchType.LOG in thrift level.
 */
public class TestOperationLoggingAPI {
  private static HiveConf hiveConf;
  private final String tableName = "testOperationLoggingAPI_table";
  private File dataFile;
  private ThriftCLIServiceClient client;
  private SessionHandle sessionHandle;
  private final String sql = "select * from " + tableName;
  private final String[] expectedLogs = {
    "Parsing command",
    "Parse Completed",
    "Starting Semantic Analysis",
    "Semantic Analysis Completed",
    "Starting command"
  };

  @BeforeClass
  public static void setUpBeforeClass() {
    hiveConf = new HiveConf();
    hiveConf.setBoolean(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_VERBOSE.varname, true);
  }

  /**
   * Start embedded mode, open a session, and create a table for cases usage
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception {
    dataFile = new File(hiveConf.get("test.data.files"), "kv1.txt");
    EmbeddedThriftBinaryCLIService service = new EmbeddedThriftBinaryCLIService();
    service.init(hiveConf);
    client = new ThriftCLIServiceClient(service);
    sessionHandle = setupSession();
  }

  @After
  public void tearDown() throws Exception {
    // Cleanup
    String queryString = "DROP TABLE " + tableName;
    client.executeStatement(sessionHandle, queryString, null);

    client.closeSession(sessionHandle);
  }

  @Test
  public void testFetchResultsOfLog() throws Exception {
    // verify whether the sql operation log is generated and fetch correctly.
    OperationHandle operationHandle = client.executeStatement(sessionHandle, sql, null);
    RowSet rowSetLog = client.fetchResults(operationHandle, FetchOrientation.FETCH_FIRST, 1000,
        FetchType.LOG);
    verifyFetchedLog(rowSetLog);
  }

  @Test
  public void testFetchResultsOfLogAsync() throws Exception {
    // verify whether the sql operation log is generated and fetch correctly in async mode.
    OperationHandle operationHandle = client.executeStatementAsync(sessionHandle, sql, null);

    // Poll on the operation status till the query is completed
    boolean isQueryRunning = true;
    long pollTimeout = System.currentTimeMillis() + 100000;
    OperationStatus opStatus;
    OperationState state = null;
    RowSet rowSetAccumulated = null;
    StringBuilder logs = new StringBuilder();

    while (isQueryRunning) {
      // Break if polling times out
      if (System.currentTimeMillis() > pollTimeout) {
        break;
      }
      opStatus = client.getOperationStatus(operationHandle);
      Assert.assertNotNull(opStatus);
      state = opStatus.getState();

      rowSetAccumulated = client.fetchResults(operationHandle, FetchOrientation.FETCH_NEXT, 1000,
          FetchType.LOG);
      for (Object[] row : rowSetAccumulated) {
        logs.append(row[0]);
      }

      if (state == OperationState.CANCELED ||
          state == OperationState.CLOSED ||
          state == OperationState.FINISHED ||
          state == OperationState.ERROR) {
        isQueryRunning = false;
      }
      Thread.sleep(10);
    }
    // The sql should be completed now.
    Assert.assertEquals("Query should be finished",  OperationState.FINISHED, state);

    // Verify the accumulated logs
    verifyFetchedLog(logs.toString());

    // Verify the fetched logs from the beginning of the log file
    RowSet rowSet = client.fetchResults(operationHandle, FetchOrientation.FETCH_FIRST, 1000,
        FetchType.LOG);
    verifyFetchedLog(rowSet);
  }

  @Test
  public void testFetchResultsOfLogWithOrientation() throws Exception {
    // (FETCH_FIRST) execute a sql, and fetch its sql operation log as expected value
    OperationHandle operationHandle = client.executeStatement(sessionHandle, sql, null);
    RowSet rowSetLog = client.fetchResults(operationHandle, FetchOrientation.FETCH_FIRST, 1000,
        FetchType.LOG);
    int expectedLogLength = rowSetLog.numRows();

    // (FETCH_NEXT) execute the same sql again,
    // and fetch the sql operation log with FETCH_NEXT orientation
    OperationHandle operationHandleWithOrientation = client.executeStatement(sessionHandle, sql,
        null);
    RowSet rowSetLogWithOrientation;
    int logLength = 0;
    int maxRows = calculateProperMaxRows(expectedLogLength);
    do {
      rowSetLogWithOrientation = client.fetchResults(operationHandleWithOrientation,
          FetchOrientation.FETCH_NEXT, maxRows, FetchType.LOG);
      logLength += rowSetLogWithOrientation.numRows();
    } while (rowSetLogWithOrientation.numRows() == maxRows);
    Assert.assertEquals(expectedLogLength, logLength);

    // (FETCH_FIRST) fetch again from the same operation handle with FETCH_FIRST orientation
    rowSetLogWithOrientation = client.fetchResults(operationHandleWithOrientation,
        FetchOrientation.FETCH_FIRST, 1000, FetchType.LOG);
    verifyFetchedLog(rowSetLogWithOrientation);
  }

  @Test
  public void testFetchResultsOfLogCleanup() throws Exception {
    // Verify cleanup functionality.
    // Open a new session, since this case needs to close the session in the end.
    SessionHandle sessionHandleCleanup = setupSession();

    // prepare
    OperationHandle operationHandle = client.executeStatement(sessionHandleCleanup, sql, null);
    RowSet rowSetLog = client.fetchResults(operationHandle, FetchOrientation.FETCH_FIRST, 1000,
        FetchType.LOG);
    verifyFetchedLog(rowSetLog);

    File sessionLogDir = new File(
        hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LOG_LOCATION) +
            File.separator + sessionHandleCleanup.getHandleIdentifier());
    File operationLogFile = new File(sessionLogDir, operationHandle.getHandleIdentifier().toString());

    // check whether exception is thrown when fetching log from a closed operation.
    client.closeOperation(operationHandle);
    try {
      client.fetchResults(operationHandle, FetchOrientation.FETCH_FIRST, 1000, FetchType.LOG);
      Assert.fail("Fetch should fail");
    } catch (HiveSQLException e) {
      Assert.assertTrue(e.getMessage().contains("Invalid OperationHandle:"));
    }

    // check whether operation log file is deleted.
    if (operationLogFile.exists()) {
      Assert.fail("Operation log file should be deleted.");
    }

    // check whether session log dir is deleted after session is closed.
    client.closeSession(sessionHandleCleanup);
    if (sessionLogDir.exists()) {
      Assert.fail("Session log dir should be deleted.");
    }
  }

  private SessionHandle setupSession() throws Exception {
    // Open a session
    SessionHandle sessionHandle = client.openSession(null, null, null);

    // Change lock manager to embedded mode
    String queryString = "SET hive.lock.manager=" +
        "org.apache.hadoop.hive.ql.lockmgr.EmbeddedLockManager";
    client.executeStatement(sessionHandle, queryString, null);

    // Drop the table if it exists
    queryString = "DROP TABLE IF EXISTS " + tableName;
    client.executeStatement(sessionHandle, queryString, null);

    // Create a test table
    queryString = "create table " + tableName + " (key int, value string)";
    client.executeStatement(sessionHandle, queryString, null);

    // Load data
    queryString = "load data local inpath '" + dataFile + "' into table " + tableName;
    client.executeStatement(sessionHandle, queryString, null);

    // Precondition check: verify whether the table is created and data is fetched correctly.
    OperationHandle operationHandle = client.executeStatement(sessionHandle, sql, null);
    RowSet rowSetResult = client.fetchResults(operationHandle);
    Assert.assertEquals(500, rowSetResult.numRows());
    Assert.assertEquals(238, rowSetResult.iterator().next()[0]);
    Assert.assertEquals("val_238", rowSetResult.iterator().next()[1]);

    return sessionHandle;
  }

  // Since the log length of the sql operation may vary during HIVE dev, calculate a proper maxRows.
  private int calculateProperMaxRows(int len) {
    if (len < 10) {
      return 1;
    } else if (len < 100) {
      return 10;
    } else {
      return 100;
    }
  }

  private void verifyFetchedLog(RowSet rowSet) {
    StringBuilder stringBuilder = new StringBuilder();

    for (Object[] row : rowSet) {
      stringBuilder.append(row[0]);
    }

    String logs = stringBuilder.toString();
    verifyFetchedLog(logs);
  }

  private void verifyFetchedLog(String logs) {
    for (String log : expectedLogs) {
      Assert.assertTrue("Checking for presence of " + log, logs.contains(log));
    }
  }
}
