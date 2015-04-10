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
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.FetchType;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.junit.After;
import org.junit.AfterClass;
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
  private CLIServiceClient client;
  private static MiniHS2 miniHS2 = null;
  private static Map<String, String> confOverlay;
  private SessionHandle sessionHandle;
  private final String sql = "select * from " + tableName;
  private final String sqlCntStar = "select count(*) from " + tableName;
  private final String[] expectedLogs = {
    "Parsing command",
    "Parse Completed",
    "Starting Semantic Analysis",
    "Semantic Analysis Completed",
    "Starting command"
  };
  private final String[] expectedLogsExecution = {
    "Number of reduce tasks determined at compile time",
    "number of splits",
    "Submitting tokens for job",
    "Ended Job"
  };
  private final String[] expectedLogsPerformance = {
    "<PERFLOG method=compile from=org.apache.hadoop.hive.ql.Driver>",
    "<PERFLOG method=parse from=org.apache.hadoop.hive.ql.Driver>",
    "<PERFLOG method=Driver.run from=org.apache.hadoop.hive.ql.Driver>",
    "<PERFLOG method=runTasks from=org.apache.hadoop.hive.ql.Driver>"
  };

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    hiveConf = new HiveConf();
    hiveConf.set(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LEVEL.varname, "verbose");
    // We need to set the below parameter to test performance level logging
    hiveConf.set("hive.ql.log.PerfLogger.level", "INFO,DRFA");
    miniHS2 = new MiniHS2(hiveConf);
    confOverlay = new HashMap<String, String>();
    confOverlay.put(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    miniHS2.start(confOverlay);
  }

  /**
   * Open a session, and create a table for cases usage
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception {
    dataFile = new File(hiveConf.get("test.data.files"), "kv1.txt");
    client = miniHS2.getServiceClient();
    sessionHandle = setupSession();
  }

  @After
  public void tearDown() throws Exception {
    // Cleanup
    String queryString = "DROP TABLE " + tableName;
    client.executeStatement(sessionHandle, queryString, null);

    client.closeSession(sessionHandle);
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    miniHS2.stop();
  }

  @Test
  public void testFetchResultsOfLog() throws Exception {
    // verify whether the sql operation log is generated and fetch correctly.
    OperationHandle operationHandle = client.executeStatement(sessionHandle, sql, null);
    RowSet rowSetLog = client.fetchResults(operationHandle, FetchOrientation.FETCH_FIRST, 1000,
        FetchType.LOG);
    verifyFetchedLog(rowSetLog, expectedLogs);
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

      rowSetAccumulated = client.fetchResults(operationHandle, FetchOrientation.FETCH_NEXT, 2000,
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
    verifyFetchedLogPost(logs.toString(), expectedLogs, true);

    // Verify the fetched logs from the beginning of the log file
    RowSet rowSet = client.fetchResults(operationHandle, FetchOrientation.FETCH_FIRST, 2000,
        FetchType.LOG);
    verifyFetchedLog(rowSet, expectedLogs);
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
    verifyFetchedLog(rowSetLogWithOrientation,  expectedLogs);
  }

  @Test
  public void testFetchResultsOfLogWithVerboseMode() throws Exception {
    String queryString = "set hive.server2.logging.operation.level=verbose";
    client.executeStatement(sessionHandle, queryString, null);
    // verify whether the sql operation log is generated and fetch correctly.
    OperationHandle operationHandle = client.executeStatement(sessionHandle, sqlCntStar, null);
    RowSet rowSetLog = client.fetchResults(operationHandle, FetchOrientation.FETCH_FIRST, 1000,
      FetchType.LOG);
    // Verbose Logs should contain everything, including execution and performance
    verifyFetchedLog(rowSetLog, expectedLogs);
    verifyFetchedLog(rowSetLog, expectedLogsExecution);
    verifyFetchedLog(rowSetLog, expectedLogsPerformance);
  }

  @Test
  public void testFetchResultsOfLogWithPerformanceMode() throws Exception {
    try {
      String queryString = "set hive.server2.logging.operation.level=performance";
      client.executeStatement(sessionHandle, queryString, null);
      // verify whether the sql operation log is generated and fetch correctly.
      OperationHandle operationHandle = client.executeStatement(sessionHandle, sqlCntStar, null);
      RowSet rowSetLog = client.fetchResults(operationHandle, FetchOrientation.FETCH_FIRST, 1000,
        FetchType.LOG);
      // rowSetLog should contain execution as well as performance logs
      verifyFetchedLog(rowSetLog, expectedLogsExecution);
      verifyFetchedLog(rowSetLog, expectedLogsPerformance);
      verifyMissingContentsInFetchedLog(rowSetLog, expectedLogs);
    } finally {
      // Restore everything to default setup to avoid discrepancy between junit test runs
      String queryString2 = "set hive.server2.logging.operation.level=verbose";
      client.executeStatement(sessionHandle, queryString2, null);
    }
  }

  @Test
  public void testFetchResultsOfLogWithExecutionMode() throws Exception {
    try {
      String queryString = "set hive.server2.logging.operation.level=execution";
      client.executeStatement(sessionHandle, queryString, null);
      // verify whether the sql operation log is generated and fetch correctly.
      OperationHandle operationHandle = client.executeStatement(sessionHandle, sqlCntStar, null);
      RowSet rowSetLog = client.fetchResults(operationHandle, FetchOrientation.FETCH_FIRST, 1000,
        FetchType.LOG);
      verifyFetchedLog(rowSetLog, expectedLogsExecution);
      verifyMissingContentsInFetchedLog(rowSetLog, expectedLogsPerformance);
      verifyMissingContentsInFetchedLog(rowSetLog, expectedLogs);
    } finally {
      // Restore everything to default setup to avoid discrepancy between junit test runs
      String queryString2 = "set hive.server2.logging.operation.level=verbose";
      client.executeStatement(sessionHandle, queryString2, null);
    }
  }

  @Test
  public void testFetchResultsOfLogWithNoneMode() throws Exception {
    try {
      String queryString = "set hive.server2.logging.operation.level=none";
      client.executeStatement(sessionHandle, queryString, null);
      // verify whether the sql operation log is generated and fetch correctly.
      OperationHandle operationHandle = client.executeStatement(sessionHandle, sqlCntStar, null);
      RowSet rowSetLog = client.fetchResults(operationHandle, FetchOrientation.FETCH_FIRST, 1000,
        FetchType.LOG);
      // We should not get any rows.
      assert(rowSetLog.numRows() == 0);
    } finally {
      // Restore everything to default setup to avoid discrepancy between junit test runs
      String queryString2 = "set hive.server2.logging.operation.level=verbose";
      client.executeStatement(sessionHandle, queryString2, null);
    }
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
    verifyFetchedLog(rowSetLog, expectedLogs);

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

  private String verifyFetchedLogPre(RowSet rowSet, String[] el) {
    StringBuilder stringBuilder = new StringBuilder();

    for (Object[] row : rowSet) {
      stringBuilder.append(row[0]);
    }

    return stringBuilder.toString();
  }

  private void verifyFetchedLog(RowSet rowSet, String[] el) {
	    String logs = verifyFetchedLogPre(rowSet, el);
	    verifyFetchedLogPost(logs, el, true);
  }

  private void verifyMissingContentsInFetchedLog(RowSet rowSet, String[] el) {
    String logs = verifyFetchedLogPre(rowSet, el);
    verifyFetchedLogPost(logs, el, false);
  }

  private void verifyFetchedLogPost(String logs, String[] el, boolean contains) {
    for (String log : el) {
      if (contains) {
        Assert.assertTrue("Checking for presence of " + log, logs.contains(log));
      } else {
        Assert.assertFalse("Checking for absence of " + log, logs.contains(log));
      }
    }
  }
}
