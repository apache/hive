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
package org.apache.hive.service.cli.operation;

import java.util.HashMap;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.FetchType;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.OperationState;
import org.apache.hive.service.cli.OperationStatus;
import org.apache.hive.service.cli.RowSet;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * TestOperationLoggingAPIWithMr
 * Test the FetchResults of TFetchType.LOG in thrift level in MR mode.
 */
public class TestOperationLoggingAPIWithMr extends OperationLoggingAPITestBase {

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    tableName  = "testOperationLoggingAPIWithMr_table";
    expectedLogsVerbose = new String[]{
      "Starting Semantic Analysis"
    };
    expectedLogsExecution = new String[]{
      "Compiling command",
      "Completed compiling command",
      "Total jobs",
      "Executing command",
      "Completed executing command",
      "Semantic Analysis Completed",
      "Number of reduce tasks determined at compile time",
      "number of splits",
      "Submitting tokens for job",
      "Ended Job"
    };
    expectedLogsPerformance = new String[]{
      "<PERFLOG method=compile from=org.apache.hadoop.hive.ql.Driver>",
      "<PERFLOG method=parse from=org.apache.hadoop.hive.ql.Driver>",
      "<PERFLOG method=runTasks from=org.apache.hadoop.hive.ql.Driver>"
    };
    hiveConf = new HiveConf();
    hiveConf.set(ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LEVEL.varname, "verbose");
    // this test is mr specific
    hiveConf.set(ConfVars.HIVE_EXECUTION_ENGINE.varname, "mr");
    miniHS2 = new MiniHS2(hiveConf);
    confOverlay = new HashMap<String, String>();
    confOverlay.put(ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    miniHS2.start(confOverlay);
  }

  @Test
  public void testFetchResultsOfLog() throws Exception {
    // verify whether the sql operation log is generated and fetch correctly.
    OperationHandle operationHandle = client.executeStatement(sessionHandle, sql, null);
    RowSet rowSetLog = client.fetchResults(operationHandle, FetchOrientation.FETCH_FIRST, 1000,
        FetchType.LOG);
    verifyFetchedLog(rowSetLog, expectedLogsVerbose);
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
      opStatus = client.getOperationStatus(operationHandle, false);
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
    verifyFetchedLogPost(logs.toString(), expectedLogsVerbose, true);

    // Verify the fetched logs from the beginning of the log file
    RowSet rowSet = client.fetchResults(operationHandle, FetchOrientation.FETCH_FIRST, 2000,
        FetchType.LOG);
    verifyFetchedLog(rowSet, expectedLogsVerbose);
  }

  @Test
  @Ignore("HIVE-27966")
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
    verifyFetchedLog(rowSetLogWithOrientation,  expectedLogsVerbose);
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
}
