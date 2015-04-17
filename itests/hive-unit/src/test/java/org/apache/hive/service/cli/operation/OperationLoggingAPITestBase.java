package org.apache.hive.service.cli.operation;

import java.io.File;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.FetchType;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * OperationLoggingAPITestBase
 * Test the FetchResults of TFetchType.LOG in thrift level.
 * This is the base class.
 */
@Ignore
public abstract class OperationLoggingAPITestBase {
  protected static HiveConf hiveConf;
  protected static String tableName;
  private File dataFile;
  protected CLIServiceClient client;
  protected static MiniHS2 miniHS2 = null;
  protected static Map<String, String> confOverlay;
  protected SessionHandle sessionHandle;
  protected final String sql = "select * from " + tableName;
  private final String sqlCntStar = "select count(*) from " + tableName;
  protected static String[] expectedLogsVerbose;
  protected static String[] expectedLogsExecution;
  protected static String[] expectedLogsPerformance;

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
  public void testFetchResultsOfLogWithVerboseMode() throws Exception {
    String queryString = "set hive.server2.logging.operation.level=verbose";
    client.executeStatement(sessionHandle, queryString, null);
    // verify whether the sql operation log is generated and fetch correctly.
    OperationHandle operationHandle = client.executeStatement(sessionHandle, sqlCntStar, null);
    RowSet rowSetLog = client.fetchResults(operationHandle, FetchOrientation.FETCH_FIRST, 1000,
      FetchType.LOG);
    // Verbose Logs should contain everything, including execution and performance
    verifyFetchedLog(rowSetLog, expectedLogsVerbose);
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
      verifyMissingContentsInFetchedLog(rowSetLog, expectedLogsVerbose);
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
      verifyMissingContentsInFetchedLog(rowSetLog, expectedLogsVerbose);
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
    verifyFetchedLog(rowSetLog, expectedLogsVerbose);

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

  private String verifyFetchedLogPre(RowSet rowSet, String[] el) {
    StringBuilder stringBuilder = new StringBuilder();

    for (Object[] row : rowSet) {
      stringBuilder.append(row[0]);
    }

    return stringBuilder.toString();
  }

  protected void verifyFetchedLog(RowSet rowSet, String[] el) {
	    String logs = verifyFetchedLogPre(rowSet, el);
	    verifyFetchedLogPost(logs, el, true);
  }

  private void verifyMissingContentsInFetchedLog(RowSet rowSet, String[] el) {
    String logs = verifyFetchedLogPre(rowSet, el);
    verifyFetchedLogPost(logs, el, false);
  }

  protected void verifyFetchedLogPost(String logs, String[] el, boolean contains) {
    for (String log : el) {
      if (contains) {
        Assert.assertTrue("Checking for presence of " + log, logs.contains(log));
      } else {
        Assert.assertFalse("Checking for absence of " + log, logs.contains(log));
      }
    }
  }
}
