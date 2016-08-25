/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.FetchType;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests to verify operation logging layout for different modes.
 */
public class TestOperationLoggingLayout {
  protected static HiveConf hiveConf;
  protected static String tableName;
  private File dataFile;
  protected CLIServiceClient client;
  protected static MiniHS2 miniHS2 = null;
  protected static Map<String, String> confOverlay;
  protected SessionHandle sessionHandle;
  protected final String sql = "select * from " + tableName;
  private final String sqlCntStar = "select count(*) from " + tableName;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    tableName = "TestOperationLoggingLayout_table";
    hiveConf = new HiveConf();
    hiveConf.set(HiveConf.ConfVars.HIVE_SERVER2_LOGGING_OPERATION_LEVEL.varname, "execution");
    miniHS2 = new MiniHS2(hiveConf);
    confOverlay = new HashMap<String, String>();
    confOverlay.put(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY.varname, "false");
    miniHS2.start(confOverlay);
  }

  /**
   * Open a session, and create a table for cases usage
   *
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
  public void testSwitchLogLayout() throws Exception {
    // verify whether the sql operation log is generated and fetch correctly.
    OperationHandle operationHandle = client.executeStatement(sessionHandle, sqlCntStar, null);
    RowSet rowSetLog = client.fetchResults(operationHandle, FetchOrientation.FETCH_FIRST, 1000,
        FetchType.LOG);
    Iterator<Object[]> iter = rowSetLog.iterator();
    // non-verbose pattern is %-5p : %m%n. Look for " : "
    while (iter.hasNext()) {
      String row = iter.next()[0].toString();
      Assert.assertEquals(true, row.matches("^.*(FATAL|ERROR|WARN|INFO|DEBUG|TRACE).*$"));
    }

    String queryString = "set hive.server2.logging.operation.level=verbose";
    client.executeStatement(sessionHandle, queryString, null);
    operationHandle = client.executeStatement(sessionHandle, sqlCntStar, null);
    // just check for first few lines, some log lines are multi-line strings which can break format
    // checks below
    rowSetLog = client.fetchResults(operationHandle, FetchOrientation.FETCH_FIRST, 10,
        FetchType.LOG);
    iter = rowSetLog.iterator();
    // verbose pattern is "%d{yy/MM/dd HH:mm:ss} %p %c{2}: %m%n"
    while (iter.hasNext()) {
      String row = iter.next()[0].toString();
      // just check if the log line starts with date
      Assert.assertEquals(true,
          row.matches("^\\d{2}[/](0[1-9]|1[012])[/](0[1-9]|[12][0-9]|3[01]).*$"));
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
}
