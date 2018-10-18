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
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.log.HushableRandomAccessFileAppender;
import org.apache.hadoop.hive.ql.log.LogDivertAppender;
import org.apache.hadoop.hive.ql.log.LogDivertAppenderForTest;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.FetchOrientation;
import org.apache.hive.service.cli.FetchType;
import org.apache.hive.service.cli.OperationHandle;
import org.apache.hive.service.cli.RowSet;
import org.apache.hive.service.cli.SessionHandle;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.AbstractLogEvent;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.routing.RoutingAppender;
import org.apache.logging.log4j.core.config.AppenderControl;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
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

  private String getQueryId(RowSet rowSetLog) {
    Iterator<Object[]> iter = rowSetLog.iterator();
    // non-verbose pattern is %-5p : %m%n. Look for " : "
    while (iter.hasNext()) {
      String row = iter.next()[0].toString();
      Assert.assertEquals(true, row.matches("^.*(FATAL|ERROR|WARN|INFO|DEBUG|TRACE).*$"));

      // look for a row like "INFO  : Query ID = asherman_20170718154720_17c7d18b-36e6-4b35-a8e2-f50847db58ae"
      String queryIdLoggingProbe = "INFO  : Query ID = ";
      int index = row.indexOf(queryIdLoggingProbe);
      if (index >= 0) {
        return row.substring(queryIdLoggingProbe.length()).trim();
      }
    }
    return null;
  }

  private void appendHushableRandomAccessFileAppender(Appender queryAppender) {
    HushableRandomAccessFileAppender hushableAppender;

    if((queryAppender!= null) && (queryAppender instanceof HushableRandomAccessFileAppender)) {
      hushableAppender = (HushableRandomAccessFileAppender) queryAppender;
      try {
        hushableAppender.append(new LocalLogEvent());
      } catch (Exception e) {
        Assert.fail("Exception is not expected while appending HushableRandomAccessFileAppender");
      }
    }
  }

  @Test
  public void testSwitchLogLayout() throws Exception {
    // verify whether the sql operation log is generated and fetch correctly.
    OperationHandle operationHandle = client.executeStatement(sessionHandle, sqlCntStar, null);
    RowSet rowSetLog = client.fetchResults(operationHandle, FetchOrientation.FETCH_FIRST, 1000,
        FetchType.LOG);
    String queryId = getQueryId(rowSetLog);
    Assert.assertNotNull("Could not find query id, perhaps a logging message changed", queryId);

    checkAppenderState("before operation close ", LogDivertAppender.QUERY_ROUTING_APPENDER, queryId, false);
    checkAppenderState("before operation close ", LogDivertAppenderForTest.TEST_QUERY_ROUTING_APPENDER, queryId, false);
    client.closeOperation(operationHandle);
    checkAppenderState("after operation close ", LogDivertAppender.QUERY_ROUTING_APPENDER, queryId, true);
    checkAppenderState("after operation close ", LogDivertAppenderForTest.TEST_QUERY_ROUTING_APPENDER, queryId, true);
  }

  @Test
  /**
   * Test to make sure that appending log event to HushableRandomAccessFileAppender even after
   * closing the corresponding operation would not throw an exception.
   */
  public void testHushableRandomAccessFileAppender() throws Exception {
    // verify whether the sql operation log is generated and fetch correctly.
    OperationHandle operationHandle = client.executeStatement(sessionHandle, sqlCntStar, null);
    RowSet rowSetLog = client.fetchResults(operationHandle, FetchOrientation.FETCH_FIRST, 1000,
            FetchType.LOG);
    Appender queryAppender;
    Appender testQueryAppender;
    String queryId = getQueryId(rowSetLog);

    Assert.assertNotNull("Could not find query id, perhaps a logging message changed", queryId);

    checkAppenderState("before operation close ", LogDivertAppender.QUERY_ROUTING_APPENDER, queryId, false);
    queryAppender = getAppender(LogDivertAppender.QUERY_ROUTING_APPENDER, queryId);
    checkAppenderState("before operation close ", LogDivertAppenderForTest.TEST_QUERY_ROUTING_APPENDER, queryId, false);
    testQueryAppender = getAppender(LogDivertAppenderForTest.TEST_QUERY_ROUTING_APPENDER, queryId);

    client.closeOperation(operationHandle);
    appendHushableRandomAccessFileAppender(queryAppender);
    appendHushableRandomAccessFileAppender(testQueryAppender);
  }
  /**
   * assert that the appender for the given queryId is in the expected state.
   * @param msg a diagnostic
   * @param routingAppenderName name of the RoutingAppender
   * @param queryId the query id to use as a key
   * @param expectedStopped the expected stop state
   */
  private void checkAppenderState(String msg, String routingAppenderName, String queryId,
      boolean expectedStopped) throws NoSuchFieldException, IllegalAccessException {
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration configuration = context.getConfiguration();
    LoggerConfig loggerConfig = configuration.getRootLogger();
    Map<String, Appender> appendersMap = loggerConfig.getAppenders();
    RoutingAppender routingAppender = (RoutingAppender) appendersMap.get(routingAppenderName);
    Assert.assertNotNull(msg + "could not find routingAppender " + routingAppenderName, routingAppender);
    Field defaultsField = RoutingAppender.class.getDeclaredField("appenders");
    defaultsField.setAccessible(true);
    ConcurrentHashMap appenders = (ConcurrentHashMap) defaultsField.get(routingAppender);
    AppenderControl appenderControl = (AppenderControl) appenders.get(queryId);
    if (!expectedStopped) {
      Assert.assertNotNull(msg + "Could not find AppenderControl for query id " + queryId, appenderControl);
      Appender appender = appenderControl.getAppender();
      Assert.assertNotNull(msg + "could not find Appender for query id " + queryId + " from AppenderControl " +
              appenderControl, appender);
      Assert.assertEquals(msg + "Appender for query is in unexpected state", expectedStopped, appender.isStopped());
    } else {
      Assert.assertNull(msg + "AppenderControl for query id is not removed" + queryId, appenderControl);
    }
  }

  /**
   * Get the appender associated with a query.
   * @param routingAppenderName Routing appender name
   * @param queryId Query Id for the operation
   * @return Appender if found, else null
   * @throws NoSuchFieldException
   * @throws IllegalAccessException
   */
  private Appender getAppender(String routingAppenderName, String queryId)
          throws NoSuchFieldException, IllegalAccessException {
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration configuration = context.getConfiguration();
    LoggerConfig loggerConfig = configuration.getRootLogger();
    Map<String, Appender> appendersMap = loggerConfig.getAppenders();
    RoutingAppender routingAppender = (RoutingAppender) appendersMap.get(routingAppenderName);
    Assert.assertNotNull("could not find routingAppender " + routingAppenderName, routingAppender);
    Field defaultsField = RoutingAppender.class.getDeclaredField("appenders");
    defaultsField.setAccessible(true);
    ConcurrentHashMap appenders = (ConcurrentHashMap) defaultsField.get(routingAppender);
    AppenderControl appenderControl = (AppenderControl) appenders.get(queryId);
    if(appenderControl != null) {
      return appenderControl.getAppender();
    } else {
      return null;
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

  /**
   * A minimal LogEvent implementation for testing
   */
  private static class LocalLogEvent extends AbstractLogEvent {

    LocalLogEvent() {
    }

    @Override public Level getLevel() {
      return Level.DEBUG;
    }
  }
}
