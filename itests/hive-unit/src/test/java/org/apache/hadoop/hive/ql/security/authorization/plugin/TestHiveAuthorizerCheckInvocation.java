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

package org.apache.hadoop.hive.ql.security.authorization.plugin;

import static org.apache.hadoop.hive.common.repl.ReplConst.SOURCE_OF_REPLICATION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.Registry;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.hive.service.cli.operation.OperationManager;
import org.apache.hive.service.server.KillQueryImpl;
import org.apache.hive.service.server.KillQueryZookeeperManager;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test HiveAuthorizer api invocation
 */
public class TestHiveAuthorizerCheckInvocation {
  private final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());;
  protected static HiveConf conf;
  protected static Driver driver;
  protected static SessionState ss;
  private static final String tableName = TestHiveAuthorizerCheckInvocation.class.getSimpleName()
      + "Table";
  private static final String viewName = TestHiveAuthorizerCheckInvocation.class.getSimpleName()
      + "View";
  private static final String inDbTableName = tableName + "_in_db";
  private static final String acidTableName = tableName + "_acid";
  private static final String dbName = TestHiveAuthorizerCheckInvocation.class.getSimpleName()
      + "Db";
  private static final String fullInTableName = StatsUtils.getFullyQualifiedTableName(dbName, inDbTableName);
  static HiveAuthorizer mockedAuthorizer;

  /**
   * This factory creates a mocked HiveAuthorizer class. Use the mocked class to
   * capture the argument passed to it in the test case.
   */
  static class MockedHiveAuthorizerFactory implements HiveAuthorizerFactory {
    @Override
    public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
        HiveConf conf, HiveAuthenticationProvider authenticator, HiveAuthzSessionContext ctx) {
      TestHiveAuthorizerCheckInvocation.mockedAuthorizer = Mockito.mock(HiveAuthorizer.class);
      return TestHiveAuthorizerCheckInvocation.mockedAuthorizer;
    }

  }

  @BeforeClass
  public static void beforeTest() throws Exception {
    conf = new HiveConfForTest(TestHiveAuthorizerCheckInvocation.class);

    // Turn on mocked authorization
    conf.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER, MockedHiveAuthorizerFactory.class.getName());
    conf.setVar(ConfVars.HIVE_AUTHENTICATOR_MANAGER, SessionStateUserAuthenticator.class.getName());
    conf.setBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
    conf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
    conf.setVar(ConfVars.HIVE_TXN_MANAGER, DbTxnManager.class.getName());
    conf.setBoolVar(ConfVars.HIVE_QUERY_RESULTS_CACHE_ENABLED, true);
    conf.setVar(HiveConf.ConfVars.HIVE_MAPRED_MODE, "nonstrict");
    conf.setBoolVar(ConfVars.HIVE_TEST_AUTHORIZATION_SQLSTD_HS2_MODE, true);
    conf.setBoolVar(ConfVars.HIVE_ZOOKEEPER_KILLQUERY_ENABLE, false);

    TestTxnDbUtil.prepDb(conf);

    SessionState ss = SessionState.start(conf);
    OperationManager operationManager = Mockito.mock(OperationManager.class);
    KillQueryZookeeperManager killQueryZookeeperManager = Mockito.mock(KillQueryZookeeperManager.class);
    KillQueryImpl killQueryImpl = new KillQueryImpl(operationManager, killQueryZookeeperManager);
    ss.setKillQuery(killQueryImpl);

    driver = new Driver(conf);
    runCmd("create table " + tableName
        + " (i int, j int, k string) partitioned by (city string, `date` string) ");
    runCmd("create view " + viewName + " as select * from " + tableName);
    runCmd("create database " + dbName + " WITH DBPROPERTIES ( '" +
            SOURCE_OF_REPLICATION + "' = '1,2,3')");
    runCmd("create table " + fullInTableName + "(i int)");
    // Need a separate table for ACID testing since it has to be bucketed and it has to be Acid
    runCmd("create table " + acidTableName + " (i int, j int, k int) clustered by (k) into 2 buckets " +
        "stored as orc TBLPROPERTIES ('transactional'='true')");
  }

  private static void runCmd(String cmd) throws Exception {
    driver.run(cmd);
  }

  @AfterClass
  public static void afterTests() throws Exception {
    // Drop the tables when we're done.  This makes the test work inside an IDE
    runCmd("drop table if exists " + acidTableName);
    runCmd("drop table if exists " + tableName);
    runCmd("drop table if exists " + viewName);
    runCmd("drop table if exists " + fullInTableName);
    runCmd("drop database if exists " + dbName + " CASCADE");
    driver.close();
  }

  @Test
  public void testInputSomeColumnsUsed() throws Exception {

    reset(mockedAuthorizer);
    int status = driver.compile("select i from " + tableName
        + " where k = 'X' and city = 'Scottsdale-AZ' ", true);
    assertEquals(0, status);

    List<HivePrivilegeObject> inputs = getHivePrivilegeObjectInputs().getLeft();
    checkSingleTableInput(inputs);
    HivePrivilegeObject tableObj = inputs.get(0);
    assertEquals("no of columns used", 3, tableObj.getColumns().size());
    assertEquals("Columns used", Arrays.asList("city", "i", "k"),
        getSortedList(tableObj.getColumns()));
  }

  @Test
  public void testInputSomeColumnsUsedView() throws Exception {

    reset(mockedAuthorizer);
    int status = driver.compile("select i from " + viewName
        + " where k = 'X' and city = 'Scottsdale-AZ' ", true);
    assertEquals(0, status);

    List<HivePrivilegeObject> inputs = getHivePrivilegeObjectInputs().getLeft();
    checkSingleViewInput(inputs);
    HivePrivilegeObject tableObj = inputs.get(0);
    assertEquals("no of columns used", 3, tableObj.getColumns().size());
    assertEquals("Columns used", Arrays.asList("city", "i", "k"),
        getSortedList(tableObj.getColumns()));
  }

  @Test
  public void testQueryCacheIgnored() throws Exception {

    reset(mockedAuthorizer);
    int status = driver.compile("select i from " + acidTableName
        + " where i > 0 ", true);
    assertEquals(0, status);
    List<HivePrivilegeObject> outputs = getHivePrivilegeObjectInputs().getRight();
    List<HivePrivilegeObject> inputs = getHivePrivilegeObjectInputs().getLeft();
    assertEquals("No outputs for a select", 0, outputs.size());
    assertEquals("One input for this select", 1, inputs.size());
  }

  @Test
  public void testInputSomeColumnsUsedJoin() throws Exception {

    reset(mockedAuthorizer);
    int status = driver.compile("select " + viewName + ".i, " + tableName + ".city from "
        + viewName + " join " + tableName + " on " + viewName + ".city = " + tableName
        + ".city where " + tableName + ".k = 'X'", true);
    assertEquals(0, status);

    List<HivePrivilegeObject> inputs = getHivePrivilegeObjectInputs().getLeft();
    Collections.sort(inputs);
    assertEquals(inputs.size(), 2);
    HivePrivilegeObject tableObj = inputs.get(0);
    assertEquals(tableObj.getObjectName().toLowerCase(), tableName.toLowerCase());
    assertEquals("no of columns used", 2, tableObj.getColumns().size());
    assertEquals("Columns used", Arrays.asList("city", "k"), getSortedList(tableObj.getColumns()));
    tableObj = inputs.get(1);
    assertEquals(tableObj.getObjectName().toLowerCase(), viewName.toLowerCase());
    assertEquals("no of columns used", 2, tableObj.getColumns().size());
    assertEquals("Columns used", Arrays.asList("city", "i"), getSortedList(tableObj.getColumns()));
  }

  private List<String> getSortedList(List<String> columns) {
    List<String> sortedCols = new ArrayList<String>(columns);
    Collections.sort(sortedCols);
    return sortedCols;
  }

  @Test
  public void testInputAllColumnsUsed() throws Exception {

    reset(mockedAuthorizer);
    int status = driver.compile("select * from " + tableName + " order by i", true);
    assertEquals(0, status);

    List<HivePrivilegeObject> inputs = getHivePrivilegeObjectInputs().getLeft();
    checkSingleTableInput(inputs);
    HivePrivilegeObject tableObj = inputs.get(0);
    assertEquals("no of columns used", 5, tableObj.getColumns().size());
    assertEquals("Columns used", Arrays.asList("city", "date", "i", "j", "k"),
        getSortedList(tableObj.getColumns()));
  }

  @Test
  public void testWindowingFunction() throws Exception {

    reset(mockedAuthorizer);
    int status = driver.compile("select AVG(`i`) OVER (PARTITION BY `city`) AS iavg FROM " + tableName, true);
    assertEquals(0, status);

    List<HivePrivilegeObject> inputs = getHivePrivilegeObjectInputs().getLeft();
    checkSingleTableInput(inputs);
    HivePrivilegeObject tableObj = inputs.get(0);
    // Make sure none of the hive privilege object contain DB name with WINDOW_FUNC_PREFIX prefix.
    for (HivePrivilegeObject obj : inputs) {
      assertTrue(!obj.getDbname().startsWith(Registry.WINDOW_FUNC_PREFIX));
    }
    assertEquals("no of columns used", 2, tableObj.getColumns().size());
    assertEquals("Columns used", Arrays.asList("city", "i"), getSortedList(tableObj.getColumns()));
  }

  @Test
  public void testCreateTableWithDb() throws Exception {
    final String newTable = "ctTableWithDb";
    checkCreateViewOrTableWithDb(newTable, "create table " + dbName + "." + newTable + "(i int)");
  }

  @Test
  public void testCreateViewWithDb() throws Exception {
    final String newTable = "ctViewWithDb";
    checkCreateViewOrTableWithDb(newTable, "create table " + dbName + "." + newTable + "(i int)");
  }

  private void checkCreateViewOrTableWithDb(String newTable, String cmd)
      throws HiveAuthzPluginException, HiveAccessControlException {
    reset(mockedAuthorizer);
    int status = driver.compile(cmd, true);
    assertEquals(0, status);

    List<HivePrivilegeObject> outputs = getHivePrivilegeObjectInputs().getRight();
    assertEquals("num outputs", 2, outputs.size());
    for (HivePrivilegeObject output : outputs) {
      switch (output.getType()) {
      case DATABASE:
        assertTrue("database name", output.getDbname().equalsIgnoreCase(dbName));
        break;
      case TABLE_OR_VIEW:
        assertTrue("database name", output.getDbname().equalsIgnoreCase(dbName));
        assertEqualsIgnoreCase("table name", output.getObjectName(), newTable);
        break;
      default:
        fail("Unexpected type : " + output.getType());
      }
    }
  }

  private void assertEqualsIgnoreCase(String msg, String expected, String actual) {
    assertEquals(msg, expected.toLowerCase(), actual.toLowerCase());
  }

  @Test
  public void testInputNoColumnsUsed() throws Exception {

    reset(mockedAuthorizer);
    int status = driver.compile("describe " + tableName, true);
    assertEquals(0, status);

    List<HivePrivilegeObject> inputs = getHivePrivilegeObjectInputs().getLeft();
    checkSingleTableInput(inputs);
    HivePrivilegeObject tableObj = inputs.get(0);
    assertNull("columns used", tableObj.getColumns());
  }

  @Test
  public void testPermFunction() throws Exception {

    reset(mockedAuthorizer);
    final String funcName = "testauthfunc1";
    int status = driver.compile("create function " + dbName + "." + funcName
        + " as 'org.apache.hadoop.hive.ql.udf.UDFPI'", true);
    assertEquals(0, status);

    List<HivePrivilegeObject> outputs = getHivePrivilegeObjectInputs().getRight();

    HivePrivilegeObject funcObj;
    assertEquals("number of output objects", 1, outputs.size());

    funcObj = outputs.get(0);

    assertEquals("input type", HivePrivilegeObjectType.FUNCTION, funcObj.getType());
    assertTrue("function name", funcName.equalsIgnoreCase(funcObj.getObjectName()));
    assertTrue("db name", dbName.equalsIgnoreCase(funcObj.getDbname()));

    // actually create the permanent function
    driver.run();

    // Verify privilege objects
    reset(mockedAuthorizer);
    status = driver.compile("select  " + dbName + "." + funcName + "() , i from " + tableName, true);
    assertEquals(0, status);

    List<HivePrivilegeObject> inputs = getHivePrivilegeObjectInputs().getLeft();
    assertEquals("number of input objects", 2, inputs.size());
    HivePrivilegeObject tableObj;
    if (inputs.get(0).getType() == HivePrivilegeObjectType.FUNCTION) {
      funcObj = inputs.get(0);
      tableObj = inputs.get(1);
    } else {
      funcObj = inputs.get(1);
      tableObj = inputs.get(0);
    }

    assertEquals("input type", HivePrivilegeObjectType.FUNCTION, funcObj.getType());
    assertEquals("function name", funcName.toLowerCase(), funcObj.getObjectName().toLowerCase());
    assertEquals("db name", dbName.toLowerCase(), funcObj.getDbname().toLowerCase());

    assertEquals("input type", HivePrivilegeObjectType.TABLE_OR_VIEW, tableObj.getType());
    assertEquals("table name", tableName.toLowerCase(), tableObj.getObjectName().toLowerCase());

    // create 2nd permanent function
    String funcName2 = "funcName2";
    driver.run("create function " + dbName + "." + funcName2 + " as 'org.apache.hadoop.hive.ql.udf.UDFRand'");

    // try using 2nd permanent function and verify its only 2nd one that shows up
    // for auth
    reset(mockedAuthorizer);
    status = driver.compile("select  " + dbName + "." + funcName2 + "(i)  from " + tableName, true);
    assertEquals(0, status);

    inputs = getHivePrivilegeObjectInputs().getLeft();
    assertEquals("number of input objects", 2, inputs.size());
    if (inputs.get(0).getType() == HivePrivilegeObjectType.FUNCTION) {
      funcObj = inputs.get(0);
      tableObj = inputs.get(1);
    } else {
      funcObj = inputs.get(1);
      tableObj = inputs.get(0);
    }

    assertEquals("input type", HivePrivilegeObjectType.FUNCTION, funcObj.getType());
    assertEquals("function name", funcName2.toLowerCase(), funcObj.getObjectName().toLowerCase());
    assertEquals("db name", dbName.toLowerCase(), funcObj.getDbname().toLowerCase());

    assertEquals("input type", HivePrivilegeObjectType.TABLE_OR_VIEW, tableObj.getType());
    assertEquals("table name", tableName.toLowerCase(), tableObj.getObjectName().toLowerCase());

    // try using both permanent functions
    reset(mockedAuthorizer);
    status = driver.compile(
        "select  " + dbName + "." + funcName2 + "(i), " + dbName + "." + funcName + "(), j  from " + tableName, true);
    assertEquals(0, status);

    inputs = getHivePrivilegeObjectInputs().getLeft();
    assertEquals("number of input objects", 3, inputs.size());
    boolean foundF1 = false;
    boolean foundF2 = false;
    boolean foundTable = false;
    for (HivePrivilegeObject inp : inputs) {
      if (inp.getType() == HivePrivilegeObjectType.FUNCTION) {
        if (funcName.equalsIgnoreCase(inp.getObjectName())) {
          foundF1 = true;
        } else if (funcName2.equalsIgnoreCase(inp.getObjectName())) {
          foundF2 = true;
        }
      } else if (inp.getType() == HivePrivilegeObjectType.TABLE_OR_VIEW
          && tableName.equalsIgnoreCase(inp.getObjectName().toLowerCase())) {
        foundTable = true;
      }
    }
    assertTrue("Found " + funcName, foundF1);
    assertTrue("Found " + funcName2, foundF2);
    assertTrue("Found Table", foundTable);
  }

  @Test
  public void testTempFunction() throws Exception {

    reset(mockedAuthorizer);
    final String funcName = "testAuthFunc2";
    int status = driver.compile("create temporary function " + funcName
        + " as 'org.apache.hadoop.hive.ql.udf.UDFPI'", true);
    assertEquals(0, status);

    List<HivePrivilegeObject> outputs = getHivePrivilegeObjectInputs().getRight();
    HivePrivilegeObject funcObj = outputs.get(0);
    assertEquals("input type", HivePrivilegeObjectType.FUNCTION, funcObj.getType());
    assertTrue("function name", funcName.equalsIgnoreCase(funcObj.getObjectName()));
    assertEquals("db name", null, funcObj.getDbname());
  }

  @Test
  public void testTempTable() throws Exception {

    String tmpTableDir = getDefaultTmp() + File.separator + "THSAC_testTableTable";

    final String tableName = "testTempTable";
    { // create temp table
      reset(mockedAuthorizer);
      driver.run("create temporary table " + tableName + "(i int) location '" + tmpTableDir + "'");

      List<HivePrivilegeObject> inputs = getHivePrivilegeObjectInputs().getLeft();
      List<HivePrivilegeObject> outputs = getHivePrivilegeObjectInputs().getRight();

      // only the URI should be passed for authorization check
      assertEquals("input count", 1, inputs.size());
      assertEquals("input type", HivePrivilegeObjectType.LOCAL_URI, inputs.get(0).getType());

      // only the dbname should be passed authorization check
      assertEquals("output count", 1, outputs.size());
      assertEquals("output type", HivePrivilegeObjectType.DATABASE, outputs.get(0).getType());

      int status = driver.compile("select * from " + tableName, true);
      assertEquals(0, status);
    }
    { // select from the temp table
      reset(mockedAuthorizer);
      int status = driver.compile("insert into " + tableName + " values(1)", true);
      assertEquals(0, status);

      // temp tables should be skipped from authorization
      List<HivePrivilegeObject> inputs = getHivePrivilegeObjectInputs().getLeft();
      List<HivePrivilegeObject> outputs = getHivePrivilegeObjectInputs().getRight();
      System.err.println("inputs " + inputs);
      System.err.println("outputs " + outputs);

      assertEquals("input count", 0, inputs.size());
      assertEquals("output count", 0, outputs.size());
    }
    { // select from the temp table
      reset(mockedAuthorizer);
      int status = driver.compile("select * from " + tableName, true);
      assertEquals(0, status);

      // temp tables should be skipped from authorization
      List<HivePrivilegeObject> inputs = getHivePrivilegeObjectInputs().getLeft();
      List<HivePrivilegeObject> outputs = getHivePrivilegeObjectInputs().getRight();
      System.err.println("inputs " + inputs);
      System.err.println("outputs " + outputs);

      assertEquals("input count", 0, inputs.size());
      assertEquals("output count", 0, outputs.size());
    }

  }

  @Test
  public void testTempTableImplicit() throws Exception {
    final String tableName = "testTempTableImplicit";
    driver.run("create table " + tableName + "(i int)");

    reset(mockedAuthorizer);
    int status = driver.compile("insert into " + tableName + " values (1)", true);
    assertEquals(0, status);

    List<HivePrivilegeObject> inputs = getHivePrivilegeObjectInputs().getLeft();
    List<HivePrivilegeObject> outputs = getHivePrivilegeObjectInputs().getRight();

    // only the URI should be passed for authorization check
    assertEquals("input count", 0, inputs.size());

    reset(mockedAuthorizer);
    status = driver.compile("select * from " + tableName, true);
    assertEquals(0, status);

    inputs = getHivePrivilegeObjectInputs().getLeft();
    outputs = getHivePrivilegeObjectInputs().getRight();

    // temp tables should be skipped from authorization
    assertEquals("input count", 1, inputs.size());
    assertEquals("output count", 0, outputs.size());

  }

  private String getDefaultTmp() {
    return System.getProperty("test.tmp.dir",
        "target" + File.separator + "test" + File.separator + "tmp");
  }

  @Test
  public void testUpdateSomeColumnsUsed() throws Exception {
    reset(mockedAuthorizer);
    int status = driver.compile("update " + acidTableName + " set i = 5 where j = 3", true);
    assertEquals(0, status);

    Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> io = getHivePrivilegeObjectInputs();
    List<HivePrivilegeObject> outputs = io.getRight();
    HivePrivilegeObject tableObj = outputs.get(0);
    LOG.debug("Got privilege object " + tableObj);
    assertEquals("no of columns used", 1, tableObj.getColumns().size());
    assertEquals("Column used", "i", tableObj.getColumns().get(0));
    List<HivePrivilegeObject> inputs = io.getLeft();
    assertEquals(1, inputs.size());
    tableObj = inputs.get(0);
    assertEquals(2, tableObj.getColumns().size());
    assertEquals("j", tableObj.getColumns().get(0));
  }

  @Test
  public void testUpdateSomeColumnsUsedExprInSet() throws Exception {
    reset(mockedAuthorizer);
    int status = driver.compile("update " + acidTableName + " set i = 5, j = k where j = 3", true);
    assertEquals(0, status);

    Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> io = getHivePrivilegeObjectInputs();
    List<HivePrivilegeObject> outputs = io.getRight();
    HivePrivilegeObject tableObj = outputs.get(0);
    LOG.debug("Got privilege object " + tableObj);
    assertEquals("no of columns used", 2, tableObj.getColumns().size());
    assertEquals("Columns used", Arrays.asList("i", "j"),
        getSortedList(tableObj.getColumns()));
    List<HivePrivilegeObject> inputs = io.getLeft();
    assertEquals(1, inputs.size());
    tableObj = inputs.get(0);
    assertEquals(2, tableObj.getColumns().size());
    assertEquals("Columns used", Arrays.asList("j", "k"),
        getSortedList(tableObj.getColumns()));
  }

  @Test
  public void testDelete() throws Exception {
    reset(mockedAuthorizer);
    int status = driver.compile("delete from " + acidTableName + " where j = 3", true);
    assertEquals(0, status);

    Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> io = getHivePrivilegeObjectInputs();
    List<HivePrivilegeObject> inputs = io.getLeft();
    assertEquals(1, inputs.size());
    HivePrivilegeObject tableObj = inputs.get(0);
    assertEquals(1, tableObj.getColumns().size());
    assertEquals("j", tableObj.getColumns().get(0));
  }

  @Test
  public void testShowTables() throws Exception {
    reset(mockedAuthorizer);
    int status = driver.compile("show tables", true);
    assertEquals(0, status);

    Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> io = getHivePrivilegeObjectInputs();
    List<HivePrivilegeObject> inputs = io.getLeft();
    assertEquals(1, inputs.size());
    HivePrivilegeObject dbObj = inputs.get(0);
    assertEquals("default", dbObj.getDbname().toLowerCase());
  }

  @Test
  public void testDescDatabase() throws Exception {
    reset(mockedAuthorizer);
    int status = driver.compile("describe database " + dbName, true);
    assertEquals(0, status);

    Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> io = getHivePrivilegeObjectInputs();
    List<HivePrivilegeObject> inputs = io.getLeft();
    assertEquals(1, inputs.size());
    HivePrivilegeObject dbObj = inputs.get(0);
    assertEquals(dbName.toLowerCase(), dbObj.getDbname().toLowerCase());
  }

  private void resetAuthorizer() throws HiveAuthzPluginException, HiveAccessControlException {
    reset(mockedAuthorizer);
    // HiveAuthorizer.filterListCmdObjects should not filter any object
    when(mockedAuthorizer.filterListCmdObjects(any(List.class),
      any(HiveAuthzContext.class))).thenAnswer(new Answer<List<HivePrivilegeObject>>() {
        @Override
        public List<HivePrivilegeObject> answer(InvocationOnMock invocation) throws Throwable {
          Object[] args = invocation.getArguments();
          return (List<HivePrivilegeObject>) args[0];
        }
      });
  }

  @Test
  public void testReplDump() throws Exception {

    resetAuthorizer();
    int status = driver.compile("repl dump " + dbName, true);
    assertEquals(0, status);
    List<HivePrivilegeObject> inputs = getHivePrivilegeObjectInputs().getLeft();
    HivePrivilegeObject dbObj = inputs.get(0);
    assertEquals("input type", HivePrivilegeObjectType.DATABASE, dbObj.getType());
    assertEquals("db name", dbName.toLowerCase(), dbObj.getDbname());

    resetAuthorizer();
    status = driver.compile("repl dump " + dbName + ".'" + inDbTableName + "'", true);
    assertEquals(0, status);
    inputs = getHivePrivilegeObjectInputs().getLeft();
    dbObj = inputs.get(0);
    assertEquals("input type", HivePrivilegeObjectType.TABLE_OR_VIEW, dbObj.getType());
    assertEquals("db name", dbName.toLowerCase(), dbObj.getDbname());
    assertEquals("table name", inDbTableName.toLowerCase(), dbObj.getObjectName());
  }

  @Test
  public void showTablesInDB() throws Exception{
    final String tableName1 = "table1";
    driver.run("create table " + dbName+"."+tableName1 + "(eid int, yoj int)");
    final String tableName2 = "table2";
    driver.run("create table " + dbName+"."+tableName2 + "(eid int, ecode int)");
    reset(mockedAuthorizer);

    int status = driver.compile("show tables in "+dbName, true);
    assertEquals(0, status);
    Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> io = getHivePrivilegeObjectInputs();
    List<HivePrivilegeObject> inputs = io.getLeft();
    HivePrivilegeObject dbObj = inputs.get(0);
    assertEquals("input type", HivePrivilegeObjectType.DATABASE, dbObj.getType());
    assertTrue(dbObj.getOwnerName() != null);
  }

  @Test
  public void AlterViewAsStmnt() throws Exception{
    reset(mockedAuthorizer);
    final String tableName1 = "foo_tbl";
    driver.run("create table " + dbName+"."+tableName1 + "(eid int, yoj int)");
    final String tableName2 = "foo_bar";
    driver.run("create table " + dbName+"."+tableName2 + "(eid int, name string)");
    final String viewName = "foo_view";
    driver.run("create view " + dbName+"."+viewName + " as select * from "+ dbName+"."+tableName1);
    reset(mockedAuthorizer);
    int status = driver.compile("Alter view "+dbName+"."+viewName+" as select * from "+ dbName+"."+tableName2, true);
    assertEquals(0, status);
    Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> io = getHivePrivilegeObjectInputs();
    List<HivePrivilegeObject> inputs = io.getLeft();
    assertEquals(1, inputs.size()); // foo_bar table object
    List<HivePrivilegeObject> outputs = io.getRight();
    assertEquals(2, outputs.size()); // Database and view objects
    HivePrivilegeObject dbObj = outputs.get(0);
    assertEquals("input type", HivePrivilegeObjectType.DATABASE, dbObj.getType());
    HivePrivilegeObject viewObj = outputs.get(1);
    assertEquals("input type", HivePrivilegeObjectType.TABLE_OR_VIEW, viewObj.getType());
  }

  private void checkSingleTableInput(List<HivePrivilegeObject> inputs) {
    assertEquals("number of inputs", 1, inputs.size());

    HivePrivilegeObject tableObj = inputs.get(0);
    assertEquals("input type", HivePrivilegeObjectType.TABLE_OR_VIEW, tableObj.getType());
    assertTrue("table name", tableName.equalsIgnoreCase(tableObj.getObjectName()));
  }

  private void checkSingleViewInput(List<HivePrivilegeObject> inputs) {
    assertEquals("number of inputs", 1, inputs.size());

    HivePrivilegeObject tableObj = inputs.get(0);
    assertEquals("input type", HivePrivilegeObjectType.TABLE_OR_VIEW, tableObj.getType());
    assertTrue("table name", viewName.equalsIgnoreCase(tableObj.getObjectName()));
  }

  /**
   * @return pair with left value as inputs and right value as outputs,
   *  passed in current call to authorizer.checkPrivileges
   * @throws HiveAuthzPluginException
   * @throws HiveAccessControlException
   */
  private Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> getHivePrivilegeObjectInputs() throws HiveAuthzPluginException,
      HiveAccessControlException {
    // Create argument capturer
    // a class variable cast to this generic of generic class
    Class<List<HivePrivilegeObject>> class_listPrivObjects = (Class) List.class;
    ArgumentCaptor<List<HivePrivilegeObject>> inputsCapturer = ArgumentCaptor
        .forClass(class_listPrivObjects);
    ArgumentCaptor<List<HivePrivilegeObject>> outputsCapturer = ArgumentCaptor
        .forClass(class_listPrivObjects);

    verify(mockedAuthorizer).checkPrivileges(any(HiveOperationType.class),
        inputsCapturer.capture(), outputsCapturer.capture(),
        any(HiveAuthzContext.class));

    return new ImmutablePair<List<HivePrivilegeObject>, List<HivePrivilegeObject>>(
        inputsCapturer.getValue(), outputsCapturer.getValue());
  }

  /**
   * Unit test for HIVE-25532.
   * Checks if the right privilege objects are being sent when a kill query call is made.
   * @throws Exception
   */
  @Test
  public void testKillQueryAuthorization() throws Exception {
    int queryStatus = driver.compile("select " + viewName + ".i, " + tableName + ".city from "
            + viewName + " join " + tableName + " on " + viewName + ".city = " + tableName
            + ".city where " + tableName + ".k = 'X'", true);
    assertEquals(0, queryStatus);

    resetAuthorizer();
    QueryState queryState = driver.getQueryState();
    String queryId = queryState.getQueryId();
    int killQueryStatus = driver.compile("kill query '" + queryId + "'", true);
    assertEquals(0, killQueryStatus);
    driver.run();

    List<HivePrivilegeObject> inputs = getHivePrivilegeObjectInputs().getLeft();
    HivePrivilegeObject dbObj = inputs.get(0);
    assertEquals("input type", HivePrivilegeObjectType.SERVICE_NAME, dbObj.getType());
    assertEquals("object name","hiveservice", dbObj.getObjectName());
  }

  @Test
  public void AddDropConstraints() throws Exception{
    reset(mockedAuthorizer);
    final String tableName = "foo_constraint_tbl";
    driver.run("create table " + dbName+"."+tableName + "(eid int, yoj int)");

    reset(mockedAuthorizer);
    int status = driver.compile("Alter table "+dbName+"."+tableName +" ADD constraint c1_unique UNIQUE(eid) disable novalidate", true);
    assertEquals(0, status);
    Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> io = getHivePrivilegeObjectInputs();
    List<HivePrivilegeObject> inputs = io.getLeft();
    assertEquals(1, inputs.size());
    HivePrivilegeObject tableObj = inputs.get(0);
    assertEquals("input type", HivePrivilegeObjectType.TABLE_OR_VIEW, tableObj.getType());

    reset(mockedAuthorizer);
    status = driver.compile("Alter table "+dbName+"."+tableName +" DROP constraint c1_unique", true);
    assertEquals(0, status);
    io = getHivePrivilegeObjectInputs();
    inputs = io.getLeft();
    assertEquals(1, inputs.size());
    tableObj = inputs.get(0);
    assertEquals("input type", HivePrivilegeObjectType.TABLE_OR_VIEW, tableObj.getType());
  }
}
