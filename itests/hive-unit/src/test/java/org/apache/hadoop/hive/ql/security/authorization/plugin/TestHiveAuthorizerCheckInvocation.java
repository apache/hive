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

package org.apache.hadoop.hive.ql.security.authorization.plugin;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

/**
 * Test HiveAuthorizer api invocation
 */
public class TestHiveAuthorizerCheckInvocation {
  private final Log LOG = LogFactory.getLog(this.getClass().getName());;
  protected static HiveConf conf;
  protected static Driver driver;
  private static final String tableName = TestHiveAuthorizerCheckInvocation.class.getSimpleName()
      + "Table";
  private static final String inDbTableName = tableName + "_in_db";
  private static final String acidTableName = tableName + "_acid";
  private static final String dbName = TestHiveAuthorizerCheckInvocation.class.getSimpleName()
      + "Db";
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
    conf = new HiveConf();

    // Turn on mocked authorization
    conf.setVar(ConfVars.HIVE_AUTHORIZATION_MANAGER, MockedHiveAuthorizerFactory.class.getName());
    conf.setVar(ConfVars.HIVE_AUTHENTICATOR_MANAGER, SessionStateUserAuthenticator.class.getName());
    conf.setBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED, true);
    conf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
    conf.setVar(ConfVars.HIVE_TXN_MANAGER, DbTxnManager.class.getName());

    SessionState.start(conf);
    driver = new Driver(conf);
    runCmd("create table " + tableName
        + " (i int, j int, k string) partitioned by (city string, `date` string) ");
    runCmd("create database " + dbName);
    runCmd("create table " + dbName + "." + inDbTableName + "(i int)");
    // Need a separate table for ACID testing since it has to be bucketed and it has to be Acid
    runCmd("create table " + acidTableName + " (i int, j int, k int) clustered by (k) into 2 buckets " +
        "stored as orc TBLPROPERTIES ('transactional'='true')");
  }

  private static void runCmd(String cmd) throws CommandNeedRetryException {
    CommandProcessorResponse resp = driver.run(cmd);
    assertEquals(0, resp.getResponseCode());
  }

  @AfterClass
  public static void afterTests() throws Exception {
    // Drop the tables when we're done.  This makes the test work inside an IDE
    runCmd("drop table if exists " + acidTableName);
    runCmd("drop table if exists " + tableName);
    runCmd("drop table if exists " + dbName + "." + inDbTableName);
    runCmd("drop database if exists " + dbName );
    driver.close();
  }

  @Test
  public void testInputSomeColumnsUsed() throws HiveAuthzPluginException, HiveAccessControlException,
      CommandNeedRetryException {

    reset(mockedAuthorizer);
    int status = driver.compile("select i from " + tableName
        + " where k = 'X' and city = 'Scottsdale-AZ' ");
    assertEquals(0, status);

    List<HivePrivilegeObject> inputs = getHivePrivilegeObjectInputs().getLeft();
    checkSingleTableInput(inputs);
    HivePrivilegeObject tableObj = inputs.get(0);
    assertEquals("no of columns used", 3, tableObj.getColumns().size());
    assertEquals("Columns used", Arrays.asList("city", "i", "k"),
        getSortedList(tableObj.getColumns()));
  }

  private List<String> getSortedList(List<String> columns) {
    List<String> sortedCols = new ArrayList<String>(columns);
    Collections.sort(sortedCols);
    return sortedCols;
  }

  @Test
  public void testInputAllColumnsUsed() throws HiveAuthzPluginException, HiveAccessControlException,
      CommandNeedRetryException {

    reset(mockedAuthorizer);
    int status = driver.compile("select * from " + tableName + " order by i");
    assertEquals(0, status);

    List<HivePrivilegeObject> inputs = getHivePrivilegeObjectInputs().getLeft();
    checkSingleTableInput(inputs);
    HivePrivilegeObject tableObj = inputs.get(0);
    assertEquals("no of columns used", 5, tableObj.getColumns().size());
    assertEquals("Columns used", Arrays.asList("city", "date", "i", "j", "k"),
        getSortedList(tableObj.getColumns()));
  }

  @Test
  public void testCreateTableWithDb() throws HiveAuthzPluginException, HiveAccessControlException,
      CommandNeedRetryException {
    final String newTable = "ctTableWithDb";
    checkCreateViewOrTableWithDb(newTable, "create table " + dbName + "." + newTable + "(i int)");
  }

  @Test
  public void testCreateViewWithDb() throws HiveAuthzPluginException, HiveAccessControlException,
      CommandNeedRetryException {
    final String newTable = "ctViewWithDb";
    checkCreateViewOrTableWithDb(newTable, "create table " + dbName + "." + newTable + "(i int)");
  }

  private void checkCreateViewOrTableWithDb(String newTable, String cmd)
      throws HiveAuthzPluginException, HiveAccessControlException {
    reset(mockedAuthorizer);
    int status = driver.compile(cmd);
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
  public void testInputNoColumnsUsed() throws HiveAuthzPluginException, HiveAccessControlException,
      CommandNeedRetryException {

    reset(mockedAuthorizer);
    int status = driver.compile("describe " + tableName);
    assertEquals(0, status);

    List<HivePrivilegeObject> inputs = getHivePrivilegeObjectInputs().getLeft();
    checkSingleTableInput(inputs);
    HivePrivilegeObject tableObj = inputs.get(0);
    assertNull("columns used", tableObj.getColumns());
  }

  @Test
  public void testPermFunction() throws HiveAuthzPluginException, HiveAccessControlException,
      CommandNeedRetryException {

    reset(mockedAuthorizer);
    final String funcName = "testauthfunc1";
    int status = driver.compile("create function " + dbName + "." + funcName
        + " as 'org.apache.hadoop.hive.ql.udf.UDFPI'");
    assertEquals(0, status);

    List<HivePrivilegeObject> outputs = getHivePrivilegeObjectInputs().getRight();

    HivePrivilegeObject funcObj;
    HivePrivilegeObject dbObj;
    assertEquals("number of output object", 2, outputs.size());
    if(outputs.get(0).getType() == HivePrivilegeObjectType.FUNCTION) {
      funcObj = outputs.get(0);
      dbObj = outputs.get(1);
    } else {
      funcObj = outputs.get(1);
      dbObj = outputs.get(0);
    }

    assertEquals("input type", HivePrivilegeObjectType.FUNCTION, funcObj.getType());
    assertTrue("function name", funcName.equalsIgnoreCase(funcObj.getObjectName()));
    assertTrue("db name", dbName.equalsIgnoreCase(funcObj.getDbname()));

    assertEquals("input type", HivePrivilegeObjectType.DATABASE, dbObj.getType());
    assertTrue("db name", dbName.equalsIgnoreCase(dbObj.getDbname()));
  }

  @Test
  public void testTempFunction() throws HiveAuthzPluginException, HiveAccessControlException,
      CommandNeedRetryException {

    reset(mockedAuthorizer);
    final String funcName = "testAuthFunc2";
    int status = driver.compile("create temporary function " + funcName
        + " as 'org.apache.hadoop.hive.ql.udf.UDFPI'");
    assertEquals(0, status);

    List<HivePrivilegeObject> outputs = getHivePrivilegeObjectInputs().getRight();
    HivePrivilegeObject funcObj = outputs.get(0);
    assertEquals("input type", HivePrivilegeObjectType.FUNCTION, funcObj.getType());
    assertTrue("function name", funcName.equalsIgnoreCase(funcObj.getObjectName()));
    assertEquals("db name", null, funcObj.getDbname());
  }

  @Test
  public void testUpdateSomeColumnsUsed() throws HiveAuthzPluginException,
      HiveAccessControlException, CommandNeedRetryException {
    reset(mockedAuthorizer);
    int status = driver.compile("update " + acidTableName + " set i = 5 where j = 3");
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
  public void testUpdateSomeColumnsUsedExprInSet() throws HiveAuthzPluginException,
      HiveAccessControlException, CommandNeedRetryException {
    reset(mockedAuthorizer);
    int status = driver.compile("update " + acidTableName + " set i = 5, l = k where j = 3");
    assertEquals(0, status);

    Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> io = getHivePrivilegeObjectInputs();
    List<HivePrivilegeObject> outputs = io.getRight();
    HivePrivilegeObject tableObj = outputs.get(0);
    LOG.debug("Got privilege object " + tableObj);
    assertEquals("no of columns used", 2, tableObj.getColumns().size());
    assertEquals("Columns used", Arrays.asList("i", "l"),
        getSortedList(tableObj.getColumns()));
    List<HivePrivilegeObject> inputs = io.getLeft();
    assertEquals(1, inputs.size());
    tableObj = inputs.get(0);
    assertEquals(2, tableObj.getColumns().size());
    assertEquals("Columns used", Arrays.asList("j", "k"),
        getSortedList(tableObj.getColumns()));
  }

  @Test
  public void testDelete() throws HiveAuthzPluginException,
      HiveAccessControlException, CommandNeedRetryException {
    reset(mockedAuthorizer);
    int status = driver.compile("delete from " + acidTableName + " where j = 3");
    assertEquals(0, status);

    Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> io = getHivePrivilegeObjectInputs();
    List<HivePrivilegeObject> inputs = io.getLeft();
    assertEquals(1, inputs.size());
    HivePrivilegeObject tableObj = inputs.get(0);
    assertEquals(1, tableObj.getColumns().size());
    assertEquals("j", tableObj.getColumns().get(0));
  }

  @Test
  public void testShowTables() throws HiveAuthzPluginException,
      HiveAccessControlException, CommandNeedRetryException {
    reset(mockedAuthorizer);
    int status = driver.compile("show tables");
    assertEquals(0, status);

    Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> io = getHivePrivilegeObjectInputs();
    List<HivePrivilegeObject> inputs = io.getLeft();
    assertEquals(1, inputs.size());
    HivePrivilegeObject dbObj = inputs.get(0);
    assertEquals("default", dbObj.getDbname().toLowerCase());
  }

  @Test
  public void testDescDatabase() throws HiveAuthzPluginException,
      HiveAccessControlException, CommandNeedRetryException {
    reset(mockedAuthorizer);
    int status = driver.compile("describe database " + dbName);
    assertEquals(0, status);

    Pair<List<HivePrivilegeObject>, List<HivePrivilegeObject>> io = getHivePrivilegeObjectInputs();
    List<HivePrivilegeObject> inputs = io.getLeft();
    assertEquals(1, inputs.size());
    HivePrivilegeObject dbObj = inputs.get(0);
    assertEquals(dbName.toLowerCase(), dbObj.getDbname().toLowerCase());
  }


  private void checkSingleTableInput(List<HivePrivilegeObject> inputs) {
    assertEquals("number of inputs", 1, inputs.size());

    HivePrivilegeObject tableObj = inputs.get(0);
    assertEquals("input type", HivePrivilegeObjectType.TABLE_OR_VIEW, tableObj.getType());
    assertTrue("table name", tableName.equalsIgnoreCase(tableObj.getObjectName()));
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

    return new ImmutablePair(inputsCapturer.getValue(), outputsCapturer.getValue());
  }

}
