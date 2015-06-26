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
import static org.mockito.Matchers.any;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.UtilsForTest;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.CommandNeedRetryException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.SessionStateUserAuthenticator;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test HiveAuthorizer api invocation for filtering objects
 */
public class TestHiveAuthorizerShowFilters {
  protected static HiveConf conf;
  protected static Driver driver;
  private static final String tableName1 = (TestHiveAuthorizerShowFilters.class.getSimpleName() + "table1")
      .toLowerCase();
  private static final String tableName2 = (TestHiveAuthorizerShowFilters.class.getSimpleName() + "table2")
      .toLowerCase();
  private static final String dbName1 = (TestHiveAuthorizerShowFilters.class.getSimpleName() + "db1")
      .toLowerCase();
  private static final String dbName2 = (TestHiveAuthorizerShowFilters.class.getSimpleName() + "db2")
      .toLowerCase();

  static HiveAuthorizer mockedAuthorizer;

  static final List<String> AllTables = getSortedList(tableName1, tableName2);
  static final List<String> AllDbs = getSortedList("default", dbName1, dbName2);

  private static List<HivePrivilegeObject> filterArguments = null;
  private static List<HivePrivilegeObject> filteredResults = new ArrayList<HivePrivilegeObject>();

  /**
   * This factory creates a mocked HiveAuthorizer class. The mocked class is
   * used to capture the argument passed to HiveAuthorizer.filterListCmdObjects.
   * It returns fileredResults object for call to
   * HiveAuthorizer.filterListCmdObjects, and stores the list argument in
   * filterArguments
   */
  static class MockedHiveAuthorizerFactory implements HiveAuthorizerFactory {
    @Override
    public HiveAuthorizer createHiveAuthorizer(HiveMetastoreClientFactory metastoreClientFactory,
        HiveConf conf, HiveAuthenticationProvider authenticator, HiveAuthzSessionContext ctx) {
      Mockito.validateMockitoUsage();

      abstract class AuthorizerWithFilterCmdImpl implements HiveAuthorizer {
        @Override
        public List<HivePrivilegeObject> filterListCmdObjects(List<HivePrivilegeObject> listObjs,
            HiveAuthzContext context) throws HiveAuthzPluginException, HiveAccessControlException {
          // capture arguments in static
          filterArguments = listObjs;
          // return static variable with results, if it is set to some set of
          // values
          // otherwise return the arguments
          if (filteredResults.size() == 0) {
            return filterArguments;
          }
          return filteredResults;
        }
      }

      mockedAuthorizer = Mockito.mock(AuthorizerWithFilterCmdImpl.class, Mockito.withSettings()
          .verboseLogging());

      try {
        Mockito.when(
            mockedAuthorizer.filterListCmdObjects((List<HivePrivilegeObject>) any(),
                (HiveAuthzContext) any())).thenCallRealMethod();
      } catch (Exception e) {
        org.junit.Assert.fail("Caught exception " + e);
      }
      return mockedAuthorizer;
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
    conf.setBoolVar(ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    UtilsForTest.setNewDerbyDbLocation(conf, TestHiveAuthorizerShowFilters.class.getSimpleName());

    SessionState.start(conf);
    driver = new Driver(conf);
    runCmd("create table " + tableName1
        + " (i int, j int, k string) partitioned by (city string, `date` string) ");
    runCmd("create table " + tableName2 + "(i int)");

    runCmd("create database " + dbName1);
    runCmd("create database " + dbName2);

  }



  @Before
  public void setup() {
    filterArguments = null;
    filteredResults.clear();
  }

  @AfterClass
  public static void afterTests() throws Exception {
    // Drop the tables when we're done. This makes the test work inside an IDE
    runCmd("drop table if exists " + tableName1);
    runCmd("drop table if exists " + tableName2);
    runCmd("drop database if exists " + dbName1);
    runCmd("drop database if exists " + dbName2);
    driver.close();
  }

  @Test
  public void testShowDatabasesAll() throws HiveAuthzPluginException, HiveAccessControlException,
      CommandNeedRetryException, IOException {
    runShowDbTest(AllDbs);
  }

  @Test
  public void testShowDatabasesSelected() throws HiveAuthzPluginException,
      HiveAccessControlException, CommandNeedRetryException, IOException {
    setFilteredResults(HivePrivilegeObjectType.DATABASE, dbName2);
    runShowDbTest(Arrays.asList(dbName2));
  }

  private void runShowDbTest(List<String> expectedDbList) throws HiveAuthzPluginException,
      HiveAccessControlException, CommandNeedRetryException, IOException {
    runCmd("show databases");
    verifyAllDb();
    assertEquals("filtered result check ", expectedDbList, getSortedResults());
  }

  @Test
  public void testShowTablesAll() throws HiveAuthzPluginException, HiveAccessControlException,
      CommandNeedRetryException, IOException {
    runShowTablesTest(AllTables);
  }

  @Test
  public void testShowTablesSelected() throws HiveAuthzPluginException, HiveAccessControlException,
      CommandNeedRetryException, IOException {
    setFilteredResults(HivePrivilegeObjectType.TABLE_OR_VIEW, tableName2);
    runShowTablesTest(Arrays.asList(tableName2));
  }

  private void runShowTablesTest(List<String> expectedTabs) throws IOException,
      CommandNeedRetryException, HiveAuthzPluginException, HiveAccessControlException {
    runCmd("show tables");
    verifyAllTables();
    assertEquals("filtered result check ", expectedTabs, getSortedResults());
  }

  private List<String> getSortedResults() throws IOException, CommandNeedRetryException {
    List<String> res = new ArrayList<String>();
    // set results to be returned
    driver.getResults(res);
    Collections.sort(res);
    return res;
  }

  /**
   * Verify that arguments to call to HiveAuthorizer.filterListCmdObjects are of
   * type DATABASE and contain all databases.
   *
   * @throws HiveAccessControlException
   * @throws HiveAuthzPluginException
   */
  private void verifyAllDb() throws HiveAuthzPluginException, HiveAccessControlException {
    List<HivePrivilegeObject> privObjs = filterArguments;

    // get the db names out
    List<String> dbArgs = new ArrayList<String>();
    for (HivePrivilegeObject privObj : privObjs) {
      assertEquals("Priv object type should be db", HivePrivilegeObjectType.DATABASE,
          privObj.getType());
      dbArgs.add(privObj.getDbname());
    }

    // sort before comparing with expected results
    Collections.sort(dbArgs);
    assertEquals("All db should be passed as arguments", AllDbs, dbArgs);
  }

  /**
   * Verify that arguments to call to HiveAuthorizer.filterListCmdObjects are of
   * type TABLE and contain all tables.
   *
   * @throws HiveAccessControlException
   * @throws HiveAuthzPluginException
   */
  private void verifyAllTables() throws HiveAuthzPluginException, HiveAccessControlException {
    List<HivePrivilegeObject> privObjs = filterArguments;

    // get the table names out
    List<String> tables = new ArrayList<String>();
    for (HivePrivilegeObject privObj : privObjs) {
      assertEquals("Priv object type should be db", HivePrivilegeObjectType.TABLE_OR_VIEW,
          privObj.getType());
      assertEquals("Database name", "default", privObj.getDbname());
      tables.add(privObj.getObjectName());
    }

    // sort before comparing with expected results
    Collections.sort(tables);
    assertEquals("All tables should be passed as arguments", AllTables, tables);
  }

  private static void setFilteredResults(HivePrivilegeObjectType type, String... objs) {
    filteredResults.clear();
    for (String obj : objs) {
      String dbname;
      String tabname = null;
      if (type == HivePrivilegeObjectType.DATABASE) {
        dbname = obj;
      } else {
        dbname = "default";
        tabname = obj;
      }
      filteredResults.add(new HivePrivilegeObject(type, dbname, tabname));
    }
  }

  private static void runCmd(String cmd) throws CommandNeedRetryException {
    CommandProcessorResponse resp = driver.run(cmd);
    assertEquals(0, resp.getResponseCode());
  }

  private static List<String> getSortedList(String... strings) {
    return getSortedList(Arrays.asList(strings));
  }

  private static List<String> getSortedList(List<String> columns) {
    List<String> sortedCols = new ArrayList<String>(columns);
    Collections.sort(sortedCols);
    return sortedCols;
  }

}
