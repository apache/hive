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
package org.apache.hadoop.hive.ql.schq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryKey;
import org.apache.hadoop.hive.metastore.model.MScheduledQuery;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.scheduled.ScheduledQueryExecutionService;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.testutils.HiveTestEnvSetup;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.Optional;

public class TestScheduledQueryStatements {

  @ClassRule
  public static HiveTestEnvSetup env_setup = new HiveTestEnvSetup();

  @Rule
  public TestRule methodRule = env_setup.getMethodRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    env_setup.getTestCtx().hiveConf.set("hive.security.authorization.scheduled.queries.supported", "true");
    env_setup.getTestCtx().hiveConf.setVar(ConfVars.USERS_IN_ADMIN_ROLE, System.getProperty("user.name"));

    IDriver driver = createDriver();
    dropTables(driver);
    String[] cmds = {
        // @formatter:off
        "create table tu(c int)",
        "create database asd",
        "create table asd.tasd(c int)",
        // @formatter:on
    };
    for (String cmd : cmds) {
      driver.run(cmd);
    }

    ScheduledQueryExecutionService.startScheduledQueryExecutorService(env_setup.getTestCtx().hiveConf);

  }

  @AfterClass
  public static void afterClass() throws Exception {
    IDriver driver = createDriver();
    dropTables(driver);
  }

  public static void dropTables(IDriver driver) throws Exception {
    String[] tables = { "tu" };
    for (String t : tables) {
      driver.run("drop table if exists " + t);
    }
  }

  @Test
  public void testSimpleCreate() throws ParseException, Exception {
    IDriver driver = createDriver();
    driver.run("set role admin");
    driver.run("create scheduled query simplecreate cron '* * * * * ? *' as select 1 from tu");
  }

  @Test(expected = CommandProcessorException.class)
  public void testNonExistentTable1() throws ParseException, Exception {
    IDriver driver = createDriver();
    driver.run("create scheduled query nonexist cron '* * * * * ? *' as select 1 from nonexist");
  }

  @Test(expected = CommandProcessorException.class)
  public void testNonExistentTable2() throws ParseException, Exception {
    IDriver driver = createDriver();
    driver.run("use asd");
    driver.run("create scheduled query nonexist2 cron '* * * * * ? *' as select 1 from tu");
  }

  @Test
  public void testCreateFromNonDefaultDatabase() throws ParseException, Exception {
    IDriver driver = createDriver();

    driver.run("set role admin");
    driver.run("use asd");

    driver.run("create table tt (a integer)");

    // the scheduled query may reference a table inside the current database
    driver.run("create scheduled query nonDef cron '* * * * * ? *' as select 1 from tt");

    try (CloseableObjectStore os = new CloseableObjectStore(env_setup.getTestCtx().hiveConf)) {
      Optional<MScheduledQuery> sq = os.getMScheduledQuery(new ScheduledQueryKey("nonDef", "hive"));
      assertTrue(sq.isPresent());
      assertEquals("select 1 from `asd`.`tt`", sq.get().toThrift().getQuery());
    }

  }

  @Test(expected = CommandProcessorException.class)
  public void testDoubleCreate() throws ParseException, Exception {
    IDriver driver = createDriver();
    driver.run("create scheduled query dc cron '* * * * * ? *' as select 1 from tu");
    driver.run("create scheduled query dc cron '* * * * * ? *' as select 1 from tu");
  }

  @Test
  public void testAlter() throws ParseException, Exception {
    IDriver driver = createDriver();

    driver.run("set role admin");
    driver.run("create scheduled query alter1 cron '* * * * * ? *' as select 1 from tu");
    driver.run("alter scheduled query alter1 executed as 'user3'");
    driver.run("alter scheduled query alter1 defined as select 22 from tu");

    try (CloseableObjectStore os = new CloseableObjectStore(env_setup.getTestCtx().hiveConf)) {
      Optional<MScheduledQuery> sq = os.getMScheduledQuery(new ScheduledQueryKey("alter1", "hive"));
      assertTrue(sq.isPresent());
      assertEquals("user3", sq.get().toThrift().getUser());
    }

  }

  @Test
  public void testImpersonation() throws ParseException, Exception {
    HiveConf conf = env_setup.getTestCtx().hiveConf;
    IDriver driver = createDriver();

    setupAuthorization();

    driver.run("create table t1 (a integer)");
    conf.set("user.name", "user1");
    driver.run("drop table t1");
  }

  private void setupAuthorization() {
    HiveConf conf = env_setup.getTestCtx().hiveConf;
    conf.set("hive.test.authz.sstd.hs2.mode", "true");
    conf.set("hive.security.authorization.manager",
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactoryForTest");
    conf.set("hive.security.authenticator.manager",
        "org.apache.hadoop.hive.ql.security.SessionStateConfigUserAuthenticator");
    conf.set("hive.security.authorization.enabled", "true");

  }

  static class CloseableObjectStore extends ObjectStore implements AutoCloseable {

    public CloseableObjectStore(HiveConf hiveConf) {
      super();
      super.setConf(hiveConf);
    }

    @Override
    public void close() throws Exception {
      super.shutdown();
    }
  }

  private static IDriver createDriver() {
    HiveConf conf = env_setup.getTestCtx().hiveConf;

    SessionState.start(conf);

    IDriver driver = DriverFactory.newDriver(conf);
    return driver;
  }
}
