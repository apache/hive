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
package org.apache.hadoop.hive.schq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.scheduled.ScheduledQueryExecutionService;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.testutils.HiveTestEnvSetup;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

/**
 * ScheduledQuery integration test.
 *
 * Checks for more complex scenarios; like impersonation works when scheduled queries are executed.
 */
public class TestScheduledQueryIntegration {

  @ClassRule
  public static HiveTestEnvSetup envSetup = new HiveTestEnvSetup();

  @Rule
  public TestRule methodRule = envSetup.getMethodRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    envSetup.getTestCtx().hiveConf.set("hive.users.in.admin.role", "user1");
    IDriver driver = createDriver();
    dropTables(driver);
    String cmds[] = {
        // @formatter:off
        "create table tu(c int)",
        "create database asd",
        "create table asd.tasd(c int)",
        // @formatter:on
    };
    for (String cmd : cmds) {
      driver.run(cmd);
    }

  }

  @AfterClass
  public static void afterClass() throws Exception {
    envSetup.getTestCtx().hiveConf.set("hive.test.authz.sstd.hs2.mode", "false");
    envSetup.getTestCtx().hiveConf.set("hive.security.authorization.enabled", "false");
    envSetup.getTestCtx().hiveConf.set("hive.users.in.admin.role", "user1");

    IDriver driver = createDriver();
    dropTables(driver);
  }

  public static void dropTables(IDriver driver) throws Exception {
    String tables[] = { "tu" };
    for (String t : tables) {
      driver.run("drop table if exists " + t);
    }
  }

  @Test
  public void testBasicImpersonation() throws ParseException, Exception {

    setupAuthorization();

    runAsUser("user1", "create table t1 (a integer)");
    try {
      runAsUser("user2", "drop table t1");
      fail("Exception expected");
    } catch (CommandProcessorException cpe) {
      assertThat(cpe.getMessage(), Matchers.containsString("HiveAccessControlException Permission denied"));
    }
    runAsUser("user1", "drop table t1");
  }

  @Test
  public void testScheduledQueryExecutionImpersonation() throws ParseException, Exception {
    envSetup.getTestCtx().hiveConf.setVar(HiveConf.ConfVars.HIVE_SCHEDULED_QUERIES_EXECUTOR_IDLE_SLEEP_TIME, "1s");
    envSetup.getTestCtx().hiveConf.setVar(HiveConf.ConfVars.HIVE_SCHEDULED_QUERIES_EXECUTOR_PROGRESS_REPORT_INTERVAL,
        "1s");
    setupAuthorization();

    try (ScheduledQueryExecutionService schqS =
        ScheduledQueryExecutionService.startScheduledQueryExecutorService(envSetup.getTestCtx().hiveConf)) {

      runAsUser("user1",
          "create scheduled query s1 cron '* * * * * ? *' defined as create table tx1 as select 12 as i", true);

      Thread.sleep(20000);

    }

    // table exists - and owner is able to select from it
    runAsUser("user1", "select * from tx1");

    // other user can't drop it
    try {
      runAsUser("user2", "drop table tx1");
      fail("should have failed");
    } catch (CommandProcessorException cpe) {
      assertEquals(40000, cpe.getResponseCode());
    }

    // but the owner can drop it
    runAsUser("user1", "drop table tx1");
  }


  private CommandProcessorResponse runAsUser(String userName, String sql) throws CommandProcessorException {
    return runAsUser(userName, sql, false);
  }

  private CommandProcessorResponse runAsUser(String userName, String sql, boolean asAdmin)
      throws CommandProcessorException {
    HiveConf conf = envSetup.getTestCtx().hiveConf;
    conf.set("user.name", userName);
    SessionState.get().setAuthenticator(null);
    SessionState.get().setAuthorizer(null);
    try (IDriver driver = createDriver()) {
      if (asAdmin) {
        driver.run("set role admin");
      }
      return driver.run(sql);
    }
  }

  private void setupAuthorization() {
    HiveConf conf = envSetup.getTestCtx().hiveConf;
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
    HiveConf conf = envSetup.getTestCtx().hiveConf;

    String userName = conf.get("user.name");
    SessionState ss = new SessionState(conf, userName);
    SessionState.start(ss);

    IDriver driver = DriverFactory.newDriver(conf);
    return driver;
  }
}
