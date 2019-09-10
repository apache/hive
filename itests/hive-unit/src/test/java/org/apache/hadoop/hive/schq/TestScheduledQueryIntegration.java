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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Optional;

import javax.jdo.PersistenceManager;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.PersistenceManagerProvider;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryKey;
import org.apache.hadoop.hive.metastore.model.MScheduledQuery;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.schq.ScheduledQueryExecutionService;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.testutils.HiveTestEnvSetup;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class TestScheduledQueryIntegration {

  @ClassRule
  public static HiveTestEnvSetup env_setup = new HiveTestEnvSetup();

  @Rule
  public TestRule methodRule = env_setup.getMethodRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
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
      int ret = driver.run(cmd).getResponseCode();
      assertEquals("Checking command success", 0, ret);
    }

    ScheduledQueryExecutionService.startScheduledQueryExecutorService(env_setup.getTestCtx().hiveConf);

  }

  @AfterClass
  public static void afterClass() throws Exception {

    IDriver driver = createDriver();
    dropTables(driver);
  }

  public static void dropTables(IDriver driver) throws Exception {
    String tables[] = { "tu" };
    for (String t : tables) {
      int ret = driver.run("drop table if exists " + t).getResponseCode();
      assertEquals("Checking command success", 0, ret);
    }
  }

  @Test
  public void testBasicImpersonation() throws ParseException, Exception {
    CommandProcessorResponse ret;

    setupAuthorization();

    ret = runAsUser("user1", "create table t1 (a integer)");
    assertEquals(0, ret.getResponseCode());

    ret = runAsUser("user2", "drop table t1");
    assertEquals(40000, ret.getResponseCode());
  }

  @Test
  public void testScheduledQueryExecutionImpersonation() throws ParseException, Exception {
    CommandProcessorResponse ret;

    env_setup.getTestCtx().hiveConf.setVar(HiveConf.ConfVars.HIVE_SCHEDULED_QUERIES_EXECUTOR_IDLE_SLEEP_TIME, "1s");
    env_setup.getTestCtx().hiveConf.setVar(HiveConf.ConfVars.HIVE_SCHEDULED_QUERIES_EXECUTOR_PROGRESS_REPORT_INTERVAL,
        "1s");
    setupAuthorization();

    try (ScheduledQueryExecutionService sservice =
        ScheduledQueryExecutionService.startScheduledQueryExecutorService(env_setup.getTestCtx().hiveConf)) {
  
      ret = runAsUser("user1",
          "create table junk0 as select 12 as i");

      ret = runAsUser("user1",
          "create scheduled query s1 cron '* * * * * ? *' defined as create table tx1 as select 12 as i");
      //      ret = runAsUser("user1",
      //          "create table tx1 as select 12 as i");
      assertEquals(0, ret.getResponseCode());

      Thread.sleep(20000);
  
      // table exists...
      ret = runAsUser("user1", "select * from tx1");
      assertEquals(0, ret.getResponseCode());

      // other user cant drop it
      ret = runAsUser("user2", "drop table tx1");
      assertEquals(40000, ret.getResponseCode());

      // but owner can
      ret = runAsUser("user1", "drop table tx1");
      assertEquals(0, ret.getResponseCode());

    }
  }

  private CommandProcessorResponse runAsUser(String userName, String sql) {
    HiveConf conf = env_setup.getTestCtx().hiveConf;
    conf.set("user.name", userName);
    IDriver driver = createDriver();
    CommandProcessorResponse ret;
    ret = driver.run(sql);
    return ret;
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
