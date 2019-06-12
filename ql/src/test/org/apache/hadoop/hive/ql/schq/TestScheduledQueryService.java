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
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryKey;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollResponse;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryProgressInfo;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.OperatorStatsReaderHook;
import org.apache.hive.testutils.HiveTestEnvSetup;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class TestScheduledQueryService {

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
        // @formatter:on
    };
    for (String cmd : cmds) {
      int ret = driver.run(cmd).getResponseCode();
      assertEquals("Checking command success", 0, ret);
    }
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

  private int getNumRowsReturned(IDriver driver, String query) throws Exception {
    int ret = driver.run(query).getResponseCode();
    assertEquals("Checking command success", 0, ret);
    FetchTask ft = driver.getFetchTask();
    List res = new ArrayList();
    if (ft == null) {
      return 0;
    }
    ft.fetch(res);
    return res.size();
  }

  public static class MockScheduledQueryService implements IScheduledQueryService {
    int id = 0;
    private String stmt;

    public MockScheduledQueryService(String string) {
      stmt = string;
    }
    
    @Override
    public ScheduledQueryPollResponse scheduledQueryPoll(String namespace) {

      ScheduledQueryPollResponse r = new ScheduledQueryPollResponse();
      r.setExecutionId(id++);
      r.setQuery(stmt);
      r.setScheduleKey(new ScheduledQueryKey("sch1", namespace));
      if (id >= 1) {
        return r;
      } else {
        return null;
      }
    }

    @Override
    public void scheduledQueryProgress(ScheduledQueryProgressInfo info) {
      System.out.printf("%ld, state: %s, error: %s", info.getScheduledExecutionId(), info.getState(),
          info.getErrorMessage());
    }


  }

  @Test
  public void testScheduledQueryExecution() throws ParseException, Exception {
    IDriver driver = createDriver();

    ExecutorService executor =
        Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("SchQ %d").build());
    HiveConf conf = env_setup.getTestCtx().hiveConf;
    ScheduledQueryExecutionContext ctx = new ScheduledQueryExecutionContext(executor, conf);
    ctx.schedulerService = new MockScheduledQueryService("insert into tu values(1),(2),(3),(4),(5)");
    ScheduledQueryExecutionService sQ = new ScheduledQueryExecutionService(ctx);

    Thread.sleep(5000);
    executor.shutdown();
    executor.awaitTermination(2, TimeUnit.SECONDS);

    String query = "select 1 from tu";

    int nr = getNumRowsReturned(driver, query);
    assertThat(nr, Matchers.greaterThan(10));

  }

  private static IDriver createDriver() {
    HiveConf conf = env_setup.getTestCtx().hiveConf;

    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    HiveConf.setVar(conf, HiveConf.ConfVars.POSTEXECHOOKS, OperatorStatsReaderHook.class.getName());
    SessionState.start(conf);

    IDriver driver = DriverFactory.newDriver(conf);
    return driver;
  }
}
