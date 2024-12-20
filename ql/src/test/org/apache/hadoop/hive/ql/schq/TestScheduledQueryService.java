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

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.QueryState;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryKey;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollResponse;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryProgressInfo;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.scheduled.IScheduledQueryMaintenanceService;
import org.apache.hadoop.hive.ql.scheduled.ScheduledQueryExecutionContext;
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

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class TestScheduledQueryService {

  @ClassRule
  public static HiveTestEnvSetup env_setup = new HiveTestEnvSetup();

  @Rule
  public TestRule methodRule = env_setup.getMethodRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    env_setup.getTestCtx().hiveConf.setVar(HiveConf.ConfVars.HIVE_SCHEDULED_QUERIES_EXECUTOR_IDLE_SLEEP_TIME, "1s");
    env_setup.getTestCtx().hiveConf.setVar(HiveConf.ConfVars.HIVE_SCHEDULED_QUERIES_EXECUTOR_PROGRESS_REPORT_INTERVAL,
        "1s");

    IDriver driver = createDriver();
    dropTables(driver);
    String cmds[] = {
        // @formatter:off
        "create table tu(c int)",
        // @formatter:on
    };
    for (String cmd : cmds) {
      driver.run(cmd);
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
      driver.run("drop table if exists " + t);
    }
  }

  private int getNumRowsReturned(IDriver driver, String query) throws Exception {
    driver.run(query);
    FetchTask ft = driver.getFetchTask();
    List<?> res = new ArrayList<>();
    if (ft == null) {
      return 0;
    }
    ft.fetch(res);
    return res.size();
  }

  @Test
  public void testScheduledQueryExecution() throws ParseException, Exception {
    IDriver driver = createDriver();

    ExecutorService executor =
        Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("SchQ %d").build());
    HiveConf conf = env_setup.getTestCtx().hiveConf;
    MockScheduledQueryService qService = new MockScheduledQueryService("insert into tu values(1),(2),(3),(4),(5)");
    ScheduledQueryExecutionContext ctx = new ScheduledQueryExecutionContext(executor, conf, qService);
    try (ScheduledQueryExecutionService sQ = ScheduledQueryExecutionService.startScheduledQueryExecutorService(ctx)) {
      // Wait for the scheduled query to finish. Hopefully 30 seconds should be more than enough.
      SessionState.getConsole().logInfo("Waiting for query execution to finish ...");
      synchronized (qService.notifier) {
        qService.notifier.wait(30000);
      }
      SessionState.getConsole().logInfo("Done waiting for query execution!");
    }

    assertThat(qService.lastProgressInfo.isSetExecutorQueryId(), is(true));
    assertThat(qService.lastProgressInfo.getExecutorQueryId(),
        Matchers.containsString(ctx.executorHostName + "/"));
    int nr = getNumRowsReturned(driver, "select 1 from tu");
    assertThat(nr, Matchers.equalTo(5));

  }

  private static IDriver createDriver() {
    HiveConf conf = env_setup.getTestCtx().hiveConf;

    SessionState.start(conf);

    IDriver driver = DriverFactory.newDriver(conf);
    return driver;
  }
}
