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
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.OperatorStatsReaderHook;
import org.apache.hive.testutils.HiveTestEnvSetup;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class TestScheduledQuery {

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

  private PlanMapper getMapperForQuery(IDriver driver, String query) {
    int ret = driver.run(query).getResponseCode();
    assertEquals("Checking command success", 0, ret);
    PlanMapper pm0 = driver.getContext().getPlanMapper();
    return pm0;
  }

  @Test
  public void testScheduledQ() throws ParseException {
    IDriver driver = createDriver();

    ExecutorService executor =
        Executors.newCachedThreadPool(new ThreadFactoryBuilder().setDaemon(true).setNameFormat("SchQ %d").build());
    ScheduledQueryExecutionContext ctx = new ScheduledQueryExecutionContext(executor);
    ctx.schedulerService = new ScheduledQueryX();
    ScheduledQueryExecutionService sQ = new ScheduledQueryExecutionService(ctx);

    String query = "select 1 from tu";

    PlanMapper pm = getMapperForQuery(driver, query);
    List<FilterOperator> fos = pm.getAll(FilterOperator.class);
    // the same operator is present 2 times
    assertEquals(4, fos.size());


  }

  private static IDriver createDriver() {
    HiveConf conf = env_setup.getTestCtx().hiveConf;

    conf.setBoolVar(ConfVars.HIVE_QUERY_REEXECUTION_ENABLED, true);
    conf.setBoolVar(ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    conf.setBoolVar(ConfVars.HIVE_QUERY_REEXECUTION_ALWAYS_COLLECT_OPERATOR_STATS, true);
    conf.setVar(ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES, "reoptimize");
    conf.set("zzz", "1");
    conf.set("reexec.overlay.zzz", "2000");
    //
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    HiveConf.setVar(conf, HiveConf.ConfVars.POSTEXECHOOKS, OperatorStatsReaderHook.class.getName());
    SessionState.start(conf);

    IDriver driver = DriverFactory.newDriver(conf);
    return driver;
  }
}
