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
package org.apache.hadoop.hive.ql.plan.mapping;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.apache.hadoop.hive.ql.plan.mapper.StatsSources;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper.EquivGroup;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.OperatorStats;
import org.apache.hadoop.hive.ql.stats.OperatorStatsReaderHook;
import org.apache.hive.testutils.HiveTestEnvSetup;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TestRule;

public class TestReOptimization {

  @ClassRule
  public static HiveTestEnvSetup env_setup = new HiveTestEnvSetup();

  @Rule
  public TestRule methodRule = env_setup.getMethodRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    IDriver driver = createDriver("");
    dropTables(driver);
    String[] cmds = {
        // @formatter:off
        "create table tu(id_uv int,id_uw int,u int)",
        "create table tv(id_uv int,v int)",
        "create table tw(id_uw int,w int)",

        "insert into tu values (10,10,10),(1,1,1),(2,2,2),(3,3,3),(4,4,4),(5,5,5),(6,6,6)",
        "insert into tv values (10,10),(1,1),(2,2),(3,3)",
        "insert into tw values (10,10),(1,1),(2,2),(3,3),(4,4),(5,5),(6,6),(7,7),(8,8),(9,9)",
        // @formatter:on
    };
    for (String cmd : cmds) {
      driver.run(cmd);
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    IDriver driver = createDriver("");
    dropTables(driver);
  }

  @After
  public void after() {
    StatsSources.clearGlobalStats();
  }

  public static void dropTables(IDriver driver) throws Exception {
    String[] tables = new String[] {"tu", "tv", "tw" };
    for (String t : tables) {
      driver.run("drop table if exists " + t);
    }
  }

  private PlanMapper getMapperForQuery(IDriver driver, String query) {
    try {
      driver.run(query);
    } catch (CommandProcessorException e) {
      throw new RuntimeException("running the query " + query + " was not successful");
    }
    PlanMapper pm0 = driver.getContext().getPlanMapper();
    return pm0;
  }

  @Test
  public void testStatsAreSetInReopt() throws Exception {
    IDriver driver = createDriver("overlay,reoptimize");
    String query = "select assert_true_oom(${hiveconf:zzz} > sum(u*v))"
        + " from tu join tv on (tu.id_uv=tv.id_uv)"
        + " where u<10 and v>1";

    PlanMapper pm = getMapperForQuery(driver, query);
    Iterator<EquivGroup> itG = pm.iterateGroups();
    int checkedOperators = 0;
    while (itG.hasNext()) {
      EquivGroup g = itG.next();
      List<FilterOperator> fos = g.getAll(FilterOperator.class);
      List<OperatorStats> oss = g.getAll(OperatorStats.class);
      // FIXME: oss seems to contain duplicates

      if (fos.size() > 0 && oss.size() > 0) {
        fos.sort(TestCounterMapping.OPERATOR_ID_COMPARATOR.reversed());

        FilterOperator fo = fos.get(0);
        OperatorStats os = oss.get(0);

        Statistics stats = fo.getStatistics();
        assertEquals(os.getOutputRecords(), stats.getNumRows());

        if (!(os.getOutputRecords() == 3 || os.getOutputRecords() == 6)) {
          fail("nonexpected number of records produced");
        }
        checkedOperators++;
      }
    }
    assertEquals(2, checkedOperators);
  }

  @Test
  public void testReExecutedIfMapJoinError() throws Exception {

    IDriver driver = createDriver("overlay,reoptimize");
    String query =
        "select assert_true_oom(${hiveconf:zzz}>sum(1)) from tu join tv on (tu.id_uv=tv.id_uv) where u<10 and v>1";
    getMapperForQuery(driver, query);

  }

  @Rule
  public ExpectedException exceptionRule = ExpectedException.none();

  @Test
  public void testNotReExecutedIfAssertionError() throws Exception {
    IDriver driver = createDriver("reoptimize");
    String query =
        "select assert_true(${hiveconf:zzz}>sum(1)) from tu join tv on (tu.id_uv=tv.id_uv) where u<10 and v>1";

    exceptionRule.expect(RuntimeException.class);
    exceptionRule.expectMessage("running the query " + query + " was not successful");

    getMapperForQuery(driver, query);
  }

  @Test
  public void testStatCachingQuery() throws Exception {
    HiveConf conf = env_setup.getTestCtx().hiveConf;
    conf.setVar(ConfVars.HIVE_QUERY_REEXECUTION_STATS_PERSISTENCE, "query");
    conf.setBoolVar(ConfVars.HIVE_QUERY_REEXECUTION_ALWAYS_COLLECT_OPERATOR_STATS, true);

    checkRuntimeStatsReuse(false, false, false);
  }

  @Test
  public void testStatCachingHS2() throws Exception {
    HiveConf conf = env_setup.getTestCtx().hiveConf;
    conf.setVar(ConfVars.HIVE_QUERY_REEXECUTION_STATS_PERSISTENCE, "hiveserver");
    conf.setBoolVar(ConfVars.HIVE_QUERY_REEXECUTION_ALWAYS_COLLECT_OPERATOR_STATS, true);

    checkRuntimeStatsReuse(true, true, false);
  }

  @Test
  public void testStatCachingMetaStore() throws Exception {
    HiveConf conf = env_setup.getTestCtx().hiveConf;
    conf.setVar(ConfVars.HIVE_QUERY_REEXECUTION_STATS_PERSISTENCE, "metastore");
    conf.setBoolVar(ConfVars.HIVE_QUERY_REEXECUTION_ALWAYS_COLLECT_OPERATOR_STATS, true);

    checkRuntimeStatsReuse(true, true, true);
  }

  private void checkRuntimeStatsReuse(
      boolean expectInSameSession,
      boolean expectNewHs2Session,
      boolean expectHs2Instance) throws CommandProcessorException {
    {
      // same session
      IDriver driver = createDriver("reoptimize");
      checkUsageOfRuntimeStats(driver, false);
      driver = DriverFactory.newDriver(env_setup.getTestCtx().hiveConf);
      checkUsageOfRuntimeStats(driver, expectInSameSession);
    }
    {
      // new session
      IDriver driver = createDriver("reoptimize");
      checkUsageOfRuntimeStats(driver, expectNewHs2Session);
    }
    StatsSources.clearGlobalStats();
    {
      // new hs2 instance session
      IDriver driver = createDriver("reoptimize");
      // loading of metastore stats is async; execute a simple to ensure they are loaded
      driver.run("select count(*) from tu group by id_uv");
      checkUsageOfRuntimeStats(driver, expectHs2Instance);
    }
  }

  @SuppressWarnings("rawtypes")
  private void checkUsageOfRuntimeStats(IDriver driver, boolean expected) throws CommandProcessorException {
    String query = "select sum(u) from tu join tv on (tu.id_uv=tv.id_uv) where u<10 and v>1";
    PlanMapper pm = getMapperForQuery(driver, query);
    assertEquals(1, driver.getContext().getExecutionIndex());
    List<CommonJoinOperator> allJoin = pm.getAll(CommonJoinOperator.class);
    CommonJoinOperator join = allJoin.iterator().next();
    Statistics joinStat = join.getStatistics();

    assertEquals("expectation of the usage of runtime stats doesn't match", expected,
        joinStat.isRuntimeStats());
  }

  @Test
  public void testExplainSupport() throws Exception {

    IDriver driver = createDriver("overlay,reoptimize");
    String query = "explain reoptimization select 1 from tu join tv on (tu.id_uv=tv.id_uv) where u<10 and v>1";
    getMapperForQuery(driver, query);
    List<String> res = new ArrayList<>();
    List<String> res1 = new ArrayList<>();
    while (driver.getResults(res1)) {
      res.addAll(res1);
    }

    assertEquals("2FIL", 2, res.stream().filter(line -> line.contains("FIL_")).count());
    assertEquals("2FIL(runtime)", 2,
        res.stream().filter(line -> line.contains("FIL") && line.contains("runtime")).count());

  }

  @Test
  public void testReOptimizationCanSendBackStatsToCBO() throws Exception {
    IDriver driver = createDriver("overlay,reoptimize");
    // @formatter:off
    String query="select assert_true_oom(${hiveconf:zzz} > sum(u*v*w)) from tu\n" +
    "        join tv on (tu.id_uv=tv.id_uv)\n" +
    "        join tw on (tu.id_uw=tw.id_uw)\n" +
    "        where w>9 and u>1 and v>3";
    // @formatter:on
    PlanMapper pm = getMapperForQuery(driver, query);

    Iterator<EquivGroup> itG = pm.iterateGroups();
    int checkedOperators = 0;
    while (itG.hasNext()) {
      EquivGroup g = itG.next();
      List<FilterOperator> fos = g.getAll(FilterOperator.class);
      List<OperatorStats> oss = g.getAll(OperatorStats.class);
      List<HiveFilter> hfs = g.getAll(HiveFilter.class);

      if (fos.size() > 0 && oss.size() > 0 && hfs.size() > 0) {
        fos.sort(TestCounterMapping.OPERATOR_ID_COMPARATOR.reversed());

        HiveFilter hf = hfs.get(0);
        FilterOperator fo = fos.get(0);
        OperatorStats os = oss.get(0);

        long cntFilter = RelMetadataQuery.instance().getRowCount(hf).longValue();
        if (fo.getStatistics() != null) {
          // in case the join order is changed the subTree-s are not matching anymore because an RS is present in the condition
          // assertEquals(os.getOutputRecords(), fo.getStatistics().getNumRows());
        }
        assertEquals(os.getOutputRecords(), cntFilter);

        checkedOperators++;
      }
    }
    assertEquals(3, checkedOperators);

  }

  private static IDriver createDriver(String strategies) {
    HiveConf conf = env_setup.getTestCtx().hiveConf;

    conf.setBoolVar(ConfVars.HIVE_QUERY_REEXECUTION_ENABLED, true);
    conf.setBoolVar(ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    conf.setVar(ConfVars.HIVE_CBO_FALLBACK_STRATEGY, "NEVER");
    conf.setVar(ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES, strategies);
    conf.setBoolVar(ConfVars.HIVE_EXPLAIN_USER, true);
    conf.set("zzz", "1");
    conf.set("reexec.overlay.zzz", "2000");
    //
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    HiveConf.setVar(conf, HiveConf.ConfVars.POST_EXEC_HOOKS, OperatorStatsReaderHook.class.getName());
    SessionState.start(conf);

    IDriver driver = DriverFactory.newDriver(conf);
    return driver;
  }


}
