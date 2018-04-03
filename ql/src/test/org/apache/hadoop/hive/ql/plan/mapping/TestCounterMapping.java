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

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.signature.OpTreeSignature;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.plan.mapper.EmptyStatsSource;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper.EquivGroup;
import org.apache.hadoop.hive.ql.plan.mapper.StatsSources;
import org.apache.hadoop.hive.ql.reexec.ReExecDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.OperatorStats;
import org.apache.hadoop.hive.ql.stats.OperatorStatsReaderHook;
import org.apache.hive.testutils.HiveTestEnvSetup;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class TestCounterMapping {

  @ClassRule
  public static HiveTestEnvSetup env_setup = new HiveTestEnvSetup();

  @Rule
  public TestRule methodRule = env_setup.getMethodRule();

  static Comparator<Operator<?>> OPERATOR_ID_COMPARATOR = new Comparator<Operator<?>>() {

    @Override
    public int compare(Operator<?> o1, Operator<?> o2) {
      Long id1 = Long.valueOf(o1.getIdentifier());
      Long id2 = Long.valueOf(o2.getIdentifier());
      int c0 = Objects.compare(o1.getOperatorName(), o2.getOperatorName(), Comparator.naturalOrder());
      if (c0 != 0) {
        return c0;
      }
      return Long.compare(id1, id2);
    }
  };


  @BeforeClass
  public static void beforeClass() throws Exception {
    IDriver driver = createDriver();
    dropTables(driver);
    String cmds[] = {
        // @formatter:off
        "create table s (x int)",
        "insert into s values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)",
        "create table tu(id_uv int,id_uw int,u int)",
        "create table tv(id_uv int,v int)",
        "create table tw(id_uw int,w int)",

        "from s\n" +
        "insert overwrite table tu\n" +
        "        select x,x,x\n" +
        "        where x<=6 or x=10\n" +
        "insert overwrite table tv\n" +
        "        select x,x\n" +
        "        where x<=3 or x=10\n" +
        "insert overwrite table tw\n" +
        "        select x,x\n" +
        "",
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
    String tables[] = { "s", "tu", "tv", "tw" };
    for (String t : tables) {
      int ret = driver.run("drop table if exists " + t).getResponseCode();
      assertEquals("Checking command success", 0, ret);
    }
  }

  private PlanMapper getMapperForQuery(IDriver driver, String query) {
    int ret;
    ret = driver.run(query).getResponseCode();
    assertEquals("Checking command success", 0, ret);
    PlanMapper pm0 = driver.getContext().getPlanMapper();
    return pm0;
  }

  @Test
  public void testUsageOfRuntimeInfo() throws ParseException {
    IDriver driver = createDriver();
    String query = "select sum(u) from tu where u>1";
    PlanMapper pm1 = getMapperForQuery(driver, query);

    List<FilterOperator> filters1 = pm1.getAll(FilterOperator.class);
    filters1.sort(OPERATOR_ID_COMPARATOR.reversed());
    FilterOperator filter1 = filters1.get(0);

    driver = createDriver();
    ((ReExecDriver) driver).setStatsSource(StatsSources.getStatsSourceContaining(EmptyStatsSource.INSTANCE, pm1));

    PlanMapper pm2 = getMapperForQuery(driver, query);

    List<FilterOperator> filters2 = pm2.getAll(FilterOperator.class);
    filters2.sort(OPERATOR_ID_COMPARATOR.reversed());
    FilterOperator filter2 = filters2.get(0);

    assertEquals("original check", 7, filter1.getStatistics().getNumRows());
    assertEquals("optimized check", 6, filter2.getStatistics().getNumRows());

  }

  @Test
  public void testInConversion() throws ParseException {
    String query =
        "explain select sum(id_uv) from tu where u in (1,2) group by u";

    HiveConf conf = env_setup.getTestCtx().hiveConf;
    conf.setIntVar(ConfVars.HIVEPOINTLOOKUPOPTIMIZERMIN, 10);
    IDriver driver = createDriver();

    PlanMapper pm = getMapperForQuery(driver, query);
    List<FilterOperator> fos = pm.getAll(FilterOperator.class);
    OpTreeSignature filterSig = pm.lookup(OpTreeSignature.class, fos.get(0));
    Object pred = filterSig.getSig().getSigMap().get("getPredicateString");

    assertEquals("((u = 1) or (u = 2)) (type: boolean)", pred);

  }

  @Test
  public void testBreakupAnd() throws ParseException {
    String query =
        "explain select sum(id_uv) from tu where u=1  and (u=2 or u=1) group by u";

    IDriver driver = createDriver();
    PlanMapper pm = getMapperForQuery(driver, query);
    List<FilterOperator> fos = pm.getAll(FilterOperator.class);
    OpTreeSignature filterSig = pm.lookup(OpTreeSignature.class, fos.get(0));
    Object pred = filterSig.getSig().getSigMap().get("getPredicateString");
    assertEquals("(u = 1) (type: boolean)", pred);
  }

  @Test
  public void testBreakupAnd2() throws ParseException {
    String query =
        "explain select sum(id_uv) from tu where u in (1,2,3) and u=2 and u=2 and 2=u group by u";

    IDriver driver = createDriver();
    PlanMapper pm = getMapperForQuery(driver, query);
    List<FilterOperator> fos = pm.getAll(FilterOperator.class);
    OpTreeSignature filterSig = pm.lookup(OpTreeSignature.class, fos.get(0));
    Object pred = filterSig.getSig().getSigMap().get("getPredicateString");
    assertEquals("(u = 2) (type: boolean)", pred);

  }


  @Test
  @Ignore("needs HiveFilter mapping")
  public void testMappingJoinLookup() throws ParseException {
    IDriver driver = createDriver();

    PlanMapper pm0 = getMapperForQuery(driver, "select sum(tu.id_uv),sum(u) from tu join tv on (tu.id_uv = tv.id_uv) where u>1 and v>1");

    Iterator<EquivGroup> itG = pm0.iterateGroups();
    int checkedOperators = 0;
    while (itG.hasNext()) {
      EquivGroup g = itG.next();
      List<HiveFilter> hfs = g.getAll(HiveFilter.class);
      List<OperatorStats> oss = g.getAll(OperatorStats.class);
      List<FilterOperator> fos = g.getAll(FilterOperator.class);

      if (fos.size() > 0 && oss.size() > 0) {
        if (hfs.size() == 0) {
          fail("HiveFilter is not connected?");
        }
        OperatorStats os = oss.get(0);
        if (!(os.getOutputRecords() == 3 || os.getOutputRecords() == 6)) {
          fail("nonexpected number of records produced");
        }
        checkedOperators++;
      }
    }
    assertEquals(2, checkedOperators);
  }

  private static IDriver createDriver() {
    HiveConf conf = env_setup.getTestCtx().hiveConf;
    conf.setBoolVar(ConfVars.HIVE_QUERY_REEXECUTION_ENABLED, true);
    conf.setBoolVar(ConfVars.HIVE_QUERY_REEXECUTION_ALWAYS_COLLECT_OPERATOR_STATS, true);
    conf.set("hive.auto.convert.join", "false");
    conf.set("hive.optimize.ppd", "false");

    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    //    conf.setVar(HiveConf.ConfVars.SEMANTIC_ANALYZER_HOOK, CheckInputReadEntityDirect.class.getName());
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    HiveConf.setVar(conf, HiveConf.ConfVars.POSTEXECHOOKS, OperatorStatsReaderHook.class.getName());
    SessionState.start(conf);

    IDriver driver = DriverFactory.newDriver(conf);
    return driver;
  }


}
