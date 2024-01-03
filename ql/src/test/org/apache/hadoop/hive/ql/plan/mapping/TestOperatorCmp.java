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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.exec.CommonMergeJoinOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.optimizer.signature.TestOperatorSignature;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.OperatorStatsReaderHook;
import org.apache.hive.testutils.HiveTestEnvSetup;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class TestOperatorCmp {

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
    IDriver driver = createDriver();
    dropTables(driver);
  }

  public static void dropTables(IDriver driver) throws Exception {
    String tables[] = { "tu", "tv", "tw" };
    for (String t : tables) {
      driver.run("drop table if exists " + t);
    }
  }

  private PlanMapper getMapperForQuery(IDriver driver, String query) throws CommandProcessorException {
    driver.run(query);
    PlanMapper pm0 = driver.getContext().getPlanMapper();
    return pm0;
  }

  @Test
  public void testUnrelatedFiltersAreNotMatched0() throws ParseException, CommandProcessorException {
    IDriver driver = createDriver();
    String query = "select u from tu where id_uv = 1 union all select v from tv where id_uv = 1";

    PlanMapper pm = getMapperForQuery(driver, query);
    List<FilterOperator> fos = pm.getAll(FilterOperator.class);
    // the same operator is present 2 times
    assertEquals(4, fos.size());

    int cnt = 0;
    for (int i = 0; i < 3; i++) {
      for (int j = i + 1; j < 4; j++) {
        if (compareOperators(fos.get(i), fos.get(j))) {
          cnt++;
        }
      }
    }
    assertEquals(2, cnt);

  }

  @Test
  public void testUnrelatedFiltersAreNotMatched1() throws ParseException, CommandProcessorException {
    IDriver driver = createDriver();
    PlanMapper pm0 = getMapperForQuery(driver, "select u from tu where id_uv = 1 group by u");
    PlanMapper pm1 = getMapperForQuery(driver, "select v from tv where id_uv = 1 group by v");
    List<FilterOperator> fos0 = pm0.getAll(FilterOperator.class);
    List<FilterOperator> fos1 = pm1.getAll(FilterOperator.class);
    assertEquals(1, fos0.size());
    assertEquals(1, fos1.size());

    assertFalse("logicalEquals", compareOperators(fos0.get(0), fos1.get(0)));
  }

  @Test
  public void testDifferentFiltersAreNotMatched() throws ParseException, CommandProcessorException {
    IDriver driver = createDriver();
    PlanMapper pm0 = getMapperForQuery(driver, "select u from tu where id_uv = 1 group by u");
    PlanMapper pm1 = getMapperForQuery(driver, "select u from tu where id_uv = 2 group by u");

    assertHelper(AssertHelperOp.NOT_SAME, pm0, pm1, FilterOperator.class);

  }

  @Test
  public void testSameFiltersMatched() throws ParseException, Exception {
    IDriver driver = createDriver();
    PlanMapper pm0 = getMapperForQuery(driver, "select u from tu where id_uv = 1 group by u");
    PlanMapper pm1 = getMapperForQuery(driver, "select u from tu where id_uv = 1 group by u");

    assertHelper(AssertHelperOp.SAME, pm0, pm1, FilterOperator.class);
    assertHelper(AssertHelperOp.SAME, pm0, pm1, TableScanOperator.class);
  }

  @Test
  public void testSameJoinMatched() throws ParseException, Exception {
    IDriver driver = createDriver();
    PlanMapper pm0 =
        getMapperForQuery(driver, "select u,v from tu,tv where tu.id_uv = tv.id_uv and u>1 and v<10 group by u,v");
    PlanMapper pm1 =
        getMapperForQuery(driver, "select u,v from tu,tv where tu.id_uv = tv.id_uv and u>1 and v<10 group by u,v");

    assertHelper(AssertHelperOp.SAME, pm0, pm1, CommonMergeJoinOperator.class);
    assertHelper(AssertHelperOp.SAME, pm0, pm1, JoinOperator.class);
    //    assertHelper(AssertHelperOp.SAME, pm0, pm1, TableScanOperator.class);
  }

  enum AssertHelperOp {
    SAME, NOT_SAME
  };

  private <T extends Operator<?>> void assertHelper(AssertHelperOp same,PlanMapper pm0, PlanMapper pm1, Class<T> clazz) {
    List<T> fos0 = pm0.getAll(clazz);
    List<T> fos1 = pm1.getAll(clazz);
    assertEquals(1, fos0.size());
    assertEquals(1, fos1.size());

    T opL = fos0.get(0);
    T opR = fos1.get(0);
    if (same == AssertHelperOp.SAME) {
      assertTrue(clazz + " " + same, compareOperators(opL, opR));
      TestOperatorSignature.checkEquals(opL, opR);
      TestOperatorSignature.checkTreeEquals(opL, opR);
    } else {
      assertFalse(clazz + " " + same, compareOperators(opL, opR));
      TestOperatorSignature.checkNotEquals(opL, opR);
      TestOperatorSignature.checkTreeNotEquals(opL, opR);
    }
  }

  private boolean compareOperators(Operator<?> opL, Operator<?> opR) {
    return opL.logicalEqualsTree(opR);
  }

  private static IDriver createDriver() {
    HiveConf conf = env_setup.getTestCtx().hiveConf;

    conf.setBoolVar(ConfVars.HIVE_OPT_PPD, false);
    conf.setBoolVar(ConfVars.HIVE_QUERY_REEXECUTION_ENABLED, true);
    conf.setBoolVar(ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    conf.setBoolVar(ConfVars.HIVE_QUERY_REEXECUTION_ALWAYS_COLLECT_OPERATOR_STATS, true);
    conf.setVar(ConfVars.HIVE_CBO_FALLBACK_STRATEGY, "NEVER");
    conf.setVar(ConfVars.HIVE_QUERY_REEXECUTION_STRATEGIES, "reoptimize");
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
