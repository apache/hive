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
import static org.junit.Assert.assertThat;

import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.testutils.HiveTestEnvSetup;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class TestStatEstimations {

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
        "create table t2(a integer, b string) STORED AS ORC",
        "insert into t2 values (1, 'A1'),(2, 'A2'),(3, 'A3'),(4, 'A4'),(5, 'A5')," +
                              "(6, 'B1'),(7, 'B2'),(8, 'B3'),(9, 'B4'),(10, 'B5')",
        "analyze table t2 compute statistics for columns"
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
    String tables[] = {"t2" };
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
  public void testFilterStringIn() throws ParseException, CommandProcessorException {
    IDriver driver = createDriver();
    String query = "explain select a from t2 where b IN ('A3', 'ABC', 'AXZ') order by a";

    PlanMapper pm = getMapperForQuery(driver, query);
    List<FilterOperator> fos = pm.getAll(FilterOperator.class);
    // the same operator is present 2 times
    fos.sort(TestCounterMapping.OPERATOR_ID_COMPARATOR.reversed());
    FilterOperator fop = fos.get(0);

    // any estimation near 1 is ok...currently 1
    assertEquals(1, fop.getStatistics().getNumRows());
  }

  // FIXME: right now not in is transformed into AND( NE(...) , NE(...) )
  @Ignore
  @Test
  public void testFilterStringNotIn() throws CommandProcessorException {
    IDriver driver = createDriver();
    String query = "explain select a from t2 where b NOT IN ('XXX', 'UUU') order by a";

    PlanMapper pm = getMapperForQuery(driver, query);
    List<FilterOperator> fos = pm.getAll(FilterOperator.class);
    // the same operator is present 2 times
    fos.sort(TestCounterMapping.OPERATOR_ID_COMPARATOR.reversed());
    assertEquals(1, fos.size());
    FilterOperator fop = fos.get(0);

    // any estimation near 10 is ok...currently 10
    assertEquals(10, fop.getStatistics().getNumRows());
  }

  @Test
  public void testFilterIntIn() throws ParseException, CommandProcessorException {
    IDriver driver = createDriver();
    String query = "explain select a from t2 where a IN (-1,0,1,2,10,20,30,40) order by a";

    PlanMapper pm = getMapperForQuery(driver, query);
    List<FilterOperator> fos = pm.getAll(FilterOperator.class);
    // the same operator is present 2 times
    fos.sort(TestCounterMapping.OPERATOR_ID_COMPARATOR.reversed());
    assertThat(fos.size(), Matchers.greaterThanOrEqualTo(1));
    FilterOperator fop = fos.get(0);

    // all outside elements should be ignored from stat estimation
    assertEquals(3, fop.getStatistics().getNumRows());

  }

  private static IDriver createDriver() {
    HiveConf conf = env_setup.getTestCtx().hiveConf;

    conf.setBoolVar(ConfVars.HIVE_STATS_USE_BITVECTORS, true);
    conf.setBoolVar(ConfVars.HIVE_VECTORIZATION_ENABLED, false);
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    SessionState.start(conf);

    IDriver driver = DriverFactory.newDriver(conf);
    return driver;
  }
}
