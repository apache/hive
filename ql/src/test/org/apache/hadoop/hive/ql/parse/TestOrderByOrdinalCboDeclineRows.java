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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.parse;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.testutils.HiveTestEnvSetup;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

/**
 * HIVE-30037: executed row order when CBO declines the statement.
 *
 * {@link TestSemanticAnalyzer} already pins AST substitution on this path.
 * This class runs the query (Tez mini cluster via {@link HiveTestEnvSetup})
 * and asserts the result is 3, 2, 1 rather than the pre-fix unsorted order.
 */
public class TestOrderByOrdinalCboDeclineRows {

  private static final String TABLE = "hive30037_ob_t";
  private static final String QUERY =
      "select d from " + TABLE + " tablesample (5 rows) s order by 1 desc";

  @ClassRule
  public static HiveTestEnvSetup env_setup = new HiveTestEnvSetup();

  @Rule
  public TestRule methodRule = env_setup.getMethodRule();

  @BeforeClass
  public static void beforeClass() throws Exception {
    IDriver driver = createDriver();
    dropTables(driver);
    driver.run("create table " + TABLE + " (d int)");
    driver.run("insert into " + TABLE + " values (1), (2), (3)");
  }

  @AfterClass
  public static void afterClass() throws Exception {
    IDriver driver = createDriver();
    dropTables(driver);
  }

  public static void dropTables(IDriver driver) throws Exception {
    driver.run("drop table if exists " + TABLE);
  }

  @Test
  public void testTablesampleOrderByOrdinalDescReturns321() throws Exception {
    IDriver driver = createDriver();
    driver.run(QUERY);

    String cboInfo = driver.getPlan() == null ? null : driver.getPlan().getCboInfo();
    assertNotNull("expected CBO info after compile", cboInfo);
    assertTrue("CBO should decline TABLESAMPLE, got cboInfo=" + cboInfo,
        cboInfo.contains("not optimized by CBO"));

    assertEquals(Arrays.asList("3", "2", "1"), fetchRows(driver));
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private static List<String> fetchRows(IDriver driver) throws Exception {
    driver.setMaxRows(100);
    List rows = new ArrayList();
    if (driver.getFetchTask() != null) {
      driver.getFetchTask().setMaxRows(100);
      List batch = new ArrayList();
      while (driver.getFetchTask().fetch(batch)) {
        rows.addAll(batch);
        batch.clear();
      }
    } else {
      List batch = new ArrayList();
      while (driver.getResults(batch)) {
        rows.addAll(batch);
        batch.clear();
      }
    }
    List<String> out = new ArrayList<>();
    for (Object row : rows) {
      out.add(String.valueOf(row));
    }
    return out;
  }

  private static IDriver createDriver() {
    HiveConf conf = env_setup.getTestCtx().hiveConf;
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_CBO_ENABLED, true);
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_FETCH_TASK_CONVERSION, "none");
    SessionState.start(conf);
    return DriverFactory.newDriver(conf);
  }
}
