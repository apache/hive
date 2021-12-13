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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.testutils.HiveTestEnvSetup;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

public class TestCBOReCompilation {

  private static final String QUERY_FAILING_WITH_CBO = "explain from ff1 as a join cc1 as b " +
      "insert overwrite table aa1 select   stf_id GROUP BY b.stf_id " +
      "insert overwrite table bb1 select b.stf_id GROUP BY b.stf_id";

  @ClassRule
  public static HiveTestEnvSetup env_setup = new HiveTestEnvSetup();

  @BeforeClass
  public static void beforeClass() throws Exception {
    try (IDriver driver = createDriver()) {
      dropTables(driver);
      String[] cmds = {
          // @formatter:off
          "create table aa1 ( stf_id string)",
          "create table bb1 ( stf_id string)",
          "create table cc1 ( stf_id string)",
          "create table ff1 ( x string)"
          // @formatter:on
      };
      for (String cmd : cmds) {
        driver.run(cmd);
      }
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    try (IDriver driver = createDriver()) {
      dropTables(driver);
    }
  }

  public static void dropTables(IDriver driver) throws Exception {
    String[] tables = new String[] {"aa1", "bb1", "cc1", "ff1" };
    for (String t : tables) {
      driver.run("drop table if exists " + t);
    }
  }

  @Test
  public void testReExecutedOnConservative() throws Exception {
    try (IDriver driver = createDriver("CONSERVATIVE")) {
      driver.run(QUERY_FAILING_WITH_CBO);
    }
  }

  @Test
  public void testReExecutedOnAlways() throws Exception {
    try (IDriver driver = createDriver("ALWAYS")) {
      driver.run(QUERY_FAILING_WITH_CBO);
    }
  }

  @Test
  public void testFailOnTest() throws Exception {
    try (IDriver driver = createDriver("TEST")) {
      Assert.assertThrows("Plan not optimized by CBO", CommandProcessorException.class,
          () -> driver.run(QUERY_FAILING_WITH_CBO));
    }
  }

  @Test
  public void testFailOnNever() throws Exception {
    try (IDriver driver = createDriver("NEVER")) {
      Assert.assertThrows("Plan not optimized by CBO", CommandProcessorException.class,
          () -> driver.run(QUERY_FAILING_WITH_CBO));
    }
  }

  @Test
  public void testFailOnNotEnoughTries() {
    try (IDriver driver = createDriver("ALWAYS")) {
      driver.getConf().setIntVar(ConfVars.HIVE_QUERY_MAX_RECOMPILATION_COUNT, 0);
      Assert.assertThrows("Plan not optimized by CBO", CommandProcessorException.class,
          () -> driver.run(QUERY_FAILING_WITH_CBO));
    }
  }

  private static IDriver createDriver() {
    return createDriver("TEST");
  }

  private static IDriver createDriver(String strategy) {
    HiveConf conf = env_setup.getTestCtx().hiveConf;

    conf.setBoolVar(ConfVars.HIVE_CBO_ENABLED, true);
    conf.setVar(ConfVars.HIVE_CBO_FALLBACK_STRATEGY, strategy);
    SessionState.start(conf);

    IDriver driver = DriverFactory.newDriver(conf);
    return driver;
  }
}
