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

  private static final String DROP_TABLE = "drop table if exists part3";
  private static final String CREATE_TABLE = "CREATE TABLE part3(p_partkey INT, p_type STRING)";
  private static final String QUERY_FAILING_WITH_CBO =
      "explain select * from part3 where p_type = ALL(select max(p_type) from part3)";

  @ClassRule
  public static HiveTestEnvSetup env_setup = new HiveTestEnvSetup();

  @BeforeClass
  public static void beforeClass() throws Exception {
    try (IDriver driver = createDriver()) {
      driver.run(DROP_TABLE);
      driver.run(CREATE_TABLE);
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    try (IDriver driver = createDriver()) {
      driver.run(DROP_TABLE);
    }
  }

  @Test
  public void testReExecutedOnConservative() {
    try (IDriver driver = createDriver("CONSERVATIVE")) {
      Assert.assertThrows("Plan not optimized by CBO", CommandProcessorException.class,
          () -> driver.run(QUERY_FAILING_WITH_CBO));
    }
  }

  @Test
  public void testReExecutedOnAlways() throws Exception {
    try (IDriver driver = createDriver("ALWAYS")) {
      driver.run(QUERY_FAILING_WITH_CBO);
    }
  }

  @Test
  public void testFailOnTest() {
    try (IDriver driver = createDriver("TEST")) {
      Assert.assertThrows("Plan not optimized by CBO", CommandProcessorException.class,
          () -> driver.run(QUERY_FAILING_WITH_CBO));
    }
  }

  @Test
  public void testFailOnNever() {
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
    conf.setBoolVar(ConfVars.HIVESTATSAUTOGATHER, false);
    conf.setVar(ConfVars.HIVE_CBO_FALLBACK_STRATEGY, strategy);
    SessionState.start(conf);

    IDriver driver = DriverFactory.newDriver(conf);
    return driver;
  }
}
