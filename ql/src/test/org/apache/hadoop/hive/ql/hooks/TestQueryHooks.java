/**
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

package org.apache.hadoop.hive.ql.hooks;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestQueryHooks {
  private static HiveConf conf;
  private static QueryLifeTimeHookContext[] ctxs;

  @BeforeClass
  public static void setUpBeforeClass() {
    conf = new HiveConf(TestQueryHooks.class);
    conf.setVar(HiveConf.ConfVars.HIVE_QUERY_LIFETIME_HOOKS, TestLifeTimeHook.class.getName());
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
  }

  @Before
  public void setUpBefore() {
    ctxs = new QueryLifeTimeHookContext[4];
  }

  @Test
  public void testCompile() throws Exception {
    Driver driver = createDriver();
    int ret = driver.compile("SELECT 1");
    assertEquals("Expected compilation to succeed", 0, ret);
    assertNotNull(ctxs[0]);
    assertNotNull(ctxs[1]);
    assertNull(ctxs[2]);
    assertNull(ctxs[3]);
    assertEquals("SELECT 1", ctxs[0].getCommand());
    assertEquals("SELECT 1", ctxs[1].getCommand());
  }

  @Test
  public void testCompileFailure() {
    Driver driver = createDriver();
    int ret = driver.compile("SELECT * FROM foo");
    assertNotEquals("Expected compilation to fail", 0, ret);
    assertNotNull(ctxs[0]);
    assertNotNull(ctxs[1]);
    assertNull(ctxs[2]);
    assertNull(ctxs[3]);
    assertEquals("SELECT * FROM foo", ctxs[0].getCommand());
    assertEquals("SELECT * FROM foo", ctxs[1].getCommand());
  }

  @Test
  public void testAll() throws Exception {
    Driver driver = createDriver();
    int ret = driver.run("SELECT 1").getResponseCode();
    assertEquals("Expected query to run", 0, ret);
    assertNotNull(ctxs[0]);
    assertNotNull(ctxs[1]);
    assertNotNull(ctxs[2]);
    assertNotNull(ctxs[3]);
    for (int i = 0; i < ctxs.length; i++) {
      assertEquals("SELECT 1", ctxs[i].getCommand());
    }
    assertNotNull(ctxs[2].getHookContext());
    assertNotNull(ctxs[3].getHookContext());
  }

  private static Driver createDriver() {
    SessionState.start(conf);
    Driver driver = new Driver(conf);
    driver.init();
    return driver;
  }

  /**
   * Testing hook which just saves the context
   */
  private static class TestLifeTimeHook implements QueryLifeTimeHook {
    public TestLifeTimeHook() {
    }

    @Override
    public void beforeCompile(QueryLifeTimeHookContext ctx) {
      ctxs[0] = ctx;
    }

    @Override
    public void afterCompile(QueryLifeTimeHookContext ctx, boolean hasError) {
      ctxs[1] = ctx;
    }

    @Override
    public void beforeExecution(QueryLifeTimeHookContext ctx) {
      ctxs[2] = ctx;
    }

    @Override
    public void afterExecution(QueryLifeTimeHookContext ctx, boolean hasError) {
      ctxs[3] = ctx;
    }
  }
}
