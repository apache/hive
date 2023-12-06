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
package org.apache.hadoop.hive.ql.hooks;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

public class TestHooks {

  @BeforeClass
  public static void onetimeSetup() throws Exception {
    HiveConf conf = new HiveConf(TestHooks.class);
    conf
    .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    Driver driver = createDriver(conf);
    driver.run("create table t1(i int)");
  }

  @AfterClass
  public static void onetimeTeardown() throws Exception {
    HiveConf conf = new HiveConf(TestHooks.class);
    Driver driver = createDriver(conf);
    driver.run("drop table t1");
  }

  @Before
  public void setup() {
  }

  @Test
  public void testRedactLogString() throws Exception {
    HiveConf conf = new HiveConf(TestHooks.class);
    String str;

    HiveConf.setVar(conf, HiveConf.ConfVars.QUERY_REDACTOR_HOOKS, SimpleQueryRedactor.class.getName());

    str = HookUtils.redactLogString(null, null);
    assertEquals(str, null);

    str = HookUtils.redactLogString(conf, null);
    assertEquals(str, null);

    str = HookUtils.redactLogString(conf, "select 'XXX' from t1");
    assertEquals(str, "select 'AAA' from t1");
  }

  @Test
  public void testQueryRedactor() throws Exception {
    HiveConf conf = new HiveConf(TestHooks.class);
    HiveConf.setVar(conf, HiveConf.ConfVars.QUERY_REDACTOR_HOOKS,
      SimpleQueryRedactor.class.getName());
    conf
    .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    Driver driver = createDriver(conf);
    int ret = driver.compile("select 'XXX' from t1", true);
    assertEquals("Checking command success", 0, ret);
    assertEquals("select 'AAA' from t1", conf.getQueryString());
  }

  public static class SimpleQueryRedactor extends Redactor {
    @Override
    public String redactQuery(String query) {
      return query.replaceAll("XXX", "AAA");
    }
  }

  private static Driver createDriver(HiveConf conf) {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    SessionState.start(conf);
    Driver driver = new Driver(conf);
    return driver;
  }

}
