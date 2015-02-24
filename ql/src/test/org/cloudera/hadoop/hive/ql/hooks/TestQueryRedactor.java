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
package org.cloudera.hadoop.hive.ql.hooks;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

public class TestQueryRedactor {

  @BeforeClass
  public static void onetimeSetup() throws Exception {
    HiveConf conf = new HiveConf(TestQueryRedactor.class);
    Driver driver = createDriver(conf);
    int ret = driver.run("create table t1(i int)").getResponseCode();
    assertEquals("Checking command success", 0, ret);
  }

  @AfterClass
  public static void onetimeTeardown() throws Exception {
    HiveConf conf = new HiveConf(TestQueryRedactor.class);
    Driver driver = createDriver(conf);
    driver.run("drop table t1");
  }

  @Before
  public void setup() {
  }

  @Test
  public void testQueryRedactor() throws Exception {
    HiveConf conf = new HiveConf(TestQueryRedactor.class);
    HiveConf.setVar(conf, HiveConf.ConfVars.QUERYREDACTORHOOKS,
      QueryRedactor.class.getName());
    String hiveRoot = System.getProperty("hive.root");
    assertNotNull("Hive root cannot be null", hiveRoot);
    conf.set("hive.query.redaction.rules", hiveRoot + "/ql/src/test/resources/test-query-redactor.json");
    Driver driver;
    int ret;
    driver = createDriver(conf);
    ret = driver.compile("select '0000-1111-2222-3333' from t1");
    assertEquals("Checking command success", 0, ret);
    assertEquals("select 'XXXX-XXXX-XXXX-3333' from t1", HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYSTRING));
    conf.set("hive.query.redaction.rules", "");
    driver = createDriver(conf);
    ret = driver.compile("select '0000-1111-2222-3333' from t1");
    assertEquals("Checking command success", 0, ret);
    assertEquals("select '0000-1111-2222-3333' from t1", HiveConf.getVar(conf, HiveConf.ConfVars.HIVEQUERYSTRING));

  }

  private static Driver createDriver(HiveConf conf) {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    SessionState.start(conf);
    Driver driver = new Driver(conf);
    driver.init();
    return driver;
  }

}
