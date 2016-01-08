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

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.Test;

public class TestPreParseHook {

  @BeforeClass
  public static void onetimeSetup() throws Exception {
    HiveConf conf = new HiveConf(TestPreParseHook.class);
    Driver driver = createDriver(conf);
    int ret = driver.run("create table t1(i int)").getResponseCode();
    assertEquals("Checking command success", 0, ret);
  }

  @AfterClass
  public static void onetimeTeardown() throws Exception {
    HiveConf conf = new HiveConf(TestPreParseHook.class);
    Driver driver = createDriver(conf);
    driver.run("drop table t1");
  }

  @Before
  public void setup() {	
  }

  @Test
  public void testWithoutHook() throws Exception {
    HiveConf conf = new HiveConf(TestPreParseHook.class);
    Driver driver = createDriver(conf);
    int ret = driver.compile("SELECT 'XXX' from t1");
    assertEquals("Checking command success", 0, ret);
  }

  @Test
  public void testBrokenHook() throws Exception {
    HiveConf conf = new HiveConf(TestPreParseHook.class);
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_PRE_PARSE_HOOKS,
    		BrokenPreParseHook.class.getName());
    Driver driver = createDriver(conf);
    int ret = driver.compile("SELECT 'XXX' from t1");
    assertEquals("Checking command success", 0, ret);
  }
  
  @Test
  public void testPreParseHook() throws Exception {
    HiveConf conf = new HiveConf(TestPreParseHook.class);
    HiveConf.setVar(conf, HiveConf.ConfVars.HIVE_PRE_PARSE_HOOKS,
    		SimpleCustomSQL.class.getName());
    Driver driver = createDriver(conf);
    int ret = driver.compile("SLCT 'XXX' from t1");
    assertEquals("Checking command success", 0, ret);
  }

  public static class SimpleCustomSQL implements PreParseHook {

	public String getCustomCommand(Context context, String command)
			throws Exception {
		return command.replaceAll("SLCT", "SELECT");
	}

  }

  public static class BrokenPreParseHook implements PreParseHook {

	public String getCustomCommand(Context context, String command)
			throws Exception {
		throw new Exception("broken hook");
	}

  }
  
  private static Driver createDriver(HiveConf conf) {
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    SessionState.start(conf);
    Driver driver = new Driver(conf);
    driver.init();
    return driver;
  }

}
