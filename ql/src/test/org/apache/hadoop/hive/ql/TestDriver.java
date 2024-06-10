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
package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.DummyTxnManager;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Driver related unit tests.
 */
public class TestDriver {

  private HiveConf conf;

  @Before
  public void beforeTest() {
    conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
  }

  @Test
  public void testDriverContextQueryErrorMessageCompileTime() {
    SessionState.start(conf);
    Driver driver = getDriver();
    try {
      driver.run("wrong sql command");
      Assert.fail("Should have thrown an exception from compile time");
    } catch (Exception e) {
      Assert.assertEquals(CommandProcessorException.class, e.getClass());
      String message = e.getMessage();
      // actual assertion: whether the message reached driverContext
      Assert.assertEquals(message, driver.driverContext.getQueryErrorMessage());
      // sanity check: the message is as expected
      Assert.assertTrue("Exception message is not as expected, got: " + message,
          e.getMessage().startsWith("FAILED: ParseException line 1:0 cannot recognize input near"));
    } finally {
      driver.close();
    }
  }

  @Test
  public void testDriverContextQueryErrorMessageRuntime() {
    conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    conf.setBoolVar(HiveConf.ConfVars.HIVE_CLI_TEZ_INITIALIZE_SESSION, false);
    SessionState.start(conf);
    Driver driver = getDriver();
    try {
      driver.run("create table test_table (id int)");
      // run a query that most probably goes to Tez execution
      driver.run("select a.id from test_table a left outer join test_table b on a.id = b.id");
      Assert.fail("Should have thrown an exception from runtime");
    } catch (Exception e) {
      Assert.assertEquals(CommandProcessorException.class, e.getClass());
      String message = e.getMessage();
      // actual assertion: whether the message reached driverContext
      Assert.assertEquals(message, driver.driverContext.getQueryErrorMessage());
      // sanity check: the message is as expected
      Assert.assertTrue("Exception message is not as expected, got: " + message,
          e.getMessage().startsWith("FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.tez.TezTask"));
    } finally {
      driver.close();
    }
  }

  private Driver getDriver() {
    QueryInfo queryInfo = new QueryInfo(null, null, null, null, null);
    return new Driver(new QueryState.Builder().withHiveConf(conf).build(), queryInfo, new DummyTxnManager());
  }
}
