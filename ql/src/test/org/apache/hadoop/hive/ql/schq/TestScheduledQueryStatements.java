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
package org.apache.hadoop.hive.ql.schq;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Optional;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryKey;
import org.apache.hadoop.hive.metastore.model.MScheduledQuery;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.testutils.HiveTestEnvSetup;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

public class TestScheduledQueryStatements {

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
        "create table tu(c int)",
        "create database asd",
        "create table asd.tasd(c int)",
        // @formatter:on
    };
    for (String cmd : cmds) {
      int ret = driver.run(cmd).getResponseCode();
      assertEquals("Checking command success", 0, ret);
    }

    ScheduledQueryExecutionService.startScheduledQueryExecutorService(env_setup.getTestCtx().hiveConf);

  }

  @AfterClass
  public static void afterClass() throws Exception {
    IDriver driver = createDriver();
    dropTables(driver);
  }

  public static void dropTables(IDriver driver) throws Exception {
    String tables[] = { "tu" };
    for (String t : tables) {
      int ret = driver.run("drop table if exists " + t).getResponseCode();
      assertEquals("Checking command success", 0, ret);
    }
  }

  @Test
  public void testSimpleCreate() throws ParseException, Exception {
    IDriver driver = createDriver();

    CommandProcessorResponse ret;
    ret = driver.run("create scheduled query simplecreate cron '* * * * * ? *' as select 1 from tu");
    if (ret.getResponseCode() != 0) {
      throw ret;
    }
  }

  @Test(expected = CommandProcessorResponse.class)
  public void testNonExistentTable1() throws ParseException, Exception {
    IDriver driver = createDriver();
    CommandProcessorResponse ret =
        driver.run("create scheduled query nonexist cron '* * * * * ? *' as select 1 from nonexist");
    if (ret.getResponseCode() != 0) {
      throw ret;
    }
  }

  @Test(expected = CommandProcessorResponse.class)
  public void testNonExistentTable2() throws ParseException, Exception {
    IDriver driver = createDriver();

    CommandProcessorResponse ret;
    ret = driver.run("use asd");
    if (ret.getResponseCode() != 0) {
      fail("use database failed");
    }

    ret = driver.run("create scheduled query nonexist2 cron '* * * * * ? *' as select 1 from tu");
    if (ret.getResponseCode() != 0) {
      throw ret;
    }
  }

  // FIXME: I think this case should fail for now...
  @Test(expected = CommandProcessorResponse.class)
  public void testCreateFromNonDefaultDatabase() throws ParseException, Exception {
    IDriver driver = createDriver();

    CommandProcessorResponse ret;
    ret = driver.run("use asd");

    if (ret.getResponseCode() != 0) {
      fail("use database failed");
    }

    // FIXME: the query is actually correct; but it should 
    ret = driver.run("create scheduled query nonDef cron '* * * * * ? *' as select 1");
    if (ret.getResponseCode() != 0) {
      throw ret;
    }
  }

  @Test
  public void testDoubleCreate() throws ParseException, Exception {
    IDriver driver = createDriver();

    CommandProcessorResponse ret;
    ret = driver.run("create scheduled query dc cron '* * * * * ? *' as select 1 from tu");
    assertEquals(0, ret.getResponseCode());
    ret = driver.run("create scheduled query dc cron '* * * * * ? *' as select 1 from tu");
    assertNotEquals("expected to fail", 0, ret.getResponseCode());
  }

  @Test
  public void testAlter() throws ParseException, Exception {
    IDriver driver = createDriver();

    CommandProcessorResponse ret;
    ret = driver.run("create scheduled query alter1 cron '* * * * * ? *' as select 1 from tu");
    assertEquals(0, ret.getResponseCode());
    ret = driver.run("alter scheduled query alter1 executed as 'user3'");
    assertEquals(0, ret.getResponseCode());
    ret = driver.run("alter scheduled query alter1 defined as select 22 from tu");
    assertEquals(0, ret.getResponseCode());

    //    try (PersistenceManager pm = PersistenceManagerProvider.getPersistenceManager()) {
    try (XObjectStore os = new XObjectStore(env_setup.getTestCtx().hiveConf)) {
      Optional<MScheduledQuery> sq = os.getMScheduledQuery(new ScheduledQueryKey("alter1", "default"));
      assertTrue(sq.isPresent());
      assertEquals("user3", sq.get().toThrift().getUser());
    }

  }

  static class XObjectStore extends ObjectStore implements AutoCloseable {

    public XObjectStore(HiveConf hiveConf) {
      super();
      super.setConf(hiveConf);
    }

    @Override
    public void close() throws Exception {
      super.shutdown();
    }
  }

  private static IDriver createDriver() {
    HiveConf conf = env_setup.getTestCtx().hiveConf;

    SessionState.start(conf);

    IDriver driver = DriverFactory.newDriver(conf);
    return driver;
  }
}
