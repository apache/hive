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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.FileSystem;
import java.util.Optional;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryKey;
import org.apache.hadoop.hive.metastore.model.MScheduledQuery;
import org.apache.hadoop.hive.ql.DriverFactory;
import org.apache.hadoop.hive.ql.IDriver;
import org.apache.hadoop.hive.ql.io.ProxyLocalFileSystem;
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

// FIXME needed? leftover class 
public class TestScheduledQueryStatements2 {

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
        "create table tu(c int) stored as orc",
        "insert into tu values(1),(3),(10)",
        "create database asd",
        "create table asd.tasd(c int)",
        // @formatter:on
    };
    for (String cmd : cmds) {
      int ret = driver.run(cmd).getResponseCode();
      assertEquals("Checking command success", 0, ret);
    }

    //    ScheduledQueryExecutionService.startScheduledQueryExecutorService(env_setup.getTestCtx().hiveConf);

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
    for (int i = 0; i < 1000; i++) {

      System.out.println("TRY# " + i);
      System.out.println("TRY# " + i);
      System.out.println("TRY# " + i);
      System.out.println("TRY# " + i);
      System.out.println("TRY# " + i);
      boolean ex1 = (i > 3);
      if (ex1) {
        ProxyLocalFileSystem.track();
      }
      try (PrintStream fos = new PrintStream("/tmp/stack/idx")) {
        RuntimeException ex = new RuntimeException("c");
        fos.println(i);
      } catch (FileNotFoundException e) {
        throw new RuntimeException(e);
      }

      doOneSelect();
      if (ex1) {
        ProxyLocalFileSystem.boom();
      }
    }
  }

  private void doOneSelect() throws IOException {

    HiveConf conf = env_setup.getTestCtx().hiveConf;
    conf.set("hive.query.results.cache.enabled", "true");
    SessionState ss = SessionState.start(conf);

    try (IDriver driver = DriverFactory.newDriver(conf)) {
      driver.run("drop table tt");
      driver.run("create table tt as select c,count(1) from tu group by c");
      //      driver.run("select 1");
    }

    ss.close();
  }

  private static IDriver createDriver() {
    HiveConf conf = env_setup.getTestCtx().hiveConf;

    SessionState.start(conf);

    IDriver driver = DriverFactory.newDriver(conf);
    return driver;
  }

}
