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

package org.apache.hive.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSchedulerQueue {

  private MiniHS2 miniHS2 = null;
  private static HiveConf conf = new HiveConf();
  private Connection hs2Conn = null;

  @BeforeClass
  public static void beforeTest() throws Exception {
    Class.forName(MiniHS2.getJdbcDriverName());
  }

  @Before
  public void setUp() throws Exception {
    DriverManager.setLoginTimeout(0);
    miniHS2 = new MiniHS2(conf, true);
    miniHS2.setConfProperty(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "false");
    miniHS2.setConfProperty(HiveConf.ConfVars.HIVE_SERVER2_MAP_FAIR_SCHEDULER_QUEUE.varname,
        "true");
    miniHS2.setConfProperty(YarnConfiguration.RM_SCHEDULER,
        "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler");
    miniHS2.start(new HashMap<String, String>());
  }

  @After
  public void tearDown() throws Exception {
    if (hs2Conn != null) {
      hs2Conn.close();
    }
    if (miniHS2 != null && miniHS2.isStarted()) {
      miniHS2.stop();
    }
    System.clearProperty("mapreduce.job.queuename");
  }

  /**
   * Verify:
   *  Test is running with MR2 and queue mapping defaults are set.
   *  Queue mapping is set for the connected user.
   *
   * @throws Exception
   */
  @Test
  public void testFairSchedulerQueueMapping() throws Exception {
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL(), "user1", "bar");
    verifyProperty("mapreduce.framework.name", "yarn");
    verifyProperty(HiveConf.ConfVars.HIVE_SERVER2_MAP_FAIR_SCHEDULER_QUEUE.varname,
        "true");
    verifyProperty(YarnConfiguration.RM_SCHEDULER,
        "org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler");
    verifyProperty("mapreduce.job.queuename", "root.user1");
  }

  /**
   * Verify that the queue refresh doesn't happen when configured to be off.
   *
   * @throws Exception
   */
  @Test
  public void testQueueMappingCheckDisabled() throws Exception {
    miniHS2.setConfProperty(
        HiveConf.ConfVars.HIVE_SERVER2_MAP_FAIR_SCHEDULER_QUEUE.varname, "false");
    hs2Conn = DriverManager.getConnection(miniHS2.getJdbcURL(), "user1", "bar");
    verifyProperty(HiveConf.ConfVars.HIVE_SERVER2_MAP_FAIR_SCHEDULER_QUEUE.varname,
        "false");
    verifyProperty("mapreduce.job.queuename", YarnConfiguration.DEFAULT_QUEUE_NAME);
  }

  /**
   * Verify that the given property contains the expected value.
   *
   * @param propertyName
   * @param expectedValue
   * @throws Exception
   */
  private void verifyProperty(String propertyName, String expectedValue) throws Exception {
    Statement stmt = hs2Conn .createStatement();
    ResultSet res = stmt.executeQuery("set " + propertyName);
    assertTrue(res.next());
    String results[] = res.getString(1).split("=");
    assertEquals("Property should be set", results.length, 2);
    assertEquals("Property should be set", expectedValue, results[1]);
  }
}
