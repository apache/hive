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

package org.apache.hive.service.cli.thrift;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.Service;
import org.apache.hive.service.cli.CLIServiceClient;
import org.apache.hive.service.cli.SessionHandle;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
*
* TestMiniHS2StateWithNoZookeeper.
* This tests HS2 shutdown is not triggered by CloseSession operation 
* while HS2 has never been registered with ZooKeeper.
*
*/

public class TestMiniHS2StateWithNoZookeeper {
  
  private static final Logger LOG = LoggerFactory.getLogger(TestMiniHS2StateWithNoZookeeper.class);
  private static MiniHS2 miniHS2 = null;
  private static HiveConf hiveConf = null;

  @BeforeClass
  public static void beforeTest() throws Exception   {
    MiniHS2.cleanupLocalDir();
    hiveConf = new HiveConf();
    hiveConf.setBoolVar(ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY, true);
    hiveConf.setIntVar(ConfVars.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES, 0);
    hiveConf.setTimeVar(ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME, 0, TimeUnit.MILLISECONDS);
    // Disable killquery, this way only HS2 start will fail, not the SessionManager service
    hiveConf.setBoolVar(ConfVars.HIVE_ZOOKEEPER_KILLQUERY_ENABLE, false);
    miniHS2 = new MiniHS2(hiveConf);
    Map<String, String> confOverlay = new HashMap<String, String>();
    try {
      miniHS2.start(confOverlay);
    } catch (Exception ex) {
      LOG.warn("Zookeeper is not set up intentionally, so the error is expected (unless it's not related to ZK): " + ex);
      miniHS2.setStarted(true);
    }
  }

  @AfterClass
  public static void afterTest() throws Exception {
    miniHS2.stop();
  }

  @Test
  public void openSessionAndClose() throws Exception {
    CLIServiceClient client = miniHS2.getServiceClient();
    SessionHandle sessionHandle = client.openSession(null, null, null);
    client.closeSession(sessionHandle);
    Thread.sleep(100);

    Assert.assertEquals(Service.STATE.STARTED, miniHS2.getState());
  }
}