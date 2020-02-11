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
package org.apache.hadoop.hive.llap.registry.impl;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.daemon.MiniLlapCluster;
import org.apache.hadoop.hive.llap.registry.impl.LlapZookeeperRegistryImpl.ConfigChangeLockResult;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import java.io.IOException;

/**
 * Llap Registry service related tests. Currently only for configuration change.
 */
public class TestLlapRegistryService {
  private static MiniLlapCluster cluster = null;
  private static HiveConf conf = new HiveConf();

  @BeforeClass
  public static void setUp() throws Exception {
    cluster = MiniLlapCluster.create("llap01", null, 1, 2L, false, false, 1L, 1);
    HiveConf.setVar(conf, HiveConf.ConfVars.LLAP_DAEMON_XMX_HEADROOM, "1");
    cluster.serviceInit(conf);
    cluster.serviceStart();
  }

  @AfterClass
  public static void tearDown() throws IOException {
    if (cluster != null) {
      cluster.serviceStop();
    }
  }

  @Test
  public void testLockForConfigChange() throws IOException {
    LlapRegistryService client1 = null;
    LlapRegistryService client2 = null;
    ConfigChangeLockResult result;

    try {
      client1 = new LlapRegistryService(false);
      client1.init(conf);
      client1.start();

      client2 = new LlapRegistryService(false);
      client2.init(conf);
      client2.start();

      assertTrue(client1.lockForConfigChange(10000, 20000).isSuccess());
      assertTrue(client2.lockForConfigChange(30000, 40000).isSuccess());

      // Can not set to before
      result = client1.lockForConfigChange(20000, 30000);
      assertFalse(result.isSuccess());
      assertEquals(result.getNextConfigChangeTime(), 40000);

      result = client1.lockForConfigChange(30000, 40000);
      assertFalse(result.isSuccess());
      assertEquals(result.getNextConfigChangeTime(), 40000);

      result = client1.lockForConfigChange(35000, 45000);
      assertFalse(result.isSuccess());
      assertEquals(result.getNextConfigChangeTime(), 40000);

      // Can start from the previous end timestamp
      result = client1.lockForConfigChange(40000, 50000);
      assertTrue(result.isSuccess());
      assertEquals(result.getNextConfigChangeTime(), 50000);
    } finally {
      if (client1 != null) {
        client1.close();
      }
      if (client2 != null) {
        client2.close();
      }
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLockForConfigChangeInvalid() throws IOException{
    LlapRegistryService client = null;

    try {
      client = new LlapRegistryService(false);
      client.init(conf);
      client.start();

      client.lockForConfigChange(20000, 10000);
    } finally {
      if (client != null) {
        client.close();
      }
    }
  }
}
