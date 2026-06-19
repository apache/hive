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

package org.apache.hive.minikdc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.security.ZooKeeperTokenStore;
import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.containers.GenericContainer;

import static org.apache.hive.minikdc.TestZooKeeperWithMiniKdc.startZooKeeper;

public class TestZooKeeperHS2HAWithMiniKdc {
  private static final String ZK_PRINCIPAL = "zookeeper";
  private static MiniHS2 miniHS2 = null;
  private static MiniHiveKdc miniKDC;
  private static HiveConf conf;
  private static GenericContainer<?> zookeeper;

  @BeforeClass
  public static void setUp() throws Exception {
    miniKDC = new MiniHiveKdc();
    conf = new HiveConf();
    miniKDC.addUserPrincipal(miniKDC.getServicePrincipalForUser(ZK_PRINCIPAL));
    zookeeper = startZooKeeper(miniKDC, conf);
    DriverManager.setLoginTimeout(0);
    conf.set("hive.cluster.delegation.token.store.class", ZooKeeperTokenStore.class.getName());
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SERVER2_SUPPORT_DYNAMIC_SERVICE_DISCOVERY, true);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_ZOOKEEPER_USE_KERBEROS, true);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SERVER2_ACTIVE_PASSIVE_HA_ENABLE, true);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    HiveConf.setBoolVar(conf, HiveConf.ConfVars.HIVE_FETCH_TASK_CACHING, false);
    miniHS2 = MiniHiveKdc.getMiniHS2WithKerb(miniKDC, conf);
    miniHS2.start(new HashMap<String, String>());
  }

  @Test
  public void testJdbcConnection() throws Exception {
    System.clearProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY);
    String url = "jdbc:hive2://localhost:" + zookeeper.getMappedPort(2181) + "/default;" +
        "serviceDiscoveryMode=zooKeeperHA;zooKeeperNamespace=hs2ActivePassiveHA;principal=hive/localhost@EXAMPLE.COM";
    try (Connection con = DriverManager.getConnection(url);
         ResultSet rs = con.getMetaData().getCatalogs()) {
      Assert.assertFalse(rs.next());
      ((HiveConnection) con).getDelegationToken("hive", "hive");
    }
    Assert.assertNotNull(System.getProperty(ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY));
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try {
      if (zookeeper != null) {
        zookeeper.stop();
      }
    } finally {
      miniKDC.shutDown();
      if (miniHS2 != null && miniHS2.isStarted()) {
        miniHS2.stop();
        miniHS2.cleanup();
      }
    }
  }
}
