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

package org.apache.hive.service.cli.session;

import java.util.concurrent.TimeUnit;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.cli.session.store.SessionStateStore;
import org.apache.hive.service.cli.session.store.ZooKeeperSessionStateStore;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestPersistableSessionWithZooKeeper extends TestPersistableSessionBase {

  private static TestingServer zkServer;
  private static final String ZK_SESSION_PATH = "/test_persistable_sessions";

  @BeforeClass
  public static void beforeTest() throws Exception {
    MiniHS2.cleanupLocalDir();
    zkServer = new TestingServer();
    zkServer.start();
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (zkServer != null) {
      zkServer.close();
      zkServer = null;
    }
    MiniHS2.cleanupLocalDir();
  }

  @Override
  protected String getStoreClassName() {
    return "org.apache.hive.service.cli.session.store.ZooKeeperSessionStateStore";
  }

  @Override
  protected void configureStore(HiveConf conf) {
    conf.setVar(ConfVars.HIVE_ZOOKEEPER_QUORUM, zkServer.getConnectString());
    conf.set(ZooKeeperSessionStateStore.CONF_ZK_PATH, ZK_SESSION_PATH);
    conf.setTimeDuration(ConfVars.HIVE_ZOOKEEPER_CONNECTION_TIMEOUT.varname, 2, TimeUnit.SECONDS);
    conf.setTimeDuration(ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME.varname,
        100, TimeUnit.MILLISECONDS);
    conf.setInt(ConfVars.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES.varname, 1);
  }

  @Override
  protected SessionStateStore createVerifyStore() throws Exception {
    HiveConf verifyConf = new HiveConf();
    verifyConf.setVar(ConfVars.HIVE_ZOOKEEPER_QUORUM, zkServer.getConnectString());
    verifyConf.set(ZooKeeperSessionStateStore.CONF_ZK_PATH, ZK_SESSION_PATH);
    ZooKeeperSessionStateStore store = new ZooKeeperSessionStateStore();
    store.init(verifyConf);
    return store;
  }
}
