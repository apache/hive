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

package org.apache.hive.service.cli.session.store;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import static org.apache.hive.service.cli.session.store.ZooKeeperSessionStateStore.CONF_ZK_PATH;

public class TestZooKeeperSessionStateStore extends TestSessionStateStoreBase {

  private static TestingServer zkServer;

  @BeforeClass
  public static void startZk() throws Exception {
    zkServer = new TestingServer();
    zkServer.start();
  }

  @AfterClass
  public static void stopZk() throws Exception {
    if (zkServer != null) {
      zkServer.close();
    }
  }

  @Override
  protected SessionStateStore createStore() {
    HiveConf conf = new HiveConf();
    conf.setVar(ConfVars.HIVE_ZOOKEEPER_QUORUM, zkServer.getConnectString());
    conf.set(CONF_ZK_PATH, "/test_hive_sessions");
    ZooKeeperSessionStateStore store = new ZooKeeperSessionStateStore();
    store.init(conf);
    return store;
  }
}
