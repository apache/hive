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

package org.apache.hadoop.hive.metastore;

import org.apache.curator.test.TestingServer;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.junit.Before;
import org.junit.experimental.categories.Category;

@Category(MetastoreCheckinTest.class)
public class TestRemoteHiveMetaStoreZK extends TestRemoteHiveMetaStore {
  private static TestingServer zkServer = null;

    @Before
    public void setUp() throws Exception {
        // Start ZooKeeper server if not done already.
        if (zkServer == null) {
            zkServer = new TestingServer();
            // Add ZK specific configurations, so that the metastore can register itself to ZK when
            // started.
            initConf();
            MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, zkServer.getConnectString());
            MetastoreConf.setVar(conf, ConfVars.THRIFT_ZOOKEEPER_NAMESPACE, this.getClass().getSimpleName());
            MetastoreConf.setVar(conf, ConfVars.THRIFT_SERVICE_DISCOVERY_MODE, "zookeeper");
        }
        super.setUp();
    }

  @Override
  protected HiveMetaStoreClient createClient() throws Exception {
      return new HiveMetaStoreClient(conf);
  }
}
