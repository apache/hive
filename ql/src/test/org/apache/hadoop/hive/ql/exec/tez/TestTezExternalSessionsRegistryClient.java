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
package org.apache.hadoop.hive.ql.exec.tez;

import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Test;

public class TestTezExternalSessionsRegistryClient {
  @Test
  public void testDummyExternalSessionsRegistry() {
    HiveConf conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_REGISTRY_CLASS,
        DummyExternalSessionsRegistry.class.getName());
    conf.set("tez.am.registry.namespace", "dummy");
    ExternalSessionsRegistry externalSessionsRegistry = ExternalSessionsRegistry.getClient(conf);
    assertTrue(externalSessionsRegistry instanceof DummyExternalSessionsRegistry);
  }

  @Test
  public void testTezExternalSessionsRegistry() {
    HiveConf conf = new HiveConf();
    conf.set("tez.am.zookeeper.quorum", "test-quorum");
    conf.set("tez.am.registry.namespace", "tez");
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_REGISTRY_CLASS,
        ZookeeperExternalSessionsRegistryClient.class.getName());
    ExternalSessionsRegistry externalSessionsRegistry = ExternalSessionsRegistry.getClient(conf);
    assertTrue(externalSessionsRegistry instanceof ZookeeperExternalSessionsRegistryClient);
  }
}
