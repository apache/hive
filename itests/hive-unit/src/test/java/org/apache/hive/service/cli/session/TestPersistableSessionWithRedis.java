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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hive.jdbc.miniHS2.MiniHS2;
import org.apache.hive.service.cli.session.store.RedisSessionStateStore;
import org.apache.hive.service.cli.session.store.SessionStateStore;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public class TestPersistableSessionWithRedis extends TestPersistableSessionBase {

  private static GenericContainer<?> redisContainer;

  @BeforeClass
  public static void beforeTest() throws Exception {
    MiniHS2.cleanupLocalDir();
    try {
      redisContainer = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
          .withExposedPorts(6379);
      redisContainer.start();
    } catch (Exception e) {
      Assume.assumeTrue("Docker not available, skipping Redis integration tests", false);
    }
  }

  @AfterClass
  public static void afterTest() throws Exception {
    if (redisContainer != null) {
      redisContainer.stop();
    }
    MiniHS2.cleanupLocalDir();
  }

  @Override
  protected String getStoreClassName() {
    return "org.apache.hive.service.cli.session.store.RedisSessionStateStore";
  }

  @Override
  protected void configureStore(HiveConf conf) {
    Assume.assumeTrue("Redis container not running",
        redisContainer != null && redisContainer.isRunning());
    conf.set(RedisSessionStateStore.CONF_REDIS_HOST, redisContainer.getHost());
    conf.set(RedisSessionStateStore.CONF_REDIS_PORT,
        String.valueOf(redisContainer.getMappedPort(6379)));
    conf.set(ConfVars.HIVE_SERVER2_SESSION_STATE_STORE_TTL.varname, "3600s");
  }

  @Override
  protected SessionStateStore createVerifyStore() throws Exception {
    HiveConf verifyConf = new HiveConf();
    verifyConf.set(RedisSessionStateStore.CONF_REDIS_HOST, redisContainer.getHost());
    verifyConf.set(RedisSessionStateStore.CONF_REDIS_PORT,
        String.valueOf(redisContainer.getMappedPort(6379)));
    verifyConf.set(ConfVars.HIVE_SERVER2_SESSION_STATE_STORE_TTL.varname, "3600s");
    RedisSessionStateStore store = new RedisSessionStateStore();
    store.init(verifyConf);
    return store;
  }
}
