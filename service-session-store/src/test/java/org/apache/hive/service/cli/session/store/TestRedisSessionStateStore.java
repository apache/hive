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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import static org.apache.hive.service.cli.session.store.RedisSessionStateStore.CONF_REDIS_HOST;
import static org.apache.hive.service.cli.session.store.RedisSessionStateStore.CONF_REDIS_PORT;

public class TestRedisSessionStateStore extends TestSessionStateStoreBase {

  private static GenericContainer<?> redisContainer;

  @BeforeClass
  public static void startRedis() {
    try {
      redisContainer = new GenericContainer<>(DockerImageName.parse("redis:7-alpine"))
          .withExposedPorts(6379);
      redisContainer.start();
    } catch (Exception e) {
      // Docker not available, skip tests
      Assume.assumeTrue("Docker not available, skipping Redis tests", false);
    }
  }

  @AfterClass
  public static void stopRedis() {
    if (redisContainer != null) {
      redisContainer.stop();
    }
  }

  @Override
  protected SessionStateStore createStore() {
    Assume.assumeTrue("Redis container not running", redisContainer != null && redisContainer.isRunning());
    HiveConf conf = new HiveConf();
    conf.set(CONF_REDIS_HOST, redisContainer.getHost());
    conf.set(CONF_REDIS_PORT, String.valueOf(redisContainer.getMappedPort(6379)));
    conf.set(ConfVars.HIVE_SERVER2_SESSION_STATE_STORE_TTL.varname, "3600s");
    RedisSessionStateStore store = new RedisSessionStateStore();
    store.init(conf);
    return store;
  }
}
