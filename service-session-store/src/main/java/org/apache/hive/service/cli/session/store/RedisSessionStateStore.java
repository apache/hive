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

import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Jedis;

public class RedisSessionStateStore implements SessionStateStore {

  private static final Logger LOG = LoggerFactory.getLogger(RedisSessionStateStore.class);
  private static final String KEY_PREFIX = "hive:session:";
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static final String CONF_REDIS_HOST = "hive.server2.session.state.store.redis.host";
  public static final String CONF_REDIS_HOST_DEFAULT = "localhost";
  public static final String CONF_REDIS_PORT = "hive.server2.session.state.store.redis.port";
  public static final int CONF_REDIS_PORT_DEFAULT = 6379;
  public static final String CONF_REDIS_PASSWORD = "hive.server2.session.state.store.redis.password";
  public static final String CONF_REDIS_SSL = "hive.server2.session.state.store.redis.ssl";

  private JedisPool jedisPool;
  private long ttlSeconds;

  @Override
  public void init(HiveConf conf) {
    String host = conf.get(CONF_REDIS_HOST, CONF_REDIS_HOST_DEFAULT);
    int port = Integer.parseInt(conf.get(CONF_REDIS_PORT,
        String.valueOf(CONF_REDIS_PORT_DEFAULT)));
    String password = conf.get(CONF_REDIS_PASSWORD);
    boolean useSsl = Boolean.parseBoolean(conf.get(CONF_REDIS_SSL, "false"));
    this.ttlSeconds = conf.getTimeVar(
        ConfVars.HIVE_SERVER2_SESSION_STATE_STORE_TTL, TimeUnit.SECONDS);

    JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setMaxTotal(16);
    poolConfig.setMaxIdle(8);
    poolConfig.setMinIdle(2);

    DefaultJedisClientConfig.Builder clientConfigBuilder = DefaultJedisClientConfig.builder();
    if (password != null && !password.isEmpty()) {
      clientConfigBuilder.password(password);
    }
    if (useSsl) {
      clientConfigBuilder.ssl(true);
    }
    jedisPool = new JedisPool(poolConfig, new HostAndPort(host, port),
        clientConfigBuilder.build());
    LOG.info("Initialized RedisSessionStateStore with host={}:{}, ssl={}, ttl={}s",
        host, port, useSsl, ttlSeconds);
  }

  @Override
  public void saveSnapshot(String sessionHandleId, HiveSessionSnapshot snapshot) {
    String key = KEY_PREFIX + sessionHandleId;
    try (Jedis jedis = jedisPool.getResource()) {
      String json = OBJECT_MAPPER.writeValueAsString(snapshot);
      jedis.setex(key, ttlSeconds, json);
      LOG.debug("Saved session snapshot to Redis: {}", key);
    } catch (Exception e) {
      LOG.error("Failed to save session snapshot to Redis: {}", key, e);
      throw new RuntimeException("Failed to save session snapshot", e);
    }
  }

  @Override
  public HiveSessionSnapshot getSnapshot(String sessionHandleId) {
    String key = KEY_PREFIX + sessionHandleId;
    try (Jedis jedis = jedisPool.getResource()) {
      String json = jedis.get(key);
      if (json == null) {
        return null;
      }
      return OBJECT_MAPPER.readValue(json, HiveSessionSnapshot.class);
    } catch (Exception e) {
      LOG.error("Failed to get session snapshot from Redis: {}", key, e);
      throw new RuntimeException("Failed to get session snapshot", e);
    }
  }

  @Override
  public void deleteSnapshot(String sessionHandleId) {
    String key = KEY_PREFIX + sessionHandleId;
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.del(key);
      LOG.debug("Deleted session snapshot from Redis: {}", key);
    } catch (Exception e) {
      LOG.error("Failed to delete session snapshot from Redis: {}", key, e);
      throw new RuntimeException("Failed to delete session snapshot", e);
    }
  }

  @Override
  public void close() {
    if (jedisPool != null) {
      jedisPool.close();
      jedisPool = null;
    }
  }
}
