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
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZooKeeperSessionStateStore implements SessionStateStore {

  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperSessionStateStore.class);

  public static final String CONF_ZK_PATH = "hive.server2.session.state.store.zk.path";
  public static final String CONF_ZK_PATH_DEFAULT = "/hive_sessions";

  private CuratorFramework zkClient;
  private String zkBasePath;
  private long ttlMillis;
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Override
  public void init(HiveConf conf) {
    String quorum = conf.getVar(ConfVars.HIVE_ZOOKEEPER_QUORUM);
    int sessionTimeout = (int) conf.getTimeVar(
        ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT, TimeUnit.MILLISECONDS);
    int connectionTimeout = (int) conf.getTimeVar(
        ConfVars.HIVE_ZOOKEEPER_CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS);
    int baseSleepTime = (int) conf.getTimeVar(
        ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME, TimeUnit.MILLISECONDS);
    int maxRetries = conf.getIntVar(ConfVars.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES);

    this.zkBasePath = conf.get(CONF_ZK_PATH, CONF_ZK_PATH_DEFAULT);
    this.ttlMillis = conf.getTimeVar(
        ConfVars.HIVE_SERVER2_SESSION_STATE_STORE_TTL, TimeUnit.MILLISECONDS);

    zkClient = CuratorFrameworkFactory.builder()
        .connectString(quorum)
        .sessionTimeoutMs(sessionTimeout)
        .connectionTimeoutMs(connectionTimeout)
        .retryPolicy(new ExponentialBackoffRetry(baseSleepTime, maxRetries))
        .build();
    zkClient.start();

    try {
      zkClient.create().creatingParentsIfNeeded().forPath(zkBasePath);
    } catch (KeeperException.NodeExistsException e) {
      LOG.debug("ZooKeeper base path already exists: {}", zkBasePath);
    } catch (Exception e) {
      LOG.error("Failed to create ZooKeeper base path: {}", zkBasePath, e);
      throw new RuntimeException("Failed to initialize ZooKeeperSessionStateStore", e);
    }

    LOG.info("Initialized ZooKeeperSessionStateStore with quorum={}, basePath={}, ttl={}ms",
        quorum, zkBasePath, ttlMillis);
  }

  @Override
  public void saveSnapshot(String sessionHandleId, HiveSessionSnapshot snapshot) {
    String path = getNodePath(sessionHandleId);
    try {
      byte[] data = OBJECT_MAPPER.writeValueAsBytes(snapshot);
      try {
        zkClient.delete().forPath(path);
      } catch (KeeperException.NoNodeException e) {
        // Node doesn't exist yet, fine
      }
      zkClient.create().withTtl(ttlMillis).creatingParentsIfNeeded()
          .withMode(CreateMode.PERSISTENT_WITH_TTL)
          .forPath(path, data);
      LOG.debug("Saved session snapshot to ZooKeeper: {}", path);
    } catch (Exception e) {
      LOG.error("Failed to save session snapshot to ZooKeeper: {}", path, e);
      throw new RuntimeException("Failed to save session snapshot", e);
    }
  }

  @Override
  public HiveSessionSnapshot getSnapshot(String sessionHandleId) {
    String path = getNodePath(sessionHandleId);
    try {
      if (zkClient.checkExists().forPath(path) == null) {
        return null;
      }
      byte[] data = zkClient.getData().forPath(path);
      return OBJECT_MAPPER.readValue(data, HiveSessionSnapshot.class);
    } catch (KeeperException.NoNodeException e) {
      return null;
    } catch (Exception e) {
      LOG.error("Failed to get session snapshot from ZooKeeper: {}", path, e);
      throw new RuntimeException("Failed to get session snapshot", e);
    }
  }

  @Override
  public void deleteSnapshot(String sessionHandleId) {
    String path = getNodePath(sessionHandleId);
    try {
      if (zkClient.checkExists().forPath(path) != null) {
        zkClient.delete().forPath(path);
      }
      LOG.debug("Deleted session snapshot from ZooKeeper: {}", path);
    } catch (KeeperException.NoNodeException e) {
      // Already gone, ignore
    } catch (Exception e) {
      LOG.error("Failed to delete session snapshot from ZooKeeper: {}", path, e);
      throw new RuntimeException("Failed to delete session snapshot", e);
    }
  }

  @Override
  public void close() {
    if (zkClient != null) {
      zkClient.close();
      zkClient = null;
    }
  }

  private String getNodePath(String sessionHandleId) {
    return zkBasePath + "/" + sessionHandleId;
  }
}
