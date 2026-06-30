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

package org.apache.hive.kubernetes.operator.autoscaling;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hive.kubernetes.operator.util.ConfigUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Deletes idle TezAM ZooKeeper registration nodes before a scale-down is applied.
 * <p>
 * A {@link CuratorFramework} client is cached per HiveCluster and reused across
 * scale-downs; it is closed when the cluster is deleted or ZK config changes.
 */
public final class TezAmZkDeregistrar {

  private static final Logger LOG = LoggerFactory.getLogger(TezAmZkDeregistrar.class);
  private static final String PATH_SEPARATOR = "/";

  private record ZkClientRecord(String zkQuorum, int connTimeoutMs, int sessionTimeoutMs,
      int baseSleepMs, int maxRetries, CuratorFramework client) {}

  private static final ConcurrentHashMap<String, ZkClientRecord> ZK_CLIENTS = new ConcurrentHashMap<>();

  private TezAmZkDeregistrar() {}

  public static void deregisterIdlePods(String namespace, String clusterName, String zkQuorum, String llapName,
      List<String> idlePodNames, Map<String, String> hiveSiteConfig) {
    if (idlePodNames.isEmpty()) {
      return;
    }
    ZkClientRecord zkRecord = getZKRecord(namespace, clusterName, zkQuorum, hiveSiteConfig);
    CuratorFramework client = zkRecord.client();
    try {
      if (!client.blockUntilConnected(zkRecord.connTimeoutMs(), TimeUnit.MILLISECONDS)) {
        LOG.warn("[tezam-{}] ZK connect timeout — skipping deregistration", llapName);
        return;
      }
      String registryPath = ConfigUtils.TEZ_EXTERNAL_SESSIONS_ZK_PREFIX + PATH_SEPARATOR + llapName;
      if (client.checkExists().forPath(registryPath) == null) {
        return;
      }
      for (String appId : client.getChildren().forPath(registryPath)) {
        String nodePath = registryPath + PATH_SEPARATOR + appId;
        byte[] data = client.getData().forPath(nodePath);
        if (data == null || data.length == 0) {
          continue;
        }
        String hostName = ConfigUtils.getJsonStringField(new String(data, StandardCharsets.UTF_8), "hostName");
        if (hostName != null) {
          // hostName = "<podName>.<svcName>.<ns>.svc.cluster.local"
          String podName = hostName.contains(".") ? hostName.substring(0, hostName.indexOf('.')) : hostName;
          if (idlePodNames.contains(podName)) {
            deleteRegistrationNode(client, llapName, nodePath, podName, appId);
          }
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("[tezam-{}] ZK deregistration interrupted: {}", llapName, e.getMessage());
    } catch (Exception e) {
      LOG.warn("[tezam-{}] ZK deregistration error: {}", llapName, e.getMessage());
    }
  }

  private static ZkClientRecord getZKRecord(String namespace, String clusterName,
      String zkQuorum, Map<String, String> hiveSiteConfig) {
    int connTimeoutMs = ConfigUtils.getTimeMs(hiveSiteConfig, ConfigUtils.HIVE_ZOOKEEPER_CONNECTION_TIMEOUT_KEY,
        ConfigUtils.HIVE_ZOOKEEPER_CONNECTION_TIMEOUT_DEFAULT_MS);
    int sessionTimeoutMs = ConfigUtils.getTimeMs(hiveSiteConfig, ConfigUtils.HIVE_ZOOKEEPER_SESSION_TIMEOUT_KEY,
        ConfigUtils.HIVE_ZOOKEEPER_SESSION_TIMEOUT_DEFAULT_MS);
    int baseSleepMs = ConfigUtils.getTimeMs(hiveSiteConfig, ConfigUtils.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME_KEY,
        ConfigUtils.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME_DEFAULT_MS);
    int maxRetries = ConfigUtils.getInt(hiveSiteConfig, ConfigUtils.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES_KEY,
        null, ConfigUtils.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES_DEFAULT);

    synchronized (ZK_CLIENTS) {
      String key = namespace + PATH_SEPARATOR + clusterName;
      ZkClientRecord cachedRecord = ZK_CLIENTS.get(key);
      if (cachedRecord != null && cachedRecord.zkQuorum().equals(zkQuorum)
          && cachedRecord.connTimeoutMs() == connTimeoutMs
          && cachedRecord.sessionTimeoutMs() == sessionTimeoutMs
          && cachedRecord.baseSleepMs() == baseSleepMs
          && cachedRecord.maxRetries() == maxRetries) {
        return cachedRecord;
      }
      if (cachedRecord != null) {
        closeClient(cachedRecord.client());
        LOG.info("Recreating cached ZK client for {} (quorum or connection settings changed)", key);
      }
      CuratorFramework client = CuratorFrameworkFactory.builder()
          .connectString(zkQuorum)
          .connectionTimeoutMs(connTimeoutMs)
          .sessionTimeoutMs(sessionTimeoutMs)
          .retryPolicy(new ExponentialBackoffRetry(baseSleepMs, maxRetries))
          .build();
      client.start();
      ZkClientRecord zkRecord = new ZkClientRecord(zkQuorum, connTimeoutMs, sessionTimeoutMs,
          baseSleepMs, maxRetries, client);
      ZK_CLIENTS.put(key, zkRecord);
      return zkRecord;
    }
  }


  private static void deleteRegistrationNode(CuratorFramework client, String llapName,
      String nodePath, String podName, String appId) {
    try {
      client.delete().forPath(nodePath);
      LOG.info("[tezam-{}] Deregistered pod {} (appId={}) from ZK — HS2 will stop routing to it",
          llapName, podName, appId);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.warn("[tezam-{}] Failed to delete ZK node for pod {} (interrupted): {}",
          llapName, podName, e.getMessage());
    } catch (Exception e) {
      LOG.warn("[tezam-{}] Failed to delete ZK node for pod {}: {}", llapName, podName, e.getMessage());
    }
  }

  private static void closeClient(CuratorFramework client) {
    try {
      client.close();
    } catch (Exception e) {
      LOG.debug("Error closing ZK client: {}", e.getMessage());
    }
  }

  public static void cleanupCluster(String namespace, String clusterName) {
    ZkClientRecord zkRecord = ZK_CLIENTS.remove(namespace + PATH_SEPARATOR + clusterName);
    if (zkRecord != null) {
      closeClient(zkRecord.client());
    }
  }
}
