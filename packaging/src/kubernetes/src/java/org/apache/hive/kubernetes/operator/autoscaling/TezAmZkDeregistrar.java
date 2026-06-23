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
 * Each TezAM registers at {@code /tez-external-sessions/<llapName>/<applicationId>}.
 * When we delete these nodes, every HS2 instance (including all replicas in an
 * active-active HA setup) sees a {@code CHILD_REMOVED} ZK event and immediately
 * stops routing new sessions to those AMs. This prevents new DAGs from arriving
 * on a pod that is about to be terminated.
 * <p>
 * Only the registration node is deleted — the ephemeral claim node
 * ({@code /tez-external-sessions/<llapName>-claims/<applicationId>}) is not touched;
 * it belongs to the HS2 ZK session and disappears naturally when HS2 releases it.
 * <p>
 * ZK connection parameters are read from the cluster's HS2 configOverrides using the
 * same keys that {@code ZookeeperExternalSessionsRegistryClient} reads from HiveConf:
 * <ul>
 *   <li>{@code hive.zookeeper.connection.timeout} (default: 15s → 15000 ms)</li>
 *   <li>{@code hive.zookeeper.session.timeout} (default: 120000ms)</li>
 *   <li>{@code hive.zookeeper.connection.basesleeptime} (default: 1000ms)</li>
 *   <li>{@code hive.zookeeper.connection.max.retries} (default: 3)</li>
 * </ul>
 */
public final class TezAmZkDeregistrar {

  private static final Logger LOG = LoggerFactory.getLogger(TezAmZkDeregistrar.class);

  private TezAmZkDeregistrar() {}

  /**
   * Deletes ZK registration nodes for the given idle TezAM pods.
   * Failures are logged as warnings and do not block the scale-down — the
   * preStop drain on the pod provides a safety net.
   *
   * @param zkQuorum       ZooKeeper connection string
   * @param llapName       LLAP cluster name (e.g. "llap0"), used as the ZK namespace
   * @param idlePodNames   pod names that are idle and about to be removed
   * @param hiveSiteConfig HS2 configOverrides map — used to read ZK connection settings
   *                       using the same keys as {@code ZookeeperExternalSessionsRegistryClient}
   */
  public static void deregisterIdlePods(String zkQuorum, String llapName,
      List<String> idlePodNames, Map<String, String> hiveSiteConfig) {
    if (idlePodNames.isEmpty()) {
      return;
    }
    int connTimeoutMs = getTimeMs(hiveSiteConfig, "hive.zookeeper.connection.timeout", 15000);
    int sessionTimeoutMs = getTimeMs(hiveSiteConfig, "hive.zookeeper.session.timeout", 120000);
    int baseSleepMs = getTimeMs(hiveSiteConfig, "hive.zookeeper.connection.basesleeptime", 1000);
    int maxRetries = getInt(hiveSiteConfig, "hive.zookeeper.connection.max.retries", 3);

    String registryPath = ConfigUtils.TEZ_EXTERNAL_SESSIONS_ZK_PREFIX + "/" + llapName;
    CuratorFramework client = CuratorFrameworkFactory.builder()
        .connectString(zkQuorum)
        .connectionTimeoutMs(connTimeoutMs)
        .sessionTimeoutMs(sessionTimeoutMs)
        .retryPolicy(new ExponentialBackoffRetry(baseSleepMs, maxRetries))
        .build();
    try {
      client.start();
      if (!client.blockUntilConnected(connTimeoutMs, TimeUnit.MILLISECONDS)) {
        LOG.warn("[tezam-{}] ZK connect timeout — skipping deregistration", llapName);
        return;
      }
      if (client.checkExists().forPath(registryPath) == null) {
        return;
      }
      for (String appId : client.getChildren().forPath(registryPath)) {
        String nodePath = registryPath + "/" + appId;
        byte[] data = client.getData().forPath(nodePath);
        if (data == null || data.length == 0) {
          continue;
        }
        String hostName = extractHostName(new String(data, StandardCharsets.UTF_8));
        if (hostName == null) {
          continue;
        }
        // hostName = "<podName>.<svcName>.<ns>.svc.cluster.local"
        String podName = hostName.contains(".") ? hostName.substring(0, hostName.indexOf('.')) : hostName;
        if (idlePodNames.contains(podName)) {
          try {
            client.delete().forPath(nodePath);
            LOG.info("[tezam-{}] Deregistered pod {} (appId={}) from ZK — HS2 will stop routing to it",
                llapName, podName, appId);
          } catch (Exception e) {
            LOG.warn("[tezam-{}] Failed to delete ZK node for pod {}: {}", llapName, podName, e.getMessage());
          }
        }
      }
    } catch (Exception e) {
      LOG.warn("[tezam-{}] ZK deregistration error: {}", llapName, e.getMessage());
    } finally {
      client.close();
    }
  }

  /**
   * Reads a time value (in milliseconds) from the config map.
   * If the key is absent or un-parseable the defaultMs value is returned.
   */
  static int getTimeMs(Map<String, String> config, String key, int defaultMs) {
    if (config == null) {
      return defaultMs;
    }
    String val = config.get(key);
    if (val == null) {
      return defaultMs;
    }
    val = val.trim();
    try {
      if (val.endsWith("ms")) {
        return Integer.parseInt(val.substring(0, val.length() - 2).trim());
      }
      if (val.endsWith("s")) {
        return (int) (Double.parseDouble(val.substring(0, val.length() - 1).trim()) * 1_000);
      }
      if (val.endsWith("m")) {
        return (int) (Double.parseDouble(val.substring(0, val.length() - 1).trim()) * 60_000);
      }
      return Integer.parseInt(val);
    } catch (NumberFormatException e) {
      LOG.debug("Unparseable ZK config '{}' = '{}', using default {}ms", key, val, defaultMs);
      return defaultMs;
    }
  }

  static int getInt(Map<String, String> config, String key, int defaultVal) {
    if (config == null) {
      return defaultVal;
    }
    String val = config.get(key);
    if (val == null) {
      return defaultVal;
    }
    try {
      return Integer.parseInt(val.trim());
    } catch (NumberFormatException e) {
      LOG.debug("Unparseable ZK config '{}' = '{}', using default {}", key, val, defaultVal);
      return defaultVal;
    }
  }

  static String extractHostName(String json) {
    String marker = "\"hostName\":\"";
    int start = json.indexOf(marker);
    if (start < 0) {
      return null;
    }
    start += marker.length();
    int end = json.indexOf('"', start);
    return end > start ? json.substring(start, end) : null;
  }
}
