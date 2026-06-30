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
 * same keys that {@code ZookeeperExternalSessionsRegistryClient} reads from HiveConf
 * (see {@link ConfigUtils} ZK constants).
 */
public final class TezAmZkDeregistrar {

  private static final Logger LOG = LoggerFactory.getLogger(TezAmZkDeregistrar.class);
  private static final String PATH_SEPARATOR = "/";

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
    int connTimeoutMs = ConfigUtils.getTimeMs(hiveSiteConfig, ConfigUtils.HIVE_ZOOKEEPER_CONNECTION_TIMEOUT_KEY,
        ConfigUtils.HIVE_ZOOKEEPER_CONNECTION_TIMEOUT_DEFAULT_MS);
    int sessionTimeoutMs = ConfigUtils.getTimeMs(hiveSiteConfig, ConfigUtils.HIVE_ZOOKEEPER_SESSION_TIMEOUT_KEY,
        ConfigUtils.HIVE_ZOOKEEPER_SESSION_TIMEOUT_DEFAULT_MS);
    int baseSleepMs = ConfigUtils.getTimeMs(hiveSiteConfig, ConfigUtils.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME_KEY,
        ConfigUtils.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME_DEFAULT_MS);
    int maxRetries = ConfigUtils.getInt(hiveSiteConfig, ConfigUtils.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES_KEY,
        null, ConfigUtils.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES_DEFAULT);

    String registryPath = ConfigUtils.TEZ_EXTERNAL_SESSIONS_ZK_PREFIX + PATH_SEPARATOR + llapName;
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
    } finally {
      client.close();
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
}
