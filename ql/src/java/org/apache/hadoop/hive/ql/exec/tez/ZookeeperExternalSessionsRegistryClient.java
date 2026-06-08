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

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.tez.monitoring.TezJobMonitor;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

// TODO: tez should provide this registry
public class ZookeeperExternalSessionsRegistryClient implements ExternalSessionsRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperExternalSessionsRegistryClient.class);

  private final HiveConf initConf;
  private final Set<String> available = new HashSet<>();
  private final Set<String> taken = new HashSet<>();
  private final Object lock = new Object();
  private final int maxAttempts;
  private CuratorFramework client;
  private CuratorCache cache;
  private CuratorCache claimsCache;
  private InterProcessMutex globalQueue;
  private String sessionsPath;
  private String claimsPath;
  private volatile boolean isInitialized;


  public ZookeeperExternalSessionsRegistryClient(final HiveConf initConf) {
    this.initConf = initConf;
    this.maxAttempts = HiveConf.getIntVar(initConf, ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_WAIT_MAX_ATTEMPTS);
  }

  private static String getApplicationId(final ChildData childData) {
    return childData.getPath().substring(childData.getPath().lastIndexOf("/") + 1);
  }

  private void init() {
    String zkServer = HiveConf.getVar(initConf, ConfVars.HIVE_ZOOKEEPER_QUORUM);
    int sessionTimeoutMs = (int) HiveConf.getTimeVar(initConf, ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT,
        TimeUnit.MILLISECONDS);
    int connectionTimeoutMs = (int) HiveConf.getTimeVar(initConf, ConfVars.HIVE_ZOOKEEPER_CONNECTION_TIMEOUT,
        TimeUnit.MILLISECONDS);
    int baseSleepTimeMs = (int) HiveConf.getTimeVar(initConf, ConfVars.HIVE_ZOOKEEPER_CONNECTION_BASESLEEPTIME,
        TimeUnit.MILLISECONDS);
    int maxRetries = HiveConf.getIntVar(initConf, ConfVars.HIVE_ZOOKEEPER_CONNECTION_MAX_RETRIES);
    String zkNamespace = HiveConf.getVar(initConf, ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_NAMESPACE);
    this.sessionsPath = normalizeZkPath(zkNamespace);
    this.claimsPath = this.sessionsPath + "-claims";
    // After connection state changes to SUSPENDED, the client has already consumed ~2/3 of the negotiated session
    // timeout. Use 33% of the remaining window so LOST aligns with when the ZK server expires the session and drops
    // ephemeral claim nodes. For Ref: Curator TN14 
    this.client = CuratorFrameworkFactory.builder()
        .connectString(zkServer)
        .sessionTimeoutMs(sessionTimeoutMs)
        .connectionTimeoutMs(connectionTimeoutMs)
        .simulatedSessionExpirationPercent(33)
        .retryPolicy(new ExponentialBackoffRetry(baseSleepTimeMs, maxRetries))
        .build();

    synchronized (lock) {
      client.start();

      client.getConnectionStateListenable().addListener(new ConnectionStateListener() {
        @Override
        public void stateChanged(CuratorFramework client, ConnectionState newState) {
          if (newState == ConnectionState.LOST) {
            Set<String> sessionsToKill;
            synchronized (lock) {
              LOG.error("ZK connection state has changed to lost; killing running DAGs on claimed AMs: {}", taken);
              sessionsToKill = new HashSet<>(taken);
              taken.clear();
            }
            for (String appId : sessionsToKill) {
              TezJobMonitor.killRunningDAGsForApplication(appId);
            }
          }
        }
      });

      this.globalQueue = new InterProcessMutex(client, sessionsPath + "-queue");
      this.cache = CuratorCache.build(client, sessionsPath);
      CuratorCacheListener listener = CuratorCacheListener.builder()
          .forPathChildrenCache(sessionsPath, client, new ExternalSessionsPathListener())
          .build();
      cache.listenable().addListener(listener);
      cache.start();

      this.claimsCache = CuratorCache.build(client, claimsPath);
      CuratorCacheListener claimsListener = CuratorCacheListener.builder().forCreates(
          childData -> {
        if (childData == null) {
          return;
        }
        String applicationId = getApplicationId(childData);
        synchronized (lock) {
          available.remove(applicationId);
        }
      }).forDeletes(
          childData -> {
        if (childData == null) {
          return;
        }
        String applicationId = getApplicationId(childData);
        synchronized (lock) {
          if (!taken.contains(applicationId)) {
            if (cache.get(sessionsPath + "/" + applicationId).isPresent()) {
              available.add(applicationId);
              lock.notifyAll();
            } else {
              LOG.info("Ignoring AM claim removal for {} because the base AM node no longer exists.", applicationId);
            }
          }
        }
      }).build();
      claimsCache.listenable().addListener(claimsListener);
      claimsCache.start();

      cache.stream()
          .filter(childData -> childData.getPath() != null
              && childData.getPath().startsWith(sessionsPath + "/"))
          .forEach(childData -> available.add(getApplicationId(childData)));
      LOG.info("Initial external sessions: {}", available);
      isInitialized = true;
    }
  }

  @VisibleForTesting
  static String normalizeZkPath(String zkNamespace) {
    return (zkNamespace.startsWith("/") ? zkNamespace : "/" + zkNamespace);
  }

  @Override
  public String getSession() throws Exception {
    if (!isInitialized) {
      synchronized (lock) {
        if (!isInitialized) {
          init();
        }
      }
    }
    
    long startTimeNs = System.nanoTime();
    long timeoutNs = TimeUnit.SECONDS.toNanos(maxAttempts);
    long queueWaitTimeMs = Math.max(0, (timeoutNs - (System.nanoTime() - startTimeNs)) / 1000000L);
    if (!globalQueue.acquire(queueWaitTimeMs, TimeUnit.MILLISECONDS)) {
      throw new IOException("Cannot get a session (timed out in queue) after " + maxAttempts + " seconds");
    }
    try {
      synchronized (lock) {
        while (System.nanoTime() - startTimeNs < timeoutNs) {
          Iterator<String> iter = available.iterator();

          while (iter.hasNext()) {
            String appId = iter.next();
            try {
              String claimNodePath = claimsPath + "/" + appId;
              client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(claimNodePath);
              iter.remove();
              taken.add(appId);
              return appId;
            } catch (KeeperException.NodeExistsException e) {
              iter.remove();
            }
          }
          long remainingTimeNs = timeoutNs - (System.nanoTime() - startTimeNs);
          if (remainingTimeNs > 0) {
            long waitTimeMs = Math.min(1000L, remainingTimeNs / 1_000_000L);
            lock.wait(waitTimeMs);
          }
        }
        throw new IOException("Cannot get a session after waiting for " + maxAttempts + " seconds (timeout exhausted)");
      }
    } finally {
      globalQueue.release();
    }
  }

  @Override
  public void returnSession(String appId) {
    synchronized (lock) {
      if (!isInitialized) {
        throw new IllegalStateException("Not initialized");
      }
      if (!taken.remove(appId)) {
        return; // Session has been removed from ZK.
      }

      try {
        client.delete().guaranteed().forPath(claimsPath + "/" + appId);
      } catch (KeeperException.NoNodeException e) {
        // If the claim Node has already been deleted, we can ignore it.
        LOG.warn("Claim Node has already been deleted for the session {}", appId, e);
      } catch (Exception e) {
        LOG.warn("Failed to delete claim node for session {}", appId, e);
      }

      available.add(appId);
      lock.notifyAll();
    }
  }

  @Override
  public void close() {
    if (claimsCache != null) {
      claimsCache.close();
    }
    if (cache != null) {
      cache.close();
    }
    if (client != null) {
      client.close();
    }
  }

  private final class ExternalSessionsPathListener implements PathChildrenCacheListener {

    @Override
    public void childEvent(final CuratorFramework client, final PathChildrenCacheEvent event) throws Exception {
      Preconditions.checkArgument(client != null && client.getState() == CuratorFrameworkState.STARTED,
          "client is not started");
      ChildData childData = event.getData();
      if (childData == null) {
        return;
      }
      String applicationId = getApplicationId(childData);
      LOG.info("{} for external session {}", event.getType(), applicationId);

      synchronized (lock) {
        switch (event.getType()) {
        case CHILD_UPDATED, CHILD_ADDED:
          if (available.contains(applicationId) || taken.contains(applicationId)) {
            return; // We do not expect updates to existing sessions; ignore them for now.
          }
          if (claimsCache.get(claimsPath + "/" + applicationId).isPresent()) {
            LOG.info("Ignoring newly added AM {} because it is already claimed by another session.", applicationId);
            return;
          }
          available.add(applicationId);
          lock.notifyAll();
          break;
        case CHILD_REMOVED:
          if (taken.remove(applicationId)) {
            LOG.warn("The session in use has disappeared from the registry ({})", applicationId);
          } else if (!available.remove(applicationId)) {
            LOG.warn("An unknown session has been removed ({})", applicationId);
          }
          break;
        default:
          // Ignore all the other events; logged above.
        }
      }
    }
  }
}
