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
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
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
    String zkNamespace = HiveConf.getVar(initConf, ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_NAMESPACE);
    String effectivePath = normalizeZkPath(zkNamespace);
    String queuePath = effectivePath + "-queue";
    this.claimsPath = effectivePath + "-claims";
    this.client = CuratorFrameworkFactory.newClient(zkServer, new ExponentialBackoffRetry(1000, 3));
    
    synchronized (lock) {
      client.start();
      this.globalQueue = new InterProcessMutex(client, queuePath);
      this.cache = CuratorCache.build(client, effectivePath);
      CuratorCacheListener listener = CuratorCacheListener.builder()
          .forPathChildrenCache(effectivePath, client, new ExternalSessionsPathListener())
          .build();
      cache.listenable().addListener(listener);
      cache.start();

      this.claimsCache = CuratorCache.build(client, claimsPath);
      CuratorCacheListener claimsListener = CuratorCacheListener.builder()
          .forPathChildrenCache(claimsPath, client, new ClaimsPathListener())
          .build();
      claimsCache.listenable().addListener(claimsListener);
      claimsCache.start();

      cache.stream()
          .filter(childData -> childData.getPath() != null
              && childData.getPath().startsWith(effectivePath + "/"))
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
    
    long endTimeNs = System.nanoTime() + (1000000000L * maxAttempts);
    long queueWaitTimeMs = Math.max(0, (endTimeNs - System.nanoTime()) / 1000000L);
    if (!globalQueue.acquire(queueWaitTimeMs, TimeUnit.MILLISECONDS)) {
      throw new IOException("Cannot get a session (timed out in queue) after " + maxAttempts + " seconds");
    }

    try {
      synchronized (lock) {
        while (System.nanoTime() < endTimeNs) {
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
          long remainingTimeNs = endTimeNs - System.nanoTime();
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
        return; // Session has already been removed from ZK.
      }

      try {
        client.delete().guaranteed().forPath(claimsPath + "/" + appId);
      } catch (KeeperException.NoNodeException e) {
        // If the claim Node has already been deleted, we can ignore it.
        LOG.debug("Claim Node has already been deleted for the session {}", appId, e);
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

  private final class ClaimsPathListener implements PathChildrenCacheListener {
    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) {
      ChildData childData = event.getData();
      if (childData == null) {
        return;
      }

      String applicationId = getApplicationId(childData);
      synchronized (lock) {
        switch (event.getType()) {
        case CHILD_REMOVED:
          if (!taken.contains(applicationId)) {
            // if the claim node was released by this particular HS2 itself,
            // it will be added back to the available list & locks are notified as part of returnSession()
            available.add(applicationId);
            lock.notifyAll();
          }
          break;
        case CHILD_ADDED:
          // A Tez AM was claimed by another HS2, so remove the AM from the available list of this particular HS2
          available.remove(applicationId);
          break;
        default:
          break;
        }
      }
    }
  }
}
