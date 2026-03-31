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

import com.google.common.annotations.VisibleForTesting;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
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

  private CuratorCache cache;
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
    CuratorFramework client = CuratorFrameworkFactory.newClient(zkServer, new ExponentialBackoffRetry(1000, 3));
    synchronized (lock) {
      client.start();
      this.cache = CuratorCache.build(client, effectivePath);
      CuratorCacheListener listener = CuratorCacheListener.builder()
          .forPathChildrenCache(effectivePath, client, new ExternalSessionsPathListener())
          .build();
      cache.listenable().addListener(listener);
      cache.start();
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
    synchronized (lock) {
      if (!isInitialized) {
        init();
      }
      long endTimeNs = System.nanoTime() + (1000000000L * maxAttempts);
      while (available.isEmpty() && ((endTimeNs - System.nanoTime()) > 0)) {
        lock.wait(1000L);
      }
      Iterator<String> iter = available.iterator();
      if (!iter.hasNext()) {
        throw new IOException("Cannot get a session after " + maxAttempts + " attempts");
      }
      String appId = iter.next();
      iter.remove();
      taken.add(appId);
      return appId;
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
      available.add(appId);
      lock.notifyAll();
    }
  }

  @Override
  public void close() {
    if (cache != null) {
      cache.close();
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
