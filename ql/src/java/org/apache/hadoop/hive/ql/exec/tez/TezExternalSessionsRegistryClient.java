/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

// TODO: tez should provide this registry
public class TezExternalSessionsRegistryClient {
  private static final Logger LOG = LoggerFactory.getLogger(TezExternalSessionsRegistryClient.class);

  // TODO: internal only for now to start and reconnect to external session
  private static final String ZK_PATH = "/tez_am/server";
  private final HiveConf initConf;
  private final HashSet<String> available = new HashSet<>(), taken = new HashSet<>();
  private final Object lock = new Object();
  private final int maxAttempts;

  private PathChildrenCache cache;
  private boolean isInitialized;


  public TezExternalSessionsRegistryClient(final HiveConf initConf) {
    this.initConf = initConf;
    this.maxAttempts = HiveConf.getIntVar(initConf,
        ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_WAIT_MAX_ATTEMPTS);
  }

  public void close() {
    if (cache != null) {
      try {
        cache.close();
      } catch (IOException e) {
        LOG.error("Failed to close the cache: {}", e.getMessage());
        LOG.debug("Failed to close the cache", e);
      }
    }
  }

  private void init() throws Exception {
    String zkServer = HiveConf.getVar(initConf, ConfVars.HIVE_ZOOKEEPER_QUORUM);
    String zkNamespace = HiveConf.getVar(initConf,
        ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_NAMESPACE);
    String effectivePath = zkNamespace + ZK_PATH;
    CuratorFramework client = CuratorFrameworkFactory.newClient(zkServer,
      new ExponentialBackoffRetry(1000, 3));
    synchronized (lock) {
      this.cache = new PathChildrenCache(client, effectivePath, true);
      client.start();
      cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
      for (ChildData childData : cache.getCurrentData()) {
        available.add(getApplicationId(childData));
      }
      cache.getListenable().addListener(new ExternalSessionsPathListener());
      LOG.info("Initial external sessions: {}", available);
      isInitialized = true;
    }
  }

  private static String getApplicationId(final ChildData childData) {
    return childData.getPath().substring(childData.getPath().lastIndexOf("/") + 1);
  }

  private class ExternalSessionsPathListener implements PathChildrenCacheListener {

    @Override
    public void childEvent(final CuratorFramework client,
        final PathChildrenCacheEvent event) throws Exception {
      Preconditions.checkArgument(client != null
        && client.getState() == CuratorFrameworkState.STARTED, "client is not started");
      ChildData childData = event.getData();
      if (childData == null) {
        return;
      }
      String applicationId = getApplicationId(childData);
      LOG.info("{} for external session {}", event.getType(), applicationId);

      synchronized (lock) {
        switch (event.getType()) {
          case CHILD_UPDATED:
          case CHILD_ADDED:
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

  public String getSession(boolean isLimitedWait) throws Exception, InterruptedException {
    synchronized (lock) {
      if (!isInitialized) {
        init();
      }
      long endTimeNs = System.nanoTime() + (1000000000 * maxAttempts);
      while (available.isEmpty() && (!isLimitedWait || ((endTimeNs - System.nanoTime()) > 0))) {
        lock.wait(1000L);
      }
      Iterator<String> iter = available.iterator();
      if (!iter.hasNext()) {
        assert isLimitedWait;
        throw new IOException("Cannot get a session after " + maxAttempts + " attempts");
      }
      String appId = iter.next();
      iter.remove();
      taken.add(appId);
      return appId;
    }
  }

  public void returnSession(String appId) {
    synchronized (lock) {
      if (!isInitialized) {
        throw new AssertionError("Not initialized");
      }
      if (!taken.remove(appId)) {
        return; // Session has been removed from ZK.
      }
      available.add(appId);
      lock.notifyAll();
    }
  }
}
