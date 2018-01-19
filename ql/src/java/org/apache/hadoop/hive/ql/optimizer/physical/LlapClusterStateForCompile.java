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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.util.concurrent.ExecutionException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstanceSet;
import org.apache.hadoop.hive.llap.registry.impl.InactiveServiceInstance;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LlapClusterStateForCompile {
  protected static final Logger LOG = LoggerFactory.getLogger(LlapClusterStateForCompile.class);

  private static final long CLUSTER_UPDATE_INTERVAL_MS = 120 * 1000L; // 2 minutes.
  private volatile Long lastClusterUpdateNs;
  private volatile Integer noConfigNodeCount, executorCount;
  private volatile int numExecutorsPerNode = -1;
  private LlapRegistryService svc;
  private final Configuration conf;
  private final long updateIntervalNs;
  /** Synchronizes the actual update from the cluster; only one thread at a time. */
  private final Object updateInfoLock = new Object();

  // It's difficult to impossible to pass global things to compilation, so we have a static cache.
  private static final Cache<String, LlapClusterStateForCompile> CACHE =
      CacheBuilder.newBuilder().initialCapacity(10).maximumSize(100).build();

  public static LlapClusterStateForCompile getClusterInfo(final Configuration conf) {
    final String nodes = HiveConf.getTrimmedVar(conf, HiveConf.ConfVars.LLAP_DAEMON_SERVICE_HOSTS);
    final String userName = HiveConf.getVar(
            conf, ConfVars.LLAP_ZK_REGISTRY_USER, LlapRegistryService.currentUser());
    Callable<LlapClusterStateForCompile> generator = new Callable<LlapClusterStateForCompile>() {
      @Override
      public LlapClusterStateForCompile call() throws Exception {
        LOG.info("Creating cluster info for " + userName + ":" + nodes);
        return new LlapClusterStateForCompile(conf, CLUSTER_UPDATE_INTERVAL_MS);
      }
    };
    try {
      return CACHE.get(userName + ":" + nodes, generator);
    } catch (ExecutionException e) {
      throw new RuntimeException(e); // Should never happen... ctor is just assignments.
    }
  }

  public LlapClusterStateForCompile(Configuration conf, long updateIntervalMs) {
    this.conf = conf;
    this.updateIntervalNs = updateIntervalMs * 1000000L;
  }

  public boolean hasClusterInfo() {
    return lastClusterUpdateNs != null;
  }

  public int getKnownExecutorCount() {
    return executorCount;
  }

  public int getNodeCountWithUnknownExecutors() {
    return noConfigNodeCount;
  }

  public int getNumExecutorsPerNode() {
    return numExecutorsPerNode;
  }

  private boolean isUpdateNeeded() {
    Long lastUpdateLocal = lastClusterUpdateNs;
    if (lastUpdateLocal == null) return true;
    long elapsed = System.nanoTime() - lastUpdateLocal;
    return (elapsed >= updateIntervalNs);
  }

  public boolean initClusterInfo() {
    if (!isUpdateNeeded()) return true;
    synchronized (updateInfoLock) {
      // At this point, no one will take the write lock and update, so we can do the last check.
      if (!isUpdateNeeded()) return true;
      if (svc == null) {
        try {
          svc = LlapRegistryService.getClient(conf);
        } catch (Throwable t) {
          LOG.info("Cannot create the client; ignoring", t);
          return false; // Don't fail; this is best-effort.
        }
      }
      LlapServiceInstanceSet instances;
      try {
        instances = svc.getInstances(10);
      } catch (IOException e) {
        LOG.info("Cannot update cluster information; ignoring", e);
        return false; // Don't wait for the cluster if not started; this is best-effort.
      }
      int executorsLocal = 0, noConfigNodesLocal = 0;
      for (LlapServiceInstance si : instances.getAll()) {
        if (si instanceof InactiveServiceInstance) continue; // Shouldn't happen in getAll.
        Map<String, String> props = si.getProperties();
        if (props == null) {
          ++noConfigNodesLocal;
          continue;
        }
        try {
          int numExecutors = Integer.parseInt(props.get(ConfVars.LLAP_DAEMON_NUM_EXECUTORS.varname));
          executorsLocal += numExecutors;
          if (numExecutorsPerNode == -1) {
            numExecutorsPerNode = numExecutors;
          }
        } catch (NumberFormatException e) {
          ++noConfigNodesLocal;
        }
      }
      noConfigNodeCount = noConfigNodesLocal;
      executorCount = executorsLocal;
      lastClusterUpdateNs = System.nanoTime();
      return true;
    }
  }
}