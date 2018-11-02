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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.registry.impl.LlapRegistryService;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.client.registry.zookeeper.ZkAMRegistryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TezExternalSessionsRegistryClient {
  private static final Logger LOG = LoggerFactory.getLogger(TezExternalSessionsRegistryClient.class);

  private final HashSet<String> available = new HashSet<>(), taken = new HashSet<>();
  private final Object lock = new Object();
  private final int maxAttempts;
  private ZkAMRegistryClient tezRegistryClient;

  private static Map<String, TezExternalSessionsRegistryClient> INSTANCES = new HashMap<>();

  public static synchronized TezExternalSessionsRegistryClient getClient(final Configuration conf) throws Exception {
    String namespace = conf.get(TezConfiguration.TEZ_AM_REGISTRY_NAMESPACE);
    TezExternalSessionsRegistryClient registry = INSTANCES.get(namespace);
    if (registry == null) {
      registry = new TezExternalSessionsRegistryClient(conf);
      INSTANCES.put(namespace, registry);
    }
    LOG.info("Returning tez external AM registry ({}) for namespace '{}'", System.identityHashCode(registry),
      namespace);
    return registry;
  }

  private TezExternalSessionsRegistryClient(final Configuration conf) throws Exception {
    this.maxAttempts = HiveConf.getIntVar(conf,
      ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_WAIT_MAX_ATTEMPTS);
    this.tezRegistryClient = new ZkAMRegistryClient(conf);
    long delay = HiveConf.getTimeVar(conf, ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_REFRESH_INTERVAL,
      TimeUnit.SECONDS);
    final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
    executorService.scheduleWithFixedDelay(new AMRefresher(tezRegistryClient, lock, available, taken), 5, delay,
      TimeUnit.SECONDS);
  }

  public void close() {
    if (tezRegistryClient != null) {
      tezRegistryClient.close();
    }
  }

  private static class AMRefresher implements Runnable {
    private final HashSet<String> available;
    private final HashSet<String> taken;
    private final Object lock;
    private final ZkAMRegistryClient tezRegistryClient;

    AMRefresher(final ZkAMRegistryClient tezRegistryClient, final Object lock, final HashSet<String> available,
      final HashSet<String> taken) {
      this.tezRegistryClient = tezRegistryClient;
      this.lock = lock;
      this.available = available;
      this.taken = taken;
    }

    @Override
    public void run() {
      synchronized (lock) {
        // clear to remove old/dead entries
        available.clear();
        // TODO: This can be expensive if getAllRecords clones. We need a way for clients to register to updates from
        // path children cache so that clients can cache and avoid this periodic polling
        for (AMRecord amRecord : tezRegistryClient.getAllRecords()) {
          if (!taken.contains(amRecord.getApplicationId())) {
            available.add(amRecord.getApplicationId().toString());
          }
        }
        LOG.info("Refreshed external sessions. available[{}]: {}", available.size(), available);
        lock.notifyAll();
      }
    }
  }

  public String getSession(boolean isLimitedWait) throws Exception {
    synchronized (lock) {
      long endTimeNs = System.nanoTime() + (1000000000L * maxAttempts);
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
      LOG.info("External session taken: {}. available: {}", appId, available.size());
      return appId;
    }
  }

  public void returnSession(String appId) {
    synchronized (lock) {
      if (!taken.remove(appId)) {
        return; // Session has been removed from ZK.
      }
      available.add(appId);
      LOG.info("External session returned: {}. available: {}", appId, available.size());
      lock.notifyAll();
    }
  }
}
