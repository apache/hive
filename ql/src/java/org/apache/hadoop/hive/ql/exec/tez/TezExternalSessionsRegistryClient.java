/*
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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.client.registry.AMRegistryClientListener;
import org.apache.tez.client.registry.zookeeper.ZkAMRegistryClient;
import org.apache.tez.dag.api.TezConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TezExternalSessionsRegistryClient {
  private static final Logger LOG = LoggerFactory.getLogger(TezExternalSessionsRegistryClient.class);

  private final HashSet<String> available = new HashSet<>(), taken = new HashSet<>();
  private final Object lock = new Object();
  private final int maxAttempts;
  private ZkAMRegistryClient tezRegistryClient;

  private static Map<String, TezExternalSessionsRegistryClient> INSTANCES = new HashMap<>();

  public static synchronized TezExternalSessionsRegistryClient getClient(final Configuration conf) {
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

  private TezExternalSessionsRegistryClient(final Configuration conf) {
    this.maxAttempts = HiveConf.getIntVar(conf,
      ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_WAIT_MAX_ATTEMPTS);
    this.tezRegistryClient = ZkAMRegistryClient.getClient(conf);
    // add listener before init to get events from the beginning (path children build initial cache)
    this.tezRegistryClient.addListener(new AMStateListener(available, taken, lock));
    try {
      this.tezRegistryClient.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    List<AMRecord> initialSessions = this.tezRegistryClient.getAllRecords();
    for (AMRecord amRecord : initialSessions) {
      if (amRecord != null && amRecord.getApplicationId() != null) {
        this.available.add(amRecord.getApplicationId().toString());
      }
    }
    LOG.info("Found {} initial external sessions.", this.available.size());
  }

  private static class AMStateListener implements AMRegistryClientListener {
    private final HashSet<String> available;
    private final HashSet<String> taken;
    private final Object lock;

    AMStateListener(final HashSet<String> available, final HashSet<String> taken, final Object lock) {
      this.available = available;
      this.taken = taken;
      this.lock = lock;
    }

    @Override
    public void onAdd(final AMRecord amRecord) {
      if (amRecord != null && amRecord.getApplicationId() != null) {
        final String appId = amRecord.getApplicationId().toString();
        synchronized (lock) {
          // this is just a safeguard, if an appId is already taken, then new AM with the same appId cannot appear.
          if (!taken.contains(appId) && !available.contains(appId)) {
            available.add(appId);
            LOG.info("Adding external session with applicationId: {}", appId);
            lock.notifyAll();
          }
        }
      }
    }

    @Override
    public void onRemove(final AMRecord amRecord) {
      if (amRecord != null && amRecord.getApplicationId() != null) {
        final String appId = amRecord.getApplicationId().toString();
        synchronized (lock) {
          available.remove(appId);
          taken.remove(appId);
          LOG.info("Removed external session with applicationId: {}", appId);
        }
      }
    }
  }

  public void close() {
    if (tezRegistryClient != null) {
      tezRegistryClient.close();
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
