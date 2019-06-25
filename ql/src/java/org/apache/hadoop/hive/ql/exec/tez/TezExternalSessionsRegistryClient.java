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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.client.registry.AMRegistryClientListener;
import org.apache.tez.client.registry.zookeeper.ZkAMRegistryClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

public class TezExternalSessionsRegistryClient implements ExternalSessionsRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(TezExternalSessionsRegistryClient.class);
  private static int DEFAULT_QUEUE_CAPACITY = 16;
  private Map<String, AMRecord> appIdAmRecordMap = new HashMap<>();
  private PriorityBlockingQueue<AMRecord> available;
  private HashSet<AMRecord> taken = new HashSet<>();
  private final int maxAttempts;
  private final SelectionStrategy selectionStrategy;
  private ZkAMRegistryClient tezRegistryClient;

  enum SelectionStrategy {
    BIN_PACK,
    ROUND_ROBIN,
    RANDOM
  }

  public TezExternalSessionsRegistryClient(final Configuration conf) {
    this.maxAttempts = HiveConf.getIntVar(conf, ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_WAIT_MAX_ATTEMPTS);
    String ss = HiveConf.getVar(conf, ConfVars.HIVE_SERVER2_TEZ_EXTERNAL_SESSIONS_ASSIGNMENT_STRATEGY);
    this.selectionStrategy = SelectionStrategy.valueOf(ss.toUpperCase());
    switch (selectionStrategy) {
      case BIN_PACK:
        this.available = new PriorityBlockingQueue<>(DEFAULT_QUEUE_CAPACITY, new AMRecordBinPackingComputeComparator());
        break;
      case ROUND_ROBIN:
        this.available = new PriorityBlockingQueue<>(DEFAULT_QUEUE_CAPACITY, new AMRecordRoundRobinComputeComparator());
        break;
      case RANDOM:
        this.available = new PriorityBlockingQueue<>(DEFAULT_QUEUE_CAPACITY, new AMRecordRandomComputeComparator());
        break;
    }
    try {
      this.tezRegistryClient = ZkAMRegistryClient.getClient(conf);
      // add listener before init to get events from the beginning (path children build initial cache)
      this.tezRegistryClient.addListener(new AMStateListener(appIdAmRecordMap, available, taken));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    LOG.info("Using selection strategy: {}", this.selectionStrategy);
  }

  private static class AMStateListener implements AMRegistryClientListener {
    private final PriorityBlockingQueue<AMRecord> available;
    private final HashSet<AMRecord> taken;
    private final Map<String, AMRecord> appIdAmRecordMap;

    AMStateListener(final Map<String, AMRecord> appIdAmRecordMap,
      final PriorityBlockingQueue<AMRecord> available, final HashSet<AMRecord> taken) {
      this.appIdAmRecordMap = appIdAmRecordMap;
      this.available = available;
      this.taken = taken;
    }

    @Override
    public void onAdd(final AMRecord amRecord) {
      if (amRecord != null && amRecord.getApplicationId() != null) {
        final String appId = amRecord.getApplicationId().toString();
        // this is just a safeguard, if an appId is already taken, then new AM with the same appId cannot appear.
        if (!taken.contains(amRecord) && !available.contains(amRecord)) {
          available.offer(amRecord);
          appIdAmRecordMap.putIfAbsent(appId, amRecord);
          LOG.info("Adding external session with applicationId: {} in compute: {}", appId, amRecord.getComputeName());
        }
      }
    }

    @Override
    public void onRemove(final AMRecord amRecord) {
      if (amRecord != null && amRecord.getApplicationId() != null) {
        final String appId = amRecord.getApplicationId().toString();
        available.remove(amRecord);
        taken.remove(amRecord);
        appIdAmRecordMap.remove(appId);
        LOG.info("Removed external session with applicationId: {} in compute: {}", appId, amRecord.getComputeName());
      }
    }
  }

  public void close() {
    if (tezRegistryClient != null) {
      tezRegistryClient.close();
    }
  }

  public String getSession() throws Exception {
    AMRecord amRecord = available.poll(maxAttempts, TimeUnit.SECONDS);
    if (amRecord == null) {
      throw new IOException("Cannot get a session after " + maxAttempts + " seconds");
    }
    taken.add(amRecord);
    LOG.info("External session taken: {} in compute: {}. available: {}",
      amRecord.getComputeName(), amRecord.getApplicationId().toString(),
      available.size());
    return amRecord.getApplicationId().toString();
  }

  public void returnSession(String appId) {
    AMRecord returnedAM = appIdAmRecordMap.get(appId);
    if (!taken.remove(returnedAM)) {
      return; // Session has been removed from ZK.
    }
    available.offer(returnedAM);
    LOG.info("External session returned: {} back to compute: {}. available: {}", appId, returnedAM.getComputeName(),
      available.size());
  }

  @VisibleForTesting
  static class AMRecordBinPackingComputeComparator implements Comparator<AMRecord> {
    @Override
    public int compare(final AMRecord o1, final AMRecord o2) {
      Integer computeOrdinal1 = getComputeOrdinal(o1.getComputeName());
      Integer computeOrdinal2 = getComputeOrdinal(o2.getComputeName());
      // compute name does not have ordinal value at the end, both have same priority
      if (computeOrdinal1 < 0 || computeOrdinal2 < 0) {
        return 0;
      }
      // AMs belong to same compute, sort by compute hostname ordinal (ordering AMs helps with footer cache hits)
      if (computeOrdinal1.compareTo(computeOrdinal2) == 0) {
        Integer hostOrdinal1 = getHostOrdinal(o1.getHost());
        Integer hostOrdinal2 = getHostOrdinal(o2.getHost());
        // host name does not have ordinal value at the end, both have same priority
        if (hostOrdinal1 < 0 || hostOrdinal2 < 0) {
          return 0;
        }
        return hostOrdinal1.compareTo(hostOrdinal2);
      }
      return computeOrdinal1.compareTo(computeOrdinal2);
    }
  }

  @VisibleForTesting
  static class AMRecordRandomComputeComparator implements Comparator<AMRecord> {
    @Override
    public int compare(final AMRecord o1, final AMRecord o2) {
      // hashcode of AMRecords is completely arbitrary as it can include random external id or appId
      return o1.hashCode() - o2.hashCode();
    }
  }

  @VisibleForTesting
  static class AMRecordRoundRobinComputeComparator implements Comparator<AMRecord> {
    @Override
    public int compare(final AMRecord o1, final AMRecord o2) {
      Integer hostOrdinal1 = getHostOrdinal(o1.getHost());
      Integer hostOrdinal2 = getHostOrdinal(o2.getHost());
      // host name does not have ordinal value at the end, both have same priority
      if (hostOrdinal1 < 0 || hostOrdinal2 < 0) {
        return 0;
      }

      // AMs belong to same compute, sort by compute hostname ordinal (ordering AMs helps with footer cache hits)
      if (hostOrdinal1.compareTo(hostOrdinal2) == 0) {
        Integer computeOrdinal1 = getComputeOrdinal(o1.getComputeName());
        Integer computeOrdinal2 = getComputeOrdinal(o2.getComputeName());
        // compute name does not have ordinal value at the end, both have same priority
        if (computeOrdinal1 < 0 || computeOrdinal2 < 0) {
          return 0;
        }
        return computeOrdinal1.compareTo(computeOrdinal2);
      }
      return hostOrdinal1.compareTo(hostOrdinal2);
    }
  }

  // compute name is expected in the format "compute-name-1"
  private static Integer getComputeOrdinal(String name) {
    if (name.lastIndexOf("-") > 0) {
      String ordinal = name.substring(name.lastIndexOf("-") + 1);
      try {
        return Integer.valueOf(ordinal);
      } catch (NumberFormatException e) {
        // ignore
      }
    }

    // the compute name does not have ordinal (like foo-1)
    LOG.warn("Returning negative ordinal for compute: {}", name);
    return -1;
  }

  // host name is expected in the format "query-coordinator-1.subdomain.domain"
  private static Integer getHostOrdinal(String host) {
    String name = host.split("\\.")[0];
    if (name.lastIndexOf("-") > 0) {
      String ordinal = name.substring(name.lastIndexOf("-") + 1);
      try {
        return Integer.valueOf(ordinal);
      } catch (NumberFormatException e) {
        // ignore
      }
    }

    // the host name does not have ordinal (like query-coordinator-1)
    LOG.warn("Returning negative ordinal for host: {}", name);
    return -1;
  }
}
