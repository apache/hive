/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.metrics;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetastoreTaskThread;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.MetricsInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.*;

/**
 * Collect and publish ACID and compaction related metrics.
 */
public class AcidMetricService  implements MetastoreTaskThread {

  private static final Logger LOG = LoggerFactory.getLogger(AcidMetricService.class);
  private static final String NO_VAL = " --- ";

  private Configuration conf;
  private TxnStore txnHandler;

  @Override
  public long runFrequency(TimeUnit unit) {
    return MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_CHECK_INTERVAL, unit);
  }

  @Override
  public void run() {
    LOG.debug("Starting AcidMetricService thread");
    try {
        long startedAt = System.currentTimeMillis();

        boolean metricsEnabled = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED) &&
            MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_THREAD_ON);
        if (!metricsEnabled) {
          return;
        }
        try {
          collectMetrics();
        } catch (Exception ex) {
         LOG.error("Caught exception in AcidMetricService loop", ex);
        }

        long elapsedTime = System.currentTimeMillis() - startedAt;
        LOG.debug("AcidMetricService thread finished one loop in {} seconds.", elapsedTime / 1000);

    } catch (Throwable t) {
      LOG.error("Caught an exception in the main loop of AcidMetricService, exiting ", t);
    }
  }

  private void collectMetrics() throws MetaException {
    ShowCompactResponse currentCompactions = txnHandler.showCompact(new ShowCompactRequest());
    updateMetricsFromShowCompact(currentCompactions);
    updateDBMetrics();
  }

  private void updateDBMetrics() throws MetaException {
    MetricsInfo metrics = txnHandler.getMetricsInfo();
    Metrics.getOrCreateGauge(NUM_TXN_TO_WRITEID).set(metrics.getTxnToWriteIdCount());
    Metrics.getOrCreateGauge(NUM_COMPLETED_TXN_COMPONENTS).set(metrics.getCompletedTxnsCount());

    // NOTE: AcidOpenTxnsCounterService has a duplicate countOpenTxns() functionality and could be disabled.
    // PS: make sure to update `numOpenTxns` counter in TxnHandler.
    Metrics.getOrCreateGauge(NUM_OPEN_TXNS).set(metrics.getOpenTxnsCount());
    Metrics.getOrCreateGauge(OLDEST_OPEN_TXN_ID).set(metrics.getOldestOpenTxnId());
    Metrics.getOrCreateGauge(OLDEST_OPEN_TXN_AGE).set(metrics.getOldestOpenTxnAge());

    Metrics.getOrCreateGauge(NUM_ABORTED_TXNS).set(metrics.getAbortedTxnsCount());
    Metrics.getOrCreateGauge(OLDEST_ABORTED_TXN_ID).set(metrics.getOldestAbortedTxnId());
    Metrics.getOrCreateGauge(OLDEST_ABORTED_TXN_AGE).set(metrics.getOldestAbortedTxnAge());

    Metrics.getOrCreateGauge(NUM_LOCKS).set(metrics.getLocksCount());
    Metrics.getOrCreateGauge(OLDEST_LOCK_AGE).set(metrics.getOldestLockAge());
  }

  @VisibleForTesting
  public static void updateMetricsFromShowCompact(ShowCompactResponse showCompactResponse) {
    Map<String, ShowCompactResponseElement> lastElements = new HashMap<>();
    long oldestEnqueueTime = Long.MAX_VALUE;

    // Get the last compaction for each db/table/partition
    for(ShowCompactResponseElement element : showCompactResponse.getCompacts()) {
      String key = element.getDbname() + "/" + element.getTablename() +
          (element.getPartitionname() != null ? "/" + element.getPartitionname() : "");
      // If new key, add the element, if there is an existing one, change to the element if the element.id is greater than old.id
      lastElements.compute(key, (k, old) -> (old == null) ? element : (element.getId() > old.getId() ? element : old));
      if (TxnStore.INITIATED_RESPONSE.equals(element.getState()) && oldestEnqueueTime > element.getEnqueueTime()) {
        oldestEnqueueTime = element.getEnqueueTime();
      }
    }

    // Get the current count for each state
    Map<String, Long> counts = lastElements.values().stream()
        .collect(Collectors.groupingBy(ShowCompactResponseElement::getState, Collectors.counting()));

    // Update metrics
    for (int i = 0; i < TxnStore.COMPACTION_STATES.length; ++i) {
      String key = COMPACTION_STATUS_PREFIX + TxnStore.COMPACTION_STATES[i];
      Long count = counts.get(TxnStore.COMPACTION_STATES[i]);
      if (count != null) {
        Metrics.getOrCreateGauge(key).set(count.intValue());
      } else {
        Metrics.getOrCreateGauge(key).set(0);
      }
    }
    if (oldestEnqueueTime == Long.MAX_VALUE) {
      Metrics.getOrCreateGauge(COMPACTION_OLDEST_ENQUEUE_AGE).set(0);
    } else {
      Metrics.getOrCreateGauge(COMPACTION_OLDEST_ENQUEUE_AGE)
          .set((int) ((System.currentTimeMillis() - oldestEnqueueTime) / 1000L));
    }

    long initiatorsCount = lastElements.values().stream()
        .map(e -> getHostFromId(e.getInitiatorId())).distinct().filter(e -> !NO_VAL.equals(e)).count();
    Metrics.getOrCreateGauge(COMPACTION_NUM_INITIATORS).set((int) initiatorsCount);
    long workersCount = lastElements.values().stream()
        .map(e -> getHostFromId(e.getWorkerid())).distinct().filter(e -> !NO_VAL.equals(e)).count();
    Metrics.getOrCreateGauge(COMPACTION_NUM_WORKERS).set((int) workersCount);

    long initiatorVersionsCount = lastElements.values().stream()
        .map(ShowCompactResponseElement::getInitiatorVersion).distinct().filter(Objects::nonNull).count();
    Metrics.getOrCreateGauge(COMPACTION_NUM_INITIATOR_VERSIONS).set((int) initiatorVersionsCount);
    long workerVersionsCount = lastElements.values().stream()
        .map(ShowCompactResponseElement::getWorkerVersion).distinct().filter(Objects::nonNull).count();
    Metrics.getOrCreateGauge(COMPACTION_NUM_WORKER_VERSIONS).set((int) workerVersionsCount);
  }

  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
    txnHandler = TxnUtils.getTxnStore(conf);
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  private static String getHostFromId(String id) {
    if (id == null) {
      return NO_VAL;
    }
    int lastDash = id.lastIndexOf('-');
    return id.substring(0, lastDash > -1 ? lastDash : id.length());
  }

}
