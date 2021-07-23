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

import static org.apache.hadoop.hive.metastore.HiveMetaStoreClient.MANUALLY_INITIATED_COMPACTION;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_NUM_INITIATORS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_NUM_INITIATOR_VERSIONS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_NUM_WORKERS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_NUM_WORKER_VERSIONS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_OLDEST_ENQUEUE_AGE;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_STATUS_PREFIX;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.NUM_ABORTED_TXNS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.NUM_COMPLETED_TXN_COMPONENTS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.NUM_LOCKS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.NUM_OPEN_NON_REPL_TXNS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.NUM_OPEN_REPL_TXNS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.NUM_TXN_TO_WRITEID;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.OLDEST_ABORTED_TXN_AGE;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.OLDEST_ABORTED_TXN_ID;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.OLDEST_LOCK_AGE;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.OLDEST_OPEN_NON_REPL_TXN_AGE;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.OLDEST_OPEN_NON_REPL_TXN_ID;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.OLDEST_OPEN_REPL_TXN_AGE;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.OLDEST_OPEN_REPL_TXN_ID;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.OLDEST_READY_FOR_CLEANING_AGE;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.TABLES_WITH_X_ABORTED_TXNS;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.NO_VAL;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getHostFromId;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getThreadIdFromId;

/**
 * Collect and publish ACID and compaction related metrics.
 */
public class AcidMetricService  implements MetastoreTaskThread {

  private static final Logger LOG = LoggerFactory.getLogger(AcidMetricService.class);

  private Configuration conf;
  private TxnStore txnHandler;
  private long lastSuccessfulLoggingTime = 0;

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
    updateMetricsFromShowCompact(currentCompactions, conf);
    updateDBMetrics();
  }

  private void updateDBMetrics() throws MetaException {
    MetricsInfo metrics = txnHandler.getMetricsInfo();
    Metrics.getOrCreateGauge(NUM_TXN_TO_WRITEID).set(metrics.getTxnToWriteIdCount());
    logDbMetrics(metrics);
    Metrics.getOrCreateGauge(NUM_COMPLETED_TXN_COMPONENTS).set(metrics.getCompletedTxnsCount());
    Metrics.getOrCreateGauge(NUM_OPEN_REPL_TXNS).set(metrics.getOpenReplTxnsCount());
    Metrics.getOrCreateGauge(OLDEST_OPEN_REPL_TXN_ID).set(metrics.getOldestOpenReplTxnId());
    Metrics.getOrCreateGauge(OLDEST_OPEN_REPL_TXN_AGE).set(metrics.getOldestOpenReplTxnAge());
    Metrics.getOrCreateGauge(NUM_OPEN_NON_REPL_TXNS).set(metrics.getOpenNonReplTxnsCount());
    Metrics.getOrCreateGauge(OLDEST_OPEN_NON_REPL_TXN_ID).set(metrics.getOldestOpenNonReplTxnId());
    Metrics.getOrCreateGauge(OLDEST_OPEN_NON_REPL_TXN_AGE).set(metrics.getOldestOpenNonReplTxnAge());
    Metrics.getOrCreateGauge(NUM_ABORTED_TXNS).set(metrics.getAbortedTxnsCount());
    Metrics.getOrCreateGauge(OLDEST_ABORTED_TXN_ID).set(metrics.getOldestAbortedTxnId());
    Metrics.getOrCreateGauge(OLDEST_ABORTED_TXN_AGE).set(metrics.getOldestAbortedTxnAge());
    Metrics.getOrCreateGauge(NUM_LOCKS).set(metrics.getLocksCount());
    Metrics.getOrCreateGauge(OLDEST_LOCK_AGE).set(metrics.getOldestLockAge());
    Metrics.getOrCreateGauge(TABLES_WITH_X_ABORTED_TXNS).set(metrics.getTablesWithXAbortedTxnsCount());
    Metrics.getOrCreateGauge(OLDEST_READY_FOR_CLEANING_AGE).set(metrics.getOldestReadyForCleaningAge());
  }

  private void logDbMetrics(MetricsInfo metrics) {
    long loggingFrequency = MetastoreConf
        .getTimeVar(conf, MetastoreConf.ConfVars.COMPACTOR_ACID_METRICS_LOGGER_FREQUENCY, TimeUnit.MILLISECONDS);
    if (loggingFrequency <= 0) {
      return;
    }
    long currentTime = System.currentTimeMillis();
    if (lastSuccessfulLoggingTime == 0 || currentTime >= lastSuccessfulLoggingTime + loggingFrequency) {
      lastSuccessfulLoggingTime = currentTime;
      if (metrics.getTxnToWriteIdCount() >=
          MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_TXN_TO_WRITEID_RECORD_THRESHOLD_WARNING) &&
          metrics.getTxnToWriteIdCount() <
              MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_TXN_TO_WRITEID_RECORD_THRESHOLD_ERROR)) {
        LOG.warn("An excessive amount of (" + metrics.getTxnToWriteIdCount() + ") Hive ACID metadata found in " +
            "TXN_TO_WRITEID table, which can cause serious performance degradation.");
      } else if (metrics.getTxnToWriteIdCount() >=
          MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_TXN_TO_WRITEID_RECORD_THRESHOLD_ERROR)) {
        LOG.error("An excessive amount of (" + metrics.getTxnToWriteIdCount() + ") Hive ACID metadata found in " +
            "TXN_TO_WRITEID table, which can cause serious performance degradation.");
      }

      if (metrics.getCompletedTxnsCount() >=
          MetastoreConf.getIntVar(conf,
              MetastoreConf.ConfVars.COMPACTOR_COMPLETED_TXN_COMPONENTS_RECORD_THRESHOLD_WARNING) &&
          metrics.getCompletedTxnsCount() <
              MetastoreConf.getIntVar(conf,
                  MetastoreConf.ConfVars.COMPACTOR_COMPLETED_TXN_COMPONENTS_RECORD_THRESHOLD_ERROR)) {
        LOG.warn("An excessive amount of (" + metrics.getCompletedTxnsCount() + ") Hive ACID metadata found in " +
            "COMPLETED_TXN_COMPONENTS table, which can cause serious performance degradation.");
      } else if (metrics.getCompletedTxnsCount() >= MetastoreConf.getIntVar(conf,
          MetastoreConf.ConfVars.COMPACTOR_COMPLETED_TXN_COMPONENTS_RECORD_THRESHOLD_ERROR)) {
        LOG.error("An excessive amount of (" + metrics.getCompletedTxnsCount() + ") Hive ACID metadata found in " +
            "COMPLETED_TXN_COMPONENTS table, which can cause serious performance degradation.");
      }

      if (metrics.getOldestOpenReplTxnAge() >=
          MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.COMPACTOR_OLDEST_REPLICATION_OPENTXN_THRESHOLD_WARNING,
              TimeUnit.SECONDS) && metrics.getOldestOpenReplTxnAge() <
          MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.COMPACTOR_OLDEST_REPLICATION_OPENTXN_THRESHOLD_ERROR,
              TimeUnit.SECONDS)) {
        LOG.warn("A replication transaction with ID " + metrics.getOldestOpenReplTxnId() +
            " has been open for " + metrics.getOldestOpenReplTxnAge() + " seconds. " +
            "Before you abort a transaction that was created by replication, and which has been open a long time, " +
            "make sure that the hive.repl.txn.timeout threshold has expired.");
      } else if (metrics.getOldestOpenReplTxnAge() >=
          MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.COMPACTOR_OLDEST_REPLICATION_OPENTXN_THRESHOLD_ERROR,
              TimeUnit.SECONDS)) {
        LOG.error("A replication transaction with ID " + metrics.getOldestOpenReplTxnId() +
            " has been open for " + metrics.getOldestOpenReplTxnAge() + " seconds. " +
            "Before you abort a transaction that was created by replication, and which has been open a long time, " +
            "make sure that the hive.repl.txn.timeout threshold has expired.");
      }

      if (metrics.getOldestOpenNonReplTxnAge() >=
          MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.COMPACTOR_OLDEST_OPENTXN_THRESHOLD_WARNING,
              TimeUnit.SECONDS)
          && metrics.getOldestOpenNonReplTxnAge() <
          MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.COMPACTOR_OLDEST_OPENTXN_THRESHOLD_ERROR,
              TimeUnit.SECONDS)) {
        LOG.warn("A non-replication transaction with ID " + metrics.getOldestOpenNonReplTxnId() +
            " has been open for " + metrics.getOldestOpenNonReplTxnAge() + " seconds.");
      } else if (metrics.getOldestOpenNonReplTxnAge() >=
          MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.COMPACTOR_OLDEST_OPENTXN_THRESHOLD_ERROR,
              TimeUnit.SECONDS)) {
        LOG.error("A non-replication transaction with ID " + metrics.getOldestOpenNonReplTxnId() +
            " has been open for " + metrics.getOldestOpenNonReplTxnAge() + " seconds.");
      }

      if (metrics.getOldestAbortedTxnAge() >=
          MetastoreConf.getTimeVar(conf,
              MetastoreConf.ConfVars.COMPACTOR_OLDEST_UNCLEANED_ABORTEDTXN_TIME_THRESHOLD_WARNING,
              TimeUnit.SECONDS) &&
          metrics.getOldestAbortedTxnAge() <
              MetastoreConf.getTimeVar(conf,
                  MetastoreConf.ConfVars.COMPACTOR_OLDEST_UNCLEANED_ABORTEDTXN_TIME_THRESHOLD_ERROR,
                  TimeUnit.SECONDS)) {
        LOG.warn("Found an aborted transaction with an ID " + metrics.getOldestAbortedTxnId() +
            " and age of " + metrics.getOldestAbortedTxnAge() + " seconds.");
      } else if (metrics.getOldestAbortedTxnAge() >=
          MetastoreConf.getTimeVar(conf,
              MetastoreConf.ConfVars.COMPACTOR_OLDEST_UNCLEANED_ABORTEDTXN_TIME_THRESHOLD_ERROR,
              TimeUnit.SECONDS)) {
        LOG.warn("Found an aborted transaction with an ID " + metrics.getOldestAbortedTxnId() +
            " and age of " + metrics.getOldestAbortedTxnAge() + " seconds.");
      }

      if (metrics.getTablesWithXAbortedTxnsCount() >
          MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.COMPACTOR_TABLES_WITH_ABORTEDTXN_THRESHOLD)) {
        LOG.error("Found " + metrics.getTablesWithXAbortedTxnsCount() + " tables/partitions with more than " +
            MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_TABLES_WITH_ABORTED_TXNS_THRESHOLD) +
            " aborts. Name of the tables/partitions are: " + metrics.getTablesWithXAbortedTxns());
      }

      if (metrics.getOldestReadyForCleaningAge() >
          MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.COMPACTOR_OLDEST_UNCLEANED_COMPACTION_TIME_THRESHOLD,
              TimeUnit.SECONDS)) {
        LOG.warn("Found entry in compaction queue in ready for cleaning state with age of " +
            metrics.getOldestReadyForCleaningAge() + " seconds.");
      }
    }
  }

  @VisibleForTesting
  public static void updateMetricsFromShowCompact(ShowCompactResponse showCompactResponse, Configuration conf) {
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
      String key = COMPACTION_STATUS_PREFIX + replaceWhitespace(TxnStore.COMPACTION_STATES[i]);
      Long count = counts.get(TxnStore.COMPACTION_STATES[i]);
      if (count != null) {
        Metrics.getOrCreateGauge(key).set(count.intValue());
      } else {
        Metrics.getOrCreateGauge(key).set(0);
      }
    }

    Long numFailedComp = counts.get(TxnStore.FAILED_RESPONSE);
    Long numNotInitiatedComp = counts.get(TxnStore.DID_NOT_INITIATE_RESPONSE);
    Long numSucceededComp = counts.get(TxnStore.SUCCEEDED_RESPONSE);
    if (numFailedComp != null && numNotInitiatedComp != null && numSucceededComp != null &&
        ((numFailedComp + numNotInitiatedComp) / (numFailedComp + numNotInitiatedComp + numSucceededComp) >
      MetastoreConf.getDoubleVar(conf, MetastoreConf.ConfVars.COMPACTOR_FAILED_COMPACTION_RATIO_THRESHOLD))) {
      LOG.warn("Many compactions are failing. Check root cause of failed/not initiated compactions.");
    }

    if (oldestEnqueueTime == Long.MAX_VALUE) {
      Metrics.getOrCreateGauge(COMPACTION_OLDEST_ENQUEUE_AGE).set(0);
    } else {
      int oldestEnqueueAge = (int) ((System.currentTimeMillis() - oldestEnqueueTime) / 1000L);
      Metrics.getOrCreateGauge(COMPACTION_OLDEST_ENQUEUE_AGE)
          .set(oldestEnqueueAge);
      if (oldestEnqueueAge >= MetastoreConf.getTimeVar(conf,
          MetastoreConf.ConfVars.COMPACTOR_OLDEST_INITIATED_COMPACTION_TIME_THRESHOLD_WARNING, TimeUnit.SECONDS) &&
          oldestEnqueueAge < MetastoreConf.getTimeVar(conf,
              MetastoreConf.ConfVars.COMPACTOR_OLDEST_INITIATED_COMPACTION_TIME_THRESHOLD_ERROR, TimeUnit.SECONDS)) {
        LOG.warn("Found compaction entry in compaction queue with an age of " + oldestEnqueueAge + " seconds. " +
            "Consider increasing the number of worker threads.");
      } else if (oldestEnqueueAge >= MetastoreConf.getTimeVar(conf,
          MetastoreConf.ConfVars.COMPACTOR_OLDEST_INITIATED_COMPACTION_TIME_THRESHOLD_ERROR, TimeUnit.SECONDS)) {
        LOG.error("Found compaction entry in compaction queue with an age of " + oldestEnqueueAge + " seconds. " +
            "Consider increasing the number of worker threads");
      }
    }

    long initiatorsCount = lastElements.values().stream()
        //manually initiated compactions don't count
        .filter(e -> !MANUALLY_INITIATED_COMPACTION.equals(getThreadIdFromId(e.getInitiatorId())))
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

  @VisibleForTesting
  public static String replaceWhitespace(String input) {
    if (input == null) {
      return input;
    }
    return input.replaceAll("\\s+", "_");
  }

}
