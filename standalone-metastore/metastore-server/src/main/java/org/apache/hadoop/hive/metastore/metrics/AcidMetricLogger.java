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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetastoreTaskThread;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionMetricsData;
import org.apache.hadoop.hive.metastore.txn.entities.MetricsInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hive.metastore.txn.entities.CompactionMetricsData.MetricType.NUM_DELTAS;
import static org.apache.hadoop.hive.metastore.txn.entities.CompactionMetricsData.MetricType.NUM_OBSOLETE_DELTAS;
import static org.apache.hadoop.hive.metastore.txn.entities.CompactionMetricsData.MetricType.NUM_SMALL_DELTAS;

/**
 *
 */
public class AcidMetricLogger implements MetastoreTaskThread {

  private static final Logger LOG = LoggerFactory.getLogger(AcidMetricLogger.class);

  private Configuration conf;
  private TxnStore txnHandler;
  private int maxCacheSize;

  @Override
  public long runFrequency(TimeUnit timeUnit) {
    return MetastoreConf
        .getTimeVar(conf, MetastoreConf.ConfVars.COMPACTOR_ACID_METRICS_LOGGER_FREQUENCY, timeUnit);
  }

  @Override
  public void run() {
    try {
      logDbMetrics();
      logMetrics();
    } catch (MetaException e) {
      LOG.warn("Caught exception while trying to log acid metrics data.", e);
    }
  }

  @Override
  public void setConf(Configuration configuration) {
    this.conf = configuration;
    txnHandler = TxnUtils.getTxnStore(conf);
    maxCacheSize = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.METASTORE_DELTAMETRICS_MAX_CACHE_SIZE);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  private void logMetrics() throws MetaException {
    ShowCompactResponse response = txnHandler.showCompact(new ShowCompactRequest());
    CompactionMetricData metricData = CompactionMetricData.of(response.getCompacts());

    logMultipleWorkerVersions(metricData);
    logFailedCompactionsPercentage(metricData);
    logOldestInitiatorAge(metricData);

    logDeltaMetrics();
  }

  private void logDeltaMetrics() throws MetaException {
    List<CompactionMetricsData> deltas = txnHandler.getTopCompactionMetricsDataPerType(maxCacheSize);
    deltas.stream().filter(d -> d.getMetricType() == NUM_DELTAS).forEach(d -> LOG.warn(
        String.format("Directory %s contains %d active delta directories. " + "This can cause performance degradation.",
            AcidMetricService.getDeltaCountKey(d.getDbName(), d.getTblName(), d.getPartitionName()),
            d.getMetricValue())));
    deltas.stream().filter(d -> d.getMetricType() == NUM_SMALL_DELTAS).forEach(d -> LOG.warn(String.format(
        "Directory %s contains %d small delta directories. "
            + "This can indicate performance degradation and there might be a problem with your streaming setup.",
        AcidMetricService.getDeltaCountKey(d.getDbName(), d.getTblName(), d.getPartitionName()), d.getMetricValue())));
    deltas.stream().filter(d -> d.getMetricType() == NUM_OBSOLETE_DELTAS).forEach(d -> LOG.warn(String.format(
        "Directory %s contains %d obsolete delta directories. " + "This can indicate compaction cleaner issues.",
        AcidMetricService.getDeltaCountKey(d.getDbName(), d.getTblName(), d.getPartitionName()), d.getMetricValue())));
  }

  private void logOldestInitiatorAge(CompactionMetricData metricData) {
    int oldestInitiatorAge = (int) ((System.currentTimeMillis() - metricData.getOldestEnqueueTime()) / 1000L);
    String oldestInitiatorMessage = "Found compaction entry in compaction queue with an age of {} seconds. " +
        "Consider increasing the number of worker threads.";
    long oldestInitiatedWarningThreshold = MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_OLDEST_INITIATED_COMPACTION_TIME_THRESHOLD_WARNING,
        TimeUnit.SECONDS);
    long oldestInitiatedErrorThreshold = MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_OLDEST_INITIATED_COMPACTION_TIME_THRESHOLD_ERROR,
        TimeUnit.SECONDS);
    if (oldestInitiatorAge >= oldestInitiatedErrorThreshold) {
      LOG.error(oldestInitiatorMessage, oldestInitiatorAge);
    } else if (oldestInitiatorAge >= oldestInitiatedWarningThreshold) {
      LOG.warn(oldestInitiatorMessage, oldestInitiatorAge);
    }
  }

  private void logMultipleWorkerVersions(CompactionMetricData metricData) {
    long workerVersionThresholdInMillis = MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_WORKER_DETECT_MULTIPLE_VERSION_THRESHOLD, TimeUnit.MILLISECONDS);
    List<String> versions = metricData
        .allWorkerVersionsSince(System.currentTimeMillis() - workerVersionThresholdInMillis);

    if (versions.size() > 1) {
      LOG.warn("Multiple Compaction Worker versions detected: {}", versions);
    }
  }

  private void logFailedCompactionsPercentage(CompactionMetricData metricData) {
    Double failedCompactionPercentage = metricData.getFailedCompactionPercentage();
    if (failedCompactionPercentage != null &&
        (failedCompactionPercentage >=
            MetastoreConf.getDoubleVar(conf, MetastoreConf.ConfVars.COMPACTOR_FAILED_COMPACTION_RATIO_THRESHOLD))) {
      LOG.warn("Many compactions are failing. Check root cause of failed/not initiated compactions.");
    }
  }
  private void logDbMetrics() throws MetaException {
    MetricsInfo metrics = txnHandler.getMetricsInfo();
    if (metrics.getTxnToWriteIdCount() >= MetastoreConf.getIntVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_TXN_TO_WRITEID_RECORD_THRESHOLD_WARNING)
        && metrics.getTxnToWriteIdCount() < MetastoreConf.getIntVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_TXN_TO_WRITEID_RECORD_THRESHOLD_ERROR)) {
      LOG.warn("An excessive amount of (" + metrics.getTxnToWriteIdCount() + ") Hive ACID metadata found in "
          + "TXN_TO_WRITEID table, which can cause serious performance degradation.");
    } else if (metrics.getTxnToWriteIdCount() >= MetastoreConf.getIntVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_TXN_TO_WRITEID_RECORD_THRESHOLD_ERROR)) {
      LOG.error("An excessive amount of (" + metrics.getTxnToWriteIdCount() + ") Hive ACID metadata found in "
          + "TXN_TO_WRITEID table, which can cause serious performance degradation.");
    }

    if (metrics.getCompletedTxnsCount() >= MetastoreConf.getIntVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_COMPLETED_TXN_COMPONENTS_RECORD_THRESHOLD_WARNING)
        && metrics.getCompletedTxnsCount() < MetastoreConf.getIntVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_COMPLETED_TXN_COMPONENTS_RECORD_THRESHOLD_ERROR)) {
      LOG.warn("An excessive amount of (" + metrics.getCompletedTxnsCount() + ") Hive ACID metadata found in "
          + "COMPLETED_TXN_COMPONENTS table, which can cause serious performance degradation.");
    } else if (metrics.getCompletedTxnsCount() >= MetastoreConf.getIntVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_COMPLETED_TXN_COMPONENTS_RECORD_THRESHOLD_ERROR)) {
      LOG.error("An excessive amount of (" + metrics.getCompletedTxnsCount() + ") Hive ACID metadata found in "
          + "COMPLETED_TXN_COMPONENTS table, which can cause serious performance degradation.");
    }

    if (metrics.getOldestOpenReplTxnAge() >= MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_OLDEST_REPLICATION_OPENTXN_THRESHOLD_WARNING, TimeUnit.SECONDS)
        && metrics.getOldestOpenReplTxnAge() < MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_OLDEST_REPLICATION_OPENTXN_THRESHOLD_ERROR, TimeUnit.SECONDS)) {
      LOG.warn("A replication transaction with ID " + metrics.getOldestOpenReplTxnId() + " has been open for "
          + metrics.getOldestOpenReplTxnAge() + " seconds. "
          + "Before you abort a transaction that was created by replication, and which has been open a long time, "
          + "make sure that the hive.repl.txn.timeout threshold has expired.");
    } else if (metrics.getOldestOpenReplTxnAge() >= MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_OLDEST_REPLICATION_OPENTXN_THRESHOLD_ERROR, TimeUnit.SECONDS)) {
      LOG.error("A replication transaction with ID " + metrics.getOldestOpenReplTxnId() + " has been open for "
          + metrics.getOldestOpenReplTxnAge() + " seconds. "
          + "Before you abort a transaction that was created by replication, and which has been open a long time, "
          + "make sure that the hive.repl.txn.timeout threshold has expired.");
    }

    if (metrics.getOldestOpenNonReplTxnAge() >= MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_OLDEST_OPENTXN_THRESHOLD_WARNING, TimeUnit.SECONDS)
        && metrics.getOldestOpenNonReplTxnAge() < MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_OLDEST_OPENTXN_THRESHOLD_ERROR, TimeUnit.SECONDS)) {
      LOG.warn("A non-replication transaction with ID " + metrics.getOldestOpenNonReplTxnId() + " has been open for "
          + metrics.getOldestOpenNonReplTxnAge() + " seconds.");
    } else if (metrics.getOldestOpenNonReplTxnAge() >= MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_OLDEST_OPENTXN_THRESHOLD_ERROR, TimeUnit.SECONDS)) {
      LOG.error("A non-replication transaction with ID " + metrics.getOldestOpenNonReplTxnId() + " has been open for "
          + metrics.getOldestOpenNonReplTxnAge() + " seconds.");
    }

    if (metrics.getOldestAbortedTxnAge() >= MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_OLDEST_UNCLEANED_ABORTEDTXN_TIME_THRESHOLD_WARNING, TimeUnit.SECONDS)
        && metrics.getOldestAbortedTxnAge() < MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_OLDEST_UNCLEANED_ABORTEDTXN_TIME_THRESHOLD_ERROR, TimeUnit.SECONDS)) {
      LOG.warn("Found an aborted transaction with an ID " + metrics.getOldestAbortedTxnId() + " and age of "
          + metrics.getOldestAbortedTxnAge() + " seconds.");
    } else if (metrics.getOldestAbortedTxnAge() >= MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_OLDEST_UNCLEANED_ABORTEDTXN_TIME_THRESHOLD_ERROR, TimeUnit.SECONDS)) {
      LOG.warn("Found an aborted transaction with an ID " + metrics.getOldestAbortedTxnId() + " and age of "
          + metrics.getOldestAbortedTxnAge() + " seconds.");
    }

    if (metrics.getTablesWithXAbortedTxnsCount() > MetastoreConf.getIntVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_TABLES_WITH_ABORTEDTXN_THRESHOLD)) {
      LOG.error("Found " + metrics.getTablesWithXAbortedTxnsCount() + " tables/partitions with more than "
          + MetastoreConf.getIntVar(conf,
          MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_TABLES_WITH_ABORTED_TXNS_THRESHOLD)
          + " aborts. Name of the tables/partitions are: " + metrics.getTablesWithXAbortedTxns());
    }

    if (metrics.getOldestReadyForCleaningAge() > MetastoreConf.getTimeVar(conf,
        MetastoreConf.ConfVars.COMPACTOR_OLDEST_UNCLEANED_COMPACTION_TIME_THRESHOLD, TimeUnit.SECONDS)) {
      LOG.warn("Found entry in compaction queue in ready for cleaning state with age of "
          + metrics.getOldestReadyForCleaningAge() + " seconds.");
    }
  }
}
