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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.AcidConstants;
import org.apache.hadoop.hive.common.metrics.MetricsMBeanImpl;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetastoreTaskThread;
import org.apache.hadoop.hive.metastore.api.CompactionMetricsDataRequest;
import org.apache.hadoop.hive.metastore.api.CompactionMetricsDataStruct;
import org.apache.hadoop.hive.metastore.api.CompactionMetricsMetricType;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionMetricsData;
import org.apache.hadoop.hive.metastore.txn.entities.MetricsInfo;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_NUM_DELTAS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_NUM_INITIATORS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_NUM_INITIATOR_VERSIONS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_NUM_SMALL_DELTAS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_NUM_WORKERS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_NUM_WORKER_VERSIONS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_OLDEST_CLEANING_AGE;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_OLDEST_ENQUEUE_AGE;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_OLDEST_WORKING_AGE;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_POOLS_INITIATED_ITEM_COUNT;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_POOLS_OLDEST_INITIATED_AGE;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_POOLS_OLDEST_WORKING_AGE;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_POOLS_WORKING_ITEM_COUNT;
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
import static org.apache.hadoop.hive.metastore.txn.entities.CompactionMetricsData.MetricType.NUM_DELTAS;
import static org.apache.hadoop.hive.metastore.txn.entities.CompactionMetricsData.MetricType.NUM_OBSOLETE_DELTAS;
import static org.apache.hadoop.hive.metastore.txn.entities.CompactionMetricsData.MetricType.NUM_SMALL_DELTAS;

/**
 * Collect and publish ACID and compaction related metrics.
 * Everything should be behind 2 feature flags: {@link MetastoreConf.ConfVars#METRICS_ENABLED} and
 * {@link MetastoreConf.ConfVars#METASTORE_ACIDMETRICS_THREAD_ON}.
 *
 */
public class AcidMetricService implements MetastoreTaskThread {

  private static final Logger LOG = LoggerFactory.getLogger(AcidMetricService.class);
  public static final String OBJECT_NAME_PREFIX = "metrics:type=compaction,name=";

  private static boolean metricsEnabled;

  private MetricsMBeanImpl deltaObject, smallDeltaObject, obsoleteDeltaObject;

  private Configuration conf;
  private TxnStore txnHandler;
  private int maxCacheSize;

  @Override
  public long runFrequency(TimeUnit unit) {
    return MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_CHECK_INTERVAL, unit);
  }

  @Override
  public void run() {
    LOG.debug("Starting AcidMetricService thread");
    try {
      if (!metricsEnabled) {
        return;
      }
      long startedAt = System.currentTimeMillis();
        try {
          updateMetrics();
          updateDeltaMetrics();
        } catch (Exception ex) {
         LOG.error("Caught exception in AcidMetricService loop", ex);
        }

        long elapsedTime = System.currentTimeMillis() - startedAt;
        LOG.debug("AcidMetricService thread finished one loop in {} seconds.", elapsedTime / 1000);

    } catch (Throwable t) {
      LOG.error("Caught an exception in the main loop of AcidMetricService, exiting ", t);
    }
  }

  public static void updateMetricsFromInitiator(String dbName, String tableName,
      String partitionName, Configuration conf, TxnStore txnHandler, long baseSize,
      Map<Path, Long> activeDeltaSizes, List<Path> obsoleteDeltaPaths) {
    if (!metricsEnabled) {
      LOG.debug("Acid metric collection is not enabled. To turn it on, \"metastore.acidmetrics.thread.on\" and " +
          "\"metastore.metrics.enabled\" must be set to true and HMS restarted.");
      return;
    }
    LOG.debug("Updating delta file metrics from initiator");
    double deltaPctThreshold =
        MetastoreConf.getDoubleVar(conf, MetastoreConf.ConfVars.METASTORE_DELTAMETRICS_DELTA_PCT_THRESHOLD);
    int deltasThreshold =
        MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.METASTORE_DELTAMETRICS_DELTA_NUM_THRESHOLD);
    int obsoleteDeltasThreshold =
        MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.METASTORE_DELTAMETRICS_OBSOLETE_DELTA_NUM_THRESHOLD);
    try {
      // We have an AcidDir from the initiator, therefore we can use that to calculate active,small, obsolete delta
      // count

      int numDeltas = activeDeltaSizes.size();
      int numSmallDeltas = 0;

      for (long deltaSize : activeDeltaSizes.values()) {
        if (baseSize != 0 && deltaSize / (float) baseSize < deltaPctThreshold) {
          numSmallDeltas++;
        }
      }

      int numObsoleteDeltas = filterOutBaseAndOriginalFiles(obsoleteDeltaPaths).size();

      updateDeltaMetrics(dbName, tableName, partitionName, NUM_DELTAS, numDeltas, deltasThreshold, txnHandler);
      updateDeltaMetrics(dbName, tableName, partitionName, NUM_SMALL_DELTAS, numSmallDeltas, deltasThreshold,
          txnHandler);
      updateDeltaMetrics(dbName, tableName, partitionName, CompactionMetricsData.MetricType.NUM_OBSOLETE_DELTAS,
          numObsoleteDeltas, obsoleteDeltasThreshold, txnHandler);

      LOG.debug("Finished updating delta file metrics from initiator.\n deltaPctThreshold = {}, deltasThreshold = {}, "
              + "obsoleteDeltasThreshold = {}, numDeltas = {}, numSmallDeltas = {},  numObsoleteDeltas = {}",
          deltaPctThreshold, deltasThreshold, obsoleteDeltasThreshold, numDeltas, numSmallDeltas, numObsoleteDeltas);

    } catch (Throwable t) {
      LOG.warn("Unknown throwable caught while updating delta metrics. Metrics will not be updated.", t);
    }
  }

  public static void updateMetricsFromWorker(String dbName, String tableName,
      String partitionName, CompactionType type, int preWorkerActiveDeltaCount, int preWorkerDeleteDeltaCount,
      Configuration conf, IMetaStoreClient client) {
    if (!(MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED) &&
        MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_THREAD_ON))) {
      LOG.debug("Acid metric collection is not enabled. To turn it on, \"metastore.acidmetrics.thread.on\" and "
          + "\"metastore.metrics.enabled\" must be set to true and HS2/HMS restarted.");
      return;
    }
    LOG.debug("Updating delta file metrics from worker");
    int deltasThreshold =
        MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.METASTORE_DELTAMETRICS_DELTA_NUM_THRESHOLD);
    int obsoleteDeltasThreshold =
        MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.METASTORE_DELTAMETRICS_OBSOLETE_DELTA_NUM_THRESHOLD);
    try {
      // we have an instance of the AcidDirectory before the compaction worker was started
      // from this we can get how many delta directories existed
      // the previously active delta directories are now moved to obsolete
      updateDeltaMetrics(dbName, tableName, partitionName, CompactionMetricsMetricType.NUM_OBSOLETE_DELTAS,
          preWorkerActiveDeltaCount, obsoleteDeltasThreshold, client);

      // We don't know the size of the newly create delta directories, that would require a fresh AcidDirectory
      // Clear the small delta num counter from the cache for this key
      removeDeltaMetrics(dbName, tableName, partitionName, CompactionMetricsMetricType.NUM_SMALL_DELTAS, client);

      // The new number of active delta dirs are either 0, 1 or 2.
      // If we ran MAJOR compaction, no new delta is created, just base dir
      // If we ran MINOR compaction, we can have 1 or 2 new delta dirs, depending on whether we had deltas or
      // delete deltas.
      if (type == CompactionType.MAJOR) {
        removeDeltaMetrics(dbName, tableName, partitionName, CompactionMetricsMetricType.NUM_DELTAS, client);
      } else {
        int numNewDeltas = 0;
        // check whether we had deltas
        if (preWorkerDeleteDeltaCount > 0) {
          numNewDeltas++;
        }

        // if the size of the current dirs is bigger than the size of delete deltas, it means we have active deltas
        if (preWorkerActiveDeltaCount > preWorkerDeleteDeltaCount) {
          numNewDeltas++;
        }

        // recalculate the delta count
        updateDeltaMetrics(dbName, tableName, partitionName, CompactionMetricsMetricType.NUM_DELTAS, numNewDeltas,
            deltasThreshold, client);
      }

      LOG.debug("Finished updating delta file metrics from worker.\n deltasThreshold = {}, "
              + "obsoleteDeltasThreshold = {}, numObsoleteDeltas = {}", deltasThreshold, obsoleteDeltasThreshold,
          preWorkerActiveDeltaCount);

    } catch (Throwable t) {
      LOG.warn("Unknown throwable caught while updating delta metrics. Metrics will not be updated.", t);
    }
  }

  public static void updateMetricsFromCleaner(String dbName, String tableName, String partitionName,
      List<Path> deletedFiles, Configuration conf, TxnStore txnHandler) {
    if (!metricsEnabled) {
      LOG.debug("Acid metric collection is not enabled. To turn it on, \"metastore.acidmetrics.thread.on\" and "
          + "\"metastore.metrics.enabled\" must be set to true and HMS restarted.");
      return;
    }
    LOG.debug("Updating delta file metrics from cleaner");
    int obsoleteDeltasThreshold = MetastoreConf.getIntVar(conf,
        MetastoreConf.ConfVars.METASTORE_DELTAMETRICS_DELTA_NUM_THRESHOLD);
    try {
      CompactionMetricsData prevObsoleteDelta =
          txnHandler.getCompactionMetricsData(dbName, tableName, partitionName,
              CompactionMetricsData.MetricType.NUM_OBSOLETE_DELTAS);
      int numObsoleteDeltas = 0;
      if (prevObsoleteDelta != null) {
        numObsoleteDeltas = prevObsoleteDelta.getMetricValue() - filterOutBaseAndOriginalFiles(deletedFiles).size();
        updateDeltaMetrics(dbName, tableName, partitionName, CompactionMetricsData.MetricType.NUM_OBSOLETE_DELTAS,
            numObsoleteDeltas, obsoleteDeltasThreshold, txnHandler);
      }

      LOG.debug("Finished updating delta file metrics from cleaner.\n obsoleteDeltasThreshold = {}, "
          + "numObsoleteDeltas = {}", obsoleteDeltasThreshold, numObsoleteDeltas);

    } catch (Throwable t) {
      LOG.warn("Unknown throwable caught while updating delta metrics. Metrics will not be updated.", t);
    }
  }

  private void updateDeltaMetrics() {
    try {
      LOG.debug("Called reporting task.");
      List<CompactionMetricsData> deltas = txnHandler.getTopCompactionMetricsDataPerType(maxCacheSize);
      Map<String, Integer> deltasMap = deltas.stream().filter(d -> d.getMetricType() == NUM_DELTAS).collect(
          Collectors.toMap(item -> getDeltaCountKey(item.getDbName(), item.getTblName(), item.getPartitionName()),
              CompactionMetricsData::getMetricValue));
      updateDeltaMBeanAndMetric(deltaObject, COMPACTION_NUM_DELTAS, deltasMap);

      Map<String, Integer> smallDeltasMap = deltas.stream().filter(d -> d.getMetricType() == NUM_SMALL_DELTAS)
          .collect(
              Collectors.toMap(item -> getDeltaCountKey(item.getDbName(), item.getTblName(), item.getPartitionName()),
                  CompactionMetricsData::getMetricValue));
      updateDeltaMBeanAndMetric(smallDeltaObject, COMPACTION_NUM_SMALL_DELTAS, smallDeltasMap);

      Map<String, Integer> obsoleteDeltasMap = deltas.stream().filter(d -> d.getMetricType() == NUM_OBSOLETE_DELTAS)
          .collect(
              Collectors.toMap(item -> getDeltaCountKey(item.getDbName(), item.getTblName(), item.getPartitionName()),
                  CompactionMetricsData::getMetricValue));
      updateDeltaMBeanAndMetric(obsoleteDeltaObject, COMPACTION_NUM_OBSOLETE_DELTAS, obsoleteDeltasMap);
    } catch (Throwable e) {
      LOG.warn("Caught exception while trying to fetch compaction metrics from metastore backend db.", e);
    }
  }

  private void updateDeltaMBeanAndMetric(MetricsMBeanImpl mbean, String metricName, Map<String, Integer> update) {
    mbean.updateAll(update);
    Metrics.getOrCreateMapMetrics(metricName)
        .update(update);
  }

  private void updateMetrics() throws MetaException {
    ShowCompactResponse currentCompactions = txnHandler.showCompact(new ShowCompactRequest());
    updateMetricsFromShowCompact(currentCompactions);
    updateDBMetrics();
  }

  private void updateDBMetrics() throws MetaException {
    MetricsInfo metrics = txnHandler.getMetricsInfo();
    Metrics.getOrCreateGauge(NUM_TXN_TO_WRITEID).set(metrics.getTxnToWriteIdCount());
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

  @VisibleForTesting
  public static void updateMetricsFromShowCompact(ShowCompactResponse showCompactResponse) {
    CompactionMetricData metricData = CompactionMetricData.of(showCompactResponse.getCompacts());

    // Get the current count for each state
    Map<String, Long> counts = metricData.getStateCount();

    // Update metrics
    for (int i = 0; i < TxnStore.COMPACTION_STATES.length; ++i) {
      String key = COMPACTION_STATUS_PREFIX + replaceWhitespace(TxnStore.COMPACTION_STATES[i]);
      Long count = counts.get(TxnStore.COMPACTION_STATES[i]);
      if (count != null) {
        Metrics.getOrCreateGauge(key)
            .set(count.intValue());
      } else {
        Metrics.getOrCreateGauge(key)
            .set(0);
      }
    }

    Metrics.getOrCreateMapMetrics(COMPACTION_POOLS_INITIATED_ITEM_COUNT).update(metricData.getInitiatedCountPerPool());
    Metrics.getOrCreateMapMetrics(COMPACTION_POOLS_WORKING_ITEM_COUNT).update(metricData.getWorkingCountPerPool());
    Metrics.getOrCreateMapMetrics(COMPACTION_POOLS_OLDEST_INITIATED_AGE).update(metricData.getLongestEnqueueDurationPerPool());
    Metrics.getOrCreateMapMetrics(COMPACTION_POOLS_OLDEST_WORKING_AGE).update(metricData.getLongestWorkingDurationPerPool());

    updateOldestCompactionMetric(COMPACTION_OLDEST_ENQUEUE_AGE, metricData.getOldestEnqueueTime());
    updateOldestCompactionMetric(COMPACTION_OLDEST_WORKING_AGE, metricData.getOldestWorkingTime());
    updateOldestCompactionMetric(COMPACTION_OLDEST_CLEANING_AGE, metricData.getOldestCleaningTime());

    Metrics.getOrCreateGauge(COMPACTION_NUM_INITIATORS)
        .set((int) metricData.getInitiatorsCount());
    Metrics.getOrCreateGauge(COMPACTION_NUM_WORKERS)
        .set((int) metricData.getWorkersCount());

    Metrics.getOrCreateGauge(COMPACTION_NUM_INITIATOR_VERSIONS)
        .set((int) metricData.getInitiatorVersionsCount());
    Metrics.getOrCreateGauge(COMPACTION_NUM_WORKER_VERSIONS)
        .set((int) metricData.getWorkerVersionsCount());
  }

  private static void updateOldestCompactionMetric(String metricName, Long oldestTime) {
    if (oldestTime == null) {
      Metrics.getOrCreateGauge(metricName)
          .set(0);
    } else {
      int oldestAge = (int) ((System.currentTimeMillis() - oldestTime) / 1000L);
      Metrics.getOrCreateGauge(metricName)
          .set(oldestAge);
    }
  }

  @Override
  public void setConf(Configuration configuration) {
    conf = configuration;
    txnHandler = TxnUtils.getTxnStore(conf);
    metricsEnabled = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED) &&
        MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_THREAD_ON);

    try {
      if (metricsEnabled) {
        maxCacheSize = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.METASTORE_DELTAMETRICS_MAX_CACHE_SIZE);
        initObjectsForMetrics();
      }
    } catch (Exception e) {
      LOG.error("Cannot initialize delta file metrics mbean server. AcidMetricService initialization aborted.", e);
    }
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

  private void initObjectsForMetrics() throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

    obsoleteDeltaObject = new MetricsMBeanImpl();
    mbs.registerMBean(obsoleteDeltaObject,
        new ObjectName(OBJECT_NAME_PREFIX + COMPACTION_NUM_OBSOLETE_DELTAS));
    deltaObject = new MetricsMBeanImpl();
    mbs.registerMBean(deltaObject,
        new ObjectName(OBJECT_NAME_PREFIX + COMPACTION_NUM_DELTAS));
    smallDeltaObject = new MetricsMBeanImpl();
    mbs.registerMBean(smallDeltaObject,
        new ObjectName(OBJECT_NAME_PREFIX + COMPACTION_NUM_SMALL_DELTAS));
  }

  static String getDeltaCountKey(String dbName, String tableName, String partitionName) {
    StringBuilder key = new StringBuilder();
    if (dbName == null || dbName.isEmpty()) {
      key.append(tableName);
    } else {
      key.append(dbName).append(".").append(tableName);
    }

    if (partitionName != null && !partitionName.isEmpty()) {
      key.append(Path.SEPARATOR);
      if (partitionName.startsWith("{") && partitionName.endsWith("}")) {
        key.append(partitionName, 1, partitionName.length() - 1);
      } else {
        key.append(partitionName);
      }
    }
    return key.toString();
  }

  private static List<Path> filterOutBaseAndOriginalFiles(List<Path> paths) {
    return paths.stream().filter(p -> p.getName().startsWith(AcidConstants.DELTA_PREFIX) || p.getName()
        .startsWith(AcidConstants.DELETE_DELTA_PREFIX)).collect(Collectors.toList());
  }

  private static void updateDeltaMetrics(String dbName, String tblName, String partitionName,
      CompactionMetricsData.MetricType type, int numDeltas, int deltasThreshold, TxnStore txnHandler) throws MetaException {
    CompactionMetricsData data = new CompactionMetricsData.Builder()
        .dbName(dbName).tblName(tblName).partitionName(partitionName).metricType(type).metricValue(numDeltas).version(0)
        .threshold(deltasThreshold).build();
    if (!txnHandler.updateCompactionMetricsData(data)) {
      LOG.warn("Compaction metric data cannot be updated because of version mismatch.");
    }
  }

  private static void updateDeltaMetrics(String dbName, String tblName, String partitionName,
      CompactionMetricsMetricType type, int numDeltas, int deltasThreshold, IMetaStoreClient client) throws TException {
    CompactionMetricsDataStruct struct = new CompactionMetricsDataStruct();
    struct.setDbname(dbName);
    struct.setTblname(tblName);
    struct.setPartitionname(partitionName);
    struct.setType(type);
    struct.setMetricvalue(numDeltas);
    struct.setVersion(0);
    struct.setThreshold(deltasThreshold);
    if (!client.updateCompactionMetricsData(struct)) {
      LOG.warn("Compaction metric data cannot be updated because of version mismatch.");
    }
  }


  private static void removeDeltaMetrics(String dbName, String tblName, String partitionName,
      CompactionMetricsMetricType type, IMetaStoreClient client) throws TException {
    CompactionMetricsDataRequest request = new CompactionMetricsDataRequest(dbName, tblName, type);
    request.setPartitionName(partitionName);
    client.removeCompactionMetricsData(request);
  }
}
