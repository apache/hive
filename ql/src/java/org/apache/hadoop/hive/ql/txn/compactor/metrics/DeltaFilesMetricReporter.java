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
package org.apache.hadoop.hive.ql.txn.compactor.metrics;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.AcidConstants;
import org.apache.hadoop.hive.common.metrics.MetricsMBeanImpl;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.CompactionMetricsDataRequest;
import org.apache.hadoop.hive.metastore.api.CompactionMetricsDataStruct;
import org.apache.hadoop.hive.metastore.api.CompactionMetricsMetricType;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.CompactionMetricsData;
import org.apache.hadoop.hive.metastore.txn.TxnStore;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.thrift.TException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_NUM_DELTAS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_NUM_SMALL_DELTAS;
import static org.apache.hadoop.hive.metastore.txn.CompactionMetricsData.MetricType.NUM_DELTAS;
import static org.apache.hadoop.hive.metastore.txn.CompactionMetricsData.MetricType.NUM_SMALL_DELTAS;

/**
 * Collects and publishes ACID compaction related metrics.
 * Everything should be behind 2 feature flags: {@link MetastoreConf.ConfVars#METRICS_ENABLED} and
 * {@link MetastoreConf.ConfVars#METASTORE_ACIDMETRICS_EXT_ON}.
 * First we store the information in the HMS backend DB COMPACTION_METRICS_CACHE table, then in a custom MBean.
 * The contents if the backend table is logged out by a separate thread in a
 * {@link MetastoreConf.ConfVars#METASTORE_DELTAMETRICS_LOGGER_FREQUENCY} frequency.
 */
public class DeltaFilesMetricReporter {

  private static final Logger LOG = LoggerFactory.getLogger(AcidUtils.class);

  public static final String OBJECT_NAME_PREFIX = "metrics:type=compaction,name=";

  private static boolean initialized = false;

  private MetricsMBeanImpl deltaObject, smallDeltaObject, obsoleteDeltaObject;
  private final List<ObjectName> registeredObjects = new ArrayList<>();

  private ScheduledExecutorService reporterExecutorService;
  private ScheduledExecutorService loggerExecutorService;
  private int maxCacheSize;

  private static class InstanceHolder {
    public static DeltaFilesMetricReporter instance = new DeltaFilesMetricReporter();
  }

  private DeltaFilesMetricReporter() {
  }

  public static DeltaFilesMetricReporter getInstance() {
    return InstanceHolder.instance;
  }

  public static synchronized void init(Configuration conf, TxnStore txnHandler) throws Exception {
    if (!initialized) {
      getInstance().configure(conf, txnHandler);
      initialized = true;
    }
  }

  private void configure(Configuration conf, TxnStore txnHandler) throws Exception {
    long reportingInterval =
        MetastoreConf.getTimeVar(conf, MetastoreConf.ConfVars.METASTORE_DELTAMETRICS_REPORTING_INTERVAL, TimeUnit.SECONDS);

    maxCacheSize = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.METASTORE_DELTAMETRICS_MAX_CACHE_SIZE);

    initObjectsForMetrics();

    ThreadFactory threadFactory =
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("DeltaFilesMetricReporter %d").build();
    reporterExecutorService = Executors.newSingleThreadScheduledExecutor(threadFactory);
    reporterExecutorService.scheduleAtFixedRate(new ReportingTask(txnHandler), 0, reportingInterval, TimeUnit.SECONDS);

    LOG.info("Started DeltaFilesMetricReporter thread");

    long loggerFrequency = MetastoreConf
        .getTimeVar(conf, MetastoreConf.ConfVars.METASTORE_DELTAMETRICS_LOGGER_FREQUENCY, TimeUnit.MINUTES);
    loggerExecutorService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("DeltaFilesMetricLogger %d").build());
    loggerExecutorService.scheduleAtFixedRate(new LoggerTask(txnHandler), loggerFrequency, loggerFrequency, TimeUnit.MINUTES);

    LOG.info("Started DeltaFilesMetricLogger thread");
  }


  private static String getDeltaCountKey(String dbName, String tableName, String partitionName) {
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

  private void initObjectsForMetrics() throws Exception {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

    obsoleteDeltaObject = new MetricsMBeanImpl();
    registeredObjects.add(
      mbs.registerMBean(obsoleteDeltaObject,
        new ObjectName(OBJECT_NAME_PREFIX + COMPACTION_NUM_OBSOLETE_DELTAS))
        .getObjectName());

    deltaObject = new MetricsMBeanImpl();
    registeredObjects.add(
      mbs.registerMBean(deltaObject,
        new ObjectName(OBJECT_NAME_PREFIX + COMPACTION_NUM_DELTAS))
        .getObjectName());

    smallDeltaObject = new MetricsMBeanImpl();
    registeredObjects.add(
      mbs.registerMBean(smallDeltaObject,
        new ObjectName(OBJECT_NAME_PREFIX + COMPACTION_NUM_SMALL_DELTAS))
        .getObjectName());
  }

  private final class ReportingTask implements Runnable {

    private final TxnStore txnHandler;

    private ReportingTask(TxnStore txnHandler) {
      this.txnHandler = txnHandler;
    }
    @Override
    public void run() {
      Metrics metrics = MetricsFactory.getInstance();
      if (metrics != null) {
        try {
          LOG.debug("Called reporting task.");
          List<CompactionMetricsData> deltas = txnHandler.getTopCompactionMetricsDataPerType(maxCacheSize);
          Map<String, Integer> deltasMap = deltas.stream()
              .filter(d -> d.getMetricType() == NUM_DELTAS).collect(
              Collectors.toMap(item -> getDeltaCountKey(item.getDbName(), item.getTblName(), item.getPartitionName()),
                  CompactionMetricsData::getMetricValue));
          deltaObject.updateAll(deltasMap);

          Map<String, Integer> smallDeltasMap = deltas.stream()
              .filter(d -> d.getMetricType() == NUM_SMALL_DELTAS).collect(
              Collectors.toMap(item -> getDeltaCountKey(item.getDbName(), item.getTblName(), item.getPartitionName()),
                  CompactionMetricsData::getMetricValue));
          smallDeltaObject.updateAll(smallDeltasMap);

          Map<String, Integer> obsoleteDeltasMap = deltas.stream()
              .filter(d -> d.getMetricType() == CompactionMetricsData.MetricType.NUM_OBSOLETE_DELTAS).collect(
              Collectors.toMap(item -> getDeltaCountKey(item.getDbName(), item.getTblName(), item.getPartitionName()),
                  CompactionMetricsData::getMetricValue));
          obsoleteDeltaObject.updateAll(obsoleteDeltasMap);
        } catch (Throwable e) {
          LOG.warn("Caught exception while trying to fetch compaction metrics from metastore backend db.", e);
        }
      }
    }
  }

  private final class LoggerTask implements Runnable {

    private final TxnStore txnHandler;

    private LoggerTask(TxnStore txnHandler) {
      this.txnHandler = txnHandler;
    }
    @Override
    public void run() {
      try {
        List<CompactionMetricsData> deltas =
            txnHandler.getTopCompactionMetricsDataPerType(maxCacheSize);
        deltas.stream().filter(d -> d.getMetricType() == NUM_DELTAS).forEach(d ->
          LOG.warn(String.format("Directory %s contains %d active delta directories. " +
              "This can cause performance degradation.",
              getDeltaCountKey(d.getDbName(), d.getTblName(), d.getPartitionName()), d.getMetricValue())));
        deltas.stream().filter(d -> d.getMetricType() == NUM_SMALL_DELTAS).forEach(d ->
            LOG.warn(String.format("Directory %s contains %d small delta directories. " +
                "This can indicate performance degradation and there might be a problem with your streaming setup.",
                getDeltaCountKey(d.getDbName(), d.getTblName(), d.getPartitionName()), d.getMetricValue())));
        deltas.stream().filter(d -> d.getMetricType() == CompactionMetricsData.MetricType.NUM_OBSOLETE_DELTAS)
            .forEach(d ->
            LOG.warn(String.format("Directory %s contains %d obsolete delta directories. " +
                "This can indicate compaction cleaner issues.",
                getDeltaCountKey(d.getDbName(), d.getTblName(), d.getPartitionName()), d.getMetricValue())));
      } catch (MetaException e) {
        LOG.warn("Caught exception while trying to log delta metrics data.", e);
      }
    }
  }

  @NotNull
  public static void close() {
    if (getInstance() != null) {
      getInstance().shutdown();
      initialized = false;
    }
  }

  private void shutdown() {
    if (reporterExecutorService != null) {
      reporterExecutorService.shutdownNow();
    }

    if (loggerExecutorService != null) {
      loggerExecutorService.shutdownNow();
    }

    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    for (ObjectName oname : registeredObjects) {
      if (mbs.isRegistered(oname)) {
        try {
          mbs.unregisterMBean(oname);
        } catch (Exception e) {
          LOG.error(e.getMessage());
        }
      }
    }
  }

  public static void updateMetricsFromInitiator(AcidDirectory dir, String dbName, String tableName, String partitionName,
      Configuration conf, TxnStore txnHandler, long baseSize, Map<Path, Long> deltaSizes) {
    LOG.debug("Updating delta file metrics from initiator");
    double deltaPctThreshold = MetastoreConf.getDoubleVar(conf, MetastoreConf.ConfVars.METASTORE_DELTAMETRICS_DELTA_PCT_THRESHOLD);
    int deltasThreshold = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.METASTORE_DELTAMETRICS_DELTA_NUM_THRESHOLD);
    int obsoleteDeltasThreshold = MetastoreConf.getIntVar(conf,
        MetastoreConf.ConfVars.METASTORE_DELTAMETRICS_OBSOLETE_DELTA_NUM_THRESHOLD);
    try {
      // We have an AcidDir from the initiator, therefore we can use that to calculate active,small, obsolete delta
      // count

      int numDeltas = dir.getCurrentDirectories().size();
      int numSmallDeltas = 0;

      for (AcidUtils.ParsedDelta delta : dir.getCurrentDirectories()) {
        if (deltaSizes.containsKey(delta.getPath())) {
          long deltaSize = deltaSizes.get(delta.getPath());
          if (baseSize != 0 && deltaSize / (float) baseSize < deltaPctThreshold) {
            numSmallDeltas++;
          }
        }
      }

      int numObsoleteDeltas = filterOutBaseAndOriginalFiles(dir.getObsolete()).size();

      if (numDeltas >= deltasThreshold) {
        updateMetrics(dbName, tableName, partitionName, NUM_DELTAS, numDeltas, txnHandler);
      } else {
        removeMetrics(dbName, tableName, partitionName, NUM_DELTAS, txnHandler);
      }

      if (numSmallDeltas >= deltasThreshold) {
        updateMetrics(dbName, tableName, partitionName, NUM_SMALL_DELTAS, numSmallDeltas, txnHandler);
      } else {
        removeMetrics(dbName, tableName, partitionName, NUM_SMALL_DELTAS, txnHandler);
      }

      if (numObsoleteDeltas >= obsoleteDeltasThreshold) {
        updateMetrics(dbName, tableName, partitionName, CompactionMetricsData.MetricType.NUM_OBSOLETE_DELTAS, numObsoleteDeltas, txnHandler);
      } else {
        removeMetrics(dbName, tableName, partitionName, CompactionMetricsData.MetricType.NUM_OBSOLETE_DELTAS, txnHandler);
      }

      LOG.debug("Finished updating delta file metrics from initiator.\n deltaPctThreshold = {}, deltasThreshold = {}, "
          + "obsoleteDeltasThreshold = {}, numDeltas = {}, numSmallDeltas = {},  numObsoleteDeltas = {}",
          deltaPctThreshold, deltasThreshold, obsoleteDeltasThreshold, numDeltas, numSmallDeltas, numObsoleteDeltas);

    } catch (Throwable t) {
      LOG.warn("Unknown throwable caught while updating delta metrics. Metrics will not be updated.", t);
    }
  }

  public static void updateMetricsFromWorker(AcidDirectory directory, String dbName, String tableName, String partitionName,
      CompactionType type, Configuration conf, IMetaStoreClient client) {
    LOG.debug("Updating delta file metrics from worker");
    int deltasThreshold = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.METASTORE_DELTAMETRICS_DELTA_NUM_THRESHOLD);
    int obsoleteDeltasThreshold = MetastoreConf.getIntVar(conf,
        MetastoreConf.ConfVars.METASTORE_DELTAMETRICS_OBSOLETE_DELTA_NUM_THRESHOLD);
    try {
      // we have an instance of the AcidDirectory before the compaction worker was started
      // from this we can get how many delta directories existed
      // the previously active delta directories are now moved to obsolete
      int numObsoleteDeltas = directory.getCurrentDirectories().size();
      if (numObsoleteDeltas >= obsoleteDeltasThreshold) {
        updateMetrics(dbName, tableName, partitionName, CompactionMetricsMetricType.NUM_OBSOLETE_DELTAS,
            numObsoleteDeltas, client);
      } else {
        removeMetrics(dbName, tableName, partitionName, CompactionMetricsMetricType.NUM_OBSOLETE_DELTAS, client);
      }

      // We don't know the size of the newly create delta directories, that would require a fresh AcidDirectory
      // Clear the small delta num counter from the cache for this key
      client.removeCompactionMetricsData(new CompactionMetricsDataRequest(dbName, tableName, partitionName,
          CompactionMetricsMetricType.NUM_SMALL_DELTAS));

      // The new number of active delta dirs are either 0, 1 or 2.
      // If we ran MAJOR compaction, no new delta is created, just base dir
      // If we ran MINOR compaction, we can have 1 or 2 new delta dirs, depending on whether we had deltas or
      // delete deltas.
      if (type == CompactionType.MAJOR) {
        client.removeCompactionMetricsData(new CompactionMetricsDataRequest(dbName, tableName, partitionName,
            CompactionMetricsMetricType.NUM_DELTAS));
      } else {
        int numNewDeltas = 0;
        // check whether we had deltas
        if (directory.getDeleteDeltas().size() > 0) {
          numNewDeltas++;
        }

        // if the size of the current dirs is bigger than the size of delete deltas, it means we have active deltas
        if (directory.getCurrentDirectories().size() > directory.getDeleteDeltas().size()) {
          numNewDeltas++;
        }

        // recalculate the delta count
        if (numNewDeltas >= deltasThreshold) {
          updateMetrics(dbName, tableName, partitionName, CompactionMetricsMetricType.NUM_DELTAS, numNewDeltas, client);
        } else {
          removeMetrics(dbName, tableName, partitionName, CompactionMetricsMetricType.NUM_DELTAS, client);
        }
      }

      LOG.debug("Finished updating delta file metrics from worker.\n deltasThreshold = {}, "
              + "obsoleteDeltasThreshold = {}, numObsoleteDeltas = {}",
          deltasThreshold, obsoleteDeltasThreshold, numObsoleteDeltas);

    } catch (Throwable t) {
      LOG.warn("Unknown throwable caught while updating delta metrics. Metrics will not be updated.", t);
    }
  }

  public static void updateMetricsFromCleaner(String dbName, String tableName, String partitionName,
      List<Path> deletedFiles, Configuration conf, TxnStore txnHandler) {
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
        if (numObsoleteDeltas >= obsoleteDeltasThreshold) {
          updateMetrics(dbName, tableName, partitionName, CompactionMetricsData.MetricType.NUM_OBSOLETE_DELTAS,
              numObsoleteDeltas, txnHandler);
        } else {
          txnHandler.removeCompactionMetricsData(dbName, tableName, partitionName,
              CompactionMetricsData.MetricType.NUM_OBSOLETE_DELTAS);
        }
      }

      LOG.debug("Finished updating delta file metrics from cleaner.\n obsoleteDeltasThreshold = {}, "
              + "numObsoleteDeltas = {}", obsoleteDeltasThreshold, numObsoleteDeltas);

    } catch (Throwable t) {
      LOG.warn("Unknown throwable caught while updating delta metrics. Metrics will not be updated.", t);
    }
  }

  private static void updateMetrics(String dbName, String tblName, String partitionName,
      CompactionMetricsData.MetricType type, int numDeltas, TxnStore txnHandler) throws MetaException {
    CompactionMetricsData data = new CompactionMetricsData.Builder()
        .dbName(dbName).tblName(tblName).partitionName(partitionName).metricType(type).metricValue(numDeltas).build();
    if (!txnHandler.updateCompactionMetricsData(data)) {
      LOG.warn("Compaction metric data cannot be updated because of version mismatch.");
    }
  }

  private static void updateMetrics(String dbName, String tblName, String partitionName,
      CompactionMetricsMetricType type, int numDeltas, IMetaStoreClient client) throws TException {
    CompactionMetricsDataStruct struct = new CompactionMetricsDataStruct();
    struct.setDbname(dbName);
    struct.setTblname(tblName);
    struct.setPartitionname(partitionName);
    struct.setType(type);
    struct.setMetricvalue(numDeltas);
    if (!client.updateCompactionMetricsData(struct)) {
      LOG.warn("Compaction metric data cannot be updated because of version mismatch.");
    }
  }

  private static void removeMetrics(String dbName, String tblName, String partitionName,
      CompactionMetricsData.MetricType type, TxnStore txnHandler) throws MetaException {
    txnHandler.removeCompactionMetricsData(dbName, tblName, partitionName, type);
  }

  private static void removeMetrics(String dbName, String tblName, String partitionName,
      CompactionMetricsMetricType type, IMetaStoreClient client) throws TException {
    CompactionMetricsDataRequest request = new CompactionMetricsDataRequest(dbName, tblName, partitionName, type);
    client.removeCompactionMetricsData(request);
  }

  private static List<Path> filterOutBaseAndOriginalFiles(List<Path> paths) {
    return paths.stream().filter(p -> p.getName().startsWith(AcidConstants.DELTA_PREFIX) || p.getName()
        .startsWith(AcidConstants.DELETE_DELTA_PREFIX)).collect(Collectors.toList());
  }
}
