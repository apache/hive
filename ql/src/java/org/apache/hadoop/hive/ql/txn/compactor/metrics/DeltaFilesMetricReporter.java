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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalNotification;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.metrics.MetricsMBeanImpl;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hive.common.util.Ref;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_NUM_DELTAS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_NUM_SMALL_DELTAS;

/**
 * Collects and publishes ACID compaction related metrics.
 * Everything should be behind 2 feature flags: {@link MetastoreConf.ConfVars#METRICS_ENABLED} and
 * {@link MetastoreConf.ConfVars#METASTORE_ACIDMETRICS_EXT_ON}.
 * First we store the information in the jobConf, then in Tez Counters, then in a cache stored here, then in a custom
 * MBean.
 */
public class DeltaFilesMetricReporter {

  private static final Logger LOG = LoggerFactory.getLogger(AcidUtils.class);

  public static final String OBJECT_NAME_PREFIX = "metrics:type=compaction,name=";

  private static boolean initialized = false;

  private Cache<String, Integer> deltaCache, smallDeltaCache;
  private Cache<String, Integer> obsoleteDeltaCache;

  private MetricsMBeanImpl deltaObject, smallDeltaObject, obsoleteDeltaObject;
  private final List<ObjectName> registeredObjects = new ArrayList<>();

  private BlockingQueue<Pair<String, Integer>> deltaTopN, smallDeltaTopN;
  private BlockingQueue<Pair<String, Integer>> obsoleteDeltaTopN;

  private ScheduledExecutorService reporterExecutorService;
  private ScheduledExecutorService loggerExecutorService;

  private static class InstanceHolder {
    public static DeltaFilesMetricReporter instance = new DeltaFilesMetricReporter();
  }

  private DeltaFilesMetricReporter() {
  }

  public static DeltaFilesMetricReporter getInstance() {
    return InstanceHolder.instance;
  }

  public static synchronized void init(Configuration conf) throws Exception {
    if (!initialized) {
      getInstance().configure(conf);
      initialized = true;
    }
  }

  private void configure(Configuration conf) throws Exception {
    long reportingInterval =
        HiveConf.getTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_REPORTING_INTERVAL, TimeUnit.SECONDS);

    initCachesForMetrics(conf);
    initObjectsForMetrics();

    ThreadFactory threadFactory =
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("DeltaFilesMetricReporter %d").build();
    reporterExecutorService = Executors.newSingleThreadScheduledExecutor(threadFactory);
    reporterExecutorService.scheduleAtFixedRate(new ReportingTask(), 0, reportingInterval, TimeUnit.SECONDS);

    LOG.info("Started DeltaFilesMetricReporter thread");

    long loggerFrequency = HiveConf
        .getTimeVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ACID_METRICS_LOGGER_FREQUENCY, TimeUnit.MINUTES);
    loggerExecutorService = Executors.newSingleThreadScheduledExecutor(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("DeltaFilesMetricLogger %d").build());
    loggerExecutorService.scheduleAtFixedRate(new LoggerTask(), loggerFrequency, loggerFrequency, TimeUnit.MINUTES);

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


  private static long getBaseSize(AcidDirectory dir) throws IOException {
    long baseSize = 0;
    if (dir.getBase() != null) {
      baseSize = getDirSize(dir.getBase(), dir.getFs());
    } else {
      for (HadoopShims.HdfsFileStatusWithId origStat : dir.getOriginalFiles()) {
        baseSize += origStat.getFileStatus().getLen();
      }
    }
    return baseSize;
  }


  private static long getDirSize(AcidUtils.ParsedDirectory dir, FileSystem fs) throws IOException {
    return dir.getFiles(fs, Ref.from(false)).stream()
      .map(HadoopShims.HdfsFileStatusWithId::getFileStatus)
      .mapToLong(FileStatus::getLen)
      .sum();
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

  private void initCachesForMetrics(Configuration conf) {
    int maxCacheSize = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_MAX_CACHE_SIZE);
    long duration = HiveConf.getTimeVar(conf,
        HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_CACHE_DURATION, TimeUnit.SECONDS);

    deltaTopN = new PriorityBlockingQueue<>(maxCacheSize, getComparator());
    smallDeltaTopN = new PriorityBlockingQueue<>(maxCacheSize, getComparator());
    obsoleteDeltaTopN = new PriorityBlockingQueue<>(maxCacheSize, getComparator());

    deltaCache = CacheBuilder.newBuilder()
      .expireAfterWrite(duration, TimeUnit.SECONDS)
      .removalListener(notification -> removalPredicate(deltaTopN, notification))
      .softValues()
      .build();

    smallDeltaCache = CacheBuilder.newBuilder()
      .expireAfterWrite(duration, TimeUnit.SECONDS)
      .removalListener(notification -> removalPredicate(smallDeltaTopN, notification))
      .softValues()
      .build();

    obsoleteDeltaCache = CacheBuilder.newBuilder()
      .expireAfterWrite(duration, TimeUnit.SECONDS)
      .removalListener(notification -> removalPredicate(obsoleteDeltaTopN, notification))
      .softValues()
      .build();
  }

  private static Comparator<Pair<String, Integer>> getComparator() {
    return Comparator.comparing(Pair::getValue);
  }

  private void removalPredicate(BlockingQueue<Pair<String, Integer>> topN, RemovalNotification notification) {
    topN.removeIf(item -> item.getKey().equals(notification.getKey()));
  }

  private final class ReportingTask implements Runnable {
    @Override
    public void run() {
      Metrics metrics = MetricsFactory.getInstance();
      if (metrics != null) {
        LOG.debug("Called reporting task.");
        obsoleteDeltaCache.cleanUp();
        obsoleteDeltaObject.updateAll(obsoleteDeltaCache.asMap());

        deltaCache.cleanUp();
        deltaObject.updateAll(deltaCache.asMap());

        smallDeltaCache.cleanUp();
        smallDeltaObject.updateAll(smallDeltaCache.asMap());
      }
    }
  }

  private final class LoggerTask implements Runnable {
    @Override
    public void run() {
      deltaCache.asMap().forEach((k, v) ->
        LOG.warn(String.format("Directory %s contains %d active delta directories. "
            + "This can cause performance degradation.", k, v)));
      obsoleteDeltaCache.asMap().forEach((k, v) ->
          LOG.warn(String.format("Directory %s contains %d obsolete delta directories. "
              + "This can indicate compaction cleaner issues.", k, v)));
      smallDeltaCache.asMap().forEach((k, v) ->
          LOG.warn(String.format("Directory %s contains %d small delta directories. "
              + "This can indicate performance degradation and there might be a problem with your streaming setup.",
              k, v)));
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

  public void updateMetricsFromInitiator(AcidDirectory dir, String dbName, String tableName, String partitionName,
      Configuration conf) {
    LOG.debug("Updating delta file metrics from initiator");
    float deltaPctThreshold = HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_DELTA_PCT_THRESHOLD);
    int deltasThreshold = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_DELTA_NUM_THRESHOLD);
    int obsoleteDeltasThreshold = HiveConf.getIntVar(conf,
        HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_OBSOLETE_DELTA_NUM_THRESHOLD);
    try {
      // We have an AcidDir from the initiator, therefore we can use that to calculate active,small, obsolete delta
      // count
      long baseSize = getBaseSize(dir);

      int numDeltas = dir.getCurrentDirectories().size();
      int numSmallDeltas = 0;

      for (AcidUtils.ParsedDelta delta : dir.getCurrentDirectories()) {
        long deltaSize = getDirSize(delta, dir.getFs());
        if (baseSize != 0 && deltaSize / (float) baseSize < deltaPctThreshold) {
          numSmallDeltas++;
        }
      }

      String deltaCountKey = getDeltaCountKey(dbName, tableName, partitionName);
      if (numDeltas > deltasThreshold) {
        updateMetrics(deltaCountKey, numDeltas, deltaCache, deltaTopN);
      }

      if (numSmallDeltas > deltasThreshold) {
        updateMetrics(deltaCountKey, numSmallDeltas, smallDeltaCache, smallDeltaTopN);
      }

      int numObsoleteDeltas = dir.getObsolete().size();
      if (numObsoleteDeltas > obsoleteDeltasThreshold) {
        updateMetrics(deltaCountKey, numObsoleteDeltas, obsoleteDeltaCache, obsoleteDeltaTopN);
      }

      LOG.debug("Finished updating delta file metrics from initiator.\n deltaPctThreshold = {}, deltasThreshold = {}, "
          + "obsoleteDeltasThreshold = {}, numDeltas = {}, numSmallDeltas = {},  numObsoleteDeltas = {}",
          deltaPctThreshold, deltasThreshold, obsoleteDeltasThreshold, numDeltas, numSmallDeltas, numObsoleteDeltas);

    } catch (Throwable t) {
      LOG.warn("Unknown throwable caught while updating delta metrics. Metrics will not be updated.", t);
      try {
        deltaCache.invalidateAll();
        smallDeltaCache.invalidateAll();
        obsoleteDeltaCache.invalidateAll();
      } catch (Exception e) {
        LOG.warn("Caught exception while trying to invalidate cache.", e);
      }
    }
  }

  public void updateMetricsFromWorker(AcidDirectory directory, String dbName, String tableName, String partitionName,
      CompactionType type, Configuration conf) {
    LOG.debug("Updating delta file metrics from worker");
    int deltasThreshold = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_DELTA_NUM_THRESHOLD);
    int obsoleteDeltasThreshold = HiveConf.getIntVar(conf,
        HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_OBSOLETE_DELTA_NUM_THRESHOLD);
    try {
      // we have an instance of the AcidDirectory before the compaction worker was started
      // from this we can get how many delta directories existed
      // the previously active delta directories are now moved to obsolete
      String deltaCountKey = getDeltaCountKey(dbName, tableName, partitionName);
      int numObsoleteDeltas = directory.getCurrentDirectories().size();
      if (numObsoleteDeltas > obsoleteDeltasThreshold) {
        updateMetrics(deltaCountKey, numObsoleteDeltas, obsoleteDeltaCache, obsoleteDeltaTopN);
      }

      // We don't know the size of the newly create delta directories, that would require a fresh AcidDirectory
      // Clear the small delta num counter from the cache for this key
      smallDeltaCache.invalidate(deltaCountKey);
      smallDeltaTopN.remove(deltaCountKey);
      // The new number of active delta dirs are either 0, 1 or 2.
      // If we ran MAJOR compaction, no new delta is created, just base dir
      // If we ran MINOR compaction, we can have 1 or 2 new delta dirs, depending on whether we had deltas or
      // delete deltas.
      Integer prev = deltaCache.getIfPresent(deltaCountKey);
      if (prev != null) {
        if (type == CompactionType.MAJOR) {
          deltaCache.invalidate(deltaCountKey);
          deltaTopN.remove(deltaCountKey);
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
          int deltaNum = prev - directory.getCurrentDirectories().size() + numNewDeltas;
          if (deltaNum > deltasThreshold) {
            updateMetrics(deltaCountKey, deltaNum, deltaCache, deltaTopN);
          } else {
            deltaCache.invalidate(deltaCountKey);
            deltaTopN.remove(deltaCountKey);
          }
        }
      }

      LOG.debug("Finished updating delta file metrics from worker.\n deltasThreshold = {}, "
              + "obsoleteDeltasThreshold = {}, numObsoleteDeltas = {}",
          deltasThreshold, obsoleteDeltasThreshold, numObsoleteDeltas);

    } catch (Throwable t) {
      LOG.warn("Unknown throwable caught while updating delta metrics. Metrics will not be updated.", t);
      try {
        deltaCache.invalidateAll();
        smallDeltaCache.invalidateAll();
        obsoleteDeltaCache.invalidateAll();
      } catch (Exception e) {
        LOG.warn("Caught exception while trying to invalidate cache", e);
      }
    }
  }

  public void updateMetricsFromCleaner(String dbName, String tableName, String partitionName,
      int deletedFilesCount, Configuration conf) {
    LOG.debug("Updating delta file metrics from cleaner");
    int obsoleteDeltasThreshold = HiveConf.getIntVar(conf,
        HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_OBSOLETE_DELTA_NUM_THRESHOLD);
    try {
      String deltaCountKey = getDeltaCountKey(dbName, tableName, partitionName);
      Integer prev = obsoleteDeltaCache.getIfPresent(deltaCountKey);
      int numObsoleteDeltas = 0;
      if (prev != null) {
        numObsoleteDeltas = prev - deletedFilesCount;
        if (numObsoleteDeltas > obsoleteDeltasThreshold) {
          updateMetrics(deltaCountKey, numObsoleteDeltas, obsoleteDeltaCache, obsoleteDeltaTopN);
        } else {
          obsoleteDeltaCache.invalidate(deltaCountKey);
          obsoleteDeltaTopN.remove(deltaCountKey);
        }
      }

      LOG.debug("Finished updating delta file metrics from cleaner.\n obsoleteDeltasThreshold = {}, "
              + "numObsoleteDeltas = {}", obsoleteDeltasThreshold, numObsoleteDeltas);

    } catch (Throwable t) {
      LOG.warn("Unknown throwable caught while updating delta metrics. Metrics will not be updated.", t);
      try {
        obsoleteDeltaCache.invalidateAll();
      } catch (Exception e) {
        LOG.warn("Caught exception while trying to invalidate cache", e);
      }
    }
  }

  private void updateMetrics(String deltaCountKey, int numDeltas, Cache<String, Integer> cache,
      Queue<Pair<String, Integer>> topN) {
    Integer prev = cache.getIfPresent(deltaCountKey);
    if (prev != null && prev != numDeltas) {
      cache.invalidate(deltaCountKey);
    }

    topN.add(Pair.of(deltaCountKey, numDeltas));
    cache.put(deltaCountKey, numDeltas);
  }
}
