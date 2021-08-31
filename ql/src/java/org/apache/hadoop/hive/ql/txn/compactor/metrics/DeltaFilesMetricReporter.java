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

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
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
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;

import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.common.util.Ref;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_NUM_DELTAS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS;
import static org.apache.hadoop.hive.metastore.metrics.MetricsConstants.COMPACTION_NUM_SMALL_DELTAS;

import static org.apache.hadoop.hive.ql.txn.compactor.metrics.DeltaFilesMetricReporter.DeltaFilesMetricType.NUM_DELTAS;
import static org.apache.hadoop.hive.ql.txn.compactor.metrics.DeltaFilesMetricReporter.DeltaFilesMetricType.NUM_OBSOLETE_DELTAS;
import static org.apache.hadoop.hive.ql.txn.compactor.metrics.DeltaFilesMetricReporter.DeltaFilesMetricType.NUM_SMALL_DELTAS;

/**
 * Collects and publishes ACID compaction related metrics.
 * Everything should be behind 2 feature flags: {@link HiveConf.ConfVars#HIVE_SERVER2_METRICS_ENABLED} and
 * {@link MetastoreConf.ConfVars#METASTORE_ACIDMETRICS_EXT_ON}.
 * First we store the information in the jobConf, then in Tez Counters, then in a cache stored here, then in a custom
 * MBean.
 */
public class DeltaFilesMetricReporter {

  private static final Logger LOG = LoggerFactory.getLogger(AcidUtils.class);

  public static final String OBJECT_NAME_PREFIX = "metrics:type=compaction,name=";
  public static final String JOB_CONF_DELTA_FILES_METRICS_METADATA = "delta.files.metrics.metadata";
  public static final char ENTRY_SEPARATOR = ';';
  public static final String KEY_VALUE_SEPARATOR = "->";

  private static long lastSuccessfulLoggingTime = 0;
  private String hiveEntitySeparator;

  public enum DeltaFilesMetricType {
    NUM_OBSOLETE_DELTAS("HIVE_ACID_NUM_OBSOLETE_DELTAS"),
    NUM_DELTAS("HIVE_ACID_NUM_DELTAS"),
    NUM_SMALL_DELTAS("HIVE_ACID_NUM_SMALL_DELTAS");

    private final String value;

    DeltaFilesMetricType(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  private Cache<String, Integer> deltaCache, smallDeltaCache;
  private Cache<String, Integer> obsoleteDeltaCache;

  private MetricsMBeanImpl deltaObject, smallDeltaObject, obsoleteDeltaObject;
  private List<ObjectName> registeredObjects = new ArrayList<>();

  private BlockingQueue<Pair<String, Integer>> deltaTopN, smallDeltaTopN;
  private BlockingQueue<Pair<String, Integer>> obsoleteDeltaTopN;

  private ScheduledExecutorService executorService;

  private static class InstanceHolder {
    public static DeltaFilesMetricReporter instance = new DeltaFilesMetricReporter();
  }

  private DeltaFilesMetricReporter() {
  }

  public static DeltaFilesMetricReporter getInstance() {
    return InstanceHolder.instance;
  }

  public static synchronized void init(HiveConf conf) throws Exception {
    getInstance().configure(conf);
  }

  private void configure(HiveConf conf) throws Exception {
    long reportingInterval =
        HiveConf.getTimeVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_REPORTING_INTERVAL, TimeUnit.SECONDS);
    hiveEntitySeparator = conf.getVar(HiveConf.ConfVars.HIVE_ENTITY_SEPARATOR);

    initCachesForMetrics(conf);
    initObjectsForMetrics();

    ThreadFactory threadFactory =
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("DeltaFilesMetricReporter %d").build();
    executorService = Executors.newSingleThreadScheduledExecutor(threadFactory);
    executorService.scheduleAtFixedRate(new ReportingTask(), 0, reportingInterval, TimeUnit.SECONDS);

    LOG.info("Started DeltaFilesMetricReporter thread");
  }

  public void submit(TezCounters counters, Set<ReadEntity> inputs) {
    try {
      updateMetrics(NUM_OBSOLETE_DELTAS, obsoleteDeltaCache, obsoleteDeltaTopN, counters, inputs);
      updateMetrics(NUM_DELTAS, deltaCache, deltaTopN, counters, inputs);
      updateMetrics(NUM_SMALL_DELTAS, smallDeltaCache, smallDeltaTopN, counters, inputs);
    } catch (Exception e) {
      LOG.warn("Caught exception while trying to update delta metrics cache. Invalidating cache", e);
      try {
        obsoleteDeltaCache.invalidateAll();
        deltaCache.invalidateAll();
        smallDeltaCache.invalidateAll();
      } catch (Exception x) {
        LOG.warn("Caught exception while trying to invalidate cache", x);
      }
    }
  }

  /**
   * Copy counters to caches.
   */
  private void updateMetrics(DeltaFilesMetricType metric, Cache<String, Integer> cache,
      Queue<Pair<String, Integer>> topN, TezCounters counters, Set<ReadEntity> inputs) {

    // Create list of paths affected by the query
    List<String> inputPaths = Lists.newArrayList();
    if (inputs != null) {
      inputs.stream().map(readEntity -> readEntity.getName().split(hiveEntitySeparator)).forEach(inputNames -> {
        String dbName = inputNames[0];
        String tableName = inputNames[1];
        String partitionName = inputNames.length > 2 ? inputNames[2] : null;
        inputPaths.add(getDeltaCountKey(dbName, tableName, partitionName));
      });
    }

    // Invalidate from cache if the counter value differs from the cache value, or if the query touched the partition
    // in question but no counter was collected
    CounterGroup group = counters.getGroup(metric.value);
    for (String key : inputPaths) {
      Integer prev = cache.getIfPresent(key);
      if (prev != null) {
        TezCounter counter = counters.findCounter(group.getName(), key);
        if (counter != null && (counter.getValue() == 0 || counter.getValue() != prev)) {
          cache.invalidate(key);
        }
      }
    }

    // Add all counter values to the cache
    for (TezCounter counter : group) {
      if (counter.getValue() != 0) {
        topN.add(Pair.of(counter.getName(), (int) counter.getValue()));
        cache.put(counter.getName(), (int) counter.getValue());
      }
    }
  }

  /**
   * Update EnumMap<DeltaFilesMetricType, Queue<Pair<String, Integer>>> deltaFilesStats with {@link AcidDirectory}
   * contents
   */
  public static void mergeDeltaFilesStats(AcidDirectory dir, long checkThresholdInSec, float deltaPctThreshold,
      int deltasThreshold, int obsoleteDeltasThreshold, int maxCacheSize,
      EnumMap<DeltaFilesMetricType, Queue<Pair<String, Integer>>> deltaFilesStats, Configuration conf)
      throws IOException {

    long baseSize = getBaseSize(dir);
    int numObsoleteDeltas = getNumObsoleteDeltas(dir, checkThresholdInSec);

    int numDeltas = 0;
    int numSmallDeltas = 0;

    long now = new Date().getTime();

    for (AcidUtils.ParsedDelta delta : dir.getCurrentDirectories()) {
      if (now - getModificationTime(delta, dir.getFs()) >= checkThresholdInSec * 1000) {
        numDeltas++;

        long deltaSize = getDirSize(delta, dir.getFs());
        if (baseSize != 0 && deltaSize / (float) baseSize < deltaPctThreshold) {
          numSmallDeltas++;
        }
      }
    }

    logDeltaDirMetrics(dir, conf, numObsoleteDeltas, numDeltas, numSmallDeltas);

    String path = getRelPath(dir);

    String serializedMetadata = conf.get(JOB_CONF_DELTA_FILES_METRICS_METADATA);
    HashMap<Path, DeltaFilesMetadata> pathToMetadata = new HashMap<>();
    pathToMetadata = SerializationUtilities.deserializeObject(serializedMetadata, pathToMetadata.getClass());
    DeltaFilesMetadata metadata = pathToMetadata.get(dir.getPath());
    filterAndAddToDeltaFilesStats(NUM_DELTAS, numDeltas, deltasThreshold, deltaFilesStats, metadata, maxCacheSize);
    filterAndAddToDeltaFilesStats(NUM_OBSOLETE_DELTAS, numObsoleteDeltas, obsoleteDeltasThreshold, deltaFilesStats,
        metadata, maxCacheSize);
    filterAndAddToDeltaFilesStats(NUM_SMALL_DELTAS, numSmallDeltas, deltasThreshold, deltaFilesStats,
        metadata, maxCacheSize);
  }

  /**
   * Add partition and delta count to deltaFilesStats if the delta count is over the recording threshold and it is in
   * the top {@link HiveConf.ConfVars#HIVE_TXN_ACID_METRICS_MAX_CACHE_SIZE} deltas.
   */
  private static void filterAndAddToDeltaFilesStats(DeltaFilesMetricType type, int deltaCount, int deltasThreshold,
      EnumMap<DeltaFilesMetricType, Queue<Pair<String, Integer>>> deltaFilesStats, DeltaFilesMetadata metadata,
      int maxCacheSize) {
    if (deltaCount > deltasThreshold) {
      Queue<Pair<String,Integer>> pairQueue = deltaFilesStats.get(type);
      if (pairQueue != null && pairQueue.size() == maxCacheSize) {
        Pair<String, Integer> lowest = pairQueue.peek();
        if (lowest != null && deltaCount > lowest.getValue()) {
          pairQueue.poll();
        }
      }
      if (pairQueue == null || pairQueue.size() < maxCacheSize) {
        String deltaCountKey = getDeltaCountKey(metadata.dbName, metadata.tableName, metadata.partitionName);
        deltaFilesStats.computeIfAbsent(type,
            v -> (new PriorityQueue<>(maxCacheSize, getComparator()))).add(Pair.of(deltaCountKey, deltaCount));
      }
    }
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

  private static void logDeltaDirMetrics(AcidDirectory dir, Configuration conf, int numObsoleteDeltas, int numDeltas,
      int numSmallDeltas) {
    long loggerFrequency = HiveConf
        .getTimeVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ACID_METRICS_LOGGER_FREQUENCY, TimeUnit.MILLISECONDS);
    if (loggerFrequency <= 0) {
      return;
    }
    long currentTime = System.currentTimeMillis();
    if (lastSuccessfulLoggingTime == 0 || currentTime >= lastSuccessfulLoggingTime + loggerFrequency) {
      lastSuccessfulLoggingTime = currentTime;
      if (numDeltas >= HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_ACTIVE_DELTA_DIR_THRESHOLD)) {
        LOG.warn("Directory " + dir.getPath() + " contains " + numDeltas + " active delta directories. This can " +
            "cause performance degradation.");
      }

      if (numObsoleteDeltas >=
          HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_OBSOLETE_DELTA_DIR_THRESHOLD)) {
        LOG.warn("Directory " + dir.getPath() + " contains " + numDeltas + " obsolete delta directories. This can " +
            "indicate compaction cleaner issues.");
      }

      if (numSmallDeltas >= HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_COMPACTOR_SMALL_DELTA_DIR_THRESHOLD)) {
        LOG.warn("Directory " + dir.getPath() + " contains " + numDeltas + " small delta directories. This can " +
            "indicate performance degradation and there might be a problem with your streaming setup.");
      }
    }
  }

  private static int getNumObsoleteDeltas(AcidDirectory dir, long checkThresholdInSec) throws IOException {
    int numObsoleteDeltas = 0;
    for (Path obsolete : dir.getObsolete()) {
      FileStatus stat = dir.getFs().getFileStatus(obsolete);
      if (System.currentTimeMillis() - stat.getModificationTime() >= checkThresholdInSec * 1000) {
        numObsoleteDeltas++;
      }
    }
    return numObsoleteDeltas;
  }

  private static String getRelPath(AcidUtils.Directory directory) {
    return directory.getPath().getName().contains("=") ?
      directory.getPath().getParent().getName() + Path.SEPARATOR + directory.getPath().getName() :
      directory.getPath().getName();
  }

  public static void createCountersForAcidMetrics(TezCounters tezCounters, JobConf jobConf) {
    if (HiveConf.getBoolVar(jobConf, HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED) &&
      MetastoreConf.getBoolVar(jobConf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_EXT_ON)) {

      Arrays.stream(DeltaFilesMetricType.values())
        .filter(type -> jobConf.get(type.name()) != null)
        .forEach(type ->
            Splitter.on(ENTRY_SEPARATOR).withKeyValueSeparator(KEY_VALUE_SEPARATOR).split(jobConf.get(type.name())).forEach(
              (path, cnt) -> tezCounters.findCounter(type.value, path).setValue(Long.parseLong(cnt))
            )
        );
    }
  }

  public static void addAcidMetricsToConfObj(EnumMap<DeltaFilesMetricType,
      Queue<Pair<String, Integer>>> deltaFilesStats, Configuration conf) {
    deltaFilesStats.forEach((type, value) ->
        conf.set(type.name(), Joiner.on(ENTRY_SEPARATOR).withKeyValueSeparator(KEY_VALUE_SEPARATOR).join(value)));
  }

  public static void backPropagateAcidMetrics(JobConf jobConf, Configuration conf) {
    if (HiveConf.getBoolVar(jobConf, HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED) &&
      MetastoreConf.getBoolVar(jobConf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_EXT_ON)) {

      Arrays.stream(DeltaFilesMetricType.values())
        .filter(type -> conf.get(type.name()) != null)
        .forEach(type ->
            jobConf.set(type.name(), conf.get(type.name()))
        );
    }
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

  private static long getModificationTime(AcidUtils.ParsedDirectory dir, FileSystem fs) throws IOException {
    return dir.getFiles(fs, Ref.from(false)).stream()
      .map(HadoopShims.HdfsFileStatusWithId::getFileStatus)
      .mapToLong(FileStatus::getModificationTime)
      .max()
      .orElse(new Date().getTime());
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

  private void initCachesForMetrics(HiveConf conf) {
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
        obsoleteDeltaCache.cleanUp();
        obsoleteDeltaObject.updateAll(obsoleteDeltaCache.asMap());

        deltaCache.cleanUp();
        deltaObject.updateAll(deltaCache.asMap());

        smallDeltaCache.cleanUp();
        smallDeltaObject.updateAll(smallDeltaCache.asMap());
      }
    }
  }

  @NotNull
  public static void close() {
    if (getInstance() != null) {
      getInstance().shutdown();
    }
  }

  private void shutdown() {
    if (executorService != null) {
      executorService.shutdownNow();
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

  public static class DeltaFilesMetadata implements Serializable {
    public String dbName, tableName, partitionName;
  }
}
