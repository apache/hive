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
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;

import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hive.common.util.Ref;
import org.apache.tez.common.counters.TezCounters;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

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
 */
public class DeltaFilesMetricReporter {

  private static final Logger LOG = LoggerFactory.getLogger(AcidUtils.class);
  private boolean acidMetricsExtEnabled;

  public static final String OBJECT_NAME_PREFIX = "metrics:type=compaction,name=";

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

  private int deltasThreshold;
  private int obsoleteDeltasThreshold;

  private int maxCacheSize;

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
    acidMetricsExtEnabled = MetastoreConf.getBoolVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_EXT_ON);

    deltasThreshold = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_DELTA_NUM_THRESHOLD);
    obsoleteDeltasThreshold = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_OBSOLETE_DELTA_NUM_THRESHOLD);

    initCachesForMetrics(conf);
    initObjectsForMetrics();

    long reportingInterval = HiveConf.getTimeVar(conf,
      HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_REPORTING_INTERVAL, TimeUnit.SECONDS);

    ThreadFactory threadFactory =
      new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat("DeltaFilesMetricReporter %d")
        .build();
    executorService = Executors.newSingleThreadScheduledExecutor(threadFactory);
    executorService.scheduleAtFixedRate(
      new ReportingTask(), 0, reportingInterval, TimeUnit.SECONDS);

    LOG.info("Started DeltaFilesMetricReporter thread");
  }

  public void submit(TezCounters counters) {
    if (acidMetricsExtEnabled) {
      updateMetrics(NUM_OBSOLETE_DELTAS,
        obsoleteDeltaCache, obsoleteDeltaTopN, obsoleteDeltasThreshold,
        counters);
      updateMetrics(NUM_DELTAS,
        deltaCache, deltaTopN, deltasThreshold,
        counters);
      updateMetrics(NUM_SMALL_DELTAS,
        smallDeltaCache, smallDeltaTopN, deltasThreshold,
        counters);
    }
  }

  private void updateMetrics(DeltaFilesMetricType metric, Cache<String, Integer> cache, Queue<Pair<String, Integer>> topN,
        int threshold, TezCounters counters) {
    counters.getGroup(metric.value).forEach(counter -> {
      Integer prev = cache.getIfPresent(counter.getName());
      if (prev != null && prev != counter.getValue()) {
        cache.invalidate(counter.getName());
      }
      if (counter.getValue() > threshold) {
        if (topN.size() == maxCacheSize) {
          Pair<String, Integer> lowest = topN.peek();
          if (lowest != null && counter.getValue() > lowest.getValue()) {
            cache.invalidate(lowest.getKey());
          }
        }
        if (topN.size() < maxCacheSize) {
          topN.add(Pair.of(counter.getName(), (int) counter.getValue()));
          cache.put(counter.getName(), (int) counter.getValue());
        }
      }
    });
  }

  public static void mergeDeltaFilesStats(AcidDirectory dir, long checkThresholdInSec,
        float deltaPctThreshold, EnumMap<DeltaFilesMetricType, Map<String, Integer>> deltaFilesStats) throws IOException {
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
    String path = getRelPath(dir);
    newDeltaFilesStats(numObsoleteDeltas, numDeltas, numSmallDeltas)
      .forEach((type, cnt) -> deltaFilesStats.computeIfAbsent(type, v -> new HashMap<>()).put(path, cnt));
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

  private static EnumMap<DeltaFilesMetricType, Integer> newDeltaFilesStats(int numObsoleteDeltas, int numDeltas, int numSmallDeltas) {
    return new EnumMap<DeltaFilesMetricType, Integer>(DeltaFilesMetricType.class) {{
      put(NUM_OBSOLETE_DELTAS, numObsoleteDeltas);
      put(NUM_DELTAS, numDeltas);
      put(NUM_SMALL_DELTAS, numSmallDeltas);
    }};
  }

  public static void createCountersForAcidMetrics(TezCounters tezCounters, JobConf jobConf) {
    if (HiveConf.getBoolVar(jobConf, HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED) &&
      MetastoreConf.getBoolVar(jobConf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_EXT_ON)) {

      Arrays.stream(DeltaFilesMetricType.values())
        .filter(type -> jobConf.get(type.name()) != null)
        .forEach(type ->
            Splitter.on(',').withKeyValueSeparator("->").split(jobConf.get(type.name())).forEach(
              (path, cnt) -> tezCounters.findCounter(type.value, path).setValue(Long.parseLong(cnt))
            )
        );
    }
  }

  public static void addAcidMetricsToConfObj(EnumMap<DeltaFilesMetricType, Map<String, Integer>> deltaFilesStats, Configuration conf) {
    deltaFilesStats.forEach((type, value) ->
        conf.set(type.name(), Joiner.on(",").withKeyValueSeparator("->").join(value))
    );
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
    maxCacheSize = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_MAX_CACHE_SIZE);
    long duration = HiveConf.getTimeVar(conf,
        HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_CACHE_DURATION, TimeUnit.SECONDS);

    Comparator<Pair<String, Integer>> c = Comparator.comparing(Pair::getValue);

    deltaTopN = new PriorityBlockingQueue<>(maxCacheSize, c);
    smallDeltaTopN = new PriorityBlockingQueue<>(maxCacheSize, c);
    obsoleteDeltaTopN = new PriorityBlockingQueue<>(maxCacheSize, c);

    deltaCache = CacheBuilder.newBuilder()
      .expireAfterWrite(duration, TimeUnit.SECONDS)
      .maximumSize(maxCacheSize)
      .removalListener(notification -> removalPredicate(deltaTopN, notification))
      .softValues()
      .build();

    smallDeltaCache = CacheBuilder.newBuilder()
      .expireAfterWrite(duration, TimeUnit.SECONDS)
      .maximumSize(maxCacheSize)
      .removalListener(notification -> removalPredicate(smallDeltaTopN, notification))
      .softValues()
      .build();

    obsoleteDeltaCache = CacheBuilder.newBuilder()
      .expireAfterWrite(duration, TimeUnit.SECONDS)
      .maximumSize(maxCacheSize)
      .removalListener(notification -> removalPredicate(obsoleteDeltaTopN, notification))
      .softValues()
      .build();
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
}
