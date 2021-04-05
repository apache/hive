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
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.ql.io.AcidDirectory;
import org.apache.hadoop.hive.ql.io.AcidUtils;

import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Comparator;

import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * Collects and publishes ACID compaction related metrics.
 */
public class DeltaFilesMetricReporter {

  private static final Logger LOG = LoggerFactory.getLogger(AcidUtils.class);

  public static final String NUM_OBSOLETE_DELTAS = "HIVE_ACID_NUM_OBSOLETE_DELTAS";
  public static final String NUM_DELTAS = "HIVE_ACID_NUM_DELTAS";

  private int deltasThreshold;
  private int obsoleteDeltasThreshold;

  private int maxCacheSize;
  private static long checkThresholdInSec;

  private Cache<String, Integer> deltaCache;
  private Cache<String, Integer> obsoleteDeltaCache;

  private BlockingQueue<Pair<String, Integer>> deltaTopN;
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

  public static synchronized void init(HiveConf conf){
    getInstance().configure(conf);
  }

  public void submit(TezCounters counters) {
    updateMetrics(counters.getGroup(NUM_OBSOLETE_DELTAS),
        obsoleteDeltaCache, obsoleteDeltaTopN, obsoleteDeltasThreshold);
    updateMetrics(counters.getGroup(NUM_DELTAS),
        deltaCache, deltaTopN, deltasThreshold);
  }

  public static int getNumDeltas(AcidDirectory dir) throws IOException {
    int numDeltas = 0;
    for (AcidUtils.ParsedDelta delta : dir.getCurrentDirectories()) {
      FileStatus stat = dir.getFs().getFileStatus(delta.getPath());
      if (System.currentTimeMillis() - stat.getModificationTime() >= checkThresholdInSec * 1000) {
        numDeltas++;
      }
    }
    return numDeltas;
  }

  public static int getNumObsoleteDeltas(AcidDirectory dir) throws IOException {
    int numObsoleteDeltas = 0;
    for (Path obsolete : dir.getObsolete()) {
      FileStatus stat = dir.getFs().getFileStatus(obsolete);
      if (System.currentTimeMillis() - stat.getModificationTime() >= checkThresholdInSec * 1000) {
        numObsoleteDeltas++;
      }
    }
    return numObsoleteDeltas;
  }

  public static void close() {
    if (getInstance() != null) {
      getInstance().executorService.shutdownNow();
    }
  }

  private void configure(HiveConf conf){
    deltasThreshold = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_DELTA_NUM_THRESHOLD);
    obsoleteDeltasThreshold = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_OBSOLETE_DELTA_NUM_THRESHOLD);
    checkThresholdInSec = HiveConf.getTimeVar(conf,
        HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_DELTA_CHECK_THRESHOLD, TimeUnit.SECONDS);

    initMetricsCache(conf);
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

  private void initMetricsCache(HiveConf conf) {
    maxCacheSize = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_MAX_CACHE_SIZE);
    long duration = HiveConf.getTimeVar(conf,
        HiveConf.ConfVars.HIVE_TXN_ACID_METRICS_CACHE_DURATION, TimeUnit.SECONDS);

    Comparator<Pair<String, Integer>> c = Comparator.comparing(Pair::getValue);
    deltaTopN = new PriorityBlockingQueue<>(maxCacheSize, c);
    obsoleteDeltaTopN = new PriorityBlockingQueue<>(maxCacheSize, c);

    deltaCache = CacheBuilder.newBuilder()
      .expireAfterWrite(duration, TimeUnit.SECONDS)
      .maximumSize(maxCacheSize)
      .removalListener(notification -> removalPredicate(deltaTopN, notification))
      .softValues()
      .build();

    obsoleteDeltaCache = CacheBuilder.newBuilder()
      .expireAfterWrite(duration, TimeUnit.SECONDS)
      .maximumSize(maxCacheSize)
      .removalListener(notification -> removalPredicate(obsoleteDeltaTopN, notification))
      .softValues()
      .build();
  }

  private boolean removalPredicate(BlockingQueue<Pair<String, Integer>> deltaTopN, RemovalNotification notification) {
    return deltaTopN.removeIf(item -> item.getKey().equals(notification.getKey()));
  }

  private final class ReportingTask implements Runnable {
    @Override
    public void run() {
      Metrics metrics = MetricsFactory.getInstance();
      if (metrics != null) {
          obsoleteDeltaCache.cleanUp();
          metrics.addGauge(MetricsConstants.COMPACTION_NUM_OBSOLETE_DELTAS,
              () -> new TreeMap<>(obsoleteDeltaCache.asMap()));
          deltaCache.cleanUp();
          metrics.addGauge(MetricsConstants.COMPACTION_NUM_DELTAS,
              () -> new TreeMap<>(deltaCache.asMap()));
      }
    }
  }

  private void updateMetrics(Iterable<TezCounter> counterGroup, Cache<String, Integer> deltaCache,
          Queue<Pair<String, Integer>> deltaTopN, int deltaThreshold) {
    counterGroup.forEach(counter -> {
      Integer prev = deltaCache.getIfPresent(counter.getName());
      if (prev != null && prev != counter.getValue()) {
        deltaCache.invalidate(counter.getName());
      }
      if (counter.getValue() > deltaThreshold) {
        if (deltaTopN.size() == maxCacheSize) {
          Pair<String, Integer> lowest = deltaTopN.peek();
          if (lowest != null && counter.getValue() > lowest.getValue()) {
            deltaCache.invalidate(lowest.getKey());
          }
        }
        if (deltaTopN.size() < maxCacheSize) {
          deltaTopN.add(Pair.of(counter.getName(), (int) counter.getValue()));
          deltaCache.put(counter.getName(), (int) counter.getValue());
        }
      }
    });
  }
}
