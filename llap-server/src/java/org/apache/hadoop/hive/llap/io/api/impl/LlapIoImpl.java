/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.llap.io.api.impl;

import org.apache.hadoop.hive.llap.LogLevels;

import java.io.IOException;
import java.util.concurrent.Executors;

import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.cache.BuddyAllocator;
import org.apache.hadoop.hive.llap.cache.Cache;
import org.apache.hadoop.hive.llap.cache.EvictionAwareAllocator;
import org.apache.hadoop.hive.llap.cache.EvictionDispatcher;
import org.apache.hadoop.hive.llap.cache.LowLevelCacheImpl;
import org.apache.hadoop.hive.llap.cache.LowLevelCacheMemoryManager;
import org.apache.hadoop.hive.llap.cache.LowLevelCachePolicy;
import org.apache.hadoop.hive.llap.cache.LowLevelFifoCachePolicy;
import org.apache.hadoop.hive.llap.cache.LowLevelLrfuCachePolicy;
import org.apache.hadoop.hive.llap.cache.NoopCache;
import org.apache.hadoop.hive.llap.io.api.LlapIo;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer;
import org.apache.hadoop.hive.llap.io.decode.OrcColumnVectorProducer;
import org.apache.hadoop.hive.llap.io.metadata.OrcMetadataCache;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonQueueMetrics;
import org.apache.hadoop.hive.llap.metrics.MetricsUtils;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.orc.encoded.OrcCacheKey;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.metrics2.util.MBeans;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class LlapIoImpl implements LlapIo<VectorizedRowBatch> {
  public static final Logger LOG = LoggerFactory.getLogger(LlapIoImpl.class);
  public static final LogLevels LOGL = new LogLevels(LOG);

  private final ColumnVectorProducer cvp;
  private final ListeningExecutorService executor;
  private LlapDaemonCacheMetrics cacheMetrics;
  private LlapDaemonQueueMetrics queueMetrics;
  private ObjectName buddyAllocatorMXBean;
  private EvictionAwareAllocator allocator;

  private LlapIoImpl(Configuration conf) throws IOException {
    boolean useLowLevelCache = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_LOW_LEVEL_CACHE);
    // High-level cache not supported yet.
    if (LOGL.isInfoEnabled()) {
      LOG.info("Initializing LLAP IO" + (useLowLevelCache ? " with low level cache" : ""));
    }

    String displayName = "LlapDaemonCacheMetrics-" + MetricsUtils.getHostName();
    String sessionId = conf.get("llap.daemon.metrics.sessionid");
    this.cacheMetrics = LlapDaemonCacheMetrics.create(displayName, sessionId);

    displayName = "LlapDaemonQueueMetrics-" + MetricsUtils.getHostName();
    int[] intervals = conf.getInts(String.valueOf(
        HiveConf.ConfVars.LLAP_QUEUE_METRICS_PERCENTILE_INTERVALS));
    this.queueMetrics = LlapDaemonQueueMetrics.create(displayName, sessionId, intervals);

    LOG.info("Started llap daemon metrics with displayName: " + displayName +
        " sessionId: " + sessionId);

    Cache<OrcCacheKey> cache = useLowLevelCache ? null : new NoopCache<OrcCacheKey>();
    boolean useLrfu = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_USE_LRFU);
    LowLevelCachePolicy cachePolicy =
        useLrfu ? new LowLevelLrfuCachePolicy(conf) : new LowLevelFifoCachePolicy(conf);
    LowLevelCacheMemoryManager memManager = new LowLevelCacheMemoryManager(
        conf, cachePolicy, cacheMetrics);
    // Memory manager uses cache policy to trigger evictions.
    OrcMetadataCache metadataCache = new OrcMetadataCache(memManager, cachePolicy);
    LowLevelCacheImpl orcCache = null;
    if (useLowLevelCache) {
      // Allocator uses memory manager to request memory.
      allocator = new BuddyAllocator(conf, memManager, cacheMetrics);
      // Cache uses allocator to allocate and deallocate.
      orcCache = new LowLevelCacheImpl(cacheMetrics, cachePolicy, allocator, true);
      // And finally cache policy uses cache to notify it of eviction. The cycle is complete!
      cachePolicy.setEvictionListener(new EvictionDispatcher(orcCache, metadataCache));
      cachePolicy.setParentDebugDumper(orcCache);
      orcCache.init();
    } else {
      cachePolicy.setEvictionListener(new EvictionDispatcher(null, metadataCache));
    }
    // Arbitrary thread pool. Listening is used for unhandled errors for now (TODO: remove?)
    int numThreads = HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_IO_THREADPOOL_SIZE);
    executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numThreads,
        new ThreadFactoryBuilder().setNameFormat("IO-Elevator-Thread-%d").setDaemon(true).build()));

    // TODO: this should depends on input format and be in a map, or something.
    this.cvp = new OrcColumnVectorProducer(metadataCache, orcCache, cache, conf, cacheMetrics,
        queueMetrics);
    if (LOGL.isInfoEnabled()) {
      LOG.info("LLAP IO initialized");
    }

    registerMXBeans();
  }

  private void registerMXBeans() {
    buddyAllocatorMXBean = MBeans.register("LlapDaemon", "BuddyAllocatorInfo", allocator);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public InputFormat<NullWritable, VectorizedRowBatch> getInputFormat(
      InputFormat sourceInputFormat) {
    return new LlapInputFormat(sourceInputFormat, cvp, executor);
  }

  public LlapDaemonCacheMetrics getCacheMetrics() {
    return cacheMetrics;
  }

  public LlapDaemonQueueMetrics getQueueMetrics() {
    return queueMetrics;
  }

  @Override
  public void close() {
    LOG.info("Closing LlapIoImpl..");
    if (buddyAllocatorMXBean != null) {
      MBeans.unregister(buddyAllocatorMXBean);
      buddyAllocatorMXBean = null;
    }
    executor.shutdownNow();
  }
}
