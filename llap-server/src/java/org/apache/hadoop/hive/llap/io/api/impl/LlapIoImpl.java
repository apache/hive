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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;

import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.cache.BuddyAllocator;
import org.apache.hadoop.hive.llap.cache.BufferUsageManager;
import org.apache.hadoop.hive.llap.cache.EvictionAwareAllocator;
import org.apache.hadoop.hive.llap.cache.EvictionDispatcher;
import org.apache.hadoop.hive.llap.cache.LowLevelCache;
import org.apache.hadoop.hive.llap.cache.LowLevelCacheImpl;
import org.apache.hadoop.hive.llap.cache.LowLevelCacheMemoryManager;
import org.apache.hadoop.hive.llap.cache.LowLevelCachePolicy;
import org.apache.hadoop.hive.llap.cache.LowLevelFifoCachePolicy;
import org.apache.hadoop.hive.llap.cache.LowLevelLrfuCachePolicy;
import org.apache.hadoop.hive.llap.cache.SimpleAllocator;
import org.apache.hadoop.hive.llap.cache.SimpleBufferManager;
import org.apache.hadoop.hive.llap.io.api.LlapIo;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer;
import org.apache.hadoop.hive.llap.io.decode.OrcColumnVectorProducer;
import org.apache.hadoop.hive.llap.io.metadata.OrcMetadataCache;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonIOMetrics;
import org.apache.hadoop.hive.llap.metrics.MetricsUtils;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.metrics2.util.MBeans;

import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class LlapIoImpl implements LlapIo<VectorizedRowBatch> {
  public static final Logger LOG = LoggerFactory.getLogger("LlapIoImpl");
  public static final Logger ORC_LOGGER = LoggerFactory.getLogger("LlapIoOrc");
  public static final Logger CACHE_LOGGER = LoggerFactory.getLogger("LlapIoCache");
  public static final Logger LOCKING_LOGGER = LoggerFactory.getLogger("LlapIoLocking");

  private static final String MODE_CACHE = "cache";

  private final ColumnVectorProducer cvp;
  private final ListeningExecutorService executor;
  private LlapDaemonCacheMetrics cacheMetrics;
  private LlapDaemonIOMetrics ioMetrics;
  private ObjectName buddyAllocatorMXBean;
  private Allocator allocator;

  private LlapIoImpl(Configuration conf) throws IOException {
    String ioMode = HiveConf.getVar(conf, HiveConf.ConfVars.LLAP_IO_MEMORY_MODE);
    boolean useLowLevelCache = LlapIoImpl.MODE_CACHE.equalsIgnoreCase(ioMode);
    LOG.info("Initializing LLAP IO in {} mode", useLowLevelCache ? LlapIoImpl.MODE_CACHE : "none");
    String displayName = "LlapDaemonCacheMetrics-" + MetricsUtils.getHostName();
    String sessionId = conf.get("llap.daemon.metrics.sessionid");
    this.cacheMetrics = LlapDaemonCacheMetrics.create(displayName, sessionId);

    displayName = "LlapDaemonIOMetrics-" + MetricsUtils.getHostName();
    String[] strIntervals = HiveConf.getTrimmedStringsVar(conf,
        HiveConf.ConfVars.LLAP_IO_DECODING_METRICS_PERCENTILE_INTERVALS);
    List<Integer> intervalList = new ArrayList<>();
    if (strIntervals != null) {
      for (String strInterval : strIntervals) {
        try {
          intervalList.add(Integer.valueOf(strInterval));
        } catch (NumberFormatException e) {
          LOG.warn("Ignoring IO decoding metrics interval {} from {} as it is invalid", strInterval,
              Arrays.toString(strIntervals));
        }
      }
    }
    this.ioMetrics = LlapDaemonIOMetrics.create(displayName, sessionId, Ints.toArray(intervalList));

    LOG.info("Started llap daemon metrics with displayName: {} sessionId: {}", displayName,
        sessionId);

    OrcMetadataCache metadataCache = null;
    LowLevelCache cache = null;
    BufferUsageManager bufferManager = null;
    if (useLowLevelCache) {
      // Memory manager uses cache policy to trigger evictions, so create the policy first.
      boolean useLrfu = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_USE_LRFU);
      LowLevelCachePolicy cachePolicy =
          useLrfu ? new LowLevelLrfuCachePolicy(conf) : new LowLevelFifoCachePolicy(conf);
      // Allocator uses memory manager to request memory, so create the manager next.
      LowLevelCacheMemoryManager memManager = new LowLevelCacheMemoryManager(
          conf, cachePolicy, cacheMetrics);
      // Cache uses allocator to allocate and deallocate, create allocator and then caches.
      EvictionAwareAllocator allocator = new BuddyAllocator(conf, memManager, cacheMetrics);
      this.allocator = allocator;
      LowLevelCacheImpl cacheImpl = new LowLevelCacheImpl(
          cacheMetrics, cachePolicy, allocator, true);
      cache = cacheImpl;
      boolean useGapCache = HiveConf.getBoolVar(conf, ConfVars.LLAP_CACHE_ENABLE_ORC_GAP_CACHE);
      metadataCache = new OrcMetadataCache(memManager, cachePolicy, useGapCache);
      // And finally cache policy uses cache to notify it of eviction. The cycle is complete!
      cachePolicy.setEvictionListener(new EvictionDispatcher(cache, metadataCache));
      cachePolicy.setParentDebugDumper(cacheImpl);
      cacheImpl.startThreads(); // Start the cache threads.
      bufferManager = cacheImpl; // Cache also serves as buffer manager.
    } else {
      this.allocator = new SimpleAllocator(conf);
      SimpleBufferManager sbm = new SimpleBufferManager(allocator, cacheMetrics);
      bufferManager = sbm;
      cache = sbm;
    }
    // IO thread pool. Listening is used for unhandled errors for now (TODO: remove?)
    int numThreads = HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_IO_THREADPOOL_SIZE);
    executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(numThreads,
        new ThreadFactoryBuilder().setNameFormat("IO-Elevator-Thread-%d").setDaemon(true).build()));
    // TODO: this should depends on input format and be in a map, or something.
    this.cvp = new OrcColumnVectorProducer(
        metadataCache, cache, bufferManager, conf, cacheMetrics, ioMetrics);
    LOG.info("LLAP IO initialized");

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
