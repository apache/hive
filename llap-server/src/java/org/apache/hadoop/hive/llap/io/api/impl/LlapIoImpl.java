/*
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.management.ObjectName;

import org.apache.hadoop.hive.llap.daemon.impl.StatsRecordingThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.FileMetadataCache;
import org.apache.hadoop.hive.common.io.Allocator.BufferObjectFactory;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.cache.BuddyAllocator;
import org.apache.hadoop.hive.llap.cache.BufferUsageManager;
import org.apache.hadoop.hive.llap.cache.CacheContentsTracker;
import org.apache.hadoop.hive.llap.cache.EvictionDispatcher;
import org.apache.hadoop.hive.llap.cache.LlapDataBuffer;
import org.apache.hadoop.hive.llap.cache.LlapOomDebugDump;
import org.apache.hadoop.hive.llap.cache.LowLevelCache;
import org.apache.hadoop.hive.llap.cache.LowLevelCacheImpl;
import org.apache.hadoop.hive.llap.cache.LowLevelCacheMemoryManager;
import org.apache.hadoop.hive.llap.cache.LowLevelCachePolicy;
import org.apache.hadoop.hive.llap.cache.LowLevelFifoCachePolicy;
import org.apache.hadoop.hive.llap.cache.LowLevelLrfuCachePolicy;
import org.apache.hadoop.hive.llap.cache.SerDeLowLevelCacheImpl;
import org.apache.hadoop.hive.llap.cache.SimpleAllocator;
import org.apache.hadoop.hive.llap.cache.SimpleBufferManager;
import org.apache.hadoop.hive.llap.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.io.api.LlapIo;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer;
import org.apache.hadoop.hive.llap.io.decode.GenericColumnVectorProducer;
import org.apache.hadoop.hive.llap.io.decode.OrcColumnVectorProducer;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonIOMetrics;
import org.apache.hadoop.hive.llap.metrics.MetricsUtils;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.LlapCacheOnlyInputFormatInterface;
import org.apache.hadoop.hive.ql.io.orc.encoded.IoTrace;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hive.common.util.FixedSizedObjectPool;




import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class LlapIoImpl implements LlapIo<VectorizedRowBatch> {
  public static final Logger LOG = LoggerFactory.getLogger("LlapIoImpl");
  public static final Logger ORC_LOGGER = LoggerFactory.getLogger("LlapIoOrc");
  public static final Logger CACHE_LOGGER = LoggerFactory.getLogger("LlapIoCache");
  public static final Logger LOCKING_LOGGER = LoggerFactory.getLogger("LlapIoLocking");
  private static final String MODE_CACHE = "cache";

  // TODO: later, we may have a map
  private final ColumnVectorProducer orcCvp, genericCvp;
  private final ExecutorService executor;
  private final LlapDaemonCacheMetrics cacheMetrics;
  private final LlapDaemonIOMetrics ioMetrics;
  private ObjectName buddyAllocatorMXBean;
  private final Allocator allocator;
  private final LlapOomDebugDump memoryDump;
  private final FileMetadataCache fileMetadataCache;
  private final LowLevelCache dataCache;
  private final BufferUsageManager bufferManager;
  private final Configuration daemonConf;
  private LowLevelCachePolicy cachePolicy;

  private LlapIoImpl(Configuration conf) throws IOException {
    this.daemonConf = conf;
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

    MetadataCache metadataCache = null;
    SerDeLowLevelCacheImpl serdeCache = null; // TODO: extract interface when needed
    BufferUsageManager bufferManagerOrc = null, bufferManagerGeneric = null;
    boolean isEncodeEnabled = useLowLevelCache
        && HiveConf.getBoolVar(conf, ConfVars.LLAP_IO_ENCODE_ENABLED);
    if (useLowLevelCache) {
      // Memory manager uses cache policy to trigger evictions, so create the policy first.
      boolean useLrfu = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_USE_LRFU);
      long totalMemorySize = HiveConf.getSizeVar(conf, ConfVars.LLAP_IO_MEMORY_MAX_SIZE);
      int minAllocSize = (int)HiveConf.getSizeVar(conf, ConfVars.LLAP_ALLOCATOR_MIN_ALLOC);
      LowLevelCachePolicy cp = useLrfu ? new LowLevelLrfuCachePolicy(
          minAllocSize, totalMemorySize, conf) : new LowLevelFifoCachePolicy();
      boolean trackUsage = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_TRACK_CACHE_USAGE);
      if (trackUsage) {
        this.cachePolicy = new CacheContentsTracker(cp);
      } else {
        this.cachePolicy = cp;
      }
      // Allocator uses memory manager to request memory, so create the manager next.
      LowLevelCacheMemoryManager memManager = new LowLevelCacheMemoryManager(
          totalMemorySize, cachePolicy, cacheMetrics);
      cacheMetrics.setCacheCapacityTotal(totalMemorySize);
      // Cache uses allocator to allocate and deallocate, create allocator and then caches.
      BuddyAllocator allocator = new BuddyAllocator(conf, memManager, cacheMetrics);
      this.allocator = allocator;
      this.memoryDump = allocator;
      LowLevelCacheImpl cacheImpl = new LowLevelCacheImpl(
          cacheMetrics, cachePolicy, allocator, true);
      dataCache = cacheImpl;
      if (isEncodeEnabled) {
        SerDeLowLevelCacheImpl serdeCacheImpl = new SerDeLowLevelCacheImpl(
            cacheMetrics, cachePolicy, allocator);
        serdeCache = serdeCacheImpl;
      }

      boolean useGapCache = HiveConf.getBoolVar(conf, ConfVars.LLAP_CACHE_ENABLE_ORC_GAP_CACHE);
      metadataCache = new MetadataCache(
          allocator, memManager, cachePolicy, useGapCache, cacheMetrics);
      fileMetadataCache = metadataCache;
      // And finally cache policy uses cache to notify it of eviction. The cycle is complete!
      EvictionDispatcher e = new EvictionDispatcher(
          dataCache, serdeCache, metadataCache, allocator);
      cachePolicy.setEvictionListener(e);
      cachePolicy.setParentDebugDumper(e);

      cacheImpl.startThreads(); // Start the cache threads.
      bufferManager = bufferManagerOrc = cacheImpl; // Cache also serves as buffer manager.
      bufferManagerGeneric = serdeCache;
    } else {
      this.allocator = new SimpleAllocator(conf);
      memoryDump = null;
      fileMetadataCache = null;
      SimpleBufferManager sbm = new SimpleBufferManager(allocator, cacheMetrics);
      bufferManager = bufferManagerOrc = bufferManagerGeneric = sbm;
      dataCache = sbm;
    }
    // IO thread pool. Listening is used for unhandled errors for now (TODO: remove?)
    int numThreads = HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_IO_THREADPOOL_SIZE);
    executor = new StatsRecordingThreadPool(numThreads, numThreads, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(),
        new ThreadFactoryBuilder().setNameFormat("IO-Elevator-Thread-%d").setDaemon(true).build());
    FixedSizedObjectPool<IoTrace> tracePool = IoTrace.createTracePool(conf);
    // TODO: this should depends on input format and be in a map, or something.
    this.orcCvp = new OrcColumnVectorProducer(
        metadataCache, dataCache, bufferManagerOrc, conf, cacheMetrics, ioMetrics, tracePool);
    this.genericCvp = isEncodeEnabled ? new GenericColumnVectorProducer(
        serdeCache, bufferManagerGeneric, conf, cacheMetrics, ioMetrics, tracePool) : null;
    LOG.info("LLAP IO initialized");

    registerMXBeans();
  }

  private void registerMXBeans() {
    buddyAllocatorMXBean = MBeans.register("LlapDaemon", "BuddyAllocatorInfo", allocator);
  }

  @Override
  public String getMemoryInfo() {
    if (memoryDump == null) return "\nNot using the allocator";
    StringBuilder sb = new StringBuilder();
    memoryDump.debugDumpShort(sb);
    return sb.toString();
  }

  @Override
  public long purge() {
    if (cachePolicy != null) {
      return cachePolicy.purge();
    }
    return 0;
  }

  @Override
  public InputFormat<NullWritable, VectorizedRowBatch> getInputFormat(
      InputFormat<?, ?> sourceInputFormat, Deserializer sourceSerDe) {
    ColumnVectorProducer cvp = genericCvp;
    if (sourceInputFormat instanceof OrcInputFormat) {
      cvp = orcCvp; // Special-case for ORC.
    } else if (cvp == null) {
      LOG.warn("LLAP encode is disabled; cannot use for " + sourceInputFormat.getClass());
      return null;
    }
    return new LlapInputFormat(sourceInputFormat, sourceSerDe, cvp, executor, daemonConf);
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


  @Override
  public void initCacheOnlyInputFormat(InputFormat<?, ?> inputFormat) {
    LlapCacheOnlyInputFormatInterface cacheIf = (LlapCacheOnlyInputFormatInterface)inputFormat;
    cacheIf.injectCaches(fileMetadataCache,
        new GenericDataCache(dataCache, bufferManager), daemonConf);
  }

  private class GenericDataCache implements DataCache, BufferObjectFactory {
    private final LowLevelCache lowLevelCache;
    private final BufferUsageManager bufferManager;

    public GenericDataCache(LowLevelCache lowLevelCache, BufferUsageManager bufferManager) {
      this.lowLevelCache = lowLevelCache;
      this.bufferManager = bufferManager;
    }

    @Override
    public DiskRangeList getFileData(Object fileKey, DiskRangeList range,
        long baseOffset, DiskRangeListFactory factory, BooleanRef gotAllData) {
      // TODO: we currently pass null counters because this doesn't use LlapRecordReader.
      //       Create counters for non-elevator-using fragments also?
      return lowLevelCache.getFileData(fileKey, range, baseOffset, factory, null, gotAllData);
    }

    @Override
    public long[] putFileData(Object fileKey, DiskRange[] ranges,
        MemoryBuffer[] data, long baseOffset) {
      return putFileData(fileKey, ranges, data, baseOffset, null);
    }

    @Override
    public long[] putFileData(Object fileKey, DiskRange[] ranges,
        MemoryBuffer[] data, long baseOffset, String tag) {
      return lowLevelCache.putFileData(
          fileKey, ranges, data, baseOffset, Priority.NORMAL, null, tag);
    }

    @Override
    public void releaseBuffer(MemoryBuffer buffer) {
      bufferManager.decRefBuffer(buffer);
    }

    @Override
    public void reuseBuffer(MemoryBuffer buffer) {
      boolean isReused = bufferManager.incRefBuffer(buffer);
      assert isReused;
    }

    @Override
    public Allocator getAllocator() {
      return bufferManager.getAllocator();
    }

    @Override
    public BufferObjectFactory getDataBufferFactory() {
      return this;
    }

    @Override
    public MemoryBuffer create() {
      return new LlapDataBuffer();
    }
  }
}
