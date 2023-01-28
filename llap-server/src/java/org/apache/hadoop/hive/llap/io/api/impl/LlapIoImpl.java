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
import java.util.function.Predicate;

import javax.annotation.Nullable;
import javax.management.ObjectName;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.common.io.encoded.MemoryBufferOrBuffers;
import org.apache.hadoop.hive.llap.ProactiveEviction;
import org.apache.hadoop.hive.llap.cache.LlapCacheHydration;
import org.apache.hadoop.hive.llap.cache.MemoryLimitedPathCache;
import org.apache.hadoop.hive.llap.cache.PathCache;
import org.apache.hadoop.hive.llap.cache.ProactiveEvictingCachePolicy;
import org.apache.hadoop.hive.llap.daemon.impl.LlapPooledIOThread;
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
import org.apache.hadoop.hive.llap.cache.LlapIoDebugDump;
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
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.io.api.LlapIo;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer;
import org.apache.hadoop.hive.llap.io.decode.GenericColumnVectorProducer;
import org.apache.hadoop.hive.llap.io.decode.OrcColumnVectorProducer;
import org.apache.hadoop.hive.llap.io.encoded.OrcEncodedDataReader;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonIOMetrics;
import org.apache.hadoop.hive.llap.metrics.MetricsUtils;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.LlapCacheOnlyInputFormatInterface;
import org.apache.hadoop.hive.ql.io.orc.OrcSplit;
import org.apache.hadoop.hive.ql.io.orc.encoded.IoTrace;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.vector.ParquetFooterInputFromCache;
import org.apache.hadoop.hive.ql.io.parquet.vector.VectorizedParquetRecordReader;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.hive.common.util.FixedSizedObjectPool;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.orc.impl.OrcTail;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.util.HadoopStreams;
import org.apache.parquet.io.SeekableInputStream;


import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import static org.apache.hadoop.hive.llap.LlapHiveUtils.throwIfCacheOnlyRead;

public class LlapIoImpl implements LlapIo<VectorizedRowBatch>, LlapIoDebugDump {
  public static final Logger LOG = LoggerFactory.getLogger("LlapIoImpl");
  public static final Logger ORC_LOGGER = LoggerFactory.getLogger("LlapIoOrc");
  public static final Logger CACHE_LOGGER = LoggerFactory.getLogger("LlapIoCache");
  public static final Logger LOCKING_LOGGER = LoggerFactory.getLogger("LlapIoLocking");
  private static final String MODE_CACHE = "cache";

  // TODO: later, we may have a map
  private final ColumnVectorProducer orcCvp, genericCvp;
  private final ExecutorService executor;
  private final ExecutorService encodeExecutor;
  private final LlapDaemonCacheMetrics cacheMetrics;
  private final LlapDaemonIOMetrics ioMetrics;
  private final boolean useLowLevelCache;
  private ObjectName buddyAllocatorMXBean;
  private final Allocator allocator;
  private final FileMetadataCache fileMetadataCache;
  private final LowLevelCache dataCache;
  private final SerDeLowLevelCacheImpl serdeCache;
  private final BufferUsageManager bufferManager;
  private final Configuration daemonConf;
  private final LowLevelCacheMemoryManager memoryManager;
  private PathCache pathCache;
  private final FixedSizedObjectPool<IoTrace> tracePool;
  private LowLevelCachePolicy realCachePolicy;

  private List<LlapIoDebugDump> debugDumpComponents = new ArrayList<>();

  private LlapIoImpl(Configuration conf) throws IOException {
    this.daemonConf = conf;
    String ioMode = HiveConf.getVar(conf, HiveConf.ConfVars.LLAP_IO_MEMORY_MODE);
    useLowLevelCache = LlapIoImpl.MODE_CACHE.equalsIgnoreCase(ioMode);
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
      int minAllocSize = (int) HiveConf.getSizeVar(conf, ConfVars.LLAP_ALLOCATOR_MIN_ALLOC);
      realCachePolicy =
          useLrfu ? new LowLevelLrfuCachePolicy(minAllocSize, totalMemorySize, conf) : new LowLevelFifoCachePolicy();
      if (!(realCachePolicy instanceof ProactiveEvictingCachePolicy.Impl)) {
        HiveConf.setBoolVar(this.daemonConf, ConfVars.LLAP_IO_PROACTIVE_EVICTION_ENABLED, false);
        LOG.info("Turning off proactive cache eviction, as selected cache policy does not support it.");
      }
      boolean trackUsage = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_TRACK_CACHE_USAGE);
      LowLevelCachePolicy cachePolicyWrapper;
      if (trackUsage) {
        cachePolicyWrapper = new CacheContentsTracker(realCachePolicy);
      } else {
        cachePolicyWrapper = realCachePolicy;
      }
      // Allocator uses memory manager to request memory, so create the manager next.
      this.memoryManager = new LowLevelCacheMemoryManager(
          totalMemorySize, cachePolicyWrapper, cacheMetrics);
      cacheMetrics.setCacheCapacityTotal(totalMemorySize);
      // Cache uses allocator to allocate and deallocate, create allocator and then caches.
      BuddyAllocator allocator = new BuddyAllocator(conf, memoryManager, cacheMetrics);
      this.allocator = allocator;
      LowLevelCacheImpl cacheImpl = new LowLevelCacheImpl(
          cacheMetrics, cachePolicyWrapper, allocator, true);
      dataCache = cacheImpl;
      if (isEncodeEnabled) {
        SerDeLowLevelCacheImpl serdeCacheImpl = new SerDeLowLevelCacheImpl(
            cacheMetrics, cachePolicyWrapper, allocator);
        serdeCache = serdeCacheImpl;
        serdeCacheImpl.setConf(conf);
      }

      boolean useGapCache = HiveConf.getBoolVar(conf, ConfVars.LLAP_CACHE_ENABLE_ORC_GAP_CACHE);
      metadataCache = new MetadataCache(
          allocator, memoryManager, cachePolicyWrapper, useGapCache, cacheMetrics);
      fileMetadataCache = metadataCache;
      // And finally cache policy uses cache to notify it of eviction. The cycle is complete!
      EvictionDispatcher e = new EvictionDispatcher(
          dataCache, serdeCache, metadataCache, allocator);
      cachePolicyWrapper.setEvictionListener(e);

      cacheImpl.startThreads(); // Start the cache threads.
      bufferManager = bufferManagerOrc = cacheImpl; // Cache also serves as buffer manager.
      bufferManagerGeneric = serdeCache;
      if (trackUsage) {
        debugDumpComponents.add(cachePolicyWrapper); // Cache contents tracker.
      }
      debugDumpComponents.add(realCachePolicy);
      debugDumpComponents.add(cacheImpl);
      if (serdeCache != null) {
        debugDumpComponents.add(serdeCache);
      }
      if (metadataCache != null) {
        debugDumpComponents.add(metadataCache);
      }
      debugDumpComponents.add(allocator);
      pathCache = new MemoryLimitedPathCache(conf);
    } else {
      this.allocator = new SimpleAllocator(conf);
      fileMetadataCache = null;
      SimpleBufferManager sbm = new SimpleBufferManager(allocator, cacheMetrics);
      bufferManager = bufferManagerOrc = bufferManagerGeneric = sbm;
      dataCache = sbm;
      this.memoryManager = null;
      debugDumpComponents.add(new LlapIoDebugDump() {
        @Override
        public void debugDumpShort(StringBuilder sb) {
          sb.append("LLAP IO allocator is not in use!");
        }
      });
    }
    this.serdeCache = serdeCache;
    // IO thread pool. Listening is used for unhandled errors for now (TODO: remove?)
    int numThreads = HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_IO_THREADPOOL_SIZE);
    executor = new StatsRecordingThreadPool(numThreads, numThreads, 0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>(),
        new ThreadFactoryBuilder().setNameFormat("IO-Elevator-Thread-%d").setDaemon(true)
            .setThreadFactory(r -> new LlapPooledIOThread(r)).build());
    tracePool = IoTrace.createTracePool(conf);
    if (isEncodeEnabled) {
      int encodePoolMultiplier = HiveConf.getIntVar(conf, ConfVars.LLAP_IO_ENCODE_THREADPOOL_MULTIPLIER);
      int encodeThreads = numThreads * encodePoolMultiplier;
      encodeExecutor = new StatsRecordingThreadPool(encodeThreads, encodeThreads, 0L, TimeUnit.MILLISECONDS,
          new LinkedBlockingQueue<Runnable>(),
          new ThreadFactoryBuilder().setNameFormat("IO-Elevator-Thread-OrcEncode-%d").setDaemon(true)
              .setThreadFactory(r -> new LlapPooledIOThread(r)).build());
    } else {
      encodeExecutor = null;
    }

    // TODO: this should depends on input format and be in a map, or something.
    this.orcCvp = new OrcColumnVectorProducer(
        metadataCache, dataCache, pathCache, bufferManagerOrc, conf, cacheMetrics, ioMetrics, tracePool);
    this.genericCvp = isEncodeEnabled ? new GenericColumnVectorProducer(
        serdeCache, bufferManagerGeneric, conf, cacheMetrics, ioMetrics, tracePool, encodeExecutor) : null;
    LOG.info("LLAP IO initialized");

    registerMXBeans();
    LlapCacheHydration.setupAndStartIfEnabled(daemonConf);
  }

  private void registerMXBeans() {
    buddyAllocatorMXBean = MBeans.register("LlapDaemon", "BuddyAllocatorInfo", allocator);
  }

  @Override
  public String getMemoryInfo() {
    StringBuilder sb = new StringBuilder();
    debugDumpShort(sb);
    return sb.toString();
  }

  @Override
  public long purge() {
    if (memoryManager != null) {
      return memoryManager.purge();
    }
    return 0;
  }

  public long evictEntity(LlapDaemonProtocolProtos.EvictEntityRequestProto protoRequest) {
    if (memoryManager == null || !HiveConf.getBoolVar(daemonConf, ConfVars.LLAP_IO_PROACTIVE_EVICTION_ENABLED)) {
      return -1;
    }
    final ProactiveEviction.Request request = ProactiveEviction.Request.Builder.create()
        .fromProtoRequest(protoRequest).build();
    Predicate<CacheTag> predicate = tag -> request.isTagMatch(tag);
    boolean isInstantDeallocation = HiveConf.getBoolVar(daemonConf,
        HiveConf.ConfVars.LLAP_IO_PROACTIVE_EVICTION_INSTANT_DEALLOC);
    LOG.debug("Starting proactive eviction.");
    long time = System.currentTimeMillis();

    long markedBytes = dataCache.markBuffersForProactiveEviction(predicate, isInstantDeallocation);
    markedBytes += fileMetadataCache.markBuffersForProactiveEviction(predicate, isInstantDeallocation);
    markedBytes += serdeCache.markBuffersForProactiveEviction(predicate, isInstantDeallocation);

    // Signal mark phase of proactive eviction was done
    if (markedBytes > 0) {
      memoryManager.notifyProactiveEvictionMark();
    }

    time = System.currentTimeMillis() - time;

    if (LOG.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder();
      sb.append(markedBytes).append(" bytes marked for eviction from LLAP cache buffers that belong to table(s): ");
      for (String table : request.getEntities().get(request.getSingleDbName()).keySet()) {
        sb.append(table).append(" ");
      }
      sb.append(" Duration: ").append(time).append(" ms");
      LOG.debug(sb.toString());
    }
    return markedBytes;
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
    if (encodeExecutor != null) {
      encodeExecutor.shutdownNow();
    }
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
        MemoryBuffer[] data, long baseOffset, CacheTag tag) {
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

  @Override
  public void debugDumpShort(StringBuilder sb) {
    for (LlapIoDebugDump child : debugDumpComponents) {
      child.debugDumpShort(sb);
    }
  }

  @Override
  public OrcTail getOrcTailFromCache(Path path, Configuration jobConf, CacheTag tag, Object fileKey)
      throws IOException {
    return OrcEncodedDataReader.getOrcTailForPath(path, jobConf, tag, daemonConf, (MetadataCache) fileMetadataCache, fileKey);
  }

  @Override
  public RecordReader<NullWritable, VectorizedRowBatch> llapVectorizedOrcReaderForPath(Object fileKey, Path path,
      CacheTag tag, List<Integer> tableIncludedCols, JobConf conf, long offset, long length, Reporter reporter)
      throws IOException {

    OrcTail tail = null;
    if (tag != null) {
      // Tag information is required for metadata lookup only - which itself can be done later should this info be yet
      // to be known
      tail = getOrcTailFromCache(path, conf, tag, fileKey);
    }
    OrcSplit split = new OrcSplit(path, fileKey, offset, length, (String[]) null, tail, false, false,
        Lists.newArrayList(), 0, length, path.getParent(), null);
    try {
      LlapRecordReader rr = LlapRecordReader.create(conf, split, tableIncludedCols, HiveStringUtils.getHostname(),
          orcCvp, executor, null, null, reporter, daemonConf);

      // May happen when attempting with unsupported schema evolution between reader and file schemas
      if (rr == null) {
        return null;
      }

      // This needs to be cleared as no partition values should be added to the result batches as constants.
      rr.setPartitionValues(null);

      // Triggers the IO thread pool to pick up this read job
      rr.start();
      return rr;
    } catch (HiveException e) {
      throw new IOException(e);
    }
  }

  @Override
  public MemoryBufferOrBuffers getParquetFooterBuffersFromCache(Path path, JobConf conf, @Nullable Object fileKey)
      throws IOException {

    Preconditions.checkNotNull(fileMetadataCache, "Metadata cache must not be null");

    boolean isReadCacheOnly = HiveConf.getBoolVar(conf, ConfVars.LLAP_IO_CACHE_ONLY);
    CacheTag tag = VectorizedParquetRecordReader.cacheTagOfParquetFile(path, daemonConf, conf);

    MemoryBufferOrBuffers footerData = (fileKey == null ) ? null
        : fileMetadataCache.getFileMetadata(fileKey);
    if (footerData != null) {
      LOG.info("Found the footer in cache for " + fileKey);
      try {
        return footerData;
      } finally {
        fileMetadataCache.decRefBuffer(footerData);
      }
    } else {
      throwIfCacheOnlyRead(isReadCacheOnly);
    }

    final FileSystem fs = path.getFileSystem(conf);
    final FileStatus stat = fs.getFileStatus(path);

    // To avoid reading the footer twice, we will cache it first and then read from cache.
    // Parquet calls protobuf methods directly on the stream and we can't get bytes after the fact.
    try (SeekableInputStream stream = HadoopStreams.wrap(fs.open(path))) {
      long footerLengthIndex = stat.getLen()
          - ParquetFooterInputFromCache.FOOTER_LENGTH_SIZE - ParquetFileWriter.MAGIC.length;
      stream.seek(footerLengthIndex);
      int footerLength = BytesUtils.readIntLittleEndian(stream);
      stream.seek(footerLengthIndex - footerLength);
      LOG.info("Caching the footer of length " + footerLength + " for " + fileKey);
      // Note: we don't pass in isStopped here - this is not on an IO thread.
      footerData = fileMetadataCache.putFileMetadata(fileKey, footerLength, stream, tag, null);
      try {
        return footerData;
      } finally {
        fileMetadataCache.decRefBuffer(footerData);
      }
    }
  }

  @Override
  public LlapDaemonProtocolProtos.CacheEntryList fetchCachedContentInfo() {
    if (useLowLevelCache) {
      GenericDataCache cache = new GenericDataCache(dataCache, bufferManager);
      LlapCacheMetadataSerializer serializer = new LlapCacheMetadataSerializer(fileMetadataCache, cache, daemonConf,
          pathCache, tracePool, realCachePolicy);
      return serializer.fetchCachedContentInfo();
    } else {
      LOG.warn("Low level cache is disabled.");
      return LlapDaemonProtocolProtos.CacheEntryList.getDefaultInstance();
    }
  }

  @Override
  public void loadDataIntoCache(LlapDaemonProtocolProtos.CacheEntryList metadata) {
    if (useLowLevelCache) {
      GenericDataCache cache = new GenericDataCache(dataCache, bufferManager);
      LlapCacheMetadataSerializer serializer = new LlapCacheMetadataSerializer(fileMetadataCache, cache, daemonConf,
          pathCache, tracePool, realCachePolicy);
      serializer.loadData(metadata);
    } else {
      LOG.warn("Cannot load data into the cache. Low level cache is disabled.");
    }
  }

  @Override
  public boolean usingLowLevelCache() {
    return useLowLevelCache;
  }

}
