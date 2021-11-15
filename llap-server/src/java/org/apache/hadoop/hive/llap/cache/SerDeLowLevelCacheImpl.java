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

package org.apache.hadoop.hive.llap.cache;

import com.google.common.base.Function;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Predicate;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.DataCache.BooleanRef;
import org.apache.hadoop.hive.common.io.DataCache.DiskRangeListFactory;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hadoop.hive.llap.metrics.LlapMetricsSystem;
import org.apache.hadoop.hive.llap.metrics.ReadWriteLockMetrics;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hive.common.util.Ref;
import org.apache.orc.OrcProto;
import org.apache.orc.OrcProto.ColumnEncoding;

public class SerDeLowLevelCacheImpl implements BufferUsageManager, LlapIoDebugDump, Configurable {
  private static final int DEFAULT_CLEANUP_INTERVAL = 600;
  private Configuration conf;
  private final Allocator allocator;
  private final AtomicInteger newEvictions = new AtomicInteger(0);
  private Thread cleanupThread = null;
  private final ConcurrentHashMap<Object, FileCache<FileData>> cache = new ConcurrentHashMap<>();
  private final LowLevelCachePolicy cachePolicy;
  private final long cleanupInterval;
  private final LlapDaemonCacheMetrics metrics;

  /// Shared singleton MetricsSource instance for all FileData locks
  private static final MetricsSource LOCK_METRICS;

  static {
    // create and register the MetricsSource for lock metrics
    MetricsSystem ms = LlapMetricsSystem.instance();
    ms.register("FileDataLockMetrics",
                "Lock metrics for R/W locks around FileData instances",
                LOCK_METRICS =
                    ReadWriteLockMetrics.createLockMetricsSource("FileData"));
  }

  public static final class LlapSerDeDataBuffer extends BaseLlapDataBuffer {
    public boolean isCached = false;

    @Override
    public void notifyEvicted(EvictionDispatcher evictionDispatcher, boolean isProactiveEviction) {
      evictionDispatcher.notifyEvicted(this, isProactiveEviction);
    }
  }

  private static final class StripeInfoComparator implements
      Comparator<StripeData> {
    @Override
    public int compare(StripeData o1, StripeData o2) {
      int starts = Long.compare(o1.knownTornStart, o2.knownTornStart);
      if (starts != 0) return starts;
      starts = Long.compare(o1.firstStart, o2.firstStart);
      if (starts != 0) return starts;
      assert (o1.lastStart == o2.lastStart) == (o1.lastEnd == o2.lastEnd);
      return Long.compare(o1.lastStart, o2.lastStart);
    }
  }

  public static class FileData {
    /**
     * RW lock ensures we have a consistent view of the file data, which is important given that
     * we generate "stripe" boundaries arbitrarily. Reading buffer data itself doesn't require
     * that this lock is held; however, everything else in stripes list does.
     * TODO: make more granular? We only care that each one reader sees consistent boundaries.
     *       So, we could shallow-copy the stripes list, then have individual locks inside each.
     */
    private final ReadWriteLock rwLock;
    private final Object fileKey;
    private final int colCount;
    private ArrayList<StripeData> stripes;

    public FileData(Configuration conf, Object fileKey, int colCount) {
      this.fileKey = fileKey;
      this.colCount = colCount;

      rwLock = ReadWriteLockMetrics.wrap(conf,
                                         new ReentrantReadWriteLock(),
                                         LOCK_METRICS);
    }

    public void toString(StringBuilder sb) {
      sb.append("File data for ").append(fileKey).append(" with ").append(colCount)
        .append(" columns: ").append(stripes);
    }

    public int getColCount() {
      return colCount;
    }

    public ArrayList<StripeData> getData() {
      return stripes;
    }

    public void addStripe(StripeData sd) {
      if (stripes == null) {
        stripes = new ArrayList<>();
      }
      stripes.add(sd);
    }

    @Override
    public String toString() {
      return "[fileKey=" + fileKey + ", colCount=" + colCount + ", stripes=" + stripes + "]";
    }
  }

  public static final class StripeData {
    // In LRR case, if we just store 2 boundaries (which could be split boundaries or reader
    // positions), we wouldn't be able to account for torn rows correctly because the semantics of
    // our "exact" reader positions, and inexact split boundaries, are different. We cannot even
    // tell LRR to use exact boundaries, as there can be a mismatch in an original mid-file split
    // wrt first row when caching - we may produce incorrect result if we adjust the split
    // boundary, and also if we don't adjust it, depending where it falls. At best, we'd end up
    // with spurious disk reads if we cache on row boundaries but splits include torn rows.
    // This structure implies that when reading a split, we skip the first torn row but fully
    // read the last torn row (as LineRecordReader does). If we want to support a different scheme,
    // we'd need to store more offsets and make logic account for that.
    private long knownTornStart; // This can change based on new splits.
    private final long firstStart, lastStart, lastEnd;
    // TODO: we can actually consider storing ALL the delta encoded row offsets - not a lot of
    //       overhead compared to the data itself, and with row offsets, we could use columnar
    //       blocks for inconsistent splits. We are not optimizing for inconsistent splits for now.

    private final long rowCount;
    private final OrcProto.ColumnEncoding[] encodings;
    private LlapSerDeDataBuffer[][][] data; // column index, stream type, buffers

    public StripeData(long knownTornStart, long firstStart, long lastStart, long lastEnd,
        long rowCount, ColumnEncoding[] encodings) {
      this.knownTornStart = knownTornStart;
      this.firstStart = firstStart;
      this.lastStart = lastStart;
      this.lastEnd = lastEnd;
      this.encodings = encodings;
      this.rowCount = rowCount;
      this.data = encodings == null ? null : new LlapSerDeDataBuffer[encodings.length][][];
    }
 
    @Override
    public String toString() {
      return toCoordinateString() + " with encodings [" + Arrays.toString(encodings)
          .replace('\n', ' ') + "] and data " + SerDeLowLevelCacheImpl.toString(data);
    }

    public long getKnownTornStart() {
      return knownTornStart;
    }

    public long getFirstStart() {
      return firstStart;
    }

    public long getLastStart() {
      return lastStart;
    }

    public long getLastEnd() {
      return lastEnd;
    }

    public long getRowCount() {
      return rowCount;
    }

    public OrcProto.ColumnEncoding[] getEncodings() {
      return encodings;
    }

    public LlapSerDeDataBuffer[][][] getData() {
      return data;
    }

    public String toCoordinateString() {
      return "stripe kts " + knownTornStart + " from "
          + firstStart + " to [" + lastStart + ", " + lastEnd + ")";
    }

    public static StripeData duplicateStructure(StripeData s) {
      return new StripeData(s.knownTornStart, s.firstStart, s.lastStart, s.lastEnd,
          s.rowCount, new OrcProto.ColumnEncoding[s.encodings.length]);
    }

    public void setKnownTornStart(long value) {
      knownTornStart = value;
    }
  }

  public static String toString(LlapSerDeDataBuffer[][][] data) {
    if (data == null) return "null";
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < data.length; ++i) {
      LlapSerDeDataBuffer[][] colData = data[i];
      if (colData == null) {
        sb.append("null, ");
        continue;
      }
      sb.append("colData [");
      for (int j = 0; j < colData.length; ++j) {
        LlapSerDeDataBuffer[] streamData = colData[j];
        if (streamData == null) {
          sb.append("null, ");
          continue;
        }
        sb.append("buffers [");
        for (int k = 0; k < streamData.length; ++k) {
          sb.append(streamData[k]);
        }
        sb.append("], ");
      }
      sb.append("], ");
    }
    sb.append("]");
    return sb.toString();
  }
  

  public static String toString(LlapSerDeDataBuffer[][] data) {
    if (data == null) return "null";
    StringBuilder sb = new StringBuilder("[");
    for (int j = 0; j < data.length; ++j) {
      LlapSerDeDataBuffer[] streamData = data[j];
      if (streamData == null) {
        sb.append("null, ");
        continue;
      }
      sb.append("[");
      for (int k = 0; k < streamData.length; ++k) {
        sb.append(streamData[k]);
      }
      sb.append("], ");
    }
    sb.append("]");
    return sb.toString();
  }

  public SerDeLowLevelCacheImpl(
      LlapDaemonCacheMetrics metrics, LowLevelCachePolicy cachePolicy, Allocator allocator) {
    this.cachePolicy = cachePolicy;
    this.allocator = allocator;
    this.cleanupInterval = DEFAULT_CLEANUP_INTERVAL;
    this.metrics = metrics;
    LlapIoImpl.LOG.info("SerDe low-level level cache; cleanup interval {} sec", cleanupInterval);
  }

  public void startThreads() {
    if (cleanupInterval < 0) return;
    cleanupThread = new CleanupThread(cache, newEvictions, cleanupInterval);
    cleanupThread.start();
  }

  public FileData getFileData(Object fileKey, long start, long end, boolean[] includes,
      DiskRangeListFactory factory, LowLevelCacheCounters qfCounters, BooleanRef gotAllData)
          throws IOException {
    FileCache<FileData> subCache = cache.get(fileKey);
    if (subCache == null || !subCache.incRef()) {
      if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
        LlapIoImpl.CACHE_LOGGER.trace("Cannot find cache for " + fileKey + " in " + cache);
      }
      markAllAsMissed(start, end, qfCounters, gotAllData);
      return null;
    }

    try {
      FileData cached = subCache.getCache();
      cached.rwLock.readLock().lock();
      if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
        LlapIoImpl.CACHE_LOGGER.trace("Cache for " + fileKey + " is " + subCache.getCache());
      }
      try {
        if (cached.stripes == null) {
          LlapIoImpl.CACHE_LOGGER.debug("Cannot find any stripes for " + fileKey);
          markAllAsMissed(start, end, qfCounters, gotAllData);
          return null;
        }
        if (includes.length > cached.colCount) {
          throw new IOException("Includes " + DebugUtils.toString(includes) + " for "
              + cached.colCount + " columns");
        }
        FileData result = new FileData(conf, cached.fileKey, cached.colCount);
        if (gotAllData != null) {
          gotAllData.value = true;
        }
        // We will adjust start and end so that we could record the metrics; save the originals.
        long origStart = start, origEnd = end;
        // startIx is inclusive, endIx is exclusive.
        int startIx = Integer.MIN_VALUE, endIx = Integer.MIN_VALUE;
        LlapIoImpl.CACHE_LOGGER.debug("Looking for data between " + start + " and " + end);
        for (int i = 0; i < cached.stripes.size() && endIx == Integer.MIN_VALUE; ++i) {
          StripeData si = cached.stripes.get(i);
          if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
            LlapIoImpl.CACHE_LOGGER.trace("Looking at " + si.toCoordinateString());
          }

          if (startIx == i) {
            // The start of the split was in the middle of the previous slice.
            start = si.knownTornStart;
          } else if (startIx == Integer.MIN_VALUE) {
            // Determine if we need to read this slice for the split.
            if (si.lastEnd <= start) continue; // Slice before the start of the split.
            // Start of the split falls somewhere within or before this slice.
            // Note the ">=" - LineRecordReader will skip the first row even if we start
            // directly at its start, because it cannot know if it's the start or not.
            // Unless it's 0; note that we DO give 0 special treatment here, unlike the EOF below,
            // because zero is zero. Need to mention it in Javadoc.
            if (start == 0 && si.firstStart == 0) {
              startIx = i;
            } else if (start >= si.firstStart) {
              // If the start of the split points into the middle of the cached slice, we cannot
              // use the cached block - it's encoded and columnar, so we cannot map the file
              // offset to some "offset" in "middle" of the slice (but see TODO for firstStart).
              startIx = i + 1;
              // continue;
            } else {
              // Start of the split is before this slice.
              startIx = i; // Simple case - we will read cache from the split start offset.
              start = si.knownTornStart;
            }
          }

          // Determine if this (or previous) is the last slice we need to read for this split.
          if (startIx != Integer.MIN_VALUE && endIx == Integer.MIN_VALUE) {
            if (si.lastEnd <= end) {
              // The entire current slice is part of the split. Note that if split end EQUALS
              // lastEnd, the split would also read the next row, so we do need to look at the
              // next slice, if any (although we'd probably find we cannot use it).
              // Note also that we DO NOT treat end-of-file differently here, cause we do not know
              // of any such thing. The caller must handle lastEnd vs end of split vs end of file
              // match correctly in terms of how LRR handles them. See above for start-of-file.
              if (i + 1 != cached.stripes.size()) continue;
              endIx = i + 1;
              end = si.lastEnd;
            } else if (si.lastStart <= end) {
              // The split ends within (and would read) the last row of this slice. Exact match.
              endIx = i + 1;
              end = si.lastEnd;
            } else {
              // Either the slice comes entirely after the end of split (following a gap in cached
              // data); or the split ends in the middle of the slice, so it's the same as in the
              // startIx logic w.r.t. the partial match; so, we either don't want to, or cannot,
              // use this. There's no need to distinguish these two cases for now.
              endIx = i;
              end = (endIx > 0) ? cached.stripes.get(endIx - 1).lastEnd : start;
            }
          }
        }
        LlapIoImpl.CACHE_LOGGER.debug("Determined stripe indexes " + startIx + ", " + endIx);
        if (endIx <= startIx) {
          if (gotAllData != null) {
            gotAllData.value = false;
          }
          return null;  // No data for the split, or it fits in the middle of one or two slices.
        }
        if (start > origStart || end < origEnd) {
          if (gotAllData != null) {
            gotAllData.value = false;
          }
          long totalMiss = Math.max(0, origEnd - end) + Math.max(0, start - origStart);
          metrics.incrCacheRequestedBytes(totalMiss);
          if (qfCounters != null) {
            qfCounters.recordCacheMiss(totalMiss);
          }
        }

        result.stripes = new ArrayList<>(endIx - startIx);
        for (int stripeIx = startIx; stripeIx < endIx; ++stripeIx) {
          getCacheDataForOneSlice(stripeIx, cached, result, gotAllData, includes, qfCounters);
        }
        return result;
      } finally {
        cached.rwLock.readLock().unlock();
      }
    } finally {
      subCache.decRef();
    }
  }


  private void getCacheDataForOneSlice(int stripeIx, FileData cached, FileData result,
      BooleanRef gotAllData, boolean[] includes, LowLevelCacheCounters qfCounters) {
    StripeData cStripe = cached.stripes.get(stripeIx);
    if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
      LlapIoImpl.CACHE_LOGGER.trace("Got stripe in cache " + cStripe);
    }
    StripeData stripe = StripeData.duplicateStructure(cStripe);
    result.stripes.add(stripe);
    boolean isMissed = false;
    for (int colIx = 0; colIx < cached.colCount; ++colIx) {
      if (!includes[colIx]) continue;
      if (cStripe.encodings[colIx] == null || cStripe.data[colIx] == null) {
        if (cStripe.data[colIx] != null) {
          throw new AssertionError(cStripe);
          // No encoding => must have no data.
        }
        isMissed = true;
        if (gotAllData != null) {
          gotAllData.value = false;
        }
        continue;
      }
      stripe.encodings[colIx] = cStripe.encodings[colIx];
      LlapSerDeDataBuffer[][] cColData = cStripe.data[colIx];
      assert cColData != null;
      for (int streamIx = 0;
          cColData != null && streamIx < cColData.length; ++streamIx) {
        LlapSerDeDataBuffer[] streamData = cColData[streamIx];
        // Note: this relies on the fact that we always evict the entire column, so if
        //       we have the column data, we assume we have all the streams we need.
        if (streamData == null) continue;
        for (int i = 0; i < streamData.length; ++i) { // Finally, we are going to use "i"!
          if (!lockBuffer(streamData[i], true)) {
            LlapIoImpl.CACHE_LOGGER.info("Couldn't lock data for stripe at "
                + stripeIx + ", colIx " + colIx + ", stream type " + streamIx);

            handleRemovedColumnData(cColData);
            cColData = null;
            isMissed = true;
            if (gotAllData != null) {
              gotAllData.value = false;
            }
            break;
          }
        }
      }
      // At this point, we have arrived at the level where we need all the data, and the
      // arrays never change. So we will just do a shallow assignment here instead of copy.
      stripe.data[colIx] = cColData;
      if (cColData == null) {
        stripe.encodings[colIx] = null;
      }
    }
    doMetricsStuffForOneSlice(qfCounters, stripe, isMissed);
  }


  private void doMetricsStuffForOneSlice(
      LowLevelCacheCounters qfCounters, StripeData stripe, boolean isMissed) {
    // Slice boundaries may not match split boundaries due to torn rows in either direction,
    // so this counter may not be consistent with splits. This is also why we increment
    // requested bytes here, instead of based on the split - we don't want the metrics to be
    // inconsistent with each other. No matter what we determine here, at least we'll account
    // for both in the same manner.
    long bytes = stripe.lastEnd - stripe.knownTornStart;
    metrics.incrCacheRequestedBytes(bytes);
    if (!isMissed) {
      metrics.incrCacheHitBytes(bytes);
    }
    if (qfCounters != null) {
      if (isMissed) {
        qfCounters.recordCacheMiss(bytes);
      } else {
        qfCounters.recordCacheHit(bytes);
      }
    }
  }

  private void markAllAsMissed(long from, long to,
      LowLevelCacheCounters qfCounters, BooleanRef gotAllData) {
    if (qfCounters != null) {
      metrics.incrCacheRequestedBytes(to - from);
      qfCounters.recordCacheMiss(to - from);
    }
    if (gotAllData != null) {
      gotAllData.value = false;
    }
  }

  private boolean lockBuffer(LlapSerDeDataBuffer buffer, boolean doNotifyPolicy) {
    int rc = buffer.incRef();
    if (rc > 0) {
      metrics.incrCacheNumLockedBuffers();
    }
    if (doNotifyPolicy && rc == 1) {
      // We have just locked a buffer that wasn't previously locked.
      cachePolicy.notifyLock(buffer);
    }
    return rc > 0;
  }

  public long markBuffersForProactiveEviction(Predicate<CacheTag> predicate, boolean isInstantDeallocation) {
    long markedBytes = 0;
    // Proactive eviction does not need to be perfectly accurate - the iterator returned here might be missing some
    // concurrent inserts / removals but it's fine for us here.
    Collection<FileCache<FileData>> fileCaches = cache.values();
    for (FileCache<FileData> fileCache : fileCaches) {
      if (predicate.test(fileCache.getTag()) && fileCache.incRef()) {
        // Locked on subcache so that the async cleaner won't pull out buffers in an unlikely scenario.
        try {
          ArrayList<StripeData> stripeDataList = fileCache.getCache().getData();
          for (StripeData stripeData : stripeDataList) {
            for (int i = 0; i < stripeData.getData().length; ++i) {
              LlapSerDeDataBuffer[][] colData = stripeData.getData()[i];
              if (colData == null) continue;
              for (int j = 0; j < colData.length; ++j) {
                LlapSerDeDataBuffer[] streamData = colData[j];
                if (streamData == null) continue;
                for (int k = 0; k < streamData.length; ++k) {
                  markedBytes += streamData[k].markForEviction();
                  if (isInstantDeallocation) {
                    allocator.deallocate(streamData[k]);
                  }
                }
              }
            }
          }
        } finally {
          fileCache.decRef();
        }
      }
    }
    return markedBytes;
  }

  public void putFileData(final FileData data, Priority priority,
      LowLevelCacheCounters qfCounters, CacheTag tag) {
    // TODO: buffers are accounted for at allocation time, but ideally we should report the memory
    //       overhead from the java objects to memory manager and remove it when discarding file.
    if (data.stripes == null || data.stripes.isEmpty()) {
      LlapIoImpl.LOG.warn("Trying to cache FileData with no data for " + data.fileKey);
      return;
    }
    FileCache<FileData> subCache = null;
    FileData cached = null;
    data.rwLock.writeLock().lock();
    try {
      subCache = FileCache.getOrAddFileSubCache(
          cache, data.fileKey, new Function<Void, FileData>() {
        @Override
        public FileData apply(Void input) {
          return data; // If we don't have a file cache, we will add this one as is.
        }
      }, tag);
      cached = subCache.getCache();
    } finally {
      if (data != cached) {
        data.rwLock.writeLock().unlock();
      }
    }
    try {
      if (data != cached) {
        cached.rwLock.writeLock().lock();
      }
      try {
        for (StripeData si : data.stripes) {
          lockAllBuffersForPut(si, priority, subCache);
        }
        if (data == cached) {
          if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
            LlapIoImpl.CACHE_LOGGER.trace("Cached new data " + data);
          }
          return;
        }
        if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
          LlapIoImpl.CACHE_LOGGER.trace("Merging old " + cached + " and new " + data);
        }
        ArrayList<StripeData> combined = new ArrayList<>(
            cached.stripes.size() + data.stripes.size());
        combined.addAll(cached.stripes);
        combined.addAll(data.stripes);
        Collections.sort(combined, new StripeInfoComparator());
        int lastIx = combined.size() - 1;
        for (int ix = 0; ix < lastIx; ++ix) {
          StripeData cur = combined.get(ix), next = combined.get(ix + 1);
          if (cur.lastEnd <= next.firstStart) continue; // All good.
          if (cur.firstStart == next.firstStart && cur.lastEnd == next.lastEnd) {
            mergeStripeInfos(cur, next);
            combined.remove(ix + 1);
            --lastIx;
            // Don't recheck with next, only 2 lists each w/o collisions.
            continue;
          }
          // The original lists do not contain collisions, so only one is 'old'.
          boolean isCurOriginal = cached.stripes.contains(cur);
          handleRemovedStripeInfo(combined.remove(isCurOriginal ? ix : ix + 1));
          --ix;
          --lastIx;
        }
        cached.stripes = combined;
        if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
          LlapIoImpl.CACHE_LOGGER.trace("New cache data is " + combined);
        }
      } finally {
        cached.rwLock.writeLock().unlock();
      }
    } finally {
      subCache.decRef();
    }
  }

  private void lockAllBuffersForPut(StripeData si, Priority priority, FileCache cache) {
    for (int i = 0; i < si.data.length; ++i) {
      LlapSerDeDataBuffer[][] colData = si.data[i];
      if (colData == null) continue;
      for (int j = 0; j < colData.length; ++j) {
        LlapSerDeDataBuffer[] streamData = colData[j];
        if (streamData == null) continue;
        for (int k = 0; k < streamData.length; ++k) {
          boolean canLock = lockBuffer(streamData[k], false); // false - not in cache yet
          assert canLock;
          streamData[k].setFileCache(cache);
          cachePolicy.cache(streamData[k], priority);
          streamData[k].isCached = true;
        }
      }
    }
  }

  private void handleRemovedStripeInfo(StripeData removed) {
    for (LlapSerDeDataBuffer[][] colData : removed.data) {
      handleRemovedColumnData(colData);
    }
  }

  private void handleRemovedColumnData(LlapSerDeDataBuffer[][] removed) {
    // TODO: could we tell the policy that we don't care about these and have them evicted? or we
    //       could just deallocate them when unlocked, and free memory + handle that in eviction.
    //       For now, just abandon the blocks - eventually, they'll get evicted.
  }

  private void mergeStripeInfos(StripeData to, StripeData from) {
    if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
      LlapIoImpl.CACHE_LOGGER.trace("Merging slices data: old " + to + " and new " + from);
    }
    to.knownTornStart = Math.min(to.knownTornStart, from.knownTornStart);
    if (from.encodings.length != to.encodings.length) {
      throw new RuntimeException("Different encodings " + from + "; " + to);
    }
    for (int colIx = 0; colIx < from.encodings.length; ++colIx) {
      if (to.encodings[colIx] == null) {
        to.encodings[colIx] = from.encodings[colIx];
      } else if (from.encodings[colIx] != null
          && !to.encodings[colIx].equals(from.encodings[colIx])) {
        throw new RuntimeException("Different encodings at " + colIx + ": " + from + "; " + to);
      }
      LlapSerDeDataBuffer[][] fromColData = from.data[colIx];
      if (fromColData != null) {
        if (to.data[colIx] != null) {
          // Note: we assume here that the data that was returned to the caller from cache will not
          // be passed back in via put. Right now it's safe since we don't do anything. But if we
          // evict proactively, we will have to compare objects all the way down.
          handleRemovedColumnData(to.data[colIx]);
        }
        to.data[colIx] = fromColData;
      }
    } 
  }

  @Override
  public void decRefBuffer(MemoryBuffer buffer) {
    unlockBuffer((LlapSerDeDataBuffer)buffer, true);
  }

  @Override
  public void decRefBuffers(List<MemoryBuffer> cacheBuffers) {
    for (MemoryBuffer b : cacheBuffers) {
      unlockBuffer((LlapSerDeDataBuffer)b, true);
    }
  }

  private void unlockBuffer(LlapSerDeDataBuffer buffer, boolean handleLastDecRef) {
    boolean isLastDecref = (buffer.decRef() == 0);
    if (handleLastDecRef && isLastDecref) {
      if (buffer.isCached) {
        cachePolicy.notifyUnlock(buffer);
      } else {
        if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
          LlapIoImpl.CACHE_LOGGER.trace("Deallocating {} that was not cached", buffer);
        }
        allocator.deallocate(buffer);
      }
    }
    metrics.decrCacheNumLockedBuffers();
  }

  public final void notifyEvicted(MemoryBuffer buffer) {
    newEvictions.incrementAndGet();

    // FileCacheCleanupThread might we waiting for eviction increment
    synchronized(newEvictions) {
      newEvictions.notifyAll();
    }
  }

  private final class CleanupThread extends FileCacheCleanupThread<FileData> {

    public CleanupThread(ConcurrentHashMap<Object, FileCache<FileData>> fileMap,
        AtomicInteger newEvictions, long cleanupInterval) {
      super("Llap serde low level cache cleanup thread", fileMap, newEvictions, cleanupInterval);
    }

    @Override
    protected int getCacheSize(FileCache<FileData> fc) {
      return 1; // Each iteration cleans the file cache as a single unit (unlike the ORC cache).
    }

    @Override
    public int cleanUpOneFileCache(FileCache<FileData> fc, int leftToCheck, long endTime,
        Ref<Boolean> isPastEndTime) throws InterruptedException {
      FileData fd = fc.getCache();
      fd.rwLock.writeLock().lock();
      try {
        for (StripeData sd : fd.stripes) {
          for (int colIx = 0; colIx < sd.data.length; ++colIx) {
            LlapSerDeDataBuffer[][] colData = sd.data[colIx];
            if (colData == null) continue;
            boolean hasAllData = true;
            for (int j = 0; (j < colData.length) && hasAllData; ++j) {
              LlapSerDeDataBuffer[] streamData = colData[j];
              if (streamData == null) continue;
              for (int k = 0; k < streamData.length; ++k) {
                LlapSerDeDataBuffer buf = streamData[k];
                hasAllData = hasAllData && lockBuffer(buf, false);
                if (!hasAllData) break;
                unlockBuffer(buf, true);
              }
            }
            if (!hasAllData) {
              handleRemovedColumnData(colData);
              sd.data[colIx] = null;
            }
          }
        }
      } finally {
        fd.rwLock.writeLock().unlock();
      }
      return leftToCheck - 1;
    }
  }

  @Override
  public boolean incRefBuffer(MemoryBuffer buffer) {
    // notifyReused implies that buffer is already locked; it's also called once for new
    // buffers that are not cached yet. Don't notify cache policy.
    return lockBuffer(((LlapSerDeDataBuffer)buffer), false);
  }

  @Override
  public Allocator getAllocator() {
    return allocator;
  }

  @Override
  public void setConf(Configuration newConf) {
    this.conf = newConf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public void debugDumpShort(StringBuilder sb) {
    sb.append("\nSerDe cache state ");
    int allLocked = 0, allUnlocked = 0, allEvicted = 0, allMoving = 0, allMarked = 0;
    for (Map.Entry<Object, FileCache<FileData>> e : cache.entrySet()) {
      if (!e.getValue().incRef()) continue;
      try {
        FileData fd = e.getValue().getCache();
        int fileLocked = 0, fileUnlocked = 0, fileEvicted = 0, fileMoving = 0, fileMarked = 0;
        for (StripeData stripe : fd.stripes) {
          if (stripe.data == null) continue;
          for (int i = 0; i < stripe.data.length; ++i) {
            LlapSerDeDataBuffer[][] colData = stripe.data[i];
            if (colData == null) continue;
            for (int j = 0; j < colData.length; ++j) {
              LlapSerDeDataBuffer[] streamData = colData[j];
              if (streamData == null) continue;
              for (int k = 0; k < streamData.length; ++k) {
                int newRc = streamData[k].incRef();
                if (newRc < 0) {
                  if (newRc == LlapAllocatorBuffer.INCREF_EVICTED) {
                    ++fileEvicted;
                  } else if (newRc == LlapAllocatorBuffer.INCREF_FAILED) {
                    ++fileMoving;
                  }
                  continue;
                }
                if (streamData[k].isMarkedForEviction()) {
                  ++fileMarked;
                }
                try {
                  if (newRc > 1) { // We hold one refcount.
                    ++fileLocked;
                  } else {
                    ++fileUnlocked;
                  }
                } finally {
                  streamData[k].decRef();
                }
              }
            }
          }
        }
        allLocked += fileLocked;
        allUnlocked += fileUnlocked;
        allEvicted += fileEvicted;
        allMoving += fileMoving;
        allMarked += fileMarked;
        sb.append("\n  file " + e.getKey() + ": " + fileLocked + " locked, " + fileUnlocked
            + " unlocked, " + fileEvicted + " evicted, " + fileMoving + " being moved, " + fileMarked
            + " marked for eviction; ");
        sb.append(fd.colCount).append(" columns, ").append(fd.stripes.size()).append(" stripes");
      } finally {
        e.getValue().decRef();
      }
    }
    sb.append("\nSerDe cache summary: " + allLocked + " locked, " + allUnlocked + " unlocked, "
        + allEvicted + " evicted, " + allMoving + " being moved, " + allMarked + " marked for eviction");
  }
}
