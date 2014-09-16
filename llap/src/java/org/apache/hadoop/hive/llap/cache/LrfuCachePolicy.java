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

package org.apache.hadoop.hive.llap.cache;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.api.Llap;
import org.apache.hadoop.hive.llap.cache.BufferPool.WeakBuffer;

import com.google.common.annotations.VisibleForTesting;

/**
 * Implementation of the "simple" algorithm from "On the Existence of a Spectrum of Policies
 * that Subsumes the Least Recently Used (LRU) and Least Frequently Used (LFU) Policies".
 * We expect the number of buffers to be relatively small (100s-1000s), so we just use one heap.
 * Additionally, the cache (as of now) is fixed size, so we know exactly how big the heap is
 * going to be. Because of that and of the difficulty of iterating the heap in order, we
 * evict the blocks as the heap overflows, and keep them in overflow list to return to ChunkPool
 * when it officially decides to do an eviction. */
public class LrfuCachePolicy implements CachePolicy {
  private final double lambda;
  private final double f(long x) {
    return Math.pow(0.5, lambda * x);
  }
  private static final double F0 = 1; // f(0) is always 1
  private final double touchPriority(long time, long lastAccess, double previous) {
    return F0 + f(time - lastAccess) * previous;
  }
  private final double expirePriority(long time, long lastAccess, double previous) {
    return f(time - lastAccess) * previous;
  }

  private final AtomicLong timer = new AtomicLong(0);
  /**
   * The heap. Currently synchronized on itself; there is a number of papers out there
   * with various lock-free/efficient priority queues which we can use if needed.
   */
  private final WeakBuffer[] heap;
  /** Number of elements. */
  private int heapSize = 0;

  public LrfuCachePolicy(Configuration conf, long bufferSize, long totalSize) {
    heap = new WeakBuffer[(int)Math.ceil((totalSize * 1.0) / bufferSize)];
    lambda = HiveConf.getFloatVar(conf, HiveConf.ConfVars.LLAP_LRFU_LAMBDA);
  }

  @Override
  public WeakBuffer cache(WeakBuffer buffer) {
    WeakBuffer evicted = null;
    buffer.lastUpdate = timer.incrementAndGet();
    buffer.priority = F0;
    assert buffer.isLocked();
    buffer.isLockedInHeap = true;
    synchronized (heap) {
      if (heapSize == heap.length) {
        evicted = evictFromHeapUnderLock(buffer.lastUpdate);
        // If we failed to lock, caller should retry.
        if (evicted == null) return CANNOT_EVICT;
      }
      assert heapSize < heap.length : heap.length + " >= " + heapSize;
      buffer.indexInHeap = heapSize;
      heapifyUpUnderLock(buffer, buffer.lastUpdate);
      if (DebugUtils.isTraceEnabled()) {
        Llap.LOG.info(buffer + " inserted at " + buffer.lastUpdate);
      }
      ++heapSize;
    }
    return evicted;
  }

  @Override
  public void notifyLock(WeakBuffer buffer) {
    long time = timer.get();
    synchronized (heap) {
      buffer.isLockedInHeap = true;
      heapifyDownUnderLock(buffer, time);
    }
  }

  @Override
  public void notifyUnlock(WeakBuffer buffer) {
    long time = timer.incrementAndGet();
    synchronized (heap) {
      if (DebugUtils.isTraceCachingEnabled()) {
        Llap.LOG.info("Touching " + buffer + " at " + time);
      }
      buffer.priority = touchPriority(time, buffer.lastUpdate, buffer.priority);
      buffer.lastUpdate = time;
      buffer.isLockedInHeap = false;
      // Buffer's priority just decreased from boosted lock priority, so move up.
      heapifyUpUnderLock(buffer, time);
    }
  }

  private WeakBuffer evictFromHeapUnderLock(long time) {
    if (heapSize == 0) return null;
    WeakBuffer result = heap[0];
    if (!result.invalidate()) {
      // We boost the priority of locked buffers to a very large value;
      // this means entire heap is locked. TODO: need to work around that for small pools?
      if (DebugUtils.isTraceCachingEnabled()) {
        Llap.LOG.info("Failed to invalidate head " + result.toString() + "; size = " + heapSize);
      }
      return null;
    }
    if (DebugUtils.isTraceCachingEnabled()) {
      Llap.LOG.info("Evicting " + result + " at " + time);
    }
    result.indexInHeap = -1;
    --heapSize;
    WeakBuffer newRoot = heap[heapSize];
    newRoot.indexInHeap = 0;
    if (newRoot.lastUpdate != time && !newRoot.isLockedInHeap)  {
      newRoot.priority = expirePriority(time, newRoot.lastUpdate, newRoot.priority);
      newRoot.lastUpdate = time;
    }
    heapifyDownUnderLock(newRoot, time);
    return result;
  }

  private void heapifyDownUnderLock(WeakBuffer buffer, long time) {
    // Relative positions of the blocks don't change over time; priorities we expire can only
    // decrease; we only have one block that could have broken heap rule and we always move it
    // down; therefore, we can update priorities of other blocks as we go for part of the heap -
    // we correct any discrepancy w/the parent after expiring priority, and any block we expire
    // the priority for already has lower priority than that of its children.
    // TODO: avoid expiring priorities if times are close? might be needlessly expensive.
    int ix = buffer.indexInHeap;
    double priority = buffer.isLockedInHeap ? Double.MAX_VALUE : buffer.priority;
    while (true) {
      int leftIx = (ix << 1) + 1, rightIx = leftIx + 1;
      if (leftIx >= heapSize) break; // Buffer is at the leaf node.
      WeakBuffer left = heap[leftIx], right = null;
      if (rightIx < heapSize) {
        right = heap[rightIx];
      }
      double leftPri = getHeapifyPriority(left, time), rightPri = getHeapifyPriority(right, time);
      if (priority <= leftPri && priority <= rightPri) break;
      if (leftPri <= rightPri) { // prefer left, cause right might be missing
        heap[ix] = left;
        left.indexInHeap = ix;
        ix = leftIx;
      } else {
        heap[ix] = right;
        right.indexInHeap = ix;
        ix = rightIx;
      }
    }
    buffer.indexInHeap = ix;
    heap[ix] = buffer;
  }

  private void heapifyUpUnderLock(WeakBuffer buffer, long time) {
    // See heapifyDown comment.
    int ix = buffer.indexInHeap;
    double priority = buffer.isLockedInHeap ? Double.MAX_VALUE : buffer.priority;
    while (true) {
      if (ix == 0) break; // Buffer is at the top of the heap.
      int parentIx = (ix - 1) >>> 1;
      WeakBuffer parent = heap[parentIx];
      double parentPri = getHeapifyPriority(parent, time);
      if (priority >= parentPri) break;
      heap[ix] = parent;
      parent.indexInHeap = ix;
      ix = parentIx;
    }
    buffer.indexInHeap = ix;
    heap[ix] = buffer;
  }

  private double getHeapifyPriority(WeakBuffer buf, long time) {
    if (buf == null || buf.isLockedInHeap) return Double.MAX_VALUE;
    if (buf.lastUpdate != time) {
      buf.priority = expirePriority(time, buf.lastUpdate, buf.priority);
      buf.lastUpdate = time;
    }
    return buf.priority;
  }

  public String debugDumpHeap() {
    if (heapSize == 0) return "<empty>";
    int levels = 32 - Integer.numberOfLeadingZeros(heapSize);
    StringBuilder result = new StringBuilder();
    int ix = 0;
    int spacesCount = heap[0].toStringForCache().length() + 3;
    String full = StringUtils.repeat(" ", spacesCount),
        half = StringUtils.repeat(" ", spacesCount / 2);
    int maxWidth = 1 << (levels - 1);
    for (int i = 0; i < levels; ++i) {
      int width = 1 << i;
      int middleGap = (maxWidth - width) / width;
      for (int j = 0; j < (middleGap >>> 1); ++j) {
        result.append(full);
      }
      if ((middleGap & 1) == 1) {
        result.append(half);
      }
      for (int j = 0; j < width && ix < heapSize; ++j, ++ix) {
        if (j != 0) {
          for (int k = 0; k < middleGap; ++k) {
            result.append(full);
          }
          if (middleGap == 0) {
            result.append(" ");
          }
        }
        if ((j & 1) == 0) {
          result.append("(");
        }
        result.append(heap[ix].toStringForCache());
        if ((j & 1) == 1) {
          result.append(")");
        }
      }
      result.append("\n");
    }
    return result.toString();
  }

  @VisibleForTesting
  public WeakBuffer evictOneMoreBlock() {
    synchronized (heap) {
      return evictFromHeapUnderLock(timer.get());
    }
  }
}
