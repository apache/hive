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

import static java.util.Comparator.nullsLast;
import static java.util.Comparator.comparing;

import java.util.Arrays;
import java.util.List;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.llap.cache.LowLevelCache.Priority;
import org.apache.hadoop.hive.llap.daemon.impl.LlapPooledIOThread;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache.LlapMetadataBuffer;
import org.apache.hadoop.hive.llap.metrics.LlapMetricsSystem;
import org.apache.hadoop.hive.llap.metrics.MetricsUtils;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.impl.MsInfo;

import com.google.common.annotations.VisibleForTesting;

/**
 * Implementation of the algorithm from "On the Existence of a Spectrum of Policies
 * that Subsumes the Least Recently Used (LRU) and Least Frequently Used (LFU) Policies".
 * Additionally, buffer locking has to be handled (locked buffer cannot be evicted).
 */
public final class LowLevelLrfuCachePolicy extends ProactiveEvictingCachePolicy.Impl implements LowLevelCachePolicy {
  private final double lambda;
  private final double cutoff;
  private double f(long x) {
    return Math.pow(0.5, lambda * x);
  }
  private static final double F0 = 1; // f(0) is always 1
  private double touchPriority(long time, long lastAccess, double previous) {
    return F0 + f(time - lastAccess) * previous;
  }
  private double expirePriority(long time, long lastAccess, double previous) {
    return f(time - lastAccess) * previous;
  }

  private final AtomicLong timer = new AtomicLong(0);
  /**
   * The heap and list. Currently synchronized on the object, which is not good. If this becomes
   * a problem (which it probably will), we can partition the cache policy, or use some better
   * structure. Heap should not be locked while holding the lock on list.
   * As of now, eviction in most cases will only need the list; locking doesn't do anything;
   * unlocking actually places item in evictable cache - unlocking is done after processing,
   * so this most expensive part (and only access to heap in most cases) will not affect it.
   * Perhaps we should use ConcurrentDoubleLinkedList (in public domain).
   * ONLY LIST REMOVAL is allowed under list lock.
   */
  private LlapCacheableBuffer[] heap;
  private final ReentrantLock heapLock = new ReentrantLock();
  private final ReentrantLock listLock = new ReentrantLock();
  private LlapCacheableBuffer listHead, listTail;
  /** Number of elements. */
  private int heapSize = 0;
  private final int maxHeapSize;
  private EvictionListener evictionListener;
  @VisibleForTesting
  final PolicyMetrics metrics;
  // BP wrapper
  private final ThreadLocal<BPWrapper> threadLocalBPWrapper;
  private final Map<Long, BPWrapper> bpWrappers = new ConcurrentHashMap<>();
  private final int maxQueueSize;

  public LowLevelLrfuCachePolicy(int minBufferSize, long maxSize, Configuration conf) {
    super(conf);
    this.maxQueueSize = HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_LRFU_BP_WRAPPER_SIZE);
    this.lambda = HiveConf.getFloatVar(conf, HiveConf.ConfVars.LLAP_LRFU_LAMBDA);
    this.cutoff = HiveConf.getFloatVar(conf, HiveConf.ConfVars.LLAP_LRFU_HOTBUFFERS_PERCENTAGE);

    int maxBuffers = (int)Math.ceil((maxSize * 1.0) / minBufferSize);
    if (lambda == 0) {
      maxHeapSize = maxBuffers; // lrfuThreshold is +inf in this case
    } else {
      int lrfuThreshold = (int)((Math.log(1 - Math.pow(0.5, lambda)) / Math.log(0.5)) / lambda);
      maxHeapSize = Math.min(lrfuThreshold, maxBuffers);
    }
    LlapIoImpl.LOG.info("LRFU cache policy with min buffer size {} and lambda {} (heap size {})",
        minBufferSize, lambda, maxHeapSize);

    heap = new LlapCacheableBuffer[maxHeapSize];
    listHead = null;
    listTail = null;

    String sessID = conf.get("llap.daemon.metrics.sessionid");
    if (null == sessID) {
      sessID = "<unknown>";
    }

    // register new metrics provider for this cache policy
    metrics = new PolicyMetrics(sessID);
    LlapMetricsSystem.instance().register("LowLevelLrfuCachePolicy-" + MetricsUtils.getHostName(), null, metrics);

    // Thread local buffer arrays are also registered in a concurrent map for more control over them. Note that this
    // concurrent hash map will only have to lock just once for every thread during startup, and later only during
    // supportability feature use cases e.g. purge, statistics gathering, it does not affect the hot code paths.
    threadLocalBPWrapper = ThreadLocal.withInitial(() -> {
      BPWrapper bpWrapper = new BPWrapper();
      bpWrappers.put(Thread.currentThread().getId(), bpWrapper);
      return bpWrapper;
    });

  }

  /**
   * Wraps around the structures used in thread locals and buffers instances of LlapCachableBuffers in order to prevent
   * lock contention in LRFU during buffer ingress initiated heap access.
   */
  private class BPWrapper {
    private final LlapCacheableBuffer[] buffers = new LlapCacheableBuffer[maxQueueSize];
    private int count = 0;
    // IO-Thread's are granted this lock for almost 100% of the time on the hot code paths, the only occasions these
    // threads are blocked by this is when supportability features are used.
    private ReentrantLock lock = new ReentrantLock();

    private void tryFlush() {
      if (heapLock.tryLock()) {
        try {
          doNotifyUnderHeapLock(count, buffers);
        } finally {
          count = 0;
          heapLock.unlock();
        }
      }
    }

    private void flush() {
      heapLock.lock();
      try {
        doNotifyUnderHeapLock(count, buffers);
      } finally {
        count = 0;
        heapLock.unlock();
      }
    }
  }

  @Override
  public void evictProactively() {
    evictOrPurge(false);
  }

  @Override
  public void cache(LlapCacheableBuffer buffer, Priority priority) {
    // LRFU cache policy doesn't store locked blocks. When we cache, the block is locked, so
    // we simply do nothing here. The fact that it was never updated will allow us to add it
    // properly on the first notifyUnlock.
    // We'll do is set priority, to account for the inbound one. No lock - not in heap.
    assert buffer.lastUpdate == -1;
    long time = timer.incrementAndGet();
    buffer.priority = F0;
    buffer.lastUpdate = time;
    if (priority == Priority.HIGH) {
      // This is arbitrary. Note that metadata may come from a big scan and nuke all the data
      // from some small frequently accessed tables, because it gets such a large priority boost
      // to start with. Think of the multiplier as the number of accesses after which the data
      // becomes more important than some random read-once metadata, in a pure-LFU scheme.
      buffer.priority *= 3;
    } else {
      assert priority == Priority.NORMAL;
    }
  }

  @Override
  public void notifyLock(LlapCacheableBuffer buffer) {
    // We do not proactively remove locked items from the heap, and opportunistically try to
    // remove from the list (since eviction is mostly from the list). If eviction stumbles upon
    // a locked item in either, it will remove it from cache; when we unlock, we are going to
    // put it back or update it, depending on whether this has happened. This should cause
    // most of the expensive cache update work to happen in unlock, not blocking processing.
    if (buffer.indexInHeap != LlapCacheableBuffer.IN_LIST || !listLock.tryLock()) {
      return;
    }

    removeFromListAndUnlock(buffer);
  }

  @Override
  public void notifyUnlock(LlapCacheableBuffer buffer) {
    // In the very rare chance that a buffer was marked but then accessed again we remove the mark from it
    // - except if instant deallocation is turned on of course -
    if (proactiveEvictionEnabled && !instantProactiveEviction) {
      buffer.removeProactiveEvictionMark();
    }

    if (Thread.currentThread() instanceof LlapPooledIOThread) {
      BPWrapper bpWrapper = threadLocalBPWrapper.get();

      // This will only block in a very very rare scenario only.
      bpWrapper.lock.lock();
      try {
        final LlapCacheableBuffer[] cacheableBuffers = bpWrapper.buffers;
        if (bpWrapper.count < maxQueueSize) {
          cacheableBuffers[bpWrapper.count] = buffer;
          ++bpWrapper.count;
        }
        if (bpWrapper.count <= maxQueueSize / 2) {
          // case too early to flush
          return;
        }

        if (bpWrapper.count == maxQueueSize) {
          // case we have to flush thus block on heap lock
          bpWrapper.flush();
          return;
        }
        bpWrapper.tryFlush(); //case 50% < queue usage < 100%, flush is preferred but not required yet
      } finally {
        bpWrapper.lock.unlock();
      }
    } else {
      heapLock.lock();
      try {
        doNotifyUnderHeapLock(buffer);
      } finally {
        heapLock.unlock();
      }
    }
  }

  private void doNotifyUnderHeapLock(int count, LlapCacheableBuffer[] cacheableBuffers) {
    for (int i = 0; i < count; i++) {
      doNotifyUnderHeapLock(cacheableBuffers[i]);
    }
  }

  private void doNotifyUnderHeapLock(LlapCacheableBuffer buffer) {
    long time = timer.incrementAndGet();
    if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
      LlapIoImpl.CACHE_LOGGER.trace("Touching {} at {}", buffer, time);
    }
    // First, update buffer priority - we have just been using it.
    buffer.priority = (buffer.lastUpdate == -1) ? F0
        : touchPriority(time, buffer.lastUpdate, buffer.priority);
    buffer.lastUpdate = time;
    // Then, if the buffer was in the list, remove it.
    if (buffer.indexInHeap == LlapCacheableBuffer.IN_LIST) {
      listLock.lock();
      removeFromListAndUnlock(buffer);
    }
    // The only concurrent change that can happen when we hold the heap lock is list removal;
    // we have just ensured the item is not in the list, so we have a definite state now.
    if (buffer.indexInHeap >= 0) {
      // The buffer has lived in the heap all along. Restore heap property.
      heapifyDownUnderLock(buffer, time);
    } else if (heapSize == heap.length) {
      // The buffer is not in the (full) heap. Demote the top item of the heap into the list.
      LlapCacheableBuffer demoted = heap[0];
      listLock.lock();
      try {
        assert demoted.indexInHeap == 0; // Noone could have moved it, we have the heap lock.
        demoted.indexInHeap = LlapCacheableBuffer.IN_LIST;
        demoted.prev = null;
        if (listHead != null) {
          demoted.next = listHead;
          listHead.prev = demoted;
          listHead = demoted;
        } else {
          listHead = demoted;
          listTail = demoted;
          demoted.next = null;
        }
      } finally {
        listLock.unlock();
      }
      // Now insert the new buffer in its place and restore heap property.
      buffer.indexInHeap = 0;
      heapifyDownUnderLock(buffer, time);
    } else {
      // Heap is not full, add the buffer to the heap and restore heap property up.
      assert heapSize < heap.length : heap.length + " < " + heapSize;
      buffer.indexInHeap = heapSize;
      heapifyUpUnderLock(buffer, time);
      ++heapSize;
    }
  }

  @Override
  public void setEvictionListener(EvictionListener listener) {
    this.evictionListener = listener;
  }

  /**
   * Flushes all BPWrappers which will in turn clear IO-Threads' threadlocal buffers.
   */
  private void flushAllBPWrappers() {
    for (BPWrapper bpWrapper : bpWrappers.values()) {
      bpWrapper.lock.lock();
      try {
        bpWrapper.flush();
      } finally {
        bpWrapper.lock.unlock();
      }
    }
  }

  private long evictOrPurge(boolean isPurge) {
    flushAllBPWrappers();
    long evicted = 0;
    LlapCacheableBuffer oldTail;
    listLock.lock();
    try {
      LlapCacheableBuffer current = listTail;
      LlapCacheableBuffer lockedHead = null;
      LlapCacheableBuffer lockedTail = null;
      oldTail = listTail;
      while (current != null) {
        // Case for when there is proactive eviction, but current buffer is not marked -> thus need to be kept
        if (!isPurge && !current.isMarkedForEviction()) {
          LlapCacheableBuffer newCurrent = current.prev;
          oldTail = removeFromLocalList(oldTail, current);

          current.indexInHeap = LlapCacheableBuffer.IN_LIST;
          if (lockedHead != null) {
            current.next = lockedHead;
            lockedHead.prev = current;
            lockedHead = current;
          } else {
            lockedHead = current;
            lockedTail = current;
            current.next = null;
          }

          current = newCurrent;
          continue;
        }
        int invalidateResult = current.invalidate();
        current.indexInHeap = LlapCacheableBuffer.NOT_IN_CACHE;
        if (invalidateResult == LlapCacheableBuffer.INVALIDATE_OK) {
          current = current.prev;
        }
        if (invalidateResult == LlapCacheableBuffer.INVALIDATE_ALREADY_INVALID && instantProactiveEviction &&
            current.isMarkedForEviction()) {
          // Found a marked and instantly deallocated buffer. If this is a purge we need to do a proper cleanup of
          // this buffer. If this is a proactive sweep, cleanup will be done later in this method.
          if (isPurge) {
            evictionListener.notifyProactivelyEvicted(current);
          } else {
            current = current.prev;
            continue;
          }
        }

        // Runs if invalidation didn't succeed due to a non-proactive eviction cause (e.g. buffer is locked and
        // currently being used). Also runs if there was proactive eviction (+instant dealloc) and this is a purge run.
        // In either case we need to remove this buffer from the list of to-be-removed buffers.
        if (invalidateResult != LlapCacheableBuffer.INVALIDATE_OK) {
          // Remove from the list.
          LlapCacheableBuffer newCurrent = current.prev;
          oldTail = removeFromLocalList(oldTail, current);
          current = newCurrent;
        }
      }
      listHead = lockedHead;
      listTail = lockedTail;
    } finally {
      listLock.unlock();
    }

    LlapCacheableBuffer[] oldHeap;
    int oldHeapSize;
    heapLock.lock();
    try {
      oldHeap = heap;
      oldHeapSize = heapSize;
      heap = new LlapCacheableBuffer[maxHeapSize];
      heapSize = 0;
      for (int i = 0; i < oldHeapSize; ++i) {
        LlapCacheableBuffer result = oldHeap[i];
        // Case for when there is proactive eviction, but current buffer is not marked -> thus need to be kept
        if (!isPurge && !result.isMarkedForEviction()) {
          oldHeap[i] = null;
          result.indexInHeap = heapSize;
          heapifyUpUnderLock(result, result.lastUpdate);
          ++heapSize;
          continue;
        }

        result.indexInHeap = LlapCacheableBuffer.NOT_IN_CACHE;
        int invalidateResult = result.invalidate();
        if (invalidateResult != LlapCacheableBuffer.INVALIDATE_OK) {
          if (invalidateResult == LlapCacheableBuffer.INVALIDATE_ALREADY_INVALID && instantProactiveEviction &&
              result.isMarkedForEviction()) {
            // Found a marked and instantly deallocated buffer. If this is a purge we need to do a proper cleanup of
            // this buffer. If this is a proactive sweep cleanup will be done later in this method.
            if (isPurge) {
              oldHeap[i] = null;
              evictionListener.notifyProactivelyEvicted(result);
            }
          } else {
            // Other - non proactive eviction relating cases
            oldHeap[i] = null; // Removed from heap without evicting.
          }
        }
      }
    } finally {
      heapLock.unlock();
    }
    LlapCacheableBuffer current = oldTail;
    while (current != null) {
      evicted += current.getMemoryUsage();
      if (isPurge) {
        evictionListener.notifyEvicted(current);
      } else {
        evictionListener.notifyProactivelyEvicted(current);
      }
      current = current.prev;
    }
    for (int i = 0; i < oldHeapSize; ++i) {
      current = oldHeap[i];
      if (current == null) {
        continue;
      }
      evicted += current.getMemoryUsage();
      if (isPurge) {
        evictionListener.notifyEvicted(current);
      } else {
        evictionListener.notifyProactivelyEvicted(current);
      }
    }
    if (isPurge) {
      LlapIoImpl.LOG.info("PURGE: evicted {} from LRFU policy",
          LlapUtil.humanReadableByteCount(evicted));
    } else {
      LlapIoImpl.LOG.info("PROACTIVE_EVICTION: evicted {} from LRFU policy",
          LlapUtil.humanReadableByteCount(evicted));
    }
    return evicted;
  }

  @Override
  public long purge() {
    return evictOrPurge(true);
  }

  private static LlapCacheableBuffer removeFromLocalList(
      LlapCacheableBuffer tail, LlapCacheableBuffer current) {
    if (current == tail) {
      tail = current.prev;
    } else {
      current.next.prev = current.prev;
    }
    if (current.prev != null) {
      current.prev.next = current.next;
    }
    current.prev = null;
    current.next = null;
    return tail;
  }


  @Override
  public long evictSomeBlocks(long memoryToReserve) {
    // In normal case, we evict the items from the list.
    long evicted = evictFromList(memoryToReserve);
    if (evicted >= memoryToReserve) {
      return evicted;
    }
    // This should not happen unless we are evicting a lot at once, or buffers are large (so
    // there's a small number of buffers and they all live in the heap).
    long time = timer.get();
    while (evicted < memoryToReserve) {
      LlapCacheableBuffer buffer;
      heapLock.lock();
      try {
        buffer = evictFromHeapUnderLock(time);
      } finally {
        heapLock.unlock();
      }
      if (buffer == null) {
        return evicted;
      }
      evicted += buffer.getMemoryUsage();
      evictionListener.notifyEvicted(buffer);
    }
    return evicted;
  }

  private long evictFromList(long memoryToReserve) {
    long evicted = 0;
    LlapCacheableBuffer nextCandidate, firstCandidate;
    listLock.lock();
    // We assume that there are no locked blocks in the list; or if they are, they can be dropped.
    // Therefore we always evict one contiguous sequence from the tail. We can find it in one pass,
    // splice it out and then finalize the eviction outside of the list lock.
    try {
      nextCandidate = listTail;
      firstCandidate = listTail;
      while (evicted < memoryToReserve && nextCandidate != null) {
        int invalidateResult = nextCandidate.invalidate();
        if (LlapCacheableBuffer.INVALIDATE_OK != invalidateResult) {
          // Locked, or invalidated, buffer was in the list - just drop it;
          // will be re-added on unlock.
          LlapCacheableBuffer lockedBuffer = nextCandidate;
          if (firstCandidate == nextCandidate) {
            firstCandidate = nextCandidate.prev;
          }
          nextCandidate = nextCandidate.prev;
          removeFromListUnderLock(lockedBuffer);
          if (instantProactiveEviction && LlapCacheableBuffer.INVALIDATE_ALREADY_INVALID == invalidateResult &&
              lockedBuffer.isMarkedForEviction()) {
            // Cleanup an already marked and deallocated buffer - this call is needed for administration purposes
            evictionListener.notifyProactivelyEvicted(lockedBuffer);
          }
          continue;
        }
        // Update the state to removed-from-list, so that parallel notifyUnlock doesn't modify us.
        nextCandidate.indexInHeap = LlapCacheableBuffer.NOT_IN_CACHE;
        evicted += nextCandidate.getMemoryUsage();
        nextCandidate = nextCandidate.prev;
      }
      if (firstCandidate != nextCandidate) {
        if (nextCandidate == null) {
          listHead = null;
          listTail = null; // We have evicted the entire list.
        } else {
          // Splice the section that we have evicted out of the list.
          // We have already updated the state above so no need to do that again.
          removeFromListUnderLockNoStateUpdate(nextCandidate.next, firstCandidate);
        }
      }
    } finally {
      listLock.unlock();
    }
    while (firstCandidate != nextCandidate) {
      evictionListener.notifyEvicted(firstCandidate);
      firstCandidate = firstCandidate.prev;
    }
    return evicted;
  }

  // Note: rarely called (unless buffers are very large or we evict a lot, or in LFU case).
  private LlapCacheableBuffer evictFromHeapUnderLock(long time) {
    while (true) {
      if (heapSize == 0) {
        return null;
      }
      LlapCacheableBuffer result = evictHeapElementUnderLock(time, 0);
      if (result != null) {
        return result;
      }
    }
  }

  private void heapifyUpUnderLock(LlapCacheableBuffer buffer, long time) {
    // See heapifyDown comment.
    int ix = buffer.indexInHeap;
    double priority = buffer.priority;
    while (true) {
      if (ix == 0) {
        break; // Buffer is at the top of the heap.
      }
      int parentIx = (ix - 1) >>> 1;
      LlapCacheableBuffer parent = heap[parentIx];
      double parentPri = getHeapifyPriority(parent, time);
      if (priority >= parentPri) {
        break;
      }
      heap[ix] = parent;
      parent.indexInHeap = ix;
      ix = parentIx;
    }
    buffer.indexInHeap = ix;
    heap[ix] = buffer;
  }

  private LlapCacheableBuffer evictHeapElementUnderLock(long time, int ix) {
    LlapCacheableBuffer result = heap[ix];
    if (LlapIoImpl.CACHE_LOGGER.isTraceEnabled()) {
      LlapIoImpl.CACHE_LOGGER.trace("Evicting {} at {}", result, time);
    }
    result.indexInHeap = LlapCacheableBuffer.NOT_IN_CACHE;
    --heapSize;
    int invalidateResult = result.invalidate();
    boolean canEvict = invalidateResult == LlapCacheableBuffer.INVALIDATE_OK;
    if (heapSize > 0) {
      LlapCacheableBuffer newRoot = heap[heapSize];
      newRoot.indexInHeap = ix;
      if (newRoot.lastUpdate != time) {
        newRoot.priority = expirePriority(time, newRoot.lastUpdate, newRoot.priority);
        newRoot.lastUpdate = time;
      }
      heapifyDownUnderLock(newRoot, time);
    }
    if (instantProactiveEviction && LlapCacheableBuffer.INVALIDATE_ALREADY_INVALID == invalidateResult &&
        result.isMarkedForEviction()) {
      // Cleanup an already marked and deallocated buffer - this call is needed for administration purposes
      evictionListener.notifyProactivelyEvicted(result);
    }
    // Otherwise we just removed a locked/invalid item from heap; we continue.
    return canEvict ? result : null;
  }

  private void heapifyDownUnderLock(LlapCacheableBuffer buffer, long time) {
    // Relative positions of the blocks don't change over time; priorities we expire can only
    // decrease; we only have one block that could have broken heap rule and we always move it
    // down; therefore, we can update priorities of other blocks as we go for part of the heap -
    // we correct any discrepancy w/the parent after expiring priority, and any block we expire
    // the priority for already has lower priority than that of its children.
    int ix = buffer.indexInHeap;
    double priority = buffer.priority;
    while (true) {
      int newIx = moveMinChildUp(ix, time, priority);
      if (newIx == -1) {
        break;
      }
      ix = newIx;
    }
    buffer.indexInHeap = ix;
    heap[ix] = buffer;
  }

  /**
   * Moves the minimum child of targetPos block up to targetPos; optionally compares priorities
   * and terminates if targetPos element has lesser value than either of its children.
   * @return the index of the child that was moved up; -1 if nothing was moved due to absence
   *         of the children, or a failed priority check.
   */
  private int moveMinChildUp(int targetPos, long time, double comparePri) {
    int leftIx = (targetPos << 1) + 1, rightIx = leftIx + 1;
    if (leftIx >= heapSize) {
      return -1; // Buffer is at the leaf node.
    }
    LlapCacheableBuffer left = heap[leftIx], right = null;
    if (rightIx < heapSize) {
      right = heap[rightIx];
    }
    double leftPri = getHeapifyPriority(left, time), rightPri = getHeapifyPriority(right, time);
    if (comparePri >= 0 && comparePri <= leftPri && comparePri <= rightPri) {
      return -1;
    }
    if (leftPri <= rightPri) { // prefer left, cause right might be missing
      heap[targetPos] = left;
      left.indexInHeap = targetPos;
      return leftIx;
    } else {
      heap[targetPos] = right;
      right.indexInHeap = targetPos;
      return rightIx;
    }
  }

  private double getHeapifyPriority(LlapCacheableBuffer buf, long time) {
    if (buf == null) {
      return Double.MAX_VALUE;
    }
    if (buf.lastUpdate != time && time >= 0) {
      buf.priority = expirePriority(time, buf.lastUpdate, buf.priority);
      buf.lastUpdate = time;
    }
    return buf.priority;
  }

  private void removeFromListAndUnlock(LlapCacheableBuffer buffer) {
    try {
      if (buffer.indexInHeap != LlapCacheableBuffer.IN_LIST) {
        return;
      }
      removeFromListUnderLock(buffer);
    } finally {
      listLock.unlock();
    }
  }

  private void removeFromListUnderLock(LlapCacheableBuffer buffer) {
    buffer.indexInHeap = LlapCacheableBuffer.NOT_IN_CACHE;
    boolean isTail = buffer == listTail, isHead = buffer == listHead;
    if ((isTail != (buffer.next == null)) || (isHead != (buffer.prev == null))) {
      debugDumpListOnError(buffer);
      throw new AssertionError("LRFU list is corrupted.");
    }
    if (isTail) {
      listTail = buffer.prev;
    } else {
      buffer.next.prev = buffer.prev;
    }
    if (isHead) {
      listHead = buffer.next;
    } else {
      buffer.prev.next = buffer.next;
    }
  }

  private void removeFromListUnderLockNoStateUpdate(
      LlapCacheableBuffer from, LlapCacheableBuffer to) {
    boolean isToTail = to == listTail, isFromHead = from == listHead;
    if ((isToTail != (to.next == null)) || (isFromHead != (from.prev == null))) {
      debugDumpListOnError(from, to);
      throw new AssertionError("LRFU list is corrupted.");
    }
    if (isToTail) {
      listTail = from.prev;
    } else {
      to.next.prev = from.prev;
    }
    if (isFromHead) {
      listHead = to.next;
    } else {
      from.prev.next = to.next;
    }
  }

  private void debugDumpListOnError(LlapCacheableBuffer... buffers) {
    // Hopefully this will be helpful in case of NPEs.
    StringBuilder listDump = new StringBuilder("Invalid list removal. List: ");
    try {
      dumpList(listDump, listHead, listTail);
      int i = 0;
      for (LlapCacheableBuffer buffer : buffers) {
        listDump.append("; list from the buffer #").append(i).append(" being removed: ");
        dumpList(listDump, buffer, null);
      }
    } catch (Throwable t) {
      LlapIoImpl.LOG.error("Error dumping the lists on error", t);
    }
    LlapIoImpl.LOG.error(listDump.toString());
  }

  public String debugDumpHeap() {
    StringBuilder result = new StringBuilder("List: ");
    dumpList(result, listHead, listTail);
    result.append("\nHeap:");
    if (heapSize == 0) {
      result.append(" <empty>\n");
      return result.toString();
    }
    result.append("\n");
    int levels = 32 - Integer.numberOfLeadingZeros(heapSize);
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

  private static void dumpList(StringBuilder result,
      LlapCacheableBuffer listHeadLocal, LlapCacheableBuffer listTailLocal) {
    if (listHeadLocal == null) {
      result.append("<empty>");
      return;
    }
    LlapCacheableBuffer listItem = listHeadLocal;
    while (listItem.prev != null) {
      listItem = listItem.prev;  // To detect incorrect lists.
    }
    while (listItem != null) {
      result.append(listItem.toStringForCache());
      if (listItem == listTailLocal) {
        result.append("(tail)"); // To detect incorrect lists.
      }
      if (listItem == listHeadLocal) {
        result.append("(head)"); // To detect incorrect lists.
      }
      result.append(" -> ");
      listItem = listItem.next;
    }
  }

  @Override
  public void debugDumpShort(StringBuilder sb) {
    long[] metricData = metrics.getUsageStats();
    sb.append("\nLRFU eviction list: ")
      .append(metricData[PolicyMetrics.LISTSIZE]).append(" items");
    sb.append("\nLRFU eviction heap: ")
      .append(heapSize).append(" items (of max ").append(maxHeapSize).append(")");
    sb.append("\nLRFU data on heap: ")
      .append(LlapUtil.humanReadableByteCount(metricData[PolicyMetrics.DATAONHEAP]));
    sb.append("\nLRFU metadata on heap: ")
      .append(LlapUtil.humanReadableByteCount(metricData[PolicyMetrics.METAONHEAP]));
    sb.append("\nLRFU data on eviction list: ")
      .append(LlapUtil.humanReadableByteCount(metricData[PolicyMetrics.DATAONLIST]));
    sb.append("\nLRFU metadata on eviction list: ")
      .append(LlapUtil.humanReadableByteCount(metricData[PolicyMetrics.METAONLIST]));
    sb.append("\nLRFU data locked: ")
      .append(LlapUtil.humanReadableByteCount(metricData[PolicyMetrics.LOCKEDDATA]));
    sb.append("\nLRFU metadata locked: ")
      .append(LlapUtil.humanReadableByteCount(metricData[PolicyMetrics.LOCKEDMETA]));
    sb.append("\nLRFU BP wrapper: ")
        .append(metricData[PolicyMetrics.BPWRAPCNT])
        .append(" total items, ")
        .append(metricData[PolicyMetrics.BPWRAPDISTINCT])
        .append(" distinct buffers, that use cache space: ")
        .append(LlapUtil.humanReadableByteCount(metricData[PolicyMetrics.BPWRAPDATA]))
        .append(" for data, ")
        .append(LlapUtil.humanReadableByteCount(metricData[PolicyMetrics.BPWRAPMETA]))
        .append(" for metadata.");
  }

  @Override public List<LlapCacheableBuffer> getHotBuffers() {
    List<LlapCacheableBuffer> buffers = Lists.newLinkedList();
    long allocatedBytesCounter = 0;
    long[] usageStats = metrics.getUsageStats();
    long limit = Math.round((usageStats[PolicyMetrics.DATAONHEAP] + usageStats[PolicyMetrics.DATAONLIST]) * cutoff);

    LlapCacheableBuffer[] copy;
    try {
      heapLock.lock();
      copy = Arrays.copyOf(heap, heap.length);
    } finally {
      heapLock.unlock();
    }

    long t = timer.get();
    Arrays.sort(copy,
        nullsLast(comparing(b -> b.lastUpdate == t ? b.priority : expirePriority(t, b.lastUpdate, b.priority))));
    for (int i = copy.length - 1; i >= 0; i--) {
      if (copy[i] != null && !(copy[i] instanceof MetadataCache.LlapMetadataBuffer)) {
        long memoryUsage = copy[i].getMemoryUsage();
        if (allocatedBytesCounter + memoryUsage <= limit) {
          buffers.add(copy[i]);
          allocatedBytesCounter += memoryUsage;
        } else {
          return buffers;
        }
      }
    }

    try {
      listLock.lock();
      LlapCacheableBuffer scan = listHead;
      while (null != scan) {
        if (scan instanceof MetadataCache.LlapMetadataBuffer) {
          scan = scan.next;
        } else {
          long memoryUsage = scan.getMemoryUsage();
          if (allocatedBytesCounter + memoryUsage <= limit) {
            buffers.add(scan);
            allocatedBytesCounter += memoryUsage;
            scan = scan.next;
          } else {
            return buffers;
          }
        }
      }
    } finally {
      listLock.unlock();
    }

    return buffers;
  }

  /**
   * Metrics Information for LRFU specific policy information.
   * This enumeration is used by the @code PolicyMetrics instance to
   * define and describe the metrics.
   */
  private enum PolicyInformation implements MetricsInfo {
    PolicyMetrics("LRFU cache policy based metrics"),
    DataOnHeap("Amount of bytes used for data on min-heap"),
    DataOnList("Amount of bytes used for data on eviction short list"),
    MetaOnHeap("Amount of bytes used for meta data on min-heap"),
    MetaOnList("Amount of bytes used for meta data on eviction short list"),
    DataLocked("Amount of locked data in bytes (in use)"),
    MetaLocked("Amount of locked meta data in bytes (in use)"),
    HeapSize("Number of buffers on the min-heap"),
    HeapSizeMax("Capacity (number of buffers) of the min-heap"),
    ListSize("Number of buffers on the eviction short list"),
    BPWrapperCount("Number of all buffers in BPWrapper threadlocals"),
    BPWrapperDistinct("Number of distinct buffers in BPWrapper threadlocals"),
    BPWrapperData("Amount of bytes for data buffers in BPWrapper threadlocals"),
    BPWrapperMeta("Amount of bytes for metadata buffers in BPWrapper threadlocals"),
    TotalData("Total amount of bytes, used for data"),
    TotalMeta("Total amount of bytes, used for meta data");

    private final String description; // metric explaination

    /**
     * Creates a new enum value.
     *
     * @param description The explaination of the metric
     */
    PolicyInformation(String description) {
      this.description = description;
    }

    @Override
    public String description() {
      return description;
    }
  }

  /**
   * Metrics provider for the LRFU cache policy.
   * An instance of this class is providing JMX (through haddoop metrics)
   * statistics for the LRFU cache policy for monitoring.
   */
  @Metrics(about = "LRFU Cache Policy Metrics", context = "cache")
  @VisibleForTesting
  class PolicyMetrics implements MetricsSource {
    public static final int DATAONHEAP     = 0;
    public static final int DATAONLIST     = 1;
    public static final int METAONHEAP     = 2;
    public static final int METAONLIST     = 3;
    public static final int LISTSIZE       = 4;
    public static final int LOCKEDDATA     = 5;
    public static final int LOCKEDMETA     = 6;
    public static final int BPWRAPCNT      = 7;
    public static final int BPWRAPDISTINCT = 8;
    public static final int BPWRAPDATA     = 9;
    public static final int BPWRAPMETA     = 10;

    private final String session;  // identifier for the LLAP daemon

    /**
     * Creates a new metrics producer.
     *
     * @param session The LLAP daemon identifier
     */
    PolicyMetrics(String session) {
      this.session = session;
    }

    /**
     * Helper to get some basic LRFU usage statistics.
     * This method returns a long array with the following content:
     * - amount of data (bytes) on min-heap
     * - amount of data (bytes) on eviction short list
     * - amount of metadata (bytes) on min-heap
     * - amount of metadata (bytes) on eviction short list
     * - size of the eviction short list
     * - amount of locked bytes for data
     * - amount of locked bytes for metadata
     *
     * @return long array with LRFU stats
     */
    public long[] getUsageStats() {
      long dataOnHeap     = 0L;   // all non-meta related buffers on min-heap
      long dataOnList     = 0L;   // all non-meta related buffers on eviction list
      long metaOnHeap     = 0L;   // meta data buffers on min-heap
      long metaOnList     = 0L;   // meta data buffers on eviction list
      long listSize       = 0L;   // number of entries on eviction list
      long lockedData     = 0L;   // number of bytes in locked data buffers
      long lockedMeta     = 0L;   // number of bytes in locked metadata buffers
      long bpWrapCount    = 0L;   // number of buffers in BP wrapper threadlocals
      long bpWrapDistinct = 0L;   // number of distinct buffers in BP wrapper threadlocals
      long bpWrapData     = 0L;   // number of bytes stored in BP wrapper data buffers
      long bpWrapMeta     = 0L;   // number of bytes stored in BP wrapper metadata buffers

      // Using set to produce result of distinct buffers only
      // (same buffer may be present in multiple thread local bp wrappers, or even inside heap/list, but ultimately
      // it uses the same cache space)
      Set<LlapCacheableBuffer> bpWrapperBuffers = new HashSet<>();
      for (BPWrapper bpWrapper : bpWrappers.values()) {
        bpWrapper.lock.lock();
        try {
          bpWrapCount += bpWrapper.count;
          for (int i = 0; i < bpWrapper.count; ++i) {
            bpWrapperBuffers.add(bpWrapper.buffers[i]);
          }
        } finally {
          bpWrapper.lock.unlock();
        }
      }

      // aggregate values on the heap
      heapLock.lock();
      try {
        for (int heapIdx = 0; heapIdx < heapSize; ++heapIdx) {
          LlapCacheableBuffer buff = heap[heapIdx];

          if (null != buff) {
            if (buff instanceof LlapMetadataBuffer) {
              metaOnHeap += buff.getMemoryUsage();
              if (buff.isLocked()) {
                lockedMeta += buff.getMemoryUsage();
              }
            } else {
              dataOnHeap += buff.getMemoryUsage();
              if (buff.isLocked()) {
                lockedData += buff.getMemoryUsage();
              }
            }
            bpWrapperBuffers.remove(buff);
          }
        }
      } finally {
        heapLock.unlock();
      }

      // aggregate values on the evicition short list
      try {
        listLock.lock();
        LlapCacheableBuffer scan = listHead;
        while (null != scan) {
          if (scan instanceof LlapMetadataBuffer) {
            metaOnList += scan.getMemoryUsage();
            if (scan.isLocked()) {
              lockedMeta += scan.getMemoryUsage();
            }
          } else {
            dataOnList += scan.getMemoryUsage();
            if (scan.isLocked()) {
              lockedData += scan.getMemoryUsage();
            }
          }
          bpWrapperBuffers.remove(scan);
          ++listSize;
          scan = scan.next;
        }
      } finally {
        listLock.unlock();
      }


      for (LlapCacheableBuffer buff : bpWrapperBuffers) {
        if (buff instanceof LlapMetadataBuffer) {
          bpWrapMeta += buff.getMemoryUsage();
        } else {
          bpWrapData += buff.getMemoryUsage();
        }
        ++bpWrapDistinct;
      }

      return new long[] {dataOnHeap, dataOnList,
                         metaOnHeap, metaOnList, listSize,
                         lockedData, lockedMeta,
                         bpWrapCount, bpWrapDistinct, bpWrapData, bpWrapMeta};
    }

    @Override
    public synchronized void getMetrics(MetricsCollector collector, boolean all) {
      long[] usageStats = getUsageStats();

      // start a new record
      MetricsRecordBuilder mrb = collector.addRecord(PolicyInformation.PolicyMetrics)
                                          .setContext("cache")
                                          .tag(MsInfo.ProcessName,
                                               MetricsUtils.METRICS_PROCESS_NAME)
                                          .tag(MsInfo.SessionId, session);

      // add the values to the new record
      mrb.addCounter(PolicyInformation.DataOnHeap,         usageStats[DATAONHEAP])
          .addCounter(PolicyInformation.DataOnList,        usageStats[DATAONLIST])
          .addCounter(PolicyInformation.MetaOnHeap,        usageStats[METAONHEAP])
          .addCounter(PolicyInformation.MetaOnList,        usageStats[METAONLIST])
          .addCounter(PolicyInformation.DataLocked,        usageStats[LOCKEDDATA])
          .addCounter(PolicyInformation.MetaLocked,        usageStats[LOCKEDMETA])
          .addCounter(PolicyInformation.HeapSize,          heapSize)
          .addCounter(PolicyInformation.HeapSizeMax,       maxHeapSize)
          .addCounter(PolicyInformation.ListSize,          usageStats[LISTSIZE])
          .addCounter(PolicyInformation.BPWrapperCount,    usageStats[BPWRAPCNT])
          .addCounter(PolicyInformation.BPWrapperDistinct, usageStats[BPWRAPDISTINCT])
          .addCounter(PolicyInformation.BPWrapperData,     usageStats[BPWRAPDATA])
          .addCounter(PolicyInformation.TotalData,         usageStats[DATAONHEAP]
                                                         + usageStats[DATAONLIST]
                                                         + usageStats[BPWRAPDATA])
          .addCounter(PolicyInformation.TotalMeta,         usageStats[METAONHEAP]
                                                         + usageStats[METAONLIST]
                                                         + usageStats[BPWRAPMETA]);
    }
  }
}
