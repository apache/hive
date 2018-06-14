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

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;

/**
 * Implementation of memory manager for low level cache. Note that memory is released during
 * reserve most of the time, by calling the evictor to evict some memory. releaseMemory is
 * called rarely.
 */
public class LowLevelCacheMemoryManager implements MemoryManager {
  private final AtomicLong usedMemory;
  private final LowLevelCachePolicy evictor;
  private final LlapDaemonCacheMetrics metrics;
  private long maxSize;

  public LowLevelCacheMemoryManager(
      long maxSize, LowLevelCachePolicy evictor, LlapDaemonCacheMetrics metrics) {
    this.maxSize = maxSize;
    this.evictor = evictor;
    this.usedMemory = new AtomicLong(0);
    this.metrics = metrics;
    if (LlapIoImpl.LOG.isInfoEnabled()) {
      LlapIoImpl.LOG.info("Memory manager initialized with max size {} and" +
          " {} ability to evict blocks", maxSize, ((evictor == null) ? "no " : ""));
    }
  }


  @Override
  public void reserveMemory(final long memoryToReserve) {
    boolean result = reserveMemory(memoryToReserve, true);
    if (result) return;
    // Can only happen if there's no evictor, or if thread is interrupted.
    throw new RuntimeException("Cannot reserve memory"
        + (Thread.currentThread().isInterrupted() ? "; thread interrupted" : ""));
  }

  @VisibleForTesting
  public boolean reserveMemory(final long memoryToReserve, boolean waitForEviction) {
    // TODO: if this cannot evict enough, it will spin infinitely. Terminate at some point?
    int badCallCount = 0;
    int nextLog = 4;
    long evictedTotalMetric = 0, reservedTotalMetric = 0, remainingToReserve = memoryToReserve;
    boolean result = true;
    while (remainingToReserve > 0) {
      long usedMem = usedMemory.get(), newUsedMem = usedMem + remainingToReserve;
      if (newUsedMem <= maxSize) {
        if (usedMemory.compareAndSet(usedMem, newUsedMem)) {
          reservedTotalMetric += remainingToReserve;
          break;
        }
        continue;
      }
      if (evictor == null) return false;
      // TODO: for one-block case, we could move notification for the last block out of the loop.
      long evicted = evictor.evictSomeBlocks(remainingToReserve);
      if (evicted == 0) {
        if (!waitForEviction) {
          result = false;
          break;
        }
        ++badCallCount;
        if (badCallCount == nextLog) {
          LlapIoImpl.LOG.warn("Cannot evict blocks for " + badCallCount + " calls; cache full?");
          nextLog <<= 1;
          try {
            Thread.sleep(Math.min(1000, nextLog));
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            result = false;
            break;
          }
        }
        continue;
      }
      evictedTotalMetric += evicted;
      badCallCount = 0;
      // Adjust the memory - we have to account for what we have just evicted.
      while (true) {
        long availableToReserveAfterEvict = maxSize - usedMem + evicted;
        long reservedAfterEvict = Math.min(remainingToReserve, availableToReserveAfterEvict);
        if (usedMemory.compareAndSet(usedMem, usedMem - evicted + reservedAfterEvict)) {
          remainingToReserve -= reservedAfterEvict;
          reservedTotalMetric += reservedAfterEvict;
          break;
        }
        usedMem = usedMemory.get();
      }
    }
    if (!result) {
      releaseMemory(reservedTotalMetric);
      reservedTotalMetric = 0;
    }
    metrics.incrCacheCapacityUsed(reservedTotalMetric - evictedTotalMetric);
    return result;
  }

  @Override
  public void releaseMemory(final long memoryToRelease) {
    long oldV;
    do {
      oldV = usedMemory.get();
      assert oldV >= memoryToRelease;
    } while (!usedMemory.compareAndSet(oldV, oldV - memoryToRelease));
    metrics.incrCacheCapacityUsed(-memoryToRelease);
  }

  @Override
  public void updateMaxSize(long maxSize) {
    this.maxSize = maxSize;
  }

  public long purge() {
    if (evictor == null) return 0;
    long evicted = evictor.purge();
    if (evicted == 0) return 0;
    long usedMem = -1;
    do {
      usedMem = usedMemory.get();
    } while (!usedMemory.compareAndSet(usedMem, usedMem - evicted));
    metrics.incrCacheCapacityUsed(-evicted);
    return evicted;
  }
}
