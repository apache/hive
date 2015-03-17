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
  protected final long maxSize;
  private final LowLevelCachePolicy evictor;
  private LlapDaemonCacheMetrics metrics;

  public LowLevelCacheMemoryManager(Configuration conf, LowLevelCachePolicy evictor,
      LlapDaemonCacheMetrics metrics) {
    this.maxSize = HiveConf.getLongVar(conf, ConfVars.LLAP_ORC_CACHE_MAX_SIZE);
    this.evictor = evictor;
    this.usedMemory = new AtomicLong(0);
    this.metrics = metrics;
    metrics.incrCacheCapacityTotal(maxSize);
    if (LlapIoImpl.LOGL.isInfoEnabled()) {
      LlapIoImpl.LOG.info("Cache memory manager initialized with max size " + maxSize);
    }
  }

  @Override
  public boolean reserveMemory(long memoryToReserve, boolean waitForEviction) {
    // TODO: if this cannot evict enough, it will spin infinitely. Terminate at some point?
    while (memoryToReserve > 0) {
      long usedMem = usedMemory.get(), newUsedMem = usedMem + memoryToReserve;
      if (newUsedMem <= maxSize) {
        if (usedMemory.compareAndSet(usedMem, newUsedMem)) break;
        continue;
      }
      // TODO: for one-block case, we could move notification for the last block out of the loop.
      long evicted = evictor.evictSomeBlocks(memoryToReserve);
      if (!waitForEviction && evicted == 0) return false;
      // Adjust the memory - we have to account for what we have just evicted.
      while (true) {
        long reserveWithEviction = Math.min(memoryToReserve, maxSize - usedMem + evicted);
        if (usedMemory.compareAndSet(usedMem, usedMem - evicted + reserveWithEviction)) {
          memoryToReserve -= reserveWithEviction;
          break;
        }
        usedMem = usedMemory.get();
      }
    }
    metrics.incrCacheCapacityUsed(memoryToReserve);
    return true;
  }

  @Override
  // Not used by the data cache.
  public void releaseMemory(long memoryToRelease) {
    long oldV;
    do {
      oldV = usedMemory.get();
      assert oldV >= memoryToRelease;
    } while (!usedMemory.compareAndSet(oldV, oldV - memoryToRelease));
  }

}
