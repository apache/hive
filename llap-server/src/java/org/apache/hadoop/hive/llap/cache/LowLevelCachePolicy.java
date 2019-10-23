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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.cache.LowLevelCache.Priority;

/**
 * Actor managing the eviction requests.
 * Cache policy relies notifications from the actual {@link LowLevelCache} to keep track of buffer access.
 */
public interface LowLevelCachePolicy extends LlapIoDebugDump {

  static LowLevelCachePolicy provideFromConf(Configuration conf) {
    final long totalMemorySize = HiveConf.getSizeVar(conf, HiveConf.ConfVars.LLAP_IO_MEMORY_MAX_SIZE);
    final int minAllocSize = (int) HiveConf.getSizeVar(conf, HiveConf.ConfVars.LLAP_ALLOCATOR_MIN_ALLOC);
    String policyName = HiveConf.getVar(conf,HiveConf.ConfVars.LLAP_IO_CACHE_STRATEGY);
    //default to fifo.
    final LowLevelCachePolicy realCachePolicy;
    switch (policyName) {
    case "lrfu":
      realCachePolicy = new LowLevelLrfuCachePolicy(minAllocSize, totalMemorySize, conf);
      break;
    case "clock":
      realCachePolicy = new ClockCachePolicy(HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_IO_MAX_CLOCK_ROTATION));
      break;
    case "fifo":
      realCachePolicy =  new LowLevelFifoCachePolicy();
      break;
    default:
      throw new IllegalArgumentException("Unknown cache replacement strategy [" + policyName +"]");
    }
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_TRACK_CACHE_USAGE)) {
      return  new CacheContentsTracker(realCachePolicy);
    } else {
      return realCachePolicy;
    }
  }

  /**
   * Signals to the policy the addition of a new page to the cache directory.
   *
   * @param buffer   buffer to be cached
   * @param priority the priority of cached element
   */
  void cache(LlapCacheableBuffer buffer, Priority priority);

  /**
   * Notifies the policy that this buffer is locked, thus take it out of the free list.
   * Note that this notification is a hint and can not be the source of truth about what can be evicted
   * currently the source of truth is the counter of reference to the buffer see {@link LlapCacheableBuffer#isLocked()}.
   *
   * @param buffer buffer to be locked.
   */
  void notifyLock(LlapCacheableBuffer buffer);

  /**
   * Notifies the policy that a buffer is unlocked after been used. This notification signals to the policy that an
   * access to this page occurred thus can be used to track what page got a read request
   *
   * @param buffer buffer that just got unlocked
   */
  void notifyUnlock(LlapCacheableBuffer buffer);

  /**
   * Signals to the policy that it has to evict some pages to make room incoming buffers.
   * Policy has to at least evict the amount requested.
   * Policy does not now about the shape of evicted buffers and only can reason about total size.
   * Not that is method will block until at least {@code memoryToReserve} bytes are evicted.
   *
   * @param memoryToReserve amount of bytes to be evicted
   * @return actual amount of evicted bytes.
   */
  long evictSomeBlocks(long memoryToReserve);

  /**
   * Sets the eviction listener dispatcher.
   *
   * @param listener eviction listener actor
   */
  void setEvictionListener(EvictionListener listener);

  /**
   * Signals to the policy to evict all the unlocked used buffers.
   *
   * @return amount (bytes) of memory evicted.
   */
  long purge();
}
