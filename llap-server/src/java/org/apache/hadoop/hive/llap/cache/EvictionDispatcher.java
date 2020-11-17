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

import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.llap.cache.SerDeLowLevelCacheImpl.LlapSerDeDataBuffer;
import org.apache.hadoop.hive.llap.io.metadata.OrcFileEstimateErrors;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache;
import org.apache.hadoop.hive.llap.io.metadata.MetadataCache.LlapMetadataBuffer;

/**
 * Eviction dispatcher - uses double dispatch to route eviction notifications to correct caches.
 */
public final class EvictionDispatcher implements EvictionListener {
  private final LowLevelCache dataCache;
  private final SerDeLowLevelCacheImpl serdeCache;
  private final MetadataCache metadataCache;
  private final EvictionAwareAllocator allocator;

  public EvictionDispatcher(LowLevelCache dataCache, SerDeLowLevelCacheImpl serdeCache,
      MetadataCache metadataCache, EvictionAwareAllocator allocator) {
    this.dataCache = dataCache;
    this.metadataCache = metadataCache;
    this.serdeCache = serdeCache;
    this.allocator = allocator;
  }

  @Override
  public void notifyEvicted(LlapCacheableBuffer buffer) {
    // This will call one of the specific notifyEvicted overloads.
    buffer.notifyEvicted(this, false);
  }

  @Override
  public void notifyProactivelyEvicted(LlapCacheableBuffer buffer) {
    // This will call one of the specific notifyEvicted overloads.
    buffer.notifyEvicted(this, true);
  }

  public void notifyEvicted(LlapSerDeDataBuffer buffer, boolean isProactiveEviction) {
    serdeCache.notifyEvicted(buffer);
    requestDeallocation(buffer, isProactiveEviction);
  }

  public void notifyEvicted(LlapDataBuffer buffer, boolean isProactiveEviction) {
    dataCache.notifyEvicted(buffer);
    requestDeallocation(buffer, isProactiveEviction);
  }

  public void notifyEvicted(LlapMetadataBuffer<?> buffer, boolean isProactiveEviction) {
    metadataCache.notifyEvicted(buffer);
    // Note: the metadata cache may deallocate additional buffers, but not this one.
    requestDeallocation(buffer, isProactiveEviction);
  }

  /**
   * For normal (reactive) evictions, MM expects the free'd up memory to be used for the originating reserve call, thus
   * deallocateEvicted method should be used.
   * For proactive eviction we can give back the MM the free'd up space right away, deallocate method will do that.
   * @param buffer
   * @param isProactiveEviction
   */
  private void requestDeallocation(MemoryBuffer buffer, boolean isProactiveEviction) {
    if (isProactiveEviction) {
      allocator.deallocateProactivelyEvicted(buffer);
    } else {
      allocator.deallocateEvicted(buffer);
    }
  }

  public void notifyEvicted(OrcFileEstimateErrors buffer) {
    metadataCache.notifyEvicted(buffer);
  }
}
