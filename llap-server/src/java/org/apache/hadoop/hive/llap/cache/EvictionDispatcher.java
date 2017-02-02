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

import org.apache.hadoop.hive.llap.io.metadata.OrcFileEstimateErrors;
import org.apache.hadoop.hive.llap.io.metadata.OrcFileMetadata;
import org.apache.hadoop.hive.llap.io.metadata.OrcMetadataCache;
import org.apache.hadoop.hive.llap.io.metadata.OrcStripeMetadata;

/**
 * Eviction dispatcher - uses double dispatch to route eviction notifications to correct caches.
 */
public final class EvictionDispatcher implements EvictionListener {
  private final LowLevelCache dataCache;
  private final SerDeLowLevelCacheImpl serdeCache;
  private final OrcMetadataCache metadataCache;
  private final EvictionAwareAllocator allocator;

  public EvictionDispatcher(LowLevelCache dataCache, SerDeLowLevelCacheImpl serdeCache,
      OrcMetadataCache metadataCache, EvictionAwareAllocator allocator) {
    this.dataCache = dataCache;
    this.metadataCache = metadataCache;
    this.serdeCache = serdeCache;
    this.allocator = allocator;
  }

  @Override
  public void notifyEvicted(LlapCacheableBuffer buffer) {
    buffer.notifyEvicted(this); // This will call one of the specific notifyEvicted overloads.
  }

  public void notifyEvicted(LlapDataBuffer buffer) {
    // Note: we don't know which cache this is from, so we notify both. They can noop if they
    //       want to find the buffer in their structures and can't.
    dataCache.notifyEvicted(buffer);
    if (serdeCache != null) {
      serdeCache.notifyEvicted(buffer);
    }
    allocator.deallocateEvicted(buffer);
  }

  public void notifyEvicted(OrcFileMetadata buffer) {
    metadataCache.notifyEvicted(buffer);
  }

  public void notifyEvicted(OrcStripeMetadata buffer) {
    metadataCache.notifyEvicted(buffer);
  }

  public void notifyEvicted(OrcFileEstimateErrors buffer) {
    metadataCache.notifyEvicted(buffer);
  }
}
