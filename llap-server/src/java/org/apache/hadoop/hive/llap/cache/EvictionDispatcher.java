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

import org.apache.hadoop.hive.llap.cache.SerDeLowLevelCacheImpl.LlapSerDeDataBuffer;
import org.apache.hadoop.hive.llap.io.metadata.OrcFileEstimateErrors;
import org.apache.hadoop.hive.llap.io.metadata.OrcFileMetadata;
import org.apache.hadoop.hive.llap.io.metadata.OrcMetadataCache;
import org.apache.hadoop.hive.llap.io.metadata.OrcStripeMetadata;

/**
 * Eviction dispatcher - uses double dispatch to route eviction notifications to correct caches.
 */
public final class EvictionDispatcher implements EvictionListener, LlapOomDebugDump {
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

  public void notifyEvicted(LlapSerDeDataBuffer buffer) {
    serdeCache.notifyEvicted(buffer);
    allocator.deallocateEvicted(buffer);
   
  }

  public void notifyEvicted(LlapDataBuffer buffer) {
    dataCache.notifyEvicted(buffer);
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

  @Override
  public String debugDumpForOom() {
    StringBuilder sb = new StringBuilder(dataCache.debugDumpForOom());
    if (serdeCache != null) {
      sb.append(serdeCache.debugDumpForOom());
    }
    if (metadataCache != null) {
      sb.append(metadataCache.debugDumpForOom());
    }
    return sb.toString();
  }

  @Override
  public void debugDumpShort(StringBuilder sb) {
    dataCache.debugDumpShort(sb);
    if (serdeCache != null) {
      serdeCache.debugDumpShort(sb);
    }
    if (metadataCache != null) {
      metadataCache.debugDumpShort(sb);
    }
  }
}
