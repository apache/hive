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

package org.apache.iceberg.rest;

/**
 * JMX MXBean interface for {@link HMSCachingCatalog} that exposes cache performance statistics.
 * <p>
 * Instances are registered under the object name:
 * {@code org.apache.iceberg.rest:type=HMSCachingCatalog,name=&lt;catalogName&gt;}.
 * </p>
 */
public interface HMSCachingCatalogMXBean {

  /**
   * Returns the total number of cache hits (table found in cache and still valid).
   *
   * @return cache hit count
   */
  long getCacheHitCount();

  /**
   * Returns the total number of cache misses (table not found in cache).
   *
   * @return cache miss count
   */
  long getCacheMissCount();

  /**
   * Returns the total number of times a table was loaded from the underlying catalog and stored in cache.
   *
   * @return cache load count
   */
  long getCacheLoadCount();

  /**
   * Returns the total number of times a cached table was invalidated because the actual metadata location differed.
   *
   * @return cache invalidation count
   */
  long getCacheInvalidateCount();

  /**
   * Returns the total number of times a metadata (virtual) table was loaded and cached.
   *
   * @return cache metadata-table load count
   */
  long getCacheMetaLoadCount();

  /**
   * Returns the cache hit rate as a value in the range {@code [0.0, 1.0]}.
   * Returns {@code 0.0} when no lookups have been performed.
   *
   * @return cache hit rate
   */
  double getCacheHitRate();

  /**
   * Returns the total number of L1 (short-lived in-memory) cache hits.
   * An L1 hit means the table was found in the L2 cache <em>and</em> its L1 TTL had not yet expired,
   * so the HMS metadata-location check was skipped entirely.
   *
   * @return L1 cache hit count
   */
  long getL1CacheHitCount();

  /**
   * Returns the total number of L1 cache misses.
   * An L1 miss means the table was in the L2 cache but the L1 entry was absent or expired,
   * so an HMS metadata-location check was required.
   *
   * @return L1 cache miss count
   */
  long getL1CacheMissCount();

  /**
   * Returns the L1 cache hit rate as a value in the range {@code [0.0, 1.0]}.
   * This reflects how often the short-circuit L1 path is taken vs. the full HMS location check.
   * Returns {@code 0.0} when no L2-cache-hit lookups have been performed.
   *
   * @return L1 cache hit rate
   */
  double getL1CacheHitRate();

  /**
   * Resets all cache statistics counters to zero.
   */
  void resetCacheStats();
}
