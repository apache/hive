/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.rest.extension;

import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.rest.HMSCachingCatalog;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A test class for HMSCachingCatalog that tracks cache metrics for test verifications.
 * It extends HMSCachingCatalog and overrides methods to increment counters for cache hits,
 * misses, invalidations, and loads.
 */
public class HMSTestCachingCatalog extends HMSCachingCatalog {
  private final HiveCatalog hiveCatalog;
  protected final AtomicInteger cacheHitCount = new AtomicInteger(0);
  protected final AtomicInteger cacheMissCount = new AtomicInteger(0);
  protected final AtomicInteger cacheInvalidationCount = new AtomicInteger(0);
  protected final AtomicInteger cacheLoadCount = new AtomicInteger(0);

  /**
   * Constructor for HMSTestCachingCatalog.
   *
   * @param catalog the HiveCatalog to wrap
   * @param expiration the cache expiration time in milliseconds
   */
  public HMSTestCachingCatalog(HiveCatalog catalog, long expiration) {
    super(catalog, expiration);
    this.hiveCatalog = catalog;
  }

  public Map<String, Integer> getCacheMetrics() {
    return Map.of(
            "hit", cacheHitCount.get(),
            "miss", cacheMissCount.get(),
            "invalidation", cacheInvalidationCount.get(),
            "load", cacheLoadCount.get()
    );
  }

  public HiveCatalog getHiveCatalog() {
    return hiveCatalog;
  }

  @Override
  protected void cacheInvalidateInc(TableIdentifier tid) {
    cacheInvalidationCount.incrementAndGet();
  }

  @Override
  protected void cacheLoadInc(TableIdentifier tid) {
    cacheLoadCount.incrementAndGet();
  }

  @Override
  protected void cacheHitInc(TableIdentifier tid) {
    cacheHitCount.incrementAndGet();
  }

  @Override
  protected void cacheMissInc(TableIdentifier tid) {
    cacheMissCount.incrementAndGet();
  }
}
