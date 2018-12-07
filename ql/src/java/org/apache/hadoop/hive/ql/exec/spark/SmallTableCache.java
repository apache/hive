/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.spark;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.apache.commons.lang3.math.NumberUtils.INTEGER_ONE;
import static org.apache.commons.lang3.math.NumberUtils.INTEGER_ZERO;

public class SmallTableCache {
  private static final Logger LOG = LoggerFactory.getLogger(SmallTableCache.class);

  private static final SmallTableLocalCache<String, MapJoinTableContainer>
          TABLE_CONTAINER_CACHE = new SmallTableLocalCache();

  private static volatile String queryId;

  /**
   * Check if this is a new query. If so, clean up the cache
   * that is for the previous query, and reset the current query id.
   */
  public static void initialize(Configuration conf) {
    String currentQueryId = conf.get(HiveConf.ConfVars.HIVEQUERYID.varname);
    if (!currentQueryId.equals(queryId)) {
      if (TABLE_CONTAINER_CACHE.size() != 0) {
        synchronized (TABLE_CONTAINER_CACHE) {
          if (!currentQueryId.equals(queryId) && TABLE_CONTAINER_CACHE.size() != 0) {
            TABLE_CONTAINER_CACHE.clear((path, tableContainer) -> tableContainer.clear());
            LOG.debug("Cleaned up small table cache for query {}", queryId);
          }
        }
      }
      queryId = currentQueryId;
    }
  }

  public static void cache(String key, MapJoinTableContainer tableContainer) {
    TABLE_CONTAINER_CACHE.put(key, tableContainer);
  }

  public static MapJoinTableContainer get(String key, Callable<MapJoinTableContainer> valueLoader)
          throws ExecutionException {
    return TABLE_CONTAINER_CACHE.get(key, valueLoader);
  }

  /**
   * Two level cache implementation. The level 1 cache keeps the cached values until 30 seconds,
   * the level 2 cache keeps the values (by using soft references) until the GC decides, that it
   * needs the memory occupied by the cache.
   *
   * @param <K> the type of the key
   * @param <V> the type of the value
   */
  @VisibleForTesting
  static class SmallTableLocalCache<K, V> {

    private static final int MAINTENANCE_THREAD_CLEANUP_PERIOD = 10;
    private static final int L1_CACHE_EXPIRE_DURATION = 30;

    private final Cache<K, V> cacheL1;
    private final Cache<K, V> cacheL2;
    private final ScheduledExecutorService cleanupService;

    SmallTableLocalCache() {
      this(Ticker.systemTicker());
    }

    @VisibleForTesting
    SmallTableLocalCache(Ticker ticker) {
      cleanupService = Executors.newScheduledThreadPool(INTEGER_ONE,
              new ThreadFactoryBuilder().setNameFormat("SmallTableCache Cleanup Thread").setDaemon(true).build());
      cacheL1 = CacheBuilder.newBuilder().expireAfterAccess(L1_CACHE_EXPIRE_DURATION, TimeUnit.SECONDS)
              .ticker(ticker).build();
      cacheL2 = CacheBuilder.newBuilder().softValues().build();
      cleanupService.scheduleAtFixedRate(() -> {
        cleanup();
      }, INTEGER_ZERO, MAINTENANCE_THREAD_CLEANUP_PERIOD, TimeUnit.SECONDS);
    }

    /**
     * Return the number of cached elements.
     */
    // L2 >= L1, because if a cached item is in L1 then its in L2 as well.
    public long size() {
      return cacheL2.size();
    }

    /**
     * Invalidate the cache, and call the action on the elements, if additional cleanup is required.
     */
    public void clear(BiConsumer<? super K, ? super V> action) {
      cacheL1.invalidateAll();
      cacheL2.asMap().forEach(action);
      cacheL2.invalidateAll();
    }

    @VisibleForTesting
    void cleanup() {
      cacheL1.cleanUp();
    }

    /**
     * Put an item into the cache. If the item was already there, it will be overwritten.
     */
    public void put(K key, V value) {
      cacheL2.put(key, value);
      cacheL1.put(key, value);
    }

    /**
     * Retrieves an item from the cache, and if its not there, it will use the valueLoader to load it and cache it.
     */
    public V get(K key, Callable<V> valueLoader) throws ExecutionException {
      return cacheL1.get(key, () -> cacheL2.get(key, valueLoader));
    }
  }
}
