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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Function;

/** Class used for a single file in LowLevelCacheImpl, etc. */
class FileCache<T> {
  private static final int EVICTED_REFCOUNT = -1, EVICTING_REFCOUNT = -2;
  private final T cache;
  private final AtomicInteger refCount = new AtomicInteger(0);

  private FileCache(T value) {
    this.cache = value;
  }

  public T getCache() {
    return cache;
  }

  boolean incRef() {
    while (true) {
      int value = refCount.get();
      if (value == EVICTED_REFCOUNT) return false;
      if (value == EVICTING_REFCOUNT) continue; // spin until it resolves; extremely rare
      assert value >= 0;
      if (refCount.compareAndSet(value, value + 1)) return true;
    }
  }

  void decRef() {
    int value = refCount.decrementAndGet();
    if (value < 0) {
      throw new AssertionError("Unexpected refCount " + value);
    }
  }

  boolean startEvicting() {
    while (true) {
      int value = refCount.get();
      if (value != 1) return false;
      if (refCount.compareAndSet(value, EVICTING_REFCOUNT)) return true;
    }
  }

  void commitEvicting() {
    boolean result = refCount.compareAndSet(EVICTING_REFCOUNT, EVICTED_REFCOUNT);
    assert result;
  }

  void abortEvicting() {
    boolean result = refCount.compareAndSet(EVICTING_REFCOUNT, 0);
    assert result;
  }

  /**
   * All this mess is necessary because we want to be able to remove sub-caches for fully
   * evicted files. It may actually be better to have non-nested map with object keys?
   */
  public static <T> FileCache<T> getOrAddFileSubCache(
      ConcurrentHashMap<Object, FileCache<T>> cache, Object fileKey,
      Function<Void, T> createFunc) {
    FileCache<T> newSubCache = null;
    while (true) { // Overwhelmingly executes once.
      FileCache<T> subCache = cache.get(fileKey);
      if (subCache != null) {
        if (subCache.incRef()) return subCache; // Main path - found it, incRef-ed it.
        if (newSubCache == null) {
          newSubCache = new FileCache<T>(createFunc.apply(null));
          newSubCache.incRef();
        }
        // Found a stale value we cannot incRef; try to replace it with new value.
        if (cache.replace(fileKey, subCache, newSubCache)) return newSubCache;
        continue; // Someone else replaced/removed a stale value, try again.
      }
      // No value found.
      if (newSubCache == null) {
        newSubCache = new FileCache<T>(createFunc.apply(null));
        newSubCache.incRef();
      }
      FileCache<T> oldSubCache = cache.putIfAbsent(fileKey, newSubCache);
      if (oldSubCache == null) return newSubCache; // Main path 2 - created a new file cache.
      if (oldSubCache.incRef()) return oldSubCache; // Someone created one in parallel.
      // Someone created one in parallel and then it went stale.
      if (cache.replace(fileKey, oldSubCache, newSubCache)) return newSubCache;
      // Someone else replaced/removed a parallel-added stale value, try again. Max confusion.
    }
  }
}