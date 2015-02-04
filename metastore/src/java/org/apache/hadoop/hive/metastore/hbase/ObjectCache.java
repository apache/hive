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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.hbase;

import java.util.HashMap;
import java.util.Map;

/**
 * A generic class for caching objects obtained from HBase.  Currently a set of
 * convenience methods around a {@link java.util.HashMap} with a max size but built
 * as a separate class in case we want to switch out the implementation to something more
 * efficient.  The cache has a max size; when this is exceeded any additional entries are dropped
 * on the floor.
 *
 * This cache is local to a particular thread and thus is not synchronized.  It is intended to be
 * flushed before a query begins to make sure it doesn't carry old versions of objects between
 * queries (that is, an object may have changed between two queries, we want to get the newest
 * version).
 */
class ObjectCache<K, V> {
  private Map<K, V> cache;
  private final int maxSize;
  private Counter hits;
  private Counter misses;
  private Counter overflows;

  /**
   *
   * @param max maximum number of objects to store in the cache.  When max is reached, eviction
   *            policy is MRU.
   * @param hits counter to increment when we find an element in the cache
   * @param misses counter to increment when we do not find an element in the cache
   * @param overflows counter to increment when we do not have room for an element in the cache
   */
  ObjectCache(int max, Counter hits, Counter misses, Counter overflows) {
    maxSize = max;
    cache = new HashMap<K, V>();
    this.hits = hits;
    this.misses = misses;
    this.overflows = overflows;
  }

  void put(K key, V value) {
    if (cache.size() < maxSize) {
      cache.put(key, value);
    } else {
      overflows.incr();
    }
  }

  V get(K key) {
    V val = cache.get(key);
    if (val == null) misses.incr();
    else hits.incr();
    return val;
  }

  void remove(K key) {
    cache.remove(key);
  }

  void flush() {
    cache.clear();
  }
}
