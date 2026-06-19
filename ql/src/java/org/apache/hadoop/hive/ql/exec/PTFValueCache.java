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

package org.apache.hadoop.hive.ql.exec;

import java.util.Arrays;
import java.util.Comparator;
import java.util.TreeMap;

import org.apache.hadoop.hive.ql.udf.ptf.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache for storing aggregated values that were already calculated within a partition - used for
 * PTF functions. This cache can lead to performance improvement when PTFPartition would have to
 * calculate a particular aggregation for the same range again and again. The caching relies on the
 * fact that evaluators are deterministic in a sense that they'll give the same result for the same
 * range in the same partition (no matter for which row is the current target).
 */
public class PTFValueCache {
  protected static Logger LOG = LoggerFactory.getLogger(PTFPartition.class);

  private int maxSize;
  private int maxSizePerEvaluator;
  private TreeMap<Range, Object>[] cache;
  private CacheHandler cacheHandler;

  private interface CacheHandler {
    Object get(int evaluatorIndex, Range range);

    void put(int evaluatorIndex, Range range, Object value);

    String getStatistics();

    default void clear() {
    }
  }

  class BaseCacheHandler implements CacheHandler {
    public void put(int evaluatorIndex, Range range, Object value) {
      cache[evaluatorIndex].put(range, value);
      if (cache[evaluatorIndex].size() > maxSizePerEvaluator) {
        cache[evaluatorIndex].pollFirstEntry();
      }
    }

    public Object get(int evaluatorIndex, Range range) {
      return cache[evaluatorIndex].get(range);
    }

    @Override
    public String getStatistics() {
      return null;
    }
  }

  class InstrumentedCacheHandler implements CacheHandler {
    int put = 0;
    int hit = 0;
    int miss = 0;
    int evict = 0;

    public void put(int evaluatorIndex, Range range, Object value) {
      cache[evaluatorIndex].put(range, value);
      put += 1;
      if (cache[evaluatorIndex].size() > maxSizePerEvaluator) {
        cache[evaluatorIndex].pollFirstEntry();
        evict += 1;
      }
    }

    public Object get(int evaluatorIndex, Range range) {
      if (cache[evaluatorIndex].containsKey(range)) {
        hit += 1;
      } else {
        miss += 1;
      }
      return cache[evaluatorIndex].get(range);
    }

    @Override
    public String getStatistics() {
      return String.format(
          "PTFValueCache.InstrumentedCacheHandler statistics: put: %d, evict: %d, hit: %d, miss: %d",
          put, evict, hit, miss);
    }

    @Override
    public void clear() {
      put = 0;
      hit = 0;
      miss = 0;
      evict = 0;
    }
  }

  public PTFValueCache(int maxSize) {
    this(maxSize, false);
  }

  public PTFValueCache(int maxSize, boolean collectStatistics) {
    if (maxSize <= 1) {
      throw new IllegalArgumentException("Cache size of 1 and below doesn't make sense.");
    }
    this.maxSize = maxSize;
    cacheHandler = collectStatistics ? new InstrumentedCacheHandler() : new BaseCacheHandler();
  }

  @SuppressWarnings("unchecked")
  public PTFValueCache init(int length) {
    cache = new TreeMap[length];
    for (int i = 0; i < length; i++) {
      cache[i] = new TreeMap<>(new Comparator<Range>() {
        @Override
        public int compare(Range r1, Range r2) {
          return r1.compareTo(r2);
        }
      });
    }
    maxSizePerEvaluator = (int) Math.ceil(maxSize / length);
    LOG.info("PTFValueCache is initialized to {} evaluators, maxSize: {} ({} entries / evaluator)",
        length, maxSize, maxSizePerEvaluator);
    return this;
  }

  public void put(int evaluatorIndex, Range range, Object value) {
    cacheHandler.put(evaluatorIndex, range, value);
  }

  public Object get(int evaluatorIndex, Range range) {
    return cacheHandler.get(evaluatorIndex, range);
  }

  public String getStatistics() {
    return cacheHandler.getStatistics();
  }

  public int size() {
    return Arrays.asList(cache).stream().mapToInt(TreeMap::size).sum();
  }

  public void clear() {
    for (int i = 0; i < cache.length; i++) {
      cache[i].clear();
    }
    cacheHandler.clear();
  }
}
