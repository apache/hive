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

import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.metastore.api.Partition;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A cache for partition objects.  This is separate from
 * {@link org.apache.hadoop.hive.metastore.hbase.ObjectCache} because we need to access it
 * differently (always by table) and because we need to be able to track whether we are caching
 * all of the partitions for a table or not.  Like ObjectCache it is local to a particular thread
 * and thus not synchronized.  Also like ObjectCache it is intended to be flushed before each query.
 */
class PartitionCache {
  // This is a trie.  The key to the first map is (dbname, tablename), since partitions are
  // always accessed within the context of the table they belong to.  The second map maps
  // partition values (not names) to partitions.
  private Map<ObjectPair<String, String>, TrieValue> cache;
  private final int maxSize;
  private int cacheSize;
  private Counter misses;
  private Counter hits;
  private Counter overflows;

  /**
   *
   * @param max maximum number of objects to store in the cache.  When max is reached, eviction
   *            policy is MRU.
   * @param hits counter to increment when we find an element in the cache
   * @param misses counter to increment when we do not find an element in the cache
   * @param overflows counter to increment when we do not have room for an element in the cache
   */
  PartitionCache(int max, Counter hits, Counter misses, Counter overflows) {
    maxSize = max;
    cache = new HashMap<ObjectPair<String, String>, TrieValue>();
    cacheSize = 0;
    this.hits = hits;
    this.misses = misses;
    this.overflows = overflows;
  }

  /**
   * Put a single partition into the cache
   * @param dbName
   * @param tableName
   * @param part
   */
  void put(String dbName, String tableName, Partition part) {
    if (cacheSize < maxSize) {
      ObjectPair<String, String> key = new ObjectPair<String, String>(dbName, tableName);
      TrieValue entry = cache.get(key);
      if (entry == null) {
        entry = new TrieValue(false);
        cache.put(key, entry);
      }
      entry.map.put(part.getValues(), part);
      cacheSize++;
    } else {
      overflows.incr();
    }
  }

  /**
   *
   * @param dbName
   * @param tableName
   * @param parts
   * @param allForTable if true indicates that all partitions for this table are present
   */
  void put(String dbName, String tableName, List<Partition> parts, boolean allForTable) {
    if (cacheSize + parts.size() < maxSize) {
      ObjectPair<String, String> key = new ObjectPair<String, String>(dbName, tableName);
      TrieValue entry = cache.get(key);
      if (entry == null) {
        entry = new TrieValue(allForTable);
        cache.put(key, entry);
      }
      for (Partition part : parts) entry.map.put(part.getValues(), part);
      cacheSize += parts.size();
    } else {
      overflows.incr();
    }
  }

  /**
   * Will only return a value if all partitions for this table are in the cache.  Otherwise you
   * should call {@link #get} individually
   * @param dbName
   * @param tableName
   * @return
   */
  Collection<Partition> getAllForTable(String dbName, String tableName) {
    TrieValue entry = cache.get(new ObjectPair<String, String>(dbName, tableName));
    if (entry != null && entry.hasAllPartitionsForTable) {
      hits.incr();
      return entry.map.values();
    } else {
      misses.incr();
      return null;
    }
  }

  Partition get(String dbName, String tableName, List<String> partVals) {
    TrieValue entry = cache.get(new ObjectPair<String, String>(dbName, tableName));
    if (entry != null) {
      hits.incr();
      return entry.map.get(partVals);
    } else {
      misses.incr();
      return null;
    }
  }

  void remove(String dbName, String tableName) {
    ObjectPair<String, String> key = new ObjectPair<String, String>(dbName, tableName);
    TrieValue entry = cache.get(key);
    if (entry != null) {
      cacheSize -= entry.map.size();
      cache.remove(key);
    }
  }

  void remove(String dbName, String tableName, List<String> partVals) {
    ObjectPair<String, String> key = new ObjectPair<String, String>(dbName, tableName);
    TrieValue entry = cache.get(key);
    if (entry != null && entry.map.remove(partVals) != null) {
      cacheSize--;
      entry.hasAllPartitionsForTable = false;
    }
  }

  void flush() {
    cache.clear();
    cacheSize = 0;
  }

  static class TrieValue {
    boolean hasAllPartitionsForTable;
    Map<List<String>, Partition> map;

    TrieValue(boolean hasAll) {
      hasAllPartitionsForTable = hasAll;
      map = new HashMap<List<String>, Partition>();
    }
  }
}
