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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Caching for stats.  This implements an LRU cache.   It does not remove entries explicitly as
 * that is generally expensive to find all entries for a table or partition.  Instead it lets them
 * time out. When the cache is full a sweep is done in the background to remove expired entries.
 * This cache is shared across all threads, and so operations are protected by reader or writer
 * locks as appropriate.
 */
class StatsCache  {
  static final private Log LOG = LogFactory.getLog(StatsCache.class.getName());

  private static StatsCache self = null;

  private final long timeToLive;
  private final int maxSize;
  private Map<Key, StatsInfo> cache;
  private ReadWriteLock lock;
  private boolean cleaning;
  private Counter tableMisses;
  private Counter partMisses;
  private Counter tableHits;
  private Counter partHits;
  private Counter cleans;
  private List<Counter> counters;

  static synchronized StatsCache getInstance(Configuration conf) {
    if (self == null) {
      int totalObjectsToCache =
          HiveConf.getIntVar(conf, HiveConf.ConfVars.METASTORE_HBASE_CACHE_SIZE);
      long timeToLive = HiveConf.getTimeVar(conf,
          HiveConf.ConfVars.METASTORE_HBASE_CACHE_TIME_TO_LIVE, TimeUnit.SECONDS);
      self = new StatsCache(totalObjectsToCache / 2, timeToLive);
    }
    return self;
  }

  /**
   * @param max maximum number of objects to store in the cache.  When max is reached, eviction
   *            policy is MRU.
   * @param timeToLive time (in seconds) that an entry is valid.  After this time the record will
   *                   discarded lazily
   */
  private StatsCache(int max, long timeToLive) {
    maxSize = max;
    this.timeToLive = timeToLive * 1000;
    cache = new HashMap<Key, StatsInfo>();
    lock = new ReentrantReadWriteLock();
    cleaning = false;
    counters = new ArrayList<Counter>();
    tableMisses = new Counter("Stats cache table misses");
    counters.add(tableMisses);
    tableHits = new Counter("Stats cache table hits");
    counters.add(tableHits);
    partMisses = new Counter("Stats cache partition misses");
    counters.add(partMisses);
    partHits = new Counter("Stats cache partition hits");
    counters.add(partHits);
    cleans = new Counter("Stats cache cleans");
    counters.add(cleans);
  }

  /**
   * Add an object to the cache.
   * @param dbName name of database table is in
   * @param tableName name of table
   * @param partName name of partition, can be null if these are table level statistics
   * @param colName name of the column these statistics are for
   * @param stats stats
   * @param lastAnalyzed last time these stats were analyzed
   */
  void put(String dbName, String tableName, String partName, String colName,
           ColumnStatisticsObj stats, long lastAnalyzed) {
    // TODO - we may want to not put an entry in if we're full.
    if (cache.size() >= maxSize) clean();
    Key key = new Key(dbName, tableName, partName, colName);
    StatsInfo info = new StatsInfo(stats, lastAnalyzed);
    lock.writeLock().lock();
    try {
      cache.put(key, info);
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Get table level statistics
   * @param dbName name of database table is in
   * @param tableName name of table
   * @param colName of column to get stats for
   * @return stats object for this column, or null if none cached
   */
  StatsInfo getTableStatistics(String dbName, String tableName, String colName) {
    return getStatistics(new Key(dbName, tableName, colName), tableHits, tableMisses);
  }

  /**
   * Get partition level statistics
   * @param dbName name of database table is in
   * @param tableName name of table
   * @param partName name of this partition
   * @param colName of column to get stats for
   * @return stats object for this column, or null if none cached
   */
  StatsInfo getPartitionStatistics(String dbName, String tableName,
                                             String partName, String colName) {
    return getStatistics(new Key(dbName, tableName, partName, colName), partHits, partMisses);
  }

  String[] dumpMetrics() {
    String[] strs = new String[counters.size()];
    for (int i = 0; i < strs.length; i++) {
      strs[i] = counters.get(i).dump();
    }
    return strs;
  }

  private StatsInfo getStatistics(Key key, Counter hits, Counter misses) {
    StatsInfo s = null;
    lock.readLock().lock();
    try {
      s = cache.get(key);
    } finally {
      lock.readLock().unlock();
    }
    if (s == null) {
      misses.incr();
      return null;
    } else if (tooLate(s)) {
      remove(key);
      misses.incr();
      return null;
    } else {
      s.lastTouched = System.currentTimeMillis();
      hits.incr();
      return s;
    }
  }

  private void remove(Key key) {
    lock.writeLock().lock();
    try {
      // It's possible that multiple callers will call remove for a given key, so don't complain
      // if the indicated key is already gone.
      cache.remove(key);
    } finally {
      lock.writeLock().unlock();
    }
  }

  private void clean() {
    // TODO - we may want to add intelligence to check if we cleaned anything after a cleaning
    // pass.  If we're still at or near capacity we may want to reduce ttl and make another run.
    // This spawns a separate thread to walk through the cache and clean.
    synchronized (this) {
      if (cleaning) return;
      cleaning = true;
      cleans.incr();
    }

    Thread cleaner = new Thread() {
      @Override
      public void run() {
        try {
          // Get the read lock and then make a copy of the map.  This is so we can work through it
          // without having concurrent modification exceptions.  Then walk through and remove things
          // one at a time.
          List<Map.Entry<Key, StatsInfo>> entries = null;
          lock.readLock().lock();
          try {
            entries = new ArrayList<Map.Entry<Key, StatsInfo>>(cache.entrySet());
          } finally {
            lock.readLock().unlock();
          }
          for (Map.Entry<Key, StatsInfo> entry : entries) {
            if (tooLate(entry.getValue())) {
              remove(entry.getKey());
            }
            // We want to make sure this runs at a low priority in the background
            Thread.yield();
          }
        } catch (Throwable t) {
          // Don't let anything past this thread that could end up killing the metastore
          LOG.error("Caught exception in stats cleaner thread", t);
        } finally {
          cleaning = false;
        }
      }
    };
    cleaner.setPriority(Thread.MIN_PRIORITY);
    cleaner.setDaemon(true);
    cleaner.start();
  }

  private boolean tooLate(StatsInfo stats) {
    return System.currentTimeMillis() - stats.lastTouched > timeToLive;
  }

  private static class Key {
    private final String dbName, tableName, partName, colName;

    Key(String db, String table, String col) {
      this(db, table, null, col);
    }

    Key(String db, String table, String part, String col) {
      dbName = db; tableName = table; partName = part; colName = col;
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof Key)) return false;
      Key that = (Key)other;
      if (partName == null) {
        return that.partName == null && dbName.equals(that.dbName) &&
            tableName.equals(that.tableName) && colName.equals(that.colName);
      } else {
        return dbName.equals(that.dbName) && tableName.equals(that.tableName) &&
            partName.equals(that.partName) && colName.equals(that.colName);
      }
    }

    @Override
    public int hashCode() {
      int hashCode = dbName.hashCode() * 31 + tableName.hashCode();
      if (partName != null) hashCode = hashCode * 31 + partName.hashCode();
      return hashCode * 31 + colName.hashCode();
    }
  }

  static class StatsInfo {
    final ColumnStatisticsObj stats;
    final long lastAnalyzed;
    long lastTouched;

    StatsInfo(ColumnStatisticsObj obj, long la) {
      stats = obj;
      lastAnalyzed = la;
      lastTouched = System.currentTimeMillis();
    }
  }


  /**
   * This returns a stats cache that will store nothing and return nothing, useful
   * for unit testing when you don't want the cache in your way.
   * @return
   */
  @VisibleForTesting
  static StatsCache getBogusStatsCache() {
    return new StatsCache(0, 0) {
      @Override
      void put(String dbName, String tableName, String partName, String colName,
               ColumnStatisticsObj stats, long lastAnalyzed) {
      }

      @Override
      StatsInfo getTableStatistics(String dbName, String tableName, String colName) {
        return null;
      }

      @Override
      StatsInfo getPartitionStatistics(String dbName, String tableName,
                                                 String partName, String colName) {
        return null;
      }
    };
  }

  /**
   * Go through and make all the entries in the cache old so they will time out when requested
   */
  @VisibleForTesting
  void makeWayOld() {
    for (StatsInfo stats : cache.values()) {
      stats.lastTouched = 1;
    }
  }

  @VisibleForTesting
  int cacheSize() {
    return cache.size();
  }

  @VisibleForTesting
  boolean cleaning() {
    return cleaning;
  }

  @VisibleForTesting
  void clear() {
    cache.clear();
  }
}
