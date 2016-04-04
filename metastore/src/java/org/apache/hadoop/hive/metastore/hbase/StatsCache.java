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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.ByteString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.HiveStatsUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.hbase.stats.ColumnStatsAggregator;
import org.apache.hadoop.hive.metastore.hbase.stats.ColumnStatsAggregatorFactory;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * A cache for stats.  This is only intended for use by
 * {@link org.apache.hadoop.hive.metastore.hbase.HBaseReadWrite} and should not be used outside
 * that class.
 */
class StatsCache {

  private static final Logger LOG = LoggerFactory.getLogger(StatsCache.class.getName());
  private static StatsCache self = null;

  private LoadingCache<StatsCacheKey, AggrStats> cache;
  private Invalidator invalidator;
  private long runInvalidatorEvery;
  private long maxTimeInCache;
  private boolean invalidatorHasRun;

  @VisibleForTesting Counter misses;
  @VisibleForTesting Counter hbaseHits;
  @VisibleForTesting Counter totalGets;

  static synchronized StatsCache getInstance(Configuration conf) {
    if (self == null) {
      self = new StatsCache(conf);
    }
    return self;
  }

  private StatsCache(final Configuration conf) {
    final StatsCache me = this;
    cache = CacheBuilder.newBuilder()
        .maximumSize(
            HiveConf.getIntVar(conf, HiveConf.ConfVars.METASTORE_HBASE_AGGR_STATS_CACHE_ENTRIES))
        .expireAfterWrite(HiveConf.getTimeVar(conf,
            HiveConf.ConfVars.METASTORE_HBASE_AGGR_STATS_MEMORY_TTL, TimeUnit.SECONDS), TimeUnit.SECONDS)
        .build(new CacheLoader<StatsCacheKey, AggrStats>() {
          @Override
          public AggrStats load(StatsCacheKey key) throws Exception {
            int numBitVectors = HiveStatsUtils.getNumBitVectorsForNDVEstimation(conf);
            boolean useDensityFunctionForNDVEstimation = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_METASTORE_STATS_NDV_DENSITY_FUNCTION);
            HBaseReadWrite hrw = HBaseReadWrite.getInstance();
            AggrStats aggrStats = hrw.getAggregatedStats(key.hashed);
            if (aggrStats == null) {
              misses.incr();
              ColumnStatsAggregator aggregator = null;
              aggrStats = new AggrStats();
              LOG.debug("Unable to find aggregated stats for " + key.colName + ", aggregating");
              List<ColumnStatistics> css = hrw.getPartitionStatistics(key.dbName, key.tableName,
                  key.partNames, HBaseStore.partNameListToValsList(key.partNames),
                  Collections.singletonList(key.colName));
              if (css != null && css.size() > 0) {
                aggrStats.setPartsFound(css.size());
                if (aggregator == null) {
                  aggregator = ColumnStatsAggregatorFactory.getColumnStatsAggregator(css.iterator()
                      .next().getStatsObj().iterator().next().getStatsData().getSetField(),
                      numBitVectors, useDensityFunctionForNDVEstimation);
                }
                ColumnStatisticsObj statsObj = aggregator
                    .aggregate(key.colName, key.partNames, css);
                aggrStats.addToColStats(statsObj);
                me.put(key, aggrStats);
              }
            } else {
              hbaseHits.incr();
            }
            return aggrStats;
          }
        });
    misses = new Counter("Stats cache table misses");
    hbaseHits = new Counter("Stats cache table hits");
    totalGets = new Counter("Total get calls to the stats cache");

    maxTimeInCache = HiveConf.getTimeVar(conf,
        HiveConf.ConfVars.METASTORE_HBASE_AGGR_STATS_HBASE_TTL, TimeUnit.SECONDS);
    // We want runEvery in milliseconds, even though we give the default value in the conf in
    // seconds.
    runInvalidatorEvery = HiveConf.getTimeVar(conf,
        HiveConf.ConfVars.METASTORE_HBASE_AGGR_STATS_INVALIDATOR_FREQUENCY, TimeUnit.MILLISECONDS);

    invalidator = new Invalidator();
    invalidator.setDaemon(true);
    invalidator.start();
  }

  /**
   * Add an object to the cache.
   * @param key Key for this entry
   * @param aggrStats stats
   * @throws java.io.IOException
   */
  void put(StatsCacheKey key, AggrStats aggrStats) throws IOException {
    HBaseReadWrite.getInstance().putAggregatedStats(key.hashed, key.dbName, key.tableName,
        key.partNames,
        key.colName, aggrStats);
    cache.put(key, aggrStats);
  }

  /**
   * Get partition level statistics
   * @param dbName name of database table is in
   * @param tableName name of table
   * @param partNames names of the partitions
   * @param colName of column to get stats for
   * @return stats object for this column, or null if none cached
   * @throws java.io.IOException
   */

  AggrStats get(String dbName, String tableName, List<String> partNames, String colName)
      throws IOException {
    totalGets.incr();
    StatsCacheKey key = new StatsCacheKey(dbName, tableName, partNames, colName);
    try {
      return cache.get(key);
    } catch (ExecutionException e) {
      throw new IOException(e);
    }
  }

  /**
   * Remove all entries that are related to a particular set of partitions.  This should be
   * called when partitions are deleted or stats are updated.
   * @param dbName name of database table is in
   * @param tableName name of table
   * @param partName name of the partition
   * @throws IOException
   */
  void invalidate(String dbName, String tableName, String partName)
      throws IOException {
    invalidator.addToQueue(
        HbaseMetastoreProto.AggrStatsInvalidatorFilter.Entry.newBuilder()
            .setDbName(ByteString.copyFrom(dbName.getBytes(HBaseUtils.ENCODING)))
            .setTableName(ByteString.copyFrom(tableName.getBytes(HBaseUtils.ENCODING)))
            .setPartName(ByteString.copyFrom(partName.getBytes(HBaseUtils.ENCODING)))
            .build());
  }

  void dumpCounters() {
    LOG.debug(misses.dump());
    LOG.debug(hbaseHits.dump());
    LOG.debug(totalGets.dump());
  }

  /**
   * Completely dump the cache from memory, used to test that we can access stats from HBase itself.
   * @throws IOException
   */
  @VisibleForTesting void flushMemory() throws IOException {
    cache.invalidateAll();
  }

  @VisibleForTesting void resetCounters() {
    misses.clear();
    hbaseHits.clear();
    totalGets.clear();
  }

  @VisibleForTesting void setRunInvalidatorEvery(long runEvery) {
    runInvalidatorEvery = runEvery;
  }

  @VisibleForTesting void setMaxTimeInCache(long maxTime) {
    maxTimeInCache = maxTime;
  }

  @VisibleForTesting void wakeInvalidator() throws InterruptedException {
    invalidatorHasRun = false;
    // Wait through 2 cycles so we're sure our entry won't be picked as too new.
    Thread.sleep(2 * runInvalidatorEvery);
    invalidator.interrupt();
    while (!invalidatorHasRun) {
      Thread.sleep(10);
    }
  }

  static class StatsCacheKey {
    final byte[] hashed;
    String dbName;
    String tableName;
    List<String> partNames;
    String colName;
    private MessageDigest md;

    StatsCacheKey(byte[] key) {
      hashed = key;
    }

    StatsCacheKey(String dbName, String tableName, List<String> partNames, String colName) {
      this.dbName = dbName;
      this.tableName = tableName;
      this.partNames = partNames;
      this.colName = colName;

      try {
        md = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new RuntimeException(e);
      }
      md.update(dbName.getBytes(HBaseUtils.ENCODING));
      md.update(tableName.getBytes(HBaseUtils.ENCODING));
      Collections.sort(this.partNames);
      for (String s : partNames) {
        md.update(s.getBytes(HBaseUtils.ENCODING));
      }
      md.update(colName.getBytes(HBaseUtils.ENCODING));
      hashed = md.digest();
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof StatsCacheKey)) return false;
      StatsCacheKey that = (StatsCacheKey)other;
      return Arrays.equals(hashed, that.hashed);
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(hashed);
    }
  }

  private class Invalidator extends Thread {
    private List<HbaseMetastoreProto.AggrStatsInvalidatorFilter.Entry> entries = new ArrayList<>();
    private Lock lock = new ReentrantLock();

    void addToQueue(HbaseMetastoreProto.AggrStatsInvalidatorFilter.Entry entry) {
      lock.lock();
      try {
        entries.add(entry);
      } finally {
        lock.unlock();
      }
    }

    @Override
    public void run() {
      while (true) {
        long startedAt = System.currentTimeMillis();
        List<HbaseMetastoreProto.AggrStatsInvalidatorFilter.Entry> thisRun = null;
        lock.lock();
        try {
          if (entries.size() > 0) {
            thisRun = entries;
            entries = new ArrayList<>();
          }
        } finally {
          lock.unlock();
        }

        if (thisRun != null) {
          try {
            HbaseMetastoreProto.AggrStatsInvalidatorFilter filter =
                HbaseMetastoreProto.AggrStatsInvalidatorFilter.newBuilder()
                .setRunEvery(runInvalidatorEvery)
                .setMaxCacheEntryLife(maxTimeInCache)
                .addAllToInvalidate(thisRun)
                .build();
            List<StatsCacheKey> keys =
                HBaseReadWrite.getInstance().invalidateAggregatedStats(filter);
            cache.invalidateAll(keys);
          } catch (IOException e) {
            // Not a lot I can do here
            LOG.error("Caught error while invalidating entries in the cache", e);
          }
        }
        invalidatorHasRun = true;

        try {
          sleep(runInvalidatorEvery - (System.currentTimeMillis() - startedAt));
        } catch (InterruptedException e) {
          LOG.warn("Interupted while sleeping", e);
        }
      }
    }
  }
}
