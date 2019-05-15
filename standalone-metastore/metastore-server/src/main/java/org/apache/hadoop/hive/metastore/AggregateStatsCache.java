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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hive.common.util.BloomFilter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AggregateStatsCache {

  private static final Logger LOG = LoggerFactory.getLogger(AggregateStatsCache.class.getName());
  private static AggregateStatsCache self = null;

  // Backing store for this cache
  private final ConcurrentHashMap<Key, AggrColStatsList> cacheStore;
  // Cache size
  private final int maxCacheNodes;
  // Current nodes in the cache
  private final AtomicInteger currentNodes = new AtomicInteger(0);
  // Run the cleaner thread when the cache is maxFull% full
  private final double maxFull;
  // Run the cleaner thread until cache is cleanUntil% occupied
  private final double cleanUntil;
  // Nodes go stale after this
  private final long timeToLiveMs;
  // Max time when waiting for write locks on node list
  private final long maxWriterWaitTime;
  // Max time when waiting for read locks on node list
  private final long maxReaderWaitTime;
  // Maximum number of paritions aggregated per cache node
  private final int maxPartsPerCacheNode;
  // Bloom filter false positive probability
  private final double falsePositiveProbability;
  // Max tolerable variance for matches
  private final double maxVariance;
  // Used to determine if cleaner thread is already running
  private boolean isCleaning = false;
  private final AtomicLong cacheHits = new AtomicLong(0);
  private final AtomicLong cacheMisses = new AtomicLong(0);
  // To track cleaner metrics
  int numRemovedTTL = 0, numRemovedLRU = 0;

  private AggregateStatsCache(int maxCacheNodes, int maxPartsPerCacheNode, long timeToLiveMs,
      double falsePositiveProbability, double maxVariance, long maxWriterWaitTime,
      long maxReaderWaitTime, double maxFull, double cleanUntil) {
    this.maxCacheNodes = maxCacheNodes;
    this.maxPartsPerCacheNode = maxPartsPerCacheNode;
    this.timeToLiveMs = timeToLiveMs;
    this.falsePositiveProbability = falsePositiveProbability;
    this.maxVariance = maxVariance;
    this.maxWriterWaitTime = maxWriterWaitTime;
    this.maxReaderWaitTime = maxReaderWaitTime;
    this.maxFull = maxFull;
    this.cleanUntil = cleanUntil;
    this.cacheStore = new ConcurrentHashMap<>();
  }

  public static synchronized AggregateStatsCache getInstance(Configuration conf) {
    if (self == null) {
      int maxCacheNodes =
          MetastoreConf.getIntVar(conf, ConfVars.AGGREGATE_STATS_CACHE_SIZE);
      // The number of partitions aggregated per cache node
      // If the number of partitions requested is > this value, we'll fetch directly from Metastore
      int maxPartitionsPerCacheNode =
          MetastoreConf.getIntVar(conf, ConfVars.AGGREGATE_STATS_CACHE_MAX_PARTITIONS);
      long timeToLiveMs = MetastoreConf.getTimeVar(conf, ConfVars.AGGREGATE_STATS_CACHE_TTL,
              TimeUnit.SECONDS)*1000;
      // False positives probability we are ready to tolerate for the underlying bloom filter
      double falsePositiveProbability =
          MetastoreConf.getDoubleVar(conf, ConfVars.AGGREGATE_STATS_CACHE_FPP);
      // Maximum tolerable variance in number of partitions between cached node and our request
      double maxVariance =
          MetastoreConf.getDoubleVar(conf, ConfVars.AGGREGATE_STATS_CACHE_MAX_VARIANCE);
      long maxWriterWaitTime = MetastoreConf.getTimeVar(conf,
              ConfVars.AGGREGATE_STATS_CACHE_MAX_WRITER_WAIT, TimeUnit.MILLISECONDS);
      long maxReaderWaitTime = MetastoreConf.getTimeVar(conf,
              ConfVars.AGGREGATE_STATS_CACHE_MAX_READER_WAIT, TimeUnit.MILLISECONDS);
      double maxFull =
          MetastoreConf.getDoubleVar(conf, ConfVars.AGGREGATE_STATS_CACHE_MAX_FULL);
      double cleanUntil =
          MetastoreConf.getDoubleVar(conf, ConfVars.AGGREGATE_STATS_CACHE_CLEAN_UNTIL);
      self =
          new AggregateStatsCache(maxCacheNodes, maxPartitionsPerCacheNode, timeToLiveMs,
              falsePositiveProbability, maxVariance, maxWriterWaitTime, maxReaderWaitTime, maxFull,
              cleanUntil);
    }
    return self;
  }

  public int getMaxCacheNodes() {
    return maxCacheNodes;
  }

  public int getCurrentNodes() {
    return currentNodes.intValue();
  }

  public float getFullPercent() {
    return (currentNodes.intValue() / (float) maxCacheNodes) * 100;
  }

  public int getMaxPartsPerCacheNode() {
    return maxPartsPerCacheNode;
  }

  public double getFalsePositiveProbability() {
    return falsePositiveProbability;
  }

  public Float getHitRatio() {
    if (cacheHits.longValue() + cacheMisses.longValue() > 0) {
      return (float) (cacheHits.longValue()) / (cacheHits.longValue() + cacheMisses.longValue());
    }
    return null;
  }

  /**
   * Return aggregate stats for a column from the cache or null.
   * While reading from the nodelist for a key, we wait maxReaderWaitTime to acquire the lock,
   * failing which we return a cache miss (i.e. null)
   * @param catName catalog name
   * @param dbName database name
   * @param tblName table name
   * @param colName column name
   * @param partNames list of partition names
   * @return aggregated col stats
   */
  public AggrColStats get(String catName, String dbName, String tblName, String colName, List<String> partNames) {
    // Cache key
    Key key = new Key(catName, dbName, tblName, colName);
    AggrColStatsList candidateList = cacheStore.get(key);
    // No key, or no nodes in candidate list
    if ((candidateList == null) || (candidateList.nodes.size() == 0)) {
      LOG.debug("No aggregate stats cached for " + key.toString());
      return null;
    }
    // Find the value object
    // Update the timestamp of the key,value if value matches the criteria
    // Return the value
    AggrColStats match = null;
    boolean isLocked = false;
    try {
      // Try to readlock the candidateList; timeout after maxReaderWaitTime
      isLocked = candidateList.readLock.tryLock(maxReaderWaitTime, TimeUnit.MILLISECONDS);
      if (isLocked) {
        match = findBestMatch(partNames, candidateList.nodes);
      }
      if (match != null) {
        // Ok to not lock the list for this and use a volatile lastAccessTime instead
        candidateList.updateLastAccessTime();
        cacheHits.incrementAndGet();
        LOG.info("Returning aggregate stats from the cache; total hits: " + cacheHits.longValue()
            + ", total misses: " + cacheMisses.longValue() + ", hit ratio: " + getHitRatio());
      }
      else {
        cacheMisses.incrementAndGet();
      }
    } catch (InterruptedException e) {
      LOG.debug("Interrupted Exception ignored ",e);
    } finally {
      if (isLocked) {
        candidateList.readLock.unlock();
      }
    }
    return match;
  }

  /**
   * Find the best match using the configurable error tolerance and time to live value
   *
   * @param partNames
   * @param candidates
   * @return best matched node or null
   */
  private AggrColStats findBestMatch(List<String> partNames, List<AggrColStats> candidates) {
    // Hits, misses tracked for a candidate node
    MatchStats matchStats;
    // MatchStats for each candidate
    Map<AggrColStats, MatchStats> candidateMatchStats = new HashMap<>();
    // The final match we intend to return
    AggrColStats bestMatch = null;
    // To compare among potentially multiple matches
    int bestMatchHits = 0;
    int numPartsRequested = partNames.size();
    // 1st pass at marking invalid candidates
    // Checks based on variance and TTL
    // Note: we're not creating a copy of the list for saving memory
    for (AggrColStats candidate : candidates) {
      // Variance check
      if (Math.abs((candidate.getNumPartsCached() - numPartsRequested) / numPartsRequested)
          > maxVariance) {
        continue;
      }
      // TTL check
      if (isExpired(candidate)) {
        continue;
      } else {
        candidateMatchStats.put(candidate, new MatchStats(0, 0));
      }
    }
    // We'll count misses as we iterate
    int maxMisses = (int) maxVariance * numPartsRequested;
    for (String partName : partNames) {
      for (Iterator<Map.Entry<AggrColStats, MatchStats>> iterator = candidateMatchStats.entrySet().iterator(); iterator.hasNext();) {
        Map.Entry<AggrColStats, MatchStats> entry = iterator.next();
        AggrColStats candidate = entry.getKey();
        matchStats = entry.getValue();
        if (candidate.getBloomFilter().test(partName.getBytes())) {
          ++matchStats.hits;
        } else {
          ++matchStats.misses;
        }
        // 2nd pass at removing invalid candidates
        // If misses so far exceed max tolerable misses
        if (matchStats.misses > maxMisses) {
          iterator.remove();
          continue;
        }
        // Check if this is the best match so far
        if (matchStats.hits > bestMatchHits) {
          bestMatch = candidate;
          bestMatchHits = matchStats.hits;
        }
      }
    }
    if (bestMatch != null) {
      // Update the last access time for this node
      bestMatch.updateLastAccessTime();
    }
    return bestMatch;
  }

  /**
   * Add a new node to the cache; may trigger the cleaner thread if the cache is near full capacity.
   * We'll however add the node even if we temporaily exceed maxCacheNodes, because the cleaner
   * will eventually create space from expired nodes or by removing LRU nodes.
   * @param catName catalog name
   * @param dbName database name
   * @param tblName table name
   * @param colName column name
   * @param numPartsCached
   * @param colStats
   * @param bloomFilter
   */
  // TODO: make add asynchronous: add shouldn't block the higher level calls
  public void add(String catName, String dbName, String tblName, String colName, long numPartsCached,
      ColumnStatisticsObj colStats, BloomFilter bloomFilter) {
    // If we have no space in the cache, run cleaner thread
    if (getCurrentNodes() / maxCacheNodes > maxFull) {
      spawnCleaner();
    }
    // Cache key
    Key key = new Key(catName, dbName, tblName, colName);
    // Add new node to the cache
    AggrColStats node = new AggrColStats(numPartsCached, bloomFilter, colStats);
    AggrColStatsList nodeList;
    AggrColStatsList newNodeList = new AggrColStatsList();
    newNodeList.nodes = new ArrayList<>();
    nodeList = cacheStore.putIfAbsent(key, newNodeList);
    if (nodeList == null) {
      nodeList = newNodeList;
    }
    boolean isLocked = false;
    try {
      isLocked = nodeList.writeLock.tryLock(maxWriterWaitTime, TimeUnit.MILLISECONDS);
      if (isLocked) {
        nodeList.nodes.add(node);
        node.updateLastAccessTime();
        nodeList.updateLastAccessTime();
        currentNodes.getAndIncrement();
      }
    } catch (InterruptedException e) {
      LOG.debug("Interrupted Exception ignored ", e);
    } finally {
      if (isLocked) {
        nodeList.writeLock.unlock();
      }
    }
  }

  /**
   * Cleans the expired nodes or removes LRU nodes of the cache,
   * until the cache size reduces to cleanUntil% full.
   */
  private void spawnCleaner() {
    // This spawns a separate thread to walk through the cache and removes expired nodes.
    // Only one cleaner thread should be running at any point.
    synchronized (this) {
      if (isCleaning) {
        return;
      }
      isCleaning = true;
    }
    Thread cleaner = new Thread("AggregateStatsCache-CleanerThread") {
      @Override
      public void run() {
        numRemovedTTL = 0;
        numRemovedLRU = 0;
        long cleanerStartTime = System.currentTimeMillis();
        LOG.info("AggregateStatsCache is " + getFullPercent() + "% full, with "
            + getCurrentNodes() + " nodes; starting cleaner thread");
        try {
          Iterator<Map.Entry<Key, AggrColStatsList>> mapIterator = cacheStore.entrySet().iterator();
          while (mapIterator.hasNext()) {
            Map.Entry<Key, AggrColStatsList> pair =
                mapIterator.next();
            AggrColStats node;
            AggrColStatsList candidateList = pair.getValue();
            List<AggrColStats> nodes = candidateList.nodes;
            if (nodes.size() == 0) {
              mapIterator.remove();
              continue;
            }
            boolean isLocked = false;
            try {
              isLocked = candidateList.writeLock.tryLock(maxWriterWaitTime, TimeUnit.MILLISECONDS);
              if (isLocked) {
                for (Iterator<AggrColStats> listIterator = nodes.iterator(); listIterator.hasNext();) {
                  node = listIterator.next();
                  // Remove the node if it has expired
                  if (isExpired(node)) {
                    listIterator.remove();
                    numRemovedTTL++;
                    currentNodes.getAndDecrement();
                  }
                }
              }
            } catch (InterruptedException e) {
              LOG.debug("Interrupted Exception ignored ",e);
            } finally {
              if (isLocked) {
                candidateList.writeLock.unlock();
              }
            }
            // We want to make sure this runs at a low priority in the background
            Thread.yield();
          }
          // If the expired nodes did not result in cache being cleanUntil% in size,
          // start removing LRU nodes
          while (getCurrentNodes() / maxCacheNodes > cleanUntil) {
            evictOneNode();
          }
        } finally {
          isCleaning = false;
          LOG.info("Stopping cleaner thread; AggregateStatsCache is now " + getFullPercent()
              + "% full, with " + getCurrentNodes() + " nodes");
          LOG.info("Number of expired nodes removed: " + numRemovedTTL);
          LOG.info("Number of LRU nodes removed: " + numRemovedLRU);
          LOG.info("Cleaner ran for: " + (System.currentTimeMillis() - cleanerStartTime) + "ms");
        }
      }
    };
    cleaner.setPriority(Thread.MIN_PRIORITY);
    cleaner.setDaemon(true);
    cleaner.start();
  }

  /**
   * Evict an LRU node or expired node whichever we find first
   */
  private void evictOneNode() {
    // Get the LRU key, value
    Key lruKey = null;
    AggrColStatsList lruValue = null;
    for (Map.Entry<Key, AggrColStatsList> entry : cacheStore.entrySet()) {
      Key key = entry.getKey();
      AggrColStatsList value = entry.getValue();
      if (lruKey == null) {
        lruKey = key;
        lruValue = value;
        continue;
      }
      if ((value.lastAccessTime < lruValue.lastAccessTime) && !(value.nodes.isEmpty())) {
        lruKey = key;
        lruValue = value;
      }
    }
    // Now delete a node for this key's list
    AggrColStatsList candidateList = cacheStore.get(lruKey);
    boolean isLocked = false;
    try {
      isLocked = candidateList.writeLock.tryLock(maxWriterWaitTime, TimeUnit.MILLISECONDS);
      if (isLocked) {
        AggrColStats candidate;
        AggrColStats lruNode = null;
        int currentIndex = 0;
        int deleteIndex = 0;
        for (Iterator<AggrColStats> iterator = candidateList.nodes.iterator(); iterator.hasNext();) {
          candidate = iterator.next();
          // Since we have to create space for 1, if we find an expired node we will remove it &
          // return
          if (isExpired(candidate)) {
            iterator.remove();
            currentNodes.getAndDecrement();
            numRemovedTTL++;
            return;
          }
          // Sorry, too many ifs but this form looks optimal
          // Update the LRU node from what we've seen so far
          if (lruNode == null) {
            lruNode = candidate;
            ++currentIndex;
            continue;
          }
          if (lruNode != null) {
            if (candidate.lastAccessTime < lruNode.lastAccessTime) {
              lruNode = candidate;
              deleteIndex = currentIndex;
            }
          }
        }
        candidateList.nodes.remove(deleteIndex);
        currentNodes.getAndDecrement();
        numRemovedLRU++;
      }
    } catch (InterruptedException e) {
      LOG.debug("Interrupted Exception ignored ",e);
    } finally {
      if (isLocked) {
        candidateList.writeLock.unlock();
      }
    }
  }

  private boolean isExpired(AggrColStats aggrColStats) {
    return (System.currentTimeMillis() - aggrColStats.lastAccessTime) > timeToLiveMs;
  }

  /**
   * Key object for the stats cache hashtable
   */
  static class Key {
    private final String catName;
    private final String dbName;
    private final String tblName;
    private final String colName;

    Key(String cat, String db, String table, String col) {
      // Don't construct an illegal cache key
      if (cat == null || (db == null) || (table == null) || (col == null)) {
        throw new IllegalArgumentException("catName, dbName, tblName, colName can't be null");
      }
      catName = cat;
      dbName = db;
      tblName = table;
      colName = col;
    }

    @Override
    public boolean equals(Object other) {
      if ((other == null) || !(other instanceof Key)) {
        return false;
      }
      Key that = (Key) other;
      return catName.equals(that.catName) && dbName.equals(that.dbName) &&
          tblName.equals(that.tblName) && colName.equals(that.colName);
    }

    @Override
    public int hashCode() {
      return catName.hashCode() * 31 + dbName.hashCode() * 31 + tblName.hashCode() * 31 +
          colName.hashCode();
    }

    @Override
    public String toString() {
      return "catalog: " + catName + ", database:" + dbName + ", table:" + tblName + ", column:" +
          colName;
    }

  }

  static class AggrColStatsList {
    // TODO: figure out a better data structure for node list(?)
    private List<AggrColStats> nodes = new ArrayList<>();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    // Read lock for get operation
    private final Lock readLock = lock.readLock();
    // Write lock for add, evict and clean operation
    private final Lock writeLock = lock.writeLock();
    // Using volatile instead of locking updates to this variable,
    // since we can rely on approx lastAccessTime but don't want a performance hit
    private volatile long lastAccessTime = 0;

    List<AggrColStats> getNodes() {
      return nodes;
    }

    void updateLastAccessTime() {
      this.lastAccessTime = System.currentTimeMillis();
    }
  }

  public static class AggrColStats {
    private final long numPartsCached;
    private final BloomFilter bloomFilter;
    private final ColumnStatisticsObj colStats;
    private volatile long lastAccessTime;

    public AggrColStats(long numPartsCached, BloomFilter bloomFilter,
        ColumnStatisticsObj colStats) {
      this.numPartsCached = numPartsCached;
      this.bloomFilter = bloomFilter;
      this.colStats = colStats;
      this.lastAccessTime = System.currentTimeMillis();
    }

    public long getNumPartsCached() {
      return numPartsCached;
    }

    public ColumnStatisticsObj getColStats() {
      return colStats;
    }

    public BloomFilter getBloomFilter() {
      return bloomFilter;
    }

    void updateLastAccessTime() {
      this.lastAccessTime = System.currentTimeMillis();
    }
  }

  /**
   * Intermediate object, used to collect hits & misses for each cache node that is evaluate for an
   * incoming request
   */
  private static class MatchStats {
    private int hits = 0;
    private int misses = 0;

    MatchStats(int hits, int misses) {
      this.hits = hits;
      this.misses = misses;
    }
  }
}
