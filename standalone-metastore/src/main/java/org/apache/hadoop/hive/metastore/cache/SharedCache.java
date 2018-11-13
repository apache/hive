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
package org.apache.hadoop.hive.metastore.cache;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.TreeMap;

import org.apache.hadoop.hive.metastore.StatObjectConverter;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.hive.ql.util.IncrementalObjectSizeEstimator;
import org.apache.hadoop.hive.ql.util.IncrementalObjectSizeEstimator.ObjectEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

public class SharedCache {
  private static ReentrantReadWriteLock cacheLock = new ReentrantReadWriteLock(true);
  private boolean isCatalogCachePrewarmed = false;
  private Map<String, Catalog> catalogCache = new TreeMap<>();
  private HashSet<String> catalogsDeletedDuringPrewarm = new HashSet<>();
  private AtomicBoolean isCatalogCacheDirty = new AtomicBoolean(false);

  // For caching Database objects. Key is database name
  private Map<String, Database> databaseCache = new TreeMap<>();
  private boolean isDatabaseCachePrewarmed = false;
  private HashSet<String> databasesDeletedDuringPrewarm = new HashSet<>();
  private AtomicBoolean isDatabaseCacheDirty = new AtomicBoolean(false);

  // For caching TableWrapper objects. Key is aggregate of database name and table name
  private Map<String, TableWrapper> tableCache = new TreeMap<>();
  private boolean isTableCachePrewarmed = false;
  private HashSet<String> tablesDeletedDuringPrewarm = new HashSet<>();
  private AtomicBoolean isTableCacheDirty = new AtomicBoolean(false);
  private Map<ByteArrayWrapper, StorageDescriptorWrapper> sdCache = new HashMap<>();
  private static MessageDigest md;
  static final private Logger LOG = LoggerFactory.getLogger(SharedCache.class.getName());
  private AtomicLong cacheUpdateCount = new AtomicLong(0);
  private static long maxCacheSizeInBytes = -1;
  private static long currentCacheSizeInBytes = 0;
  private static HashMap<Class<?>, ObjectEstimator> sizeEstimators = null;

  enum StatsType {
    ALL(0), ALLBUTDEFAULT(1);

    private final int position;

    StatsType(int position) {
      this.position = position;
    }

    public int getPosition() {
      return position;
    }
  }

  static {
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("should not happen", e);
    }
  }


  public void initialize(long maxSharedCacheSizeInBytes) {
    maxCacheSizeInBytes = maxSharedCacheSizeInBytes;
    // Create estimators
    if ((maxCacheSizeInBytes > 0) && (sizeEstimators == null)) {
      sizeEstimators = IncrementalObjectSizeEstimator.createEstimators(SharedCache.class);
    }
  }

  private static ObjectEstimator getMemorySizeEstimator(Class<?> clazz) {
    ObjectEstimator estimator = sizeEstimators.get(clazz);
    if (estimator == null) {
      IncrementalObjectSizeEstimator.createEstimators(clazz, sizeEstimators);
      estimator = sizeEstimators.get(clazz);
    }
    return estimator;
  }

  static class TableWrapper {
    Table t;
    String location;
    Map<String, String> parameters;
    byte[] sdHash;
    ReentrantReadWriteLock tableLock = new ReentrantReadWriteLock(true);
    // For caching column stats for an unpartitioned table
    // Key is column name and the value is the col stat object
    private Map<String, ColumnStatisticsObj> tableColStatsCache =
        new ConcurrentHashMap<String, ColumnStatisticsObj>();
    private AtomicBoolean isTableColStatsCacheDirty = new AtomicBoolean(false);
    // For caching partition objects
    // Ket is partition values and the value is a wrapper around the partition object
    private Map<String, PartitionWrapper> partitionCache =
        new ConcurrentHashMap<String, PartitionWrapper>();
    private AtomicBoolean isPartitionCacheDirty = new AtomicBoolean(false);
    // For caching column stats for a partitioned table
    // Key is aggregate of partition values, column name and the value is the col stat object
    private Map<String, ColumnStatisticsObj> partitionColStatsCache =
        new ConcurrentHashMap<String, ColumnStatisticsObj>();
    private AtomicBoolean isPartitionColStatsCacheDirty = new AtomicBoolean(false);
    // For caching aggregate column stats for all and all minus default partition
    // Key is column name and the value is a list of 2 col stat objects
    // (all partitions and all but default)
    private Map<String, List<ColumnStatisticsObj>> aggrColStatsCache =
        new ConcurrentHashMap<String, List<ColumnStatisticsObj>>();
    private AtomicBoolean isAggrPartitionColStatsCacheDirty = new AtomicBoolean(false);

    TableWrapper(Table t, byte[] sdHash, String location, Map<String, String> parameters) {
      this.t = t;
      this.sdHash = sdHash;
      this.location = location;
      this.parameters = parameters;
    }

    public Table getTable() {
      return t;
    }

    public void setTable(Table t) {
      this.t = t;
    }

    public byte[] getSdHash() {
      return sdHash;
    }

    public void setSdHash(byte[] sdHash) {
      this.sdHash = sdHash;
    }

    public String getLocation() {
      return location;
    }

    public void setLocation(String location) {
      this.location = location;
    }

    public Map<String, String> getParameters() {
      return parameters;
    }

    public void setParameters(Map<String, String> parameters) {
      this.parameters = parameters;
    }

    boolean sameDatabase(String catName, String dbName) {
      return catName.equals(t.getCatName()) && dbName.equals(t.getDbName());
    }

    void cachePartition(Partition part, SharedCache sharedCache) {
      try {
        tableLock.writeLock().lock();
        PartitionWrapper wrapper = makePartitionWrapper(part, sharedCache);
        partitionCache.put(CacheUtils.buildPartitionCacheKey(part.getValues()), wrapper);
        isPartitionCacheDirty.set(true);
        // Invalidate cached aggregate stats
        if (!aggrColStatsCache.isEmpty()) {
          aggrColStatsCache.clear();
        }
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    boolean cachePartitions(List<Partition> parts, SharedCache sharedCache, boolean fromPrewarm) {
      try {
        tableLock.writeLock().lock();
        for (Partition part : parts) {
          PartitionWrapper ptnWrapper = makePartitionWrapper(part, sharedCache);
          if (maxCacheSizeInBytes > 0) {
            ObjectEstimator ptnWrapperSizeEstimator =
                getMemorySizeEstimator(PartitionWrapper.class);
            long estimatedMemUsage = ptnWrapperSizeEstimator.estimate(ptnWrapper, sizeEstimators);
            LOG.trace("Memory needed to cache Partition: {} is {} bytes", part, estimatedMemUsage);
            if (isCacheMemoryFull(estimatedMemUsage)) {
              LOG.debug(
                  "Cannot cache Partition: {}. Memory needed is {} bytes, whereas the memory remaining is: {} bytes.",
                  part, estimatedMemUsage, (0.8 * maxCacheSizeInBytes - currentCacheSizeInBytes));
              return false;
            } else {
              currentCacheSizeInBytes += estimatedMemUsage;
            }
            LOG.trace("Current cache size: {} bytes", currentCacheSizeInBytes);
          }
          partitionCache.put(CacheUtils.buildPartitionCacheKey(part.getValues()), ptnWrapper);
          if (!fromPrewarm) {
            isPartitionCacheDirty.set(true);
          }
        }
        // Invalidate cached aggregate stats
        if (!aggrColStatsCache.isEmpty()) {
          aggrColStatsCache.clear();
        }
        return true;
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public Partition getPartition(List<String> partVals, SharedCache sharedCache) {
      Partition part = null;
      try {
        tableLock.readLock().lock();
        PartitionWrapper wrapper = partitionCache.get(CacheUtils.buildPartitionCacheKey(partVals));
        if (wrapper == null) {
          return null;
        }
        part = CacheUtils.assemble(wrapper, sharedCache);
      } finally {
        tableLock.readLock().unlock();
      }
      return part;
    }

    public List<Partition> listPartitions(int max, SharedCache sharedCache) {
      List<Partition> parts = new ArrayList<>();
      int count = 0;
      try {
        tableLock.readLock().lock();
        for (PartitionWrapper wrapper : partitionCache.values()) {
          if (max == -1 || count < max) {
            parts.add(CacheUtils.assemble(wrapper, sharedCache));
            count++;
          }
        }
      } finally {
        tableLock.readLock().unlock();
      }
      return parts;
    }

    public boolean containsPartition(List<String> partVals) {
      boolean containsPart = false;
      try {
        tableLock.readLock().lock();
        containsPart = partitionCache.containsKey(CacheUtils.buildPartitionCacheKey(partVals));
      } finally {
        tableLock.readLock().unlock();
      }
      return containsPart;
    }

    public Partition removePartition(List<String> partVal, SharedCache sharedCache) {
      Partition part = null;
      try {
        tableLock.writeLock().lock();
        PartitionWrapper wrapper =
            partitionCache.remove(CacheUtils.buildPartitionCacheKey(partVal));
        isPartitionCacheDirty.set(true);
        part = CacheUtils.assemble(wrapper, sharedCache);
        if (wrapper.getSdHash() != null) {
          sharedCache.decrSd(wrapper.getSdHash());
        }
        // Remove col stats
        String partialKey = CacheUtils.buildPartitionCacheKey(partVal);
        Iterator<Entry<String, ColumnStatisticsObj>> iterator =
            partitionColStatsCache.entrySet().iterator();
        while (iterator.hasNext()) {
          Entry<String, ColumnStatisticsObj> entry = iterator.next();
          String key = entry.getKey();
          if (key.toLowerCase().startsWith(partialKey.toLowerCase())) {
            iterator.remove();
          }
        }
        // Invalidate cached aggregate stats
        if (!aggrColStatsCache.isEmpty()) {
          aggrColStatsCache.clear();
        }
      } finally {
        tableLock.writeLock().unlock();
      }
      return part;
    }

    public void removePartitions(List<List<String>> partVals, SharedCache sharedCache) {
      try {
        tableLock.writeLock().lock();
        for (List<String> partVal : partVals) {
          removePartition(partVal, sharedCache);
        }
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public void alterPartition(List<String> partVals, Partition newPart, SharedCache sharedCache) {
      try {
        tableLock.writeLock().lock();
        removePartition(partVals, sharedCache);
        cachePartition(newPart, sharedCache);
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public void alterPartitions(List<List<String>> partValsList, List<Partition> newParts,
        SharedCache sharedCache) {
      try {
        tableLock.writeLock().lock();
        for (int i = 0; i < partValsList.size(); i++) {
          List<String> partVals = partValsList.get(i);
          Partition newPart = newParts.get(i);
          alterPartition(partVals, newPart, sharedCache);
        }
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public void refreshPartitions(List<Partition> partitions, SharedCache sharedCache) {
      Map<String, PartitionWrapper> newPartitionCache = new HashMap<String, PartitionWrapper>();
      try {
        tableLock.writeLock().lock();
        for (Partition part : partitions) {
          if (isPartitionCacheDirty.compareAndSet(true, false)) {
            LOG.debug("Skipping partition cache update for table: " + getTable().getTableName()
                + "; the partition list we have is dirty.");
            return;
          }
          String key = CacheUtils.buildPartitionCacheKey(part.getValues());
          PartitionWrapper wrapper = partitionCache.get(key);
          if (wrapper != null) {
            if (wrapper.getSdHash() != null) {
              sharedCache.decrSd(wrapper.getSdHash());
            }
          }
          wrapper = makePartitionWrapper(part, sharedCache);
          newPartitionCache.put(key, wrapper);
        }
        partitionCache = newPartitionCache;
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public boolean updateTableColStats(List<ColumnStatisticsObj> colStatsForTable) {
      try {
        tableLock.writeLock().lock();
        for (ColumnStatisticsObj colStatObj : colStatsForTable) {
          // Get old stats object if present
          String key = colStatObj.getColName();
          ColumnStatisticsObj oldStatsObj = tableColStatsCache.get(key);
          if (oldStatsObj != null) {
            // Update existing stat object's field
            StatObjectConverter.setFieldsIntoOldStats(oldStatsObj, colStatObj);
          } else {
            // No stats exist for this key; add a new object to the cache
            // TODO: get rid of deepCopy after making sure callers don't use references
            if (maxCacheSizeInBytes > 0) {
              ObjectEstimator tblColStatsSizeEstimator =
                  getMemorySizeEstimator(ColumnStatisticsObj.class);
              long estimatedMemUsage =
                  tblColStatsSizeEstimator.estimate(colStatObj, sizeEstimators);
              LOG.trace("Memory needed to cache Table Column Statistics Object: {} is {} bytes",
                  colStatObj, estimatedMemUsage);
              if (isCacheMemoryFull(estimatedMemUsage)) {
                LOG.debug(
                    "Cannot cache Table Column Statistics Object: {}. Memory needed is {} bytes, "
                        + "whereas the memory remaining is: {} bytes.",
                    colStatObj, estimatedMemUsage,
                    (0.8 * maxCacheSizeInBytes - currentCacheSizeInBytes));
                return false;
              } else {
                currentCacheSizeInBytes += estimatedMemUsage;
              }
              LOG.trace("Current cache size: {} bytes", currentCacheSizeInBytes);
            }
            tableColStatsCache.put(key, colStatObj.deepCopy());
          }
        }
        isTableColStatsCacheDirty.set(true);
        return true;
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public void refreshTableColStats(List<ColumnStatisticsObj> colStatsForTable) {
      Map<String, ColumnStatisticsObj> newTableColStatsCache =
          new HashMap<String, ColumnStatisticsObj>();
      try {
        tableLock.writeLock().lock();
        for (ColumnStatisticsObj colStatObj : colStatsForTable) {
          if (isTableColStatsCacheDirty.compareAndSet(true, false)) {
            LOG.debug("Skipping table col stats cache update for table: "
                + getTable().getTableName() + "; the table col stats list we have is dirty.");
            return;
          }
          String key = colStatObj.getColName();
          // TODO: get rid of deepCopy after making sure callers don't use references
          newTableColStatsCache.put(key, colStatObj.deepCopy());
        }
        tableColStatsCache = newTableColStatsCache;
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public List<ColumnStatisticsObj> getCachedTableColStats(List<String> colNames) {
      List<ColumnStatisticsObj> colStatObjs = new ArrayList<ColumnStatisticsObj>();
      try {
        tableLock.readLock().lock();
        for (String colName : colNames) {
          ColumnStatisticsObj colStatObj = tableColStatsCache.get(colName);
          if (colStatObj != null) {
            colStatObjs.add(colStatObj);
          }
        }
      } finally {
        tableLock.readLock().unlock();
      }
      return colStatObjs;
    }

    public void removeTableColStats(String colName) {
      try {
        tableLock.writeLock().lock();
        if (colName == null) {
          tableColStatsCache.clear();
        } else {
          tableColStatsCache.remove(colName);
        }
        isTableColStatsCacheDirty.set(true);
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public void removeAllTableColStats() {
      try {
        tableLock.writeLock().lock();
        tableColStatsCache.clear();
        isTableColStatsCacheDirty.set(true);
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public ColumnStatisticsObj getPartitionColStats(List<String> partVal, String colName) {
      try {
        tableLock.readLock().lock();
        return partitionColStatsCache
            .get(CacheUtils.buildPartitonColStatsCacheKey(partVal, colName));
      } finally {
        tableLock.readLock().unlock();
      }
    }

    public boolean updatePartitionColStats(List<String> partVal,
        List<ColumnStatisticsObj> colStatsObjs) {
      try {
        tableLock.writeLock().lock();
        for (ColumnStatisticsObj colStatObj : colStatsObjs) {
          // Get old stats object if present
          String key = CacheUtils.buildPartitonColStatsCacheKey(partVal, colStatObj.getColName());
          ColumnStatisticsObj oldStatsObj = partitionColStatsCache.get(key);
          if (oldStatsObj != null) {
            // Update existing stat object's field
            StatObjectConverter.setFieldsIntoOldStats(oldStatsObj, colStatObj);
          } else {
            // No stats exist for this key; add a new object to the cache
            // TODO: get rid of deepCopy after making sure callers don't use references
            if (maxCacheSizeInBytes > 0) {
              ObjectEstimator ptnColStatsSizeEstimator =
                  getMemorySizeEstimator(ColumnStatisticsObj.class);
              long estimatedMemUsage =
                  ptnColStatsSizeEstimator.estimate(colStatObj, sizeEstimators);
              LOG.trace("Memory needed to cache Partition Column Statistics Object: {} is {} bytes",
                  colStatObj, estimatedMemUsage);
              if (isCacheMemoryFull(estimatedMemUsage)) {
                LOG.debug(
                    "Cannot cache Partition Column Statistics Object: {}. Memory needed is {} bytes, "
                    + "whereas the memory remaining is: {} bytes.",
                    colStatObj, estimatedMemUsage,
                    (0.8 * maxCacheSizeInBytes - currentCacheSizeInBytes));
                return false;
              } else {
                currentCacheSizeInBytes += estimatedMemUsage;
              }
              LOG.trace("Current cache size: {} bytes", currentCacheSizeInBytes);
            }
            partitionColStatsCache.put(key, colStatObj.deepCopy());
          }
        }
        isPartitionColStatsCacheDirty.set(true);
        // Invalidate cached aggregate stats
        if (!aggrColStatsCache.isEmpty()) {
          aggrColStatsCache.clear();
        }
      } finally {
        tableLock.writeLock().unlock();
      }
      return true;
    }

    public void removePartitionColStats(List<String> partVals, String colName) {
      try {
        tableLock.writeLock().lock();
        partitionColStatsCache.remove(CacheUtils.buildPartitonColStatsCacheKey(partVals, colName));
        isPartitionColStatsCacheDirty.set(true);
        // Invalidate cached aggregate stats
        if (!aggrColStatsCache.isEmpty()) {
          aggrColStatsCache.clear();
        }
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public void removeAllPartitionColStats() {
      try {
        tableLock.writeLock().lock();
        partitionColStatsCache.clear();
        isPartitionColStatsCacheDirty.set(true);
        // Invalidate cached aggregate stats
        if (!aggrColStatsCache.isEmpty()) {
          aggrColStatsCache.clear();
        }
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public void refreshPartitionColStats(List<ColumnStatistics> partitionColStats) {
      Map<String, ColumnStatisticsObj> newPartitionColStatsCache =
          new HashMap<String, ColumnStatisticsObj>();
      try {
        tableLock.writeLock().lock();
        String tableName = StringUtils.normalizeIdentifier(getTable().getTableName());
        for (ColumnStatistics cs : partitionColStats) {
          if (isPartitionColStatsCacheDirty.compareAndSet(true, false)) {
            LOG.debug("Skipping partition column stats cache update for table: "
                + getTable().getTableName() + "; the partition column stats list we have is dirty");
            return;
          }
          List<String> partVal;
          try {
            partVal = Warehouse.makeValsFromName(cs.getStatsDesc().getPartName(), null);
            List<ColumnStatisticsObj> colStatsObjs = cs.getStatsObj();
            for (ColumnStatisticsObj colStatObj : colStatsObjs) {
              if (isPartitionColStatsCacheDirty.compareAndSet(true, false)) {
                LOG.debug("Skipping partition column stats cache update for table: "
                    + getTable().getTableName() + "; the partition column list we have is dirty");
                return;
              }
              String key =
                  CacheUtils.buildPartitonColStatsCacheKey(partVal, colStatObj.getColName());
              newPartitionColStatsCache.put(key, colStatObj.deepCopy());
            }
          } catch (MetaException e) {
            LOG.debug("Unable to cache partition column stats for table: " + tableName, e);
          }
        }
        partitionColStatsCache = newPartitionColStatsCache;
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public List<ColumnStatisticsObj> getAggrPartitionColStats(List<String> colNames,
        StatsType statsType) {
      List<ColumnStatisticsObj> colStats = new ArrayList<ColumnStatisticsObj>();
      try {
        tableLock.readLock().lock();
        for (String colName : colNames) {
          List<ColumnStatisticsObj> colStatList = aggrColStatsCache.get(colName);
          // If unable to find stats for a column, return null so we can build stats
          if (colStatList == null) {
            return null;
          }
          ColumnStatisticsObj colStatObj = colStatList.get(statsType.getPosition());
          // If unable to find stats for this StatsType, return null so we can build stats
          if (colStatObj == null) {
            return null;
          }
          colStats.add(colStatObj);
        }
      } finally {
        tableLock.readLock().unlock();
      }
      return colStats;
    }

    public void cacheAggrPartitionColStats(AggrStats aggrStatsAllPartitions,
        AggrStats aggrStatsAllButDefaultPartition) {
      try {
        tableLock.writeLock().lock();
        if (aggrStatsAllPartitions != null) {
          for (ColumnStatisticsObj statObj : aggrStatsAllPartitions.getColStats()) {
            if (statObj != null) {
              List<ColumnStatisticsObj> aggrStats = new ArrayList<ColumnStatisticsObj>();
              aggrStats.add(StatsType.ALL.ordinal(), statObj.deepCopy());
              aggrColStatsCache.put(statObj.getColName(), aggrStats);
            }
          }
        }
        if (aggrStatsAllButDefaultPartition != null) {
          for (ColumnStatisticsObj statObj : aggrStatsAllButDefaultPartition.getColStats()) {
            if (statObj != null) {
              List<ColumnStatisticsObj> aggrStats = aggrColStatsCache.get(statObj.getColName());
              if (aggrStats == null) {
                aggrStats = new ArrayList<ColumnStatisticsObj>();
              }
              aggrStats.add(StatsType.ALLBUTDEFAULT.ordinal(), statObj.deepCopy());
            }
          }
        }
        isAggrPartitionColStatsCacheDirty.set(true);
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public void refreshAggrPartitionColStats(AggrStats aggrStatsAllPartitions,
        AggrStats aggrStatsAllButDefaultPartition) {
      Map<String, List<ColumnStatisticsObj>> newAggrColStatsCache =
          new HashMap<String, List<ColumnStatisticsObj>>();
      try {
        tableLock.writeLock().lock();
        if (aggrStatsAllPartitions != null) {
          for (ColumnStatisticsObj statObj : aggrStatsAllPartitions.getColStats()) {
            if (isAggrPartitionColStatsCacheDirty.compareAndSet(true, false)) {
              LOG.debug("Skipping aggregate stats cache update for table: "
                  + getTable().getTableName() + "; the aggregate stats list we have is dirty");
              return;
            }
            if (statObj != null) {
              List<ColumnStatisticsObj> aggrStats = new ArrayList<ColumnStatisticsObj>();
              aggrStats.add(StatsType.ALL.ordinal(), statObj.deepCopy());
              newAggrColStatsCache.put(statObj.getColName(), aggrStats);
            }
          }
        }
        if (aggrStatsAllButDefaultPartition != null) {
          for (ColumnStatisticsObj statObj : aggrStatsAllButDefaultPartition.getColStats()) {
            if (isAggrPartitionColStatsCacheDirty.compareAndSet(true, false)) {
              LOG.debug("Skipping aggregate stats cache update for table: "
                  + getTable().getTableName() + "; the aggregate stats list we have is dirty");
              return;
            }
            if (statObj != null) {
              List<ColumnStatisticsObj> aggrStats = newAggrColStatsCache.get(statObj.getColName());
              if (aggrStats == null) {
                aggrStats = new ArrayList<ColumnStatisticsObj>();
              }
              aggrStats.add(StatsType.ALLBUTDEFAULT.ordinal(), statObj.deepCopy());
            }
          }
        }
        aggrColStatsCache = newAggrColStatsCache;
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    private void updateTableObj(Table newTable, SharedCache sharedCache) {
      byte[] sdHash = getSdHash();
      // Remove old table object's sd hash
      if (sdHash != null) {
        sharedCache.decrSd(sdHash);
      }
      Table tblCopy = newTable.deepCopy();
      if (tblCopy.getPartitionKeys() != null) {
        for (FieldSchema fs : tblCopy.getPartitionKeys()) {
          fs.setName(StringUtils.normalizeIdentifier(fs.getName()));
        }
      }
      setTable(tblCopy);
      if (tblCopy.getSd() != null) {
        sdHash = MetaStoreUtils.hashStorageDescriptor(tblCopy.getSd(), md);
        StorageDescriptor sd = tblCopy.getSd();
        sharedCache.increSd(sd, sdHash);
        tblCopy.setSd(null);
        setSdHash(sdHash);
        setLocation(sd.getLocation());
        setParameters(sd.getParameters());
      } else {
        setSdHash(null);
        setLocation(null);
        setParameters(null);
      }
    }

    private PartitionWrapper makePartitionWrapper(Partition part, SharedCache sharedCache) {
      Partition partCopy = part.deepCopy();
      PartitionWrapper wrapper;
      if (part.getSd() != null) {
        byte[] sdHash = MetaStoreUtils.hashStorageDescriptor(part.getSd(), md);
        StorageDescriptor sd = part.getSd();
        sharedCache.increSd(sd, sdHash);
        partCopy.setSd(null);
        wrapper = new PartitionWrapper(partCopy, sdHash, sd.getLocation(), sd.getParameters());
      } else {
        wrapper = new PartitionWrapper(partCopy, null, null, null);
      }
      return wrapper;
    }
  }

  static class PartitionWrapper {
    Partition p;
    String location;
    Map<String, String> parameters;
    byte[] sdHash;

    PartitionWrapper(Partition p, byte[] sdHash, String location, Map<String, String> parameters) {
      this.p = p;
      this.sdHash = sdHash;
      this.location = location;
      this.parameters = parameters;
    }

    public Partition getPartition() {
      return p;
    }

    public byte[] getSdHash() {
      return sdHash;
    }

    public String getLocation() {
      return location;
    }

    public Map<String, String> getParameters() {
      return parameters;
    }
  }

  static class StorageDescriptorWrapper {
    StorageDescriptor sd;
    int refCount = 0;

    StorageDescriptorWrapper(StorageDescriptor sd, int refCount) {
      this.sd = sd;
      this.refCount = refCount;
    }

    public StorageDescriptor getSd() {
      return sd;
    }

    public int getRefCount() {
      return refCount;
    }
  }

  public void populateCatalogsInCache(Collection<Catalog> catalogs) {
    for (Catalog cat : catalogs) {
      Catalog catCopy = cat.deepCopy();
      // ObjectStore also stores db name in lowercase
      catCopy.setName(catCopy.getName().toLowerCase());
      try {
        cacheLock.writeLock().lock();
        // Since we allow write operations on cache while prewarm is happening:
        // 1. Don't add databases that were deleted while we were preparing list for prewarm
        // 2. Skip overwriting exisiting db object
        // (which is present because it was added after prewarm started)
        if (catalogsDeletedDuringPrewarm.contains(catCopy.getName())) {
          continue;
        }
        catalogCache.putIfAbsent(catCopy.getName(), catCopy);
        catalogsDeletedDuringPrewarm.clear();
        isCatalogCachePrewarmed = true;
      } finally {
        cacheLock.writeLock().unlock();
      }
    }
  }

  public Catalog getCatalogFromCache(String name) {
    Catalog cat = null;
    try {
      cacheLock.readLock().lock();
      if (catalogCache.get(name) != null) {
        cat = catalogCache.get(name).deepCopy();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return cat;
  }

  public void addCatalogToCache(Catalog cat) {
    try {
      cacheLock.writeLock().lock();
      Catalog catCopy = cat.deepCopy();
      // ObjectStore also stores db name in lowercase
      catCopy.setName(catCopy.getName().toLowerCase());
      catalogCache.put(cat.getName(), catCopy);
      isCatalogCacheDirty.set(true);
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  public void alterCatalogInCache(String catName, Catalog newCat) {
    try {
      cacheLock.writeLock().lock();
      removeCatalogFromCache(catName);
      addCatalogToCache(newCat.deepCopy());
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  public void removeCatalogFromCache(String name) {
    name = normalizeIdentifier(name);
    try {
      cacheLock.writeLock().lock();
      // If db cache is not yet prewarmed, add this to a set which the prewarm thread can check
      // so that the prewarm thread does not add it back
      if (!isCatalogCachePrewarmed) {
        catalogsDeletedDuringPrewarm.add(name);
      }
      if (catalogCache.remove(name) != null) {
        isCatalogCacheDirty.set(true);
      }
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  public List<String> listCachedCatalogs() {
    try {
      cacheLock.readLock().lock();
      return new ArrayList<>(catalogCache.keySet());
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public boolean isCatalogCachePrewarmed() {
    return isCatalogCachePrewarmed;
  }

  public Database getDatabaseFromCache(String catName, String name) {
    Database db = null;
    try {
      cacheLock.readLock().lock();
      String key = CacheUtils.buildDbKey(catName, name);
      if (databaseCache.get(key) != null) {
        db = databaseCache.get(key).deepCopy();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return db;
  }

  public void populateDatabasesInCache(List<Database> databases) {
    for (Database db : databases) {
      Database dbCopy = db.deepCopy();
      // ObjectStore also stores db name in lowercase
      dbCopy.setName(dbCopy.getName().toLowerCase());
      try {
        cacheLock.writeLock().lock();
        // Since we allow write operations on cache while prewarm is happening:
        // 1. Don't add databases that were deleted while we were preparing list for prewarm
        // 2. Skip overwriting exisiting db object
        // (which is present because it was added after prewarm started)
        String key = CacheUtils.buildDbKey(dbCopy.getCatalogName().toLowerCase(),
            dbCopy.getName().toLowerCase());
        if (databasesDeletedDuringPrewarm.contains(key)) {
          continue;
        }
        databaseCache.putIfAbsent(key, dbCopy);
        databasesDeletedDuringPrewarm.clear();
        isDatabaseCachePrewarmed = true;
      } finally {
        cacheLock.writeLock().unlock();
      }
    }
  }

  public boolean isDatabaseCachePrewarmed() {
    return isDatabaseCachePrewarmed;
  }

  public void addDatabaseToCache(Database db) {
    try {
      cacheLock.writeLock().lock();
      Database dbCopy = db.deepCopy();
      // ObjectStore also stores db name in lowercase
      dbCopy.setName(dbCopy.getName().toLowerCase());
      dbCopy.setCatalogName(dbCopy.getCatalogName().toLowerCase());
      databaseCache.put(CacheUtils.buildDbKey(dbCopy.getCatalogName(), dbCopy.getName()), dbCopy);
      isDatabaseCacheDirty.set(true);
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  public void removeDatabaseFromCache(String catName, String dbName) {
    try {
      cacheLock.writeLock().lock();
      // If db cache is not yet prewarmed, add this to a set which the prewarm thread can check
      // so that the prewarm thread does not add it back
      String key = CacheUtils.buildDbKey(catName, dbName);
      if (!isDatabaseCachePrewarmed) {
        databasesDeletedDuringPrewarm.add(key);
      }
      if (databaseCache.remove(key) != null) {
        isDatabaseCacheDirty.set(true);
      }
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  public List<String> listCachedDatabases(String catName) {
    List<String> results = new ArrayList<>();
    try {
      cacheLock.readLock().lock();
      for (String pair : databaseCache.keySet()) {
        String[] n = CacheUtils.splitDbName(pair);
        if (catName.equals(n[0]))
          results.add(n[1]);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return results;
  }

  public List<String> listCachedDatabases(String catName, String pattern) {
    List<String> results = new ArrayList<>();
    try {
      cacheLock.readLock().lock();
      for (String pair : databaseCache.keySet()) {
        String[] n = CacheUtils.splitDbName(pair);
        if (catName.equals(n[0])) {
          n[1] = StringUtils.normalizeIdentifier(n[1]);
          if (CacheUtils.matches(n[1], pattern)) {
            results.add(n[1]);
          }
        }
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return results;
  }

  /**
   * Replaces the old db object with the new one. This will add the new database to cache if it does
   * not exist.
   */
  public void alterDatabaseInCache(String catName, String dbName, Database newDb) {
    try {
      cacheLock.writeLock().lock();
      removeDatabaseFromCache(catName, dbName);
      addDatabaseToCache(newDb.deepCopy());
      isDatabaseCacheDirty.set(true);
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  public boolean refreshDatabasesInCache(List<Database> databases) {
    if (isDatabaseCacheDirty.compareAndSet(true, false)) {
      LOG.debug("Skipping database cache update; the database list we have is dirty.");
      return false;
    }
    try {
      cacheLock.writeLock().lock();
      databaseCache.clear();
      for (Database db : databases) {
        addDatabaseToCache(db);
      }
      return true;
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  public int getCachedDatabaseCount() {
    try {
      cacheLock.readLock().lock();
      return databaseCache.size();
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public boolean populateTableInCache(Table table, ColumnStatistics tableColStats,
      List<Partition> partitions, List<ColumnStatistics> partitionColStats,
      AggrStats aggrStatsAllPartitions, AggrStats aggrStatsAllButDefaultPartition) {
    String catName = StringUtils.normalizeIdentifier(table.getCatName());
    String dbName = StringUtils.normalizeIdentifier(table.getDbName());
    String tableName = StringUtils.normalizeIdentifier(table.getTableName());
    // Since we allow write operations on cache while prewarm is happening:
    // 1. Don't add tables that were deleted while we were preparing list for prewarm
    if (tablesDeletedDuringPrewarm.contains(CacheUtils.buildTableKey(catName, dbName, tableName))) {
      return false;
    }
    TableWrapper tblWrapper = createTableWrapper(catName, dbName, tableName, table);
    if (maxCacheSizeInBytes > 0) {
      ObjectEstimator tblWrapperSizeEstimator = getMemorySizeEstimator(TableWrapper.class);
      long estimatedMemUsage = tblWrapperSizeEstimator.estimate(tblWrapper, sizeEstimators);
      LOG.debug("Memory needed to cache Database: {}'s Table: {}, is {} bytes", dbName, tableName,
          estimatedMemUsage);
      if (isCacheMemoryFull(estimatedMemUsage)) {
        LOG.debug(
            "Cannot cache Database: {}'s Table: {}. Memory needed is {} bytes, "
                + "whereas the memory we have remaining is: {} bytes.",
            dbName, tableName, estimatedMemUsage,
            (0.8 * maxCacheSizeInBytes - currentCacheSizeInBytes));
        return false;
      } else {
        currentCacheSizeInBytes += estimatedMemUsage;
      }
      LOG.debug("Current cache size: {} bytes", currentCacheSizeInBytes);
    }
    if (table.getPartitionKeys().isEmpty() && (tableColStats != null)) {
      if (!tblWrapper.updateTableColStats(tableColStats.getStatsObj())) {
        return false;
      }
    } else {
      if (partitions != null) {
        // If the partitions were not added due to memory limit, return false
        if (!tblWrapper.cachePartitions(partitions, this, true)) {
          return false;
        }
      }
      if (partitionColStats != null) {
        for (ColumnStatistics cs : partitionColStats) {
          List<String> partVal;
          try {
            partVal = Warehouse.makeValsFromName(cs.getStatsDesc().getPartName(), null);
            List<ColumnStatisticsObj> colStats = cs.getStatsObj();
            if (!tblWrapper.updatePartitionColStats(partVal, colStats)) {
              return false;
            }
          } catch (MetaException e) {
            LOG.debug("Unable to cache partition column stats for table: " + tableName, e);
          }
        }
      }
      tblWrapper.cacheAggrPartitionColStats(aggrStatsAllPartitions,
          aggrStatsAllButDefaultPartition);
    }
    tblWrapper.isPartitionCacheDirty.set(false);
    tblWrapper.isTableColStatsCacheDirty.set(false);
    tblWrapper.isPartitionColStatsCacheDirty.set(false);
    tblWrapper.isAggrPartitionColStatsCacheDirty.set(false);
    try {
      cacheLock.writeLock().lock();
      // 2. Skip overwriting exisiting table object
      // (which is present because it was added after prewarm started)
      tableCache.putIfAbsent(CacheUtils.buildTableKey(catName, dbName, tableName), tblWrapper);
      return true;
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  private static boolean isCacheMemoryFull(long estimatedMemUsage) {
    return (0.8*maxCacheSizeInBytes) < (currentCacheSizeInBytes + estimatedMemUsage);
  }

  public void completeTableCachePrewarm() {
    try {
      cacheLock.writeLock().lock();
      tablesDeletedDuringPrewarm.clear();
      isTableCachePrewarmed = true;
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  public Table getTableFromCache(String catName, String dbName, String tableName) {
    Table t = null;
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper =
          tableCache.get(CacheUtils.buildTableKey(catName, dbName, tableName));
      if (tblWrapper != null) {
        t = CacheUtils.assemble(tblWrapper, this);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return t;
  }

  public TableWrapper addTableToCache(String catName, String dbName, String tblName, Table tbl) {
    try {
      cacheLock.writeLock().lock();
      TableWrapper wrapper = createTableWrapper(catName, dbName, tblName, tbl);
      tableCache.put(CacheUtils.buildTableKey(catName, dbName, tblName), wrapper);
      isTableCacheDirty.set(true);
      return wrapper;
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  private TableWrapper createTableWrapper(String catName, String dbName, String tblName,
      Table tbl) {
    TableWrapper wrapper;
    Table tblCopy = tbl.deepCopy();
    tblCopy.setCatName(normalizeIdentifier(catName));
    tblCopy.setDbName(normalizeIdentifier(dbName));
    tblCopy.setTableName(normalizeIdentifier(tblName));
    if (tblCopy.getPartitionKeys() != null) {
      for (FieldSchema fs : tblCopy.getPartitionKeys()) {
        fs.setName(normalizeIdentifier(fs.getName()));
      }
    }
    if (tbl.getSd() != null) {
      byte[] sdHash = MetaStoreUtils.hashStorageDescriptor(tbl.getSd(), md);
      StorageDescriptor sd = tbl.getSd();
      increSd(sd, sdHash);
      tblCopy.setSd(null);
      wrapper = new TableWrapper(tblCopy, sdHash, sd.getLocation(), sd.getParameters());
    } else {
      wrapper = new TableWrapper(tblCopy, null, null, null);
    }
    return wrapper;
  }

  public void removeTableFromCache(String catName, String dbName, String tblName) {
    try {
      cacheLock.writeLock().lock();
      // If table cache is not yet prewarmed, add this to a set which the prewarm thread can check
      // so that the prewarm thread does not add it back
      if (!isTableCachePrewarmed) {
        tablesDeletedDuringPrewarm.add(CacheUtils.buildTableKey(catName, dbName, tblName));
      }
      TableWrapper tblWrapper = tableCache.remove(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        byte[] sdHash = tblWrapper.getSdHash();
        if (sdHash != null) {
          decrSd(sdHash);
        }
        isTableCacheDirty.set(true);
      }
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  public void alterTableInCache(String catName, String dbName, String tblName, Table newTable) {
    try {
      cacheLock.writeLock().lock();
      TableWrapper tblWrapper =
          tableCache.remove(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.updateTableObj(newTable, this);
        String newDbName = StringUtils.normalizeIdentifier(newTable.getDbName());
        String newTblName = StringUtils.normalizeIdentifier(newTable.getTableName());
        tableCache.put(CacheUtils.buildTableKey(catName, newDbName, newTblName), tblWrapper);
        isTableCacheDirty.set(true);
      }
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  public List<Table> listCachedTables(String catName, String dbName) {
    List<Table> tables = new ArrayList<>();
    try {
      cacheLock.readLock().lock();
      for (TableWrapper wrapper : tableCache.values()) {
        if (wrapper.sameDatabase(catName, dbName)) {
          tables.add(CacheUtils.assemble(wrapper, this));
        }
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return tables;
  }

  public List<String> listCachedTableNames(String catName, String dbName) {
    List<String> tableNames = new ArrayList<>();
    try {
      cacheLock.readLock().lock();
      for (TableWrapper wrapper : tableCache.values()) {
        if (wrapper.sameDatabase(catName, dbName)) {
          tableNames.add(StringUtils.normalizeIdentifier(wrapper.getTable().getTableName()));
        }
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return tableNames;
  }

  public List<String> listCachedTableNames(String catName, String dbName, String pattern,
      short maxTables) {
    List<String> tableNames = new ArrayList<>();
    try {
      cacheLock.readLock().lock();
      int count = 0;
      for (TableWrapper wrapper : tableCache.values()) {
        if (wrapper.sameDatabase(catName, dbName)
            && CacheUtils.matches(wrapper.getTable().getTableName(), pattern)
            && (maxTables == -1 || count < maxTables)) {
          tableNames.add(StringUtils.normalizeIdentifier(wrapper.getTable().getTableName()));
          count++;
        }
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return tableNames;
  }

  public List<String> listCachedTableNames(String catName, String dbName, String pattern,
      TableType tableType) {
    List<String> tableNames = new ArrayList<>();
    try {
      cacheLock.readLock().lock();
      for (TableWrapper wrapper : tableCache.values()) {
        if (wrapper.sameDatabase(catName, dbName)
            && CacheUtils.matches(wrapper.getTable().getTableName(), pattern)
            && wrapper.getTable().getTableType().equals(tableType.toString())) {
          tableNames.add(StringUtils.normalizeIdentifier(wrapper.getTable().getTableName()));
        }
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return tableNames;
  }

  public boolean refreshTablesInCache(String catName, String dbName, List<Table> tables) {
    try {
      if (isTableCacheDirty.compareAndSet(true, false)) {
        LOG.debug("Skipping table cache update; the table list we have is dirty.");
        return false;
      }
      Map<String, TableWrapper> newCacheForDB = new TreeMap<>();
      for (Table tbl : tables) {
        String tblName = StringUtils.normalizeIdentifier(tbl.getTableName());
        TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tblName));
        if (tblWrapper != null) {
          tblWrapper.updateTableObj(tbl, this);
        } else {
          tblWrapper = createTableWrapper(catName, dbName, tblName, tbl);
        }
        newCacheForDB.put(CacheUtils.buildTableKey(catName, dbName, tblName), tblWrapper);
      }
      cacheLock.writeLock().lock();
      Iterator<Entry<String, TableWrapper>> entryIterator = tableCache.entrySet().iterator();
      while (entryIterator.hasNext()) {
        String key = entryIterator.next().getKey();
        if (key.startsWith(CacheUtils.buildDbKeyWithDelimiterSuffix(catName, dbName))) {
          entryIterator.remove();
        }
      }
      tableCache.putAll(newCacheForDB);
      return true;
    } finally {
      if (cacheLock.writeLock().isHeldByCurrentThread()) {
        cacheLock.writeLock().unlock();
      }
    }
  }

  public List<ColumnStatisticsObj> getTableColStatsFromCache(String catName, String dbName,
      String tblName, List<String> colNames) {
    List<ColumnStatisticsObj> colStatObjs = new ArrayList<>();
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        colStatObjs = tblWrapper.getCachedTableColStats(colNames);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return colStatObjs;
  }

  public void removeTableColStatsFromCache(String catName, String dbName, String tblName,
      String colName) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.removeTableColStats(colName);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void removeAllTableColStatsFromCache(String catName, String dbName, String tblName) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.removeAllTableColStats();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void updateTableColStatsInCache(String catName, String dbName, String tableName,
      List<ColumnStatisticsObj> colStatsForTable) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper =
          tableCache.get(CacheUtils.buildTableKey(catName, dbName, tableName));
      if (tblWrapper != null) {
        tblWrapper.updateTableColStats(colStatsForTable);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void refreshTableColStatsInCache(String catName, String dbName, String tableName,
      List<ColumnStatisticsObj> colStatsForTable) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper =
          tableCache.get(CacheUtils.buildTableKey(catName, dbName, tableName));
      if (tblWrapper != null) {
        tblWrapper.refreshTableColStats(colStatsForTable);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public int getCachedTableCount() {
    try {
      cacheLock.readLock().lock();
      return tableCache.size();
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public List<TableMeta> getTableMeta(String catName, String dbNames, String tableNames,
      List<String> tableTypes) {
    List<TableMeta> tableMetas = new ArrayList<>();
    try {
      cacheLock.readLock().lock();
      for (String dbName : listCachedDatabases(catName)) {
        if (CacheUtils.matches(dbName, dbNames)) {
          for (Table table : listCachedTables(catName, dbName)) {
            if (CacheUtils.matches(table.getTableName(), tableNames)) {
              if (tableTypes == null || tableTypes.contains(table.getTableType())) {
                TableMeta metaData =
                    new TableMeta(dbName, table.getTableName(), table.getTableType());
                metaData.setCatName(catName);
                metaData.setComments(table.getParameters().get("comment"));
                tableMetas.add(metaData);
              }
            }
          }
        }
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return tableMetas;
  }

  public void addPartitionToCache(String catName, String dbName, String tblName, Partition part) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.cachePartition(part, this);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void addPartitionsToCache(String catName, String dbName, String tblName, List<Partition> parts) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.cachePartitions(parts, this, false);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public Partition getPartitionFromCache(String catName, String dbName, String tblName,
      List<String> partVals) {
    Partition part = null;
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        part = tblWrapper.getPartition(partVals, this);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return part;
  }

  public boolean existPartitionFromCache(String catName, String dbName, String tblName,
      List<String> partVals) {
    boolean existsPart = false;
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        existsPart = tblWrapper.containsPartition(partVals);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return existsPart;
  }

  public Partition removePartitionFromCache(String catName, String dbName, String tblName,
      List<String> partVals) {
    Partition part = null;
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        part = tblWrapper.removePartition(partVals, this);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return part;
  }

  public void removePartitionsFromCache(String catName, String dbName, String tblName,
      List<List<String>> partVals) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.removePartitions(partVals, this);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public List<Partition> listCachedPartitions(String catName, String dbName, String tblName,
      int max) {
    List<Partition> parts = new ArrayList<Partition>();
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        parts = tblWrapper.listPartitions(max, this);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return parts;
  }

  public void alterPartitionInCache(String catName, String dbName, String tblName,
      List<String> partVals, Partition newPart) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.alterPartition(partVals, newPart, this);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void alterPartitionsInCache(String catName, String dbName, String tblName,
      List<List<String>> partValsList, List<Partition> newParts) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.alterPartitions(partValsList, newParts, this);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void refreshPartitionsInCache(String catName, String dbName, String tblName,
      List<Partition> partitions) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.refreshPartitions(partitions, this);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void removePartitionColStatsFromCache(String catName, String dbName, String tblName,
      List<String> partVals, String colName) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.removePartitionColStats(partVals, colName);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void removeAllPartitionColStatsFromCache(String catName, String dbName, String tblName) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.removeAllPartitionColStats();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void updatePartitionColStatsInCache(String catName, String dbName, String tableName,
      List<String> partVals, List<ColumnStatisticsObj> colStatsObjs) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper =
          tableCache.get(CacheUtils.buildTableKey(catName, dbName, tableName));
      if (tblWrapper != null) {
        tblWrapper.updatePartitionColStats(partVals, colStatsObjs);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public ColumnStatisticsObj getPartitionColStatsFromCache(String catName, String dbName,
      String tblName, List<String> partVal, String colName) {
    ColumnStatisticsObj colStatObj = null;
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        colStatObj = tblWrapper.getPartitionColStats(partVal, colName);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return colStatObj;
  }

  public void refreshPartitionColStatsInCache(String catName, String dbName, String tblName,
      List<ColumnStatistics> partitionColStats) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.refreshPartitionColStats(partitionColStats);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public List<ColumnStatisticsObj> getAggrStatsFromCache(String catName, String dbName,
      String tblName, List<String> colNames, StatsType statsType) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        return tblWrapper.getAggrPartitionColStats(colNames, statsType);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return null;
  }

  public void addAggregateStatsToCache(String catName, String dbName, String tblName,
      AggrStats aggrStatsAllPartitions, AggrStats aggrStatsAllButDefaultPartition) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.cacheAggrPartitionColStats(aggrStatsAllPartitions,
            aggrStatsAllButDefaultPartition);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void refreshAggregateStatsInCache(String catName, String dbName, String tblName,
      AggrStats aggrStatsAllPartitions, AggrStats aggrStatsAllButDefaultPartition) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.get(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.refreshAggrPartitionColStats(aggrStatsAllPartitions,
            aggrStatsAllButDefaultPartition);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public synchronized void increSd(StorageDescriptor sd, byte[] sdHash) {
    ByteArrayWrapper byteArray = new ByteArrayWrapper(sdHash);
    if (sdCache.containsKey(byteArray)) {
      sdCache.get(byteArray).refCount++;
    } else {
      StorageDescriptor sdToCache = sd.deepCopy();
      sdToCache.setLocation(null);
      sdToCache.setParameters(null);
      sdCache.put(byteArray, new StorageDescriptorWrapper(sdToCache, 1));
    }
  }

  public synchronized void decrSd(byte[] sdHash) {
    ByteArrayWrapper byteArray = new ByteArrayWrapper(sdHash);
    StorageDescriptorWrapper sdWrapper = sdCache.get(byteArray);
    sdWrapper.refCount--;
    if (sdWrapper.getRefCount() == 0) {
      sdCache.remove(byteArray);
    }
  }

  public synchronized StorageDescriptor getSdFromCache(byte[] sdHash) {
    StorageDescriptorWrapper sdWrapper = sdCache.get(new ByteArrayWrapper(sdHash));
    return sdWrapper.getSd();
  }

  @VisibleForTesting
  Map<String, Database> getDatabaseCache() {
    return databaseCache;
  }

  @VisibleForTesting
  Map<String, TableWrapper> getTableCache() {
    return tableCache;
  }

  @VisibleForTesting
  Map<ByteArrayWrapper, StorageDescriptorWrapper> getSdCache() {
    return sdCache;
  }

  /**
   * This resets the contents of the cataog cache so that we can re-fill it in another test.
   */
  void resetCatalogCache() {
    isCatalogCachePrewarmed = false;
    catalogCache.clear();
    catalogsDeletedDuringPrewarm.clear();
    isCatalogCacheDirty.set(false);
  }

  void clearDirtyFlags() {
    isCatalogCacheDirty.set(false);
    isDatabaseCacheDirty.set(false);
    isTableCacheDirty.set(false);
  }

  public long getUpdateCount() {
    return cacheUpdateCount.get();
  }

  public void incrementUpdateCount() {
    cacheUpdateCount.incrementAndGet();
  }
}
