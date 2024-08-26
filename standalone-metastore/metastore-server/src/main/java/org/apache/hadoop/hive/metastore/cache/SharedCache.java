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

import java.lang.reflect.Field;
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
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheStats;
import com.google.common.cache.RemovalCause;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.cache.Weigher;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.StatObjectConverter;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SQLAllTableConstraints;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.hive.ql.util.IncrementalObjectSizeEstimator;
import org.apache.hadoop.hive.ql.util.IncrementalObjectSizeEstimator.ObjectEstimator;
import org.eclipse.jetty.util.ConcurrentHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

public class SharedCache {
  private static ReentrantReadWriteLock cacheLock = new ReentrantReadWriteLock(true);
  private static final long MAX_DEFAULT_CACHE_SIZE = 1024 * 1024;
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
  private Cache<String, TableWrapper> tableCache = null;
  private int concurrencyLevel = -1;
  private int refreshInterval = 10000;

  private boolean isTableCachePrewarmed = false;
  private HashSet<String> tablesDeletedDuringPrewarm = new HashSet<>();
  private AtomicBoolean isTableCacheDirty = new AtomicBoolean(false);
  private Map<ByteArrayWrapper, StorageDescriptorWrapper> sdCache = new HashMap<>();
  private static MessageDigest md;
  private static final Logger LOG = LoggerFactory.getLogger(SharedCache.class.getName());
  private AtomicLong cacheUpdateCount = new AtomicLong(0);
  private long maxCacheSizeInBytes = -1;
  private HashMap<Class<?>, ObjectEstimator> sizeEstimators = null;
  private Set<String> tableToUpdateSize = new ConcurrentHashSet<>();
  private ScheduledExecutorService executor = null;
  private Map<String, Integer> tableSizeMap = null;

  enum StatsType {
    ALL(0), ALLBUTDEFAULT(1), PARTIAL(2);

    private final int position;

    StatsType(int position) {
      this.position = position;
    }

    public int getPosition() {
      return position;
    }
  }

  private enum MemberName {
    TABLE_COL_STATS_CACHE,
    PARTITION_CACHE,
    PARTITION_COL_STATS_CACHE,
    AGGR_COL_STATS_CACHE,
    PRIMARY_KEY_CACHE,
    FOREIGN_KEY_CACHE,
    NOTNULL_CONSTRAINT_CACHE,
    UNIQUE_CONSTRAINT_CACHE,
    DEFAULT_CONSTRAINT_CACHE,
    CHECK_CONSTRAINT_CACHE
  }

  static {
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("should not happen", e);
    }
  }

  static class TableWrapperSizeUpdater implements Runnable {
    private Set<String> setToUpdate;
    private Cache<String, TableWrapper> cache;

    TableWrapperSizeUpdater(Set<String> set, Cache<String, TableWrapper> cache1) {
      setToUpdate = set;
      cache = cache1;
    }

    @Override
    public void run() {
      for (String s : setToUpdate) {
        refreshTableWrapperInCache(s);
      }
      setToUpdate.clear();
    }

    void refreshTableWrapperInCache(String tblKey) {
      TableWrapper tw = cache.getIfPresent(tblKey);
      if (tw != null) {
        //cache will re-weigh the TableWrapper and record new weight.
        cache.put(tblKey, tw);
      }
    }
  }

  //concurrency level of table cache. Set to -1 to let Guava use default.
  public void setConcurrencyLevel(int cl){
    this.concurrencyLevel = cl;
  }
  //number of milliseconds between size updates.
  public void setRefreshInterval(int interval){
    this.refreshInterval = interval;
  }
  //set the table size map to fake table size. This is for testing only.
  public void setTableSizeMap(Map<String, Integer> map){
    this.tableSizeMap = map;
  }

  public void initialize(Configuration conf) {
    maxCacheSizeInBytes = MetastoreConf.getSizeVar(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_MAX_CACHE_MEMORY);

    // Create estimators
    if ((maxCacheSizeInBytes > 0) && (sizeEstimators == null)) {
      sizeEstimators = IncrementalObjectSizeEstimator.createEstimators(SharedCache.class);
    }

    if (tableCache == null) {
      CacheBuilder<String, TableWrapper> b = CacheBuilder.newBuilder()
          .maximumWeight(maxCacheSizeInBytes > 0 ? maxCacheSizeInBytes : MAX_DEFAULT_CACHE_SIZE)
          .weigher(new Weigher<String, TableWrapper>() {
            @Override
            public int weigh(String key, TableWrapper value) {
              return value.getSize();
            }
          }).removalListener(new RemovalListener<String, TableWrapper>() {
            @Override
            public void onRemoval(RemovalNotification<String, TableWrapper> notification) {
              LOG.debug("Eviction happened for table " + notification.getKey());
              LOG.debug("current table cache contains " + tableCache.size() + "entries");
              TableWrapper tblWrapper = notification.getValue();
              RemovalCause cause = notification.getCause();
              if (cause.equals(RemovalCause.COLLECTED) || cause.equals(RemovalCause.EXPIRED)) {
                byte[] sdHash = tblWrapper.getSdHash();
                if (sdHash != null) {
                  decrSd(sdHash);
                }
              }
            }
          });

      if (concurrencyLevel > 0) {
        b.concurrencyLevel(concurrencyLevel);
      }
      tableCache = b.recordStats().build();
    }

    executor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
      @Override
      public Thread newThread(Runnable r) {
        Thread t = Executors.defaultThreadFactory().newThread(r);
        t.setName("SharedCache table size updater: Thread-" + t.getId());
        t.setDaemon(true);
        return t;
      }
    });
    executor.scheduleAtFixedRate(new TableWrapperSizeUpdater(tableToUpdateSize, tableCache), 0,
        refreshInterval, TimeUnit.MILLISECONDS);

  }

  private ObjectEstimator getMemorySizeEstimator(Class<?> clazz) {
    if (sizeEstimators == null) {
      return null;
    }
    ObjectEstimator estimator = sizeEstimators.get(clazz);
    if (estimator == null) {
      IncrementalObjectSizeEstimator.createEstimators(clazz, sizeEstimators);
      estimator = sizeEstimators.get(clazz);
    }
    return estimator;
  }

  public int getObjectSize(Class<?> clazz, Object obj) {
    if (sizeEstimators == null) {
      return 0;
    }

    try {
      ObjectEstimator oe = getMemorySizeEstimator(clazz);
      return oe.estimate(obj, sizeEstimators);
    } catch (Exception e) {
      LOG.error("Error while getting object size.", e);
    }
    return 0;
  }

  enum SizeMode {
    Delta, Snapshot
  }

  class TableWrapper {
    private Table t;
    private String location;
    private Map<String, String> parameters;
    private byte[] sdHash;
    private int otherSize;

    // Arrays to hold the size/updated bit of cached objects.
    // These arrays are to be referenced using MemberName enum only.
    private int[] memberObjectsSize = new int[MemberName.values().length];
    private AtomicBoolean[] memberCacheUpdated = new AtomicBoolean[MemberName.values().length];

    private ReentrantReadWriteLock tableLock = new ReentrantReadWriteLock(true);
    // For caching column stats for an unpartitioned table
    // Key is column name and the value is the col stat object
    private Map<String, ColumnStatisticsObj> tableColStatsCache = new ConcurrentHashMap<String, ColumnStatisticsObj>();
    // For caching partition objects
    // Ket is partition values and the value is a wrapper around the partition object
    private Map<String, PartitionWrapper> partitionCache = new ConcurrentHashMap<String, PartitionWrapper>();
    // For caching column stats for a partitioned table
    // Key is aggregate of partition values, column name and the value is the col stat object
    private Map<String, ColumnStatisticsObj> partitionColStatsCache =
        new ConcurrentHashMap<String, ColumnStatisticsObj>();
    // For caching aggregate column stats for all and all minus default partition
    // Key is column name and the value is a list of 2 col stat objects
    // (all partitions and all but default)
    private Map<String, List<ColumnStatisticsObj>> aggrColStatsCache =
        new ConcurrentHashMap<String, List<ColumnStatisticsObj>>();

    private Map<String, SQLPrimaryKey> primaryKeyCache = new ConcurrentHashMap<>();
    private Map<String, SQLForeignKey> foreignKeyCache = new ConcurrentHashMap<>();
    private Map<String, SQLNotNullConstraint> notNullConstraintCache = new ConcurrentHashMap<>();
    private Map<String, SQLUniqueConstraint> uniqueConstraintCache = new ConcurrentHashMap<>();
    private Map<String, SQLDefaultConstraint> defaultConstraintCache = new ConcurrentHashMap<>();
    private Map<String, SQLCheckConstraint> checkConstraintCache = new ConcurrentHashMap<>();
    /**
     * this flag is used to validate/invalidate cached constraints snapshot.
     * All table constraints are update successfully either via updater thread or
     * while create_table_with_constraint api then it is set to true else
     * false and redirect all the call to raw metastore.
     */
    private boolean isConstraintsValid = false;

    TableWrapper(Table t, byte[] sdHash, String location, Map<String, String> parameters) {
      this.t = t;
      this.sdHash = sdHash;
      this.location = location;
      this.parameters = parameters;
      for(MemberName mn : MemberName.values()) {
        this.memberObjectsSize[mn.ordinal()] = 0;
        this.memberCacheUpdated[mn.ordinal()] = new AtomicBoolean(false);
      }
      this.otherSize = getTableWrapperSizeWithoutMaps();
    }

    public boolean isConstraintsValid() {
      return isConstraintsValid;
    }

    public void setConstraintsValid(boolean constraintsValid) {
      isConstraintsValid = constraintsValid;
    }

    public boolean compareAndSetMemberCacheUpdated(MemberName mn, boolean oldValue, boolean newValue) {
      return this.memberCacheUpdated[mn.ordinal()].compareAndSet(oldValue, newValue);
    }

    public void setMemberCacheUpdated(MemberName mn, boolean newValue) {
      this.memberCacheUpdated[mn.ordinal()].set(newValue);
    }

    private int getTableWrapperSizeWithoutMaps() {
      Class<?> clazz = TableWrapper.class;
      Field[] fields = clazz.getDeclaredFields();
      int size = 0;
      for (Field field : fields) {
        if (field.getType().equals(ConcurrentHashMap.class)) {
          continue;
        }
        if (field.getType().equals(SharedCache.class)) {
          continue;
        }
        try {
          field.setAccessible(true);
          Object val = field.get(this);
          ObjectEstimator oe = getMemorySizeEstimator(field.getType());
          if (oe != null) {
            size += oe.estimate(val, sizeEstimators);
          }
        } catch (Exception ex) {
          LOG.error("Not able to estimate size.", ex);
        }
      }

      return size;
    }

    public int getSize() {
      //facilitate testing only. In production we won't use tableSizeMap at all.
      if (tableSizeMap != null) {
        String tblKey = CacheUtils.buildTableKey(this.t.getCatName(), this.t.getDbName(), this.t.getTableName());
        if (tableSizeMap.containsKey(tblKey)) {
          return tableSizeMap.get(tblKey);
        }
      }
      if (sizeEstimators == null) {
        return 0;
      }
      int membersSize = 0;
      for(MemberName mn : MemberName.values()) {
        membersSize += this.memberObjectsSize[mn.ordinal()];
      }
      return otherSize + membersSize;
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

    private void updateMemberSize(MemberName mn, Integer size, SizeMode mode) {
      if (sizeEstimators == null) {
        return;
      }

      switch (mode) {
        case Delta:
          this.memberObjectsSize[mn.ordinal()] += size;
          break;
        case Snapshot:
          this.memberObjectsSize[mn.ordinal()] = size;
          break;
      }

      String tblKey = getTblKey();
      tableToUpdateSize.add(tblKey);
    }

    String getTblKey() {
      Table tbl = this.t;
      String catName = tbl.getCatName();
      String dbName = tbl.getDbName();
      String tblName = tbl.getTableName();
      return CacheUtils.buildTableKey(catName, dbName, tblName);
    }

    void cachePartition(Partition part, SharedCache sharedCache) {
      try {
        tableLock.writeLock().lock();
        PartitionWrapper wrapper = makePartitionWrapper(part, sharedCache);
        partitionCache.put(CacheUtils.buildPartitionCacheKey(part.getValues()), wrapper);
        int size = getObjectSize(PartitionWrapper.class, wrapper);
        updateMemberSize(MemberName.PARTITION_CACHE, size, SizeMode.Delta);
        setMemberCacheUpdated(MemberName.PARTITION_CACHE, true);

        // Invalidate cached aggregate stats
        if (!aggrColStatsCache.isEmpty()) {
          aggrColStatsCache.clear();
          updateMemberSize(MemberName.AGGR_COL_STATS_CACHE, 0, SizeMode.Snapshot);
        }
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    boolean cachePartitions(Iterable<Partition> parts, SharedCache sharedCache, boolean fromPrewarm) {
      try {
        tableLock.writeLock().lock();
        int size = 0;
        for (Partition part : parts) {
          PartitionWrapper wrapper = makePartitionWrapper(part, sharedCache);
          partitionCache.put(CacheUtils.buildPartitionCacheKey(part.getValues()), wrapper);
          size += getObjectSize(PartitionWrapper.class, wrapper);

          if (!fromPrewarm) {
            setMemberCacheUpdated(MemberName.PARTITION_CACHE, true);
          }
        }
        updateMemberSize(MemberName.PARTITION_CACHE, size, SizeMode.Delta);
        // Invalidate cached aggregate stats
        if (!aggrColStatsCache.isEmpty()) {
          aggrColStatsCache.clear();
          updateMemberSize(MemberName.AGGR_COL_STATS_CACHE, 0, SizeMode.Snapshot);
        }
        return true;
      } finally {
        tableLock.writeLock().unlock();
      }
    }


    boolean cacheConstraints(SQLAllTableConstraints constraints, boolean fromPrewarm) {
      try {
        tableLock.writeLock().lock();
        this.isConstraintsValid =
            cacheConstraints(constraints.getPrimaryKeys(), fromPrewarm, MemberName.PRIMARY_KEY_CACHE) && cacheConstraints(
                constraints.getForeignKeys(), fromPrewarm, MemberName.FOREIGN_KEY_CACHE) && cacheConstraints(
                constraints.getDefaultConstraints(), fromPrewarm, MemberName.DEFAULT_CONSTRAINT_CACHE)
                && cacheConstraints(constraints.getUniqueConstraints(), fromPrewarm, MemberName.UNIQUE_CONSTRAINT_CACHE)
                && cacheConstraints(constraints.getNotNullConstraints(), fromPrewarm, MemberName.NOTNULL_CONSTRAINT_CACHE)
                && cacheConstraints(constraints.getCheckConstraints(), fromPrewarm, MemberName.CHECK_CONSTRAINT_CACHE);
        tableToUpdateSize.add(getTblKey());
        return this.isConstraintsValid;
      } finally {
        tableLock.writeLock().unlock();
      }

    }

    /**
     * Common method to cache constraints
     * @param constraintsList list of constraints to add to cache
     * @param fromPrewarm is this method called as part of prewarm phase
     * @param mn Constraint type
     * @return memory constraint is handled by cache eviction policy hence this method will always return true
     * if correct constraint type is provided.
     */
    private boolean cacheConstraints(List constraintsList,
                             boolean fromPrewarm,
                             MemberName mn) {
      if (CollectionUtils.isEmpty(constraintsList)) {
        return true;
      }
      try {
        tableLock.writeLock().lock();
        int totalSize = constraintsList.stream().mapToInt(constraint -> {
          int size = 0;
          switch (mn) {
          case PRIMARY_KEY_CACHE:
            SQLPrimaryKey pk = (SQLPrimaryKey) constraint;
            String pkName = normalizeIdentifier(pk.getPk_name());
            this.primaryKeyCache.put(pkName, pk);
            size = getObjectSize(SQLPrimaryKey.class, constraint);
            break;
          case FOREIGN_KEY_CACHE:
            SQLForeignKey fk = (SQLForeignKey) constraint;
            String fkName = normalizeIdentifier(fk.getFk_name());
            this.foreignKeyCache.put(fkName, fk);
            size = getObjectSize(SQLForeignKey.class, constraint);
            break;
          case UNIQUE_CONSTRAINT_CACHE:
            SQLUniqueConstraint uc = (SQLUniqueConstraint) constraint;
            String ucName = normalizeIdentifier(uc.getUk_name());
            this.uniqueConstraintCache.put(ucName, uc);
            size = getObjectSize(SQLUniqueConstraint.class, constraint);
            break;
          case NOTNULL_CONSTRAINT_CACHE:
            SQLNotNullConstraint nn = (SQLNotNullConstraint) constraint;
            String nnName = normalizeIdentifier(nn.getNn_name());
            this.notNullConstraintCache.put(nnName, nn);
            size = getObjectSize(SQLNotNullConstraint.class, constraint);
            break;
          case DEFAULT_CONSTRAINT_CACHE:
            SQLDefaultConstraint dc = (SQLDefaultConstraint) constraint;
            String dcName = normalizeIdentifier(dc.getDc_name());
            this.defaultConstraintCache.put(dcName, dc);
            size = getObjectSize(SQLDefaultConstraint.class, constraint);
            break;
          case CHECK_CONSTRAINT_CACHE:
            SQLCheckConstraint cc = (SQLCheckConstraint) constraint;
            String ccName = normalizeIdentifier(cc.getDc_name());
            this.checkConstraintCache.put(ccName, cc);
            size = getObjectSize(SQLCheckConstraint.class, constraint);
            break;
          default:
            LOG.error("Should not reach here");
            break;
          }
          return size;
        }).sum();

        if (!fromPrewarm) {
          setMemberCacheUpdated(mn, true);
        }

        updateMemberSize(mn, totalSize, SizeMode.Snapshot);
        return true;
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public void invalidateConstraintsCache() {
      try {
        tableLock.writeLock().lock();
        this.setConstraintsValid(false);
        tableToUpdateSize.add(getTblKey());
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public List<SQLPrimaryKey> getPrimaryKeys() {
      try {
        tableLock.readLock().lock();
        return new ArrayList<>(this.primaryKeyCache.values());
      } finally {
        tableLock.readLock().unlock();
      }
    }

    public SQLAllTableConstraints getAllTableConstraints() {
      try {
        tableLock.readLock().lock();
        SQLAllTableConstraints constraints = new SQLAllTableConstraints();
        if (isConstraintsValid) {
          constraints.setPrimaryKeys(new ArrayList<>(this.primaryKeyCache.values()));
          constraints.setForeignKeys(new ArrayList<>(this.foreignKeyCache.values()));
          constraints.setNotNullConstraints(new ArrayList<>(this.notNullConstraintCache.values()));
          constraints.setUniqueConstraints(new ArrayList<>(this.uniqueConstraintCache.values()));
          constraints.setDefaultConstraints(new ArrayList<>(this.defaultConstraintCache.values()));
          constraints.setCheckConstraints(new ArrayList<>(this.checkConstraintCache.values()));
        }
        return constraints;
      } finally {
        tableLock.readLock().unlock();
      }
    }

    public List<SQLForeignKey> getForeignKeys() {
      try {
        tableLock.readLock().lock();
        return new ArrayList<>(this.foreignKeyCache.values());
      } finally {
        tableLock.readLock().unlock();
      }
    }

    public List<SQLUniqueConstraint> getUniqueConstraints() {
      try {
        tableLock.readLock().lock();
        return new ArrayList<>(this.uniqueConstraintCache.values());
      } finally {
        tableLock.readLock().unlock();
      }
    }

    public List<SQLNotNullConstraint> getNotNullConstraints() {
      try {
        tableLock.readLock().lock();
        return new ArrayList<>(this.notNullConstraintCache.values());
      } finally {
        tableLock.readLock().unlock();
      }
    }

    public List<SQLDefaultConstraint> getDefaultConstraints() {
      try {
        tableLock.readLock().lock();
        return new ArrayList<>(this.defaultConstraintCache.values());
      } finally {
        tableLock.readLock().unlock();
      }
    }

    public List<SQLCheckConstraint> getCheckConstraints() {
      try {
        tableLock.readLock().lock();
        return new ArrayList<>(this.checkConstraintCache.values());
      } finally {
        tableLock.readLock().unlock();
      }
    }

    public Partition getPartition(List<String> partVals, SharedCache sharedCache) {
      Partition part = null;
      try {
        tableLock.readLock().lock();
        PartitionWrapper wrapper = partitionCache.get(CacheUtils.buildPartitionCacheKey(partVals));
        if (wrapper == null) {
          LOG.debug("Partition: " + partVals + " is not present in the cache.");
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

    public void removeConstraint(String name) {
      try {
        tableLock.writeLock().lock();
        Object constraint = null;
        MemberName mn = null;
        Class constraintClass = null;
        name = StringUtils.normalizeIdentifier(name);
        if (this.primaryKeyCache.containsKey(name)) {
          constraint = this.primaryKeyCache.remove(name);
          mn = MemberName.PRIMARY_KEY_CACHE;
          constraintClass = SQLPrimaryKey.class;
        } else if (this.foreignKeyCache.containsKey(name)) {
          constraint = this.foreignKeyCache.remove(name);
          mn = MemberName.FOREIGN_KEY_CACHE;
          constraintClass = SQLForeignKey.class;
        } else if (this.notNullConstraintCache.containsKey(name)) {
          constraint = this.notNullConstraintCache.remove(name);
          mn = MemberName.NOTNULL_CONSTRAINT_CACHE;
          constraintClass = SQLNotNullConstraint.class;
        } else if (this.uniqueConstraintCache.containsKey(name)) {
          constraint = this.uniqueConstraintCache.remove(name);
          mn = MemberName.UNIQUE_CONSTRAINT_CACHE;
          constraintClass = SQLUniqueConstraint.class;
        } else if (this.defaultConstraintCache.containsKey(name)) {
          constraint = this.defaultConstraintCache.remove(name);
          mn = MemberName.DEFAULT_CONSTRAINT_CACHE;
          constraintClass = SQLDefaultConstraint.class;
        } else if (this.checkConstraintCache.containsKey(name)) {
          constraint = this.checkConstraintCache.remove(name);
          mn = MemberName.CHECK_CONSTRAINT_CACHE;
          constraintClass = SQLCheckConstraint.class;
        }

        if (constraint == null) {
          LOG.debug("Constraint: " + name + " does not exist in cache.");
          return;
        }
        setMemberCacheUpdated(mn, true);
        int size = getObjectSize(constraintClass, constraint);
        updateMemberSize(mn, -1 * size, SizeMode.Delta);
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public void refreshAllTableConstraints(SQLAllTableConstraints constraints) {
      try {
        tableLock.writeLock().lock();
        this.isConstraintsValid =
            refreshConstraint(constraints.getPrimaryKeys(), MemberName.PRIMARY_KEY_CACHE) && refreshConstraint(
                constraints.getForeignKeys(), MemberName.FOREIGN_KEY_CACHE) && refreshConstraint(
                constraints.getUniqueConstraints(), MemberName.UNIQUE_CONSTRAINT_CACHE) && refreshConstraint(
                constraints.getDefaultConstraints(), MemberName.DEFAULT_CONSTRAINT_CACHE) && refreshConstraint(
                constraints.getNotNullConstraints(), MemberName.NOTNULL_CONSTRAINT_CACHE) && refreshConstraint(
                constraints.getCheckConstraints(), MemberName.CHECK_CONSTRAINT_CACHE);
        tableToUpdateSize.add(getTblKey());
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    /**
     * Common method to refresh cache constraint
     * @param constraints list of constraints to update
     * @param mn Constraint type
     * @return Since memory constraint is handled by cache eviction policy.
     * Hence this method will always return true if correct constraint type passed.
     */
    private boolean refreshConstraint(List constraints, MemberName mn) {

      int size = 0;
      switch (mn) {
      case PRIMARY_KEY_CACHE:
        Map<String, SQLPrimaryKey> newPk = new ConcurrentHashMap<>();
        if (!CollectionUtils.isEmpty(constraints)) {
          for (SQLPrimaryKey key : (List<SQLPrimaryKey>) constraints) {
            String pkName = StringUtils.normalizeIdentifier(key.getPk_name());
            key.setPk_name(pkName);
            newPk.put(pkName, key);
            size += getObjectSize(SQLPrimaryKey.class, key);
          }
        }
        primaryKeyCache = newPk;
        break;
      case FOREIGN_KEY_CACHE:
        Map<String, SQLForeignKey> newFk = new ConcurrentHashMap<>();
        if (!CollectionUtils.isEmpty(constraints)) {
          for (SQLForeignKey key : (List<SQLForeignKey>) constraints) {
            String fkName = StringUtils.normalizeIdentifier(key.getFk_name());
            key.setFk_name(fkName);
            newFk.put(fkName, key);
            size += getObjectSize(SQLForeignKey.class, key);
          }
        }
        foreignKeyCache = newFk;
        break;
      case UNIQUE_CONSTRAINT_CACHE:
        Map<String, SQLUniqueConstraint> newUc = new ConcurrentHashMap<>();
        if (!CollectionUtils.isEmpty(constraints)) {
          for (SQLUniqueConstraint constraint : (List<SQLUniqueConstraint>) constraints) {
            String ucName = StringUtils.normalizeIdentifier(constraint.getUk_name());
            constraint.setUk_name(ucName);
            newUc.put(ucName, constraint);
            size += getObjectSize(SQLUniqueConstraint.class, constraint);
          }
        }
        uniqueConstraintCache = newUc;
        break;
      case NOTNULL_CONSTRAINT_CACHE:
        Map<String, SQLNotNullConstraint> newNn = new ConcurrentHashMap<>();
        if (!CollectionUtils.isEmpty(constraints)) {
          for (SQLNotNullConstraint constraint : (List<SQLNotNullConstraint>) constraints) {
            String nnName = StringUtils.normalizeIdentifier(constraint.getNn_name());
            constraint.setNn_name(nnName);
            newNn.put(nnName, constraint);
            size += getObjectSize(SQLNotNullConstraint.class, constraint);
          }
        }
        notNullConstraintCache = newNn;
        break;
      case DEFAULT_CONSTRAINT_CACHE:
        Map<String, SQLDefaultConstraint> newDc = new ConcurrentHashMap<>();
        if (!CollectionUtils.isEmpty(constraints)) {
          for (SQLDefaultConstraint constraint : (List<SQLDefaultConstraint>) constraints) {
            String dcName = StringUtils.normalizeIdentifier(constraint.getDc_name());
            constraint.setDc_name(dcName);
            newDc.put(dcName, constraint);
            size += getObjectSize(SQLDefaultConstraint.class, constraint);
          }
        }
        defaultConstraintCache = newDc;
        break;
      case CHECK_CONSTRAINT_CACHE:
        Map<String, SQLCheckConstraint> newCc = new ConcurrentHashMap<>();
        if (!CollectionUtils.isEmpty(constraints)) {
          for (SQLCheckConstraint constraint : (List<SQLCheckConstraint>) constraints) {
            String ccName = StringUtils.normalizeIdentifier(constraint.getDc_name());
            constraint.setDc_name(ccName);
            newCc.put(ccName, constraint);
            size += getObjectSize(SQLCheckConstraint.class, constraint);
          }
        }
        checkConstraintCache = newCc;
        break;
      default:
        LOG.error("Should not reach here");
        return false;
      }
      updateMemberSize(mn, size, SizeMode.Snapshot);
      LOG.debug("{}  refresh was successful for {}.{}.{}", mn, this.getTable().getCatName(),
          this.getTable().getDbName(), this.getTable().getTableName());
      return true;
    }

    public Partition removePartition(List<String> partVal, SharedCache sharedCache) {
      Partition part = null;
      try {
        tableLock.writeLock().lock();
        PartitionWrapper wrapper = partitionCache.remove(CacheUtils.buildPartitionCacheKey(partVal));
        if (wrapper == null) {
          return null;
        }
        setMemberCacheUpdated(MemberName.PARTITION_CACHE, true);

        int size = getObjectSize(PartitionWrapper.class, wrapper);
        updateMemberSize(MemberName.PARTITION_CACHE, -1 * size, SizeMode.Delta);

        part = CacheUtils.assemble(wrapper, sharedCache);
        if (wrapper.getSdHash() != null) {
          sharedCache.decrSd(wrapper.getSdHash());
        }
        // Remove col stats
        String partialKey = CacheUtils.buildPartitionCacheKey(partVal);
        Iterator<Entry<String, ColumnStatisticsObj>> iterator = partitionColStatsCache.entrySet().iterator();
        while (iterator.hasNext()) {
          Entry<String, ColumnStatisticsObj> entry = iterator.next();
          String key = entry.getKey();
          if (key.toLowerCase().startsWith(partialKey.toLowerCase())) {
            int statsSize = getObjectSize(ColumnStatisticsObj.class, entry.getValue());
            updateMemberSize(MemberName.PARTITION_COL_STATS_CACHE, -1 * statsSize, SizeMode.Delta);
            iterator.remove();
          }
        }

        // Invalidate cached aggregate stats
        if (!aggrColStatsCache.isEmpty()) {
          aggrColStatsCache.clear();
          updateMemberSize(MemberName.PARTITION_COL_STATS_CACHE, 0, SizeMode.Snapshot);
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

    public void alterPartitionAndStats(List<String> partVals, SharedCache sharedCache, long writeId,
        Map<String, String> parameters, List<ColumnStatisticsObj> colStatsObjs) {
      try {
        tableLock.writeLock().lock();
        PartitionWrapper partitionWrapper = partitionCache.get(CacheUtils.buildPartitionCacheKey(partVals));
        if (partitionWrapper == null) {
          LOG.info("Partition " + partVals + " is missing from cache. Cannot update the partition stats in cache.");
          return;
        }
        Partition newPart = partitionWrapper.getPartition();
        newPart.setParameters(parameters);
        newPart.setWriteId(writeId);
        removePartition(partVals, sharedCache);
        cachePartition(newPart, sharedCache);
        updatePartitionColStats(partVals, colStatsObjs);
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public void alterPartitions(List<List<String>> partValsList, List<Partition> newParts, SharedCache sharedCache) {
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
        int size = 0;
        for (Partition part : partitions) {
          if (compareAndSetMemberCacheUpdated(MemberName.PARTITION_CACHE, true, false)) {
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
          size += getObjectSize(PartitionWrapper.class, wrapper);
        }
        partitionCache = newPartitionCache;
        updateMemberSize(MemberName.PARTITION_CACHE, size, SizeMode.Snapshot);
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public boolean updateTableColStats(List<ColumnStatisticsObj> colStatsForTable) {
      try {
        tableLock.writeLock().lock();
        int statsSize = 0;
        for (ColumnStatisticsObj colStatObj : colStatsForTable) {
          // Get old stats object if present
          String key = colStatObj.getColName();
          ColumnStatisticsObj oldStatsObj = tableColStatsCache.get(key);
          if (oldStatsObj != null) {
            // Update existing stat object's field
            statsSize -= getObjectSize(ColumnStatisticsObj.class, oldStatsObj);
            StatObjectConverter.setFieldsIntoOldStats(oldStatsObj, colStatObj);
          } else {
            // No stats exist for this key; add a new object to the cache
            // TODO: get rid of deepCopy after making sure callers don't use references
            tableColStatsCache.put(key, colStatObj.deepCopy());
          }
          statsSize += getObjectSize(ColumnStatisticsObj.class, colStatObj);
        }
        updateMemberSize(MemberName.TABLE_COL_STATS_CACHE, statsSize, SizeMode.Delta);
        setMemberCacheUpdated(MemberName.TABLE_COL_STATS_CACHE, true);
        return true;
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public void refreshTableColStats(List<ColumnStatisticsObj> colStatsForTable) {
      Map<String, ColumnStatisticsObj> newTableColStatsCache = new HashMap<String, ColumnStatisticsObj>();
      try {
        tableLock.writeLock().lock();
        int statsSize = 0;
        for (ColumnStatisticsObj colStatObj : colStatsForTable) {
          if (compareAndSetMemberCacheUpdated(MemberName.TABLE_COL_STATS_CACHE,true, false)) {
            LOG.debug("Skipping table col stats cache update for table: " + getTable().getTableName()
                + "; the table col stats list we have is dirty.");
            return;
          }
          String key = colStatObj.getColName();
          // TODO: get rid of deepCopy after making sure callers don't use references
          newTableColStatsCache.put(key, colStatObj.deepCopy());
          statsSize += getObjectSize(ColumnStatisticsObj.class, colStatObj);
        }
        tableColStatsCache = newTableColStatsCache;
        updateMemberSize(MemberName.TABLE_COL_STATS_CACHE, statsSize, SizeMode.Snapshot);
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public ColumnStatistics getCachedTableColStats(ColumnStatisticsDesc csd, List<String> colNames,
        String validWriteIds, boolean areTxnStatsSupported) throws MetaException {
      List<ColumnStatisticsObj> colStatObjs = new ArrayList<ColumnStatisticsObj>();
      try {
        tableLock.readLock().lock();
        for (String colName : colNames) {
          ColumnStatisticsObj colStatObj = tableColStatsCache.get(colName);
          if (colStatObj != null) {
            colStatObjs.add(colStatObj);
          }
        }
        ColumnStatistics colStat = new ColumnStatistics(csd, colStatObjs);
        colStat.setEngine(CacheUtils.HIVE_ENGINE);
        return CachedStore.adjustColStatForGet(getTable().getParameters(), colStat,
            getTable().getWriteId(), validWriteIds, areTxnStatsSupported);
      } finally {
        tableLock.readLock().unlock();
      }
    }

    public void removeTableColStats(String colName) {
      try {
        tableLock.writeLock().lock();
        if (colName == null) {
          tableColStatsCache.clear();
          updateMemberSize(MemberName.TABLE_COL_STATS_CACHE, 0, SizeMode.Snapshot);
        } else {
          tableColStatsCache.remove(colName);
          updateMemberSize(MemberName.TABLE_COL_STATS_CACHE, 0, SizeMode.Snapshot);
        }
        setMemberCacheUpdated(MemberName.TABLE_COL_STATS_CACHE, true);
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public void removeAllTableColStats() {
      try {
        tableLock.writeLock().lock();
        tableColStatsCache.clear();
        updateMemberSize(MemberName.TABLE_COL_STATS_CACHE, 0, SizeMode.Snapshot);
        setMemberCacheUpdated(MemberName.TABLE_COL_STATS_CACHE, true);
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public ColumStatsWithWriteId getPartitionColStats(List<String> partVal, String colName, String writeIdList) {
      try {
        tableLock.readLock().lock();
        ColumnStatisticsObj statisticsObj =
            partitionColStatsCache.get(CacheUtils.buildPartitonColStatsCacheKey(partVal, colName));
        if (statisticsObj == null || writeIdList == null) {
          return new ColumStatsWithWriteId(-1, statisticsObj);
        }
        PartitionWrapper wrapper = partitionCache.get(CacheUtils.buildPartitionCacheKey(partVal));
        if (wrapper == null) {
          LOG.info("Partition: " + partVal + " is not present in the cache. Cannot update stats in cache.");
          return null;
        }
        long writeId = wrapper.getPartition().getWriteId();
        ValidWriteIdList list4TheQuery = new ValidReaderWriteIdList(writeIdList);
        // Just check if the write ID is valid. If it's valid (i.e. we are allowed to see it),
        // that means it cannot possibly be a concurrent write. If it's not valid (we are not
        // allowed to see it), that means it's either concurrent or aborted, same thing for us.
        if (!list4TheQuery.isWriteIdValid(writeId)) {
          LOG.debug("Write id list " + writeIdList + " is not compatible with write id " + writeId);
          return null;
        }
        return new ColumStatsWithWriteId(writeId, statisticsObj);
      } finally {
        tableLock.readLock().unlock();
      }
    }

    public List<ColumnStatistics> getPartColStatsList(List<String> partNames, List<String> colNames, String writeIdList,
        boolean txnStatSupported) throws MetaException {
      List<ColumnStatistics> colStatObjs = new ArrayList<>();
      try {
        tableLock.readLock().lock();
        Table tbl = getTable();
        for (String partName : partNames) {
          ColumnStatisticsDesc csd = new ColumnStatisticsDesc(false, tbl.getDbName(), tbl.getTableName());
          csd.setCatName(tbl.getCatName());
          csd.setPartName(partName);
          csd.setLastAnalyzed(0); //TODO : Need to get last analysed. This is not being used by anybody now.
          List<ColumnStatisticsObj> statObject = new ArrayList<>();
          List<String> partVal = Warehouse.getPartValuesFromPartName(partName);
          for (String colName : colNames) {
            ColumnStatisticsObj statisticsObj =
                partitionColStatsCache.get(CacheUtils.buildPartitonColStatsCacheKey(partVal, colName));
            if (statisticsObj != null) {
              statObject.add(statisticsObj);
            } else {
              LOG.info("Stats not available in cachedStore for col " + colName + " in partition " + partVal);
              return null;
            }
          }
          ColumnStatistics columnStatistics = new ColumnStatistics(csd, statObject);
          columnStatistics.setEngine(CacheUtils.HIVE_ENGINE);
          if (writeIdList != null && TxnUtils.isTransactionalTable(getParameters())) {
            columnStatistics.setIsStatsCompliant(true);
            if (!txnStatSupported) {
              columnStatistics.setIsStatsCompliant(false);
            } else {
              PartitionWrapper wrapper = partitionCache.get(CacheUtils.buildPartitionCacheKey(partVal));
              if (wrapper == null) {
                columnStatistics.setIsStatsCompliant(false);
              } else {
                Partition partition = wrapper.getPartition();
                if (!ObjectStore
                    .isCurrentStatsValidForTheQuery(partition.getParameters(), partition.getWriteId(), writeIdList,
                        false)) {
                  LOG.debug("The current cached store transactional partition column statistics for {}.{}.{} "
                          + "(write ID {}) are not valid for current query ({})", tbl.getDbName(), tbl.getTableName(),
                      partName, partition.getWriteId(), writeIdList);
                  columnStatistics.setIsStatsCompliant(false);
                }
              }
            }
          }
          colStatObjs.add(columnStatistics);
        }
      } finally {
        tableLock.readLock().unlock();
      }
      return colStatObjs;
    }

    public boolean updatePartitionColStats(List<String> partVal, List<ColumnStatisticsObj> colStatsObjs) {
      try {
        tableLock.writeLock().lock();
        int statsSize = 0;
        for (ColumnStatisticsObj colStatObj : colStatsObjs) {
          // Get old stats object if present
          String key = CacheUtils.buildPartitonColStatsCacheKey(partVal, colStatObj.getColName());
          ColumnStatisticsObj oldStatsObj = partitionColStatsCache.get(key);
          if (oldStatsObj != null) {
            // Update existing stat object's field
            statsSize -= getObjectSize(ColumnStatisticsObj.class, oldStatsObj);
            StatObjectConverter.setFieldsIntoOldStats(oldStatsObj, colStatObj);
          } else {
            // No stats exist for this key; add a new object to the cache
            // TODO: get rid of deepCopy after making sure callers don't use references
            partitionColStatsCache.put(key, colStatObj.deepCopy());
          }
          statsSize += getObjectSize(ColumnStatisticsObj.class, colStatObj);
        }
        updateMemberSize(MemberName.PARTITION_COL_STATS_CACHE, statsSize, SizeMode.Delta);
        setMemberCacheUpdated(MemberName.PARTITION_COL_STATS_CACHE, true);
        // Invalidate cached aggregate stats
        if (!aggrColStatsCache.isEmpty()) {
          aggrColStatsCache.clear();
          updateMemberSize(MemberName.AGGR_COL_STATS_CACHE, 0, SizeMode.Snapshot);
        }
      } finally {
        tableLock.writeLock().unlock();
      }
      return true;
    }

    public void removePartitionColStats(List<String> partVals, String colName) {
      try {
        tableLock.writeLock().lock();
        ColumnStatisticsObj statsObj =
            partitionColStatsCache.remove(CacheUtils.buildPartitonColStatsCacheKey(partVals, colName));
        if (statsObj != null) {
          int statsSize = getObjectSize(ColumnStatisticsObj.class, statsObj);
          updateMemberSize(MemberName.PARTITION_COL_STATS_CACHE, -1 * statsSize, SizeMode.Delta);
        }
        setMemberCacheUpdated(MemberName.PARTITION_COL_STATS_CACHE, true);
        // Invalidate cached aggregate stats
        if (!aggrColStatsCache.isEmpty()) {
          aggrColStatsCache.clear();
          updateMemberSize(MemberName.AGGR_COL_STATS_CACHE, 0, SizeMode.Snapshot);
        }
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public void removeAllPartitionColStats() {
      try {
        tableLock.writeLock().lock();
        partitionColStatsCache.clear();
        updateMemberSize(MemberName.PARTITION_COL_STATS_CACHE, 0, SizeMode.Snapshot);
        setMemberCacheUpdated(MemberName.PARTITION_COL_STATS_CACHE, true);
        // Invalidate cached aggregate stats
        if (!aggrColStatsCache.isEmpty()) {
          aggrColStatsCache.clear();
          updateMemberSize(MemberName.AGGR_COL_STATS_CACHE, 0, SizeMode.Snapshot);
        }
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public void refreshPartitionColStats(List<ColumnStatistics> partitionColStats) {
      Map<String, ColumnStatisticsObj> newPartitionColStatsCache = new HashMap<String, ColumnStatisticsObj>();
      try {
        tableLock.writeLock().lock();
        String tableName = StringUtils.normalizeIdentifier(getTable().getTableName());
        int statsSize = 0;
        for (ColumnStatistics cs : partitionColStats) {
          if (compareAndSetMemberCacheUpdated(MemberName.PARTITION_COL_STATS_CACHE, true, false)) {
            LOG.debug("Skipping partition column stats cache update for table: " + getTable().getTableName()
                + "; the partition column stats list we have is dirty");
            return;
          }
          List<String> partVal;
          try {
            partVal = Warehouse.makeValsFromName(cs.getStatsDesc().getPartName(), null);
            List<ColumnStatisticsObj> colStatsObjs = cs.getStatsObj();
            for (ColumnStatisticsObj colStatObj : colStatsObjs) {
              if (compareAndSetMemberCacheUpdated(MemberName.PARTITION_COL_STATS_CACHE, true, false)) {
                LOG.debug("Skipping partition column stats cache update for table: " + getTable().getTableName()
                    + "; the partition column list we have is dirty");
                return;
              }
              String key = CacheUtils.buildPartitonColStatsCacheKey(partVal, colStatObj.getColName());
              newPartitionColStatsCache.put(key, colStatObj.deepCopy());
              statsSize += getObjectSize(ColumnStatisticsObj.class, colStatObj);
            }
          } catch (MetaException e) {
            LOG.debug("Unable to cache partition column stats for table: " + tableName, e);
          }
        }
        partitionColStatsCache = newPartitionColStatsCache;
        updateMemberSize(MemberName.PARTITION_COL_STATS_CACHE, statsSize, SizeMode.Snapshot);
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public List<ColumnStatisticsObj> getAggrPartitionColStats(List<String> colNames, StatsType statsType) {
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
        int statsSize = 0;
        if (aggrStatsAllPartitions != null) {
          for (ColumnStatisticsObj statObj : aggrStatsAllPartitions.getColStats()) {
            if (statObj != null) {
              List<ColumnStatisticsObj> aggrStats = new ArrayList<ColumnStatisticsObj>();
              aggrStats.add(StatsType.ALL.ordinal(), statObj.deepCopy());
              aggrColStatsCache.put(statObj.getColName(), aggrStats);
              statsSize += getObjectSize(ColumnStatisticsObj.class, statObj);
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
              statsSize += getObjectSize(ColumnStatisticsObj.class, statObj);
            }
          }
        }
        updateMemberSize(MemberName.AGGR_COL_STATS_CACHE, statsSize, SizeMode.Snapshot);
        setMemberCacheUpdated(MemberName.AGGR_COL_STATS_CACHE, true);
      } finally {
        tableLock.writeLock().unlock();
      }
    }

    public void refreshAggrPartitionColStats(AggrStats aggrStatsAllPartitions,
        AggrStats aggrStatsAllButDefaultPartition, SharedCache sharedCache, Map<List<String>, Long> partNameToWriteId) {
      Map<String, List<ColumnStatisticsObj>> newAggrColStatsCache = new HashMap<String, List<ColumnStatisticsObj>>();
      try {
        tableLock.writeLock().lock();
        int statsSize = 0;
        if (partNameToWriteId != null) {
          for (Entry<List<String>, Long> partValuesWriteIdSet : partNameToWriteId.entrySet()) {
            List<String> partValues = partValuesWriteIdSet.getKey();
            Partition partition = getPartition(partValues, sharedCache);
            if (partition == null) {
              LOG.info("Could not refresh the aggregate stat as partition " + partValues + " does not exist");
              return;
            }

            // for txn tables, if the write id is modified means the partition is updated post fetching of stats. So
            // skip updating the aggregate stats in the cache.
            long writeId = partition.getWriteId();
            if (writeId != partValuesWriteIdSet.getValue()) {
              LOG.info("Could not refresh the aggregate stat as partition " + partValues + " has write id "
                  + partValuesWriteIdSet.getValue() + " instead of " + writeId);
              return;
            }
          }
        }
        if (aggrStatsAllPartitions != null) {
          for (ColumnStatisticsObj statObj : aggrStatsAllPartitions.getColStats()) {
            if (compareAndSetMemberCacheUpdated(MemberName.AGGR_COL_STATS_CACHE, true, false)) {
              LOG.debug("Skipping aggregate stats cache update for table: " + getTable().getTableName()
                  + "; the aggregate stats list we have is dirty");
              return;
            }
            if (statObj != null) {
              List<ColumnStatisticsObj> aggrStats = new ArrayList<ColumnStatisticsObj>();
              aggrStats.add(StatsType.ALL.ordinal(), statObj.deepCopy());
              newAggrColStatsCache.put(statObj.getColName(), aggrStats);
              statsSize += getObjectSize(ColumnStatisticsObj.class, statObj);
            }
          }
        }
        if (aggrStatsAllButDefaultPartition != null) {
          for (ColumnStatisticsObj statObj : aggrStatsAllButDefaultPartition.getColStats()) {
            if (compareAndSetMemberCacheUpdated(MemberName.AGGR_COL_STATS_CACHE, true, false)) {
              LOG.debug("Skipping aggregate stats cache update for table: " + getTable().getTableName()
                  + "; the aggregate stats list we have is dirty");
              return;
            }
            if (statObj != null) {
              List<ColumnStatisticsObj> aggrStats = newAggrColStatsCache.get(statObj.getColName());
              if (aggrStats == null) {
                aggrStats = new ArrayList<ColumnStatisticsObj>();
              }
              aggrStats.add(StatsType.ALLBUTDEFAULT.ordinal(), statObj.deepCopy());
              statsSize += getObjectSize(ColumnStatisticsObj.class, statObj);
            }
          }
        }
        aggrColStatsCache = newAggrColStatsCache;
        updateMemberSize(MemberName.AGGR_COL_STATS_CACHE, statsSize, SizeMode.Snapshot);
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

      if (tblCopy.getSd() != null) {
        sdHash = MetaStoreServerUtils.hashStorageDescriptor(tblCopy.getSd(), md);
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
      tblCopy.setTableName(tblCopy.getTableName().toLowerCase());
      setTable(tblCopy);
    }

    private PartitionWrapper makePartitionWrapper(Partition part, SharedCache sharedCache) {
      Partition partCopy = part.deepCopy();
      PartitionWrapper wrapper;
      if (part.getSd() != null) {
        byte[] sdHash = MetaStoreServerUtils.hashStorageDescriptor(part.getSd(), md);
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
    private Partition p;
    private String location;
    private Map<String, String> parameters;
    private byte[] sdHash;

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
    private StorageDescriptor sd;
    private int refCount = 0;

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

  public static class ColumStatsWithWriteId {
    private long writeId;
    private ColumnStatisticsObj columnStatisticsObj;

    public ColumStatsWithWriteId(long writeId, ColumnStatisticsObj columnStatisticsObj) {
      this.writeId = writeId;
      this.columnStatisticsObj = columnStatisticsObj;
    }

    public long getWriteId() {
      return writeId;
    }

    public ColumnStatisticsObj getColumnStatisticsObj() {
      return columnStatisticsObj;
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
        // 2. Skip overwriting existing db object
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
        // 2. Skip overwriting existing db object
        // (which is present because it was added after prewarm started)
        String key = CacheUtils.buildDbKey(dbCopy.getCatalogName().toLowerCase(), dbCopy.getName().toLowerCase());
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
        if (catName.equals(n[0])) {
          results.add(n[1]);
        }
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

  public boolean populateTableInCache(Table table, TableCacheObjects cacheObjects) {
    String catName = StringUtils.normalizeIdentifier(table.getCatName());
    String dbName = StringUtils.normalizeIdentifier(table.getDbName());
    String tableName = StringUtils.normalizeIdentifier(table.getTableName());
    SQLAllTableConstraints constraints = cacheObjects.getTableConstraints();
    // Since we allow write operations on cache while prewarm is happening:
    // 1. Don't add tables that were deleted while we were preparing list for prewarm
    if (tablesDeletedDuringPrewarm.contains(CacheUtils.buildTableKey(catName, dbName, tableName))) {
      return false;
    }
    TableWrapper tblWrapper = createTableWrapper(catName, dbName, tableName, table);
    if (!table.isSetPartitionKeys() && (cacheObjects.getTableColStats() != null)) {
      if (table.getPartitionKeys().isEmpty() && (cacheObjects.getTableColStats() != null)) {
        return false;
      }
    } else {
      if (cacheObjects.getPartitions() != null) {
        // If the partitions were not added due to memory limit, return false
        if (!tblWrapper.cachePartitions(cacheObjects.getPartitions(), this, true)) {
          return false;
        }
      }
      if (cacheObjects.getPartitionColStats() != null) {
        for (ColumnStatistics cs : cacheObjects.getPartitionColStats()) {
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
      tblWrapper.cacheAggrPartitionColStats(cacheObjects.getAggrStatsAllPartitions(),
        cacheObjects.getAggrStatsAllButDefaultPartition());
    }
    tblWrapper.setMemberCacheUpdated(MemberName.PARTITION_CACHE, false);
    tblWrapper.setMemberCacheUpdated(MemberName.TABLE_COL_STATS_CACHE, false);
    tblWrapper.setMemberCacheUpdated(MemberName.PARTITION_COL_STATS_CACHE, false);
    tblWrapper.setMemberCacheUpdated(MemberName.AGGR_COL_STATS_CACHE, false);

    if (tblWrapper.cacheConstraints(constraints, true)) {
      tblWrapper.setMemberCacheUpdated(MemberName.PRIMARY_KEY_CACHE, false);
      tblWrapper.setMemberCacheUpdated(MemberName.FOREIGN_KEY_CACHE, false);
      tblWrapper.setMemberCacheUpdated(MemberName.NOTNULL_CONSTRAINT_CACHE, false);
      tblWrapper.setMemberCacheUpdated(MemberName.UNIQUE_CONSTRAINT_CACHE, false);
      tblWrapper.setMemberCacheUpdated(MemberName.DEFAULT_CONSTRAINT_CACHE, false);
      tblWrapper.setMemberCacheUpdated(MemberName.CHECK_CONSTRAINT_CACHE, false);
    } else {
      return false;
    }

    try {
      cacheLock.writeLock().lock();
      // 2. Skip overwriting existing table object
      // (which is present because it was added after prewarm started)
      tableCache.put(CacheUtils.buildTableKey(catName, dbName, tableName), tblWrapper);
      return true;
    } finally {
      cacheLock.writeLock().unlock();
    }
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
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tableName));
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

  private TableWrapper createTableWrapper(String catName, String dbName, String tblName, Table tbl) {
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
      byte[] sdHash = MetaStoreServerUtils.hashStorageDescriptor(tbl.getSd(), md);
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
      String tblKey = CacheUtils.buildTableKey(catName, dbName, tblName);
      TableWrapper tblWrapper = tableCache.getIfPresent(tblKey);
      if (tblWrapper == null) {
        //in case of retry, ignore second try.
        return;
      }

      byte[] sdHash = tblWrapper.getSdHash();
      if (sdHash != null) {
        decrSd(sdHash);
      }
      tableCache.invalidate(tblKey);
      isTableCacheDirty.set(true);
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  public void alterTableInCache(String catName, String dbName, String tblName, Table newTable) {
    try {
      cacheLock.writeLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.updateTableObj(newTable, this);
        tableCache.put(CacheUtils.buildTableKey(catName, newTable.getDbName(), newTable.getTableName()), tblWrapper);
        isTableCacheDirty.set(true);
      }
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  public void alterTableAndStatsInCache(String catName, String dbName, String tblName, long writeId,
      List<ColumnStatisticsObj> colStatsObjs, Map<String, String> newParams) {
    try {
      cacheLock.writeLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper == null) {
        LOG.info("Table " + tblName + " is missing from cache. Cannot update table stats in cache");
        return;
      }
      Table newTable = tblWrapper.getTable();
      newTable.setWriteId(writeId);
      newTable.setParameters(newParams);
      //tblWrapper.updateTableObj(newTable, this);
      String newDbName = StringUtils.normalizeIdentifier(newTable.getDbName());
      String newTblName = StringUtils.normalizeIdentifier(newTable.getTableName());
      tblWrapper.updateTableColStats(colStatsObjs);
      tableCache.put(CacheUtils.buildTableKey(catName, newDbName, newTblName), tblWrapper);
      isTableCacheDirty.set(true);
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  public List<Table> listCachedTables(String catName, String dbName) {
    List<Table> tables = new ArrayList<>();
    try {
      cacheLock.readLock().lock();
      for (TableWrapper wrapper : tableCache.asMap().values()) {
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
      for (TableWrapper wrapper : tableCache.asMap().values()) {
        if (wrapper.sameDatabase(catName, dbName)) {
          tableNames.add(StringUtils.normalizeIdentifier(wrapper.getTable().getTableName()));
        }
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return tableNames;
  }

  public List<String> listCachedTableNames(String catName, String dbName, String pattern, int maxTables) {
    List<String> tableNames = new ArrayList<>();
    try {
      cacheLock.readLock().lock();
      int count = 0;
      for (TableWrapper wrapper : tableCache.asMap().values()) {
        if (wrapper.sameDatabase(catName, dbName) && CacheUtils.matches(wrapper.getTable().getTableName(), pattern) && (
            maxTables == -1 || count < maxTables)) {
          tableNames.add(StringUtils.normalizeIdentifier(wrapper.getTable().getTableName()));
          count++;
        }
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return tableNames;
  }

  public List<String> listCachedTableNames(String catName, String dbName, String pattern, TableType tableType,
      int limit) {
    List<String> tableNames = new ArrayList<>();
    try {
      cacheLock.readLock().lock();
      int count = 0;
      for (TableWrapper wrapper : tableCache.asMap().values()) {
        if (wrapper.sameDatabase(catName, dbName) && CacheUtils.matches(wrapper.getTable().getTableName(), pattern)
            && wrapper.getTable().getTableType().equals(tableType.toString()) && (limit == -1 || count < limit)) {
          tableNames.add(StringUtils.normalizeIdentifier(wrapper.getTable().getTableName()));
          count++;
        }
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return tableNames;
  }

  public boolean refreshTablesInCache(String catName, String dbName, List<Table> tables) {
    if (isTableCacheDirty.compareAndSet(true, false)) {
      LOG.debug("Skipping table cache update; the table list we have is dirty.");
      return false;
    }
    Map<String, TableWrapper> newCacheForDB = new TreeMap<>();
    for (Table tbl : tables) {
      String tblName = StringUtils.normalizeIdentifier(tbl.getTableName());
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.updateTableObj(tbl, this);
      } else {
        tblWrapper = createTableWrapper(catName, dbName, tblName, tbl);
      }
      newCacheForDB.put(CacheUtils.buildTableKey(catName, dbName, tblName), tblWrapper);
    }
    try {
      cacheLock.writeLock().lock();
      Iterator<Entry<String, TableWrapper>> entryIterator = tableCache.asMap().entrySet().iterator();
      while (entryIterator.hasNext()) {
        String key = entryIterator.next().getKey();
        if (key.startsWith(CacheUtils.buildDbKeyWithDelimiterSuffix(catName, dbName))) {
          entryIterator.remove();
        }
      }
      tableCache.putAll(newCacheForDB);
      return true;
    } finally {
      cacheLock.writeLock().unlock();
    }
  }

  public ColumnStatistics getTableColStatsFromCache(String catName, String dbName, String tblName,
      List<String> colNames, String validWriteIds, boolean areTxnStatsSupported) throws MetaException {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper == null) {
        LOG.info("Table " + tblName + " is missing from cache.");
        return null;
      }
      ColumnStatisticsDesc csd = new ColumnStatisticsDesc(true, dbName, tblName);
      return tblWrapper.getCachedTableColStats(csd, colNames, validWriteIds, areTxnStatsSupported);
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void removeTableColStatsFromCache(String catName, String dbName, String tblName, String colName) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.removeTableColStats(colName);
      } else {
        LOG.info("Table " + tblName + " is missing from cache.");
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void removeAllTableColStatsFromCache(String catName, String dbName, String tblName) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.removeAllTableColStats();
      } else {
        LOG.info("Table " + tblName + " is missing from cache.");
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void updateTableColStatsInCache(String catName, String dbName, String tableName,
      List<ColumnStatisticsObj> colStatsForTable) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tableName));
      if (tblWrapper != null) {
        tblWrapper.updateTableColStats(colStatsForTable);
      } else {
        LOG.info("Table " + tableName + " is missing from cache.");
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void refreshTableColStatsInCache(String catName, String dbName, String tableName,
      List<ColumnStatisticsObj> colStatsForTable) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tableName));
      if (tblWrapper != null) {
        tblWrapper.refreshTableColStats(colStatsForTable);
      } else {
        LOG.info("Table " + tableName + " is missing from cache.");
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public int getCachedTableCount() {
    try {
      cacheLock.readLock().lock();
      return tableCache.asMap().size();
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public List<TableMeta> getTableMeta(String catName, String dbNames, String tableNames, List<String> tableTypes) {
    List<TableMeta> tableMetas = new ArrayList<>();
    try {
      cacheLock.readLock().lock();
      for (String dbName : listCachedDatabases(catName)) {
        if (CacheUtils.matches(dbName, dbNames)) {
          for (Table table : listCachedTables(catName, dbName)) {
            if (CacheUtils.matches(table.getTableName(), tableNames)) {
              if (tableTypes == null || tableTypes.contains(table.getTableType())) {
                TableMeta metaData = new TableMeta(dbName, table.getTableName(), table.getTableType());
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
      String tblKey = CacheUtils.buildTableKey(catName, dbName, tblName);
      TableWrapper tblWrapper = tableCache.getIfPresent(tblKey);
      if (tblWrapper != null) {
        tblWrapper.cachePartition(part, this);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void addPrimaryKeysToCache(String catName, String dbName, String tblName, List<SQLPrimaryKey> keys) {
    try {
      cacheLock.readLock().lock();
      String tblKey = CacheUtils.buildTableKey(catName, dbName, tblName);
      TableWrapper tblWrapper = tableCache.getIfPresent(tblKey);
      if ((tblWrapper != null) && tblWrapper.isConstraintsValid()) {
        // Because lake of snapshot freshness validation.
        // For now disabling cached constraint snapshot addition in parts by invalidating constraint snapshot.
        tblWrapper.invalidateConstraintsCache();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void addForeignKeysToCache(String catName, String dbName, String tblName, List<SQLForeignKey> keys) {
    try {
      cacheLock.readLock().lock();
      String tblKey = CacheUtils.buildTableKey(catName, dbName, tblName);
      TableWrapper tblWrapper = tableCache.getIfPresent(tblKey);
      if ((tblWrapper != null) && tblWrapper.isConstraintsValid()) {
        // Because lake of snapshot freshness validation.
        // For now disabling cached constraint snapshot addition in parts by invalidating constraint snapshot.
        tblWrapper.invalidateConstraintsCache();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void addUniqueConstraintsToCache(String catName, String dbName, String tblName, List<SQLUniqueConstraint> keys) {
    try {
      cacheLock.readLock().lock();
      String tblKey = CacheUtils.buildTableKey(catName, dbName, tblName);
      TableWrapper tblWrapper = tableCache.getIfPresent(tblKey);
      if ((tblWrapper != null) && tblWrapper.isConstraintsValid()) {
        // Because lake of snapshot freshness validation.
        // For now disabling cached constraint snapshot addition in parts by invalidating constraint snapshot.
        tblWrapper.invalidateConstraintsCache();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void addNotNullConstraintsToCache(String catName, String dbName, String tblName, List<SQLNotNullConstraint> keys) {
    try {
      cacheLock.readLock().lock();
      String tblKey = CacheUtils.buildTableKey(catName, dbName, tblName);
      TableWrapper tblWrapper = tableCache.getIfPresent(tblKey);
      if ((tblWrapper != null) && tblWrapper.isConstraintsValid()) {
        // Because lake of snapshot freshness validation.
        // For now disabling cached constraint snapshot addition in parts by invalidating constraint snapshot.
        tblWrapper.invalidateConstraintsCache();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void addDefaultConstraintsToCache(String catName, String dbName, String tblName, List<SQLDefaultConstraint> keys) {
    try {
      cacheLock.readLock().lock();
      String tblKey = CacheUtils.buildTableKey(catName, dbName, tblName);
      TableWrapper tblWrapper = tableCache.getIfPresent(tblKey);
      if ((tblWrapper != null) && tblWrapper.isConstraintsValid()) {
        // Because lake of snapshot freshness validation.
        // For now disabling cached constraint snapshot addition in parts by invalidating constraint snapshot.
        tblWrapper.invalidateConstraintsCache();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void addCheckConstraintsToCache(String catName, String dbName, String tblName, List<SQLCheckConstraint> keys) {
    try {
      cacheLock.readLock().lock();
      String tblKey = CacheUtils.buildTableKey(catName, dbName, tblName);
      TableWrapper tblWrapper = tableCache.getIfPresent(tblKey);
      if ((tblWrapper != null) && tblWrapper.isConstraintsValid()) {
        // Because lake of snapshot freshness validation.
        // For now disabling cached constraint snapshot addition in parts by invalidating constraint snapshot.
        tblWrapper.invalidateConstraintsCache();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void addTableConstraintsToCache(String catName, String dbName, String tblName, SQLAllTableConstraints constraints){
    try {
      cacheLock.readLock().lock();
      String tblKey = CacheUtils.buildTableKey(catName, dbName, tblName);
      TableWrapper tblWrapper = tableCache.getIfPresent(tblKey);
      if (tblWrapper != null) {
        tblWrapper.cacheConstraints(constraints, false);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void addPartitionsToCache(String catName, String dbName, String tblName, Iterable<Partition> parts) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.cachePartitions(parts, this, false);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public Partition getPartitionFromCache(String catName, String dbName, String tblName, List<String> partVals) {
    Partition part = null;
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        part = tblWrapper.getPartition(partVals, this);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return part;
  }

  public boolean existPartitionFromCache(String catName, String dbName, String tblName, List<String> partVals) {
    boolean existsPart = false;
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        existsPart = tblWrapper.containsPartition(partVals);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return existsPart;
  }

  public void removeConstraintFromCache(String catName, String dbName, String tblName, String constraintName) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if ((tblWrapper != null) && tblWrapper.isConstraintsValid()) {
        // Because lake of snapshot freshness validation.
        // For now disabling cached constraint snapshot deletion in parts by invalidating constraint snapshot.
        tblWrapper.invalidateConstraintsCache();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public Partition removePartitionFromCache(String catName, String dbName, String tblName, List<String> partVals) {
    Partition part = null;
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        part = tblWrapper.removePartition(partVals, this);
      } else {
        LOG.warn("This is abnormal");
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return part;
  }

  public void removePartitionsFromCache(String catName, String dbName, String tblName, List<List<String>> partVals) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.removePartitions(partVals, this);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public List<Partition> listCachedPartitions(String catName, String dbName, String tblName, int max) {
    List<Partition> parts = new ArrayList<Partition>();
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        parts = tblWrapper.listPartitions(max, this);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return parts;
  }

  public List<SQLPrimaryKey> listCachedPrimaryKeys(String catName, String dbName, String tblName) {
    List<SQLPrimaryKey> keys = new ArrayList<>();
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if ((tblWrapper != null) && tblWrapper.isConstraintsValid()) {
        keys = tblWrapper.getPrimaryKeys();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return keys;
  }

  public SQLAllTableConstraints listCachedAllTableConstraints(String catName, String dbName, String tblName) {
    SQLAllTableConstraints constraints = new SQLAllTableConstraints();
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if ((tblWrapper != null) && tblWrapper.isConstraintsValid()) {
        constraints = tblWrapper.getAllTableConstraints();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return constraints;
  }

  public List<SQLForeignKey> listCachedForeignKeys(String catName, String foreignDbName, String foreignTblName,
                                                   String parentDbName, String parentTblName) {
    List<SQLForeignKey> keys = new ArrayList<>();
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, foreignDbName, foreignTblName));
      if ((tblWrapper != null) && tblWrapper.isConstraintsValid()) {
        keys = tblWrapper.getForeignKeys();
      }
    } finally {
      cacheLock.readLock().unlock();
    }

    // filter out required foreign keys based on parent db/tbl name
    if (!StringUtils.isEmpty(parentTblName) && !StringUtils.isEmpty(parentDbName)) {
      return keys
        .stream()
        .filter(key -> parentDbName.equalsIgnoreCase(key.getPktable_db())
          && parentTblName.equalsIgnoreCase(key.getPktable_name()))
        .collect(Collectors.toList());
    }
    return keys;
  }

  public List<SQLUniqueConstraint> listCachedUniqueConstraint(String catName, String dbName, String tblName) {
    List<SQLUniqueConstraint> keys = new ArrayList<>();
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if ((tblWrapper != null) && tblWrapper.isConstraintsValid()) {
        keys = tblWrapper.getUniqueConstraints();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return keys;
  }

  public List<SQLDefaultConstraint> listCachedDefaultConstraint(String catName, String dbName, String tblName) {
    List<SQLDefaultConstraint> keys = new ArrayList<>();
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if ((tblWrapper != null) && tblWrapper.isConstraintsValid()) {
        keys = tblWrapper.getDefaultConstraints();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return keys;
  }

  public List<SQLCheckConstraint> listCachedCheckConstraint(String catName, String dbName, String tblName) {
    List<SQLCheckConstraint> keys = new ArrayList<>();
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if ((tblWrapper != null) && tblWrapper.isConstraintsValid()) {
        keys = tblWrapper.getCheckConstraints();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return keys;
  }

  public List<SQLNotNullConstraint> listCachedNotNullConstraints(String catName, String dbName, String tblName) {
    List<SQLNotNullConstraint> keys = new ArrayList<>();
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if ((tblWrapper != null) && tblWrapper.isConstraintsValid()) {
        keys = tblWrapper.getNotNullConstraints();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return keys;
  }

  public boolean isTableConstraintValid(String catName, String dbName, String tblName){
    boolean isValid = false;
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        isValid = tblWrapper.isConstraintsValid();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return isValid;
  }

  public void refreshAllTableConstraintsInCache(String catName, String dbName, String tblName,
      SQLAllTableConstraints constraints) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.refreshAllTableConstraints(constraints);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void alterPartitionInCache(String catName, String dbName, String tblName, List<String> partVals,
      Partition newPart) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.alterPartition(partVals, newPart, this);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void alterPartitionAndStatsInCache(String catName, String dbName, String tblName, long writeId,
      List<String> partVals, Map<String, String> parameters, List<ColumnStatisticsObj> colStatsObjs) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.alterPartitionAndStats(partVals, this, writeId, parameters, colStatsObjs);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void alterPartitionsInCache(String catName, String dbName, String tblName, List<List<String>> partValsList,
      List<Partition> newParts) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.alterPartitions(partValsList, newParts, this);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void refreshPartitionsInCache(String catName, String dbName, String tblName, List<Partition> partitions) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.refreshPartitions(partitions, this);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void removePartitionColStatsFromCache(String catName, String dbName, String tblName, List<String> partVals,
      String colName) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
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
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.removeAllPartitionColStats();
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void updatePartitionColStatsInCache(String catName, String dbName, String tableName, List<String> partVals,
      List<ColumnStatisticsObj> colStatsObjs) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tableName));
      if (tblWrapper != null) {
        tblWrapper.updatePartitionColStats(partVals, colStatsObjs);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public ColumStatsWithWriteId getPartitionColStatsFromCache(String catName, String dbName, String tblName,
      List<String> partVal, String colName, String writeIdList) {
    ColumStatsWithWriteId colStatObj = null;
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        colStatObj = tblWrapper.getPartitionColStats(partVal, colName, writeIdList);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return colStatObj;
  }

  public List<ColumnStatistics> getPartitionColStatsListFromCache(String catName, String dbName, String tblName,
      List<String> partNames, List<String> colNames, String writeIdList, boolean txnStatSupported) {
    List<ColumnStatistics> colStatObjs = null;
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        colStatObjs = tblWrapper.getPartColStatsList(partNames, colNames, writeIdList, txnStatSupported);
      }
    } catch (MetaException e) {
      LOG.warn("Failed to get partition column statistics");
    } finally {
      cacheLock.readLock().unlock();
    }
    return colStatObjs;
  }

  public void refreshPartitionColStatsInCache(String catName, String dbName, String tblName,
      List<ColumnStatistics> partitionColStats) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.refreshPartitionColStats(partitionColStats);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public List<ColumnStatisticsObj> getAggrStatsFromCache(String catName, String dbName, String tblName,
      List<String> colNames, StatsType statsType) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        return tblWrapper.getAggrPartitionColStats(colNames, statsType);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
    return null;
  }

  public void addAggregateStatsToCache(String catName, String dbName, String tblName, AggrStats aggrStatsAllPartitions,
      AggrStats aggrStatsAllButDefaultPartition) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.cacheAggrPartitionColStats(aggrStatsAllPartitions, aggrStatsAllButDefaultPartition);
      }
    } finally {
      cacheLock.readLock().unlock();
    }
  }

  public void refreshAggregateStatsInCache(String catName, String dbName, String tblName,
      AggrStats aggrStatsAllPartitions, AggrStats aggrStatsAllButDefaultPartition,
      Map<List<String>, Long> partNameToWriteId) {
    try {
      cacheLock.readLock().lock();
      TableWrapper tblWrapper = tableCache.getIfPresent(CacheUtils.buildTableKey(catName, dbName, tblName));
      if (tblWrapper != null) {
        tblWrapper.refreshAggrPartitionColStats(aggrStatsAllPartitions, aggrStatsAllButDefaultPartition, this,
            partNameToWriteId);
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
  void clearTableCache() {
    tableCache.invalidateAll();
  }

  @VisibleForTesting
  Map<ByteArrayWrapper, StorageDescriptorWrapper> getSdCache() {
    return sdCache;
  }

  /**
   * This resets the contents of the catalog cache so that we can re-fill it in another test.
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

  public void printCacheStats() {
    CacheStats cs = tableCache.stats();
    LOG.info(cs.toString());
  }

  public long getUpdateCount() {
    return cacheUpdateCount.get();
  }

  public void incrementUpdateCount() {
    cacheUpdateCount.incrementAndGet();
  }


}
