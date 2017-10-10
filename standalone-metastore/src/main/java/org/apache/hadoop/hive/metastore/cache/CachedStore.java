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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.Deadline;
import org.apache.hadoop.hive.metastore.FileMetadataHandler;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.PartFilterExprUtil;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

// TODO filter->expr
// TODO functionCache
// TODO constraintCache
// TODO need sd nested copy?
// TODO String intern
// TODO restructure HBaseStore
// TODO monitor event queue
// TODO initial load slow?
// TODO size estimation
// TODO factor in extrapolation logic (using partitions found) during aggregate stats calculation

public class CachedStore implements RawStore, Configurable {
  private static ScheduledExecutorService cacheUpdateMaster = null;
  private static ReentrantReadWriteLock databaseCacheLock = new ReentrantReadWriteLock(true);
  private static AtomicBoolean isDatabaseCacheDirty = new AtomicBoolean(false);
  private static ReentrantReadWriteLock tableCacheLock = new ReentrantReadWriteLock(true);
  private static AtomicBoolean isTableCacheDirty = new AtomicBoolean(false);
  private static ReentrantReadWriteLock partitionCacheLock = new ReentrantReadWriteLock(true);
  private static AtomicBoolean isPartitionCacheDirty = new AtomicBoolean(false);
  private static ReentrantReadWriteLock tableColStatsCacheLock = new ReentrantReadWriteLock(true);
  private static AtomicBoolean isTableColStatsCacheDirty = new AtomicBoolean(false);
  private static ReentrantReadWriteLock partitionColStatsCacheLock = new ReentrantReadWriteLock(
      true);
  private static AtomicBoolean isPartitionColStatsCacheDirty = new AtomicBoolean(false);
  private RawStore rawStore = null;
  private Configuration conf;
  private PartitionExpressionProxy expressionProxy = null;
  // Default value set to 100 milliseconds for test purpose
  private static long cacheRefreshPeriod = 100;

  /** A wrapper over SharedCache. Allows one to get SharedCache safely; should be merged
   *  into SharedCache itself (see the TODO on the class). */
  private static final SharedCacheWrapper sharedCacheWrapper = new SharedCacheWrapper();

  static final private Logger LOG = LoggerFactory.getLogger(CachedStore.class.getName());

  static class TableWrapper {
    Table t;
    String location;
    Map<String, String> parameters;
    byte[] sdHash;
    TableWrapper(Table t, byte[] sdHash, String location, Map<String, String> parameters) {
      this.t = t;
      this.sdHash = sdHash;
      this.location = location;
      this.parameters = parameters;
    }
    public Table getTable() {
      return t;
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

  public CachedStore() {
  }

  public static void initSharedCacheAsync(Configuration conf) {
    String clazzName = null;
    boolean isEnabled = false;
    try {
      clazzName = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.RAW_STORE_IMPL);
      isEnabled = JavaUtils.getClass(clazzName, RawStore.class).isAssignableFrom(CachedStore.class);
    } catch (MetaException e) {
      LOG.error("Cannot instantiate metastore class", e);
    }
    if (!isEnabled) {
      LOG.debug("CachedStore is not enabled; using " + clazzName);
      return;
    }
    sharedCacheWrapper.startInit(conf);
  }

  @Override
  public void setConf(Configuration conf) {
    String rawStoreClassName = MetastoreConf.getVar(conf, ConfVars.CACHED_RAW_STORE_IMPL,
        ObjectStore.class.getName());
    if (rawStore == null) {
      try {
        rawStore = (JavaUtils.getClass(rawStoreClassName, RawStore.class)).newInstance();
      } catch (Exception e) {
        throw new RuntimeException("Cannot instantiate " + rawStoreClassName, e);
      }
    }
    rawStore.setConf(conf);
    Configuration oldConf = this.conf;
    this.conf = conf;
    if (expressionProxy != null && conf != oldConf) {
      LOG.warn("Unexpected setConf when we were already configured");
    }
    if (expressionProxy == null || conf != oldConf) {
      expressionProxy = PartFilterExprUtil.createExpressionProxy(conf);
    }
  }

  @VisibleForTesting
  static void prewarm(RawStore rawStore) throws Exception {
    // Prevents throwing exceptions in our raw store calls since we're not using RawStoreProxy
    Deadline.registerIfNot(1000000);
    List<String> dbNames = rawStore.getAllDatabases();
    SharedCache sharedCache = sharedCacheWrapper.getUnsafe();
    for (String dbName : dbNames) {
      Database db = rawStore.getDatabase(dbName);
      sharedCache.addDatabaseToCache(StringUtils.normalizeIdentifier(dbName), db);
      List<String> tblNames = rawStore.getAllTables(dbName);
      for (String tblName : tblNames) {
        Table table = rawStore.getTable(dbName, tblName);
        sharedCache.addTableToCache(StringUtils.normalizeIdentifier(dbName),
            StringUtils.normalizeIdentifier(tblName), table);
        Deadline.startTimer("getPartitions");
        List<Partition> partitions = rawStore.getPartitions(dbName, tblName, Integer.MAX_VALUE);
        Deadline.stopTimer();
        for (Partition partition : partitions) {
          sharedCache.addPartitionToCache(StringUtils.normalizeIdentifier(dbName),
              StringUtils.normalizeIdentifier(tblName), partition);
        }
        // Cache partition column stats
        Deadline.startTimer("getColStatsForTablePartitions");
        Map<String, List<ColumnStatisticsObj>> colStatsPerPartition =
            rawStore.getColStatsForTablePartitions(dbName, tblName);
        Deadline.stopTimer();
        if (colStatsPerPartition != null) {
          sharedCache.addPartitionColStatsToCache(dbName, tblName, colStatsPerPartition);
        }
        // Cache table column stats
        List<String> colNames = MetaStoreUtils.getColumnNamesForTable(table);
        Deadline.startTimer("getTableColumnStatistics");
        ColumnStatistics tableColStats =
            rawStore.getTableColumnStatistics(dbName, tblName, colNames);
        Deadline.stopTimer();
        if ((tableColStats != null) && (tableColStats.getStatsObjSize() > 0)) {
          sharedCache.addTableColStatsToCache(StringUtils.normalizeIdentifier(dbName),
              StringUtils.normalizeIdentifier(tblName), tableColStats.getStatsObj());
        }
      }
    }
  }

  @VisibleForTesting
  synchronized static void startCacheUpdateService(Configuration conf) {
    if (cacheUpdateMaster == null) {
      cacheUpdateMaster = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread t = Executors.defaultThreadFactory().newThread(r);
          t.setName("CachedStore-CacheUpdateService: Thread-" + t.getId());
          t.setDaemon(true);
          return t;
        }
      });
      if (!MetastoreConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST)) {
        cacheRefreshPeriod =
            MetastoreConf.getTimeVar(conf, ConfVars.CACHED_RAW_STORE_CACHE_UPDATE_FREQUENCY,
                TimeUnit.MILLISECONDS);
      }
      LOG.info("CachedStore: starting cache update service (run every " + cacheRefreshPeriod + "ms");
      cacheUpdateMaster.scheduleAtFixedRate(new CacheUpdateMasterWork(conf), 0,
          cacheRefreshPeriod, TimeUnit.MILLISECONDS);
    }
  }

  @VisibleForTesting
  synchronized static boolean stopCacheUpdateService(long timeout) {
    boolean tasksStoppedBeforeShutdown = false;
    if (cacheUpdateMaster != null) {
      LOG.info("CachedStore: shutting down cache update service");
      try {
        tasksStoppedBeforeShutdown =
            cacheUpdateMaster.awaitTermination(timeout, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.info("CachedStore: cache update service was interrupted while waiting for tasks to "
            + "complete before shutting down. Will make a hard stop now.");
      }
      cacheUpdateMaster.shutdownNow();
      cacheUpdateMaster = null;
    }
    return tasksStoppedBeforeShutdown;
  }

  @VisibleForTesting
  static void setCacheRefreshPeriod(long time) {
    cacheRefreshPeriod = time;
  }

  static class CacheUpdateMasterWork implements Runnable {
    private boolean isFirstRun = true;
    private final RawStore rawStore;

    public CacheUpdateMasterWork(Configuration conf) {
      String rawStoreClassName = MetastoreConf.getVar(conf, ConfVars.CACHED_RAW_STORE_IMPL,
          ObjectStore.class.getName());
      try {
        rawStore = JavaUtils.getClass(rawStoreClassName, RawStore.class).newInstance();
        rawStore.setConf(conf);
      } catch (InstantiationException | IllegalAccessException | MetaException e) {
        // MetaException here really means ClassNotFound (see the utility method).
        // So, if any of these happen, that means we can never succeed.
        sharedCacheWrapper.updateInitState(e, true);
        throw new RuntimeException("Cannot instantiate " + rawStoreClassName, e);
      }
    }

    @Override
    public void run() {
      if (isFirstRun) {
        while (isFirstRun) {
          try {
            LOG.info("Prewarming CachedStore");
            prewarm(rawStore);
            LOG.info("CachedStore initialized");
          } catch (Exception e) {
            LOG.error("Prewarm failure", e);
            sharedCacheWrapper.updateInitState(e, false);
            return;
          }
          sharedCacheWrapper.updateInitState(null, false);
          isFirstRun = false;
        }
      } else {
        // TODO: prewarm and update can probably be merged.
        update();
      }
    }

    public void update() {
      Deadline.registerIfNot(1000000);
      LOG.debug("CachedStore: updating cached objects");
      try {
        List<String> dbNames = rawStore.getAllDatabases();
        if (dbNames != null) {
          // Update the database in cache
          updateDatabases(rawStore, dbNames);
          for (String dbName : dbNames) {
            // Update the tables in cache
            updateTables(rawStore, dbName);
            List<String> tblNames = getAllTablesInternal(dbName, sharedCacheWrapper.getUnsafe());
            for (String tblName : tblNames) {
              // Update the partitions for a table in cache
              updateTablePartitions(rawStore, dbName, tblName);
              // Update the table column stats for a table in cache
              updateTableColStats(rawStore, dbName, tblName);
              // Update the partitions column stats for a table in cache
              updateTablePartitionColStats(rawStore, dbName, tblName);
            }
          }
        }
      } catch (Exception e) {
        LOG.error("Updating CachedStore: error happen when refresh; ignoring", e);
      }
    }

    private void updateDatabases(RawStore rawStore, List<String> dbNames) {
      // Prepare the list of databases
      List<Database> databases = new ArrayList<>();
      for (String dbName : dbNames) {
        Database db;
        try {
          db = rawStore.getDatabase(dbName);
          databases.add(db);
        } catch (NoSuchObjectException e) {
          LOG.info("Updating CachedStore: database - " + dbName + " does not exist.", e);
        }
      }
      // Update the cached database objects
      try {
        if (databaseCacheLock.writeLock().tryLock()) {
          // Skip background updates if we detect change
          if (isDatabaseCacheDirty.compareAndSet(true, false)) {
            LOG.debug("Skipping database cache update; the database list we have is dirty.");
            return;
          }
          sharedCacheWrapper.getUnsafe().refreshDatabases(databases);
        }
      } finally {
        if (databaseCacheLock.isWriteLockedByCurrentThread()) {
          databaseCacheLock.writeLock().unlock();
        }
      }
    }

    // Update the cached table objects
    private void updateTables(RawStore rawStore, String dbName) {
      List<Table> tables = new ArrayList<>();
      try {
        List<String> tblNames = rawStore.getAllTables(dbName);
        for (String tblName : tblNames) {
          Table table =
              rawStore.getTable(StringUtils.normalizeIdentifier(dbName),
                  StringUtils.normalizeIdentifier(tblName));
          tables.add(table);
        }
        if (tableCacheLock.writeLock().tryLock()) {
          // Skip background updates if we detect change
          if (isTableCacheDirty.compareAndSet(true, false)) {
            LOG.debug("Skipping table cache update; the table list we have is dirty.");
            return;
          }
          sharedCacheWrapper.getUnsafe().refreshTables(dbName, tables);
        }
      } catch (MetaException e) {
        LOG.info("Updating CachedStore: unable to read tables for database - " + dbName, e);
      } finally {
        if (tableCacheLock.isWriteLockedByCurrentThread()) {
          tableCacheLock.writeLock().unlock();
        }
      }
    }

    // Update the cached partition objects for a table
    private void updateTablePartitions(RawStore rawStore, String dbName, String tblName) {
      try {
        Deadline.startTimer("getPartitions");
        List<Partition> partitions = rawStore.getPartitions(dbName, tblName, Integer.MAX_VALUE);
        Deadline.stopTimer();
        if (partitionCacheLock.writeLock().tryLock()) {
          // Skip background updates if we detect change
          if (isPartitionCacheDirty.compareAndSet(true, false)) {
            LOG.debug("Skipping partition cache update; the partition list we have is dirty.");
            return;
          }
          sharedCacheWrapper.getUnsafe().refreshPartitions(
              StringUtils.normalizeIdentifier(dbName),
              StringUtils.normalizeIdentifier(tblName), partitions);
        }
      } catch (MetaException | NoSuchObjectException e) {
        LOG.info("Updating CachedStore: unable to read partitions of table: " + tblName, e);
      } finally {
        if (partitionCacheLock.isWriteLockedByCurrentThread()) {
          partitionCacheLock.writeLock().unlock();
        }
      }
    }

    // Update the cached col stats for this table
    private void updateTableColStats(RawStore rawStore, String dbName, String tblName) {
      try {
        Table table = rawStore.getTable(dbName, tblName);
        List<String> colNames = MetaStoreUtils.getColumnNamesForTable(table);
        Deadline.startTimer("getTableColumnStatistics");
        ColumnStatistics tableColStats =
            rawStore.getTableColumnStatistics(dbName, tblName, colNames);
        Deadline.stopTimer();
        if (tableColStats != null) {
          if (tableColStatsCacheLock.writeLock().tryLock()) {
            // Skip background updates if we detect change
            if (isTableColStatsCacheDirty.compareAndSet(true, false)) {
              LOG.debug("Skipping table column stats cache update; the table column stats list we "
                  + "have is dirty.");
              return;
            }
            sharedCacheWrapper.getUnsafe().refreshTableColStats(
                StringUtils.normalizeIdentifier(dbName),
                StringUtils.normalizeIdentifier(tblName), tableColStats.getStatsObj());
          }
        }
      } catch (MetaException | NoSuchObjectException e) {
        LOG.info("Updating CachedStore: unable to read table column stats of table: " + tblName, e);
      } finally {
        if (tableColStatsCacheLock.isWriteLockedByCurrentThread()) {
          tableColStatsCacheLock.writeLock().unlock();
        }
      }
    }

    // Update the cached partition col stats for a table
    private void updateTablePartitionColStats(RawStore rawStore, String dbName, String tblName) {
      try {
        Deadline.startTimer("getColStatsForTablePartitions");
        Map<String, List<ColumnStatisticsObj>> colStatsPerPartition =
            rawStore.getColStatsForTablePartitions(dbName, tblName);
        Deadline.stopTimer();
        if (colStatsPerPartition != null) {
          if (partitionColStatsCacheLock.writeLock().tryLock()) {
            // Skip background updates if we detect change
            if (isPartitionColStatsCacheDirty.compareAndSet(true, false)) {
              LOG.debug("Skipping partition column stats cache update; the partition column stats "
                  + "list we have is dirty.");
              return;
            }
            sharedCacheWrapper.getUnsafe().refreshPartitionColStats(
                StringUtils.normalizeIdentifier(dbName),
                StringUtils.normalizeIdentifier(tblName), colStatsPerPartition);
          }
        }
      } catch (MetaException | NoSuchObjectException e) {
        LOG.info("Updating CachedStore: unable to read partitions column stats of table: "
            + tblName, e);
      } finally {
        if (partitionColStatsCacheLock.isWriteLockedByCurrentThread()) {
          partitionColStatsCacheLock.writeLock().unlock();
        }
      }
    }
  }

  @Override
  public Configuration getConf() {
    return rawStore.getConf();
  }

  @Override
  public void shutdown() {
    rawStore.shutdown();
  }

  @Override
  public boolean openTransaction() {
    return rawStore.openTransaction();
  }

  @Override
  public boolean commitTransaction() {
    return rawStore.commitTransaction();
  }

  @Override
  public boolean isActiveTransaction() {
    return rawStore.isActiveTransaction();
  }

  @Override
  public void rollbackTransaction() {
    rawStore.rollbackTransaction();
  }

  @Override
  public void createDatabase(Database db) throws InvalidObjectException, MetaException {
    rawStore.createDatabase(db);
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) return;
    try {
      // Wait if background cache update is happening
      databaseCacheLock.readLock().lock();
      isDatabaseCacheDirty.set(true);
      sharedCache.addDatabaseToCache(StringUtils.normalizeIdentifier(db.getName()),
          db.deepCopy());
    } finally {
      databaseCacheLock.readLock().unlock();
    }
  }

  @Override
  public Database getDatabase(String dbName) throws NoSuchObjectException {
    SharedCache sharedCache;
    try {
      sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) {
        return rawStore.getDatabase(dbName);
      }
    } catch (MetaException e) {
      throw new RuntimeException(e); // TODO: why doesn't getDatabase throw MetaEx?
    }
    Database db = sharedCache.getDatabaseFromCache(StringUtils.normalizeIdentifier(dbName));
    if (db == null) {
      throw new NoSuchObjectException();
    }
    return db;
  }

  @Override
  public boolean dropDatabase(String dbname) throws NoSuchObjectException, MetaException {
    boolean succ = rawStore.dropDatabase(dbname);
    if (succ) {
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) return true;
      try {
        // Wait if background cache update is happening
        databaseCacheLock.readLock().lock();
        isDatabaseCacheDirty.set(true);
        sharedCache.removeDatabaseFromCache(StringUtils.normalizeIdentifier(dbname));
      } finally {
        databaseCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  public boolean alterDatabase(String dbName, Database db) throws NoSuchObjectException,
      MetaException {
    boolean succ = rawStore.alterDatabase(dbName, db);
    if (succ) {
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) return true;
      try {
        // Wait if background cache update is happening
        databaseCacheLock.readLock().lock();
        isDatabaseCacheDirty.set(true);
        sharedCache.alterDatabaseInCache(StringUtils.normalizeIdentifier(dbName), db);
      } finally {
        databaseCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  public List<String> getDatabases(String pattern) throws MetaException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.getDatabases(pattern);
    }
    List<String> results = new ArrayList<>();
    for (String dbName : sharedCache.listCachedDatabases()) {
      dbName = StringUtils.normalizeIdentifier(dbName);
      if (CacheUtils.matches(dbName, pattern)) {
        results.add(dbName);
      }
    }
    return results;
  }

  @Override
  public List<String> getAllDatabases() throws MetaException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.getAllDatabases();
    }
    return sharedCache.listCachedDatabases();
  }

  @Override
  public boolean createType(Type type) {
    return rawStore.createType(type);
  }

  @Override
  public Type getType(String typeName) {
    return rawStore.getType(typeName);
  }

  @Override
  public boolean dropType(String typeName) {
    return rawStore.dropType(typeName);
  }

  private void validateTableType(Table tbl) {
    // If the table has property EXTERNAL set, update table type
    // accordingly
    String tableType = tbl.getTableType();
    boolean isExternal = Boolean.parseBoolean(tbl.getParameters().get("EXTERNAL"));
    if (TableType.MANAGED_TABLE.toString().equals(tableType)) {
      if (isExternal) {
        tableType = TableType.EXTERNAL_TABLE.toString();
      }
    }
    if (TableType.EXTERNAL_TABLE.toString().equals(tableType)) {
      if (!isExternal) {
        tableType = TableType.MANAGED_TABLE.toString();
      }
    }
    tbl.setTableType(tableType);
  }

  @Override
  public void createTable(Table tbl) throws InvalidObjectException, MetaException {
    rawStore.createTable(tbl);
    validateTableType(tbl);
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) return;
    try {
      // Wait if background cache update is happening
      tableCacheLock.readLock().lock();
      isTableCacheDirty.set(true);
      sharedCache.addTableToCache(StringUtils.normalizeIdentifier(tbl.getDbName()),
          StringUtils.normalizeIdentifier(tbl.getTableName()), tbl);
    } finally {
      tableCacheLock.readLock().unlock();
    }
  }

  @Override
  public boolean dropTable(String dbName, String tableName) throws MetaException,
      NoSuchObjectException, InvalidObjectException, InvalidInputException {
    boolean succ = rawStore.dropTable(dbName, tableName);
    if (succ) {
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) return true;
      // Remove table
      try {
        // Wait if background table cache update is happening
        tableCacheLock.readLock().lock();
        isTableCacheDirty.set(true);
        sharedCache.removeTableFromCache(StringUtils.normalizeIdentifier(dbName),
            StringUtils.normalizeIdentifier(tableName));
      } finally {
        tableCacheLock.readLock().unlock();
      }
      // Remove table col stats
      try {
        // Wait if background table col stats cache update is happening
        tableColStatsCacheLock.readLock().lock();
        isTableColStatsCacheDirty.set(true);
        sharedCache.removeTableColStatsFromCache(StringUtils.normalizeIdentifier(dbName),
            StringUtils.normalizeIdentifier(tableName));
      } finally {
        tableColStatsCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  public Table getTable(String dbName, String tableName) throws MetaException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.getTable(dbName, tableName);
    }
    Table tbl = sharedCache.getTableFromCache(StringUtils.normalizeIdentifier(dbName),
        StringUtils.normalizeIdentifier(tableName));
    if (tbl != null) {
      tbl.unsetPrivileges();
      tbl.setRewriteEnabled(tbl.isRewriteEnabled());
    }
    return tbl;
  }

  @Override
  public boolean addPartition(Partition part) throws InvalidObjectException, MetaException {
    boolean succ = rawStore.addPartition(part);
    if (succ) {
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) return true;
      try {
        // Wait if background cache update is happening
        partitionCacheLock.readLock().lock();
        isPartitionCacheDirty.set(true);
        sharedCache.addPartitionToCache(StringUtils.normalizeIdentifier(part.getDbName()),
            StringUtils.normalizeIdentifier(part.getTableName()), part);
      } finally {
        partitionCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  public boolean addPartitions(String dbName, String tblName, List<Partition> parts)
      throws InvalidObjectException, MetaException {
    boolean succ = rawStore.addPartitions(dbName, tblName, parts);
    if (succ) {
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) return true;
      try {
        // Wait if background cache update is happening
        partitionCacheLock.readLock().lock();
        isPartitionCacheDirty.set(true);
        for (Partition part : parts) {
          sharedCache.addPartitionToCache(StringUtils.normalizeIdentifier(dbName),
              StringUtils.normalizeIdentifier(tblName), part);
        }
      } finally {
        partitionCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  public boolean addPartitions(String dbName, String tblName, PartitionSpecProxy partitionSpec,
      boolean ifNotExists) throws InvalidObjectException, MetaException {
    boolean succ = rawStore.addPartitions(dbName, tblName, partitionSpec, ifNotExists);
    if (succ) {
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) return true;
      try {
        // Wait if background cache update is happening
        partitionCacheLock.readLock().lock();
        isPartitionCacheDirty.set(true);
        PartitionSpecProxy.PartitionIterator iterator = partitionSpec.getPartitionIterator();
        while (iterator.hasNext()) {
          Partition part = iterator.next();
          sharedCache.addPartitionToCache(StringUtils.normalizeIdentifier(dbName),
              StringUtils.normalizeIdentifier(tblName), part);
        }
      } finally {
        partitionCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  public Partition getPartition(String dbName, String tableName, List<String> part_vals)
      throws MetaException, NoSuchObjectException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.getPartition(dbName, tableName, part_vals);
    }
    Partition part =
        sharedCache.getPartitionFromCache(StringUtils.normalizeIdentifier(dbName),
            StringUtils.normalizeIdentifier(tableName), part_vals);
    if (part != null) {
      part.unsetPrivileges();
    } else {
      throw new NoSuchObjectException("partition values=" + part_vals.toString());
    }
    return part;
  }

  @Override
  public boolean doesPartitionExist(String dbName, String tableName,
      List<String> part_vals) throws MetaException, NoSuchObjectException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.doesPartitionExist(dbName, tableName, part_vals);
    }
    return sharedCache.existPartitionFromCache(StringUtils.normalizeIdentifier(dbName),
        StringUtils.normalizeIdentifier(tableName), part_vals);
  }

  @Override
  public boolean dropPartition(String dbName, String tableName, List<String> part_vals)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    boolean succ = rawStore.dropPartition(dbName, tableName, part_vals);
    if (succ) {
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) return true;
      // Remove partition
      try {
        // Wait if background cache update is happening
        partitionCacheLock.readLock().lock();
        isPartitionCacheDirty.set(true);
        sharedCache.removePartitionFromCache(StringUtils.normalizeIdentifier(dbName),
            StringUtils.normalizeIdentifier(tableName), part_vals);
      } finally {
        partitionCacheLock.readLock().unlock();
      }
      // Remove partition col stats
      try {
        // Wait if background cache update is happening
        partitionColStatsCacheLock.readLock().lock();
        isPartitionColStatsCacheDirty.set(true);
        sharedCache.removePartitionColStatsFromCache(StringUtils.normalizeIdentifier(dbName),
            StringUtils.normalizeIdentifier(tableName), part_vals);
      } finally {
        partitionColStatsCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  public List<Partition> getPartitions(String dbName, String tableName, int max)
      throws MetaException, NoSuchObjectException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.getPartitions(dbName, tableName, max);
    }
    List<Partition> parts = sharedCache.listCachedPartitions(StringUtils.normalizeIdentifier(dbName),
        StringUtils.normalizeIdentifier(tableName), max);
    if (parts != null) {
      for (Partition part : parts) {
        part.unsetPrivileges();
      }
    }
    return parts;
  }

  @Override
  public void alterTable(String dbName, String tblName, Table newTable)
      throws InvalidObjectException, MetaException {
    rawStore.alterTable(dbName, tblName, newTable);
    validateTableType(newTable);
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) return;
    // Update table cache
    try {
      // Wait if background cache update is happening
      tableCacheLock.readLock().lock();
      isTableCacheDirty.set(true);
      sharedCache.alterTableInCache(StringUtils.normalizeIdentifier(dbName),
          StringUtils.normalizeIdentifier(tblName), newTable);
    } finally {
      tableCacheLock.readLock().unlock();
    }
    // Update partition cache (key might have changed since table name is a
    // component of key)
    try {
      // Wait if background cache update is happening
      partitionCacheLock.readLock().lock();
      isPartitionCacheDirty.set(true);
      sharedCache.alterTableInPartitionCache(StringUtils.normalizeIdentifier(dbName),
          StringUtils.normalizeIdentifier(tblName), newTable);
    } finally {
      partitionCacheLock.readLock().unlock();
    }
  }

  @Override
  public List<String> getTables(String dbName, String pattern)
      throws MetaException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.getTables(dbName, pattern);
    }
    List<String> tableNames = new ArrayList<>();
    for (Table table : sharedCache.listCachedTables(StringUtils.normalizeIdentifier(dbName))) {
      if (CacheUtils.matches(table.getTableName(), pattern)) {
        tableNames.add(table.getTableName());
      }
    }
    return tableNames;
  }

  @Override
  public List<String> getTables(String dbName, String pattern,
      TableType tableType) throws MetaException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.getTables(dbName, pattern);
    }
    List<String> tableNames = new ArrayList<>();
    for (Table table : sharedCache.listCachedTables(StringUtils.normalizeIdentifier(dbName))) {
      if (CacheUtils.matches(table.getTableName(), pattern) &&
          table.getTableType().equals(tableType.toString())) {
        tableNames.add(table.getTableName());
      }
    }
    return tableNames;
  }

  @Override
  public List<TableMeta> getTableMeta(String dbNames, String tableNames,
      List<String> tableTypes) throws MetaException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.getTableMeta(dbNames, tableNames, tableTypes);
    }
    return sharedCache.getTableMeta(StringUtils.normalizeIdentifier(dbNames),
        StringUtils.normalizeIdentifier(tableNames), tableTypes);
  }

  @Override
  public List<Table> getTableObjectsByName(String dbName,
      List<String> tblNames) throws MetaException, UnknownDBException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.getTableObjectsByName(dbName, tblNames);
    }
    List<Table> tables = new ArrayList<>();
    for (String tblName : tblNames) {
      tables.add(sharedCache.getTableFromCache(StringUtils.normalizeIdentifier(dbName),
          StringUtils.normalizeIdentifier(tblName)));
    }
    return tables;
  }

  @Override
  public List<String> getAllTables(String dbName) throws MetaException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.getAllTables(dbName);
    }
    return getAllTablesInternal(dbName, sharedCache);
  }

  private static List<String> getAllTablesInternal(String dbName, SharedCache sharedCache) {
    List<String> tblNames = new ArrayList<>();
    for (Table tbl : sharedCache.listCachedTables(StringUtils.normalizeIdentifier(dbName))) {
      tblNames.add(StringUtils.normalizeIdentifier(tbl.getTableName()));
    }
    return tblNames;
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter,
      short max_tables) throws MetaException, UnknownDBException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.listTableNamesByFilter(dbName, filter, max_tables);
    }
    List<String> tableNames = new ArrayList<>();
    int count = 0;
    for (Table table : sharedCache.listCachedTables(StringUtils.normalizeIdentifier(dbName))) {
      if (CacheUtils.matches(table.getTableName(), filter)
          && (max_tables == -1 || count < max_tables)) {
        tableNames.add(table.getTableName());
        count++;
      }
    }
    return tableNames;
  }

  @Override
  public List<String> listPartitionNames(String dbName, String tblName,
      short max_parts) throws MetaException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.listPartitionNames(dbName, tblName, max_parts);
    }
    List<String> partitionNames = new ArrayList<>();
    Table t = sharedCache.getTableFromCache(StringUtils.normalizeIdentifier(dbName),
        StringUtils.normalizeIdentifier(tblName));
    int count = 0;
    for (Partition part : sharedCache.listCachedPartitions(StringUtils.normalizeIdentifier(dbName),
        StringUtils.normalizeIdentifier(tblName), max_parts)) {
      if (max_parts == -1 || count < max_parts) {
        partitionNames.add(Warehouse.makePartName(t.getPartitionKeys(), part.getValues()));
      }
    }
    return partitionNames;
  }

  @Override
  public PartitionValuesResponse listPartitionValues(String db_name, String tbl_name, List<FieldSchema> cols,
                                                     boolean applyDistinct, String filter, boolean ascending,
                                                     List<FieldSchema> order, long maxParts) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listPartitionNamesByFilter(String db_name,
      String tbl_name, String filter, short max_parts) throws MetaException {
    // TODO Translate filter -> expr
    return null;
  }

  @Override
  public void alterPartition(String dbName, String tblName, List<String> partVals, Partition newPart)
      throws InvalidObjectException, MetaException {
    rawStore.alterPartition(dbName, tblName, partVals, newPart);
    // Update partition cache
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) return;
    try {
      // Wait if background cache update is happening
      partitionCacheLock.readLock().lock();
      isPartitionCacheDirty.set(true);
      sharedCache.alterPartitionInCache(StringUtils.normalizeIdentifier(dbName),
          StringUtils.normalizeIdentifier(tblName), partVals, newPart);
    } finally {
      partitionCacheLock.readLock().unlock();
    }
    // Update partition column stats cache
    try {
      // Wait if background cache update is happening
      partitionColStatsCacheLock.readLock().lock();
      isPartitionColStatsCacheDirty.set(true);
      sharedCache.alterPartitionInColStatsCache(StringUtils.normalizeIdentifier(dbName),
          StringUtils.normalizeIdentifier(tblName), partVals, newPart);
    } finally {
      partitionColStatsCacheLock.readLock().unlock();
    }
  }

  @Override
  public void alterPartitions(String dbName, String tblName, List<List<String>> partValsList,
      List<Partition> newParts) throws InvalidObjectException, MetaException {
    rawStore.alterPartitions(dbName, tblName, partValsList, newParts);
    // Update partition cache
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) return;
    try {
      // Wait if background cache update is happening
      partitionCacheLock.readLock().lock();
      isPartitionCacheDirty.set(true);
      for (int i = 0; i < partValsList.size(); i++) {
        List<String> partVals = partValsList.get(i);
        Partition newPart = newParts.get(i);
        sharedCache.alterPartitionInCache(StringUtils.normalizeIdentifier(dbName),
            StringUtils.normalizeIdentifier(tblName), partVals, newPart);
      }
    } finally {
      partitionCacheLock.readLock().unlock();
    }
    // Update partition column stats cache
    try {
      // Wait if background cache update is happening
      partitionColStatsCacheLock.readLock().lock();
      isPartitionColStatsCacheDirty.set(true);
      for (int i = 0; i < partValsList.size(); i++) {
        List<String> partVals = partValsList.get(i);
        Partition newPart = newParts.get(i);
        sharedCache.alterPartitionInColStatsCache(StringUtils.normalizeIdentifier(dbName),
            StringUtils.normalizeIdentifier(tblName), partVals, newPart);
      }
    } finally {
      partitionColStatsCacheLock.readLock().unlock();
    }
  }

  @Override
  public boolean addIndex(Index index)
      throws InvalidObjectException, MetaException {
    return rawStore.addIndex(index);
  }

  @Override
  public Index getIndex(String dbName, String origTableName, String indexName)
      throws MetaException {
    return rawStore.getIndex(dbName, origTableName, indexName);
  }

  @Override
  public boolean dropIndex(String dbName, String origTableName,
      String indexName) throws MetaException {
    return rawStore.dropIndex(dbName, origTableName, indexName);
  }

  @Override
  public List<Index> getIndexes(String dbName, String origTableName, int max)
      throws MetaException {
    return rawStore.getIndexes(dbName, origTableName, max);
  }

  @Override
  public List<String> listIndexNames(String dbName, String origTableName,
      short max) throws MetaException {
    return rawStore.listIndexNames(dbName, origTableName, max);
  }

  @Override
  public void alterIndex(String dbname, String baseTblName, String name,
      Index newIndex) throws InvalidObjectException, MetaException {
    rawStore.alterIndex(dbname, baseTblName, name, newIndex);
  }

  private boolean getPartitionNamesPrunedByExprNoTxn(Table table, byte[] expr,
      String defaultPartName, short maxParts, List<String> result, SharedCache sharedCache)
          throws MetaException, NoSuchObjectException {
    List<Partition> parts = sharedCache.listCachedPartitions(
        StringUtils.normalizeIdentifier(table.getDbName()),
        StringUtils.normalizeIdentifier(table.getTableName()), maxParts);
    for (Partition part : parts) {
      result.add(Warehouse.makePartName(table.getPartitionKeys(), part.getValues()));
    }
    if (defaultPartName == null || defaultPartName.isEmpty()) {
      defaultPartName = MetastoreConf.getVar(getConf(), ConfVars.DEFAULTPARTITIONNAME);
    }
    return expressionProxy.filterPartitionsByExpr(
        table.getPartitionKeys(), expr, defaultPartName, result);
  }

  @Override
  public List<Partition> getPartitionsByFilter(String dbName, String tblName,
      String filter, short maxParts)
      throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean getPartitionsByExpr(String dbName, String tblName, byte[] expr,
      String defaultPartitionName, short maxParts, List<Partition> result)
      throws TException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.getPartitionsByExpr(
          dbName, tblName, expr, defaultPartitionName, maxParts, result);
    }
    List<String> partNames = new LinkedList<>();
    Table table = sharedCache.getTableFromCache(StringUtils.normalizeIdentifier(dbName),
        StringUtils.normalizeIdentifier(tblName));
    boolean hasUnknownPartitions = getPartitionNamesPrunedByExprNoTxn(
        table, expr, defaultPartitionName, maxParts, partNames, sharedCache);
    for (String partName : partNames) {
      Partition part = sharedCache.getPartitionFromCache(StringUtils.normalizeIdentifier(dbName),
          StringUtils.normalizeIdentifier(tblName), partNameToVals(partName));
      part.unsetPrivileges();
      result.add(part);
    }
    return hasUnknownPartitions;
  }

  @Override
  public int getNumPartitionsByFilter(String dbName, String tblName,
      String filter) throws MetaException, NoSuchObjectException {
    // TODO filter -> expr
    // SharedCache sharedCache = sharedCacheWrapper.get();
    // if (sharedCache == null) {
      return rawStore.getNumPartitionsByFilter(dbName, tblName, filter);
    // }
    // Table table = sharedCache.getTableFromCache(StringUtils.normalizeIdentifier(dbName),
    //     StringUtils.normalizeIdentifier(tblName));
    // return 0;
  }

  @Override
  public int getNumPartitionsByExpr(String dbName, String tblName, byte[] expr)
      throws MetaException, NoSuchObjectException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.getNumPartitionsByExpr(dbName, tblName, expr);
    }
    String defaultPartName = MetastoreConf.getVar(getConf(), ConfVars.DEFAULTPARTITIONNAME);
    List<String> partNames = new LinkedList<>();
    Table table = sharedCache.getTableFromCache(StringUtils.normalizeIdentifier(dbName),
        StringUtils.normalizeIdentifier(tblName));
    getPartitionNamesPrunedByExprNoTxn(
        table, expr, defaultPartName, Short.MAX_VALUE, partNames, sharedCache);
    return partNames.size();
  }

  private static List<String> partNameToVals(String name) {
    if (name == null) return null;
    List<String> vals = new ArrayList<>();
    String[] kvp = name.split("/");
    for (String kv : kvp) {
      vals.add(FileUtils.unescapePathName(kv.substring(kv.indexOf('=') + 1)));
    }
    return vals;
  }

  @Override
  public List<Partition> getPartitionsByNames(String dbName, String tblName,
      List<String> partNames) throws MetaException, NoSuchObjectException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.getPartitionsByNames(dbName, tblName, partNames);
    }
    List<Partition> partitions = new ArrayList<>();
    for (String partName : partNames) {
      Partition part = sharedCache.getPartitionFromCache(StringUtils.normalizeIdentifier(dbName),
          StringUtils.normalizeIdentifier(tblName), partNameToVals(partName));
      if (part!=null) {
        partitions.add(part);
      }
    }
    return partitions;
  }

  @Override
  public Table markPartitionForEvent(String dbName, String tblName,
      Map<String, String> partVals, PartitionEventType evtType)
      throws MetaException, UnknownTableException, InvalidPartitionException,
      UnknownPartitionException {
    return rawStore.markPartitionForEvent(dbName, tblName, partVals, evtType);
  }

  @Override
  public boolean isPartitionMarkedForEvent(String dbName, String tblName,
      Map<String, String> partName, PartitionEventType evtType)
      throws MetaException, UnknownTableException, InvalidPartitionException,
      UnknownPartitionException {
    return rawStore.isPartitionMarkedForEvent(dbName, tblName, partName, evtType);
  }

  @Override
  public boolean addRole(String rowName, String ownerName)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    return rawStore.addRole(rowName, ownerName);
  }

  @Override
  public boolean removeRole(String roleName)
      throws MetaException, NoSuchObjectException {
    return rawStore.removeRole(roleName);
  }

  @Override
  public boolean grantRole(Role role, String userName,
      PrincipalType principalType, String grantor, PrincipalType grantorType,
      boolean grantOption)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    return rawStore.grantRole(role, userName, principalType, grantor, grantorType, grantOption);
  }

  @Override
  public boolean revokeRole(Role role, String userName,
      PrincipalType principalType, boolean grantOption)
      throws MetaException, NoSuchObjectException {
    return rawStore.revokeRole(role, userName, principalType, grantOption);
  }

  @Override
  public PrincipalPrivilegeSet getUserPrivilegeSet(String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {
    return rawStore.getUserPrivilegeSet(userName, groupNames);
  }

  @Override
  public PrincipalPrivilegeSet getDBPrivilegeSet(String dbName, String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {
    return rawStore.getDBPrivilegeSet(dbName, userName, groupNames);
  }

  @Override
  public PrincipalPrivilegeSet getTablePrivilegeSet(String dbName,
      String tableName, String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    return rawStore.getTablePrivilegeSet(dbName, tableName, userName, groupNames);
  }

  @Override
  public PrincipalPrivilegeSet getPartitionPrivilegeSet(String dbName,
      String tableName, String partition, String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {
    return rawStore.getPartitionPrivilegeSet(dbName, tableName, partition, userName, groupNames);
  }

  @Override
  public PrincipalPrivilegeSet getColumnPrivilegeSet(String dbName,
      String tableName, String partitionName, String columnName,
      String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    return rawStore.getColumnPrivilegeSet(dbName, tableName, partitionName, columnName, userName, groupNames);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalGlobalGrants(
      String principalName, PrincipalType principalType) {
    return rawStore.listPrincipalGlobalGrants(principalName, principalType);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDBGrants(String principalName,
      PrincipalType principalType, String dbName) {
    return rawStore.listPrincipalDBGrants(principalName, principalType, dbName);
  }

  @Override
  public List<HiveObjectPrivilege> listAllTableGrants(String principalName,
      PrincipalType principalType, String dbName, String tableName) {
    return rawStore.listAllTableGrants(principalName, principalType, dbName, tableName);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName, List<String> partValues, String partName) {
    return rawStore.listPrincipalPartitionGrants(principalName, principalType, dbName, tableName, partValues, partName);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableColumnGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName, String columnName) {
    return rawStore.listPrincipalTableColumnGrants(principalName, principalType, dbName, tableName, columnName);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName, List<String> partValues, String partName,
      String columnName) {
    return rawStore.listPrincipalPartitionColumnGrants(principalName, principalType, dbName, tableName, partValues, partName, columnName);
  }

  @Override
  public boolean grantPrivileges(PrivilegeBag privileges)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    return rawStore.grantPrivileges(privileges);
  }

  @Override
  public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    return rawStore.revokePrivileges(privileges, grantOption);
  }

  @Override
  public Role getRole(String roleName) throws NoSuchObjectException {
    return rawStore.getRole(roleName);
  }

  @Override
  public List<String> listRoleNames() {
    return rawStore.listRoleNames();
  }

  @Override
  public List<Role> listRoles(String principalName,
      PrincipalType principalType) {
    return rawStore.listRoles(principalName, principalType);
  }

  @Override
  public List<RolePrincipalGrant> listRolesWithGrants(String principalName,
      PrincipalType principalType) {
    return rawStore.listRolesWithGrants(principalName, principalType);
  }

  @Override
  public List<RolePrincipalGrant> listRoleMembers(String roleName) {
    return rawStore.listRoleMembers(roleName);
  }

  @Override
  public Partition getPartitionWithAuth(String dbName, String tblName,
      List<String> partVals, String userName, List<String> groupNames)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.getPartitionWithAuth(dbName, tblName, partVals, userName, groupNames);
    }
    Partition p = sharedCache.getPartitionFromCache(StringUtils.normalizeIdentifier(dbName),
        StringUtils.normalizeIdentifier(tblName), partVals);
    if (p!=null) {
      Table t = sharedCache.getTableFromCache(StringUtils.normalizeIdentifier(dbName),
          StringUtils.normalizeIdentifier(tblName));
      String partName = Warehouse.makePartName(t.getPartitionKeys(), partVals);
      PrincipalPrivilegeSet privs = getPartitionPrivilegeSet(dbName, tblName, partName,
          userName, groupNames);
      p.setPrivileges(privs);
    }
    return p;
  }

  @Override
  public List<Partition> getPartitionsWithAuth(String dbName, String tblName,
      short maxParts, String userName, List<String> groupNames)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.getPartitionsWithAuth(dbName, tblName, maxParts, userName, groupNames);
    }
    Table t = sharedCache.getTableFromCache(StringUtils.normalizeIdentifier(dbName),
        StringUtils.normalizeIdentifier(tblName));
    List<Partition> partitions = new ArrayList<>();
    int count = 0;
    for (Partition part : sharedCache.listCachedPartitions(StringUtils.normalizeIdentifier(dbName),
        StringUtils.normalizeIdentifier(tblName), maxParts)) {
      if (maxParts == -1 || count < maxParts) {
        String partName = Warehouse.makePartName(t.getPartitionKeys(), part.getValues());
        PrincipalPrivilegeSet privs = getPartitionPrivilegeSet(dbName, tblName, partName,
            userName, groupNames);
        part.setPrivileges(privs);
        partitions.add(part);
        count++;
      }
    }
    return partitions;
  }

  @Override
  public List<String> listPartitionNamesPs(String dbName, String tblName,
      List<String> partVals, short maxParts)
      throws MetaException, NoSuchObjectException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.listPartitionNamesPs(dbName, tblName, partVals, maxParts);
    }
    List<String> partNames = new ArrayList<>();
    int count = 0;
    Table t = sharedCache.getTableFromCache(StringUtils.normalizeIdentifier(dbName),
        StringUtils.normalizeIdentifier(tblName));
    for (Partition part : sharedCache.listCachedPartitions(StringUtils.normalizeIdentifier(dbName),
        StringUtils.normalizeIdentifier(tblName), maxParts)) {
      boolean psMatch = true;
      for (int i=0;i<partVals.size();i++) {
        String psVal = partVals.get(i);
        String partVal = part.getValues().get(i);
        if (psVal!=null && !psVal.isEmpty() && !psVal.equals(partVal)) {
          psMatch = false;
          break;
        }
      }
      if (!psMatch) {
        continue;
      }
      if (maxParts == -1 || count < maxParts) {
        partNames.add(Warehouse.makePartName(t.getPartitionKeys(), part.getValues()));
        count++;
      }
    }
    return partNames;
  }

  @Override
  public List<Partition> listPartitionsPsWithAuth(String dbName,
      String tblName, List<String> partVals, short maxParts, String userName,
      List<String> groupNames)
      throws MetaException, InvalidObjectException, NoSuchObjectException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.listPartitionsPsWithAuth(
          dbName, tblName, partVals, maxParts, userName, groupNames);
    }
    List<Partition> partitions = new ArrayList<>();
    Table t = sharedCache.getTableFromCache(StringUtils.normalizeIdentifier(dbName),
        StringUtils.normalizeIdentifier(tblName));
    int count = 0;
    for (Partition part : sharedCache.listCachedPartitions(StringUtils.normalizeIdentifier(dbName),
        StringUtils.normalizeIdentifier(tblName), maxParts)) {
      boolean psMatch = true;
      for (int i=0;i<partVals.size();i++) {
        String psVal = partVals.get(i);
        String partVal = part.getValues().get(i);
        if (psVal!=null && !psVal.isEmpty() && !psVal.equals(partVal)) {
          psMatch = false;
          break;
        }
      }
      if (!psMatch) {
        continue;
      }
      if (maxParts == -1 || count < maxParts) {
        String partName = Warehouse.makePartName(t.getPartitionKeys(), part.getValues());
        PrincipalPrivilegeSet privs = getPartitionPrivilegeSet(dbName, tblName, partName,
            userName, groupNames);
        part.setPrivileges(privs);
        partitions.add(part);
      }
    }
    return partitions;
  }

  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics colStats)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean succ = rawStore.updateTableColumnStatistics(colStats);
    if (succ) {
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) return true;
      String dbName = colStats.getStatsDesc().getDbName();
      String tableName = colStats.getStatsDesc().getTableName();
      List<ColumnStatisticsObj> statsObjs = colStats.getStatsObj();
      Table tbl = getTable(dbName, tableName);
      List<String> colNames = new ArrayList<>();
      for (ColumnStatisticsObj statsObj : statsObjs) {
        colNames.add(statsObj.getColName());
      }
      StatsSetupConst.setColumnStatsState(tbl.getParameters(), colNames);

      // Update table
      try {
        // Wait if background cache update is happening
        tableCacheLock.readLock().lock();
        isTableCacheDirty.set(true);
        sharedCache.alterTableInCache(StringUtils.normalizeIdentifier(dbName),
            StringUtils.normalizeIdentifier(tableName), tbl);
      } finally {
        tableCacheLock.readLock().unlock();
      }

      // Update table col stats
      try {
        // Wait if background cache update is happening
        tableColStatsCacheLock.readLock().lock();
        isTableColStatsCacheDirty.set(true);
        sharedCache.updateTableColStatsInCache(StringUtils.normalizeIdentifier(dbName),
            StringUtils.normalizeIdentifier(tableName), statsObjs);
      } finally {
        tableColStatsCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  public ColumnStatistics getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames) throws MetaException, NoSuchObjectException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.getTableColumnStatistics(dbName, tableName, colNames);
    }
    ColumnStatisticsDesc csd = new ColumnStatisticsDesc(true, dbName, tableName);
    List<ColumnStatisticsObj> colStatObjs = new ArrayList<>();
    for (String colName : colNames) {
      String colStatsCacheKey =
          CacheUtils.buildKey(StringUtils.normalizeIdentifier(dbName),
              StringUtils.normalizeIdentifier(tableName), colName);
      ColumnStatisticsObj colStat = sharedCache.getCachedTableColStats(colStatsCacheKey);
      if (colStat != null) {
        colStatObjs.add(colStat);
      }
    }
    if (colStatObjs.isEmpty()) {
      return null;
    } else {
      return new ColumnStatistics(csd, colStatObjs);
    }
  }

  @Override
  public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean succ = rawStore.deleteTableColumnStatistics(dbName, tableName, colName);
    if (succ) {
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) return true;
      try {
        // Wait if background cache update is happening
        tableColStatsCacheLock.readLock().lock();
        isTableColStatsCacheDirty.set(true);
        sharedCache.removeTableColStatsFromCache(StringUtils.normalizeIdentifier(dbName),
            StringUtils.normalizeIdentifier(tableName), colName);
      } finally {
        tableColStatsCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  public boolean updatePartitionColumnStatistics(ColumnStatistics colStats, List<String> partVals)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean succ = rawStore.updatePartitionColumnStatistics(colStats, partVals);
    if (succ) {
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) return true;
      String dbName = colStats.getStatsDesc().getDbName();
      String tableName = colStats.getStatsDesc().getTableName();
      List<ColumnStatisticsObj> statsObjs = colStats.getStatsObj();
      Partition part = getPartition(dbName, tableName, partVals);
      List<String> colNames = new ArrayList<>();
      for (ColumnStatisticsObj statsObj : statsObjs) {
        colNames.add(statsObj.getColName());
      }
      StatsSetupConst.setColumnStatsState(part.getParameters(), colNames);

      // Update partition
      try {
        // Wait if background cache update is happening
        partitionCacheLock.readLock().lock();
        isPartitionCacheDirty.set(true);
        sharedCache.alterPartitionInCache(StringUtils.normalizeIdentifier(dbName),
            StringUtils.normalizeIdentifier(tableName), partVals, part);
      } finally {
        partitionCacheLock.readLock().unlock();
      }

      // Update partition column stats
      try {
        // Wait if background cache update is happening
        partitionColStatsCacheLock.readLock().lock();
        isPartitionColStatsCacheDirty.set(true);
        sharedCache.updatePartitionColStatsInCache(
            StringUtils.normalizeIdentifier(colStats.getStatsDesc().getDbName()),
            StringUtils.normalizeIdentifier(colStats.getStatsDesc().getTableName()), partVals,
            colStats.getStatsObj());
      } finally {
        partitionColStatsCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  // TODO: calculate from cached values.
  // Need to see if it makes sense to do this as some col stats maybe out of date/missing on cache.
  public List<ColumnStatistics> getPartitionColumnStatistics(String dbName, String tblName,
      List<String> partNames, List<String> colNames) throws MetaException, NoSuchObjectException {
    return rawStore.getPartitionColumnStatistics(dbName, tblName, partNames, colNames);
  }

  @Override
  public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName,
      List<String> partVals, String colName) throws NoSuchObjectException, MetaException,
      InvalidObjectException, InvalidInputException {
    boolean succ =
        rawStore.deletePartitionColumnStatistics(dbName, tableName, partName, partVals, colName);
    if (succ) {
      SharedCache sharedCache = sharedCacheWrapper.get();
      if (sharedCache == null) return true;
      try {
        // Wait if background cache update is happening
        partitionColStatsCacheLock.readLock().lock();
        isPartitionColStatsCacheDirty.set(true);
        sharedCache.removePartitionColStatsFromCache(StringUtils.normalizeIdentifier(dbName),
            StringUtils.normalizeIdentifier(tableName), partVals, colName);
      } finally {
        partitionColStatsCacheLock.readLock().unlock();
      }
    }
    return succ;
  }

  @Override
  public AggrStats get_aggr_stats_for(String dbName, String tblName, List<String> partNames,
    List<String> colNames) throws MetaException, NoSuchObjectException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.get_aggr_stats_for(dbName, tblName, partNames, colNames);
    }
    List<ColumnStatisticsObj> colStats = mergeColStatsForPartitions(
      StringUtils.normalizeIdentifier(dbName), StringUtils.normalizeIdentifier(tblName),
      partNames, colNames, sharedCache);
    return new AggrStats(colStats, partNames.size());
  }

  private List<ColumnStatisticsObj> mergeColStatsForPartitions(String dbName, String tblName,
      List<String> partNames, List<String> colNames, SharedCache sharedCache)
          throws MetaException {
    final boolean useDensityFunctionForNDVEstimation = MetastoreConf.getBoolVar(getConf(),
        ConfVars.STATS_NDV_DENSITY_FUNCTION);
    final double ndvTuner = MetastoreConf.getDoubleVar(getConf(), ConfVars.STATS_NDV_TUNER);
    Map<String, List<ColumnStatistics>> map = new HashMap<>();

    for (String colName : colNames) {
      List<ColumnStatistics> colStats = new ArrayList<>();
      for (String partName : partNames) {
        String colStatsCacheKey = CacheUtils.buildKey(dbName, tblName, partNameToVals(partName),
            colName);
        List<ColumnStatisticsObj> colStat = new ArrayList<>();
        ColumnStatisticsObj colStatsForPart = sharedCache
            .getCachedPartitionColStats(colStatsCacheKey);
        if (colStatsForPart != null) {
          colStat.add(colStatsForPart);
          ColumnStatisticsDesc csDesc = new ColumnStatisticsDesc(false, dbName, tblName);
          csDesc.setPartName(partName);
          colStats.add(new ColumnStatistics(csDesc, colStat));
        } else {
          LOG.debug("Stats not found in CachedStore for: dbName={} tblName={} partName={} colName={}",
            dbName, tblName,partName, colName);
        }
      }
      map.put(colName, colStats);
    }
    // Note that enableBitVector does not apply here because ColumnStatisticsObj
    // itself will tell whether
    // bitvector is null or not and aggr logic can automatically apply.
    return MetaStoreUtils.aggrPartitionStats(map, dbName, tblName, partNames, colNames,
        useDensityFunctionForNDVEstimation, ndvTuner);
  }


  @Override
  public long cleanupEvents() {
    return rawStore.cleanupEvents();
  }

  @Override
  public boolean addToken(String tokenIdentifier, String delegationToken) {
    return rawStore.addToken(tokenIdentifier, delegationToken);
  }

  @Override
  public boolean removeToken(String tokenIdentifier) {
    return rawStore.removeToken(tokenIdentifier);
  }

  @Override
  public String getToken(String tokenIdentifier) {
    return rawStore.getToken(tokenIdentifier);
  }

  @Override
  public List<String> getAllTokenIdentifiers() {
    return rawStore.getAllTokenIdentifiers();
  }

  @Override
  public int addMasterKey(String key) throws MetaException {
    return rawStore.addMasterKey(key);
  }

  @Override
  public void updateMasterKey(Integer seqNo, String key)
      throws NoSuchObjectException, MetaException {
    rawStore.updateMasterKey(seqNo, key);
  }

  @Override
  public boolean removeMasterKey(Integer keySeq) {
    return rawStore.removeMasterKey(keySeq);
  }

  @Override
  public String[] getMasterKeys() {
    return rawStore.getMasterKeys();
  }

  @Override
  public void verifySchema() throws MetaException {
    rawStore.verifySchema();
  }

  @Override
  public String getMetaStoreSchemaVersion() throws MetaException {
    return rawStore.getMetaStoreSchemaVersion();
  }

  @Override
  public void setMetaStoreSchemaVersion(String version, String comment)
      throws MetaException {
    rawStore.setMetaStoreSchemaVersion(version, comment);
  }

  @Override
  public void dropPartitions(String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException {
    rawStore.dropPartitions(dbName, tblName, partNames);
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) return;
    // Remove partitions
    try {
      // Wait if background cache update is happening
      partitionCacheLock.readLock().lock();
      isPartitionCacheDirty.set(true);
      for (String partName : partNames) {
        List<String> vals = partNameToVals(partName);
        sharedCache.removePartitionFromCache(StringUtils.normalizeIdentifier(dbName),
            StringUtils.normalizeIdentifier(tblName), vals);
      }
    } finally {
      partitionCacheLock.readLock().unlock();
    }
    // Remove partition col stats
    try {
      // Wait if background cache update is happening
      partitionColStatsCacheLock.readLock().lock();
      isPartitionColStatsCacheDirty.set(true);
      for (String partName : partNames) {
        List<String> part_vals = partNameToVals(partName);
        sharedCache.removePartitionColStatsFromCache(StringUtils.normalizeIdentifier(dbName),
            StringUtils.normalizeIdentifier(tblName), part_vals);
      }
    } finally {
      partitionColStatsCacheLock.readLock().unlock();
    }
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDBGrantsAll(
      String principalName, PrincipalType principalType) {
    return rawStore.listPrincipalDBGrantsAll(principalName, principalType);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableGrantsAll(
      String principalName, PrincipalType principalType) {
    return rawStore.listPrincipalTableGrantsAll(principalName, principalType);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(
      String principalName, PrincipalType principalType) {
    return rawStore.listPrincipalPartitionGrantsAll(principalName, principalType);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(
      String principalName, PrincipalType principalType) {
    return rawStore.listPrincipalTableColumnGrantsAll(principalName, principalType);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(
      String principalName, PrincipalType principalType) {
    return rawStore.listPrincipalPartitionColumnGrantsAll(principalName, principalType);
  }

  @Override
  public List<HiveObjectPrivilege> listGlobalGrantsAll() {
    return rawStore.listGlobalGrantsAll();
  }

  @Override
  public List<HiveObjectPrivilege> listDBGrantsAll(String dbName) {
    return rawStore.listDBGrantsAll(dbName);
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionColumnGrantsAll(String dbName,
      String tableName, String partitionName, String columnName) {
    return rawStore.listPartitionColumnGrantsAll(dbName, tableName, partitionName, columnName);
  }

  @Override
  public List<HiveObjectPrivilege> listTableGrantsAll(String dbName,
      String tableName) {
    return rawStore.listTableGrantsAll(dbName, tableName);
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionGrantsAll(String dbName,
      String tableName, String partitionName) {
    return rawStore.listPartitionGrantsAll(dbName, tableName, partitionName);
  }

  @Override
  public List<HiveObjectPrivilege> listTableColumnGrantsAll(String dbName,
      String tableName, String columnName) {
    return rawStore.listTableColumnGrantsAll(dbName, tableName, columnName);
  }

  @Override
  public void createFunction(Function func)
      throws InvalidObjectException, MetaException {
    // TODO fucntionCache
    rawStore.createFunction(func);
  }

  @Override
  public void alterFunction(String dbName, String funcName,
      Function newFunction) throws InvalidObjectException, MetaException {
    // TODO fucntionCache
    rawStore.alterFunction(dbName, funcName, newFunction);
  }

  @Override
  public void dropFunction(String dbName, String funcName) throws MetaException,
      NoSuchObjectException, InvalidObjectException, InvalidInputException {
    // TODO fucntionCache
    rawStore.dropFunction(dbName, funcName);
  }

  @Override
  public Function getFunction(String dbName, String funcName)
      throws MetaException {
    // TODO fucntionCache
    return rawStore.getFunction(dbName, funcName);
  }

  @Override
  public List<Function> getAllFunctions() throws MetaException {
    // TODO fucntionCache
    return rawStore.getAllFunctions();
  }

  @Override
  public List<String> getFunctions(String dbName, String pattern)
      throws MetaException {
    // TODO fucntionCache
    return rawStore.getFunctions(dbName, pattern);
  }

  @Override
  public NotificationEventResponse getNextNotification(
      NotificationEventRequest rqst) {
    return rawStore.getNextNotification(rqst);
  }

  @Override
  public void addNotificationEvent(NotificationEvent event) {
    rawStore.addNotificationEvent(event);
  }

  @Override
  public void cleanNotificationEvents(int olderThan) {
    rawStore.cleanNotificationEvents(olderThan);
  }

  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() {
    return rawStore.getCurrentNotificationEventId();
  }

  @Override
  public NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst) {
    return rawStore.getNotificationEventsCount(rqst);
  }

  @Override
  public void flushCache() {
    rawStore.flushCache();
  }

  @Override
  public ByteBuffer[] getFileMetadata(List<Long> fileIds) throws MetaException {
    return rawStore.getFileMetadata(fileIds);
  }

  @Override
  public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata,
      FileMetadataExprType type) throws MetaException {
    rawStore.putFileMetadata(fileIds, metadata, type);
  }

  @Override
  public boolean isFileMetadataSupported() {
    return rawStore.isFileMetadataSupported();
  }

  @Override
  public void getFileMetadataByExpr(List<Long> fileIds,
      FileMetadataExprType type, byte[] expr, ByteBuffer[] metadatas,
      ByteBuffer[] exprResults, boolean[] eliminated) throws MetaException {
    rawStore.getFileMetadataByExpr(fileIds, type, expr, metadatas, exprResults, eliminated);
  }

  @Override
  public FileMetadataHandler getFileMetadataHandler(FileMetadataExprType type) {
    return rawStore.getFileMetadataHandler(type);
  }

  @Override
  public int getTableCount() throws MetaException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.getTableCount();
    }
    return sharedCache.getCachedTableCount();
  }

  @Override
  public int getPartitionCount() throws MetaException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.getPartitionCount();
    }
    return sharedCache.getCachedPartitionCount();
  }

  @Override
  public int getDatabaseCount() throws MetaException {
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) {
      return rawStore.getDatabaseCount();
    }
    return sharedCache.getCachedDatabaseCount();
  }

  @Override
  public List<SQLPrimaryKey> getPrimaryKeys(String db_name, String tbl_name)
      throws MetaException {
    // TODO constraintCache
    return rawStore.getPrimaryKeys(db_name, tbl_name);
  }

  @Override
  public List<SQLForeignKey> getForeignKeys(String parent_db_name,
      String parent_tbl_name, String foreign_db_name, String foreign_tbl_name)
      throws MetaException {
    // TODO constraintCache
    return rawStore.getForeignKeys(parent_db_name, parent_tbl_name, foreign_db_name, foreign_tbl_name);
  }

  @Override
  public List<SQLUniqueConstraint> getUniqueConstraints(String db_name, String tbl_name)
      throws MetaException {
    // TODO constraintCache
    return rawStore.getUniqueConstraints(db_name, tbl_name);
  }

  @Override
  public List<SQLNotNullConstraint> getNotNullConstraints(String db_name, String tbl_name)
      throws MetaException {
    // TODO constraintCache
    return rawStore.getNotNullConstraints(db_name, tbl_name);
  }

  @Override
  public List<String> createTableWithConstraints(Table tbl,
      List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys,
      List<SQLUniqueConstraint> uniqueConstraints,
      List<SQLNotNullConstraint> notNullConstraints)
      throws InvalidObjectException, MetaException {
    // TODO constraintCache
    List<String> constraintNames = rawStore.createTableWithConstraints(tbl, primaryKeys, foreignKeys,
            uniqueConstraints, notNullConstraints);
    SharedCache sharedCache = sharedCacheWrapper.get();
    if (sharedCache == null) return constraintNames;
    sharedCache.addTableToCache(StringUtils.normalizeIdentifier(tbl.getDbName()),
        StringUtils.normalizeIdentifier(tbl.getTableName()), tbl);
    return constraintNames;
  }

  @Override
  public void dropConstraint(String dbName, String tableName,
      String constraintName) throws NoSuchObjectException {
    // TODO constraintCache
    rawStore.dropConstraint(dbName, tableName, constraintName);
  }

  @Override
  public List<String> addPrimaryKeys(List<SQLPrimaryKey> pks)
      throws InvalidObjectException, MetaException {
    // TODO constraintCache
    return rawStore.addPrimaryKeys(pks);
  }

  @Override
  public List<String> addForeignKeys(List<SQLForeignKey> fks)
      throws InvalidObjectException, MetaException {
    // TODO constraintCache
    return rawStore.addForeignKeys(fks);
  }

  @Override
  public List<String> addUniqueConstraints(List<SQLUniqueConstraint> uks)
      throws InvalidObjectException, MetaException {
    // TODO constraintCache
    return rawStore.addUniqueConstraints(uks);
  }

  @Override
  public List<String> addNotNullConstraints(List<SQLNotNullConstraint> nns)
      throws InvalidObjectException, MetaException {
    // TODO constraintCache
    return rawStore.addNotNullConstraints(nns);
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getColStatsForTablePartitions(String dbName,
      String tableName) throws MetaException, NoSuchObjectException {
    return rawStore.getColStatsForTablePartitions(dbName, tableName);
  }

  public RawStore getRawStore() {
    return rawStore;
  }

  @VisibleForTesting
  public void setRawStore(RawStore rawStore) {
    this.rawStore = rawStore;
  }

  @Override
  public String getMetastoreDbUuid() throws MetaException {
    return rawStore.getMetastoreDbUuid();
  }

  // TODO: this is only used to hide SharedCache instance from direct use; ideally, the stuff in
  //       CachedStore that's specific to SharedCache (e.g. update threads) should be refactored to
  //       be part of this, then this could be moved out of this file (or merged with SharedCache).
  private static final class SharedCacheWrapper {
    private enum InitState {
      NOT_ENABLED, INITIALIZING, INITIALIZED, FAILED_FATAL
    }

    private final SharedCache instance = new SharedCache();
    private final Object initLock = new Object();
    private InitState initState = InitState.NOT_ENABLED;
    // We preserve the old setConf init behavior, where a failed prewarm would fail the query
    // and give a chance to another query to try prewarming again. Basically, we'd increment the
    // count and all the queries waiting for prewarm would fail; however, we will retry the prewarm
    // again infinitely, so some queries might succeed later.
    private int initFailureCount;
    private Throwable lastError;

    /**
     * A callback to updates the initialization state.
     * @param error Error, if any. Null means the initialization has succeeded.
     * @param isFatal Whether the error (if present) is fatal, or whether init will be retried.
     */
    void updateInitState(Throwable error, boolean isFatal) {
      boolean isSuccessful = error == null;
      synchronized (initLock) {
        if (isSuccessful) {
          initState = InitState.INITIALIZED;
        } else if (isFatal) {
          initState = InitState.FAILED_FATAL; 
          lastError = error;
        } else {
          ++initFailureCount;
          lastError = error;
        }
        initLock.notifyAll();
      }
    }

    void startInit(Configuration conf) {
      LOG.info("Initializing shared cache");
      synchronized (initLock) {
        assert initState == InitState.NOT_ENABLED;
        initState = InitState.INITIALIZING;
      }
      // The first iteration of the update thread will prewarm the cache.
      startCacheUpdateService(conf);
    }

    /**
     * Gets the SharedCache, waiting for initialization to happen if necessary.
     * Fails on any initialization error, even if the init will be retried later.
     */
    public SharedCache get() throws MetaException {
      if (!waitForInit()) return null;
      return instance;
    }

    /** Gets the shared cache unsafely (may not be ready to use); used by init methods. */
    SharedCache getUnsafe() {
      return instance;
    }

    private boolean waitForInit() throws MetaException {
      synchronized (initLock) {
        int localFailureCount = initFailureCount;
        while (true) {
          switch (initState) {
          case INITIALIZED: return true;
          case NOT_ENABLED: return false;
          case FAILED_FATAL: {
            throw new RuntimeException("CachedStore prewarm had a fatal error", lastError);
          }
          case INITIALIZING: {
            try {
              initLock.wait(100);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              throw new MetaException("Interrupted");
            }
            break;
          }
          default: throw new AssertionError(initState);
          }
          // Fail if any errors occured; mimicks the old behavior where a setConf prewarm failure
          // would fail the current task, but cause the next setConf to try prewarm again forever.
          if (initFailureCount != localFailureCount) {
            throw new RuntimeException("CachedStore prewarm failed", lastError);
          }
        }
      }
    }
  }

  @VisibleForTesting
  void setInitializedForTest() {
    sharedCacheWrapper.updateInitState(null, false);
  }
}
