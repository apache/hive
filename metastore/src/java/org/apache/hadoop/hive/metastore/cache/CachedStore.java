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
package org.apache.hadoop.hive.metastore.cache;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.FileMetadataHandler;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.PartFilterExprUtil;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.common.util.HiveStringUtils;
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
// TODO factor in NDV estimation (density based estimation) logic when merging NDVs from 2 colStats object
// TODO refactor to use same common code with StatObjectConverter (for merging 2 col stats objects)

public class CachedStore implements RawStore, Configurable {
  private static ScheduledExecutorService cacheUpdateMaster = null;
  private static AtomicReference<Thread> runningMasterThread = new AtomicReference<Thread>(null);
  RawStore rawStore;
  Configuration conf;
  private PartitionExpressionProxy expressionProxy = null;
  static boolean firstTime = true;

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

  @Override
  public void setConf(Configuration conf) {
    String rawStoreClassName = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_CACHED_RAW_STORE_IMPL,
        ObjectStore.class.getName());
    try {
      rawStore = ((Class<? extends RawStore>) MetaStoreUtils.getClass(
          rawStoreClassName)).newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Cannot instantiate " + rawStoreClassName, e);
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
    if (firstTime) {
      try {
        LOG.info("Prewarming CachedStore");
        prewarm();
        LOG.info("CachedStore initialized");
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      firstTime = false;
    }
  }

  private void prewarm() throws Exception {
    List<String> dbNames = rawStore.getAllDatabases();
    for (String dbName : dbNames) {
      Database db = rawStore.getDatabase(dbName);
      SharedCache.addDatabaseToCache(HiveStringUtils.normalizeIdentifier(dbName), db);
      List<String> tblNames = rawStore.getAllTables(dbName);
      for (String tblName : tblNames) {
        Table table = rawStore.getTable(dbName, tblName);
        SharedCache.addTableToCache(HiveStringUtils.normalizeIdentifier(dbName),
            HiveStringUtils.normalizeIdentifier(tblName), table);
        List<Partition> partitions = rawStore.getPartitions(dbName, tblName, Integer.MAX_VALUE);
        for (Partition partition : partitions) {
          SharedCache.addPartitionToCache(HiveStringUtils.normalizeIdentifier(dbName),
              HiveStringUtils.normalizeIdentifier(tblName), partition);
        }
        Map<String, ColumnStatisticsObj> aggrStatsPerPartition = rawStore
            .getAggrColStatsForTablePartitions(dbName, tblName);
        SharedCache.addPartitionColStatsToCache(aggrStatsPerPartition);
      }
    }
    // Start the cache update master-worker threads
    startCacheUpdateService();
  }

  private synchronized void startCacheUpdateService() {
    if (cacheUpdateMaster == null) {
      cacheUpdateMaster = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        public Thread newThread(Runnable r) {
          Thread t = Executors.defaultThreadFactory().newThread(r);
          t.setDaemon(true);
          return t;
        }
      });
      cacheUpdateMaster.scheduleAtFixedRate(new CacheUpdateMasterWork(this), 0, HiveConf
          .getTimeVar(conf, HiveConf.ConfVars.METASTORE_CACHED_RAW_STORE_CACHE_UPDATE_FREQUENCY,
              TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS);
    }
  }

  static class CacheUpdateMasterWork implements Runnable {

    private CachedStore cachedStore;

    public CacheUpdateMasterWork(CachedStore cachedStore) {
      this.cachedStore = cachedStore;
    }

    @Override
    public void run() {
      runningMasterThread.set(Thread.currentThread());
      RawStore rawStore = cachedStore.getRawStore();
      try {
        List<String> dbNames = rawStore.getAllDatabases();
        // Update the database in cache
        if (!updateDatabases(rawStore, dbNames)) {
          return;
        }
        // Update the tables and their partitions in cache
        if (!updateTables(rawStore, dbNames)) {
          return;
        }
      } catch (MetaException e) {
        LOG.error("Updating CachedStore: error getting database names", e);
      }
    }

    private boolean updateDatabases(RawStore rawStore, List<String> dbNames) {
      if (dbNames != null) {
        List<Database> databases = new ArrayList<Database>();
        for (String dbName : dbNames) {
          // If a preemption of this thread was requested, simply return before proceeding
          if (Thread.interrupted()) {
            return false;
          }
          Database db;
          try {
            db = rawStore.getDatabase(dbName);
            databases.add(db);
          } catch (NoSuchObjectException e) {
            LOG.info("Updating CachedStore: database - " + dbName + " does not exist.", e);
          }
        }
        // Update the cached database objects
        SharedCache.refreshDatabases(databases);
      }
      return true;
    }

    private boolean updateTables(RawStore rawStore, List<String> dbNames) {
      if (dbNames != null) {
        List<Table> tables = new ArrayList<Table>();
        for (String dbName : dbNames) {
          try {
            List<String> tblNames = rawStore.getAllTables(dbName);
            for (String tblName : tblNames) {
              // If a preemption of this thread was requested, simply return before proceeding
              if (Thread.interrupted()) {
                return false;
              }
              Table table = rawStore.getTable(dbName, tblName);
              tables.add(table);
            }
            // Update the cached database objects
            SharedCache.refreshTables(dbName, tables);
            for (String tblName : tblNames) {
              // If a preemption of this thread was requested, simply return before proceeding
              if (Thread.interrupted()) {
                return false;
              }
              List<Partition> partitions =
                  rawStore.getPartitions(dbName, tblName, Integer.MAX_VALUE);
              SharedCache.refreshPartitions(dbName, tblName, partitions);
            }
          } catch (MetaException | NoSuchObjectException e) {
            LOG.error("Updating CachedStore: unable to read table", e);
            return false;
          }
        }
      }
      return true;
    }
  }

  // Interrupt the cache update background thread
  // Fire and forget (the master will respond appropriately when it gets a chance)
  // All writes to the cache go through synchronized methods, so fire & forget is fine.
  private void interruptCacheUpdateMaster() {
    if (runningMasterThread.get() != null) {
      runningMasterThread.get().interrupt();
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
  public void rollbackTransaction() {
    rawStore.rollbackTransaction();
  }

  @Override
  public void createDatabase(Database db)
      throws InvalidObjectException, MetaException {
    rawStore.createDatabase(db);
    interruptCacheUpdateMaster();
    SharedCache.addDatabaseToCache(HiveStringUtils.normalizeIdentifier(db.getName()), db.deepCopy());
  }

  @Override
  public Database getDatabase(String dbName) throws NoSuchObjectException {
    Database db = SharedCache.getDatabaseFromCache(HiveStringUtils.normalizeIdentifier(dbName));
    if (db == null) {
      throw new NoSuchObjectException();
    }
    return SharedCache.getDatabaseFromCache(HiveStringUtils.normalizeIdentifier(dbName));
  }

  @Override
  public boolean dropDatabase(String dbname) throws NoSuchObjectException, MetaException {
    boolean succ = rawStore.dropDatabase(dbname);
    if (succ) {
      interruptCacheUpdateMaster();
      SharedCache.removeDatabaseFromCache(HiveStringUtils.normalizeIdentifier(dbname));
    }
    return succ;
  }

  @Override
  public boolean alterDatabase(String dbName, Database db)
      throws NoSuchObjectException, MetaException {
    boolean succ = rawStore.alterDatabase(dbName, db);
    if (succ) {
      interruptCacheUpdateMaster();
      SharedCache.alterDatabaseInCache(HiveStringUtils.normalizeIdentifier(dbName), db);
    }
    return succ;
  }

  @Override
  public List<String> getDatabases(String pattern) throws MetaException {
    List<String> results = new ArrayList<String>();
    for (String dbName : SharedCache.listCachedDatabases()) {
      dbName = HiveStringUtils.normalizeIdentifier(dbName);
      if (CacheUtils.matches(dbName, pattern)) {
        results.add(dbName);
      }
    }
    return results;
  }

  @Override
  public List<String> getAllDatabases() throws MetaException {
    return SharedCache.listCachedDatabases();
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
    boolean isExternal = "TRUE".equals(tbl.getParameters().get("EXTERNAL"));
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
  public void createTable(Table tbl)
      throws InvalidObjectException, MetaException {
    rawStore.createTable(tbl);
    interruptCacheUpdateMaster();
    validateTableType(tbl);
    SharedCache.addTableToCache(HiveStringUtils.normalizeIdentifier(tbl.getDbName()),
        HiveStringUtils.normalizeIdentifier(tbl.getTableName()), tbl);
  }

  @Override
  public boolean dropTable(String dbName, String tableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException,
      InvalidInputException {
    boolean succ = rawStore.dropTable(dbName, tableName);
    if (succ) {
      interruptCacheUpdateMaster();
      SharedCache.removeTableFromCache(HiveStringUtils.normalizeIdentifier(dbName),
          HiveStringUtils.normalizeIdentifier(tableName));
    }
    return succ;
  }

  @Override
  public Table getTable(String dbName, String tableName) throws MetaException {
    Table tbl = SharedCache.getTableFromCache(HiveStringUtils.normalizeIdentifier(dbName),
        HiveStringUtils.normalizeIdentifier(tableName));
    if (tbl != null) {
      tbl.unsetPrivileges();
      tbl.setRewriteEnabled(tbl.isRewriteEnabled());
    }
    return tbl;
  }

  @Override
  public boolean addPartition(Partition part)
      throws InvalidObjectException, MetaException {
    boolean succ = rawStore.addPartition(part);
    if (succ) {
      interruptCacheUpdateMaster();
      SharedCache.addPartitionToCache(HiveStringUtils.normalizeIdentifier(part.getDbName()),
          HiveStringUtils.normalizeIdentifier(part.getTableName()), part);
    }
    return succ;
  }

  @Override
  public boolean addPartitions(String dbName, String tblName,
      List<Partition> parts) throws InvalidObjectException, MetaException {
    boolean succ = rawStore.addPartitions(dbName, tblName, parts);
    if (succ) {
      interruptCacheUpdateMaster();
      for (Partition part : parts) {
        SharedCache.addPartitionToCache(HiveStringUtils.normalizeIdentifier(dbName),
            HiveStringUtils.normalizeIdentifier(tblName), part);
      }
    }
    return succ;
  }

  @Override
  public boolean addPartitions(String dbName, String tblName,
      PartitionSpecProxy partitionSpec, boolean ifNotExists)
      throws InvalidObjectException, MetaException {
    boolean succ = rawStore.addPartitions(dbName, tblName, partitionSpec, ifNotExists);
    if (succ) {
      interruptCacheUpdateMaster();
      PartitionSpecProxy.PartitionIterator iterator = partitionSpec.getPartitionIterator();
      while (iterator.hasNext()) {
        Partition part = iterator.next();
        SharedCache.addPartitionToCache(HiveStringUtils.normalizeIdentifier(dbName),
            HiveStringUtils.normalizeIdentifier(tblName), part);
      }
    }
    return succ;
  }

  @Override
  public Partition getPartition(String dbName, String tableName,
      List<String> part_vals) throws MetaException, NoSuchObjectException {
    Partition part = SharedCache.getPartitionFromCache(HiveStringUtils.normalizeIdentifier(dbName),
        HiveStringUtils.normalizeIdentifier(tableName), part_vals);
    if (part != null) {
      part.unsetPrivileges();
    }
    return part;
  }

  @Override
  public boolean doesPartitionExist(String dbName, String tableName,
      List<String> part_vals) throws MetaException, NoSuchObjectException {
    return SharedCache.existPartitionFromCache(HiveStringUtils.normalizeIdentifier(dbName),
        HiveStringUtils.normalizeIdentifier(tableName), part_vals);
  }

  @Override
  public boolean dropPartition(String dbName, String tableName,
      List<String> part_vals) throws MetaException, NoSuchObjectException,
      InvalidObjectException, InvalidInputException {
    boolean succ = rawStore.dropPartition(dbName, tableName, part_vals);
    if (succ) {
      interruptCacheUpdateMaster();
      SharedCache.removePartitionFromCache(HiveStringUtils.normalizeIdentifier(dbName),
          HiveStringUtils.normalizeIdentifier(tableName), part_vals);
    }
    return succ;
  }

  @Override
  public List<Partition> getPartitions(String dbName, String tableName, int max)
      throws MetaException, NoSuchObjectException {
    List<Partition> parts = SharedCache.listCachedPartitions(HiveStringUtils.normalizeIdentifier(dbName),
        HiveStringUtils.normalizeIdentifier(tableName), max);
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
    interruptCacheUpdateMaster();
    validateTableType(newTable);
    SharedCache.alterTableInCache(HiveStringUtils.normalizeIdentifier(dbName),
        HiveStringUtils.normalizeIdentifier(tblName), newTable);
  }

  @Override
  public List<String> getTables(String dbName, String pattern)
      throws MetaException {
    List<String> tableNames = new ArrayList<String>();
    for (Table table : SharedCache.listCachedTables(HiveStringUtils.normalizeIdentifier(dbName))) {
      if (CacheUtils.matches(table.getTableName(), pattern)) {
        tableNames.add(table.getTableName());
      }
    }
    return tableNames;
  }

  @Override
  public List<String> getTables(String dbName, String pattern,
      TableType tableType) throws MetaException {
    List<String> tableNames = new ArrayList<String>();
    for (Table table : SharedCache.listCachedTables(HiveStringUtils.normalizeIdentifier(dbName))) {
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
    return SharedCache.getTableMeta(HiveStringUtils.normalizeIdentifier(dbNames),
        HiveStringUtils.normalizeIdentifier(tableNames), tableTypes);
  }

  @Override
  public List<Table> getTableObjectsByName(String dbName,
      List<String> tblNames) throws MetaException, UnknownDBException {
    List<Table> tables = new ArrayList<Table>();
    for (String tblName : tblNames) {
      tables.add(SharedCache.getTableFromCache(HiveStringUtils.normalizeIdentifier(dbName),
          HiveStringUtils.normalizeIdentifier(tblName)));
    }
    return tables;
  }

  @Override
  public List<String> getAllTables(String dbName) throws MetaException {
    List<String> tblNames = new ArrayList<String>();
    for (Table tbl : SharedCache.listCachedTables(HiveStringUtils.normalizeIdentifier(dbName))) {
      tblNames.add(HiveStringUtils.normalizeIdentifier(tbl.getTableName()));
    }
    return tblNames;
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter,
      short max_tables) throws MetaException, UnknownDBException {
    List<String> tableNames = new ArrayList<String>();
    int count = 0;
    for (Table table : SharedCache.listCachedTables(HiveStringUtils.normalizeIdentifier(dbName))) {
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
    List<String> partitionNames = new ArrayList<String>();
    Table t = SharedCache.getTableFromCache(HiveStringUtils.normalizeIdentifier(dbName),
        HiveStringUtils.normalizeIdentifier(tblName));
    int count = 0;
    for (Partition part : SharedCache.listCachedPartitions(HiveStringUtils.normalizeIdentifier(dbName),
        HiveStringUtils.normalizeIdentifier(tblName), max_parts)) {
      if (max_parts == -1 || count < max_parts) {
        partitionNames.add(Warehouse.makePartName(t.getPartitionKeys(), part.getValues()));
      }
    }
    return partitionNames;
  }

  @Override
  public List<String> listPartitionNamesByFilter(String db_name,
      String tbl_name, String filter, short max_parts) throws MetaException {
    // TODO Translate filter -> expr
    return null;
  }

  @Override
  public void alterPartition(String dbName, String tblName,
      List<String> partVals, Partition newPart)
      throws InvalidObjectException, MetaException {
    rawStore.alterPartition(dbName, tblName, partVals, newPart);
    interruptCacheUpdateMaster();
    SharedCache.alterPartitionInCache(HiveStringUtils.normalizeIdentifier(dbName),
        HiveStringUtils.normalizeIdentifier(tblName), partVals, newPart);
  }

  @Override
  public void alterPartitions(String dbName, String tblName,
      List<List<String>> partValsList, List<Partition> newParts)
      throws InvalidObjectException, MetaException {
    rawStore.alterPartitions(dbName, tblName, partValsList, newParts);
    interruptCacheUpdateMaster();
    for (int i=0;i<partValsList.size();i++) {
      List<String> partVals = partValsList.get(i);
      Partition newPart = newParts.get(i);
      SharedCache.alterPartitionInCache(HiveStringUtils.normalizeIdentifier(dbName),
          HiveStringUtils.normalizeIdentifier(tblName), partVals, newPart);
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
      String defaultPartName, short maxParts, List<String> result) throws MetaException, NoSuchObjectException {
    List<Partition> parts = SharedCache.listCachedPartitions(
        HiveStringUtils.normalizeIdentifier(table.getDbName()),
        HiveStringUtils.normalizeIdentifier(table.getTableName()), maxParts);
    for (Partition part : parts) {
      result.add(Warehouse.makePartName(table.getPartitionKeys(), part.getValues()));
    }
    List<String> columnNames = new ArrayList<String>();
    List<PrimitiveTypeInfo> typeInfos = new ArrayList<PrimitiveTypeInfo>();
    for (FieldSchema fs : table.getPartitionKeys()) {
      columnNames.add(fs.getName());
      typeInfos.add(TypeInfoFactory.getPrimitiveTypeInfo(fs.getType()));
    }
    if (defaultPartName == null || defaultPartName.isEmpty()) {
      defaultPartName = HiveConf.getVar(getConf(), HiveConf.ConfVars.DEFAULTPARTITIONNAME);
    }
    return expressionProxy.filterPartitionsByExpr(
        columnNames, typeInfos, expr, defaultPartName, result);
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
    List<String> partNames = new LinkedList<String>();
    Table table = SharedCache.getTableFromCache(HiveStringUtils.normalizeIdentifier(dbName), HiveStringUtils.normalizeIdentifier(tblName));
    boolean hasUnknownPartitions = getPartitionNamesPrunedByExprNoTxn(
        table, expr, defaultPartitionName, maxParts, partNames);
    for (String partName : partNames) {
      Partition part = SharedCache.getPartitionFromCache(HiveStringUtils.normalizeIdentifier(dbName),
          HiveStringUtils.normalizeIdentifier(tblName), partNameToVals(partName));
      result.add(part);
    }
    return hasUnknownPartitions;
  }

  @Override
  public int getNumPartitionsByFilter(String dbName, String tblName,
      String filter) throws MetaException, NoSuchObjectException {
    Table table = SharedCache.getTableFromCache(HiveStringUtils.normalizeIdentifier(dbName),
        HiveStringUtils.normalizeIdentifier(tblName));
    // TODO filter -> expr
    return 0;
  }

  @Override
  public int getNumPartitionsByExpr(String dbName, String tblName, byte[] expr)
      throws MetaException, NoSuchObjectException {
    String defaultPartName = HiveConf.getVar(getConf(), HiveConf.ConfVars.DEFAULTPARTITIONNAME);
    List<String> partNames = new LinkedList<String>();
    Table table = SharedCache.getTableFromCache(HiveStringUtils.normalizeIdentifier(dbName),
        HiveStringUtils.normalizeIdentifier(tblName));
    getPartitionNamesPrunedByExprNoTxn(table, expr, defaultPartName, Short.MAX_VALUE, partNames);
    return partNames.size();
  }

  public static List<String> partNameToVals(String name) {
    if (name == null) return null;
    List<String> vals = new ArrayList<String>();
    String[] kvp = name.split("/");
    for (String kv : kvp) {
      vals.add(FileUtils.unescapePathName(kv.substring(kv.indexOf('=') + 1)));
    }
    return vals;
  }

  @Override
  public List<Partition> getPartitionsByNames(String dbName, String tblName,
      List<String> partNames) throws MetaException, NoSuchObjectException {
    List<Partition> partitions = new ArrayList<Partition>();
    for (String partName : partNames) {
      Partition part = SharedCache.getPartitionFromCache(HiveStringUtils.normalizeIdentifier(dbName),
          HiveStringUtils.normalizeIdentifier(tblName), partNameToVals(partName));
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
    Partition p = SharedCache.getPartitionFromCache(HiveStringUtils.normalizeIdentifier(dbName),
        HiveStringUtils.normalizeIdentifier(tblName), partVals);
    if (p!=null) {
      Table t = SharedCache.getTableFromCache(HiveStringUtils.normalizeIdentifier(dbName),
          HiveStringUtils.normalizeIdentifier(tblName));
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
    Table t = SharedCache.getTableFromCache(HiveStringUtils.normalizeIdentifier(dbName),
        HiveStringUtils.normalizeIdentifier(tblName));
    List<Partition> partitions = new ArrayList<Partition>();
    int count = 0;
    for (Partition part : SharedCache.listCachedPartitions(HiveStringUtils.normalizeIdentifier(dbName),
        HiveStringUtils.normalizeIdentifier(tblName), maxParts)) {
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
    List<String> partNames = new ArrayList<String>();
    int count = 0;
    Table t = SharedCache.getTableFromCache(HiveStringUtils.normalizeIdentifier(dbName),
        HiveStringUtils.normalizeIdentifier(tblName));
    for (Partition part : SharedCache.listCachedPartitions(HiveStringUtils.normalizeIdentifier(dbName),
        HiveStringUtils.normalizeIdentifier(tblName), maxParts)) {
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
        break;
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
    List<Partition> partitions = new ArrayList<Partition>();
    Table t = SharedCache.getTableFromCache(HiveStringUtils.normalizeIdentifier(dbName),
        HiveStringUtils.normalizeIdentifier(tblName));
    int count = 0;
    for (Partition part : SharedCache.listCachedPartitions(HiveStringUtils.normalizeIdentifier(dbName),
        HiveStringUtils.normalizeIdentifier(tblName), maxParts)) {
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
      throws NoSuchObjectException, MetaException, InvalidObjectException,
      InvalidInputException {
    boolean succ = rawStore.updateTableColumnStatistics(colStats);
    if (succ) {
      SharedCache.updateTableColumnStatistics(HiveStringUtils.normalizeIdentifier(colStats.getStatsDesc().getDbName()),
          HiveStringUtils.normalizeIdentifier(colStats.getStatsDesc().getTableName()), colStats.getStatsObj());
    }
    return succ;
  }

  @Override
  public boolean updatePartitionColumnStatistics(ColumnStatistics colStats,
      List<String> partVals) throws NoSuchObjectException, MetaException,
      InvalidObjectException, InvalidInputException {
    boolean succ = rawStore.updatePartitionColumnStatistics(colStats, partVals);
    if (succ) {
      SharedCache.updatePartitionColumnStatistics(HiveStringUtils.normalizeIdentifier(colStats.getStatsDesc().getDbName()),
          HiveStringUtils.normalizeIdentifier(colStats.getStatsDesc().getTableName()), partVals, colStats.getStatsObj());
    }
    return succ;
  }

  @Override
  public ColumnStatistics getTableColumnStatistics(String dbName,
      String tableName, List<String> colName)
      throws MetaException, NoSuchObjectException {
    return rawStore.getTableColumnStatistics(dbName, tableName, colName);
  }

  @Override
  public List<ColumnStatistics> getPartitionColumnStatistics(String dbName,
      String tblName, List<String> partNames, List<String> colNames)
      throws MetaException, NoSuchObjectException {
    return rawStore.getPartitionColumnStatistics(dbName, tblName, partNames, colNames);
  }

  @Override
  public boolean deletePartitionColumnStatistics(String dbName,
      String tableName, String partName, List<String> partVals, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException,
      InvalidInputException {
    return rawStore.deletePartitionColumnStatistics(dbName, tableName, partName, partVals, colName);
  }

  @Override
  public boolean deleteTableColumnStatistics(String dbName, String tableName,
      String colName) throws NoSuchObjectException, MetaException,
      InvalidObjectException, InvalidInputException {
    return rawStore.deleteTableColumnStatistics(dbName, tableName, colName);
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
  public void dropPartitions(String dbName, String tblName,
      List<String> partNames) throws MetaException, NoSuchObjectException {
    rawStore.dropPartitions(dbName, tblName, partNames);
    interruptCacheUpdateMaster();
    for (String partName : partNames) {
      List<String> vals = partNameToVals(partName);
      SharedCache.removePartitionFromCache(HiveStringUtils.normalizeIdentifier(dbName),
          HiveStringUtils.normalizeIdentifier(tblName), vals);
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
  public AggrStats get_aggr_stats_for(String dbName, String tblName,
      List<String> partNames, List<String> colNames)
      throws MetaException, NoSuchObjectException {
    List<ColumnStatisticsObj> colStats = new ArrayList<ColumnStatisticsObj>(colNames.size());
    for (String colName : colNames) {
      colStats.add(mergeColStatsForPartitions(HiveStringUtils.normalizeIdentifier(dbName),
          HiveStringUtils.normalizeIdentifier(tblName), partNames, colName));
    }
    // TODO: revisit the partitions not found case for extrapolation
    return new AggrStats(colStats, partNames.size());
  }

  private ColumnStatisticsObj mergeColStatsForPartitions(String dbName, String tblName,
      List<String> partNames, String colName) throws MetaException {
    ColumnStatisticsObj colStats = null;
    for (String partName : partNames) {
      String colStatsCacheKey = CacheUtils.buildKey(dbName, tblName, partNameToVals(partName), colName);
      ColumnStatisticsObj colStatsForPart = SharedCache.getCachedPartitionColStats(
          colStatsCacheKey);
      if (colStats == null) {
        colStats = colStatsForPart;
      } else {
        colStats = mergeColStatsObj(colStats, colStatsForPart);
      }
    }
    return colStats;
  }

  private ColumnStatisticsObj mergeColStatsObj(ColumnStatisticsObj colStats1,
      ColumnStatisticsObj colStats2) throws MetaException {
    if ((!colStats1.getColType().equalsIgnoreCase(colStats2.getColType()))
        && (!colStats1.getColName().equalsIgnoreCase(colStats2.getColName()))) {
      throw new MetaException("Can't merge column stats for two partitions for different columns.");
    }
    ColumnStatisticsData csd = new ColumnStatisticsData();
    ColumnStatisticsObj cso = new ColumnStatisticsObj(colStats1.getColName(),
        colStats1.getColType(), csd);
    ColumnStatisticsData csData1 = colStats1.getStatsData();
    ColumnStatisticsData csData2 = colStats2.getStatsData();
    String colType = colStats1.getColType().toLowerCase();
    if (colType.equals("boolean")) {
      BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
      boolStats.setNumFalses(csData1.getBooleanStats().getNumFalses()
          + csData2.getBooleanStats().getNumFalses());
      boolStats.setNumTrues(csData1.getBooleanStats().getNumTrues()
          + csData2.getBooleanStats().getNumTrues());
      boolStats.setNumNulls(csData1.getBooleanStats().getNumNulls()
          + csData2.getBooleanStats().getNumNulls());
      csd.setBooleanStats(boolStats);
    } else if (colType.equals("string") || colType.startsWith("varchar")
        || colType.startsWith("char")) {
      StringColumnStatsData stringStats = new StringColumnStatsData();
      stringStats.setNumNulls(csData1.getStringStats().getNumNulls()
          + csData2.getStringStats().getNumNulls());
      stringStats.setAvgColLen(Math.max(csData1.getStringStats().getAvgColLen(), csData2
          .getStringStats().getAvgColLen()));
      stringStats.setMaxColLen(Math.max(csData1.getStringStats().getMaxColLen(), csData2
          .getStringStats().getMaxColLen()));
      stringStats.setNumDVs(Math.max(csData1.getStringStats().getNumDVs(), csData2.getStringStats()
          .getNumDVs()));
      csd.setStringStats(stringStats);
    } else if (colType.equals("binary")) {
      BinaryColumnStatsData binaryStats = new BinaryColumnStatsData();
      binaryStats.setNumNulls(csData1.getBinaryStats().getNumNulls()
          + csData2.getBinaryStats().getNumNulls());
      binaryStats.setAvgColLen(Math.max(csData1.getBinaryStats().getAvgColLen(), csData2
          .getBinaryStats().getAvgColLen()));
      binaryStats.setMaxColLen(Math.max(csData1.getBinaryStats().getMaxColLen(), csData2
          .getBinaryStats().getMaxColLen()));
      csd.setBinaryStats(binaryStats);
    } else if (colType.equals("bigint") || colType.equals("int") || colType.equals("smallint")
        || colType.equals("tinyint") || colType.equals("timestamp")) {
      LongColumnStatsData longStats = new LongColumnStatsData();
      longStats.setNumNulls(csData1.getLongStats().getNumNulls()
          + csData2.getLongStats().getNumNulls());
      longStats.setHighValue(Math.max(csData1.getLongStats().getHighValue(), csData2.getLongStats()
          .getHighValue()));
      longStats.setLowValue(Math.min(csData1.getLongStats().getLowValue(), csData2.getLongStats()
          .getLowValue()));
      longStats.setNumDVs(Math.max(csData1.getLongStats().getNumDVs(), csData2.getLongStats()
          .getNumDVs()));
      csd.setLongStats(longStats);
    } else if (colType.equals("date")) {
      DateColumnStatsData dateStats = new DateColumnStatsData();
      dateStats.setNumNulls(csData1.getDateStats().getNumNulls()
          + csData2.getDateStats().getNumNulls());
      dateStats.setHighValue(new Date(Math.max(csData1.getDateStats().getHighValue()
          .getDaysSinceEpoch(), csData2.getDateStats().getHighValue().getDaysSinceEpoch())));
      dateStats.setHighValue(new Date(Math.min(csData1.getDateStats().getLowValue()
          .getDaysSinceEpoch(), csData2.getDateStats().getLowValue().getDaysSinceEpoch())));
      dateStats.setNumDVs(Math.max(csData1.getDateStats().getNumDVs(), csData2.getDateStats()
          .getNumDVs()));
      csd.setDateStats(dateStats);
    } else if (colType.equals("double") || colType.equals("float")) {
      DoubleColumnStatsData doubleStats = new DoubleColumnStatsData();
      doubleStats.setNumNulls(csData1.getDoubleStats().getNumNulls()
          + csData2.getDoubleStats().getNumNulls());
      doubleStats.setHighValue(Math.max(csData1.getDoubleStats().getHighValue(), csData2
          .getDoubleStats().getHighValue()));
      doubleStats.setLowValue(Math.min(csData1.getDoubleStats().getLowValue(), csData2
          .getDoubleStats().getLowValue()));
      doubleStats.setNumDVs(Math.max(csData1.getDoubleStats().getNumDVs(), csData2.getDoubleStats()
          .getNumDVs()));
      csd.setDoubleStats(doubleStats);
    } else if (colType.startsWith("decimal")) {
      DecimalColumnStatsData decimalStats = new DecimalColumnStatsData();
      decimalStats.setNumNulls(csData1.getDecimalStats().getNumNulls()
          + csData2.getDecimalStats().getNumNulls());
      Decimal high = (csData1.getDecimalStats().getHighValue()
          .compareTo(csData2.getDecimalStats().getHighValue()) > 0) ? csData1.getDecimalStats()
          .getHighValue() : csData2.getDecimalStats().getHighValue();
      decimalStats.setHighValue(high);
      Decimal low = (csData1.getDecimalStats().getLowValue()
          .compareTo(csData2.getDecimalStats().getLowValue()) < 0) ? csData1.getDecimalStats()
          .getLowValue() : csData2.getDecimalStats().getLowValue();
      decimalStats.setLowValue(low);
      decimalStats.setNumDVs(Math.max(csData1.getDecimalStats().getNumDVs(), csData2
          .getDecimalStats().getNumDVs()));
      csd.setDecimalStats(decimalStats);
    }
    return cso;
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
    return SharedCache.getCachedTableCount();
  }

  @Override
  public int getPartitionCount() throws MetaException {
    return SharedCache.getCachedPartitionCount();
  }

  @Override
  public int getDatabaseCount() throws MetaException {
    return SharedCache.getCachedDatabaseCount();
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
  public void createTableWithConstraints(Table tbl,
      List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys)
      throws InvalidObjectException, MetaException {
    // TODO constraintCache
    rawStore.createTableWithConstraints(tbl, primaryKeys, foreignKeys);
    SharedCache.addTableToCache(HiveStringUtils.normalizeIdentifier(tbl.getDbName()),
        HiveStringUtils.normalizeIdentifier(tbl.getTableName()), tbl);
  }

  @Override
  public void dropConstraint(String dbName, String tableName,
      String constraintName) throws NoSuchObjectException {
    // TODO constraintCache
    rawStore.dropConstraint(dbName, tableName, constraintName);
  }

  @Override
  public void addPrimaryKeys(List<SQLPrimaryKey> pks)
      throws InvalidObjectException, MetaException {
    // TODO constraintCache
    rawStore.addPrimaryKeys(pks);
  }

  @Override
  public void addForeignKeys(List<SQLForeignKey> fks)
      throws InvalidObjectException, MetaException {
    // TODO constraintCache
    rawStore.addForeignKeys(fks);
  }

  @Override
  public Map<String, ColumnStatisticsObj> getAggrColStatsForTablePartitions(
      String dbName, String tableName)
      throws MetaException, NoSuchObjectException {
    return rawStore.getAggrColStatsForTablePartitions(dbName, tableName);
  }

  public RawStore getRawStore() {
    return rawStore;
  }

  @VisibleForTesting
  public void setRawStore(RawStore rawStore) {
    this.rawStore = rawStore;
  }
}
