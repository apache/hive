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

import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.ISchemaName;
import org.apache.hadoop.hive.metastore.api.SchemaVersionDescriptor;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
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
import org.apache.hadoop.hive.metastore.api.WMNullablePool;
import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.cache.SharedCache.StatsType;
import org.apache.hadoop.hive.metastore.columnstats.aggr.ColumnStatsAggregator;
import org.apache.hadoop.hive.metastore.columnstats.aggr.ColumnStatsAggregatorFactory;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.ColStatsObjWithSourceInfo;
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
// TODO monitor event queue
// TODO initial load slow?
// TODO size estimation

public class CachedStore implements RawStore, Configurable {
  private static ScheduledExecutorService cacheUpdateMaster = null;
  private static List<Pattern> whitelistPatterns = null;
  private static List<Pattern> blacklistPatterns = null;
  // Default value set to 100 milliseconds for test purpose
  private static long DEFAULT_CACHE_REFRESH_PERIOD = 100;
  // Time after which metastore cache is updated from metastore DB by the background update thread
  private static long cacheRefreshPeriodMS = DEFAULT_CACHE_REFRESH_PERIOD;
  private static AtomicBoolean isCachePrewarmed = new AtomicBoolean(false);
  private RawStore rawStore = null;
  private Configuration conf;
  private PartitionExpressionProxy expressionProxy = null;
  private static final SharedCache sharedCache = new SharedCache();

  static final private Logger LOG = LoggerFactory.getLogger(CachedStore.class.getName());

  public CachedStore() {

  }

  @Override
  public void setConf(Configuration conf) {
    setConfInternal(conf);
    initBlackListWhiteList(conf);
    startCacheUpdateService(conf, false, true);
  }

  /**
   * Similar to setConf but used from within the tests
   * This does start the background thread for prewarm and update
   * @param conf
   */
  void setConfForTest(Configuration conf) {
    setConfInternal(conf);
    initBlackListWhiteList(conf);
  }

  private void setConfInternal(Configuration conf) {
    String rawStoreClassName =
        MetastoreConf.getVar(conf, ConfVars.CACHED_RAW_STORE_IMPL, ObjectStore.class.getName());
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
    } else {
      expressionProxy = PartFilterExprUtil.createExpressionProxy(conf);
    }
  }

  @VisibleForTesting
  /**
   * This initializes the caches in SharedCache by getting the objects from Metastore DB via
   * ObjectStore and populating the respective caches
   *
   * @param rawStore
   * @throws Exception
   */
  static void prewarm(RawStore rawStore) {
    if (isCachePrewarmed.get()) {
      return;
    }
    long startTime = System.nanoTime();
    LOG.info("Prewarming CachedStore");
    while (!isCachePrewarmed.get()) {
      // Prevents throwing exceptions in our raw store calls since we're not using RawStoreProxy
      Deadline.registerIfNot(1000000);
      List<String> dbNames;
      try {
        dbNames = rawStore.getAllDatabases();
      } catch (MetaException e) {
        // Try again
        continue;
      }
      LOG.info("Number of databases to prewarm: {}", dbNames.size());
      List<Database> databases = new ArrayList<>(dbNames.size());
      for (String dbName : dbNames) {
        try {
          databases.add(rawStore.getDatabase(dbName));
        } catch (NoSuchObjectException e) {
          // Continue with next database
          continue;
        }
      }
      sharedCache.populateDatabasesInCache(databases);
      LOG.debug(
          "Databases cache is now prewarmed. Now adding tables, partitions and statistics to the cache");
      int numberOfDatabasesCachedSoFar = 0;
      for (String dbName : dbNames) {
        dbName = StringUtils.normalizeIdentifier(dbName);
        List<String> tblNames;
        try {
          tblNames = rawStore.getAllTables(dbName);
        } catch (MetaException e) {
          // Continue with next database
          continue;
        }
        int numberOfTablesCachedSoFar = 0;
        for (String tblName : tblNames) {
          tblName = StringUtils.normalizeIdentifier(tblName);
          if (!shouldCacheTable(dbName, tblName)) {
            continue;
          }
          Table table;
          try {
            table = rawStore.getTable(dbName, tblName);
          } catch (MetaException e) {
            // It is possible the table is deleted during fetching tables of the database,
            // in that case, continue with the next table
            continue;
          }
          List<String> colNames = MetaStoreUtils.getColumnNamesForTable(table);
          try {
            ColumnStatistics tableColStats = null;
            List<Partition> partitions = null;
            List<ColumnStatistics> partitionColStats = null;
            AggrStats aggrStatsAllPartitions = null;
            AggrStats aggrStatsAllButDefaultPartition = null;
            if (table.isSetPartitionKeys()) {
              Deadline.startTimer("getPartitions");
              partitions = rawStore.getPartitions(dbName, tblName, Integer.MAX_VALUE);
              Deadline.stopTimer();
              List<String> partNames = new ArrayList<>(partitions.size());
              for (Partition p : partitions) {
                partNames.add(Warehouse.makePartName(table.getPartitionKeys(), p.getValues()));
              }
              if (!partNames.isEmpty()) {
                // Get partition column stats for this table
                Deadline.startTimer("getPartitionColumnStatistics");
                partitionColStats =
                    rawStore.getPartitionColumnStatistics(dbName, tblName, partNames, colNames);
                Deadline.stopTimer();
                // Get aggregate stats for all partitions of a table and for all but default
                // partition
                Deadline.startTimer("getAggrPartitionColumnStatistics");
                aggrStatsAllPartitions =
                    rawStore.get_aggr_stats_for(dbName, tblName, partNames, colNames);
                Deadline.stopTimer();
                // Remove default partition from partition names and get aggregate
                // stats again
                List<FieldSchema> partKeys = table.getPartitionKeys();
                String defaultPartitionValue =
                    MetastoreConf.getVar(rawStore.getConf(), ConfVars.DEFAULTPARTITIONNAME);
                List<String> partCols = new ArrayList<>();
                List<String> partVals = new ArrayList<>();
                for (FieldSchema fs : partKeys) {
                  partCols.add(fs.getName());
                  partVals.add(defaultPartitionValue);
                }
                String defaultPartitionName = FileUtils.makePartName(partCols, partVals);
                partNames.remove(defaultPartitionName);
                Deadline.startTimer("getAggrPartitionColumnStatistics");
                aggrStatsAllButDefaultPartition =
                    rawStore.get_aggr_stats_for(dbName, tblName, partNames, colNames);
                Deadline.stopTimer();
              }
            } else {
              Deadline.startTimer("getTableColumnStatistics");
              tableColStats = rawStore.getTableColumnStatistics(dbName, tblName, colNames);
              Deadline.stopTimer();
            }
            sharedCache.populateTableInCache(table, tableColStats, partitions, partitionColStats,
                aggrStatsAllPartitions, aggrStatsAllButDefaultPartition);
          } catch (MetaException | NoSuchObjectException e) {
            // Continue with next table
            continue;
          }
          LOG.debug("Processed database: {}'s table: {}. Cached {} / {}  tables so far.", dbName,
              tblName, ++numberOfTablesCachedSoFar, tblNames.size());
        }
        LOG.debug("Processed database: {}. Cached {} / {} databases so far.", dbName,
            ++numberOfDatabasesCachedSoFar, dbNames.size());
      }
      isCachePrewarmed.set(true);
    }
    LOG.info("CachedStore initialized");
    long endTime = System.nanoTime();
    LOG.info("Time taken in prewarming = " + (endTime - startTime) / 1000000 + "ms");
    sharedCache.completeTableCachePrewarm();
  }

  @VisibleForTesting
  static void setCachePrewarmedState(boolean state) {
    isCachePrewarmed.set(state);
  }

  private static void initBlackListWhiteList(Configuration conf) {
    if (whitelistPatterns == null || blacklistPatterns == null) {
      whitelistPatterns = createPatterns(MetastoreConf.getAsString(conf,
          MetastoreConf.ConfVars.CACHED_RAW_STORE_CACHED_OBJECTS_WHITELIST));
      blacklistPatterns = createPatterns(MetastoreConf.getAsString(conf,
          MetastoreConf.ConfVars.CACHED_RAW_STORE_CACHED_OBJECTS_BLACKLIST));
    }
  }

  @VisibleForTesting
  /**
   * This starts a background thread, which initially populates the SharedCache and later
   * periodically gets updates from the metastore db
   *
   * @param conf
   * @param runOnlyOnce
   * @param shouldRunPrewarm
   */
  static synchronized void startCacheUpdateService(Configuration conf, boolean runOnlyOnce,
      boolean shouldRunPrewarm) {
    if (cacheUpdateMaster == null) {
      initBlackListWhiteList(conf);
      if (!MetastoreConf.getBoolVar(conf, ConfVars.HIVE_IN_TEST)) {
        cacheRefreshPeriodMS = MetastoreConf.getTimeVar(conf,
            ConfVars.CACHED_RAW_STORE_CACHE_UPDATE_FREQUENCY, TimeUnit.MILLISECONDS);
      }
      LOG.info("CachedStore: starting cache update service (run every {} ms", cacheRefreshPeriodMS);
      cacheUpdateMaster = Executors.newScheduledThreadPool(1, new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread t = Executors.defaultThreadFactory().newThread(r);
          t.setName("CachedStore-CacheUpdateService: Thread-" + t.getId());
          t.setDaemon(true);
          return t;
        }
      });
      if (!runOnlyOnce) {
        cacheUpdateMaster.scheduleAtFixedRate(new CacheUpdateMasterWork(conf, shouldRunPrewarm), 0,
            cacheRefreshPeriodMS, TimeUnit.MILLISECONDS);
      }
    }
    if (runOnlyOnce) {
      // Some tests control the execution of the background update thread
      cacheUpdateMaster.schedule(new CacheUpdateMasterWork(conf, shouldRunPrewarm), 0,
          TimeUnit.MILLISECONDS);
    }
  }

  @VisibleForTesting
  static synchronized boolean stopCacheUpdateService(long timeout) {
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
    cacheRefreshPeriodMS = time;
  }

  static class CacheUpdateMasterWork implements Runnable {
    private boolean shouldRunPrewarm = true;
    private final RawStore rawStore;

    CacheUpdateMasterWork(Configuration conf, boolean shouldRunPrewarm) {
      this.shouldRunPrewarm = shouldRunPrewarm;
      String rawStoreClassName =
          MetastoreConf.getVar(conf, ConfVars.CACHED_RAW_STORE_IMPL, ObjectStore.class.getName());
      try {
        rawStore = JavaUtils.getClass(rawStoreClassName, RawStore.class).newInstance();
        rawStore.setConf(conf);
      } catch (InstantiationException | IllegalAccessException | MetaException e) {
        // MetaException here really means ClassNotFound (see the utility method).
        // So, if any of these happen, that means we can never succeed.
        throw new RuntimeException("Cannot instantiate " + rawStoreClassName, e);
      }
    }

    @Override
    public void run() {
      if (!shouldRunPrewarm) {
        // TODO: prewarm and update can probably be merged.
        update();
      } else {
        try {
          prewarm(rawStore);
        } catch (Exception e) {
          LOG.error("Prewarm failure", e);
          return;
        }
      }
    }

    void update() {
      Deadline.registerIfNot(1000000);
      LOG.debug("CachedStore: updating cached objects");
      List<String> dbNames;
      try {
        dbNames = rawStore.getAllDatabases();
      } catch (MetaException e) {
        LOG.error("Updating CachedStore: error happen when refresh; skipping this iteration", e);
        return;
      }
      // Update the database in cache
      updateDatabases(rawStore, dbNames);
      for (String dbName : dbNames) {
        // Update the tables in cache
        updateTables(rawStore, dbName);
        List<String> tblNames;
        try {
          tblNames = rawStore.getAllTables(dbName);
        } catch (MetaException e) {
          // Continue with next database
          continue;
        }
        for (String tblName : tblNames) {
          if (!shouldCacheTable(dbName, tblName)) {
            continue;
          }
          // Update the table column stats for a table in cache
          updateTableColStats(rawStore, dbName, tblName);
          // Update the partitions for a table in cache
          updateTablePartitions(rawStore, dbName, tblName);
          // Update the partition col stats for a table in cache
          updateTablePartitionColStats(rawStore, dbName, tblName);
          // Update aggregate partition column stats for a table in cache
          updateTableAggregatePartitionColStats(rawStore, dbName, tblName);
        }
      }
      sharedCache.incrementUpdateCount();
    }

    private void updateDatabases(RawStore rawStore, List<String> dbNames) {
      List<Database> databases = new ArrayList<>(dbNames.size());
      for (String dbName : dbNames) {
        Database db;
        try {
          db = rawStore.getDatabase(dbName);
          databases.add(db);
        } catch (NoSuchObjectException e) {
          LOG.info("Updating CachedStore: database - " + dbName + " does not exist.", e);
        }
      }
      sharedCache.refreshDatabasesInCache(databases);
    }

    private void updateTables(RawStore rawStore, String dbName) {
      List<Table> tables = new ArrayList<>();
      try {
        List<String> tblNames = rawStore.getAllTables(dbName);
        for (String tblName : tblNames) {
          if (!shouldCacheTable(dbName, tblName)) {
            continue;
          }
          Table table = rawStore.getTable(StringUtils.normalizeIdentifier(dbName),
              StringUtils.normalizeIdentifier(tblName));
          tables.add(table);
        }
        sharedCache.refreshTablesInCache(dbName, tables);
      } catch (MetaException e) {
        LOG.debug("Unable to refresh cached tables for database: " + dbName, e);
      }
    }

    private void updateTableColStats(RawStore rawStore, String dbName, String tblName) {
      try {
        Table table = rawStore.getTable(dbName, tblName);
        if (!table.isSetPartitionKeys()) {
          List<String> colNames = MetaStoreUtils.getColumnNamesForTable(table);
          Deadline.startTimer("getTableColumnStatistics");
          ColumnStatistics tableColStats =
              rawStore.getTableColumnStatistics(dbName, tblName, colNames);
          Deadline.stopTimer();
          if (tableColStats != null) {
            sharedCache.refreshTableColStatsInCache(StringUtils.normalizeIdentifier(dbName),
                StringUtils.normalizeIdentifier(tblName), tableColStats.getStatsObj());
          }
        }
      } catch (MetaException | NoSuchObjectException e) {
        LOG.info("Unable to refresh table column stats for table: " + tblName, e);
      }
    }

    private void updateTablePartitions(RawStore rawStore, String dbName, String tblName) {
      try {
        Deadline.startTimer("getPartitions");
        List<Partition> partitions = rawStore.getPartitions(dbName, tblName, Integer.MAX_VALUE);
        Deadline.stopTimer();
        sharedCache.refreshPartitionsInCache(StringUtils.normalizeIdentifier(dbName),
            StringUtils.normalizeIdentifier(tblName), partitions);
      } catch (MetaException | NoSuchObjectException e) {
        LOG.info("Updating CachedStore: unable to read partitions of table: " + tblName, e);
      }
    }

    private void updateTablePartitionColStats(RawStore rawStore, String dbName, String tblName) {
      try {
        Table table = rawStore.getTable(dbName, tblName);
        List<String> colNames = MetaStoreUtils.getColumnNamesForTable(table);
        List<String> partNames = rawStore.listPartitionNames(dbName, tblName, (short) -1);
        // Get partition column stats for this table
        Deadline.startTimer("getPartitionColumnStatistics");
        List<ColumnStatistics> partitionColStats =
            rawStore.getPartitionColumnStatistics(dbName, tblName, partNames, colNames);
        Deadline.stopTimer();
        sharedCache.refreshPartitionColStatsInCache(dbName, tblName, partitionColStats);
      } catch (MetaException | NoSuchObjectException e) {
        LOG.info("Updating CachedStore: unable to read partitions of table: " + tblName, e);
      }
    }

    // Update cached aggregate stats for all partitions of a table and for all
    // but default partition
    private void updateTableAggregatePartitionColStats(RawStore rawStore, String dbName,
        String tblName) {
      try {
        Table table = rawStore.getTable(dbName, tblName);
        List<String> partNames = rawStore.listPartitionNames(dbName, tblName, (short) -1);
        List<String> colNames = MetaStoreUtils.getColumnNamesForTable(table);
        if ((partNames != null) && (partNames.size() > 0)) {
          Deadline.startTimer("getAggregareStatsForAllPartitions");
          AggrStats aggrStatsAllPartitions =
              rawStore.get_aggr_stats_for(dbName, tblName, partNames, colNames);
          Deadline.stopTimer();
          // Remove default partition from partition names and get aggregate stats again
          List<FieldSchema> partKeys = table.getPartitionKeys();
          String defaultPartitionValue =
              MetastoreConf.getVar(rawStore.getConf(), ConfVars.DEFAULTPARTITIONNAME);
          List<String> partCols = new ArrayList<String>();
          List<String> partVals = new ArrayList<String>();
          for (FieldSchema fs : partKeys) {
            partCols.add(fs.getName());
            partVals.add(defaultPartitionValue);
          }
          String defaultPartitionName = FileUtils.makePartName(partCols, partVals);
          partNames.remove(defaultPartitionName);
          Deadline.startTimer("getAggregareStatsForAllPartitionsExceptDefault");
          AggrStats aggrStatsAllButDefaultPartition =
              rawStore.get_aggr_stats_for(dbName, tblName, partNames, colNames);
          Deadline.stopTimer();
          sharedCache.refreshAggregateStatsInCache(StringUtils.normalizeIdentifier(dbName),
              StringUtils.normalizeIdentifier(tblName), aggrStatsAllPartitions,
              aggrStatsAllButDefaultPartition);
        }
      } catch (MetaException | NoSuchObjectException e) {
        LOG.info("Updating CachedStore: unable to read aggregate column stats of table: " + tblName,
            e);
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
    sharedCache.addDatabaseToCache(db);
  }

  @Override
  public Database getDatabase(String dbName) throws NoSuchObjectException {
    if (!sharedCache.isDatabaseCachePrewarmed()) {
      return rawStore.getDatabase(dbName);
    }
    dbName = dbName.toLowerCase();
    Database db =
        sharedCache.getDatabaseFromCache(StringUtils.normalizeIdentifier(dbName));
    if (db == null) {
      throw new NoSuchObjectException();
    }
    return db;
  }

  @Override
  public boolean dropDatabase(String dbName) throws NoSuchObjectException, MetaException {
    boolean succ = rawStore.dropDatabase(dbName);
    if (succ) {
      dbName = dbName.toLowerCase();
      sharedCache.removeDatabaseFromCache(StringUtils.normalizeIdentifier(dbName));
    }
    return succ;
  }

  @Override
  public boolean alterDatabase(String dbName, Database db)
      throws NoSuchObjectException, MetaException {
    boolean succ = rawStore.alterDatabase(dbName, db);
    if (succ) {
      dbName = dbName.toLowerCase();
      sharedCache.alterDatabaseInCache(StringUtils.normalizeIdentifier(dbName), db);
    }
    return succ;
  }

  @Override
  public List<String> getDatabases(String pattern) throws MetaException {
    if (!sharedCache.isDatabaseCachePrewarmed()) {
      return rawStore.getDatabases(pattern);
    }
    return sharedCache.listCachedDatabases(pattern);
  }

  @Override
  public List<String> getAllDatabases() throws MetaException {
    if (!sharedCache.isDatabaseCachePrewarmed()) {
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
    String dbName = StringUtils.normalizeIdentifier(tbl.getDbName());
    String tblName = StringUtils.normalizeIdentifier(tbl.getTableName());
    if (!shouldCacheTable(dbName, tblName)) {
      return;
    }
    validateTableType(tbl);
    sharedCache.addTableToCache(dbName, tblName, tbl);
  }

  @Override
  public boolean dropTable(String dbName, String tblName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    boolean succ = rawStore.dropTable(dbName, tblName);
    if (succ) {
      dbName = StringUtils.normalizeIdentifier(dbName);
      tblName = StringUtils.normalizeIdentifier(tblName);
      if (!shouldCacheTable(dbName, tblName)) {
        return succ;
      }
      sharedCache.removeTableFromCache(dbName, tblName);
    }
    return succ;
  }

  @Override
  public Table getTable(String dbName, String tblName) throws MetaException {
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    if (!shouldCacheTable(dbName, tblName)) {
      return rawStore.getTable(dbName, tblName);
    }
    Table tbl = sharedCache.getTableFromCache(dbName, tblName);
    if (tbl == null) {
      // This table is not yet loaded in cache
      return rawStore.getTable(dbName, tblName);
    }
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
      String dbName = StringUtils.normalizeIdentifier(part.getDbName());
      String tblName = StringUtils.normalizeIdentifier(part.getTableName());
      if (!shouldCacheTable(dbName, tblName)) {
        return succ;
      }
      sharedCache.addPartitionToCache(dbName, tblName, part);
    }
    return succ;
  }

  @Override
  public boolean addPartitions(String dbName, String tblName, List<Partition> parts)
      throws InvalidObjectException, MetaException {
    boolean succ = rawStore.addPartitions(dbName, tblName, parts);
    if (succ) {
      dbName = StringUtils.normalizeIdentifier(dbName);
      tblName = StringUtils.normalizeIdentifier(tblName);
      if (!shouldCacheTable(dbName, tblName)) {
        return succ;
      }
      sharedCache.addPartitionsToCache(dbName, tblName, parts);
    }
    return succ;
  }

  @Override
  public boolean addPartitions(String dbName, String tblName, PartitionSpecProxy partitionSpec,
      boolean ifNotExists) throws InvalidObjectException, MetaException {
    boolean succ = rawStore.addPartitions(dbName, tblName, partitionSpec, ifNotExists);
    if (succ) {
      dbName = StringUtils.normalizeIdentifier(dbName);
      tblName = StringUtils.normalizeIdentifier(tblName);
      if (!shouldCacheTable(dbName, tblName)) {
        return succ;
      }
      PartitionSpecProxy.PartitionIterator iterator = partitionSpec.getPartitionIterator();
      while (iterator.hasNext()) {
        Partition part = iterator.next();
        sharedCache.addPartitionToCache(dbName, tblName, part);
      }
    }
    return succ;
  }

  @Override
  public Partition getPartition(String dbName, String tblName, List<String> part_vals)
      throws MetaException, NoSuchObjectException {
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    if (!shouldCacheTable(dbName, tblName)) {
      return rawStore.getPartition(dbName, tblName, part_vals);
    }
    Partition part = sharedCache.getPartitionFromCache(dbName, tblName, part_vals);
    if (part == null) {
      // The table containing the partition is not yet loaded in cache
      return rawStore.getPartition(dbName, tblName, part_vals);
    }
    return part;
  }

  @Override
  public boolean doesPartitionExist(String dbName, String tblName,
      List<String> part_vals) throws MetaException, NoSuchObjectException {
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    if (!shouldCacheTable(dbName, tblName)) {
      return rawStore.doesPartitionExist(dbName, tblName, part_vals);
    }
    Table tbl = sharedCache.getTableFromCache(dbName, tblName);
    if (tbl == null) {
      // The table containing the partition is not yet loaded in cache
      return rawStore.doesPartitionExist(dbName, tblName, part_vals);
    }
    return sharedCache.existPartitionFromCache(dbName, tblName, part_vals);
  }

  @Override
  public boolean dropPartition(String dbName, String tblName, List<String> part_vals)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    boolean succ = rawStore.dropPartition(dbName, tblName, part_vals);
    if (succ) {
      dbName = StringUtils.normalizeIdentifier(dbName);
      tblName = StringUtils.normalizeIdentifier(tblName);
      if (!shouldCacheTable(dbName, tblName)) {
        return succ;
      }
      sharedCache.removePartitionFromCache(dbName, tblName, part_vals);
    }
    return succ;
  }

  @Override
  public void dropPartitions(String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException {
    rawStore.dropPartitions(dbName, tblName, partNames);
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    if (!shouldCacheTable(dbName, tblName)) {
      return;
    }
    List<List<String>> partVals = new ArrayList<List<String>>();
    for (String partName : partNames) {
      partVals.add(partNameToVals(partName));
    }
    sharedCache.removePartitionsFromCache(dbName, tblName, partVals);
  }

  @Override
  public List<Partition> getPartitions(String dbName, String tblName, int max)
      throws MetaException, NoSuchObjectException {
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    if (!shouldCacheTable(dbName, tblName)) {
      return rawStore.getPartitions(dbName, tblName, max);
    }
    Table tbl = sharedCache.getTableFromCache(dbName, tblName);
    if (tbl == null) {
      // The table containing the partitions is not yet loaded in cache
      return rawStore.getPartitions(dbName, tblName, max);
    }
    List<Partition> parts = sharedCache.listCachedPartitions(dbName, tblName, max);
    return parts;
  }

  @Override
  public void alterTable(String dbName, String tblName, Table newTable)
      throws InvalidObjectException, MetaException {
    rawStore.alterTable(dbName, tblName, newTable);
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    String newTblName = StringUtils.normalizeIdentifier(newTable.getTableName());
    if (!shouldCacheTable(dbName, tblName) && !shouldCacheTable(dbName, newTblName)) {
      return;
    }
    Table tbl = sharedCache.getTableFromCache(dbName, tblName);
    if (tbl == null) {
      // The table is not yet loaded in cache
      return;
    }
    if (shouldCacheTable(dbName, tblName) && shouldCacheTable(dbName, newTblName)) {
      // If old table is in the cache and the new table can also be cached
      sharedCache.alterTableInCache(dbName, tblName, newTable);
    } else if (!shouldCacheTable(dbName, tblName) && shouldCacheTable(dbName, newTblName)) {
      // If old table is *not* in the cache but the new table can be cached
      sharedCache.addTableToCache(dbName, newTblName, newTable);
    } else if (shouldCacheTable(dbName, tblName) && !shouldCacheTable(dbName, newTblName)) {
      // If old table is in the cache but the new table *cannot* be cached
      sharedCache.removeTableFromCache(dbName, tblName);
    }
  }

  @Override
  public void updateCreationMetadata(String dbname, String tablename, CreationMetadata cm)
      throws MetaException {
    rawStore.updateCreationMetadata(dbname, tablename, cm);
  }

  @Override
  public List<String> getTables(String dbName, String pattern) throws MetaException {
    if (!isBlacklistWhitelistEmpty(conf) || !isCachePrewarmed.get()) {
      return rawStore.getTables(dbName, pattern);
    }
    return sharedCache.listCachedTableNames(StringUtils.normalizeIdentifier(dbName), pattern,
        (short) -1);
  }

  @Override
  public List<String> getTables(String dbName, String pattern, TableType tableType)
      throws MetaException {
    if (!isBlacklistWhitelistEmpty(conf) || !isCachePrewarmed.get()) {
      return rawStore.getTables(dbName, pattern, tableType);
    }
    return sharedCache.listCachedTableNames(StringUtils.normalizeIdentifier(dbName), pattern,
        tableType);
  }

  @Override
  public List<String> getMaterializedViewsForRewriting(String dbName)
      throws MetaException, NoSuchObjectException {
    return rawStore.getMaterializedViewsForRewriting(dbName);
  }

  @Override
  public List<TableMeta> getTableMeta(String dbNames, String tableNames, List<String> tableTypes)
      throws MetaException {
    // TODO Check if all required tables are allowed, if so, get it from cache
    if (!isBlacklistWhitelistEmpty(conf) || !isCachePrewarmed.get()) {
      return rawStore.getTableMeta(dbNames, tableNames, tableTypes);
    }
    return sharedCache.getTableMeta(StringUtils.normalizeIdentifier(dbNames),
        StringUtils.normalizeIdentifier(tableNames), tableTypes);
  }

  @Override
  public List<Table> getTableObjectsByName(String dbName, List<String> tblNames)
      throws MetaException, UnknownDBException {
    dbName = StringUtils.normalizeIdentifier(dbName);
    boolean missSomeInCache = false;
    for (String tblName : tblNames) {
      tblName = StringUtils.normalizeIdentifier(tblName);
      if (!shouldCacheTable(dbName, tblName)) {
        missSomeInCache = true;
        break;
      }
    }
    if (!isCachePrewarmed.get() || missSomeInCache) {
      return rawStore.getTableObjectsByName(dbName, tblNames);
    }
    List<Table> tables = new ArrayList<>();
    for (String tblName : tblNames) {
      tblName = StringUtils.normalizeIdentifier(tblName);
      Table tbl = sharedCache.getTableFromCache(dbName, tblName);
      if (tbl == null) {
        tbl = rawStore.getTable(dbName, tblName);
      }
      tables.add(tbl);
    }
    return tables;
  }

  @Override
  public List<String> getAllTables(String dbName) throws MetaException {
    if (!isBlacklistWhitelistEmpty(conf) || !isCachePrewarmed.get()) {
      return rawStore.getAllTables(dbName);
    }
    return sharedCache.listCachedTableNames(StringUtils.normalizeIdentifier(dbName));
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter, short max_tables)
      throws MetaException, UnknownDBException {
    if (!isBlacklistWhitelistEmpty(conf) || !isCachePrewarmed.get()) {
      return rawStore.listTableNamesByFilter(dbName, filter, max_tables);
    }
    return sharedCache.listCachedTableNames(StringUtils.normalizeIdentifier(dbName), filter,
        max_tables);
  }

  @Override
  public List<String> listPartitionNames(String dbName, String tblName,
      short max_parts) throws MetaException {
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    if (!shouldCacheTable(dbName, tblName)) {
      return rawStore.listPartitionNames(dbName, tblName, max_parts);
    }
    Table tbl = sharedCache.getTableFromCache(dbName, tblName);
    if (tbl == null) {
      // The table is not yet loaded in cache
      return rawStore.listPartitionNames(dbName, tblName, max_parts);
    }
    List<String> partitionNames = new ArrayList<>();
    int count = 0;
    for (Partition part : sharedCache.listCachedPartitions(dbName, tblName, max_parts)) {
      if (max_parts == -1 || count < max_parts) {
        partitionNames.add(Warehouse.makePartName(tbl.getPartitionKeys(), part.getValues()));
      }
    }
    return partitionNames;
  }

  @Override
  public PartitionValuesResponse listPartitionValues(String db_name, String tbl_name,
      List<FieldSchema> cols, boolean applyDistinct, String filter, boolean ascending,
      List<FieldSchema> order, long maxParts) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listPartitionNamesByFilter(String dbName,
      String tblName, String filter, short max_parts) throws MetaException {
    // TODO Translate filter -> expr
    return rawStore.listPartitionNamesByFilter(dbName, tblName, filter, max_parts);
  }

  @Override
  public void alterPartition(String dbName, String tblName, List<String> partVals, Partition newPart)
      throws InvalidObjectException, MetaException {
    rawStore.alterPartition(dbName, tblName, partVals, newPart);
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    if (!shouldCacheTable(dbName, tblName)) {
      return;
    }
    sharedCache.alterPartitionInCache(dbName, tblName, partVals, newPart);
  }

  @Override
  public void alterPartitions(String dbName, String tblName, List<List<String>> partValsList,
      List<Partition> newParts) throws InvalidObjectException, MetaException {
    rawStore.alterPartitions(dbName, tblName, partValsList, newParts);
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    if (!shouldCacheTable(dbName, tblName)) {
      return;
    }
    sharedCache.alterPartitionsInCache(dbName, tblName, partValsList, newParts);
  }

  private boolean getPartitionNamesPrunedByExprNoTxn(Table table, byte[] expr,
      String defaultPartName, short maxParts, List<String> result, SharedCache sharedCache)
      throws MetaException, NoSuchObjectException {
    List<Partition> parts =
        sharedCache.listCachedPartitions(StringUtils.normalizeIdentifier(table.getDbName()),
            StringUtils.normalizeIdentifier(table.getTableName()), maxParts);
    for (Partition part : parts) {
      result.add(Warehouse.makePartName(table.getPartitionKeys(), part.getValues()));
    }
    if (defaultPartName == null || defaultPartName.isEmpty()) {
      defaultPartName = MetastoreConf.getVar(getConf(), ConfVars.DEFAULTPARTITIONNAME);
    }
    return expressionProxy.filterPartitionsByExpr(table.getPartitionKeys(), expr, defaultPartName,
        result);
  }

  @Override
  public List<Partition> getPartitionsByFilter(String dbName, String tblName,
      String filter, short maxParts)
      throws MetaException, NoSuchObjectException {
    return rawStore.getPartitionsByFilter(dbName, tblName, filter, maxParts);
  }

  @Override
  public boolean getPartitionsByExpr(String dbName, String tblName, byte[] expr,
      String defaultPartitionName, short maxParts, List<Partition> result) throws TException {
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    if (!shouldCacheTable(dbName, tblName)) {
      return rawStore.getPartitionsByExpr(dbName, tblName, expr, defaultPartitionName, maxParts,
          result);
    }
    List<String> partNames = new LinkedList<>();
    Table table = sharedCache.getTableFromCache(dbName, tblName);
    if (table == null) {
      // The table is not yet loaded in cache
      return rawStore.getPartitionsByExpr(dbName, tblName, expr, defaultPartitionName, maxParts,
          result);
    }
    boolean hasUnknownPartitions = getPartitionNamesPrunedByExprNoTxn(table, expr,
        defaultPartitionName, maxParts, partNames, sharedCache);
    return hasUnknownPartitions;
  }

  @Override
  public int getNumPartitionsByFilter(String dbName, String tblName, String filter)
      throws MetaException, NoSuchObjectException {
    return rawStore.getNumPartitionsByFilter(dbName, tblName, filter);
  }

  @Override
  public int getNumPartitionsByExpr(String dbName, String tblName, byte[] expr)
      throws MetaException, NoSuchObjectException {
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    if (!shouldCacheTable(dbName, tblName)) {
      return rawStore.getNumPartitionsByExpr(dbName, tblName, expr);
    }
    String defaultPartName = MetastoreConf.getVar(getConf(), ConfVars.DEFAULTPARTITIONNAME);
    List<String> partNames = new LinkedList<>();
    Table table = sharedCache.getTableFromCache(dbName, tblName);
    if (table == null) {
      // The table is not yet loaded in cache
      return rawStore.getNumPartitionsByExpr(dbName, tblName, expr);
    }
    getPartitionNamesPrunedByExprNoTxn(table, expr, defaultPartName, Short.MAX_VALUE, partNames,
        sharedCache);
    return partNames.size();
  }

  private static List<String> partNameToVals(String name) {
    if (name == null) {
      return null;
    }
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
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    if (!shouldCacheTable(dbName, tblName)) {
      return rawStore.getPartitionsByNames(dbName, tblName, partNames);
    }
    Table table = sharedCache.getTableFromCache(dbName, tblName);
    if (table == null) {
      // The table is not yet loaded in cache
      return rawStore.getPartitionsByNames(dbName, tblName, partNames);
    }
    List<Partition> partitions = new ArrayList<>();
    for (String partName : partNames) {
      Partition part = sharedCache.getPartitionFromCache(dbName, tblName, partNameToVals(partName));
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
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    if (!shouldCacheTable(dbName, tblName)) {
      return rawStore.getPartitionWithAuth(dbName, tblName, partVals, userName, groupNames);
    }
    Table table = sharedCache.getTableFromCache(dbName, tblName);
    if (table == null) {
      // The table is not yet loaded in cache
      return rawStore.getPartitionWithAuth(dbName, tblName, partVals, userName, groupNames);
    }
    Partition p = sharedCache.getPartitionFromCache(dbName, tblName, partVals);
    if (p != null) {
      String partName = Warehouse.makePartName(table.getPartitionKeys(), partVals);
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
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    if (!shouldCacheTable(dbName, tblName)) {
      return rawStore.getPartitionsWithAuth(dbName, tblName, maxParts, userName, groupNames);
    }
    Table table = sharedCache.getTableFromCache(dbName, tblName);
    if (table == null) {
      // The table is not yet loaded in cache
      return rawStore.getPartitionsWithAuth(dbName, tblName, maxParts, userName, groupNames);
    }
    List<Partition> partitions = new ArrayList<>();
    int count = 0;
    for (Partition part : sharedCache.listCachedPartitions(dbName, tblName, maxParts)) {
      if (maxParts == -1 || count < maxParts) {
        String partName = Warehouse.makePartName(table.getPartitionKeys(), part.getValues());
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
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    if (!shouldCacheTable(dbName, tblName)) {
      return rawStore.listPartitionNamesPs(dbName, tblName, partVals, maxParts);
    }
    Table table = sharedCache.getTableFromCache(dbName, tblName);
    if (table == null) {
      // The table is not yet loaded in cache
      return rawStore.listPartitionNamesPs(dbName, tblName, partVals, maxParts);
    }
    List<String> partNames = new ArrayList<>();
    int count = 0;
    for (Partition part : sharedCache.listCachedPartitions(dbName, tblName, maxParts)) {
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
        partNames.add(Warehouse.makePartName(table.getPartitionKeys(), part.getValues()));
        count++;
      }
    }
    return partNames;
  }

  @Override
  public List<Partition> listPartitionsPsWithAuth(String dbName, String tblName,
      List<String> partVals, short maxParts, String userName, List<String> groupNames)
      throws MetaException, InvalidObjectException, NoSuchObjectException {
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    if (!shouldCacheTable(dbName, tblName)) {
      return rawStore.listPartitionsPsWithAuth(dbName, tblName, partVals, maxParts, userName,
          groupNames);
    }
    Table table = sharedCache.getTableFromCache(dbName, tblName);
    if (table == null) {
      // The table is not yet loaded in cache
      return rawStore.listPartitionsPsWithAuth(dbName, tblName, partVals, maxParts, userName,
          groupNames);
    }
    List<Partition> partitions = new ArrayList<>();
    int count = 0;
    for (Partition part : sharedCache.listCachedPartitions(dbName, tblName, maxParts)) {
      boolean psMatch = true;
      for (int i = 0; i < partVals.size(); i++) {
        String psVal = partVals.get(i);
        String partVal = part.getValues().get(i);
        if (psVal != null && !psVal.isEmpty() && !psVal.equals(partVal)) {
          psMatch = false;
          break;
        }
      }
      if (!psMatch) {
        continue;
      }
      if (maxParts == -1 || count < maxParts) {
        String partName = Warehouse.makePartName(table.getPartitionKeys(), part.getValues());
        PrincipalPrivilegeSet privs =
            getPartitionPrivilegeSet(dbName, tblName, partName, userName, groupNames);
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
      String dbName = StringUtils.normalizeIdentifier(colStats.getStatsDesc().getDbName());
      String tblName = StringUtils.normalizeIdentifier(colStats.getStatsDesc().getTableName());
      if (!shouldCacheTable(dbName, tblName)) {
        return succ;
      }
      Table table = sharedCache.getTableFromCache(dbName, tblName);
      if (table == null) {
        // The table is not yet loaded in cache
        return succ;
      }
      List<ColumnStatisticsObj> statsObjs = colStats.getStatsObj();
      List<String> colNames = new ArrayList<>();
      for (ColumnStatisticsObj statsObj : statsObjs) {
        colNames.add(statsObj.getColName());
      }
      StatsSetupConst.setColumnStatsState(table.getParameters(), colNames);
      sharedCache.alterTableInCache(dbName, tblName, table);
      sharedCache.updateTableColStatsInCache(dbName, tblName, statsObjs);
    }
    return succ;
  }

  @Override
  public ColumnStatistics getTableColumnStatistics(String dbName, String tblName,
      List<String> colNames) throws MetaException, NoSuchObjectException {
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    if (!shouldCacheTable(dbName, tblName)) {
      return rawStore.getTableColumnStatistics(dbName, tblName, colNames);
    }
    Table table = sharedCache.getTableFromCache(dbName, tblName);
    if (table == null) {
      // The table is not yet loaded in cache
      return rawStore.getTableColumnStatistics(dbName, tblName, colNames);
    }
    ColumnStatisticsDesc csd = new ColumnStatisticsDesc(true, dbName, tblName);
    List<ColumnStatisticsObj> colStatObjs =
        sharedCache.getTableColStatsFromCache(dbName, tblName, colNames);
    return new ColumnStatistics(csd, colStatObjs);
  }

  @Override
  public boolean deleteTableColumnStatistics(String dbName, String tblName, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean succ = rawStore.deleteTableColumnStatistics(dbName, tblName, colName);
    if (succ) {
      dbName = StringUtils.normalizeIdentifier(dbName);
      tblName = StringUtils.normalizeIdentifier(tblName);
      if (!shouldCacheTable(dbName, tblName)) {
        return succ;
      }
      sharedCache.removeTableColStatsFromCache(dbName, tblName, colName);
    }
    return succ;
  }

  @Override
  public boolean updatePartitionColumnStatistics(ColumnStatistics colStats, List<String> partVals)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean succ = rawStore.updatePartitionColumnStatistics(colStats, partVals);
    if (succ) {
      String dbName = StringUtils.normalizeIdentifier(colStats.getStatsDesc().getDbName());
      String tblName = StringUtils.normalizeIdentifier(colStats.getStatsDesc().getTableName());
      if (!shouldCacheTable(dbName, tblName)) {
        return succ;
      }
      List<ColumnStatisticsObj> statsObjs = colStats.getStatsObj();
      Partition part = getPartition(dbName, tblName, partVals);
      List<String> colNames = new ArrayList<>();
      for (ColumnStatisticsObj statsObj : statsObjs) {
        colNames.add(statsObj.getColName());
      }
      StatsSetupConst.setColumnStatsState(part.getParameters(), colNames);
      sharedCache.alterPartitionInCache(dbName, tblName, partVals, part);
      sharedCache.updatePartitionColStatsInCache(dbName, tblName, partVals, colStats.getStatsObj());
    }
    return succ;
  }

  @Override
  // TODO: calculate from cached values.
  public List<ColumnStatistics> getPartitionColumnStatistics(String dbName, String tblName,
      List<String> partNames, List<String> colNames) throws MetaException, NoSuchObjectException {
    return rawStore.getPartitionColumnStatistics(dbName, tblName, partNames, colNames);
  }

  @Override
  public boolean deletePartitionColumnStatistics(String dbName, String tblName, String partName,
      List<String> partVals, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean succ =
        rawStore.deletePartitionColumnStatistics(dbName, tblName, partName, partVals, colName);
    if (succ) {
      dbName = StringUtils.normalizeIdentifier(dbName);
      tblName = StringUtils.normalizeIdentifier(tblName);
      if (!shouldCacheTable(dbName, tblName)) {
        return succ;
      }
      sharedCache.removePartitionColStatsFromCache(dbName, tblName, partVals, colName);
    }
    return succ;
  }

  @Override
  public AggrStats get_aggr_stats_for(String dbName, String tblName, List<String> partNames,
      List<String> colNames) throws MetaException, NoSuchObjectException {
    List<ColumnStatisticsObj> colStats;
    dbName = StringUtils.normalizeIdentifier(dbName);
    tblName = StringUtils.normalizeIdentifier(tblName);
    if (!shouldCacheTable(dbName, tblName)) {
      rawStore.get_aggr_stats_for(dbName, tblName, partNames, colNames);
    }
    Table table = sharedCache.getTableFromCache(dbName, tblName);
    if (table == null) {
      // The table is not yet loaded in cache
      return rawStore.get_aggr_stats_for(dbName, tblName, partNames, colNames);
    }
    List<String> allPartNames = rawStore.listPartitionNames(dbName, tblName, (short) -1);
    if (partNames.size() == allPartNames.size()) {
      colStats = sharedCache.getAggrStatsFromCache(dbName, tblName, colNames, StatsType.ALL);
      if (colStats != null) {
        return new AggrStats(colStats, partNames.size());
      }
    } else if (partNames.size() == (allPartNames.size() - 1)) {
      String defaultPartitionName = MetastoreConf.getVar(getConf(), ConfVars.DEFAULTPARTITIONNAME);
      if (!partNames.contains(defaultPartitionName)) {
        colStats =
            sharedCache.getAggrStatsFromCache(dbName, tblName, colNames, StatsType.ALLBUTDEFAULT);
        if (colStats != null) {
          return new AggrStats(colStats, partNames.size());
        }
      }
    }
    LOG.debug("Didn't find aggr stats in cache. Merging them. tblName= {}, parts= {}, cols= {}",
        tblName, partNames, colNames);
    MergedColumnStatsForPartitions mergedColStats =
        mergeColStatsForPartitions(dbName, tblName, partNames, colNames, sharedCache);
    return new AggrStats(mergedColStats.getColStats(), mergedColStats.getPartsFound());
  }

  private MergedColumnStatsForPartitions mergeColStatsForPartitions(String dbName, String tblName,
      List<String> partNames, List<String> colNames, SharedCache sharedCache) throws MetaException {
    final boolean useDensityFunctionForNDVEstimation =
        MetastoreConf.getBoolVar(getConf(), ConfVars.STATS_NDV_DENSITY_FUNCTION);
    final double ndvTuner = MetastoreConf.getDoubleVar(getConf(), ConfVars.STATS_NDV_TUNER);
    Map<ColumnStatsAggregator, List<ColStatsObjWithSourceInfo>> colStatsMap =
        new HashMap<ColumnStatsAggregator, List<ColStatsObjWithSourceInfo>>();
    boolean areAllPartsFound = true;
    long partsFound = 0;
    for (String colName : colNames) {
      long partsFoundForColumn = 0;
      ColumnStatsAggregator colStatsAggregator = null;
      List<ColStatsObjWithSourceInfo> colStatsWithPartInfoList =
          new ArrayList<ColStatsObjWithSourceInfo>();
      for (String partName : partNames) {
        ColumnStatisticsObj colStatsForPart =
            sharedCache.getPartitionColStatsFromCache(dbName, tblName, partNameToVals(partName), colName);
        if (colStatsForPart != null) {
          ColStatsObjWithSourceInfo colStatsWithPartInfo =
              new ColStatsObjWithSourceInfo(colStatsForPart, dbName, tblName, partName);
          colStatsWithPartInfoList.add(colStatsWithPartInfo);
          if (colStatsAggregator == null) {
            colStatsAggregator = ColumnStatsAggregatorFactory.getColumnStatsAggregator(
                colStatsForPart.getStatsData().getSetField(), useDensityFunctionForNDVEstimation,
                ndvTuner);
          }
          partsFoundForColumn++;
        } else {
          LOG.debug(
              "Stats not found in CachedStore for: dbName={} tblName={} partName={} colName={}",
              dbName, tblName, partName, colName);
        }
      }
      if (colStatsWithPartInfoList.size() > 0) {
        colStatsMap.put(colStatsAggregator, colStatsWithPartInfoList);
      }
      if (partsFoundForColumn == partNames.size()) {
        partsFound++;
      }
      if (colStatsMap.size() < 1) {
        LOG.debug("No stats data found for: dbName={} tblName= {} partNames= {} colNames= ", dbName,
            tblName, partNames, colNames);
        return new MergedColumnStatsForPartitions(new ArrayList<ColumnStatisticsObj>(), 0);
      }
    }
    // Note that enableBitVector does not apply here because ColumnStatisticsObj
    // itself will tell whether bitvector is null or not and aggr logic can automatically apply.
    return new MergedColumnStatsForPartitions(MetaStoreUtils.aggrPartitionStats(colStatsMap,
        partNames, areAllPartsFound, useDensityFunctionForNDVEstimation, ndvTuner), partsFound);
  }

  class MergedColumnStatsForPartitions {
    List<ColumnStatisticsObj> colStats = new ArrayList<ColumnStatisticsObj>();
    long partsFound;

    MergedColumnStatsForPartitions(List<ColumnStatisticsObj> colStats, long partsFound) {
      this.colStats = colStats;
      this.partsFound = partsFound;
    }

    List<ColumnStatisticsObj> getColStats() {
      return colStats;
    }

    long getPartsFound() {
      return partsFound;
    }
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
    return rawStore.getTableCount();
  }

  @Override
  public int getPartitionCount() throws MetaException {
    return rawStore.getPartitionCount();
  }

  @Override
  public int getDatabaseCount() throws MetaException {
    return rawStore.getDatabaseCount();
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
  public List<SQLDefaultConstraint> getDefaultConstraints(String db_name, String tbl_name)
      throws MetaException {
    // TODO constraintCache
    return rawStore.getDefaultConstraints(db_name, tbl_name);
  }

  @Override
  public List<SQLCheckConstraint> getCheckConstraints(String db_name, String tbl_name)
      throws MetaException {
    // TODO constraintCache
    return rawStore.getCheckConstraints(db_name, tbl_name);
  }

  @Override
  public List<String> createTableWithConstraints(Table tbl, List<SQLPrimaryKey> primaryKeys,
      List<SQLForeignKey> foreignKeys, List<SQLUniqueConstraint> uniqueConstraints,
      List<SQLNotNullConstraint> notNullConstraints,
      List<SQLDefaultConstraint> defaultConstraints,
      List<SQLCheckConstraint> checkConstraints) throws InvalidObjectException, MetaException {
    // TODO constraintCache
    List<String> constraintNames = rawStore.createTableWithConstraints(tbl, primaryKeys,
        foreignKeys, uniqueConstraints, notNullConstraints, defaultConstraints, checkConstraints);
    String dbName = StringUtils.normalizeIdentifier(tbl.getDbName());
    String tblName = StringUtils.normalizeIdentifier(tbl.getTableName());
    if (!shouldCacheTable(dbName, tblName)) {
      return constraintNames;
    }
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

  public List<String> addDefaultConstraints(List<SQLDefaultConstraint> nns)
      throws InvalidObjectException, MetaException {
    // TODO constraintCache
    return rawStore.addDefaultConstraints(nns);
  }

  @Override
  public List<String> addCheckConstraints(List<SQLCheckConstraint> nns)
      throws InvalidObjectException, MetaException {
    // TODO constraintCache
    return rawStore.addCheckConstraints(nns);
  }

  // TODO - not clear if we should cache these or not.  For now, don't bother
  @Override
  public void createISchema(ISchema schema)
      throws AlreadyExistsException, NoSuchObjectException, MetaException {
    rawStore.createISchema(schema);
  }

  @Override
  public void alterISchema(ISchemaName schemaName, ISchema newSchema)
      throws NoSuchObjectException, MetaException {
    rawStore.alterISchema(schemaName, newSchema);
  }

  @Override
  public ISchema getISchema(ISchemaName schemaName) throws MetaException {
    return rawStore.getISchema(schemaName);
  }

  @Override
  public void dropISchema(ISchemaName schemaName) throws NoSuchObjectException, MetaException {
    rawStore.dropISchema(schemaName);
  }

  @Override
  public void addSchemaVersion(SchemaVersion schemaVersion) throws
      AlreadyExistsException, InvalidObjectException, NoSuchObjectException, MetaException {
    rawStore.addSchemaVersion(schemaVersion);
  }

  @Override
  public void alterSchemaVersion(SchemaVersionDescriptor version, SchemaVersion newVersion) throws
      NoSuchObjectException, MetaException {
    rawStore.alterSchemaVersion(version, newVersion);
  }

  @Override
  public SchemaVersion getSchemaVersion(SchemaVersionDescriptor version) throws MetaException {
    return rawStore.getSchemaVersion(version);
  }

  @Override
  public SchemaVersion getLatestSchemaVersion(ISchemaName schemaName) throws MetaException {
    return rawStore.getLatestSchemaVersion(schemaName);
  }

  @Override
  public List<SchemaVersion> getAllSchemaVersion(ISchemaName schemaName) throws MetaException {
    return rawStore.getAllSchemaVersion(schemaName);
  }

  @Override
  public List<SchemaVersion> getSchemaVersionsByColumns(String colName, String colNamespace,
                                                        String type) throws MetaException {
    return rawStore.getSchemaVersionsByColumns(colName, colNamespace, type);
  }

  @Override
  public void dropSchemaVersion(SchemaVersionDescriptor version) throws NoSuchObjectException,
      MetaException {
    rawStore.dropSchemaVersion(version);
  }

  @Override
  public SerDeInfo getSerDeInfo(String serDeName) throws NoSuchObjectException, MetaException {
    return rawStore.getSerDeInfo(serDeName);
  }

  @Override
  public void addSerde(SerDeInfo serde) throws AlreadyExistsException, MetaException {
    rawStore.addSerde(serde);
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

  @Override
  public void createResourcePlan(WMResourcePlan resourcePlan, String copyFrom, int defaultPoolSize)
      throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException {
    rawStore.createResourcePlan(resourcePlan, copyFrom, defaultPoolSize);
  }

  @Override
  public WMFullResourcePlan getResourcePlan(String name)
      throws NoSuchObjectException, MetaException {
    return rawStore.getResourcePlan(name);
  }

  @Override
  public List<WMResourcePlan> getAllResourcePlans() throws MetaException {
    return rawStore.getAllResourcePlans();
  }

  @Override
  public WMFullResourcePlan alterResourcePlan(String name, WMNullableResourcePlan resourcePlan,
    boolean canActivateDisabled, boolean canDeactivate, boolean isReplace)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
          MetaException {
    return rawStore.alterResourcePlan(
      name, resourcePlan, canActivateDisabled, canDeactivate, isReplace);
  }

  @Override
  public WMFullResourcePlan getActiveResourcePlan() throws MetaException {
    return rawStore.getActiveResourcePlan();
  }

  @Override
  public WMValidateResourcePlanResponse validateResourcePlan(String name)
      throws NoSuchObjectException, InvalidObjectException, MetaException {
    return rawStore.validateResourcePlan(name);
  }

  @Override
  public void dropResourcePlan(String name) throws NoSuchObjectException, MetaException {
    rawStore.dropResourcePlan(name);
  }

  @Override
  public void createWMTrigger(WMTrigger trigger)
      throws AlreadyExistsException, MetaException, NoSuchObjectException,
          InvalidOperationException {
    rawStore.createWMTrigger(trigger);
  }

  @Override
  public void alterWMTrigger(WMTrigger trigger)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    rawStore.alterWMTrigger(trigger);
  }

  @Override
  public void dropWMTrigger(String resourcePlanName, String triggerName)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    rawStore.dropWMTrigger(resourcePlanName, triggerName);
  }

  @Override
  public List<WMTrigger> getTriggersForResourcePlan(String resourcePlanName)
      throws NoSuchObjectException, MetaException {
    return rawStore.getTriggersForResourcePlan(resourcePlanName);
  }

  @Override
  public void createPool(WMPool pool) throws AlreadyExistsException, NoSuchObjectException,
      InvalidOperationException, MetaException {
    rawStore.createPool(pool);
  }

  @Override
  public void alterPool(WMNullablePool pool, String poolPath) throws AlreadyExistsException,
      NoSuchObjectException, InvalidOperationException, MetaException {
    rawStore.alterPool(pool, poolPath);
  }

  @Override
  public void dropWMPool(String resourcePlanName, String poolPath)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    rawStore.dropWMPool(resourcePlanName, poolPath);
  }

  @Override
  public void createOrUpdateWMMapping(WMMapping mapping, boolean update)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
      MetaException {
    rawStore.createOrUpdateWMMapping(mapping, update);
  }

  @Override
  public void dropWMMapping(WMMapping mapping)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    rawStore.dropWMMapping(mapping);
  }

  @Override
  public void createWMTriggerToPoolMapping(String resourcePlanName, String triggerName,
      String poolPath) throws AlreadyExistsException, NoSuchObjectException,
      InvalidOperationException, MetaException {
    rawStore.createWMTriggerToPoolMapping(resourcePlanName, triggerName, poolPath);
  }

  @Override
  public void dropWMTriggerToPoolMapping(String resourcePlanName, String triggerName,
      String poolPath) throws NoSuchObjectException, InvalidOperationException, MetaException {
    rawStore.dropWMTriggerToPoolMapping(resourcePlanName, triggerName, poolPath);
  }

  public long getCacheUpdateCount() {
    return sharedCache.getUpdateCount();
  }

  static boolean isNotInBlackList(String dbName, String tblName) {
    String str = dbName + "." + tblName;
    for (Pattern pattern : blacklistPatterns) {
      LOG.debug("Trying to match: {} against blacklist pattern: {}", str, pattern);
      Matcher matcher = pattern.matcher(str);
      if (matcher.matches()) {
        LOG.debug("Found matcher group: {} at start index: {} and end index: {}", matcher.group(),
            matcher.start(), matcher.end());
        return false;
      }
    }
    return true;
  }

  static boolean isInWhitelist(String dbName, String tblName) {
    String str = dbName + "." + tblName;
    for (Pattern pattern : whitelistPatterns) {
      LOG.debug("Trying to match: {} against whitelist pattern: {}", str, pattern);
      Matcher matcher = pattern.matcher(str);
      if (matcher.matches()) {
        LOG.debug("Found matcher group: {} at start index: {} and end index: {}", matcher.group(),
            matcher.start(), matcher.end());
        return true;
      }
    }
    return false;
  }

  // For testing
  static void setWhitelistPattern(List<Pattern> patterns) {
    whitelistPatterns = patterns;
  }

  // For testing
  static void setBlacklistPattern(List<Pattern> patterns) {
    blacklistPatterns = patterns;
  }

  // Determines if we should cache a table (& its partitions, stats etc),
  // based on whitelist/blacklist
  static boolean shouldCacheTable(String dbName, String tblName) {
    if (!isNotInBlackList(dbName, tblName)) {
      LOG.debug("{}.{} is in blacklist, skipping", dbName, tblName);
      return false;
    }
    if (!isInWhitelist(dbName, tblName)) {
      LOG.debug("{}.{} is not in whitelist, skipping", dbName, tblName);
      return false;
    }
    return true;
  }

  static List<Pattern> createPatterns(String configStr) {
    List<String> patternStrs = Arrays.asList(configStr.split(","));
    List<Pattern> patterns = new ArrayList<Pattern>();
    for (String str : patternStrs) {
      patterns.add(Pattern.compile(str));
    }
    return patterns;
  }

  static boolean isBlacklistWhitelistEmpty(Configuration conf) {
    return MetastoreConf.getAsString(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_CACHED_OBJECTS_WHITELIST)
        .equals(".*")
        && MetastoreConf.getAsString(conf, MetastoreConf.ConfVars.CACHED_RAW_STORE_CACHED_OBJECTS_BLACKLIST).isEmpty();
  }
}
