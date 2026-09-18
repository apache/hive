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
package org.apache.hadoop.hive.metastore.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EmptyStackException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.DatabaseName;
import org.apache.hadoop.hive.metastore.Deadline;
import org.apache.hadoop.hive.metastore.ObjectStore;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AllTableConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SQLAllTableConstraints;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.GetPartitionsArgs;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default {@link MetaCachePreWarm} implementation: walks the catalogs, databases and tables of
 * the backing database through RawStore and populates the given {@link SharedCache}. Warming the
 * tables can be spread over metastore.cached.rawstore.prewarm.threads worker threads, each with
 * its own RawStore instance and hence its own connection to the backing database.
 */
class DefaultMetaCachePreWarm implements MetaCachePreWarm {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultMetaCachePreWarm.class);
  // How long to wait for the prewarm workers to terminate before giving up on a clean shutdown
  private static final long WORKER_SHUTDOWN_TIMEOUT_MS = 10000;

  private final RawStore rawStore;
  private final SharedCache sharedCache;
  private final CachedStore.TablesPendingPrewarm tblsPendingPrewarm;
  private Configuration conf;
  private ExecutorService prewarmPool;
  private final List<RawStore> workerStores = new ArrayList<>();

  DefaultMetaCachePreWarm(RawStore rawStore, SharedCache sharedCache,
      CachedStore.TablesPendingPrewarm tblsPendingPrewarm) {
    this.rawStore = rawStore;
    this.sharedCache = sharedCache;
    this.tblsPendingPrewarm = tblsPendingPrewarm;
  }

  @Override public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override public Configuration getConf() {
    return conf;
  }

  @Override public void initialize() throws MetaException {
    int prewarmThreads = Math.max(1, MetastoreConf.getIntVar(conf, ConfVars.CACHED_RAW_STORE_PREWARM_THREADS));
    if (prewarmThreads > 1) {
      try {
        // RawStore implementations (ObjectStore) are not thread safe, so each worker gets its
        // own instance and hence its own connection to the backing database
        for (int i = 0; i < prewarmThreads; i++) {
          workerStores.add(createRawStore(conf));
        }
        LOG.info("Prewarming table cache with {} threads", prewarmThreads);
        prewarmPool = Executors.newFixedThreadPool(prewarmThreads, new ThreadFactory() {
          private final AtomicInteger threadCount = new AtomicInteger();
          @Override public Thread newThread(Runnable r) {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setName("CachedStore-PrewarmWorker-" + threadCount.getAndIncrement());
            t.setDaemon(true);
            return t;
          }
        });
      } catch (RuntimeException e) {
        LOG.warn("Failed to create RawStores for prewarm workers, falling back to single threaded prewarm", e);
        close();
      }
    }
  }

  @Override public boolean preWarm() throws MetaException {
    long sleepTime = 100;
    while (true) {
      // Prevents throwing exceptions in our raw store calls since we're not using RawStoreProxy
      Deadline.registerIfNot(1000000);
      Collection<String> catalogsToCache;
      try {
        catalogsToCache = preWarmCatalogs();
      } catch (MetaException | NoSuchObjectException e) {
        LOG.warn("Failed to populate catalogs in cache, going to try again", e);
        try {
          Thread.sleep(sleepTime);
          sleepTime = sleepTime * 2;
        } catch (InterruptedException timerEx) {
          Thread.currentThread().interrupt();
          LOG.warn("Interrupted while waiting to retry the catalog prewarm, stopping prewarm");
          return false;
        }
        // try again
        continue;
      }
      LOG.info("Finished prewarming catalogs, starting on databases");
      List<Database> databases = listDatabases(catalogsToCache);
      sharedCache.populateDatabasesInCache(databases);
      LOG.info("Databases cache is now prewarmed. Now adding tables, partitions and statistics to the cache");
      return preWarmTables(databases);
    }
  }

  /** Caches all catalogs and returns their names. */
  private Collection<String> preWarmCatalogs() throws MetaException, NoSuchObjectException {
    Collection<String> catalogsToCache = CachedStore.catalogsToCache(rawStore);
    LOG.info("Going to cache catalogs: {}", org.apache.commons.lang3.StringUtils.join(catalogsToCache, ", "));
    List<Catalog> catalogs = new ArrayList<>(catalogsToCache.size());
    for (String catName : catalogsToCache) {
      catalogs.add(rawStore.getCatalog(catName));
    }
    sharedCache.populateCatalogsInCache(catalogs);
    return catalogsToCache;
  }

  /** Lists the databases of the given catalogs, skipping the ones that cannot be read. */
  private List<Database> listDatabases(Collection<String> catalogsToCache) {
    List<Database> databases = new ArrayList<>();
    for (String catName : catalogsToCache) {
      try {
        List<String> dbNames = rawStore.getAllDatabases(catName);
        LOG.info("Number of databases to prewarm in catalog {}: {}", catName, dbNames.size());
        for (String dbName : dbNames) {
          try {
            databases.add(rawStore.getDatabase(catName, dbName));
          } catch (NoSuchObjectException e) {
            // Continue with next database
            LOG.warn("Failed to cache database {}, moving on", DatabaseName.getQualified(catName, dbName), e);
          }
        }
      } catch (MetaException e) {
        LOG.warn("Failed to cache databases in catalog {}, moving on", catName, e);
      }
    }
    return databases;
  }

  /**
   * Warms all cacheable tables of the given databases, using the worker pool when one was
   * created. Returns true if all metadata was cached, false if warming stopped early because the
   * cache memory limit was reached or the thread was interrupted. The caller must {@link #close()}
   * this prewarmer, which awaits worker termination, before publishing completion.
   */
  private boolean preWarmTables(List<Database> databases) {
    int numberOfDatabasesCachedSoFar = 0;
    for (Database db : databases) {
      String catName = StringUtils.normalizeIdentifier(db.getCatalogName());
      String dbName = StringUtils.normalizeIdentifier(db.getName());
      List<String> tblNames;
      try {
        tblNames = rawStore.getAllTables(catName, dbName);
      } catch (MetaException e) {
        LOG.warn("Failed to cache tables for database {}, moving on", DatabaseName.getQualified(catName, dbName));
        // Continue with next database
        continue;
      }
      tblsPendingPrewarm.addTableNamesForPrewarming(tblNames);
      int totalTablesToCache = tblNames.size();
      AtomicBoolean stopPrewarm = new AtomicBoolean(false);
      AtomicInteger tablesCachedSoFar = new AtomicInteger();
      if (prewarmPool != null) {
        List<Future<?>> workers = new ArrayList<>(workerStores.size());
        for (RawStore workerStore : workerStores) {
          workers.add(prewarmPool.submit(
              () -> drainTablesPendingPrewarm(workerStore, catName, dbName, stopPrewarm, tablesCachedSoFar,
                  totalTablesToCache)));
        }
        if (!awaitWorkers(workers, dbName, stopPrewarm)) {
          return false;
        }
      } else {
        drainTablesPendingPrewarm(rawStore, catName, dbName, stopPrewarm, tablesCachedSoFar, totalTablesToCache);
      }
      if (stopPrewarm.get()) {
        // The cache is full: stop here and serve with whatever has been cached so far
        return false;
      }
      LOG.debug("Processed database: {}. Cached {} / {} databases so far.", dbName, ++numberOfDatabasesCachedSoFar,
          databases.size());
    }
    return true;
  }

  /**
   * Waits for all the given workers to finish. On interruption, tells the remaining workers to
   * stop (close() then awaits their termination) and returns false; a failed worker is only
   * logged, the tables it could not cache are served from the raw store until the cache update
   * service refreshes them.
   */
  private boolean awaitWorkers(List<Future<?>> workers, String dbName, AtomicBoolean stopPrewarm) {
    for (Future<?> worker : workers) {
      try {
        worker.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Interrupted while waiting for prewarm workers on database {}; "
            + "completing prewarm with the metadata cached so far", dbName);
        stopPrewarm.set(true);
        return false;
      } catch (ExecutionException e) {
        LOG.warn("Prewarm worker failed for database {}, moving on", dbName, e);
      }
    }
    return true;
  }

  /**
   * Drains tables from tblsPendingPrewarm for the given database, caching each one, until the
   * pending list is empty or the shared cache reports its memory limit is reached. Safe to run
   * from multiple threads concurrently: the pending-table stack hands out each table exactly once,
   * and hot tables promoted by prioritizeTableForPrewarm are picked up by whichever worker pops next.
   */
  private void drainTablesPendingPrewarm(RawStore rawStore, String catName, String dbName,
      AtomicBoolean stopPrewarm, AtomicInteger tablesCachedSoFar, int totalTablesToCache) {
    // Deadline is thread local; register it for prewarm worker threads
    Deadline.registerIfNot(1000000);
    while (!stopPrewarm.get() && !Thread.currentThread().isInterrupted()
        && tblsPendingPrewarm.hasMoreTablesToPrewarm()) {
      String tblName;
      try {
        tblName = StringUtils.normalizeIdentifier(tblsPendingPrewarm.getNextTableNameToPrewarm());
      } catch (EmptyStackException e) {
        // Another worker drained the remaining tables between our check and pop
        break;
      }
      if (CachedStore.shouldCacheTable(catName, dbName, tblName)) {
        if (!preWarmTable(rawStore, catName, dbName, tblName)) {
          LOG.info("Unable to cache Database: {}'s Table: {}, since the cache memory is full. "
              + "Will stop attempting to cache any more tables.", dbName, tblName);
          stopPrewarm.set(true);
          return;
        }
        LOG.debug("Processed database: {}'s table: {}. Cached {} / {}  tables so far.", dbName, tblName,
            tablesCachedSoFar.incrementAndGet(), totalTablesToCache);
      }
    }
  }

  /**
   * Fetches one table with its partitions, statistics and constraints from the backing database
   * and populates it in the shared cache. Returns false only when the shared cache reports that
   * its memory limit is reached; a table that vanished or failed to load is skipped by returning
   * true so that prewarm continues with the next table.
   */
  private boolean preWarmTable(RawStore rawStore, String catName, String dbName, String tblName) {
    Table table;
    try {
      table = rawStore.getTable(catName, dbName, tblName);
    } catch (MetaException e) {
      LOG.debug(ExceptionUtils.getStackTrace(e));
      // It is possible the table is deleted during fetching tables of the database,
      // in that case, continue with the next table
      return true;
    }
    List<String> colNames = MetaStoreUtils.getColumnNamesForTable(table);
    try {
      ColumnStatistics tableColStats = null;
      List<Partition> partitions = null;
      List<ColumnStatistics> partitionColStats = null;
      AggrStats aggrStatsAllPartitions = null;
      AggrStats aggrStatsAllButDefaultPartition = null;
      TableCacheObjects cacheObjects = new TableCacheObjects();
      if (!table.getPartitionKeys().isEmpty()) {
        Deadline.startTimer("getPartitions");
        partitions = rawStore.getPartitions(catName, dbName, tblName, GetPartitionsArgs.getAllPartitions());
        Deadline.stopTimer();
        cacheObjects.setPartitions(partitions);
        List<String> partNames = new ArrayList<>(partitions.size());
        for (Partition p : partitions) {
          partNames.add(Warehouse.makePartName(table.getPartitionKeys(), p.getValues()));
        }
        if (!partNames.isEmpty()) {
          // Get partition column stats for this table
          Deadline.startTimer("getPartitionColumnStatistics");
          partitionColStats = rawStore.getPartitionColumnStatistics(catName, dbName, tblName, partNames, colNames,
              CacheUtils.HIVE_ENGINE);
          Deadline.stopTimer();
          cacheObjects.setPartitionColStats(partitionColStats);
          // Get aggregate stats for all partitions of a table and for all but default
          // partition
          Deadline.startTimer("getAggrPartitionColumnStatistics");
          aggrStatsAllPartitions = rawStore.get_aggr_stats_for(catName, dbName, tblName, partNames, colNames,
              CacheUtils.HIVE_ENGINE);
          Deadline.stopTimer();
          cacheObjects.setAggrStatsAllPartitions(aggrStatsAllPartitions);
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
              rawStore.get_aggr_stats_for(catName, dbName, tblName, partNames, colNames, CacheUtils.HIVE_ENGINE);
          Deadline.stopTimer();
          cacheObjects.setAggrStatsAllButDefaultPartition(aggrStatsAllButDefaultPartition);
        }
      } else {
        Deadline.startTimer("getTableColumnStatistics");
        tableColStats = rawStore.getTableColumnStatistics(catName, dbName, tblName, colNames, CacheUtils.HIVE_ENGINE);
        Deadline.stopTimer();
        cacheObjects.setTableColStats(tableColStats);
      }

      Deadline.startTimer("getAllTableConstraints");
      SQLAllTableConstraints tableConstraints = rawStore.getAllTableConstraints(
          new AllTableConstraintsRequest(catName, dbName, tblName));
      Deadline.stopTimer();
      cacheObjects.setTableConstraints(tableConstraints);

      // If the table could not be cached due to memory limit, stop prewarm
      boolean isSuccess = sharedCache
          .populateTableInCache(table, cacheObjects);
      if (isSuccess) {
        LOG.trace("Cached Database: {}'s Table: {}.", dbName, tblName);
      } else {
        return false;
      }
    } catch (MetaException | NoSuchObjectException e) {
      LOG.debug(ExceptionUtils.getStackTrace(e));
      // Continue with next table
    }
    return true;
  }

  /**
   * Creates a fresh RawStore instance for a prewarm worker thread, mirroring the way
   * CacheUpdateMasterWork creates its own store.
   */
  private static RawStore createRawStore(Configuration conf) {
    String rawStoreClassName = MetastoreConf.getVar(conf, ConfVars.CACHED_RAW_STORE_IMPL, ObjectStore.class.getName());
    try {
      RawStore rs = JavaUtils.getClass(rawStoreClassName, RawStore.class).newInstance();
      rs.setConf(conf);
      return rs;
    } catch (InstantiationException | IllegalAccessException | MetaException e) {
      throw new RuntimeException("Cannot instantiate " + rawStoreClassName, e);
    }
  }

  /**
   * Stops the prewarm workers and waits for them to terminate before their RawStores are closed,
   * so that no worker can still be reading from a closed store or writing to the shared cache
   * once prewarm reports completion. Idempotent.
   */
  @Override public void close() {
    if (prewarmPool != null) {
      prewarmPool.shutdownNow();
      try {
        if (!prewarmPool.awaitTermination(WORKER_SHUTDOWN_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
          LOG.warn("Prewarm workers did not terminate within {} ms", WORKER_SHUTDOWN_TIMEOUT_MS);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.warn("Interrupted while waiting for the prewarm workers to terminate");
      }
      prewarmPool = null;
    }
    for (RawStore workerStore : workerStores) {
      try {
        workerStore.shutdown();
      } catch (RuntimeException e) {
        LOG.warn("Failed to shut down a prewarm worker RawStore", e);
      }
    }
    workerStores.clear();
  }
}
