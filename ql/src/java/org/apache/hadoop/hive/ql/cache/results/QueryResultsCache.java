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

package org.apache.hadoop.hive.ql.cache.results;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.annotation.concurrent.GuardedBy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.common.MetricsVariable;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.messaging.MessageBuilder;
import org.apache.hadoop.hive.ql.hooks.Entity.Type;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.events.EventConsumer;
import org.apache.hadoop.hive.ql.parse.ColumnAccessInfo;
import org.apache.hadoop.hive.ql.parse.TableAccessInfo;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.HiveOperation;
import org.apache.hive.common.util.TxnIdUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * A class to handle management and lookup of cached Hive query results.
 */
public final class QueryResultsCache {

  private static final Logger LOG = LoggerFactory.getLogger(QueryResultsCache.class);

  public static class LookupInfo {
    private final String queryText;
    private final Supplier<ValidTxnWriteIdList> txnWriteIdListProvider;

    public LookupInfo(String queryText, Supplier<ValidTxnWriteIdList> txnWriteIdListProvider) {
      this.queryText = Objects.requireNonNull(queryText);
      this.txnWriteIdListProvider = txnWriteIdListProvider;
    }

    public String getQueryText() {
      return queryText;
    }
  }

  public static class QueryInfo {
    private long queryTime;
    private LookupInfo lookupInfo;
    private HiveOperation hiveOperation;
    private List<FieldSchema> resultSchema;
    private TableAccessInfo tableAccessInfo;
    private ColumnAccessInfo columnAccessInfo;
    private Set<ReadEntity> inputs;

    public QueryInfo(
        long queryTime,
        LookupInfo lookupInfo,
        HiveOperation hiveOperation,
        List<FieldSchema> resultSchema,
        TableAccessInfo tableAccessInfo,
        ColumnAccessInfo columnAccessInfo,
        Set<ReadEntity> inputs) {
      this.queryTime = queryTime;
      this.lookupInfo = lookupInfo;
      this.hiveOperation = hiveOperation;
      this.resultSchema = resultSchema;
      this.tableAccessInfo = tableAccessInfo;
      this.columnAccessInfo = columnAccessInfo;
      this.inputs = inputs;
    }

    public LookupInfo getLookupInfo() {
      return lookupInfo;
    }

    public void setLookupInfo(LookupInfo lookupInfo) {
      this.lookupInfo = lookupInfo;
    }

    public HiveOperation getHiveOperation() {
      return hiveOperation;
    }

    public void setHiveOperation(HiveOperation hiveOperation) {
      this.hiveOperation = hiveOperation;
    }

    public List<FieldSchema> getResultSchema() {
      return resultSchema;
    }

    public void setResultSchema(List<FieldSchema> resultSchema) {
      this.resultSchema = resultSchema;
    }

    public TableAccessInfo getTableAccessInfo() {
      return tableAccessInfo;
    }

    public void setTableAccessInfo(TableAccessInfo tableAccessInfo) {
      this.tableAccessInfo = tableAccessInfo;
    }

    public ColumnAccessInfo getColumnAccessInfo() {
      return columnAccessInfo;
    }

    public void setColumnAccessInfo(ColumnAccessInfo columnAccessInfo) {
      this.columnAccessInfo = columnAccessInfo;
    }

    public Set<ReadEntity> getInputs() {
      return inputs;
    }

    public void setInputs(Set<ReadEntity> inputs) {
      this.inputs = inputs;
    }

    public long getQueryTime() {
      return queryTime;
    }

    public void setQueryTime(long queryTime) {
      this.queryTime = queryTime;
    }
  }

  public enum CacheEntryStatus {
    VALID, INVALID, PENDING
  }

  public static class CacheEntry {
    private QueryInfo queryInfo;
    private FetchWork fetchWork;
    private Path cachedResultsPath;
    private Set<FileStatus> cachedResultPaths;

    // Cache administration
    private long size;
    private AtomicInteger readers = new AtomicInteger(0);
    private ScheduledFuture<?> invalidationFuture = null;
    private volatile CacheEntryStatus status = CacheEntryStatus.PENDING;
    private ValidTxnWriteIdList txnWriteIdList;

    public void releaseReader() {
      int readerCount = 0;
      synchronized (this) {
        readerCount = readers.decrementAndGet();
      }
      LOG.debug("releaseReader: entry: {}, readerCount: {}", this, readerCount);

      cleanupIfNeeded();
    }

    public String toString() {
      return String.format("CacheEntry#%s query: [ %s ], status: %s, location: %s, size: %d",
          System.identityHashCode(this), getQueryInfo().getLookupInfo().getQueryText(), status,
          cachedResultsPath, size);
    }

    public boolean addReader() {
      boolean added = false;
      int readerCount = 0;
      synchronized (this) {
        if (status == CacheEntryStatus.VALID) {
          readerCount = readers.incrementAndGet();
          added = true;
        }
      }
      LOG.debug("addReader: entry: {}, readerCount: {}, added: {}", this, readerCount, added);
      return added;
    }

    private void invalidate() {
      LOG.info("Invalidating cache entry: {}", this);
      CacheEntryStatus prevStatus = setStatus(CacheEntryStatus.INVALID);
      if (prevStatus == CacheEntryStatus.VALID) {
        if (invalidationFuture != null) {
          // The cache entry has just been invalidated, no need for the scheduled invalidation.
          invalidationFuture.cancel(false);
        }
        cleanupIfNeeded();
        decrementMetric(MetricsConstant.QC_VALID_ENTRIES);
      } else if (prevStatus == CacheEntryStatus.PENDING) {
        decrementMetric(MetricsConstant.QC_PENDING_FAILS);
      }
    }

    public CacheEntryStatus getStatus() {
      return status;
    }

    private CacheEntryStatus setStatus(CacheEntryStatus newStatus) {
      synchronized (this) {
        CacheEntryStatus oldStatus = status;
        status = newStatus;
        this.notifyAll();
        return oldStatus;
      }
    }

    private void cleanupIfNeeded() {
      if (status == CacheEntryStatus.INVALID && readers.get() <= 0) {
        QueryResultsCache.cleanupEntry(this);
      }
    }

    private String getQueryText() {
      return getQueryInfo().getLookupInfo().getQueryText();
    }

    public FetchWork getFetchWork() {
      // FetchWork's sink is used to hold results, so each query needs a separate copy of FetchWork
      FetchWork fetch = new FetchWork(fetchWork.getTblDir(), fetchWork.getTblDesc(), fetchWork.getLimit());
      fetch.setCachedResult(true);
      fetch.setFilesToFetch(this.cachedResultPaths);
      return fetch;
    }

    public QueryInfo getQueryInfo() {
      return queryInfo;
    }

    public Path getCachedResultsPath() {
      return cachedResultsPath;
    }

    /**
     * Wait for the cache entry to go from PENDING to VALID status.
     *
     * @return true if the cache entry successfully changed to VALID status,
     *         false if the status changes from PENDING to INVALID
     */
    public boolean waitForValidStatus() {
      LOG.info("Waiting on pending cacheEntry: {}", this);

      final long startTime = System.nanoTime();
      long endTime;

      while (true) {
        try {
          switch (status) {
          case VALID:
            endTime = System.nanoTime();
            incrementMetric(MetricsConstant.QC_PENDING_SUCCESS_WAIT_TIME,
                TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS));
            return true;
          case INVALID:
            endTime = System.nanoTime();
            incrementMetric(MetricsConstant.QC_PENDING_FAILS_WAIT_TIME,
                TimeUnit.MILLISECONDS.convert(endTime - startTime, TimeUnit.NANOSECONDS));
            return false;
          case PENDING:
            // Status has not changed, continue waiting.
            break;
          }
          synchronized (this) {
            this.wait();
          }
        } catch (InterruptedException err) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
    }

    public Stream<String> getTableNames() {
      return queryInfo.getInputs().stream()
          .filter(readEntity -> readEntity.getType() == Type.TABLE)
          .map(readEntity -> readEntity.getTable().getFullyQualifiedName());
    }
  }

  // Allow lookup by query string
  @GuardedBy("cacheLock")
  private final Multimap<String, CacheEntry> queryMap = ArrayListMultimap.create();

  // LRU. Could also implement LRU as a doubly linked list if CacheEntry keeps its node.
  // Use synchronized map since even read actions cause the lru to get updated.
  private final Map<CacheEntry, CacheEntry> lru =
      Collections.synchronizedMap(new LinkedHashMap<CacheEntry, CacheEntry>(16, 0.75f, true));

  // Lookup of cache entries by table used in the query, for cache invalidation
  @GuardedBy("cacheLock")
  private final Multimap<String, CacheEntry> tableToEntryMap = ArrayListMultimap.create();

  private final HiveConf conf;
  private Path cacheDirPath;
  private Path zeroRowsPath;
  private long cacheSize = 0;
  private long maxCacheSize;
  private long maxEntrySize;
  private long maxEntryLifetime;
  private final ReadWriteLock cacheLock = new ReentrantReadWriteLock();
  private final Lock cacheReadLock = cacheLock.readLock();
  private final Lock cacheWriteLock = cacheLock.writeLock();
  private ScheduledFuture<?> invalidationPollFuture;

  private QueryResultsCache(HiveConf configuration) throws IOException {
    this.conf = configuration;

    // Set up cache directory
    Path rootCacheDir = new Path(conf.getVar(HiveConf.ConfVars.HIVE_QUERY_RESULTS_CACHE_DIRECTORY));
    LOG.info("Initializing query results cache at {}", rootCacheDir);

    String currentCacheDirName = "results-" + UUID.randomUUID().toString();
    cacheDirPath = new Path(rootCacheDir, currentCacheDirName);
    FileSystem fs = cacheDirPath.getFileSystem(conf);
    FsPermission fsPermission = new FsPermission("700");
    fs.mkdirs(cacheDirPath, fsPermission);

    // Create non-existent path for 0-row results
    zeroRowsPath = new Path(cacheDirPath, "dummy_zero_rows");

    // Results cache directory should be cleaned up at process termination.
    fs.deleteOnExit(cacheDirPath);

    maxCacheSize = conf.getLongVar(HiveConf.ConfVars.HIVE_QUERY_RESULTS_CACHE_MAX_SIZE);
    maxEntrySize = conf.getLongVar(HiveConf.ConfVars.HIVE_QUERY_RESULTS_CACHE_MAX_ENTRY_SIZE);
    maxEntryLifetime = conf.getTimeVar(
        HiveConf.ConfVars.HIVE_QUERY_RESULTS_CACHE_MAX_ENTRY_LIFETIME,
        TimeUnit.MILLISECONDS);

    LOG.info("Query results cache: cacheDirectory {}, maxCacheSize {}, maxEntrySize {}, maxEntryLifetime {}",
        cacheDirPath, maxCacheSize, maxEntrySize, maxEntryLifetime);
  }

  private static final AtomicBoolean inited = new AtomicBoolean(false);
  private static QueryResultsCache instance;

  public static void initialize(HiveConf conf) throws IOException {
    if (!inited.getAndSet(true)) {
      try {
        instance = new QueryResultsCache(conf);

        Metrics metrics = MetricsFactory.getInstance();
        if (metrics != null) {
          registerMetrics(metrics, instance);
        }
      } catch (Exception err) {
        inited.set(false);
        throw err;
      }
    }
  }

  public static QueryResultsCache getInstance() {
    return instance;
  }

  public Path getCacheDirPath() {
    return cacheDirPath;
  }

  /**
   * Check if the cache contains an entry for the requested LookupInfo.
   *
   * @param request
   * @return The cached result if there is a match in the cache, or null if no
   *         match is found.
   * @throws NullPointerException if request is {@code null}
   */
  public CacheEntry lookup(final LookupInfo request) {
    Objects.requireNonNull(request);

    LOG.debug("QueryResultsCache lookup for query: {}", request.queryText);

    CacheEntry result = null;
    boolean foundPending = false;
    // Cannot modify entries while we currently hold read lock, so keep track of
    // them to delete later.
    Set<CacheEntry> entriesToRemove = new HashSet<>();
    cacheReadLock.lock();
    try {
      // Note: ReentrantReadWriteLock does not allow upgrading a read lock to a write lock.
      // Care must be taken while under read lock, to make sure we do not perform any actions
      // which attempt to take a write lock.
      Collection<CacheEntry> candidates = queryMap.get(request.queryText);
      // Try to find valid entry, but settle for pending entry if that is all
      // there is available
      for (CacheEntry candidate : candidates) {
        if (entryMatches(request, candidate, entriesToRemove)) {
          CacheEntryStatus entryStatus = candidate.status;
          if (entryStatus == CacheEntryStatus.VALID) {
            result = candidate;
            break;
          }
          if (entryStatus == CacheEntryStatus.PENDING) {
            // Only accept first pending result
            result = (result == null) ? candidate : result;
          }
        }
        if (result != null) {
          foundPending = (result.status == CacheEntryStatus.PENDING);
          lru.get(result); // Update LRU
        }
      }
    } finally {
      cacheReadLock.unlock();
    }

    // Now that we have exited read lock it is safe to remove any invalid entries.
    for (CacheEntry invalidEntry : entriesToRemove) {
      removeEntry(invalidEntry);
    }

    LOG.debug("QueryResultsCache lookup result: {}", result);
    incrementMetric(MetricsConstant.QC_LOOKUPS);
    if (result != null) {
      if (foundPending) {
        incrementMetric(MetricsConstant.QC_PENDING_HITS);
      } else {
        incrementMetric(MetricsConstant.QC_VALID_HITS);
      }
    }

    return result;
  }

  /**
   * Add an entry to the cache.
   * The new entry will be in PENDING state and not usable setEntryValid() is called on the entry.
   * @param queryInfo
   * @return
   */
  public CacheEntry addToCache(QueryInfo queryInfo, ValidTxnWriteIdList txnWriteIdList) {
    // Create placeholder entry with PENDING state.
    String queryText = queryInfo.getLookupInfo().getQueryText();
    CacheEntry addedEntry = new CacheEntry();
    addedEntry.queryInfo = queryInfo;
    addedEntry.txnWriteIdList = txnWriteIdList;

    cacheWriteLock.lock();
    try {
      LOG.info("Adding placeholder cache entry for query '{}'", queryText);

      // Add the entry to the cache structures while under write lock.
      queryMap.put(queryText, addedEntry);
      lru.put(addedEntry, addedEntry);
      // Index of entries by table usage.
      addedEntry.getTableNames().forEach(tableName -> tableToEntryMap.put(tableName, addedEntry));
    } finally {
      cacheWriteLock.unlock();
    }

    return addedEntry;
  }

  /**
   * Updates a pending cache entry with a FetchWork result from a finished query.
   * If successful the cache entry will be set to valid status and be usable for cached queries.
   * Important: Adding the entry to the cache will increment the reader count for the cache entry.
   * CacheEntry.releaseReader() should be called when the caller is done with the cache entry.
   * @param cacheEntry
   * @param fetchWork
   * @return
   */
  public boolean setEntryValid(CacheEntry cacheEntry, FetchWork fetchWork) {
    Path queryResultsPath = null;

    try {
      // if we are here file sink op should have created files to fetch from
      assert(fetchWork.getFilesToFetch() != null );

      boolean requiresCaching = true;
      queryResultsPath = fetchWork.getTblDir();
      FileSystem resultsFs = queryResultsPath.getFileSystem(conf);

      long resultSize = 0;
      for (FileStatus fs : fetchWork.getFilesToFetch()) {
        if (resultsFs.exists(fs.getPath())) {
          resultSize += fs.getLen();
        } else {
          // No actual result directory, no need to cache anything.
          requiresCaching = false;
          break;
        }
      }

      if (!shouldEntryBeAdded(cacheEntry, resultSize)) {
        return false;
      }

      // Synchronize on the cache entry so that no one else can invalidate this entry
      // while we are in the process of setting it to valid.
      synchronized (cacheEntry) {
        if (cacheEntry.getStatus() == CacheEntryStatus.INVALID) {
          // Entry either expired, or was invalidated due to table updates
          return false;
        }

        if (requiresCaching) {
          cacheEntry.cachedResultPaths = new HashSet<>();
            for(FileStatus fs:fetchWork.getFilesToFetch()) {
              cacheEntry.cachedResultPaths.add(fs);
            }
          LOG.info("Cached query result paths located at {} (size {}) for query '{}'",
              queryResultsPath, resultSize, cacheEntry.getQueryText());
        }

        // Create a new FetchWork to reference the new cache location.
        FetchWork fetchWorkForCache =
            new FetchWork(fetchWork.getTblDir(), fetchWork.getTblDesc(), fetchWork.getLimit());
        fetchWorkForCache.setCachedResult(true);
        fetchWorkForCache.setFilesToFetch(fetchWork.getFilesToFetch());
        cacheEntry.fetchWork = fetchWorkForCache;
        cacheEntry.size = resultSize;
        this.cacheSize += resultSize;

        cacheEntry.setStatus(CacheEntryStatus.VALID);
        // Mark this entry as being in use. Caller will need to release later.
        cacheEntry.addReader();

        scheduleEntryInvalidation(cacheEntry);
      }

      incrementMetric(MetricsConstant.QC_VALID_ENTRIES);
      incrementMetric(MetricsConstant.QC_TOTAL_ENTRIES_ADDED);
    } catch (Exception err) {
      LOG.error("Failed to create cache entry for query results for query: " + cacheEntry.getQueryText(), err);
      cacheEntry.size = 0;
      cacheEntry.cachedResultsPath = null;

      // Invalidate the entry. Rely on query cleanup to remove from lookup.
      cacheEntry.invalidate();
      return false;
    }

    return true;
  }

  public void clear() {
    cacheWriteLock.lock();
    try {
      LOG.info("Clearing the results cache");
      CacheEntry[] allEntries = null;
      synchronized (lru) {
        allEntries = lru.keySet().toArray(new CacheEntry[0]);
      }
      for (CacheEntry entry : allEntries) {
        try {
          removeEntry(entry);
        } catch (Exception err) {
          LOG.error("Error removing cache entry " + entry, err);
        }
      }
    } finally {
      cacheWriteLock.unlock();
    }
  }

  public long getSize() {
    cacheReadLock.lock();
    try {
      return cacheSize;
    } finally {
      cacheReadLock.unlock();
    }
  }

  public void notifyTableChanged(String dbName, String tableName, long updateTime) {
    LOG.debug("Table changed: {}.{}, at {}", dbName, tableName, updateTime);

    final String key = (dbName.toLowerCase() + "." + tableName.toLowerCase());

    cacheWriteLock.lock();
    try {
      Collection<CacheEntry> entriesForTable = tableToEntryMap.get(key);

      // Possible concurrent modification issues if we try to remove cache
      // entries while traversing the cache structures. Save the entries to
      // remove in a separate list.
      final List<CacheEntry> entriesToInvalidate =
          entriesForTable.isEmpty() ? Collections.emptyList() : new ArrayList<>(entriesForTable);

      for (CacheEntry entry : entriesToInvalidate) {
        // Ignore updates that occured before this cached query was created.
        if (entry.getQueryInfo().getQueryTime() <= updateTime) {
          removeEntry(entry);
        }
      }

    } finally {
      cacheWriteLock.unlock();
    }
  }

  /**
   * Check that the cache entry matches the lookupInfo.
   * @param lookupInfo
   * @param entry
   * @param entriesToRemove Set of entries to be removed after exiting read lock section.
   *                        If the entry is found to be invalid it will be added to this set.
   * @return
   */
  private boolean entryMatches(LookupInfo lookupInfo, CacheEntry entry, Set<CacheEntry> entriesToRemove) {
    QueryInfo queryInfo = entry.getQueryInfo();
    for (ReadEntity readEntity : queryInfo.getInputs()) {
      // Check that the tables used do not resolve to temp tables.
      if (readEntity.getType() == Type.TABLE) {
        Table tableUsed = readEntity.getTable();
        Map<String, Table> tempTables =
            SessionHiveMetaStoreClient.getTempTablesForDatabase(tableUsed.getDbName(), tableUsed.getTableName());
        if (tempTables != null && tempTables.containsKey(tableUsed.getTableName())) {
          LOG.info("{} resolves to a temporary table in the current session. This query cannot use the cache.",
              tableUsed.getTableName());
          return false;
        }

        // Has the table changed since the query was cached?
        // For transactional tables, can compare the table writeIDs of the current/cached query.
        if (AcidUtils.isTransactionalTable(tableUsed)) {
          boolean writeIdCheckPassed = false;
          String tableName = tableUsed.getFullyQualifiedName();
          ValidTxnWriteIdList currentTxnWriteIdList = lookupInfo.txnWriteIdListProvider.get();
          if (currentTxnWriteIdList == null) {
            LOG.warn("Current query's txnWriteIdList is null!");
            return false;
          }
          if (entry.txnWriteIdList == null) {
            LOG.warn("Cache entry's txnWriteIdList is null!");
            return false;
          }
          ValidWriteIdList currentWriteIdForTable =
              currentTxnWriteIdList.getTableValidWriteIdList(tableName);
          ValidWriteIdList cachedWriteIdForTable = entry.txnWriteIdList.getTableValidWriteIdList(tableName);

          LOG.debug("Checking writeIds for table {}: currentWriteIdForTable {}, cachedWriteIdForTable {}",
              tableName, currentWriteIdForTable, cachedWriteIdForTable);
          if (currentWriteIdForTable != null && cachedWriteIdForTable != null) {
            if (TxnIdUtils.checkEquivalentWriteIds(currentWriteIdForTable, cachedWriteIdForTable)) {
              writeIdCheckPassed = true;
            }
          }

          if (!writeIdCheckPassed) {
            LOG.debug("Cached query no longer valid due to table {}", tableUsed.getFullyQualifiedName());
            // We can invalidate the entry now, but calling removeEntry() requires a write lock
            // and we may already have read lock taken now. Add to entriesToRemove to delete later.
            entriesToRemove.add(entry);
            entry.invalidate();
            return false;
          }
        }
      }
    }

    return true;
  }

  public void removeEntry(CacheEntry entry) {
    entry.invalidate();
    cacheWriteLock.lock();
    try {
      String queryString = entry.getQueryText();
      if (!queryMap.remove(queryString, entry)) {
        LOG.warn("Attempted to remove entry but it was not in the cache: {}", entry);
      }

      // Remove this entry from the table usage mappings.
      entry.getTableNames().forEach(tableName -> tableToEntryMap.remove(tableName, entry));

      lru.remove(entry);
      // Should the cache size be updated here, or after the result data has actually been deleted?
      cacheSize -= entry.size;
    } finally {
      cacheWriteLock.unlock();
    }
  }

  /**
   * Determines if the cache entry should be added to the results cache.
   */
  private boolean shouldEntryBeAdded(CacheEntry entry, long size) {
    // Assumes the cache lock has already been taken.
    if (maxEntrySize >= 0 && size > maxEntrySize) {
      LOG.debug("Cache entry size {} larger than max entry size ({})", size, maxEntrySize);
      incrementMetric(MetricsConstant.QC_REJECTED_TOO_LARGE);
      return false;
    }

    if (!clearSpaceForCacheEntry(entry, size)) {
      return false;
    }

    return true;
  }

  private boolean hasSpaceForCacheEntry(CacheEntry entry, long size) {
    if (maxCacheSize >= 0) {
      return (cacheSize + size) <= maxCacheSize;
    }
    // Negative max cache size means unbounded.
    return true;
  }

  private CacheEntry findEntryToRemove() {
    // Entries should be in LRU order in the keyset iterator.
    Set<CacheEntry> entries = lru.keySet();
    synchronized (lru) {
      for (CacheEntry removalCandidate : entries) {
        if (removalCandidate.getStatus() == CacheEntryStatus.VALID) {
          return removalCandidate;
        }
      }
    }
    return null;
  }

  private boolean clearSpaceForCacheEntry(CacheEntry entry, long size) {
    if (hasSpaceForCacheEntry(entry, size)) {
      return true;
    }

    LOG.info("Clearing space for cache entry for query: [{}] with size {}",
        entry.getQueryText(), size);

    CacheEntry removalCandidate;
    while ((removalCandidate = findEntryToRemove()) != null) {
      LOG.info("Removing entry: {}", removalCandidate);
      removeEntry(removalCandidate);
      // TODO: Should we wait for the entry to actually be deleted from HDFS? Would have to
      // poll the reader count, waiting for it to reach 0, at which point cleanup should occur.
      if (hasSpaceForCacheEntry(entry, size)) {
        return true;
      }
    }

    LOG.info("Could not free enough space for cache entry for query: [{}] with size {}",
        entry.getQueryText(), size);
    return false;
  }

  @VisibleForTesting
  public static void cleanupInstance() {
    // This should only ever be called in testing scenarios.
    // There should not be any other users of the cache or its entries or this may mess up cleanup.
    if (inited.get()) {
      if (instance.invalidationPollFuture != null) {
        instance.invalidationPollFuture.cancel(true);
        instance.invalidationPollFuture = null;
      }
      instance.clear();
      instance = null;
      inited.set(false);
    }
  }

  private static ScheduledExecutorService invalidationExecutor = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("QueryCacheInvalidator %d").build());
  private static ExecutorService deletionExecutor = Executors.newSingleThreadScheduledExecutor(
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("QueryCacheDeletor %d").build());

  private void scheduleEntryInvalidation(final CacheEntry entry) {
    if (maxEntryLifetime >= 0) {
      // Schedule task to invalidate cache entry and remove from lookup.
      ScheduledFuture<?> future = invalidationExecutor.schedule(new Runnable() {
        @Override
        public void run() {
          removeEntry(entry);
        }
      }, maxEntryLifetime, TimeUnit.MILLISECONDS);
      entry.invalidationFuture = future;
    }
  }

  private static void cleanupEntry(final CacheEntry entry) {
    Preconditions.checkState(entry.getStatus() == CacheEntryStatus.INVALID);
    if (entry.cachedResultsPath != null &&
        !getInstance().zeroRowsPath.equals(entry.cachedResultsPath)) {
      deletionExecutor.execute(new Runnable() {
        @Override
        public void run() {
          Path path = entry.cachedResultsPath;
          LOG.info("Cache directory cleanup: deleting {}", path);
          try {
            FileSystem fs = entry.cachedResultsPath.getFileSystem(getInstance().conf);
            fs.delete(entry.cachedResultsPath, true);
          } catch (Exception err) {
            LOG.error("Error while trying to delete " + path, err);
          }
        }
      });
    }
  }

  public static void incrementMetric(String name, long count) {
    Metrics metrics = MetricsFactory.getInstance();
    if (metrics != null) {
      metrics.incrementCounter(name, count);
    }
  }

  public static void decrementMetric(String name, long count) {
    Metrics metrics = MetricsFactory.getInstance();
    if (metrics != null) {
      metrics.decrementCounter(name, count);
    }
  }

  public static void incrementMetric(String name) {
    incrementMetric(name, 1);
  }

  public static void decrementMetric(String name) {
    decrementMetric(name, 1);
  }

  private static void registerMetrics(Metrics metrics, final QueryResultsCache cache) {
    MetricsVariable<Long> maxCacheSize = new MetricsVariable<Long>() {
      @Override
      public Long getValue() {
        return cache.maxCacheSize;
      }
    };

    MetricsVariable<Long> curCacheSize = new MetricsVariable<Long>() {
      @Override
      public Long getValue() {
        return cache.cacheSize;
      }
    };

    metrics.addGauge(MetricsConstant.QC_MAX_SIZE, maxCacheSize);
    metrics.addGauge(MetricsConstant.QC_CURRENT_SIZE, curCacheSize);
  }

  // EventConsumer to invalidate cache entries based on metastore notification events (alter table, add partition, etc).
  public static class InvalidationEventConsumer implements EventConsumer {
    Configuration conf;

    @Override
    public Configuration getConf() {
      return conf;
    }

    @Override
    public void setConf(Configuration conf) {
      this.conf = conf;
    }

    @Override
    public void accept(NotificationEvent event) {
      String dbName;
      String tableName;

      switch (event.getEventType()) {
      case MessageBuilder.ADD_PARTITION_EVENT:
      case MessageBuilder.ALTER_PARTITION_EVENT:
      case MessageBuilder.DROP_PARTITION_EVENT:
      case MessageBuilder.ALTER_TABLE_EVENT:
      case MessageBuilder.DROP_TABLE_EVENT:
      case MessageBuilder.INSERT_EVENT:
        dbName = event.getDbName();
        tableName = event.getTableName();
        break;
      default:
        return;
      }

      if (dbName == null || tableName == null) {
        LOG.info("Possibly malformed notification event, missing db or table name: {}", event);
        return;
      }

      LOG.debug("Handling event {} on table {}.{}", event.getEventType(), dbName, tableName);

      QueryResultsCache cache = QueryResultsCache.getInstance();
      if (cache != null) {
        long eventTime = event.getEventTime() * 1000L;
        cache.notifyTableChanged(dbName, tableName, eventTime);
      } else {
        LOG.debug("Cache not instantiated, skipping event on {}.{}", dbName, tableName);
      }
    }
  }
}
