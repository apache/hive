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

package org.apache.hadoop.hive.ql.cache.results;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.Entity.Type;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.SessionHiveMetaStoreClient;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ColumnAccessInfo;
import org.apache.hadoop.hive.ql.parse.TableAccessInfo;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.HiveOperation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class to handle management and lookup of cached Hive query results.
 */
public final class QueryResultsCache {

  private static final Logger LOG = LoggerFactory.getLogger(QueryResultsCache.class);

  public static class LookupInfo {
    private String queryText;

    public LookupInfo(String queryText) {
      super();
      this.queryText = queryText;
    }

    public String getQueryText() {
      return queryText;
    }
  }

  public static class QueryInfo {
    private LookupInfo lookupInfo;
    private HiveOperation hiveOperation;
    private List<FieldSchema> resultSchema;
    private TableAccessInfo tableAccessInfo;
    private ColumnAccessInfo columnAccessInfo;
    private Set<ReadEntity> inputs;

    public QueryInfo(
        LookupInfo lookupInfo,
        HiveOperation hiveOperation,
        List<FieldSchema> resultSchema,
        TableAccessInfo tableAccessInfo,
        ColumnAccessInfo columnAccessInfo,
        Set<ReadEntity> inputs) {
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
  }

  public static class CacheEntry {
    private QueryInfo queryInfo;
    private FetchWork fetchWork;
    private Path cachedResultsPath;

    // Cache administration
    private long createTime;
    private long size;
    private AtomicBoolean valid = new AtomicBoolean(false);
    private AtomicInteger readers = new AtomicInteger(0);
    private ScheduledFuture<?> invalidationFuture = null;

    public boolean isValid() {
      return valid.get();
    }

    public void releaseReader() {
      int readerCount = 0;
      synchronized (this) {
        readerCount = readers.decrementAndGet();
      }
      LOG.debug("releaseReader: entry: {}, readerCount: {}", this, readerCount);
      Preconditions.checkState(readerCount >= 0);

      cleanupIfNeeded();
    }

    public String toString() {
      return "CacheEntry query: [" + getQueryInfo().getLookupInfo().getQueryText()
          + "], location: " + cachedResultsPath
          + ", size: " + size;
    }

    public boolean addReader() {
      boolean added = false;
      int readerCount = 0;
      synchronized (this) {
        if (valid.get()) {
          readerCount = readers.incrementAndGet();
          added = true;
        }
      }
      Preconditions.checkState(readerCount > 0);
      LOG.debug("addReader: entry: {}, readerCount: {}", this, readerCount);
      return added;
    }

    private int numReaders() {
      return readers.get();
    }

    private void invalidate() {
      boolean wasValid = setValidity(false);

      if (wasValid) {
        LOG.info("Invalidated cache entry: {}", this);

        if (invalidationFuture != null) {
          // The cache entry has just been invalidated, no need for the scheduled invalidation.
          invalidationFuture.cancel(false);
        }
        cleanupIfNeeded();
      }
    }

    /**
     * Set the validity, returning the previous validity value.
     * @param valid
     * @return
     */
    private boolean setValidity(boolean valid) {
      synchronized(this) {
        return this.valid.getAndSet(valid);
      }
    }

    private void cleanupIfNeeded() {
      if (!isValid() && readers.get() <= 0) {
        QueryResultsCache.cleanupEntry(this);
      }
    }

    private String getQueryText() {
      return getQueryInfo().getLookupInfo().getQueryText();
    }

    public FetchWork getFetchWork() {
      return fetchWork;
    }

    public QueryInfo getQueryInfo() {
      return queryInfo;
    }

    public Path getCachedResultsPath() {
      return cachedResultsPath;
    }
  }

  // Allow lookup by query string
  private final Map<String, Set<CacheEntry>> queryMap = new HashMap<String, Set<CacheEntry>>();

  // LRU. Could also implement LRU as a doubly linked list if CacheEntry keeps its node.
  // Use synchronized map since even read actions cause the lru to get updated.
  private final Map<CacheEntry, CacheEntry> lru = Collections.synchronizedMap(
      new LinkedHashMap<CacheEntry, CacheEntry>(INITIAL_LRU_SIZE, LRU_LOAD_FACTOR, true));

  private final HiveConf conf;
  private Path cacheDirPath;
  private long cacheSize = 0;
  private long maxCacheSize;
  private long maxEntrySize;
  private long maxEntryLifetime;
  private ReadWriteLock rwLock = new ReentrantReadWriteLock();

  private QueryResultsCache(HiveConf configuration) throws IOException {
    this.conf = configuration;

    // Set up cache directory
    Path rootCacheDir = new Path(conf.getVar(HiveConf.ConfVars.HIVE_QUERY_RESULTS_CACHE_DIRECTORY));
    LOG.info("Initializing query results cache at {}", rootCacheDir);
    Utilities.ensurePathIsWritable(rootCacheDir, conf);

    String currentCacheDirName = "results-" + UUID.randomUUID().toString();
    cacheDirPath = new Path(rootCacheDir, currentCacheDirName);
    FileSystem fs = cacheDirPath.getFileSystem(conf);
    FsPermission fsPermission = new FsPermission("700");
    fs.mkdirs(cacheDirPath, fsPermission);

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
      } catch (IOException err) {
        inited.set(false);
        throw err;
      }
    }
  }

  public static QueryResultsCache getInstance() {
    return instance;
  }

  /**
   * Check if the cache contains an entry for the requested LookupInfo.
   * @param request
   * @param addReader Should the reader count be incremented during the lookup.
   *        This will ensure the returned entry can be used after the lookup.
   *        If true, the caller will be responsible for decrementing the reader count
   *        using CacheEntry.releaseReader().
   * @return  The cached result if there is a match in the cache, or null if no match is found.
   */
  public CacheEntry lookup(LookupInfo request, boolean addReader) {
    CacheEntry result = null;

    LOG.debug("QueryResultsCache lookup for query: {}", request.queryText);

    Lock readLock = rwLock.readLock();
    try {
      readLock.lock();
      Set<CacheEntry> candidates = queryMap.get(request.queryText);
      if (candidates != null) {
        for (CacheEntry candidate : candidates) {
          if (entryMatches(request, candidate)) {
            result = candidate;
            break;
          }
        }

        if (result != null) {
          lru.get(result);  // Update LRU

          if (!result.isValid()) {
            // Entry is in the cache, but not valid.
            // This can happen when the entry is first added, before the data has been moved
            // to the results cache directory. We cannot use this entry yet.
            result = null;
          } else {
            if (addReader) {
              // Caller will need to be responsible for releasing the reader count.
              result.addReader();
            }
          }
        }
      }
    } finally {
      readLock.unlock();
    }

    LOG.debug("QueryResultsCache lookup result: {}", result);

    return result;
  }

  /**
   * Add an entry to the query results cache.
   * Important: Adding the entry to the cache will increment the reader count for the cache entry.
   * CacheEntry.releaseReader() should be called when the caller is done with the cache entry.
   *
   * @param queryInfo
   * @param fetchWork
   * @return The entry if added to the cache. null if the entry is not added.
   */
  public CacheEntry addToCache(QueryInfo queryInfo, FetchWork fetchWork) {

    CacheEntry addedEntry = null;
    boolean dataDirMoved = false;
    Path queryResultsPath = null;
    Path cachedResultsPath = null;
    String queryText = queryInfo.getLookupInfo().getQueryText();

    // Should we remove other candidate entries if they are equivalent to these query results?
    try {
      CacheEntry potentialEntry = new CacheEntry();
      potentialEntry.queryInfo = queryInfo;
      queryResultsPath = fetchWork.getTblDir();
      FileSystem resultsFs = queryResultsPath.getFileSystem(conf);
      ContentSummary cs = resultsFs.getContentSummary(queryResultsPath);
      potentialEntry.size = cs.getLength();

      Lock writeLock = rwLock.writeLock();
      try {
        writeLock.lock();

        if (!shouldEntryBeAdded(potentialEntry)) {
          return null;
        }
        if (!clearSpaceForCacheEntry(potentialEntry)) {
          return null;
        }

        LOG.info("Adding cache entry for query '{}'", queryText);

        // Add the entry to the cache structures while under write lock. Do not mark the entry
        // as valid yet, since the query results have not yet been moved to the cache directory.
        // Do the data move after unlocking since it might take time.
        // Mark the entry as valid once the data has been moved to the cache directory.
        Set<CacheEntry> entriesForQuery = queryMap.get(queryText);
        if (entriesForQuery == null) {
          entriesForQuery = new HashSet<CacheEntry>();
          queryMap.put(queryText, entriesForQuery);
        }
        entriesForQuery.add(potentialEntry);
        lru.put(potentialEntry, potentialEntry);
        cacheSize += potentialEntry.size;
        addedEntry = potentialEntry;

      } finally {
        writeLock.unlock();
      }

      // Move the query results to the query cache directory.
      cachedResultsPath = moveResultsToCacheDirectory(queryResultsPath);
      dataDirMoved = true;
      LOG.info("Moved query results from {} to {} (size {}) for query '{}'",
          queryResultsPath, cachedResultsPath, cs.getLength(), queryText);

      // Create a new FetchWork to reference the new cache location.
      FetchWork fetchWorkForCache =
          new FetchWork(cachedResultsPath, fetchWork.getTblDesc(), fetchWork.getLimit());
      fetchWorkForCache.setCachedResult(true);
      addedEntry.fetchWork = fetchWorkForCache;
      addedEntry.cachedResultsPath = cachedResultsPath;
      addedEntry.createTime = System.currentTimeMillis();
      addedEntry.setValidity(true);

      // Mark this entry as being in use. Caller will need to release later.
      addedEntry.addReader();

      scheduleEntryInvalidation(addedEntry);
    } catch (Exception err) {
      LOG.error("Failed to create cache entry for query results for query: " + queryText, err);

      if (addedEntry != null) {
        // If the entry was already added to the cache when we hit error, clean up properly.

        if (dataDirMoved) {
          // If data was moved from original location to cache directory, we need to move it back!
          LOG.info("Restoring query results from {} back to {}", cachedResultsPath, queryResultsPath);
          try {
            FileSystem fs = cachedResultsPath.getFileSystem(conf);
            fs.rename(cachedResultsPath, queryResultsPath);
            addedEntry.cachedResultsPath = null;
          } catch (Exception err2) {
            String errMsg = "Failed cleanup during failed attempt to cache query: " + queryText;
            LOG.error(errMsg);
            throw new RuntimeException(errMsg);
          }
        }

        addedEntry.invalidate();
        if (addedEntry.numReaders() > 0) {
          addedEntry.releaseReader();
        }
      }

      return null;
    }

    return addedEntry;
  }

  public void clear() {
    Lock writeLock = rwLock.writeLock();
    try {
      writeLock.lock();
      LOG.info("Clearing the results cache");
      for (CacheEntry entry : lru.keySet().toArray(EMPTY_CACHEENTRY_ARRAY)) {
        try {
          removeEntry(entry);
        } catch (Exception err) {
          LOG.error("Error removing cache entry " + entry, err);
        }
      }
    } finally {
      writeLock.unlock();
    }
  }

  public long getSize() {
    Lock readLock = rwLock.readLock();
    try {
      readLock.lock();
      return cacheSize;
    } finally {
      readLock.unlock();
    }
  }

  private static final int INITIAL_LRU_SIZE = 16;
  private static final float LRU_LOAD_FACTOR = 0.75f;
  private static final CacheEntry[] EMPTY_CACHEENTRY_ARRAY = {};

  private boolean entryMatches(LookupInfo lookupInfo, CacheEntry entry) {
    QueryInfo queryInfo = entry.getQueryInfo();
    for (ReadEntity readEntity : queryInfo.getInputs()) {
      // Check that the tables used do not resolve to temp tables.
      if (readEntity.getType() == Type.TABLE) {
        Table tableUsed = readEntity.getTable();
        Map<String, Table> tempTables =
            SessionHiveMetaStoreClient.getTempTablesForDatabase(tableUsed.getDbName());
        if (tempTables != null && tempTables.containsKey(tableUsed.getTableName())) {
          LOG.info("{} resolves to a temporary table in the current session. This query cannot use the cache.",
              tableUsed.getTableName());
          return false;
        }
      }
    }

    return true;
  }

  private void removeEntry(CacheEntry entry) {
    entry.invalidate();
    removeFromLookup(entry);
    lru.remove(entry);
    // Should the cache size be updated here, or after the result data has actually been deleted?
    cacheSize -= entry.size;
  }

  private void removeFromLookup(CacheEntry entry) {
    String queryString = entry.getQueryText();
    Set<CacheEntry> entries = queryMap.get(queryString);
    Preconditions.checkState(entries != null);
    boolean deleted = entries.remove(entry);
    Preconditions.checkState(deleted);
    if (entries.isEmpty()) {
      queryMap.remove(queryString);
    }
  }

  private void calculateEntrySize(CacheEntry entry, FetchWork fetchWork) throws IOException {
    Path queryResultsPath = fetchWork.getTblDir();
    FileSystem resultsFs = queryResultsPath.getFileSystem(conf);
    ContentSummary cs = resultsFs.getContentSummary(queryResultsPath);
    entry.size = cs.getLength();
  }

  /**
   * Determines if the cache entry should be added to the results cache.
   */
  private boolean shouldEntryBeAdded(CacheEntry entry) {
    // Assumes the cache lock has already been taken.
    if (maxEntrySize >= 0 && entry.size > maxEntrySize) {
      LOG.debug("Cache entry size {} larger than max entry size ({})", entry.size, maxEntrySize);
      return false;
    }

    return true;
  }

  private Path moveResultsToCacheDirectory(Path queryResultsPath) throws IOException {
    String dirName = UUID.randomUUID().toString();
    Path cachedResultsPath = new Path(cacheDirPath, dirName);
    FileSystem fs = cachedResultsPath.getFileSystem(conf);
    fs.rename(queryResultsPath, cachedResultsPath);
    return cachedResultsPath;
  }

  private boolean hasSpaceForCacheEntry(CacheEntry entry) {
    if (maxCacheSize >= 0) {
      return (cacheSize + entry.size) <= maxCacheSize;
    }
    // Negative max cache size means unbounded.
    return true;
  }

  private boolean clearSpaceForCacheEntry(CacheEntry entry) {
    if (hasSpaceForCacheEntry(entry)) {
      return true;
    }

    LOG.info("Clearing space for cache entry for query: [{}] with size {}",
        entry.getQueryText(), entry.size);

    // Entries should be in LRU order in the keyset iterator.
    CacheEntry[] entries = lru.keySet().toArray(EMPTY_CACHEENTRY_ARRAY);
    for (CacheEntry removalCandidate : entries) {
      if (!removalCandidate.isValid()) {
        // Likely an entry which is still getting its results moved to the cache directory.
        continue;
      }
      // Only delete the entry if it has no readers.
      if (!(removalCandidate.numReaders() > 0)) {
        LOG.info("Removing entry: {}", removalCandidate);
        removeEntry(removalCandidate);
        if (hasSpaceForCacheEntry(entry)) {
          return true;
        }
      }
    }

    LOG.info("Could not free enough space for cache entry for query: [{}] withe size {}",
        entry.getQueryText(), entry.size);
    return false;
  }


  @VisibleForTesting
  public static void cleanupInstance() {
    // This should only ever be called in testing scenarios.
    // There should not be any other users of the cache or its entries or this may mess up cleanup.
    if (inited.get()) {
      getInstance().clear();
      instance = null;
      inited.set(false);
    }
  }

  private static ScheduledExecutorService invalidationExecutor = null;
  private static ExecutorService deletionExecutor = null;

  static {
    ThreadFactory threadFactory =
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("QueryResultsCache %d").build();
    invalidationExecutor = Executors.newSingleThreadScheduledExecutor(threadFactory);
    deletionExecutor = Executors.newSingleThreadExecutor(threadFactory);
  }

  private void scheduleEntryInvalidation(final CacheEntry entry) {
    if (maxEntryLifetime >= 0) {
      // Schedule task to invalidate cache entry.
      ScheduledFuture<?> future = invalidationExecutor.schedule(new Runnable() {
        @Override
        public void run() {
          entry.invalidate();
        }
      }, maxEntryLifetime, TimeUnit.MILLISECONDS);
      entry.invalidationFuture = future;
    }
  }

  private static void cleanupEntry(final CacheEntry entry) {
    Preconditions.checkState(!entry.isValid());

    if (entry.cachedResultsPath != null) {
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
}
