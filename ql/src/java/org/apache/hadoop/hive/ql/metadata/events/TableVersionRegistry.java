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

package org.apache.hadoop.hive.ql.metadata.events;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Versions of the tables cached in HS2, kept by the HMS notification events (HIVE-30264).
 * <p>
 * A version is a number taken from one counter each time a table, its database or everything is invalidated;
 * the version of a table is the largest of the three. Cache entries carry the version they were loaded at, so
 * an invalidation makes them unreachable: nothing is evicted here, old entries age out of the cache.
 * <p>
 * A version is read before the metastore is asked, so an entry never holds data older than its version:
 * an event applied in between gives the table a new version and the entry is not found again.
 * <p>
 * Only tables already asked for have a version; events on other tables are ignored, as nothing of theirs is
 * cached.
 */
public final class TableVersionRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(TableVersionRegistry.class);
  private static final TableVersionRegistry INSTANCE = new TableVersionRegistry();

  /**
   * Key of the {@link QuerySettings} in the query-level cache: there only when the query may use the event-based
   * cache, i.e. the session opted in and, in strict mode, the events were applied up to the start of the query.
   */
  public static final Object QUERY_SETTINGS_KEY = new Object() {
    @Override
    public String toString() {
      return "TableVersionRegistry.QUERY_SETTINGS_KEY";
    }
  };

  /** Table property to never cache a table by its version. */
  public static final String TABLE_CACHE_DISABLED_PROPERTY = "hive.metadata.cache.disabled";

  /** How long a query waits in strict mode for the events to be applied, before it compiles without the cache. */
  static final long SYNC_TIMEOUT_MS = 5000;

  private final AtomicLong counter = new AtomicLong();
  private final ConcurrentMap<String, TableVersion> tables = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, Long> databases = new ConcurrentHashMap<>();
  private volatile long epoch = counter.incrementAndGet();
  private volatile boolean enabled;
  private volatile String defaultCatalog = "hive";

  private TableVersionRegistry() {
  }

  public static TableVersionRegistry get() {
    return INSTANCE;
  }

  /**
   * Enables the registry once the poller has its starting event id.
   * @return whether it got enabled; it does not when the HMS results are filtered per user
   */
  boolean enable(Configuration conf) {
    String filterHook = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_FILTER_HOOK);
    if (StringUtils.isNotEmpty(filterHook) && !DefaultMetaStoreFilterHookImpl.class.getName().equals(filterHook)) {
      // A shared cache would hand out what the filter hook hid from the user
      LOG.warn("The HS2 metadata cache by events is disabled: {} is set to {}",
          HiveConf.ConfVars.METASTORE_FILTER_HOOK.varname, filterHook);
      return false;
    }
    defaultCatalog = MetaStoreUtils.getDefaultCatalog(conf);
    invalidateAll();
    enabled = true;
    LOG.info("The HS2 metadata cache by events is enabled");
    return true;
  }

  void disable() {
    enabled = false;
    invalidateAll();
  }

  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Decides at the start of the compilation whether the query may use the cache by versions, and records it in
   * the query-level cache. In strict mode the events are applied first up to the latest one in the metastore; if
   * that fails, or takes longer than the timeout, the query compiles without the cache by versions.
   */
  public static void startQuery(Configuration conf, Map<Object, Object> queryCache) {
    if (queryCache == null || !INSTANCE.enabled) {
      return;
    }
    String mode = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_METADATA_CACHE_NONTRANSACTIONAL_MODE);
    if ("off".equalsIgnoreCase(mode)) {
      return;
    }
    if ("strict".equalsIgnoreCase(mode)) {
      if (!NotificationEventPoll.syncToLatest(SYNC_TIMEOUT_MS)) {
        LOG.warn("Compiling without the metadata cache by events: they could not be applied up to the latest one");
        return;
      }
    }
    queryCache.put(QUERY_SETTINGS_KEY, new QuerySettings(conf));
  }

  /**
   * @param maxStalenessMs a table having the same version longer than that gets a new one, so it is loaded
   *     again; not positive for no limit
   */
  public long getVersion(String catName, String dbName, String tblName, long maxStalenessMs) {
    long now = System.currentTimeMillis();
    TableVersion v = tables.compute(tableKey(catName, dbName, tblName), (k, old) ->
        old == null || (maxStalenessMs > 0 && now - old.since > maxStalenessMs) ?
            new TableVersion(counter.incrementAndGet(), now) : old);
    long dbVersion = databases.getOrDefault(databaseKey(catName, dbName), 0L);
    return Math.max(v.version, Math.max(dbVersion, epoch));
  }

  public void invalidateTable(String catName, String dbName, String tblName) {
    if (dbName == null || tblName == null) {
      return;
    }
    tables.computeIfPresent(tableKey(catName, dbName, tblName),
        (k, old) -> new TableVersion(counter.incrementAndGet(), System.currentTimeMillis()));
  }

  public void invalidateDatabase(String catName, String dbName) {
    if (dbName == null) {
      return;
    }
    databases.put(databaseKey(catName, dbName), counter.incrementAndGet());
  }

  public void invalidateAll() {
    epoch = counter.incrementAndGet();
    // the versions are all older than the epoch now
    tables.clear();
    databases.clear();
  }

  /**
   * Invalidates what a query wrote, so that the session sees its own changes before their events arrive.
   */
  public void invalidate(Collection<WriteEntity> outputs) {
    if (!enabled || outputs == null) {
      return;
    }
    for (WriteEntity output : outputs) {
      switch (output.getType()) {
      case TABLE:
      case PARTITION:
        if (output.getTable() != null) {
          invalidateTable(output.getTable().getCatName(), output.getTable().getDbName(),
              output.getTable().getTableName());
        }
        break;
      case DATABASE:
        if (output.getDatabase() != null) {
          invalidateDatabase(output.getDatabase().getCatalogName(), output.getDatabase().getName());
        }
        break;
      default:
        break;
      }
    }
  }

  private String tableKey(String catName, String dbName, String tblName) {
    return databaseKey(catName, dbName) + "." + StringUtils.lowerCase(tblName);
  }

  private String databaseKey(String catName, String dbName) {
    return (StringUtils.isEmpty(catName) ? defaultCatalog : catName.toLowerCase()) + "." +
        StringUtils.lowerCase(dbName);
  }

  int tableCount() {
    return tables.size();
  }

  private static final class TableVersion {
    final long version;
    final long since;

    TableVersion(long version, long since) {
      this.version = version;
      this.since = since;
    }
  }

  /**
   * What a query decided at its start about using the cache by versions.
   */
  public static final class QuerySettings {
    final long maxStalenessMs;
    final boolean excludeExternalTables;

    public QuerySettings(Configuration conf) {
      maxStalenessMs = HiveConf.getTimeVar(conf,
          HiveConf.ConfVars.HIVE_METADATA_CACHE_NONTRANSACTIONAL_MAX_STALENESS, TimeUnit.MILLISECONDS);
      // external tables are not trusted to change only through the metastore
      excludeExternalTables =
          HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_DISABLE_UNSAFE_EXTERNALTABLE_OPERATIONS);
    }

    public long getMaxStalenessMs() {
      return maxStalenessMs;
    }

    /**
     * Whether the metadata of the table may be cached by its version.
     */
    public boolean canCache(Table table) {
      if (table == null) {
        return false;
      }
      Map<String, String> params = table.getParameters();
      if (params != null && Boolean.parseBoolean(params.get(TABLE_CACHE_DISABLED_PROPERTY))) {
        return false;
      }
      return !excludeExternalTables || !TableType.EXTERNAL_TABLE.name().equals(table.getTableType());
    }
  }
}
