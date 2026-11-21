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
import java.util.function.Function;
import java.util.function.LongPredicate;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.security.authorization.plugin.AuthorizationMetaStoreFilterHook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import com.google.common.annotations.VisibleForTesting;

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
 * cached. The versions of at most {@link #MAX_TABLES} tables are kept, each for at most
 * hive.metadata.cache.nontransactional.max.staleness: a table whose version was dropped gets a new one from the
 * counter, so dropping a version only costs a cache miss, never a stale hit.
 */
public final class TableVersionRegistry {
  private static final Logger LOG = LoggerFactory.getLogger(TableVersionRegistry.class);

  /**
   * Key of the {@link QuerySettings} in the query-level cache: there only when the query may use the event-based
   * cache, i.e. the session opted in and the events were applied up to the start of the query.
   */
  public static final Object QUERY_SETTINGS_KEY = new Object() {
    @Override
    public String toString() {
      return "TableVersionRegistry.QUERY_SETTINGS_KEY";
    }
  };

  /** Table property to never cache a table by its version. */
  public static final String TABLE_CACHE_DISABLED_PROPERTY = "hive.metadata.cache.disabled";

  /** How long a query waits for the events to be applied, before it compiles without the cache. */
  static final long SYNC_TIMEOUT_MS = 5000;

  /** The most tables with a version; queries for any table name add one, so it has to be bounded. */
  static final int MAX_TABLES = 200_000;

  /** Applies the events up to the latest one, waiting at most the given time; whether it did. */
  @VisibleForTesting
  static volatile LongPredicate syncer = NotificationEventPoll::syncToLatest;

  @VisibleForTesting
  static volatile Ticker ticker = Ticker.systemTicker();

  // after the ticker, which the constructor uses
  private static final TableVersionRegistry INSTANCE = new TableVersionRegistry();

  private final AtomicLong counter = new AtomicLong();
  private volatile Cache<String, Long> tables = newTables(0);
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
   * @param metastoreConf the value of a setting of the metastore server HS2 is connected to
   * @return whether it got enabled; it does not when the HMS results may depend on the user
   */
  boolean enable(Configuration conf, Function<String, String> metastoreConf) {
    // A shared cache would hand out to one user what HMS or its client filtered or denied for another
    String reason = perUserMetadataReason(conf, metastoreConf);
    if (reason != null) {
      LOG.warn("The HS2 metadata cache by events is disabled: {}", reason);
      return false;
    }
    defaultCatalog = MetaStoreUtils.getDefaultCatalog(conf);
    tables = newTables(HiveConf.getTimeVar(conf,
        HiveConf.ConfVars.HIVE_METADATA_CACHE_NONTRANSACTIONAL_MAX_STALENESS, TimeUnit.MILLISECONDS));
    invalidateAll();
    enabled = true;
    LOG.info("The HS2 metadata cache by events is enabled");
    return true;
  }

  /**
   * @return why the metadata HMS returns may depend on the user asking, or null if it does not
   */
  private static String perUserMetadataReason(Configuration conf, Function<String, String> metastoreConf) {
    String filterHook = HiveConf.getVar(conf, HiveConf.ConfVars.METASTORE_FILTER_HOOK);
    if (!isSharedSafeFilterHook(filterHook)) {
      return HiveConf.ConfVars.METASTORE_FILTER_HOOK.varname + " is set to " + filterHook;
    }
    if (!HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS)) {
      // HS2 asks HMS as itself, so HMS answers every session alike
      return null;
    }
    // With doAs HMS sees the session user, and may filter or deny by it. Only the HMS HS2 is connected to now is
    // checked: the others behind the same URIs are expected to be configured alike.
    String setting = null;
    try {
      setting = MetastoreConf.ConfVars.METASTORE_SERVER_FILTER_ENABLED.getVarname();
      if (Boolean.parseBoolean(StringUtils.trim(metastoreConf.apply(setting)))) {
        setting = MetastoreConf.ConfVars.FILTER_HOOK.getVarname();
        String serverFilterHook = StringUtils.trim(metastoreConf.apply(setting));
        if (!isSharedSafeFilterHook(serverFilterHook)) {
          return HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname + " is on and the metastore filters by " +
              serverFilterHook;
        }
      }
      setting = MetastoreConf.ConfVars.PRE_EVENT_LISTENERS.getVarname();
      String preEventListeners = StringUtils.trim(metastoreConf.apply(setting));
      if (StringUtils.isNotEmpty(preEventListeners)) {
        return HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname + " is on and the metastore authorizes by " +
            preEventListeners;
      }
    } catch (RuntimeException e) {
      LOG.warn("Could not read {} from the metastore", setting, e);
      return HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname + " is on and " + setting +
          " of the metastore could not be read";
    }
    return null;
  }

  /**
   * Whether a filter hook keeps the results the cache holds the same for every user. The
   * {@link AuthorizationMetaStoreFilterHook}, which HS2 sets for the V2 authorizers, filters lists of names, tables
   * and databases only, none of which is cached by version.
   */
  private static boolean isSharedSafeFilterHook(String filterHook) {
    return StringUtils.isEmpty(filterHook) ||
        DefaultMetaStoreFilterHookImpl.class.getName().equals(filterHook) ||
        AuthorizationMetaStoreFilterHook.class.getName().equals(filterHook);
  }

  private static Cache<String, Long> newTables(long maxStalenessMs) {
    Caffeine<Object, Object> builder = Caffeine.newBuilder()
        .maximumSize(MAX_TABLES)
        .ticker(ticker)
        // maintenance on the calling thread, so that the bound holds without a background task
        .executor(Runnable::run);
    if (maxStalenessMs > 0) {
      // a table having the same version longer than that gets a new one, so it is loaded again
      builder.expireAfterWrite(maxStalenessMs, TimeUnit.MILLISECONDS);
    }
    return builder.build();
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
   * the query-level cache. The events are applied first up to the latest one in the metastore; if that fails, or
   * takes longer than the timeout, the query compiles without the cache by versions.
   */
  public static void startQuery(Configuration conf, Map<Object, Object> queryCache) {
    if (queryCache == null || !INSTANCE.enabled) {
      return;
    }
    String mode = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_METADATA_CACHE_NONTRANSACTIONAL_MODE);
    if (!"strict".equalsIgnoreCase(mode)) {
      return;
    }
    if (!syncer.test(SYNC_TIMEOUT_MS)) {
      LOG.warn("Compiling without the metadata cache by events: they could not be applied up to the latest one");
      return;
    }
    queryCache.put(QUERY_SETTINGS_KEY, new QuerySettings(conf));
  }

  public long getVersion(String catName, String dbName, String tblName) {
    // get, not compute: reading a version must not extend how long it is kept
    long version = tables.get(tableKey(catName, dbName, tblName), k -> counter.incrementAndGet());
    long dbVersion = databases.getOrDefault(databaseKey(catName, dbName), 0L);
    return Math.max(version, Math.max(dbVersion, epoch));
  }

  public void invalidateTable(String catName, String dbName, String tblName) {
    if (dbName == null || tblName == null) {
      return;
    }
    tables.asMap().computeIfPresent(tableKey(catName, dbName, tblName), (k, old) -> counter.incrementAndGet());
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
    tables.invalidateAll();
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

  long tableCount() {
    tables.cleanUp();
    return tables.estimatedSize();
  }

  /**
   * What a query decided at its start about using the cache by versions.
   */
  public static final class QuerySettings {
    final boolean excludeExternalTables;

    public QuerySettings(Configuration conf) {
      // external tables are not trusted to change only through the metastore
      excludeExternalTables =
          HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_DISABLE_UNSAFE_EXTERNALTABLE_OPERATIONS);
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
