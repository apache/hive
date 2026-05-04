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

package org.apache.iceberg.rest;

import com.github.benmanes.caffeine.cache.Ticker;

import java.lang.ref.SoftReference;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.hive.MetadataLocator;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class that wraps an Iceberg Catalog to cache tables.
 */
public class HMSCachingCatalog extends CachingCatalog implements SupportsNamespaces, ViewCatalog {
  protected static final Logger LOG = LoggerFactory.getLogger(HMSCachingCatalog.class);

  @TestOnly
  private static SoftReference<HMSCachingCatalog> cacheRef = new SoftReference<>(null);

  @TestOnly @SuppressWarnings("unchecked")
  public static <C extends Catalog> C  getLatestCache(Function<HMSCachingCatalog, C> extractor) {
    HMSCachingCatalog cache = cacheRef.get();
    if (cache == null) {
      return null;
    }
    return extractor == null ? (C) cache : extractor.apply(cache);
  }

  @TestOnly
  public HiveCatalog getCatalog() {
    return hiveCatalog;
  }

  // The underlying HiveCatalog instance.
  private final HiveCatalog hiveCatalog;
  // Duplicate because CachingCatalog doesn't expose the case sensitivity of the underlying catalog,
  // which is needed for canonicalizing identifiers before caching.
  private final boolean caseSensitive;
  // The locator.
  private final MetadataLocator metadataLocator;
  // An L1 small latency cache.
  // This is used to cache the last cached time for each table identifier,
  // so that we can skip location check for repeated access to the same table within a short period of time,
  // which can significantly reduce the latency for repeated access to the same table.
  private final Map<TableIdentifier, Long> l1Cache;
  // The TTL for L1 cache (3s).
  private final int l1Ttl;
  // The L1 cache size.
  private final int l1CacheSize;

  // Metrics counters.
  private final AtomicLong cacheHitCount = new AtomicLong(0);
  private final AtomicLong cacheMissCount = new AtomicLong(0);
  private final AtomicLong cacheLoadCount = new AtomicLong(0);
  private final AtomicLong cacheInvalidateCount = new AtomicLong(0);
  private final AtomicLong cacheMetaLoadCount = new AtomicLong(0);

  public HMSCachingCatalog(HiveCatalog catalog, long expirationMs) {
    this(catalog, expirationMs, /*caseSensitive*/ true);
  }

  public HMSCachingCatalog(HiveCatalog catalog, long expirationMs, boolean caseSensitive) {
    super(catalog, caseSensitive, expirationMs, Ticker.systemTicker());
    this.hiveCatalog = catalog;
    this.caseSensitive = caseSensitive;
    this.metadataLocator = new MetadataLocator(catalog);
    Configuration conf = catalog.getConf();
    if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_IN_TEST)) {
      // Only keep a reference to the latest cache for testing purpose, so that tests can manipulate the catalog.
      cacheRef = new SoftReference<>(this);
    }
    int l1size = conf.getInt("hms.caching.catalog.l1.cache.size", 32);
    int l1ttl = conf.getInt("hms.caching.catalog.l1.cache.ttl", 3_000);
    if (l1size > 0 && l1ttl > 0) {
       l1Cache = Collections.synchronizedMap(new LinkedHashMap<TableIdentifier, Long>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<TableIdentifier, Long> eldest) {
          return size() > l1CacheSize;
        }
      });
       l1Ttl = l1ttl;
       l1CacheSize = l1size;
    } else {
       l1Cache = Collections.emptyMap();
       l1Ttl = 0;
       l1CacheSize = 0;
    }
  }

  /**
   * Callback when cache invalidates the entry for a given table identifier.
   *
   * @param tid the table identifier to invalidate
   */
  protected void onCacheInvalidate(TableIdentifier tid) {
    long count = cacheInvalidateCount.incrementAndGet();
    LOG.debug("Cache invalidate {}: {}", tid, count);
  }

  /**
   * Callback when cache loads a table for a given table identifier.
   *
   * @param tid the table identifier
   */
  protected void onCacheLoad(TableIdentifier tid) {
    long count = cacheLoadCount.incrementAndGet();
    LOG.debug("Cache load {}: {}", tid, count);
  }

  /**
   * Callback when cache hit for a given table identifier.
   *
   * @param tid the table identifier
   */
  protected void onCacheHit(TableIdentifier tid) {
    long count = cacheHitCount.incrementAndGet();
    LOG.debug("Cache hit {} : {}", tid, count);
  }

  /**
   * Callback when cache miss occurs for a given table identifier.
   *
   * @param tid the table identifier
   */
  protected void onCacheMiss(TableIdentifier tid) {
    long count = cacheMissCount.incrementAndGet();
    LOG.debug("Cache miss {}: {}", tid, count);
  }

  /**
   * Callback when cache loads a metadata table for a given table identifier.
   *
   * @param tid the table identifier
   */
  protected void onCacheMetaLoad(TableIdentifier tid) {
    long count = cacheMetaLoadCount.incrementAndGet();
    LOG.debug("Cache meta-load {}: {}", tid, count);
  }

  // Getter methods for accessing metrics
  public long getCacheHitCount() {
    return cacheHitCount.get();
  }

  public long getCacheMissCount() {
    return cacheMissCount.get();
  }

  public long getCacheLoadCount() {
    return cacheLoadCount.get();
  }

  public long getCacheInvalidateCount() {
    return cacheInvalidateCount.get();
  }

  public long getCacheMetaLoadCount() {
    return cacheMetaLoadCount.get();
  }

  public double getCacheHitRate() {
    long hits = cacheHitCount.get();
    long total = hits + cacheMissCount.get();
    return total == 0 ? 0.0 : (double) hits / total;
  }

  /**
   * Generates a map of this cache's performance metrics, including hit count,
   * miss count, load count, invalidate count, meta-load count, and hit rate.
   * This can be used for monitoring and debugging purposes to understand the effectiveness of the cache.
   * @return a map of cache performance metrics
   */
  public Map<String, Number> cacheStats() {
    return Map.of(
            "hit", getCacheHitCount(),
            "miss", getCacheMissCount(),
            "load", getCacheLoadCount(),
            "invalidate", getCacheInvalidateCount(),
            "metaload", getCacheMetaLoadCount(),
            "hit-rate", getCacheHitRate()
    );
  }


  @Override
  public void createNamespace(Namespace namespace, Map<String, String> map) {
    hiveCatalog.createNamespace(namespace, map);
  }

  @Override
  public List<Namespace> listNamespaces(Namespace namespace) throws NoSuchNamespaceException {
    return hiveCatalog.listNamespaces(namespace);
  }

  /**
   * Canonicalizes the given table identifier based on the case sensitivity of the underlying catalog.
   * Copied from CachingCatalog that exposes it as private.
   * @param tableIdentifier the table identifier to canonicalize
   * @return the canonicalized table identifier
   */
  private TableIdentifier canonicalizeIdentifier(TableIdentifier tableIdentifier) {
    return this.caseSensitive ? tableIdentifier : tableIdentifier.toLowerCase();
  }

  @Override
  public void invalidateTable(TableIdentifier ident) {
    super.invalidateTable(ident);
    l1Cache.remove(ident);
  }

  @Override
  public Table loadTable(final TableIdentifier identifier) {
    final TableIdentifier canonicalized = canonicalizeIdentifier(identifier);
    final Table cachedTable = tableCache.getIfPresent(canonicalized);
    long now = System.currentTimeMillis();
    if (cachedTable != null) {
      // Determine if L1 cache is valid based on the last cached time and the TTL.
      // If the table is in L1 cache, we can skip the location check and return the cached table directly,
      // which can significantly reduce the latency for repeated access to the same table.
      Long lastCached = l1Cache.get(canonicalized);
      if (lastCached != null) {
        if (now - lastCached < l1Ttl) {
          LOG.debug("Table {} is in L1 cache, returning cached table", canonicalized);
          onCacheHit(canonicalized);
          return cachedTable;
        } else {
          l1Cache.remove(canonicalized);
        }
      }
      // If the table is no longer in L1 cache, we need to check the location.
      final String location = metadataLocator.getLocation(canonicalized);
      if (location == null) {
        LOG.debug("Table {} has no location, returning cached table without location", canonicalized);
        onCacheHit(canonicalized);
        l1Cache.put(canonicalized, now);
        return cachedTable;
      }
      String cachedLocation = cachedTable instanceof HasTableOperations tableOps
              ? tableOps.operations().current().metadataFileLocation()
              : null;
      if (location.equals(cachedLocation)) {
        onCacheHit(canonicalized);
        l1Cache.put(canonicalized, now);
        return cachedTable;
      } else {
        LOG.debug("Invalidate table {}, cached {} != actual {}", canonicalized, cachedLocation, location);
        // Invalidate the cached table if the location is different
        invalidateTable(canonicalized);
        onCacheInvalidate(canonicalized);
      }
    } else {
      onCacheMiss(canonicalized);
    }
    // The following code is copied from CachingCatalog.loadTable(), but with additional handling for L1 cache and stats.
    final Table table = tableCache.get(canonicalized, this::loadTableWithoutCache);
    if (table instanceof BaseMetadataTable) {
      // Cache underlying table: there must be a table named by the namespace (?)
      TableIdentifier originTableIdentifier = TableIdentifier.of(canonicalized.namespace().levels());
      Table originTable = tableCache.get(originTableIdentifier, this::loadTableWithoutCache);
      // Share TableOperations instance of origin table for all metadata tables, so that metadata
      // table instances are refreshed as well when origin table instance is refreshed.
      if (originTable instanceof HasTableOperations tableOps) {
        TableOperations ops = tableOps.operations();
        MetadataTableType type = MetadataTableType.from(canonicalized.name());
        // Defensive: CachingCatalog doesn't perform this check
        if (type != null) {
          Table metadataTable = MetadataTableUtils.createMetadataTableInstance(ops, hiveCatalog.name(), originTableIdentifier, canonicalized, type);
          tableCache.put(canonicalized, metadataTable);
          l1Cache.put(canonicalized, now);
          onCacheMetaLoad(canonicalized);
          LOG.debug("Loaded metadata table: {} for origin table: {}", canonicalized, originTableIdentifier);
          // Return the metadata table instead of the original table
          return metadataTable;
        }
      }
    }
    l1Cache.put(canonicalized, now);
    onCacheLoad(canonicalized);
    return table;
  }

  private Table loadTableWithoutCache(TableIdentifier identifier) {
      return hiveCatalog.loadTable(identifier);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
    return hiveCatalog.loadNamespaceMetadata(namespace);
  }

  @Override
  public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
    List<TableIdentifier> tables = listTables(namespace);
    for (TableIdentifier ident : tables) {
      invalidateTable(ident);
    }
    return hiveCatalog.dropNamespace(namespace);
  }

  @Override
  public boolean setProperties(Namespace namespace, Map<String, String> map) throws NoSuchNamespaceException {
    return hiveCatalog.setProperties(namespace, map);
  }

  @Override
  public boolean removeProperties(Namespace namespace, Set<String> set) throws NoSuchNamespaceException {
    return hiveCatalog.removeProperties(namespace, set);
  }

  @Override
  public boolean namespaceExists(Namespace namespace) {
    return hiveCatalog.namespaceExists(namespace);
  }

  @Override
  public Catalog.TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return hiveCatalog.buildTable(identifier, schema);
  }

  @Override
  public List<TableIdentifier> listViews(Namespace namespace) {
    return hiveCatalog.listViews(namespace);
  }

  @Override
  public View loadView(TableIdentifier identifier) {
    return hiveCatalog.loadView(identifier);
  }

  @Override
  public boolean viewExists(TableIdentifier identifier) {
    return hiveCatalog.viewExists(identifier);
  }

  @Override
  public ViewBuilder buildView(TableIdentifier identifier) {
    return hiveCatalog.buildView(identifier);
  }

  @Override
  public boolean dropView(TableIdentifier identifier) {
    return hiveCatalog.dropView(identifier);
  }

  @Override
  public void renameView(TableIdentifier from, TableIdentifier to) {
    hiveCatalog.renameView(from, to);
  }

  @Override
  public void invalidateView(TableIdentifier identifier) {
    hiveCatalog.invalidateView(identifier);
  }

  @Override
  public void initialize(String name, Map<String, String> properties) {
    hiveCatalog.initialize(name, properties);
  }
}
