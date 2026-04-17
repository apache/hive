/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.rest;

import com.github.benmanes.caffeine.cache.Ticker;

import java.lang.ref.SoftReference;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.iceberg.exceptions.NoSuchTableException;
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

  private static SoftReference<HMSCachingCatalog> CACHE = new SoftReference<>(null);
  @TestOnly
  public static <C extends Catalog> C  getLatestCache(Function<HMSCachingCatalog, C> extractor) {
    HMSCachingCatalog cache = CACHE.get();
    if (cache == null) {
      return null;
    }
    return extractor == null ? (C) cache : extractor.apply(cache);
  }

  @TestOnly
  public HiveCatalog getCatalog() {
    return hiveCatalog;
  }

  protected final HiveCatalog hiveCatalog;
  // Metrics counters
  private final AtomicLong cacheHitCount = new AtomicLong(0);
  private final AtomicLong cacheMissCount = new AtomicLong(0);
  private final AtomicLong cacheLoadCount = new AtomicLong(0);
  private final AtomicLong cacheInvalidateCount = new AtomicLong(0);
  private final AtomicLong cacheMetaLoadCount = new AtomicLong(0);

  public HMSCachingCatalog(HiveCatalog catalog, long expirationMs) {
    this(catalog, expirationMs, /*caseSensitive*/ true, null);
  }

  public HMSCachingCatalog(HiveCatalog catalog, long expirationMs, boolean caseSensitive, Configuration conf) {
    super(catalog, caseSensitive, expirationMs, Ticker.systemTicker());
    this.hiveCatalog = catalog;
    if (catalog.getConf().getBoolean("metastore.iceberg.catalog.cache.debug", false)) {
      CACHE = new SoftReference<>(this);
    }
  }

  /**
   * Callback when cache invalidates the entry for a given table identifier.
   *
   * @param tid the table identifier to invalidate
   */
  protected void onCacheInvalidate(TableIdentifier tid) {
    cacheInvalidateCount.incrementAndGet();
    LOG.debug("Cache invalidate {}: {}", tid, cacheInvalidateCount.get());
  }

  /**
   * Callback when cache loads a table for a given table identifier.
   *
   * @param tid the table identifier
   */
  protected void onCacheLoad(TableIdentifier tid) {
    cacheLoadCount.incrementAndGet();
    LOG.debug("Cache load {}: {}", tid, cacheLoadCount.get());
  }

  /**
   * Callback when cache hit for a given table identifier.
   *
   * @param tid the table identifier
   */
  protected void onCacheHit(TableIdentifier tid) {
    cacheHitCount.incrementAndGet();
    LOG.debug("Cache hit {} : {}", tid, cacheHitCount.get());
  }

  /**
   * Callback when cache miss occurs for a given table identifier.
   *
   * @param tid the table identifier
   */
  protected void onCacheMiss(TableIdentifier tid) {
    cacheMissCount.incrementAndGet();
    LOG.debug("Cache miss {}: {}", tid, cacheMissCount.get());
  }

  /**
   * Callback when cache loads a metadata table for a given table identifier.
   *
   * @param tid the table identifier
   */
  protected void onCacheMetaLoad(TableIdentifier tid) {
    cacheMetaLoadCount.incrementAndGet();
    LOG.debug("Cache meta-load {}: {}", tid, cacheMetaLoadCount.get());
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

  @Override
  public Table loadTable(final TableIdentifier identifier) {
    final TableIdentifier canonicalized = identifier.toLowerCase();
    final Table cachedTable = tableCache.getIfPresent(canonicalized);
    if (cachedTable != null) {
      final String location = new MetadataLocator(hiveCatalog).getLocation(canonicalized);
      if (location == null) {
        LOG.debug("Table {} has no location, returning cached table without location", canonicalized);
      } else {
        String cachedLocation = cachedTable instanceof HasTableOperations tableOps
                ? tableOps.operations().current().metadataFileLocation()
                : null;
        if (!location.equals(cachedLocation)) {
          LOG.debug("Invalidate table {}, cached {} != actual {}", canonicalized, cachedLocation, location);
          // Invalidate the cached table if the location is different
          invalidateTable(canonicalized);
          onCacheInvalidate(canonicalized);
        } else {
          onCacheHit(canonicalized);
          return cachedTable;
        }
      }
    } else {
      onCacheMiss(canonicalized);
    }
    final Table table = tableCache.get(canonicalized, this::loadTableWithoutCache);
    if (table instanceof BaseMetadataTable) {
      // Cache underlying table
      TableIdentifier originTableIdentifier =
              TableIdentifier.of(canonicalized.namespace().levels());
      Table originTable = tableCache.get(originTableIdentifier, this::loadTableWithoutCache);
      // Share TableOperations instance of origin table for all metadata tables, so that metadata
      // table instances are refreshed as well when origin table instance is refreshed.
      if (originTable instanceof HasTableOperations tableOps) {
        TableOperations ops = tableOps.operations();
        MetadataTableType type = MetadataTableType.from(canonicalized.name());
        Table metadataTable =
                MetadataTableUtils.createMetadataTableInstance(
                        ops, hiveCatalog.name(), originTableIdentifier, canonicalized, type);
        tableCache.put(canonicalized, metadataTable);
        onCacheMetaLoad(canonicalized);
        LOG.debug("Loaded metadata table: {} for origin table: {}", canonicalized, originTableIdentifier);
        // Return the metadata table instead of the original table
        return metadataTable;
      }
    }
    onCacheLoad(canonicalized);
    return table;
  }

  private Table loadTableWithoutCache(TableIdentifier identifier) {
    try {
      return hiveCatalog.loadTable(identifier);
    } catch (NoSuchTableException exception) {
      return null;
    }
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
