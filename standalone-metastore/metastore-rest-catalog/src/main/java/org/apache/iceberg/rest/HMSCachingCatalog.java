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

package org.apache.iceberg.rest;

import java.io.Closeable;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Ticker;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetadataTable;
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
 * Caching wrapper around a {@link HiveCatalog} that adds two-level table caching.
 *
 * <h2>Table caching (L2 + L1)</h2>
 * <p><b>L2 — Caffeine cache.</b> The primary table store. Each {@link Table} object is keyed by
 * its {@link TableIdentifier} and expires after the configured inactivity period
 * ({@code ICEBERG_CATALOG_CACHE_EXPIRY}, in milliseconds). On a cache miss, the table is loaded
 * from the underlying {@link HiveCatalog} and its current metadata location is recorded.
 * Subsequent hits skip the HMS round-trip entirely.</p>
 *
 * <p><b>L1 — LinkedHashMap recency guard.</b> A small bounded, access-ordered (LRU) map (default
 * 32 entries, 3 s TTL; configurable via {@code hms.caching.catalog.l1.cache.size} and
 * {@code hms.caching.catalog.l1.cache.ttl}) that tracks when each L2-cached table was last
 * confirmed fresh; when it overflows, the least-recently-used entry is evicted. While the L1 entry is live, {@code loadTable} skips the metadata-location
 * staleness check against HMS. Once the L1 entry expires, the next call re-validates the stored
 * metadata location; if it has changed, the L2 entry is evicted ({@code onCacheInvalidate}) and
 * a fresh load is performed. The L1 layer trades a small risk of serving a stale snapshot for a
 * large reduction in HMS round-trips under repeated access to the same table.</p>
 *
 * <p>Both cache levels are invalidated together by {@link #invalidateTable(TableIdentifier)},
 * which also evicts all derived {@link org.apache.iceberg.MetadataTableType metadata-table}
 * entries that share the base identifier.</p>
 *
 * <h2>Observability</h2>
 * <p>This class implements {@link HMSCachingCatalogMXBean} and registers itself with the platform
 * MBean server under the name {@code org.apache.iceberg.rest:type=HMSCachingCatalog,name=&lt;catalogName&gt;}
 * so that cache hit/miss counts and invalidation counts can be monitored via JMX. The
 * {@code catalogName} is {@link org.apache.iceberg.catalog.Catalog#name()} of the wrapped catalog
 * (the metastore's configured default catalog name), sanitized for use in an {@link ObjectName}.</p>
 */
public final class HMSCachingCatalog
    implements Catalog, SupportsNamespaces, ViewCatalog, HMSCachingCatalogMXBean, Closeable {
  private static final Logger LOG = LoggerFactory.getLogger(HMSCachingCatalog.class);

  /**
   * Returns the underlying {@link HiveCatalog} that this caching catalog wraps.
   * This is intended for testing purposes only; production code should not rely on the underlying catalog.
   */
  @TestOnly
  public HiveCatalog getCatalog() {
    return hiveCatalog;
  }

  // The underlying HiveCatalog that this caching catalog wraps.
  private final HiveCatalog hiveCatalog;
  // Authorizes reads served from the cache. A cache hit never reaches Hive Metastore, so its read
  // authorization cannot be deferred to HMS and is enforced here instead. May be null (no
  // authorization), in which case cache hits are served without a check.
  private final IcebergAuthorizer authorizer;
  // A helper that locates the metadata location for a given base table identifier.
  private final MetadataLocator metadataLocator;
  // An L2 table cache (Caffeine).
  private final Cache<TableIdentifier, Table> tableCache;
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
  private final LongAdder cacheHitCount = new LongAdder();
  private final LongAdder cacheMissCount = new LongAdder();
  private final LongAdder cacheLoadCount = new LongAdder();
  private final LongAdder cacheInvalidateCount = new LongAdder();
  private final LongAdder cacheMetaLoadCount = new LongAdder();
  // L1 cache metrics: counted only when the L2 (Caffeine) cache already has the entry.
  private final LongAdder l1CacheHitCount = new LongAdder();
  private final LongAdder l1CacheMissCount = new LongAdder();
  // JMX ObjectName under which this instance is registered (may be null if registration failed).
  private ObjectName jmxObjectName;

  /**
   * Creates a new caching catalog that wraps the given HiveCatalog, without cache-hit
   * authorization.
   * @param catalog the underlying HiveCatalog
   * @param expirationMs the expiration time for the L2 cache, in milliseconds
   */
  public HMSCachingCatalog(HiveCatalog catalog, long expirationMs) {
    this(catalog, expirationMs, null);
  }

  /**
   * Creates a new caching catalog that wraps the given HiveCatalog.
   * @param catalog the underlying HiveCatalog
   * @param expirationMs the expiration time for the L2 cache, in milliseconds
   * @param authorizer authorizes reads served from the cache; may be null for no authorization.
   *                   A cache hit does not reach Hive Metastore, so read authorization for it is
   *                   enforced here rather than deferred to HMS. Cache misses reload through the
   *                   underlying {@link HiveCatalog} and are authorized by HMS as usual.
   */
  public HMSCachingCatalog(HiveCatalog catalog, long expirationMs, IcebergAuthorizer authorizer) {
    this.hiveCatalog = catalog;
    this.authorizer = authorizer;
    this.metadataLocator = new MetadataLocator(catalog);
    this.tableCache = Caffeine.newBuilder()
        .expireAfterAccess(expirationMs, TimeUnit.MILLISECONDS)
        .ticker(Ticker.systemTicker())
        .build();
    Configuration conf = catalog.getConf();
    int l1size = conf.getInt("hms.caching.catalog.l1.cache.size", 32);
    int l1ttl = conf.getInt("hms.caching.catalog.l1.cache.ttl", 3_000);
    if (l1size > 0 && l1ttl > 0) {
      // Access-ordered (LRU) so that re-confirming a hot table via l1MarkFresh (a put on an
      // existing key) moves it to the tail; the eldest evicted by removeEldestEntry is then the
      // least-recently-used entry rather than the least-recently-inserted one.
      l1Cache = Collections.synchronizedMap(new LinkedHashMap<TableIdentifier, Long>(l1size, 0.75f, true) {
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
    // Register this instance as a JMX MBean for monitoring. The catalog was initialized with the
    // metastore's CATALOG_DEFAULT name (see HMSCatalogFactory), so catalog.name() already yields
    // that value; using it directly keeps this class free of a Configuration/MetastoreConf
    // dependency and reflects the actual identity of the wrapped catalog.
    registerJmx(catalog.name());
  }

  /**
   * Registers this instance as a JMX MBean.
   *
   * @param catalogName the catalog name, used to build the {@link ObjectName}
   */
  private void registerJmx(String catalogName) {
    try {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      String sanitized = catalogName == null || catalogName.isEmpty()
        ? "default"
        : catalogName.replaceAll("[^a-zA-Z0-9.\\\\-]", "_");
      ObjectName name = new ObjectName("org.apache.iceberg.rest:type=HMSCachingCatalog,name=" + sanitized);
      if (mbs.isRegistered(name)) {
        mbs.unregisterMBean(name);
      }
      mbs.registerMBean(this, name);
      this.jmxObjectName = name;
      LOG.info("Registered JMX MBean: {}", name);
    } catch (JMException e) {
      LOG.error("Failed to register JMX MBean for HMSCachingCatalog", e);
    }
  }

  /**
   * Callback when cache invalidates the entry for a given table identifier.
   *
   * @param tid the table identifier to invalidate
   */
  private void onCacheInvalidate(TableIdentifier tid) {
    cacheInvalidateCount.increment();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cache invalidate {}: {}", tid, cacheInvalidateCount.sum());
    }
  }

  /**
   * Callback when cache loads a table for a given table identifier.
   *
   * @param tid the table identifier
   */
  private void onCacheLoad(TableIdentifier tid) {
    cacheLoadCount.increment();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cache load {}: {}", tid, cacheLoadCount.sum());
    }
  }

  /**
   * Callback when cache hit for a given table identifier.
   *
   * @param tid the table identifier
   */
  private void onCacheHit(TableIdentifier tid) {
    cacheHitCount.increment();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cache hit {} : {}", tid, cacheHitCount.sum());
    }
  }

  /**
   * Callback when cache miss occurs for a given table identifier.
   *
   * @param tid the table identifier
   */
  private void onCacheMiss(TableIdentifier tid) {
    cacheMissCount.increment();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cache miss {}: {}", tid, cacheMissCount.sum());
    }
  }

  /**
   * Callback when cache loads a metadata table for a given table identifier.
   *
   * @param tid the table identifier
   */
  private void onCacheMetaLoad(TableIdentifier tid) {
    cacheMetaLoadCount.increment();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Cache meta-load {}: {}", tid, cacheMetaLoadCount.sum());
    }
  }

  /**
   * Callback when an L1 cache hit occurs for a given table identifier.
   * Only fired when the L2 cache also has the entry.
   *
   * @param tid the table identifier
   */
  private void onL1CacheHit(TableIdentifier tid) {
    l1CacheHitCount.increment();
    if (LOG.isDebugEnabled()) {
      LOG.debug("L1 cache hit {}: {}", tid, l1CacheHitCount.sum());
    }
  }

  /**
   * Callback when an L1 cache miss occurs for a given table identifier.
   * Only fired when the L2 cache has the entry but L1 is absent or expired.
   *
   * @param tid the table identifier
   */
  private void onL1CacheMiss(TableIdentifier tid) {
    l1CacheMissCount.increment();
    if (LOG.isDebugEnabled()) {
      LOG.debug("L1 cache miss {}: {}", tid, l1CacheMissCount.sum());
    }
  }

  // Getter methods for accessing metrics
  @Override
  public long getCacheHitCount() {
    return cacheHitCount.sum();
  }

  @Override
  public long getCacheMissCount() {
    return cacheMissCount.sum();
  }

  @Override
  public long getCacheLoadCount() {
    return cacheLoadCount.sum();
  }

  @Override
  public long getCacheInvalidateCount() {
    return cacheInvalidateCount.sum();
  }

  @Override
  public long getCacheMetaLoadCount() {
    return cacheMetaLoadCount.sum();
  }

  @Override
  public double getCacheHitRate() {
    long hits = cacheHitCount.sum();
    long total = hits + cacheMissCount.sum();
    return total == 0 ? 0.0 : (double) hits / total;
  }

  @Override
  public long getL1CacheHitCount() {
    return l1CacheHitCount.sum();
  }

  @Override
  public long getL1CacheMissCount() {
    return l1CacheMissCount.sum();
  }

  @Override
  public double getL1CacheHitRate() {
    long hits = l1CacheHitCount.sum();
    long total = hits + l1CacheMissCount.sum();
    return total == 0 ? 0.0 : (double) hits / total;
  }

  @Override
  public void resetCacheStats() {
    cacheHitCount.reset();
    cacheMissCount.reset();
    cacheLoadCount.reset();
    cacheInvalidateCount.reset();
    cacheMetaLoadCount.reset();
    l1CacheHitCount.reset();
    l1CacheMissCount.reset();
    LOG.debug("Cache stats reset");
  }

  @Override
  public void close() {
    unregisterJmx();
  }

  /**
   * Unregisters this instance from the platform MBeanServer.
   */
  private void unregisterJmx() {
    if (jmxObjectName != null) {
      try {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        if (mbs.isRegistered(jmxObjectName)) {
          mbs.unregisterMBean(jmxObjectName);
          LOG.info("Unregistered JMX MBean: {}", jmxObjectName);
        }
      } catch (JMException e) {
        LOG.warn("Failed to unregister JMX MBean: {}", jmxObjectName, e);
      } finally {
        jmxObjectName = null;
      }
    }
  }

  @Override
  public String name() {
    return hiveCatalog.name();
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    return hiveCatalog.listTables(namespace);
  }

  @Override
  public boolean dropTable(TableIdentifier identifier, boolean purge) {
    boolean dropped = hiveCatalog.dropTable(identifier, purge);
    invalidateTable(identifier);
    return dropped;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    hiveCatalog.renameTable(from, to);
    invalidateTable(from);
  }

  @Override
  public Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
    Table registered = hiveCatalog.registerTable(identifier, metadataFileLocation);
    invalidateTable(identifier);
    return registered;
  }

  @Override
  public void invalidateTable(TableIdentifier ident) {
    hiveCatalog.invalidateTable(ident);
    tableCache.invalidate(ident);
    tableCache.invalidateAll(metadataTableIdentifiers(ident));
    l1Invalidate(ident);
  }

  /**
   * Records {@code now} as the last time the given identifier was confirmed fresh in the L1
   * recency guard. No-op when L1 is disabled: in that case {@link #l1Cache} is an immutable empty
   * map, so writing to it would throw {@link UnsupportedOperationException}.
   */
  private void l1MarkFresh(TableIdentifier ident, long now) {
    if (l1Ttl > 0) {
      l1Cache.put(ident, now);
    }
  }

  /** Evicts the given identifier from the L1 recency guard. No-op when L1 is disabled. */
  private void l1Invalidate(TableIdentifier ident) {
    if (l1Ttl > 0) {
      l1Cache.remove(ident);
    }
  }

  /**
   * Returns the identifiers of all metadata tables derived from the given base table identifier,
   * in both upper-case and lower-case type-name forms so that eviction covers both variants.
   */
  private List<TableIdentifier> metadataTableIdentifiers(TableIdentifier identifier) {
    MetadataTableType[] types = MetadataTableType.values();
    List<TableIdentifier> result = new ArrayList<>(types.length * 2);
    for (MetadataTableType type : types) {
      result.add(TableIdentifier.parse(identifier + "." + type.name()));
      result.add(TableIdentifier.parse(identifier + "." + type.name().toLowerCase(Locale.ROOT)));
    }
    return result;
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
  public void invalidateView(TableIdentifier identifier) {
    hiveCatalog.invalidateView(identifier);
  }

  /**
   * Authorizes a read that is about to be served from the cache. A cache hit never reaches Hive
   * Metastore, so its read authorization cannot be deferred to the HMS pre-event listener as a cache
   * miss's can, and must be enforced here. No-op when no authorizer is configured.
   *
   * @param identifier the table (or metadata-table) identifier being read
   * @throws org.apache.iceberg.exceptions.ForbiddenException if the current user may not read the table
   */
  private void authorizeCachedRead(TableIdentifier identifier) {
    if (authorizer != null) {
      authorizer.authorizeLoadTable(hiveCatalog.name(), identifier);
    }
  }

  @Override
  public Table loadTable(final TableIdentifier identifier) {
    final Table cachedTable = tableCache.getIfPresent(identifier);
    long now = System.currentTimeMillis();
    if (cachedTable != null) {
      // Determine if L1 cache is valid based on the last cached time and the TTL.
      // If the table is in L1 cache, we can skip the location check and return the cached table directly,
      // which can significantly reduce the latency for repeated access to the same table.
      Long lastCached = l1Cache.get(identifier);
      if (lastCached != null) {
        if (now - lastCached < l1Ttl) {
          LOG.debug("Table {} is in L1 cache, returning cached table", identifier);
          onL1CacheHit(identifier);
          onCacheHit(identifier);
          authorizeCachedRead(identifier);
          return cachedTable;
        } else {
          l1Invalidate(identifier);
          onL1CacheMiss(identifier);
        }
      } else {
        onL1CacheMiss(identifier);
      }
      // If the table is no longer in L1 cache, we need to check the location.
      final String location = metadataLocator.getLocation(identifier);
      if (location == null) {
        // A null location means the table no longer exists in HMS. The cached instance is stale and
        // its metadata/manifests are highly likely to be deleted, so we must not serve it: evict the
        // entry and signal not-found rather than returning a ghost table.
        LOG.debug("Table {} no longer exists in HMS, evicting stale cache entry", identifier);
        invalidateTable(identifier);
        throw new NoSuchTableException("Table does not exist: %s", identifier);
      }
      String cachedLocation =
          cachedTable instanceof HasTableOperations tableOps ? tableOps.operations().current().metadataFileLocation() : null;
      if (location.equals(cachedLocation)) {
        onCacheHit(identifier);
        l1MarkFresh(identifier, now);
        authorizeCachedRead(identifier);
        return cachedTable;
      } else {
        LOG.debug("Invalidate table {}, cached {} != actual {}", identifier, cachedLocation, location);
        // Invalidate the cached table if the location is different
        invalidateTable(identifier);
        onCacheInvalidate(identifier);
      }
    } else {
      onCacheMiss(identifier);
    }
    final Table table = tableCache.get(identifier, this::loadTableWithoutCache);
    if (table instanceof BaseMetadataTable) {
      // Cache underlying table: there must be a table named by the namespace (?)
      TableIdentifier originTableIdentifier = TableIdentifier.of(identifier.namespace().levels());
      Table originTable = tableCache.get(originTableIdentifier, this::loadTableWithoutCache);
      // Share TableOperations instance of origin table for all metadata tables, so that metadata
      // table instances are refreshed as well when origin table instance is refreshed.
      if (originTable instanceof HasTableOperations tableOps) {
        TableOperations ops = tableOps.operations();
        MetadataTableType type = MetadataTableType.from(identifier.name());
        // Defensive: MetadataTableType.from may return null for unknown names
        if (type != null) {
          Table metadataTable =
              MetadataTableUtils.createMetadataTableInstance(ops, hiveCatalog.name(), originTableIdentifier, identifier, type);
          tableCache.put(identifier, metadataTable);
          l1MarkFresh(identifier, now);
          onCacheMetaLoad(identifier);
          LOG.debug("Loaded metadata table: {} for origin table: {}", identifier, originTableIdentifier);
          // Return the metadata table instead of the original table
          return metadataTable;
        }
      }
    }
    l1MarkFresh(identifier, now);
    onCacheLoad(identifier);
    return table;
  }

  @Override
  public boolean tableExists(TableIdentifier identifier) {
    return metadataLocator.getLocation(identifier) != null;
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
    for (TableIdentifier ident : hiveCatalog.listTables(namespace)) {
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
  public void initialize(String name, Map<String, String> properties) {
    hiveCatalog.initialize(name, properties);
  }
}
