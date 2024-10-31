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

package org.apache.iceberg;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Ticker;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * Class that wraps an Iceberg Catalog to cache tables.
 * Initial code in:
 * https://github.com/apache/iceberg/blob/1.3.x/core/src/main/java/org/apache/iceberg/CachingCatalog.java
 * Main difference is the SupportsNamespace and the fact that loadTable performs a metadata refresh.
 *
 * <p>See {@link CatalogProperties#CACHE_EXPIRATION_INTERVAL_MS} for more details regarding special
 * values for {@code expirationIntervalMillis}.
 */
public class HiveCachingCatalog<CATALOG extends Catalog & SupportsNamespaces> implements Catalog, SupportsNamespaces {
  private static final Logger LOG = LoggerFactory.getLogger(HiveCachingCatalog.class);
  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected final long expirationIntervalMillis;
  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected final Cache<TableIdentifier, Table> tableCache;
  private final CATALOG catalog;
  private final boolean caseSensitive;

  @SuppressWarnings("checkstyle:VisibilityModifier")
  protected HiveCachingCatalog(CATALOG catalog, long expirationIntervalMillis) {
    Preconditions.checkArgument(
        expirationIntervalMillis != 0,
        "When %s is set to 0, the catalog cache should be disabled. This indicates a bug.",
        CatalogProperties.CACHE_EXPIRATION_INTERVAL_MS);
    this.catalog = catalog;
    this.caseSensitive = true;
    this.expirationIntervalMillis = expirationIntervalMillis;
    this.tableCache = createTableCache(Ticker.systemTicker());
  }

  public CATALOG unwrap() {
    return catalog;
  }

  public static <C extends Catalog & SupportsNamespaces>
  HiveCachingCatalog<C> wrap(C catalog, long expirationIntervalMillis) {
    return new HiveCachingCatalog(catalog, expirationIntervalMillis);
  }


  private Cache<TableIdentifier, Table> createTableCache(Ticker ticker) {
    Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder().softValues();
    if (expirationIntervalMillis > 0) {
      return cacheBuilder
          .removalListener(new MetadataTableInvalidatingRemovalListener())
          .executor(Runnable::run) // Makes the callbacks to removal listener synchronous
          .expireAfterAccess(Duration.ofMillis(expirationIntervalMillis))
          .ticker(ticker)
          .build();
    }
    return cacheBuilder.build();
  }

  private TableIdentifier canonicalizeIdentifier(TableIdentifier tableIdentifier) {
    return caseSensitive ? tableIdentifier : tableIdentifier.toLowerCase();
  }

  @Override
  public String name() {
    return catalog.name();
  }

  @Override
  public List<TableIdentifier> listTables(Namespace namespace) {
    return catalog.listTables(namespace);
  }

  private Table loadTableHive(TableIdentifier ident) {
    return catalog.loadTable(ident);
  }

  @Override
  public Table loadTable(TableIdentifier ident) {
    TableIdentifier canonicalized = canonicalizeIdentifier(ident);
    Table cached = tableCache.getIfPresent(canonicalized);
    if (cached != null) {
      return cached;
    }

    if (MetadataTableUtils.hasMetadataTableName(canonicalized)) {
      TableIdentifier originTableIdentifier =
          TableIdentifier.of(canonicalized.namespace().levels());
      Table originTable = tableCache.get(originTableIdentifier, this::loadTableHive);

      // share TableOperations instance of origin table for all metadata tables, so that metadata
      // table instances are
      // also refreshed as well when origin table instance is refreshed.
      if (originTable instanceof HasTableOperations) {
        TableOperations ops = ((HasTableOperations) originTable).operations();
        MetadataTableType type = MetadataTableType.from(canonicalized.name());
        Table metadataTable =
            MetadataTableUtils.createMetadataTableInstance(
                ops, catalog.name(), originTableIdentifier, canonicalized, type);
        tableCache.put(canonicalized, metadataTable);
        return metadataTable;
      }
    }
    return tableCache.get(canonicalized, this::loadTableHive);
  }

  @Override
  public boolean dropTable(TableIdentifier ident, boolean purge) {
    boolean dropped = catalog.dropTable(ident, purge);
    invalidateTable(ident);
    return dropped;
  }

  @Override
  public void renameTable(TableIdentifier from, TableIdentifier to) {
    catalog.renameTable(from, to);
    invalidateTable(from);
  }

  @Override
  public void invalidateTable(TableIdentifier ident) {
    catalog.invalidateTable(ident);
    TableIdentifier canonicalized = canonicalizeIdentifier(ident);
    tableCache.invalidate(canonicalized);
    tableCache.invalidateAll(metadataTableIdentifiers(canonicalized));
  }

  @Override
  public Table registerTable(TableIdentifier identifier, String metadataFileLocation) {
    Table table = catalog.registerTable(identifier, metadataFileLocation);
    invalidateTable(identifier);
    return table;
  }

  private Iterable<TableIdentifier> metadataTableIdentifiers(TableIdentifier ident) {
    ImmutableList.Builder<TableIdentifier> builder = ImmutableList.builder();

    for (MetadataTableType type : MetadataTableType.values()) {
      // metadata table resolution is case insensitive right now
      builder.add(TableIdentifier.parse(ident + "." + type.name()));
      builder.add(TableIdentifier.parse(ident + "." + type.name().toLowerCase(Locale.ROOT)));
    }

    return builder.build();
  }

  @Override
  public TableBuilder buildTable(TableIdentifier identifier, Schema schema) {
    return new CachingTableBuilder(identifier, schema);
  }

  @Override
  public void createNamespace(Namespace nmspc, Map<String, String> map) {
    catalog.createNamespace(nmspc, map);
  }

  @Override
  public List<Namespace> listNamespaces(Namespace nmspc) throws NoSuchNamespaceException {
    return catalog.listNamespaces(nmspc);
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace nmspc) throws NoSuchNamespaceException {
    return catalog.loadNamespaceMetadata(nmspc);
  }

  @Override
  public boolean dropNamespace(Namespace nmspc) throws NamespaceNotEmptyException {
    List<TableIdentifier> tables = listTables(nmspc);
    for (TableIdentifier ident : tables) {
      TableIdentifier canonicalized = canonicalizeIdentifier(ident);
      tableCache.invalidate(canonicalized);
      tableCache.invalidateAll(metadataTableIdentifiers(canonicalized));
    }
    return catalog.dropNamespace(nmspc);
  }

  @Override
  public boolean setProperties(Namespace nmspc, Map<String, String> map) throws NoSuchNamespaceException {
    return catalog.setProperties(nmspc, map);
  }

  @Override
  public boolean removeProperties(Namespace nmspc, Set<String> set) throws NoSuchNamespaceException {
    return catalog.removeProperties(nmspc, set);
  }

  /**
   * RemovalListener class for removing metadata tables when their associated data table is expired
   * via cache expiration.
   */
  class MetadataTableInvalidatingRemovalListener
      implements RemovalListener<TableIdentifier, Table> {
    @Override
    public void onRemoval(TableIdentifier tableIdentifier, Table table, RemovalCause cause) {
      LOG.debug("Evicted {} from the table cache ({})", tableIdentifier, cause);
      if (RemovalCause.EXPIRED.equals(cause)) {
        if (!MetadataTableUtils.hasMetadataTableName(tableIdentifier)) {
          tableCache.invalidateAll(metadataTableIdentifiers(tableIdentifier));
        }
      }
    }
  }

  private class CachingTableBuilder implements TableBuilder {
    private final TableIdentifier ident;
    private final TableBuilder innerBuilder;

    private CachingTableBuilder(TableIdentifier identifier, Schema schema) {
      this.innerBuilder = catalog.buildTable(identifier, schema);
      this.ident = identifier;
    }

    @Override
    public TableBuilder withPartitionSpec(PartitionSpec spec) {
      innerBuilder.withPartitionSpec(spec);
      return this;
    }

    @Override
    public TableBuilder withSortOrder(SortOrder sortOrder) {
      innerBuilder.withSortOrder(sortOrder);
      return this;
    }

    @Override
    public TableBuilder withLocation(String location) {
      innerBuilder.withLocation(location);
      return this;
    }

    @Override
    public TableBuilder withProperties(Map<String, String> properties) {
      innerBuilder.withProperties(properties);
      return this;
    }

    @Override
    public TableBuilder withProperty(String key, String value) {
      innerBuilder.withProperty(key, value);
      return this;
    }

    @Override
    public Table create() {
      AtomicBoolean created = new AtomicBoolean(false);
      Table table =
          tableCache.get(
              canonicalizeIdentifier(ident),
              identifier -> {
                created.set(true);
                return innerBuilder.create();
              });
      if (!created.get()) {
        throw new AlreadyExistsException("Table already exists: %s", ident);
      }
      return table;
    }

    @Override
    public Transaction createTransaction() {
      // create a new transaction without altering the cache. the table doesn't exist until the
      // transaction is
      // committed. if the table is created before the transaction commits, any cached version is
      // correct and the
      // transaction create will fail. if the transaction commits before another create, then the
      // cache will be empty.
      return innerBuilder.createTransaction();
    }

    @Override
    public Transaction replaceTransaction() {
      // create a new transaction without altering the cache. the table doesn't change until the
      // transaction is
      // committed. when the transaction commits, invalidate the table in the cache if it is
      // present.
      return CommitCallbackTransaction.addCallback(
          innerBuilder.replaceTransaction(), () -> invalidateTable(ident));
    }

    @Override
    public Transaction createOrReplaceTransaction() {
      // create a new transaction without altering the cache. the table doesn't change until the
      // transaction is
      // committed. when the transaction commits, invalidate the table in the cache if it is
      // present.
      return CommitCallbackTransaction.addCallback(
          innerBuilder.createOrReplaceTransaction(), () -> invalidateTable(ident));
    }
  }
}
