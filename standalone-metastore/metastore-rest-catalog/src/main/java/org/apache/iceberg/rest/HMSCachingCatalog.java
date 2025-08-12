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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Ticker;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.BaseMetadataTable;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.view.View;
import org.apache.iceberg.view.ViewBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Class that wraps an Iceberg Catalog to cache tables.
 */
public class HMSCachingCatalog extends CachingCatalog implements SupportsNamespaces, ViewCatalog {
  private static final Logger LOG = LoggerFactory.getLogger(HMSCachingCatalog.class);
  protected final HiveCatalog hiveCatalog;
  
  public HMSCachingCatalog(HiveCatalog catalog, long expiration) {
    super(catalog, false, expiration, Ticker.systemTicker());
    this.hiveCatalog = catalog;
  }

  @Override
  public void createNamespace(Namespace nmspc, Map<String, String> map) {
    hiveCatalog.createNamespace(nmspc, map);
  }

  @Override
  public List<Namespace> listNamespaces(Namespace nmspc) throws NoSuchNamespaceException {
    return hiveCatalog.listNamespaces(nmspc);
  }

  protected void cacheInvalidateInc(TableIdentifier tid) {
    // This method is intentionally left empty. It can be overridden in subclasses if needed.
  }

  protected void cacheLoadInc(TableIdentifier tid) {
    // This method is intentionally left empty. It can be overridden in subclasses if needed.
  }

  protected void cacheHitInc(TableIdentifier tid) {
    // This method is intentionally left empty. It can be overridden in subclasses if needed.
  }

  protected void cacheMissInc(TableIdentifier tid) {
    // This method is intentionally left empty. It can be overridden in subclasses if needed.
  }

  protected void cacheMetaLoadInc(TableIdentifier tid) {
    // This method is intentionally left empty. It can be overridden in subclasses if needed.
  }

  /**
   * Gets the metadata file location of a table.
   *
   * @param table the table
   * @return the location of the metadata file, or null if the table does not have a location
   */
  protected static String getMetadataLocation(final Table table) {
    if (table instanceof HasTableOperations tableOps) {
      final TableOperations ops = tableOps.operations();
      final TableMetadata meta;
      if (ops != null && (meta = ops.current()) != null) {
        return meta.metadataFileLocation();
      }
    }
    return null;
  }

  @Override
  public Table loadTable(final TableIdentifier identifier) {
    final Cache<TableIdentifier, Table> cache = this.tableCache;
    final HiveCatalog catalog = this.hiveCatalog;
    final TableIdentifier canonicalized = identifier.toLowerCase();
    Table cachedTable = cache.getIfPresent(canonicalized);
    if (cachedTable != null) {
      final String location = catalog.getTableMetadataLocation(canonicalized);
      if (location == null) {
        LOG.debug("Table {} has no location, returning cached table without location", canonicalized);
      } else {
        String cachedLocation = getMetadataLocation(cachedTable);
        if (!location.equals(cachedLocation)) {
          LOG.debug("Invalidate table {}, cached location {} != actual location {}", canonicalized, cachedLocation, location);
          // Invalidate the cached table if the location is different
          invalidateTable(canonicalized);
          cacheInvalidateInc(canonicalized);
        } else {
          LOG.debug("Returning cached table: {}", canonicalized);
          cacheHitInc(canonicalized);
          return cachedTable;
        }
      }
    } else {
      LOG.debug("Cache miss for table: {}", canonicalized);
      cacheMissInc(canonicalized);
    }
    Table table = cache.get(canonicalized, catalog::loadTable);
    if (table instanceof BaseMetadataTable) {
      // Cache underlying table
      TableIdentifier originTableIdentifier =
              TableIdentifier.of(canonicalized.namespace().levels());
      Table originTable = cache.get(originTableIdentifier, catalog::loadTable);
      // Share TableOperations instance of origin table for all metadata tables, so that metadata
      // table instances are refreshed as well when origin table instance is refreshed.
      if (originTable instanceof HasTableOperations originTableOps) {
        TableOperations ops = originTableOps.operations();
        MetadataTableType type = MetadataTableType.from(canonicalized.name());
        Table metadataTable =
                MetadataTableUtils.createMetadataTableInstance(
                        ops, catalog.name(), originTableIdentifier, canonicalized, type);
        cache.put(canonicalized, metadataTable);
        cacheMetaLoadInc(canonicalized);
        LOG.debug("Loaded metadata table: {} for origin table: {}", canonicalized, originTableIdentifier);
        // Return the metadata table instead of the original table
        return metadataTable;
      }
    }
    cacheLoadInc(canonicalized);
    LOG.debug("Loaded table: {} ", canonicalized);
    return table;
  }

  @Override
  public Map<String, String> loadNamespaceMetadata(Namespace nmspc) throws NoSuchNamespaceException {
    return hiveCatalog.loadNamespaceMetadata(nmspc);
  }

  @Override
  public boolean dropNamespace(Namespace nmspc) throws NamespaceNotEmptyException {
    List<TableIdentifier> tables = listTables(nmspc);
    for (TableIdentifier ident : tables) {
      invalidateTable(ident);
    }
    return hiveCatalog.dropNamespace(nmspc);
  }

  @Override
  public boolean setProperties(Namespace nmspc, Map<String, String> map) throws NoSuchNamespaceException {
    return hiveCatalog.setProperties(nmspc, map);
  }

  @Override
  public boolean removeProperties(Namespace nmspc, Set<String> set) throws NoSuchNamespaceException {
    return hiveCatalog.removeProperties(nmspc, set);
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
