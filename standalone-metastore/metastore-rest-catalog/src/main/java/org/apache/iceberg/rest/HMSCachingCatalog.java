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
  private final HiveCatalog hiveCatalog;
  
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

  @Override
  public Table loadTable(TableIdentifier identifier) {
    final Cache<TableIdentifier, Table> cache = this.tableCache;
    final HiveCatalog catalog = this.hiveCatalog;
    final TableIdentifier canonicalized = identifier.toLowerCase();
    Table cachedTable = cache.getIfPresent(canonicalized);
    if (cachedTable != null) {
      String location = catalog.getTableLocation(canonicalized);
      if (location == null) {
        LOG.debug("Table {} has no location, returning cached table without location", canonicalized);
      } else if (!location.equals(cachedTable.location())) {
        LOG.debug("Cached table {} has a different location than the one in the catalog: {} != {}",
                 canonicalized, cachedTable.location(), location);
        // Invalidate the cached table if the location is different
        invalidateTable(canonicalized);
      } else {
        LOG.debug("Returning cached table: {}", canonicalized);
        return cachedTable;
      }
    }
    Table table = cache.get(canonicalized, catalog::loadTable);
    if (table instanceof BaseMetadataTable) {
      // Cache underlying table
      TableIdentifier originTableIdentifier =
              TableIdentifier.of(canonicalized.namespace().levels());
      Table originTable = cache.get(originTableIdentifier, catalog::loadTable);
      // Share TableOperations instance of origin table for all metadata tables, so that metadata
      // table instances are refreshed as well when origin table instance is refreshed.
      if (originTable instanceof HasTableOperations) {
        TableOperations ops = ((HasTableOperations) originTable).operations();
        MetadataTableType type = MetadataTableType.from(canonicalized.name());

        Table metadataTable =
                MetadataTableUtils.createMetadataTableInstance(
                        ops, catalog.name(), originTableIdentifier, canonicalized, type);
        cache.put(canonicalized, metadataTable);
        return metadataTable;
      }
    }
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
