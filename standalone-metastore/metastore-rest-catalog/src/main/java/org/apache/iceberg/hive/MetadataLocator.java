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

package org.apache.iceberg.hive;

import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.GetProjectionsSpec;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.client.builder.GetTableProjectionsSpecBuilder;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.thrift.TException;

/**
 * Fetches the location of a given metadata table.
 * <p>Since the location mutates with each transaction, this allows determining if a cached version of the
 * table is the latest known in the HMS database.</p>
 */
public class MetadataLocator {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(MetadataLocator.class);
  private static final GetProjectionsSpec PARAM_SPEC =
      new GetTableProjectionsSpecBuilder()
          .includeParameters() // only fetches table.parameters
          .build();
  private static final GetProjectionsSpec OWNER_AND_PARAM_SPEC =
      new GetTableProjectionsSpecBuilder()
          .includeOwner()
          .includeParameters()
          .build();
  private final HiveCatalog catalog;

  /**
   * Creates a locator that resolves metadata-file locations through {@code catalog}.
   * @param catalog the catalog to resolve metadata-file locations through
   */
  public MetadataLocator(HiveCatalog catalog) {
    this.catalog = catalog;
  }

  /**
   * Returns the catalog this locator resolves metadata-file locations through.
   * @return the catalog
   */
  public HiveCatalog getCatalog() {
    return catalog;
  }

  /**
   * Returns the current metadata-file location of the table identified by the given identifier. The
   * identifier may be either a base table (e.g. {@code db.table}) or one of its metadata tables
   * (e.g. {@code db.table.snapshots}), which is resolved to its base table before the lookup.
   * <p>This uses the Thrift API to fetch the table parameters, which is more efficient than fetching the entire table object.</p>
   * @param  identifier the base-table or metadata-table identifier to fetch the location for
   * @return the current metadata-file location, or null if the table (or its database/catalog) does
   *         not exist, or the identifier is neither a valid table nor a valid metadata-table identifier
   * @throws RuntimeException if the HMS lookup fails for any reason other than the object not existing
   */
  public String getLocation(TableIdentifier identifier) {
    final ClientPool<IMetaStoreClient, TException> clients = catalog.clientPool();
    final String catName = catalog.name();
    final TableIdentifier baseTableIdentifier;
    if (!catalog.isValidIdentifier(identifier)) {
      if (!isValidMetadataIdentifier(identifier)) {
        return null;
      } else {
        baseTableIdentifier = TableIdentifier.of(identifier.namespace().levels());
      }
    } else {
      baseTableIdentifier = identifier;
    }
    String database = baseTableIdentifier.namespace().level(0);
    String tableName = baseTableIdentifier.name();
    try {
      List<Table> tables =
          clients.run(client -> client.getTables(catName, database, Collections.singletonList(tableName), PARAM_SPEC));
      if (tables != null && !tables.isEmpty()) {
        Table table = tables.getFirst();
        if (table != null) {
          HiveOperationsBase.validateIcebergViewNotLoadedAsIcebergTable(table, baseTableIdentifier.toString());
          return table.getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
        }
      }
      return null;
    } catch (NoSuchObjectException | UnknownDBException e) {
      // HMS raises these for an unknown catalog or database. Like an empty getTables result, they mean the
      // object does not exist, so we return null and let callers treat null uniformly as not-found (matching
      // the missing-table case above).
      LOGGER.debug("Table {} not found: {}", baseTableIdentifier, e.getMessage());
      return null;
    } catch (TException e) {
      LOGGER.warn("Table {} parameters fetch failed: {}", baseTableIdentifier, e.getMessage());
      throw new RuntimeException("Failed to fetch table parameters for " + baseTableIdentifier, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while fetching table parameters for " + baseTableIdentifier, e);
    }
  }

  /**
   * Returns the owner and parameters HMS records for a view or table, the state HMS keeps outside the Iceberg
   * metadata. Like {@link #getLocation(TableIdentifier)}, this fetches only those fields rather than the
   * entire object; the parameters include the metadata location the state belongs to.
   * @param identifier the view or table identifier
   * @return an HMS table holding only the owner and parameters, or null if the object (or its database/catalog)
   *         does not exist
   * @throws RuntimeException if the HMS lookup fails for any reason other than the object not existing
   */
  public Table getOwnerAndParameters(TableIdentifier identifier) {
    final ClientPool<IMetaStoreClient, TException> clients = catalog.clientPool();
    final String catName = catalog.name();
    String database = identifier.namespace().level(0);
    String tableName = identifier.name();
    try {
      List<Table> tables = clients.run(client ->
          client.getTables(catName, database, Collections.singletonList(tableName), OWNER_AND_PARAM_SPEC));
      return tables == null || tables.isEmpty() ? null : tables.getFirst();
    } catch (NoSuchObjectException | UnknownDBException e) {
      LOGGER.debug("Object {} not found: {}", identifier, e.getMessage());
      return null;
    } catch (TException e) {
      LOGGER.warn("Object {} owner and parameters fetch failed: {}", identifier, e.getMessage());
      throw new RuntimeException("Failed to fetch owner and parameters for " + identifier, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted while fetching owner and parameters for " + identifier, e);
    }
  }

  private boolean isValidMetadataIdentifier(TableIdentifier identifier) {
    return MetadataTableType.from(identifier.name()) != null
        && catalog.isValidIdentifier(TableIdentifier.of(identifier.namespace().levels()));
  }
}
