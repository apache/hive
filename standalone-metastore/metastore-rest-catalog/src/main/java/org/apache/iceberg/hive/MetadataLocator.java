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

package org.apache.iceberg.hive;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.GetProjectionsSpec;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.client.builder.GetTableProjectionsSpecBuilder;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.ClientPool;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.thrift.TException;

import java.util.Collections;
import java.util.List;

/**
 * Fetches the location of a given metadata table.
 * <p>Since the location mutates with each transaction, this allows determining if a cached version of the
 * table is the latest known in the HMS database.</p>
 */
public class MetadataLocator {
  private static final org.slf4j.Logger LOGGER = org.slf4j.LoggerFactory.getLogger(MetadataLocator.class);
  static final GetProjectionsSpec PARAM_SPEC = new GetTableProjectionsSpecBuilder()
      .includeParameters()  // only fetches table.parameters
      .build();
  private final HiveCatalog catalog;

  public MetadataLocator(HiveCatalog catalog) {
    this.catalog = catalog;
  }

  public HiveCatalog getCatalog() {
    return catalog;
  }

  /**
   * Returns the location of the metadata table identified by the given identifier, or null if the table does not exist or is
   * not a metadata table.
   * <p>This uses the Thrift API to fetch the table parameters, which is more efficient than fetching the entire table object.</p>
   * @param  identifier the identifier of the metadata table to fetch the location for
   * @return the location of the metadata table, or null if the table does not exist or is not a metadata table
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
      List<Table> tables = clients.run(
          client -> client.getTables(catName, database, Collections.singletonList(tableName), PARAM_SPEC)
      );
      return tables == null || tables.isEmpty()
          ? null
          : tables.getFirst().getParameters().get(BaseMetastoreTableOperations.METADATA_LOCATION_PROP);
    } catch (NoSuchTableException | NoSuchObjectException e) {
      LOGGER.info("Table not found {}", baseTableIdentifier, e);
    } catch (TException e) {
      LOGGER.info("Table parameters fetch failed {}", baseTableIdentifier, e);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.error("Interrupted in call to check table existence of {}", baseTableIdentifier, e);
    }
    return null;
  }

  private boolean isValidMetadataIdentifier(TableIdentifier identifier) {
    return MetadataTableType.from(identifier.name()) != null
        && catalog.isValidIdentifier(TableIdentifier.of(identifier.namespace().levels()));
  }
}
