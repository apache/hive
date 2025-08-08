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
package org.apache.hadoop.hive.metastore.utils;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableIterable;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class TableFetcher {
  private static final Logger LOG = LoggerFactory.getLogger(TableFetcher.class);

  // mandatory client passed to this fetcher, has to be closed from caller
  private final IMetaStoreClient client;
  // mandatory catalogName
  private final String catalogName;
  // mandatory dbPattern: use "*" to fetch all, empty to fetch none
  private final String dbPattern;
  // optional tableTypes: comma separated table types to fetch, fetcher result is empty list if this is empty
  // typical value: "MANAGED_TABLE,EXTERNAL_TABLE"
  @VisibleForTesting
  final Set<String> tableTypes = new HashSet<>();
  // tableFilter built from the input tablePattern and custom table conditions
  @VisibleForTesting
  String tableFilter;

  private TableFetcher(Builder builder) {
    this.client = builder.client;
    if ("*".equalsIgnoreCase(builder.catalogName)) {
      LOG.warn("Invalid wildcard '*' parameter for catalogName, exact catalog name is expected instead of regexp");
    }
    this.catalogName = Optional.ofNullable(builder.catalogName).orElse(Warehouse.DEFAULT_CATALOG_NAME);
    this.dbPattern = Optional.ofNullable(builder.dbPattern).orElse("");
    String tablePattern = Optional.ofNullable(builder.tablePattern).orElse("");
    String stringTableTypes = Optional.ofNullable(builder.tableTypes).orElse("");

    for (String type : stringTableTypes.split(",")) {
      try {
        tableTypes.add(TableType.valueOf(type.trim().toUpperCase()).name());
      } catch (IllegalArgumentException e) {
        LOG.warn("Unknown table type: {}", type);
      }
    }

    buildTableFilter(tablePattern, builder.tableConditions);
  }

  private void buildTableFilter(String tablePattern, List<String> conditions) {
    boolean external = tableTypes.contains(TableType.EXTERNAL_TABLE.name());
    boolean managed = tableTypes.contains(TableType.MANAGED_TABLE.name());
    if (!managed && external) {
      // only for external tables
      conditions.add(
          hive_metastoreConstants.HIVE_FILTER_FIELD_TABLE_TYPE + " = \"" + TableType.EXTERNAL_TABLE.name() + "\" ");
    } else if (managed && !external) {
      // only for managed tables
      conditions.add(
          hive_metastoreConstants.HIVE_FILTER_FIELD_TABLE_TYPE + " = \"" + TableType.MANAGED_TABLE.name() + "\" ");
    }
    if (!tablePattern.trim().isEmpty()) {
      conditions.add(hive_metastoreConstants.HIVE_FILTER_FIELD_TABLE_NAME + " like \"" +
          tablePattern.replaceAll("\\*", ".*") + "\"");
    }
    this.tableFilter = String.join(" and ", conditions);
  }

  public List<TableName> getTableNames() throws Exception {
    List<TableName> candidates = new ArrayList<>();

    // if tableTypes is empty, then a list with single empty string has to specified to scan no tables.
    if (tableTypes.isEmpty()) {
      LOG.info("Table fetcher returns empty list as no table types specified");
      return candidates;
    }

    List<String> databases = client.getDatabases(catalogName, dbPattern);

    for (String db : databases) {
      List<String> tablesNames = getTableNamesForDatabase(catalogName, db);
      tablesNames.forEach(tablesName -> candidates.add(TableName.fromString(tablesName, catalogName, db)));
    }
    return candidates;
  }

  public List<Table> getTables(int maxBatchSize) throws Exception {
    List<Table> candidates = new ArrayList<>();

    // if tableTypes is empty, then a list with single empty string has to specified to scan no tables.
    if (tableTypes.isEmpty()) {
      LOG.info("Table fetcher returns empty list as no table types specified");
      return candidates;
    }

    List<String> databases = client.getDatabases(catalogName, dbPattern);

    for (String db : databases) {
      List<String> tablesNames = getTableNamesForDatabase(catalogName, db);
      for (Table table : new TableIterable(client, db, tablesNames, maxBatchSize)) {
        candidates.add(table);
      }
    }
    return candidates;
  }

  private List<String> getTableNamesForDatabase(String catalogName, String dbName) throws Exception {
    List<String> tableNames = new ArrayList<>();
    Database database = client.getDatabase(catalogName, dbName);
    if (MetaStoreUtils.checkIfDbNeedsToBeSkipped(database)) {
      LOG.debug("Skipping table under database: {}", dbName);
      return tableNames;
    }
    if (MetaStoreUtils.isDbBeingPlannedFailedOver(database)) {
      LOG.info("Skipping table that belongs to database {} being failed over.", dbName);
      return tableNames;
    }
    tableNames = client.listTableNamesByFilter(catalogName, dbName, tableFilter, -1);
    return tableNames;
  }

  public static class Builder {
    private final IMetaStoreClient client;
    private final String catalogName;
    private final String dbPattern;
    private final String tablePattern;
    private final List<String> tableConditions = new ArrayList<>();
    private String tableTypes;

    public Builder(IMetaStoreClient client, String catalogName, String dbPattern, String tablePattern) {
      this.client = client;
      this.catalogName = catalogName;
      this.dbPattern = dbPattern;
      this.tablePattern = tablePattern;
    }

    public Builder tableTypes(String tableTypes) {
      this.tableTypes = tableTypes;
      return this;
    }

    public Builder tableCondition(String condition) {
      this.tableConditions.add(condition);
      return this;
    }

    public TableFetcher build() {
      return new TableFetcher(this);
    }
  }
}