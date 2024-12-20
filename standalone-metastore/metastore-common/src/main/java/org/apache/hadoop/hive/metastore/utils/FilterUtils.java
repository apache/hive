/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.utils;


import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.CATALOG_DB_SEPARATOR;

import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoResponse;
import org.apache.hadoop.hive.metastore.api.CompactionInfoStruct;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utilities common to Filtering operations.
 */
public class FilterUtils {

  /**
   * Filter the DB if filtering is enabled. Otherwise, return original DB object
   * @param isFilterEnabled true: filtering is enabled; false: filtring is disabled.
   * @param filterHook: the object that does filtering
   * @param db: the database object from HMS metadata
   * @return the original database object if current user has access;
   *         otherwise, throw NoSuchObjectException exception
   * @throws MetaException
   * @throws NoSuchObjectException
   */
  public static Database filterDbIfEnabled(
      boolean isFilterEnabled,
      MetaStoreFilterHook filterHook,
      Database db) throws MetaException, NoSuchObjectException {

    if (isFilterEnabled) {
      Database filteredDb = filterHook.filterDatabase(db);

      if (filteredDb == null) {
        throw new NoSuchObjectException("DB " + db.getName() + " not found.");
      }
    }

    return  db;
  }

  /**
   * Filter the list of databases if filtering is enabled. Otherwise, return original list
   * @param isFilterEnabled true: filtering is enabled; false: filtring is disabled.
   * @param filterHook: the object that does filtering
   * @param dbNames: the list of database names to filter
   * @return the list of database names that current user has access if filtering is enabled;
   *         otherwise, the original list
   * @throws MetaException
   */
  public static List<String> filterDbNamesIfEnabled(
      boolean isFilterEnabled,
      MetaStoreFilterHook filterHook,
      List<String> dbNames) throws MetaException {

    if (isFilterEnabled) {
      return filterHook.filterDatabases(dbNames);
    }

    return dbNames;
  }

  /**
   * Filter the list of dataconnectors if filtering is enabled. Otherwise, return original list
   * @param isFilterEnabled true: filtering is enabled; false: filtring is disabled.
   * @param filterHook: the object that does filtering
   * @param connectorNames: the list of dataconnector names to filter
   * @return the list of dataconnector names that current user has access if filtering is enabled;
   *         otherwise, the original list
   * @throws MetaException
   */
  public static List<String> filterDataConnectorsIfEnabled(
      boolean isFilterEnabled,
      MetaStoreFilterHook filterHook,
      List<String> connectorNames) throws MetaException {

    if (isFilterEnabled) {
      return filterHook.filterDataConnectors(connectorNames);
    }
    return connectorNames;
  }

  /**
   * Filter the list of tables if filtering is enabled. Otherwise, return original list
   * @param isFilterEnabled true: filtering is enabled; false: filtring is disabled.
   * @param filterHook: the object that does filtering
   * @param catName: the catalog name of the tables
   * @param dbName: the database name to the tables
   * @param tableNames: the list of table names to filter
   * @return the list of table names that current user has access if filtering is enabled;
   *         otherwise, the original list
   * @throws MetaException
   */
  public static List<String> filterTableNamesIfEnabled(
      boolean isFilterEnabled, MetaStoreFilterHook filterHook, String catName, String dbName,
      List<String> tableNames) throws MetaException{

    if (isFilterEnabled) {
      return filterHook.filterTableNames(catName, dbName, tableNames);
    }

    return tableNames;
  }

  /**
   * Filter the list of tables if filtering is enabled. Otherwise, return original list
   * @param isFilterEnabled true: filtering is enabled; false: filtring is disabled.
   * @param filterHook: the object that does filtering
   * @param tables: the list of table objects to filter
   * @return the list of tables that current user has access if filtering is enabled;
   *         otherwise, the original list
   * @throws MetaException
   */
  public static List<Table> filterTablesIfEnabled(
      boolean isFilterEnabled, MetaStoreFilterHook filterHook, List<Table> tables)
      throws MetaException{

    if (isFilterEnabled) {
      return filterHook.filterTables(tables);
    }

    return tables;
  }

  /**
   * Filter the table if filtering is enabled. Otherwise, return original table object
   * @param isFilterEnabled true: filtering is enabled; false: filtring is disabled.
   * @param filterHook: the object that does filtering
   * @param table: the table object from Hive meta data
   * @return the table object if user has access or filtering is disabled;
   *         throw NoSuchObjectException if user does not have access to this table
   * @throws MetaException
   * @throws NoSuchObjectException
   */
  public static Table filterTableIfEnabled(
      boolean isFilterEnabled, MetaStoreFilterHook filterHook, Table table)
      throws MetaException, NoSuchObjectException {
    if (isFilterEnabled) {
      Table filteredTable = filterHook.filterTable(table);

      if (filteredTable == null) {
        throw new NoSuchObjectException("Table " + table.getDbName() + "." +
            table.getTableName() + " not found.");
      }
    }

    return table;
  }

  /**
   * Filter list of meta data of tables if filtering is enabled. Otherwise, return original list
   * @param isFilterEnabled true: filtering is enabled; false: filtring is disabled.
   * @param filterHook: the object that does filtering
   * @param tableMetas: the list of meta data of tables
   * @return the list of table meta data that current user has access if filtering is enabled;
   *         otherwise, the original list
   * @throws MetaException
   * @throws NoSuchObjectException
   */
  public static List<TableMeta> filterTableMetasIfEnabled(boolean isFilterEnabled, MetaStoreFilterHook filterHook,
      List<TableMeta> tableMetas) throws MetaException, NoSuchObjectException {
    if (tableMetas == null || tableMetas.isEmpty()) {
      return tableMetas;
    }

    if (isFilterEnabled) {
      return filterHook.filterTableMetas(tableMetas);
    }

    return tableMetas;
  }

  /**
   * Filter the partition if filtering is enabled. Otherwise, return original object
   * @param isFilterEnabled true: filtering is enabled; false: filtring is disabled.
   * @param filterHook: the object that does filtering
   * @param p: the partition object
   * @return the partition object that user has access or original list if filtering is disabled;
   *         Otherwise, throw NoSuchObjectException
   * @throws MetaException
   * @throws NoSuchObjectException
   */
  public static Partition filterPartitionIfEnabled(
      boolean isFilterEnabled,
      MetaStoreFilterHook filterHook, Partition p) throws MetaException, NoSuchObjectException {

    if (isFilterEnabled) {
      Partition filteredPartition = filterHook.filterPartition(p);

      if (filteredPartition == null) {
        throw new NoSuchObjectException("Partition in " + p.getCatName() + CATALOG_DB_SEPARATOR + p.getDbName() + "." +
            p.getTableName() + " not found.");
      }
    }

    return p;
  }

  /**
   * Filter the list of partitions if filtering is enabled. Otherwise, return original list
   * @param isFilterEnabled true: filtering is enabled; false: filtring is disabled.
   * @param filterHook: the object that does filtering
   * @param partitions: the list of partitions
   * @return the list of partitions that user has access or original list if filtering is disabled;
   * @throws MetaException
   */
  public static List<Partition> filterPartitionsIfEnabled(
      boolean isFilterEnabled,
      MetaStoreFilterHook filterHook, List<Partition> partitions) throws MetaException {

    if (isFilterEnabled) {
      return filterHook.filterPartitions(partitions);
    }

    return partitions;
  }

  /**
   * Filter the list of partitions if filtering is enabled. Otherwise, return original list
   * @param isFilterEnabled true: filtering is enabled; false: filtring is disabled.
   * @param filterHook: the object that does filtering
   * @param catName: the catalog name
   * @param dbName: the database name
   * @param tableName: the table name
   * @param partitionNames: the list of partition names
   * @return the list of partitions that current user has access if filtering is enabled;
   *         Otherwise, the original list
   * @throws MetaException
   */
  public static List<String> filterPartitionNamesIfEnabled(
      boolean isFilterEnabled,
      MetaStoreFilterHook filterHook,
      final String catName, final String dbName,
      final String tableName, List<String> partitionNames) throws MetaException {
    if (isFilterEnabled) {
      return
          filterHook.filterPartitionNames(catName,
              dbName, tableName, partitionNames);
    }

    return partitionNames;
  }

  /**
   * Filter the list of PartitionSpec if filtering is enabled; Otherwise, return original list
   * @param isFilterEnabled true: filtering is enabled; false: filtring is disabled.
   * @param filterHook: the object that does filtering
   * @param partitionSpecs: the list of PartitionSpec
   * @return the list of PartitionSpec that current user has access if filtering is enabled;
   *         Otherwise, the original list
   * @throws MetaException
   */
  public static List<PartitionSpec> filterPartitionSpecsIfEnabled(
      boolean isFilterEnabled,
      MetaStoreFilterHook filterHook,
      List<PartitionSpec> partitionSpecs) throws MetaException {
    if (isFilterEnabled) {
      return
          filterHook.filterPartitionSpecs(partitionSpecs);
    }

    return partitionSpecs;
  }

  /**
   * Filter the catalog if filtering is enabled; Otherwise, return original object
   * @param isFilterEnabled true: filtering is enabled; false: filtring is disabled.
   * @param filterHook: the object that does filtering
   * @param catalog: the catalog object
   * @return the catalog object that current user has access or filtering is disabled;
   *         Otherwise, throw NoSuchObjectException
   * @throws MetaException
   * @throws NoSuchObjectException
   */
  public static Catalog filterCatalogIfEnabled(
      boolean isFilterEnabled,
      MetaStoreFilterHook filterHook,
      Catalog catalog
  ) throws MetaException, NoSuchObjectException {
    if (isFilterEnabled) {
      Catalog filteredCatalog = filterHook.filterCatalog(catalog);

      if (filteredCatalog == null) {
        throw new NoSuchObjectException("Catalog " + catalog.getName() + " not found.");
      }
    }

    return  catalog;
  }

  /**
   * Filter list of catalog names if filtering is enabled; Otherwise, return original list
   * @param isFilterEnabled true: filtering is enabled; false: filtring is disabled.
   * @param filterHook: the object that does filtering
   * @param catalogNames: the list of catalog names
   * @return the list of catalog names that the current user has access or
   *         original list if filtering is disabled;
   * @throws MetaException
   */
  public static List<String> filterCatalogNamesIfEnabled(
      boolean isFilterEnabled, MetaStoreFilterHook filterHook,
      List<String> catalogNames) throws MetaException{

    if (isFilterEnabled) {
      return filterHook.filterCatalogs(catalogNames);
    }

    return catalogNames;
  }


  /**
   * Check if the current user has access to a given database and table name. Throw
   * NoSuchObjectException if user has no access. When the db or table is filtered out, we don't need
   * to even fetch the partitions. Therefore this check ensures table-level security and
   * could improve performance when filtering partitions.
   * @param dbName the database name
   * @param tblName the table name contained in the database
   * @throws NoSuchObjectException if the database or table is filtered out
   */
  public static void checkDbAndTableFilters(boolean isFilterEnabled,
      MetaStoreFilterHook filterHook,
      final String catName, final String dbName, final String tblName)
      throws NoSuchObjectException, MetaException {

    if (catName == null) {
      throw new NullPointerException("catName is null");
    }

    if (isBlank(catName)) {
      throw new NoSuchObjectException("catName is not valid");
    }

    if (dbName == null) {
      throw new NullPointerException("dbName is null");
    }

    if (isBlank(dbName)) {
      throw new NoSuchObjectException("dbName is not valid");
    }

    List<String> filteredDb = filterDbNamesIfEnabled(isFilterEnabled, filterHook,
        Collections.singletonList(dbName));

    if (filteredDb.isEmpty()) {
      throw new NoSuchObjectException("Database " + dbName + " does not exist");
    }

    if (tblName == null) {
      throw new NullPointerException("tblName is null");
    }

    if (isBlank(tblName)) {
      throw new NoSuchObjectException("tblName is not valid");
    }

    List<String> filteredTable =
        filterTableNamesIfEnabled(isFilterEnabled, filterHook,
            catName, dbName, Collections.singletonList(tblName));
    if (filteredTable.isEmpty()) {
      throw new NoSuchObjectException("Table " + tblName + " does not exist");
    }
  }

  /**
   * Filter the list of compactions if filtering is enabled. Otherwise, return original list
   *
   * @param isFilterEnabled true: filtering is enabled; false: filtering is disabled.
   * @param filterHook      the object that does filtering
   * @param compactions     the list of compactions
   * @return the list of compactions that user has access or original list if filtering is disabled;
   * @throws MetaException
   */
  public static List<ShowCompactResponseElement> filterCompactionsIfEnabled(
          boolean isFilterEnabled,
          MetaStoreFilterHook filterHook, String catName, List<ShowCompactResponseElement> compactions)
          throws MetaException {

    if (isFilterEnabled) {
      List<ShowCompactResponseElement> result = new ArrayList<>(compactions.size());

      // DBName -> List of TableNames map used for checking access rights for non partitioned tables
      Map<String, List<String>> nonPartTables = new HashMap<>();
      // DBName -> TableName -> List of PartitionNames map used for checking access rights for
      // partitioned tables
      Map<String, Map<String, List<String>>> partTables = new HashMap<>();
      for (ShowCompactResponseElement c : compactions) {
        if (c.getPartitionname() == null) {
          nonPartTables.computeIfAbsent(c.getDbname(), k -> new ArrayList<>());
          if (!nonPartTables.get(c.getDbname()).contains(c.getTablename())) {
            nonPartTables.get(c.getDbname()).add(c.getTablename());
          }
        } else {
          partTables.computeIfAbsent(c.getDbname(), k -> new HashMap<>());
          partTables.get(c.getDbname()).computeIfAbsent(c.getTablename(), k -> new ArrayList<>());
          if (!partTables.get(c.getDbname()).get(c.getTablename()).contains(c.getPartitionname())) {
            partTables.get(c.getDbname()).get(c.getTablename()).add(c.getPartitionname());
          }
        }
      }
      // Checking non partitioned table access rights
      for (Map.Entry<String, List<String>> e : nonPartTables.entrySet()) {
        nonPartTables.put(e.getKey(), filterHook.filterTableNames(catName, e.getKey(), e.getValue()));
      }
      // Checking partitioned table access rights
      for (Map.Entry<String, Map<String, List<String>>> dbName : partTables.entrySet()) {
        for (Map.Entry<String, List<String>> tableName : dbName.getValue().entrySet()) {
          dbName.getValue().put(tableName.getKey(),
              filterHook.filterPartitionNames(catName, dbName.getKey(), tableName.getKey(), tableName.getValue()));
        }
      }

      // Add the compactions to the response only we have access right
      for (ShowCompactResponseElement c : compactions) {
        if (c.getPartitionname() == null) {
          if (nonPartTables.get(c.getDbname()).contains(c.getTablename())) {
            result.add(c);
          }
        } else {
          if (partTables.get(c.getDbname()).get(c.getTablename()).contains(c.getPartitionname())) {
            result.add(c);
          }
        }
      }
      return result;
    } else {
      return compactions;
    }
  }

  public static GetLatestCommittedCompactionInfoResponse filterCommittedCompactionInfoStructIfEnabled(
      boolean isFilterEnabled, MetaStoreFilterHook filterHook,
      String catName, String dbName, String tableName, GetLatestCommittedCompactionInfoResponse response
  ) throws MetaException {
    if (isFilterEnabled && response.getCompactionsSize() > 0) {
      List<CompactionInfoStruct> compactions = response.getCompactions();
      if (compactions.get(0).getPartitionname() == null) {
        // non partitioned table
        List<String> tableNames = new ArrayList<>();
        tableNames.add(tableName);
        tableNames = filterHook.filterTableNames(catName, dbName, tableNames);
        if (tableNames.isEmpty()) {
          response.unsetCompactions();
        }
      } else {
        // partitioned table
        List<String> partitionNames = compactions.stream()
            .map(CompactionInfoStruct::getPartitionname)
            .collect(Collectors.toList());
        partitionNames = filterHook.filterPartitionNames(
            catName, dbName, tableName, partitionNames);
        Set<String> partitionNameSet = new HashSet<>(partitionNames);
        compactions = compactions.stream()
            .filter(lci -> partitionNameSet.contains(lci.getPartitionname()))
            .collect(Collectors.toList());
        response.setCompactions(compactions);
      }
    }
    return response;
  }
}
