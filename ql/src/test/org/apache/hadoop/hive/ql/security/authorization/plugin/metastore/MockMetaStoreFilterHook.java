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

package org.apache.hadoop.hive.ql.security.authorization.plugin.metastore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.MetaStoreFilterHook;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Mock implementation of MetaStoreFilterHook that filters based on ownership.
 */
public class MockMetaStoreFilterHook implements MetaStoreFilterHook {

  private static final Logger LOG = LoggerFactory.getLogger(MockMetaStoreFilterHook.class);
  private static final String AUTHORIZED_OWNER = TestHiveMetaStoreAuthorizer.authorizedUser;

  @SuppressWarnings("unused")
  public MockMetaStoreFilterHook(Configuration conf) {
    LOG.info("Initialized MockMetaStoreFilterHook with authorized owner: " + AUTHORIZED_OWNER);
  }

  @Override
  public List<String> filterDatabases(List<String> dbList) throws MetaException {
    LOG.debug("filterDatabases: Original list size: {}", dbList != null ? dbList.size() : 0);
    return dbList;
  }

  @Override
  public Database filterDatabase(Database dataBase) throws MetaException, NoSuchObjectException {
    if (dataBase == null) {
      LOG.debug("filterDatabase: Database is null");
      return null;
    }

    String dbName = dataBase.getName();
    String ownerName = dataBase.getOwnerName();

    if (!AUTHORIZED_OWNER.equals(ownerName)) {
      LOG.info("filterDatabase: Filtering out database '{}' - owner '{}' is not authorized", dbName, ownerName);
      return null;
    }

    LOG.debug("filterDatabase: Allowed access to database '{}' for owner '{}'", dbName, ownerName);
    return dataBase;
  }

  @Override
  public List<String> filterTableNames(String catName, String dbName, List<String> tableList) throws MetaException {
    LOG.debug("filterTableNames: Returning all tables for {}.{}, count: {}", catName, dbName, tableList != null ? tableList.size() : 0);
    return tableList;
  }

  @Override
  public List<TableMeta> filterTableMetas(String catName, String dbName, List<TableMeta> tableMetas) throws MetaException {
    if (tableMetas == null) {
      LOG.debug("filterTableMetas: Table metas list is null");
      return null;
    }

    LOG.info("filterTableMetas: Filtering {} table metas for {}.{}", tableMetas.size(), catName, dbName);

    List<TableMeta> filtered = tableMetas.stream()
        .filter(tm -> {
          boolean authorized = AUTHORIZED_OWNER.equals(tm.getOwnerName());
          if (!authorized) {
            LOG.debug("Filtering out table meta '{}' - owner '{}' is not authorized", tm.getTableName(), tm.getOwnerName());
          }
          return authorized;
        })
        .collect(Collectors.toList());

    LOG.info("filterTableMetas: Filtered result contains {} table metas", filtered.size());
    return filtered;
  }

  @Override
  public List<TableMeta> filterTableMetas(List<TableMeta> tableMetas) throws MetaException {
    if (tableMetas == null) {
      return null;
    }

    LOG.info("filterTableMetas: Filtering {} table metas", tableMetas.size());

    return tableMetas.stream()
        .filter(tm -> {
          return AUTHORIZED_OWNER.equals(tm.getOwnerName());
        })
        .collect(Collectors.toList());
  }

  @Override
  public Table filterTable(Table table) throws MetaException, NoSuchObjectException {
    if (table == null) {
      LOG.debug("filterTable: Table is null");
      return null;
    }

    String tableName = table.getTableName();
    String owner = table.getOwner();

    if (!AUTHORIZED_OWNER.equals(owner)) {
      LOG.info("filterTable: Filtering out table '{}' - owner '{}' is not authorized", tableName, owner);
      return null;
    }

    LOG.debug("filterTable: Allowed access to table '{}' for owner '{}'", tableName, owner);
    return table;
  }

  @Override
  public List<Table> filterTables(List<Table> tableList) throws MetaException {
    if (tableList == null) {
      LOG.debug("filterTables: Table list is null");
      return null;
    }

    LOG.info("filterTables: Filtering {} tables", tableList.size());

    List<Table> filtered = tableList.stream()
        .filter(t -> {
          boolean authorized = AUTHORIZED_OWNER.equals(t.getOwner());
          if (!authorized) {
            LOG.debug("Filtering out table '{}' - owner '{}' is not authorized", t.getTableName(), t.getOwner());
          }
          return authorized;
        })
        .collect(Collectors.toList());

    LOG.info("filterTables: Filtered result contains " + filtered.size() + " tables");
    return filtered;
  }

  @Override
  public List<Partition> filterPartitions(List<Partition> partitionList) throws MetaException {
    LOG.debug("filterPartitions: Returning all partitions, count: {}", partitionList != null ? partitionList.size() : 0);
    return partitionList;
  }

  @Override
  public List<PartitionSpec> filterPartitionSpecs(List<PartitionSpec> partitionSpecList) throws MetaException {
    LOG.debug("filterPartitionSpecs: Returning all partition specs, count: {}", partitionSpecList != null ? partitionSpecList.size() : 0);
    return partitionSpecList;
  }

  @Override
  public Partition filterPartition(Partition partition) throws MetaException, NoSuchObjectException {
    if (partition != null) {
      LOG.debug("filterPartition: Returning partition for table: {}", partition.getTableName());
    }
    return partition;
  }

  @Override
  public List<String> filterPartitionNames(String catName, String dbName, String tblName, List<String> partitionNames) throws MetaException {
    LOG.debug("filterPartitionNames: Returning all partition names for {}.{}.{}, count: {}", catName, dbName, tblName, partitionNames != null ? partitionNames.size() : 0);
    return partitionNames;
  }

  @Override
  public List<String> filterDataConnectors(List<String> dcList) throws MetaException {
    LOG.debug("filterDataConnectors: Returning all data connectors, count: {}", dcList != null ? dcList.size() : 0);
    return dcList;
  }
}