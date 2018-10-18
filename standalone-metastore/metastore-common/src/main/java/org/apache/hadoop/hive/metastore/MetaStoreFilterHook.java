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

package org.apache.hadoop.hive.metastore;

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;

/**
 * Metadata filter hook for metastore client. This will be useful for authorization
 * plugins on hiveserver2 to filter metadata results, especially in case of
 * non-impersonation mode where the metastore doesn't know the end user's identity.
 */
@InterfaceAudience.LimitedPrivate(value = {"Apache Sentry (Incubating)" })
@InterfaceStability.Evolving
public interface MetaStoreFilterHook {

  /**
   * Filter a catalog object.  Default implementation returns the passed in catalog.
   * @param catalog catalog to filter
   * @return filtered catalog
   * @throws MetaException something bad happened
   */
  default Catalog filterCatalog(Catalog catalog) throws MetaException {
    return catalog;
  }

  /**
   * Filter a list of catalog names.  Default implementation returns the passed in list.
   * @param catalogs list of catalog names.
   * @return filtered list of catalog names.
   * @throws MetaException something bad happened.
   */
  default List<String> filterCatalogs(List<String> catalogs) throws MetaException {
    return catalogs;
  }

  /**
   * Filter given list of databases
   * @param dbList
   * @return List of filtered Db names
   */
  List<String> filterDatabases(List<String> dbList) throws MetaException;

  /**
   * filter to given database object if applicable
   * @param dataBase
   * @return the same database if it's not filtered out
   * @throws NoSuchObjectException
   */
  Database filterDatabase(Database dataBase) throws MetaException, NoSuchObjectException;

  /**
   * Filter given list of tables
   * @param catName catalog name
   * @param dbName database name
   * @param tableList list of table returned by the metastore
   * @return List of filtered table names
   */
  List<String> filterTableNames(String catName, String dbName, List<String> tableList)
      throws MetaException;

  // Previously this was handled by filterTableNames.  But it can't be anymore because we can no
  // longer depend on a 1-1 mapping between table name and entry in the list.
  /**
   * Filter a list of TableMeta objects.
   * @param tableMetas list of TableMetas to filter
   * @return filtered table metas
   * @throws MetaException something went wrong
   */
  List<TableMeta> filterTableMetas(String catName,String dbName,List<TableMeta> tableMetas) throws MetaException;

  /**
   * filter to given table object if applicable
   * @param table
   * @return the same table if it's not filtered out
   * @throws NoSuchObjectException
   */
  Table filterTable(Table table) throws MetaException, NoSuchObjectException;

  /**
   * Filter given list of tables
   * @param tableList
   * @return List of filtered table names
   */
  List<Table> filterTables(List<Table> tableList) throws MetaException;

  /**
   * Filter given list of partitions
   * @param partitionList
   * @return
   */
  List<Partition> filterPartitions(List<Partition> partitionList) throws MetaException;

  /**
   * Filter given list of partition specs
   * @param partitionSpecList
   * @return
   */
  List<PartitionSpec> filterPartitionSpecs(List<PartitionSpec> partitionSpecList)
      throws MetaException;

  /**
   * filter to given partition object if applicable
   * @param partition
   * @return the same partition object if it's not filtered out
   * @throws NoSuchObjectException
   */
  Partition filterPartition(Partition partition) throws MetaException, NoSuchObjectException;

  /**
   * Filter given list of partition names
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param partitionNames list of partition names.
   * @return list of filtered partition names.
   */
  List<String> filterPartitionNames(String catName, String dbName, String tblName,
      List<String> partitionNames) throws MetaException;
}

