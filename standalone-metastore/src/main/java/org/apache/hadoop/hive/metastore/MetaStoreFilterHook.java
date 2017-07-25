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
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * Metadata filter hook for metastore client. This will be useful for authorization
 * plugins on hiveserver2 to filter metadata results, especially in case of
 * non-impersonation mode where the metastore doesn't know the end user's identity.
 */
@InterfaceAudience.LimitedPrivate(value = {"Apache Sentry (Incubating)" })
@InterfaceStability.Evolving
public interface MetaStoreFilterHook {

  /**
   * Filter given list of databases
   * @param dbList
   * @return List of filtered Db names
   */
  public List<String> filterDatabases(List<String> dbList) throws MetaException;

  /**
   * filter to given database object if applicable
   * @param dataBase
   * @return the same database if it's not filtered out
   * @throws NoSuchObjectException
   */
  public Database filterDatabase(Database dataBase) throws MetaException, NoSuchObjectException;

  /**
   * Filter given list of tables
   * @param dbName
   * @param tableList
   * @return List of filtered table names
   */
  public List<String> filterTableNames(String dbName, List<String> tableList) throws MetaException;

  /**
   * filter to given table object if applicable
   * @param table
   * @return the same table if it's not filtered out
   * @throws NoSuchObjectException
   */
  public Table filterTable(Table table) throws MetaException, NoSuchObjectException;

  /**
   * Filter given list of tables
   * @param tableList
   * @return List of filtered table names
   */
  public List<Table> filterTables(List<Table> tableList) throws MetaException;

  /**
   * Filter given list of partitions
   * @param partitionList
   * @return
   */
  public List<Partition> filterPartitions(List<Partition> partitionList) throws MetaException;

  /**
   * Filter given list of partition specs
   * @param partitionSpecList
   * @return
   */
  public List<PartitionSpec> filterPartitionSpecs(List<PartitionSpec> partitionSpecList)
      throws MetaException;

  /**
   * filter to given partition object if applicable
   * @param partition
   * @return the same partition object if it's not filtered out
   * @throws NoSuchObjectException
   */
  public Partition filterPartition(Partition partition) throws MetaException, NoSuchObjectException;

  /**
   * Filter given list of partition names
   * @param dbName
   * @param tblName
   * @param partitionNames
   * @return
   */
  public List<String> filterPartitionNames(String dbName, String tblName,
      List<String> partitionNames) throws MetaException;

  public Index filterIndex(Index index) throws MetaException, NoSuchObjectException;

  /**
   * Filter given list of index names
   * @param dbName
   * @param tblName
   * @param indexList
   * @return
   */
  public List<String> filterIndexNames(String dbName, String tblName,
      List<String> indexList) throws MetaException;

  /**
   * Filter given list of index objects
   * @param indexeList
   * @return
   */
  public List<Index> filterIndexes(List<Index> indexeList) throws MetaException;
}

