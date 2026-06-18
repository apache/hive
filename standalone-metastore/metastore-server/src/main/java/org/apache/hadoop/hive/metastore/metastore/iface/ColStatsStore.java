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
package org.apache.hadoop.hive.metastore.metastore.iface;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.TransactionalMetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.metastore.MetaDescriptor;
import org.apache.hadoop.hive.metastore.metastore.impl.ColStatsStoreImpl;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;

@MetaDescriptor(alias = "stats", defaultImpl = ColStatsStoreImpl.class)
public interface ColStatsStore {
  /** Persists the given column statistics object to the metastore
   * @param colStats object to persist
   * @return Boolean indicating the outcome of the operation
   * @throws NoSuchObjectException No such table.
   * @throws MetaException error accessing the RDBMS.
   * @throws InvalidObjectException the stats object is invalid
   * @throws InvalidInputException unable to record the stats for the table
   */
  Map<String, String> updateTableColumnStatistics(ColumnStatistics colStats, String validWriteIds, long writeId)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  Map<String, String> updatePartitionColumnStatistics(Table table, MTable mTable,
      ColumnStatistics statsObj, List<String> partVals,
      String validWriteIds, long writeId)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  /**
   * Returns the relevant column statistics for a given column in a given table in a given database
   * if such statistics exist.
   * @param tableName name of the table
   * @param colName names of the columns for which statistics is requested
   * @return Relevant column statistics for the column for the given table
   * @throws NoSuchObjectException No such table
   * @throws MetaException error accessing the RDBMS
   *
   */
  List<ColumnStatistics> getTableColumnStatistics(TableName tableName,
      List<String> colName) throws MetaException, NoSuchObjectException;

  /**
   * Returns the relevant column statistics for a given column in a given table in a given database
   * if such statistics exist.
   * @param tableName name of the table
   * @param colName names of the columns for which statistics is requested
   * @param engine engine requesting the statistics
   * @return Relevant column statistics for the column for the given table
   * @throws NoSuchObjectException No such table
   * @throws MetaException error accessing the RDBMS
   *
   */
  ColumnStatistics getTableColumnStatistics(TableName tableName,
      List<String> colName, String engine) throws MetaException, NoSuchObjectException;

  /**
   * Returns the relevant column statistics for a given column in a given table in a given database
   * if such statistics exist.
   * @param tableName name of the table
   * @param colName names of the columns for which statistics is requested
   * @param engine engine requesting the statistics
   * @param writeIdList string format of valid writeId transaction list
   * @return Relevant column statistics for the column for the given table
   * @throws NoSuchObjectException No such table
   * @throws MetaException error accessing the RDBMS
   *
   */
  ColumnStatistics getTableColumnStatistics(
      TableName tableName,
      List<String> colName, String engine, String writeIdList)
      throws MetaException, NoSuchObjectException;

  /**
   * Get statistics for a partition for a set of columns.
   * @param tableName table name.
   * @param partNames list of partition names.  These are names so must be key1=val1[/key2=val2...]
   * @param colNames list of columns to get stats for
   * @return list of statistics objects
   * @throws MetaException error accessing the RDBMS
   * @throws NoSuchObjectException no such partition.
   */
  List<List<ColumnStatistics>> getPartitionColumnStatistics(
      TableName tableName, List<String> partNames, List<String> colNames)
      throws MetaException, NoSuchObjectException;

  /**
   * Get statistics for a partition for a set of columns.
   * @param tableName table name.
   * @param partNames list of partition names.  These are names so must be key1=val1[/key2=val2...]
   * @param colNames list of columns to get stats for
   * @param engine engine requesting the statistics
   * @return list of statistics objects
   * @throws MetaException error accessing the RDBMS
   * @throws NoSuchObjectException no such partition.
   */
  List<ColumnStatistics> getPartitionColumnStatistics(
      TableName tableName, List<String> partNames, List<String> colNames,
      String engine) throws MetaException, NoSuchObjectException;

  /**
   * Get statistics for a partition for a set of columns.
   * @param tableName table name.
   * @param partNames list of partition names.  These are names so must be key1=val1[/key2=val2...]
   * @param colNames list of columns to get stats for
   * @param engine engine requesting the statistics
   * @param writeIdList string format of valid writeId transaction list
   * @return list of statistics objects
   * @throws MetaException error accessing the RDBMS
   * @throws NoSuchObjectException no such partition.
   */
  List<ColumnStatistics> getPartitionColumnStatistics(
      TableName tableName,
      List<String> partNames, List<String> colNames,
      String engine, String writeIdList)
      throws MetaException, NoSuchObjectException;

  /**
   * Deletes column statistics if present associated with a given db, table, partition and a list of cols. If
   * null is passed instead of a colName, stats when present for all columns associated
   * with a given db, table and partition are deleted.
   * @param tableName table name.
   * @param partNames partition names.
   * @param colNames a list of column names.
   * @param engine engine for which we want to delete statistics
   * @return Boolean indicating the outcome of the operation
   * @throws NoSuchObjectException no such partition
   * @throws MetaException error access the RDBMS
   * @throws InvalidObjectException error dropping the stats
   * @throws InvalidInputException bad input, such as null table or database name.
   */
  boolean deletePartitionColumnStatistics(TableName tableName,
      List<String> partNames, List<String> colNames, String engine)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  /**
   * Delete statistics for a single column, a list of columns or all columns in a table.
   * @param tableName table name
   * @param colNames a list of column names.  Null to delete stats for all columns in the table.
   * @param engine engine for which we want to delete statistics
   * @return true if the statistics were deleted.
   * @throws NoSuchObjectException no such table or column.
   * @throws MetaException error access the RDBMS.
   * @throws InvalidObjectException error dropping the stats
   * @throws InvalidInputException bad inputs, such as null table name.
   */
  boolean deleteTableColumnStatistics(TableName tableName,
      List<String> colNames, String engine)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  /**
   * Get aggregated stats for a table or partition(s).
   * @param tableName table name.
   * @param partNames list of partition names.  These are the names of the partitions, not
   *                  values.
   * @param colNames list of column names
   * @param engine engine requesting the statistics
   * @return aggregated stats
   * @throws MetaException error accessing RDBMS
   * @throws NoSuchObjectException no such table or partition
   */
  AggrStats get_aggr_stats_for(TableName tableName,
      List<String> partNames, List<String> colNames, String engine) throws MetaException, NoSuchObjectException;

  /**
   * Get aggregated stats for a table or partition(s).
   * @param partNames list of partition names.  These are the names of the partitions, not
   *                  values.
   * @param colNames list of column names
   * @param engine engine requesting the statistics
   * @param writeIdList string format of valid writeId transaction list
   * @return aggregated stats
   * @throws MetaException error accessing RDBMS
   * @throws NoSuchObjectException no such table or partition
   */
  AggrStats get_aggr_stats_for(TableName tableName,
      List<String> partNames, List<String> colNames,
      String engine, String writeIdList)
      throws MetaException, NoSuchObjectException;

  Map<String, Map<String, String>> updatePartitionColumnStatisticsInBatch(
      Map<String, ColumnStatistics> partColStatsMap,
      Table tbl, List<TransactionalMetaStoreEventListener> listeners,
      String validWriteIds, long writeId)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  /**
   * Get column stats for all partitions of all tables in the database
   * @param catName catalog name
   * @param dbName database name
   * @return List of column stats objects for all partitions of all tables in the database
   * @throws MetaException error accessing RDBMS
   * @throws NoSuchObjectException no such database
   */
  List<MetaStoreServerUtils.ColStatsObjWithSourceInfo> getPartitionColStatsForDatabase(String catName, String dbName)
      throws MetaException, NoSuchObjectException;

  List<TableName> getTableNamesWithStats() throws MetaException, NoSuchObjectException;

  List<TableName> getAllTableNamesForStats() throws MetaException, NoSuchObjectException;

  Map<String, List<String>> getPartitionColsWithStats(TableName tableName) throws MetaException, NoSuchObjectException;
}
