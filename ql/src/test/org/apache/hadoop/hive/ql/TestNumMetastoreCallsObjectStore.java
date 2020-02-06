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

package org.apache.hadoop.hive.ql;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.*;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.GetPartitionsFilterSpec;
import org.apache.hadoop.hive.metastore.api.GetPartitionsProjectionSpec;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.thrift.TException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

// this class is a test wrapper around ObjectStore overriding most of the get methods
// used by compiler. This is used my TestNumMetastoreCalls to ensure the number of calls to
// metastore
public class TestNumMetastoreCallsObjectStore extends ObjectStore {

  static Map<String, Integer> callMap = new HashMap<>();
  static private int numCalls = 0;

  static void incrementCall() {
    numCalls++;
  }

  static int getNumCalls() {
    return numCalls;
  }

  public TestNumMetastoreCallsObjectStore() {
    super();
  }


  @Override public Catalog getCatalog(String catalogName)
      throws NoSuchObjectException, MetaException {
    incrementCall();
    return super.getCatalog(catalogName);
  }

  @Override public List<String> getCatalogs() throws MetaException {
    incrementCall();
    return super.getCatalogs();
  }

  @Override public Database getDatabase(String catalogName, String name)
      throws NoSuchObjectException {
    incrementCall();
    return super.getDatabase(catalogName, name);
  }

  @Override public List<String> getDatabases(String catName, String pattern) throws MetaException {
    incrementCall();
    return super.getDatabases(catName, pattern);
  }

  @Override public List<String> getAllDatabases(String catName) throws MetaException {
    incrementCall();
    return super.getAllDatabases(catName);
  }

  @Override public Table getTable(String catName, String dbName, String tableName,
      String writeIdList) throws MetaException {
    incrementCall();
    return super.getTable(catName, dbName, tableName, writeIdList);
  }

  @Override public List<String> getTables(String catName, String dbName, String pattern)
      throws MetaException {
    incrementCall();
    return super.getTables(catName, dbName, pattern);
  }

  @Override public List<String> getTables(String catName, String dbName, String pattern,
      TableType tableType, int limit) throws MetaException {
    incrementCall();
    return super.getTables(catName, dbName, pattern, tableType, limit);
  }

  @Override public List<TableName> getTableNamesWithStats()
      throws MetaException, NoSuchObjectException {
    incrementCall();
    return super.getTableNamesWithStats();
  }

  @Override public Map<String, List<String>> getPartitionColsWithStats(String catName,
      String dbName, String tableName) throws MetaException, NoSuchObjectException {
    incrementCall();
    return super.getPartitionColsWithStats(catName, dbName, tableName);
  }

  @Override public List<TableName> getAllTableNamesForStats()
      throws MetaException, NoSuchObjectException {
    incrementCall();
    return super.getAllTableNamesForStats();
  }

  @Override public List<Table> getAllMaterializedViewObjectsForRewriting(String catName)
      throws MetaException {
    incrementCall();
    return super.getAllMaterializedViewObjectsForRewriting(catName);
  }

  @Override public List<String> getMaterializedViewsForRewriting(String catName, String dbName)
      throws MetaException, NoSuchObjectException {
    incrementCall();
    return super.getMaterializedViewsForRewriting(catName, dbName);
  }

  @Override public int getDatabaseCount() throws MetaException {
    incrementCall();
    return super.getDatabaseCount();
  }

  @Override public int getPartitionCount() throws MetaException {
    incrementCall();
    return super.getPartitionCount();
  }

  @Override public int getTableCount() throws MetaException {
    incrementCall();
    return super.getTableCount();
  }

  @Override public List<TableMeta> getTableMeta(String catName, String dbNames, String tableNames,
      List<String> tableTypes) throws MetaException {
    incrementCall();
    return super.getTableMeta(catName, dbNames, tableNames, tableTypes);
  }

  @Override public List<String> getAllTables(String catName, String dbName) throws MetaException {
    incrementCall();
    return super.getAllTables(catName, dbName);
  }

  @Override public List<Table> getTableObjectsByName(String catName, String db,
      List<String> tbl_names) throws MetaException, UnknownDBException {
    incrementCall();
    return super.getTableObjectsByName(catName, db, tbl_names);
  }

  @Override public Partition getPartition(String catName, String dbName, String tableName,
      List<String> part_vals) throws NoSuchObjectException, MetaException {
    incrementCall();
    return super.getPartition(catName, dbName, tableName, part_vals);
  }

  @Override public Partition getPartition(String catName, String dbName, String tableName,
      List<String> part_vals, String validWriteIds) throws NoSuchObjectException, MetaException {
    incrementCall();
    return super.getPartition(catName, dbName, tableName, part_vals, validWriteIds);
  }

  @Override public List<Partition> getPartitions(String catName, String dbName, String tableName,
      int maxParts) throws MetaException, NoSuchObjectException {
    incrementCall();
    return super.getPartitions(catName, dbName, tableName, maxParts);
  }

  @Override public Map<String, String> getPartitionLocations(String catName, String dbName,
      String tblName, String baseLocationToNotShow, int max) {
    incrementCall();
    return super.getPartitionLocations(catName, dbName, tblName, baseLocationToNotShow, max);
  }

  @Override public List<Partition> getPartitionsWithAuth(String catName, String dbName,
      String tblName, short max, String userName, List<String> groupNames)
      throws MetaException, InvalidObjectException {
    incrementCall();
    return super.getPartitionsWithAuth(catName, dbName, tblName, max, userName, groupNames);
  }

  @Override public Partition getPartitionWithAuth(String catName, String dbName, String tblName,
      List<String> partVals, String user_name, List<String> group_names)
      throws NoSuchObjectException, MetaException, InvalidObjectException {
    incrementCall();
    return super.getPartitionWithAuth(catName, dbName, tblName, partVals, user_name, group_names);
  }

  @Override public List<String> listPartitionNames(String catName, String dbName, String tableName,
      short max) throws MetaException {
    incrementCall();
    return super.listPartitionNames(catName, dbName, tableName, max);
  }

  @Override public PartitionValuesResponse listPartitionValues(String catName, String dbName,
      String tableName, List<FieldSchema> cols, boolean applyDistinct, String filter,
      boolean ascending, List<FieldSchema> order, long maxParts) throws MetaException {
    incrementCall();
    return super
        .listPartitionValues(catName, dbName, tableName, cols, applyDistinct, filter, ascending,
            order, maxParts);
  }

  @Override public List<Partition> listPartitionsPsWithAuth(String catName, String db_name,
      String tbl_name, List<String> part_vals, short max_parts, String userName,
      List<String> groupNames) throws MetaException, InvalidObjectException, NoSuchObjectException {
    incrementCall();
    return super
        .listPartitionsPsWithAuth(catName, db_name, tbl_name, part_vals, max_parts, userName,
            groupNames);
  }

  @Override public List<String> listPartitionNamesPs(String catName, String dbName,
      String tableName, List<String> part_vals, short max_parts)
      throws MetaException, NoSuchObjectException {
    incrementCall();
    return super.listPartitionNamesPs(catName, dbName, tableName, part_vals, max_parts);
  }

  @Override public List<Partition> getPartitionsByNames(String catName, String dbName,
      String tblName, List<String> partNames) throws MetaException, NoSuchObjectException {
    incrementCall();
    return super.getPartitionsByNames(catName, dbName, tblName, partNames);
  }

  @Override public boolean getPartitionsByExpr(String catName, String dbName, String tblName,
      byte[] expr, String defaultPartitionName, short maxParts, List<Partition> result)
      throws TException {
    incrementCall();
    return super.getPartitionsByExpr(catName, dbName, tblName, expr, defaultPartitionName, maxParts,
        result);
  }

  @Override protected boolean getPartitionsByExprInternal(String catName, String dbName,
      String tblName, byte[] expr, String defaultPartitionName, short maxParts,
      List<Partition> result, boolean allowSql, boolean allowJdo) throws TException {
    incrementCall();
    return super
        .getPartitionsByExprInternal(catName, dbName, tblName, expr, defaultPartitionName, maxParts,
            result, allowSql, allowJdo);
  }

  @Override public List<Partition> getPartitionsByFilter(String catName, String dbName,
      String tblName, String filter, short maxParts) throws MetaException, NoSuchObjectException {
    incrementCall();
    return super.getPartitionsByFilter(catName, dbName, tblName, filter, maxParts);
  }

  @Override public int getNumPartitionsByFilter(String catName, String dbName, String tblName,
      String filter) throws MetaException, NoSuchObjectException {
    incrementCall();
    return super.getNumPartitionsByFilter(catName, dbName, tblName, filter);
  }

  @Override public int getNumPartitionsByExpr(String catName, String dbName, String tblName,
      byte[] expr) throws MetaException, NoSuchObjectException {
    incrementCall();
    return super.getNumPartitionsByExpr(catName, dbName, tblName, expr);
  }

  @Override protected List<Partition> getPartitionsByFilterInternal(String catName, String dbName,
      String tblName, String filter, short maxParts, boolean allowSql, boolean allowJdo)
      throws MetaException, NoSuchObjectException {
    incrementCall();
    return super.getPartitionsByFilterInternal(catName, dbName, tblName, filter, maxParts, allowSql,
        allowJdo);
  }

  @Override public List<Partition> getPartitionSpecsByFilterAndProjection(Table table,
      GetPartitionsProjectionSpec partitionsProjectSpec, GetPartitionsFilterSpec filterSpec)
      throws MetaException, NoSuchObjectException {
    incrementCall();
    return super.getPartitionSpecsByFilterAndProjection(table, partitionsProjectSpec, filterSpec);
  }

  @Override public List<String> listTableNamesByFilter(String catName, String dbName, String filter,
      short maxTables) throws MetaException {
    incrementCall();
    return super.listTableNamesByFilter(catName, dbName, filter, maxTables);
  }

  @Override public List<ColumnStatistics> getTableColumnStatistics(String catName, String dbName,
      String tableName, List<String> colNames) throws MetaException, NoSuchObjectException {
    incrementCall();
    return super.getTableColumnStatistics(catName, dbName, tableName, colNames);
  }

  @Override public ColumnStatistics getTableColumnStatistics(String catName, String dbName,
      String tableName, List<String> colNames, String engine)
      throws MetaException, NoSuchObjectException {
    incrementCall();
    return super.getTableColumnStatistics(catName, dbName, tableName, colNames, engine);
  }

  @Override public ColumnStatistics getTableColumnStatistics(String catName, String dbName,
      String tableName, List<String> colNames, String engine, String writeIdList)
      throws MetaException, NoSuchObjectException {
    incrementCall();
    return super
        .getTableColumnStatistics(catName, dbName, tableName, colNames, engine, writeIdList);
  }

  @Override public List<List<ColumnStatistics>> getPartitionColumnStatistics(String catName,
      String dbName, String tableName, List<String> partNames, List<String> colNames)
      throws MetaException, NoSuchObjectException {
    incrementCall();
    return super.getPartitionColumnStatistics(catName, dbName, tableName, partNames, colNames);
  }

  @Override public List<ColumnStatistics> getPartitionColumnStatistics(String catName,
      String dbName, String tableName, List<String> partNames, List<String> colNames, String engine)
      throws MetaException, NoSuchObjectException {
    incrementCall();
    return super
        .getPartitionColumnStatistics(catName, dbName, tableName, partNames, colNames, engine);
  }

  @Override public List<ColumnStatistics> getPartitionColumnStatistics(String catName,
      String dbName, String tableName, List<String> partNames, List<String> colNames, String engine,
      String writeIdList) throws MetaException, NoSuchObjectException {
    incrementCall();
    return super
        .getPartitionColumnStatistics(catName, dbName, tableName, partNames, colNames, engine,
            writeIdList);
  }

  @Override public AggrStats get_aggr_stats_for(String catName, String dbName, String tblName,
      List<String> partNames, List<String> colNames, String engine, String writeIdList)
      throws MetaException, NoSuchObjectException {
    incrementCall();
    return super
        .get_aggr_stats_for(catName, dbName, tblName, partNames, colNames, engine, writeIdList);
  }

  @Override public List<MetaStoreServerUtils.ColStatsObjWithSourceInfo> getPartitionColStatsForDatabase(
      String catName, String dbName) throws MetaException, NoSuchObjectException {
    incrementCall();
    return super.getPartitionColStatsForDatabase(catName, dbName);
  }

  @Override public List<SQLPrimaryKey> getPrimaryKeys(String catName, String db_name,
      String tbl_name) throws MetaException {
    incrementCall();
    return super.getPrimaryKeys(catName, db_name, tbl_name);
  }

  @Override public List<SQLForeignKey> getForeignKeys(String catName, String parent_db_name,
      String parent_tbl_name, String foreign_db_name, String foreign_tbl_name)
      throws MetaException {
    incrementCall();
    return super.getForeignKeys(catName, parent_db_name, parent_tbl_name, foreign_db_name,
        foreign_tbl_name);
  }

  @Override public List<SQLUniqueConstraint> getUniqueConstraints(String catName, String db_name,
      String tbl_name) throws MetaException {
    incrementCall();
    return super.getUniqueConstraints(catName, db_name, tbl_name);
  }

  @Override public List<SQLNotNullConstraint> getNotNullConstraints(String catName, String db_name,
      String tbl_name) throws MetaException {
    incrementCall();
    return super.getNotNullConstraints(catName, db_name, tbl_name);
  }

  @Override public List<SQLDefaultConstraint> getDefaultConstraints(String catName, String db_name,
      String tbl_name) throws MetaException {
    incrementCall();
    return super.getDefaultConstraints(catName, db_name, tbl_name);
  }

  @Override public List<SQLCheckConstraint> getCheckConstraints(String catName, String db_name,
      String tbl_name) throws MetaException {
    incrementCall();
    return super.getCheckConstraints(catName, db_name, tbl_name);
  }

  @Override public List<RuntimeStat> getRuntimeStats(int maxEntries, int maxCreateTime)
      throws MetaException {
    incrementCall();
    return super.getRuntimeStats(maxEntries, maxCreateTime);
  }
}
