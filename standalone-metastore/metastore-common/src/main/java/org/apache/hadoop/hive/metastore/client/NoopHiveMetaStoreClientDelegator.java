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

package org.apache.hadoop.hive.metastore.client;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.api.Package;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class NoopHiveMetaStoreClientDelegator implements IMetaStoreClient {
  // effectively final
  private IMetaStoreClient delegate;

  public NoopHiveMetaStoreClientDelegator() {
  }

  public NoopHiveMetaStoreClientDelegator(IMetaStoreClient delegate) {
    this.delegate = delegate;
  }

  public IMetaStoreClient getDelegate() {
    return delegate;
  }

  public final void setDelegate(IMetaStoreClient delegate) {
    synchronized (this) {
      if (this.delegate == null) {
        this.delegate = delegate;
      }
    }
  }

  @Override
  public boolean isCompatibleWith(Configuration conf) {
    return delegate.isCompatibleWith(conf);
  }

  @Override
  public void setHiveAddedJars(String addedJars) {
    delegate.setHiveAddedJars(addedJars);
  }

  @Override
  public boolean isLocalMetaStore() {
    return delegate.isLocalMetaStore();
  }

  @Override
  public void reconnect() throws MetaException {
    delegate.isLocalMetaStore();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public void setMetaConf(String key, String value) throws MetaException, TException {
    delegate.setMetaConf(key, value);
  }

  @Override
  public String getMetaConf(String key) throws MetaException, TException {
    return delegate.getMetaConf(key);
  }

  @Override
  public void createCatalog(Catalog catalog)
      throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    delegate.createCatalog(catalog);
  }

  @Override
  public void alterCatalog(String catalogName, Catalog newCatalog)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    delegate.alterCatalog(catalogName, newCatalog);
  }

  @Override
  public Catalog getCatalog(String catName) throws NoSuchObjectException, MetaException, TException {
    return delegate.getCatalog(catName);
  }

  @Override
  public List<String> getCatalogs() throws MetaException, TException {
    return delegate.getCatalogs();
  }

  @Override
  public void dropCatalog(String catName)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    delegate.dropCatalog(catName);
  }

  @Override
  public void dropCatalog(String catName, boolean ifExists) throws TException {
    delegate.dropCatalog(catName, ifExists);
  }

  @Override
  public List<String> getDatabases(String databasePattern) throws MetaException, TException {
    return delegate.getDatabases(databasePattern);
  }

  @Override
  public List<String> getDatabases(String catName, String databasePattern) throws MetaException, TException {
    return delegate.getDatabases(catName, databasePattern);
  }

  @Override
  public List<String> getAllDatabases() throws MetaException, TException {
    return delegate.getAllDatabases();
  }

  @Override
  public List<String> getAllDatabases(String catName) throws MetaException, TException {
    return delegate.getAllDatabases(catName);
  }

  @Override
  public List<String> getTables(String dbName, String tablePattern)
      throws MetaException, TException, UnknownDBException {
    return delegate.getTables(dbName, tablePattern);
  }

  @Override
  public List<String> getTables(String catName, String dbName, String tablePattern)
      throws MetaException, TException, UnknownDBException {
    return delegate.getTables(catName, dbName, tablePattern);
  }

  @Override
  public List<String> getTables(String dbName, String tablePattern, TableType tableType)
      throws MetaException, TException, UnknownDBException {
    return delegate.getTables(dbName, tablePattern, tableType);
  }

  @Override
  public List<String> getTables(String catName, String dbName, String tablePattern, TableType tableType)
      throws MetaException, TException, UnknownDBException {
    return delegate.getTables(catName, dbName, tablePattern, tableType);
  }

  @Override
  public List<Table> getAllMaterializedViewObjectsForRewriting()
      throws MetaException, TException, UnknownDBException {
    return delegate.getAllMaterializedViewObjectsForRewriting();
  }

  @Override
  public List<ExtendedTableInfo> getTablesExt(String catName, String dbName, String tablePattern,
      int requestedFields, int limit) throws MetaException, TException {
    return delegate.getTablesExt(catName, dbName, tablePattern, requestedFields, limit);
  }

  @Override
  public List<String> getMaterializedViewsForRewriting(String dbName)
      throws MetaException, TException, UnknownDBException {
    return delegate.getMaterializedViewsForRewriting(dbName);
  }

  @Override
  public List<String> getMaterializedViewsForRewriting(String catName, String dbName)
      throws MetaException, TException, UnknownDBException {
    return delegate.getMaterializedViewsForRewriting(catName, dbName);
  }

  @Override
  public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes)
      throws MetaException, TException, UnknownDBException {
    return delegate.getTableMeta(dbPatterns, tablePatterns, tableTypes);
  }

  @Override
  public List<TableMeta> getTableMeta(String catName, String dbPatterns, String tablePatterns,
      List<String> tableTypes) throws MetaException, TException, UnknownDBException {
    return delegate.getTableMeta(catName, dbPatterns, tablePatterns, tableTypes);
  }

  @Override
  public List<String> getAllTables(String dbName) throws MetaException, TException, UnknownDBException {
    return delegate.getAllTables(dbName);
  }

  @Override
  public List<String> getAllTables(String catName, String dbName)
      throws MetaException, TException, UnknownDBException {
    return delegate.getAllTables(catName, dbName);
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter, short maxTables)
      throws TException, InvalidOperationException, UnknownDBException {
    return delegate.listTableNamesByFilter(dbName, filter, maxTables);
  }

  @Override
  public List<String> listTableNamesByFilter(String catName, String dbName, String filter, int maxTables)
      throws TException, InvalidOperationException, UnknownDBException {
    return delegate.listTableNamesByFilter(catName, dbName, filter, maxTables);
  }

  @Override
  public void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab)
      throws MetaException, TException, NoSuchObjectException {
    delegate.dropTable(dbname, tableName, deleteData, ignoreUnknownTab);
  }

  @Override
  public void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab,
      boolean ifPurge) throws MetaException, TException, NoSuchObjectException {
    delegate.dropTable(dbname, tableName, deleteData, ignoreUnknownTab, ifPurge);
  }

  @Override
  public void dropTable(Table table, boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge)
      throws TException {
    delegate.dropTable(table, deleteData, ignoreUnknownTab, ifPurge);
  }

  @Override
  public void dropTable(String dbname, String tableName)
      throws MetaException, TException, NoSuchObjectException {
    delegate.dropTable(dbname, tableName);
  }

  @Override
  public void dropTable(String catName, String dbName, String tableName, boolean deleteData,
      boolean ignoreUnknownTable, boolean ifPurge) throws MetaException, NoSuchObjectException, TException {
    delegate.dropTable(catName, dbName, tableName, deleteData, ignoreUnknownTable, ifPurge);
  }

  @Override
  public void truncateTable(String dbName, String tableName, List<String> partNames)
      throws MetaException, TException {
    delegate.truncateTable(dbName, tableName, partNames);
  }

  @Override
  public void truncateTable(TableName table, List<String> partNames) throws TException {
    delegate.truncateTable(table, partNames);
  }

  @Override
  public void truncateTable(String dbName, String tableName, List<String> partNames, String validWriteIds,
      long writeId) throws TException {
    delegate.truncateTable(dbName, tableName, partNames, validWriteIds, writeId);
  }

  @Override
  public void truncateTable(String dbName, String tableName, List<String> partNames, String validWriteIds,
      long writeId, boolean deleteData) throws TException {
    delegate.truncateTable(dbName, tableName, partNames, validWriteIds, writeId, deleteData);
  }

  @Override
  public void truncateTable(String catName, String dbName, String tableName, List<String> partNames)
      throws MetaException, TException {
    delegate.truncateTable(catName, dbName, tableName, partNames);
  }

  @Override
  public CmRecycleResponse recycleDirToCmPath(CmRecycleRequest request) throws MetaException, TException {
    return delegate.recycleDirToCmPath(request);
  }

  @Override
  public boolean tableExists(String databaseName, String tableName)
      throws MetaException, TException, UnknownDBException {
    return delegate.tableExists(databaseName, tableName);
  }

  @Override
  public boolean tableExists(String catName, String dbName, String tableName)
      throws MetaException, TException, UnknownDBException {
    return delegate.tableExists(catName, dbName, tableName);
  }

  @Override
  public Database getDatabase(String databaseName) throws NoSuchObjectException, MetaException, TException {
    return delegate.getDatabase(databaseName);
  }

  @Override
  public Database getDatabase(String catalogName, String databaseName)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.getDatabase(catalogName, databaseName);
  }

  @Override
  public Table getTable(String dbName, String tableName)
      throws MetaException, TException, NoSuchObjectException {
    return delegate.getTable(dbName, tableName);
  }

  @Override
  public Table getTable(String dbName, String tableName, boolean getColumnStats, String engine)
      throws MetaException, TException, NoSuchObjectException {
    return delegate.getTable(dbName, tableName, getColumnStats, engine);
  }

  @Override
  public Table getTable(String catName, String dbName, String tableName) throws MetaException, TException {
    return delegate.getTable(catName, dbName, tableName);
  }

  @Override
  public Table getTable(String catName, String dbName, String tableName, String validWriteIdList)
      throws TException {
    return delegate.getTable(catName, dbName, tableName, validWriteIdList);
  }

  @Override
  public Table getTable(String catName, String dbName, String tableName, String validWriteIdList,
      boolean getColumnStats, String engine) throws TException {
    return delegate.getTable(catName, dbName, tableName, validWriteIdList, getColumnStats, engine);
  }

  @Override
  public Table getTable(GetTableRequest getTableRequest)
      throws MetaException, TException, NoSuchObjectException {
    return delegate.getTable(getTableRequest);
  }

  @Override
  public List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    return delegate.getTableObjectsByName(dbName, tableNames);
  }

  @Override
  public List<Table> getTables(String catName, String dbName, List<String> tableNames,
      GetProjectionsSpec projectionsSpec)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    return delegate.getTables(catName, dbName, tableNames, projectionsSpec);
  }

  @Override
  public List<Table> getTableObjectsByName(String catName, String dbName, List<String> tableNames)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    return delegate.getTableObjectsByName(catName, dbName, tableNames);
  }

  @Override
  public Materialization getMaterializationInvalidationInfo(CreationMetadata cm, String validTxnList)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    return delegate.getMaterializationInvalidationInfo(cm, validTxnList);
  }

  @Override
  public void updateCreationMetadata(String dbName, String tableName, CreationMetadata cm)
      throws MetaException, TException {
    delegate.updateCreationMetadata(dbName, tableName, cm);
  }

  @Override
  public void updateCreationMetadata(String catName, String dbName, String tableName, CreationMetadata cm)
      throws MetaException, TException {
    delegate.updateCreationMetadata(catName, dbName, tableName, cm);
  }

  @Override
  public Partition appendPartition(String dbName, String tableName, List<String> partVals)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return delegate.appendPartition(dbName, tableName, partVals);
  }

  @Override
  public Partition appendPartition(String catName, String dbName, String tableName, List<String> partVals)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return delegate.appendPartition(catName, dbName, tableName, partVals);
  }

  @Override
  public Partition appendPartition(String dbName, String tableName, String name)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return delegate.appendPartition(dbName, tableName, name);
  }

  @Override
  public Partition appendPartition(String catName, String dbName, String tableName, String name)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return delegate.appendPartition(catName, dbName, tableName, name);
  }

  @Override
  public Partition add_partition(Partition partition)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return delegate.add_partition(partition);
  }

  @Override
  public int add_partitions(List<Partition> partitions)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return delegate.add_partitions(partitions);
  }

  @Override
  public int add_partitions_pspec(PartitionSpecProxy partitionSpec)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return delegate.add_partitions_pspec(partitionSpec);
  }

  @Override
  public List<Partition> add_partitions(List<Partition> partitions, boolean ifNotExists, boolean needResults)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return delegate.add_partitions(partitions,ifNotExists, needResults);
  }

  @Override
  public Partition getPartition(String dbName, String tblName, List<String> partVals)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.getPartition(dbName, tblName, partVals);
  }

  @Override
  public GetPartitionResponse getPartitionRequest(GetPartitionRequest req)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.getPartitionRequest(req);
  }

  @Override
  public Partition getPartition(String catName, String dbName, String tblName, List<String> partVals)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.getPartition(catName, dbName, tblName, partVals);
  }

  @Override
  public Partition exchange_partition(Map<String, String> partitionSpecs, String sourceDb, String sourceTable,
      String destdb, String destTableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    return delegate.exchange_partition(partitionSpecs, sourceDb, sourceTable, destdb, destTableName);
  }

  @Override
  public Partition exchange_partition(Map<String, String> partitionSpecs, String sourceCat, String sourceDb,
      String sourceTable, String destCat, String destdb, String destTableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    return delegate.exchange_partition(partitionSpecs, sourceCat, sourceDb, sourceTable, destCat, destdb,
        destTableName);
  }

  @Override
  public List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String sourceDb,
      String sourceTable, String destdb, String destTableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    return delegate.exchange_partitions(partitionSpecs, sourceDb, sourceTable, destdb, destTableName);
  }

  @Override
  public List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String sourceCat,
      String sourceDb, String sourceTable, String destCat, String destdb, String destTableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    return delegate.exchange_partitions(partitionSpecs, sourceCat, sourceDb, sourceTable, destCat, destdb,
        destTableName);
  }

  @Override
  public Partition getPartition(String dbName, String tblName, String name)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    return delegate.getPartition(dbName, tblName, name);
  }

  @Override
  public Partition getPartition(String catName, String dbName, String tblName, String name)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    return delegate.getPartition(catName, dbName, tblName, name);
  }

  @Override
  public Partition getPartitionWithAuthInfo(String dbName, String tableName, List<String> pvals,
      String userName, List<String> groupNames)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    return delegate.getPartitionWithAuthInfo(dbName, tableName, pvals, userName, groupNames);
  }

  @Override
  public Partition getPartitionWithAuthInfo(String catName, String dbName, String tableName,
      List<String> pvals, String userName, List<String> groupNames)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    return delegate.getPartitionWithAuthInfo(catName, dbName, tableName, pvals, userName, groupNames);
  }

  @Override
  public List<Partition> listPartitions(String db_name, String tbl_name, short max_parts)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.listPartitions(db_name, tbl_name, max_parts);
  }

  @Override
  public List<Partition> listPartitions(String catName, String db_name, String tbl_name, int max_parts)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.listPartitions(catName, db_name, tbl_name, max_parts);
  }

  @Override
  public PartitionSpecProxy listPartitionSpecs(String dbName, String tableName, int maxParts)
      throws TException {
    return delegate.listPartitionSpecs(dbName, tableName, maxParts);
  }

  @Override
  public PartitionSpecProxy listPartitionSpecs(String catName, String dbName, String tableName, int maxParts)
      throws TException {
    return delegate.listPartitionSpecs(catName, dbName, tableName, maxParts);
  }

  @Override
  public List<Partition> listPartitions(String db_name, String tbl_name, List<String> part_vals,
      short max_parts) throws NoSuchObjectException, MetaException, TException {
    return delegate.listPartitions(db_name, tbl_name, part_vals, max_parts);
  }

  @Override
  public List<Partition> listPartitions(String catName, String db_name, String tbl_name,
      List<String> part_vals, int max_parts) throws NoSuchObjectException, MetaException, TException {
    return delegate.listPartitions(catName, db_name, tbl_name, part_vals, max_parts);
  }

  @Override
  public List<String> listPartitionNames(String db_name, String tbl_name, short max_parts)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.listPartitionNames(db_name, tbl_name, max_parts);
  }

  @Override
  public GetPartitionNamesPsResponse listPartitionNamesRequest(GetPartitionNamesPsRequest req)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.listPartitionNamesRequest(req);
  }

  @Override
  public List<String> listPartitionNames(String catName, String db_name, String tbl_name, int max_parts)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.listPartitionNames(catName, db_name, tbl_name, max_parts);
  }

  @Override
  public List<String> listPartitionNames(String db_name, String tbl_name, List<String> part_vals,
      short max_parts) throws MetaException, TException, NoSuchObjectException {
    return delegate.listPartitionNames(db_name, tbl_name, part_vals, max_parts);
  }

  @Override
  public List<String> listPartitionNames(String catName, String db_name, String tbl_name,
      List<String> part_vals, int max_parts) throws MetaException, TException, NoSuchObjectException {
    return delegate.listPartitionNames(catName, db_name, tbl_name, part_vals, max_parts);
  }

  @Override
  public List<String> listPartitionNames(PartitionsByExprRequest request)
      throws MetaException, TException, NoSuchObjectException {
    return delegate.listPartitionNames(request);
  }

  @Override
  public PartitionValuesResponse listPartitionValues(PartitionValuesRequest request)
      throws MetaException, TException, NoSuchObjectException {
    return delegate.listPartitionValues(request);
  }

  @Override
  public int getNumPartitionsByFilter(String dbName, String tableName, String filter)
      throws MetaException, NoSuchObjectException, TException {
    return delegate.getNumPartitionsByFilter(dbName, tableName, filter);
  }

  @Override
  public int getNumPartitionsByFilter(String catName, String dbName, String tableName, String filter)
      throws MetaException, NoSuchObjectException, TException {
    return delegate.getNumPartitionsByFilter(catName, dbName, tableName, filter);
  }

  @Override
  public List<Partition> listPartitionsByFilter(String db_name, String tbl_name, String filter,
      short max_parts) throws MetaException, NoSuchObjectException, TException {
    return delegate.listPartitionsByFilter(db_name, tbl_name, filter, max_parts);
  }

  @Override
  public List<Partition> listPartitionsByFilter(String catName, String db_name, String tbl_name,
      String filter, int max_parts) throws MetaException, NoSuchObjectException, TException {
    return delegate.listPartitionsByFilter(catName, db_name, tbl_name, filter, max_parts);
  }

  @Override
  public PartitionSpecProxy listPartitionSpecsByFilter(String db_name, String tbl_name, String filter,
      int max_parts) throws MetaException, NoSuchObjectException, TException {
    return delegate.listPartitionSpecsByFilter(db_name, tbl_name, filter, max_parts);
  }

  @Override
  public PartitionSpecProxy listPartitionSpecsByFilter(String catName, String db_name, String tbl_name,
      String filter, int max_parts) throws MetaException, NoSuchObjectException, TException {
    return delegate.listPartitionSpecsByFilter(catName, db_name, tbl_name, filter, max_parts);
  }

  @Override
  public boolean listPartitionsSpecByExpr(PartitionsByExprRequest req, List<PartitionSpec> result)
      throws TException {
    return delegate.listPartitionsSpecByExpr(req, result);
  }

  @Override
  public boolean listPartitionsByExpr(String db_name, String tbl_name, byte[] expr,
      String default_partition_name, short max_parts, List<Partition> result) throws TException {
    return delegate.listPartitionsByExpr(db_name, tbl_name, expr, default_partition_name, max_parts, result);
  }

  @Override
  public boolean listPartitionsByExpr(String catName, String db_name, String tbl_name, byte[] expr,
      String default_partition_name, int max_parts, List<Partition> result) throws TException {
    return delegate.listPartitionsByExpr(catName, db_name, tbl_name, expr, default_partition_name, max_parts,
        result);
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String dbName, String tableName, short maxParts,
      String userName, List<String> groupNames) throws MetaException, TException, NoSuchObjectException {
    return delegate.listPartitionsWithAuthInfo(dbName, tableName, maxParts, userName, groupNames);
  }

  @Override
  public GetPartitionsPsWithAuthResponse listPartitionsWithAuthInfoRequest(
      GetPartitionsPsWithAuthRequest req) throws MetaException, TException, NoSuchObjectException {
    return delegate.listPartitionsWithAuthInfoRequest(req);
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
      int maxParts, String userName, List<String> groupNames)
      throws MetaException, TException, NoSuchObjectException {
    return delegate.listPartitionsWithAuthInfo(catName, dbName, tableName, maxParts, userName, groupNames);
  }

  @Override
  public List<Partition> getPartitionsByNames(String db_name, String tbl_name, List<String> part_names)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.getPartitionsByNames(db_name, tbl_name, part_names);
  }

  @Override
  public List<Partition> getPartitionsByNames(String catName, String db_name, String tbl_name,
      List<String> part_names) throws NoSuchObjectException, MetaException, TException {
    return delegate.getPartitionsByNames(catName, db_name, tbl_name, part_names);
  }

  @Override
  public PartitionsResponse getPartitionsRequest(PartitionsRequest req)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.getPartitionsRequest(req);
  }

  @Override
  public GetPartitionsByNamesResult getPartitionsByNames(GetPartitionsByNamesRequest req) throws TException {
    return delegate.getPartitionsByNames(req);
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String dbName, String tableName,
      List<String> partialPvals, short maxParts, String userName, List<String> groupNames)
      throws MetaException, TException, NoSuchObjectException {
    return delegate.listPartitionsWithAuthInfo(dbName, tableName, partialPvals, maxParts, userName,
        groupNames);
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
      List<String> partialPvals, int maxParts, String userName, List<String> groupNames)
      throws MetaException, TException, NoSuchObjectException {
    return delegate.listPartitionsWithAuthInfo(catName, dbName, tableName, maxParts, userName, groupNames);
  }

  @Override
  public void markPartitionForEvent(String db_name, String tbl_name, Map<String, String> partKVs,
      PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException,
      UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {
    delegate.markPartitionForEvent(db_name, tbl_name, partKVs, eventType);
  }

  @Override
  public void markPartitionForEvent(String catName, String db_name, String tbl_name,
      Map<String, String> partKVs, PartitionEventType eventType) throws MetaException, NoSuchObjectException,
      TException, UnknownTableException, UnknownDBException, UnknownPartitionException,
      InvalidPartitionException {
    delegate.markPartitionForEvent(catName, db_name, tbl_name, partKVs, eventType);
  }

  @Override
  public boolean isPartitionMarkedForEvent(String db_name, String tbl_name, Map<String, String> partKVs,
      PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException,
      UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {
    return delegate.isPartitionMarkedForEvent(db_name, tbl_name, partKVs, eventType);
  }

  @Override
  public boolean isPartitionMarkedForEvent(String catName, String db_name, String tbl_name,
      Map<String, String> partKVs, PartitionEventType eventType) throws MetaException, NoSuchObjectException,
      TException, UnknownTableException, UnknownDBException, UnknownPartitionException,
      InvalidPartitionException {
    return delegate.isPartitionMarkedForEvent(catName, db_name, tbl_name, partKVs, eventType);
  }

  @Override
  public void validatePartitionNameCharacters(List<String> partVals) throws TException, MetaException {
    delegate.validatePartitionNameCharacters(partVals);
  }

  @Override
  public Table getTranslateTableDryrun(Table tbl) throws AlreadyExistsException, InvalidObjectException,
      MetaException, NoSuchObjectException, TException {
    return delegate.getTranslateTableDryrun(tbl);
  }

  @Override
  public void createTable(Table tbl) throws AlreadyExistsException, InvalidObjectException, MetaException,
      NoSuchObjectException, TException {
    delegate.createTable(tbl);
  }

  @Override
  public void createTable(CreateTableRequest request) throws AlreadyExistsException, InvalidObjectException,
      MetaException, NoSuchObjectException, TException {
    delegate.createTable(request);
  }

  @Override
  public void alter_table(String databaseName, String tblName, Table table)
      throws InvalidOperationException, MetaException, TException {
    delegate.alter_table(databaseName, tblName, table);
  }

  @Override
  public void alter_table(String catName, String dbName, String tblName, Table newTable,
      EnvironmentContext envContext) throws InvalidOperationException, MetaException, TException {
    delegate.alter_table(catName, dbName, tblName, newTable, envContext);
  }

  @Override
  public void alter_table(String defaultDatabaseName, String tblName, Table table, boolean cascade)
      throws InvalidOperationException, MetaException, TException {
    delegate.alter_table(defaultDatabaseName, tblName, table, cascade);
  }

  @Override
  public void alter_table_with_environmentContext(String databaseName, String tblName, Table table,
      EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
    delegate.alter_table_with_environmentContext(databaseName, tblName, table, environmentContext);
  }

  @Override
  public void alter_table(String catName, String databaseName, String tblName, Table table,
      EnvironmentContext environmentContext, String validWriteIdList)
      throws InvalidOperationException, MetaException, TException {
    delegate.alter_table(catName, databaseName, tblName, table, environmentContext, validWriteIdList);
  }

  @Override
  public void createDatabase(Database db)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    delegate.createDatabase(db);
  }

  @Override
  public void dropDatabase(String name)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    delegate.dropDatabase(name);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    delegate.dropDatabase(name, deleteData, ignoreUnknownDb);
  }

  @Override
  public void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    delegate.dropDatabase(name, deleteData, ignoreUnknownDb, cascade);
  }

  @Override
  public void dropDatabase(DropDatabaseRequest req) throws TException {
    delegate.dropDatabase(req);
  }

  @Override
  public void alterDatabase(String name, Database db)
      throws NoSuchObjectException, MetaException, TException {
    delegate.alterDatabase(name, db);
  }

  @Override
  public void alterDatabase(String catName, String dbName, Database newDb)
      throws NoSuchObjectException, MetaException, TException {
    delegate.alterDatabase(catName, dbName, newDb);
  }

  @Override
  public void createDataConnector(DataConnector connector)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    delegate.createDataConnector(connector);
  }

  @Override
  public void dropDataConnector(String name, boolean ifNotExists, boolean checkReferences)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    delegate.dropDataConnector(name, ifNotExists, checkReferences);
  }

  @Override
  public void alterDataConnector(String name, DataConnector connector)
      throws NoSuchObjectException, MetaException, TException {
    delegate.alterDataConnector(name, connector);
  }

  @Override
  public DataConnector getDataConnector(String name) throws MetaException, TException {
    return delegate.getDataConnector(name);
  }

  @Override
  public List<String> getAllDataConnectorNames() throws MetaException, TException {
    return delegate.getAllDataConnectorNames();
  }

  @Override
  public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.dropPartition(db_name, tbl_name, part_vals, deleteData);
  }

  @Override
  public boolean dropPartition(String catName, String db_name, String tbl_name, List<String> part_vals,
      boolean deleteData) throws NoSuchObjectException, MetaException, TException {
    return delegate.dropPartition(catName, db_name, tbl_name, part_vals, deleteData);
  }

  @Override
  public boolean dropPartition(String db_name, String tbl_name, List<String> part_vals,
      PartitionDropOptions options) throws NoSuchObjectException, MetaException, TException {
    return delegate.dropPartition(db_name, tbl_name, part_vals, options);
  }

  @Override
  public boolean dropPartition(String catName, String db_name, String tbl_name, List<String> part_vals,
      PartitionDropOptions options) throws NoSuchObjectException, MetaException, TException {
    return delegate.dropPartition(catName, db_name, tbl_name, part_vals, options);
  }

  @Override
  public List<Partition> dropPartitions(String dbName, String tblName, List<Pair<Integer, byte[]>> partExprs,
      boolean deleteData, boolean ifExists) throws NoSuchObjectException, MetaException, TException {
    return delegate.dropPartitions(dbName, tblName, partExprs, deleteData, ifExists);
  }

  @Override
  public List<Partition> dropPartitions(String dbName, String tblName, List<Pair<Integer, byte[]>> partExprs,
      boolean deleteData, boolean ifExists, boolean needResults)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.dropPartitions(dbName, tblName, partExprs, deleteData, ifExists, needResults);
  }

  @Override
  public List<Partition> dropPartitions(String dbName, String tblName, List<Pair<Integer, byte[]>> partExprs,
      PartitionDropOptions options) throws NoSuchObjectException, MetaException, TException {
    return delegate.dropPartitions(dbName, tblName, partExprs, options);
  }

  @Override
  public List<Partition> dropPartitions(String catName, String dbName, String tblName,
      List<Pair<Integer, byte[]>> partExprs, PartitionDropOptions options)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.dropPartitions(catName, dbName, tblName, partExprs, options);
  }

  @Override
  public boolean dropPartition(String db_name, String tbl_name, String name, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.dropPartition(db_name, tbl_name, name, deleteData);
  }

  @Override
  public boolean dropPartition(String catName, String db_name, String tbl_name, String name,
      boolean deleteData) throws NoSuchObjectException, MetaException, TException {
    return delegate.dropPartition(catName, db_name, tbl_name, name, deleteData);
  }

  @Override
  public void alter_partition(String dbName, String tblName, Partition newPart)
      throws InvalidOperationException, MetaException, TException {
    delegate.alter_partition(dbName, tblName, newPart);
  }

  @Override
  public void alter_partition(String dbName, String tblName, Partition newPart,
      EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
    delegate.alter_partition(dbName, tblName, newPart, environmentContext);
  }

  @Override
  public void alter_partition(String catName, String dbName, String tblName, Partition newPart,
      EnvironmentContext environmentContext, String writeIdList)
      throws InvalidOperationException, MetaException, TException {
    delegate.alter_partition(catName, dbName, tblName, newPart, environmentContext, writeIdList);
  }

  @Override
  public void alter_partition(String catName, String dbName, String tblName, Partition newPart,
      EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
    delegate.alter_partition(catName, dbName, tblName, newPart, environmentContext);
  }

  @Override
  public void alter_partitions(String dbName, String tblName, List<Partition> newParts)
      throws InvalidOperationException, MetaException, TException {
    delegate.alter_partitions(dbName, tblName, newParts);
  }

  @Override
  public void alter_partitions(String dbName, String tblName, List<Partition> newParts,
      EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
    delegate.alter_partitions(dbName, tblName, newParts, environmentContext);
  }

  @Override
  public void alter_partitions(String dbName, String tblName, List<Partition> newParts,
      EnvironmentContext environmentContext, String writeIdList, long writeId)
      throws InvalidOperationException, MetaException, TException {
    delegate.alter_partitions(dbName, tblName, newParts, environmentContext, writeIdList, writeId);
  }

  @Override
  public void alter_partitions(String catName, String dbName, String tblName, List<Partition> newParts,
      EnvironmentContext environmentContext, String writeIdList, long writeId)
      throws InvalidOperationException, MetaException, TException {
    delegate.alter_partitions(catName, dbName, tblName, newParts, environmentContext, writeIdList, writeId);
  }

  @Override
  public void renamePartition(String dbname, String tableName, List<String> part_vals, Partition newPart)
      throws InvalidOperationException, MetaException, TException {
    delegate.renamePartition(dbname, tableName, part_vals, newPart);
  }

  @Override
  public void renamePartition(String catName, String dbname, String tableName, List<String> part_vals,
      Partition newPart, String validWriteIds, long txnId, boolean makeCopy) throws TException {
    delegate.renamePartition(catName, dbname, tableName, part_vals, newPart, validWriteIds, txnId, makeCopy);
  }

  @Override
  public List<FieldSchema> getFields(String db, String tableName) throws MetaException, TException,
      UnknownTableException, UnknownDBException {
    return delegate.getFields(db, tableName);
  }

  @Override
  public List<FieldSchema> getFields(String catName, String db, String tableName) throws MetaException,
      TException, UnknownTableException, UnknownDBException {
    return delegate.getFields(catName, db, tableName);
  }

  @Override
  public GetFieldsResponse getFieldsRequest(GetFieldsRequest req) throws MetaException, TException,
      UnknownTableException, UnknownDBException {
    return delegate.getFieldsRequest(req);
  }

  @Override
  public List<FieldSchema> getSchema(String db, String tableName) throws MetaException, TException,
      UnknownTableException, UnknownDBException {
    return delegate.getSchema(db, tableName);
  }

  @Override
  public List<FieldSchema> getSchema(String catName, String db, String tableName) throws MetaException,
      TException, UnknownTableException, UnknownDBException {
    return delegate.getSchema(catName, db, tableName);
  }

  @Override
  public GetSchemaResponse getSchemaRequest(GetSchemaRequest req) throws MetaException, TException,
      UnknownTableException, UnknownDBException {
    return delegate.getSchemaRequest(req);
  }

  @Override
  public String getConfigValue(String name, String defaultValue)
      throws TException, ConfigValSecurityException {
    return delegate.getConfigValue(name, defaultValue);
  }

  @Override
  public List<String> partitionNameToVals(String name) throws MetaException, TException {
    return delegate.partitionNameToVals(name);
  }

  @Override
  public Map<String, String> partitionNameToSpec(String name) throws MetaException, TException {
    return delegate.partitionNameToSpec(name);
  }

  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics statsObj)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
    return delegate.updateTableColumnStatistics(statsObj);
  }

  @Override
  public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
    return delegate.updatePartitionColumnStatistics(statsObj);
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames, String engine) throws NoSuchObjectException, MetaException, TException {
    return delegate.getTableColumnStatistics(dbName, tableName, colNames, engine);
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames, String engine, String validWriteIdList)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.getTableColumnStatistics(dbName, tableName, colNames, engine, validWriteIdList);
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String catName, String dbName, String tableName,
      List<String> colNames, String engine) throws NoSuchObjectException, MetaException, TException {
    return delegate.getTableColumnStatistics(catName, dbName, tableName, colNames, engine);
  }

  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String catName, String dbName, String tableName,
      List<String> colNames, String engine, String validWriteIdList)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.getTableColumnStatistics(catName, dbName, tableName, colNames, engine, validWriteIdList);
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName, String tableName,
      List<String> partNames, List<String> colNames, String engine)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.getPartitionColumnStatistics(dbName, tableName, partNames, colNames, engine);
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName, String tableName,
      List<String> partNames, List<String> colNames, String engine, String validWriteIdList)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.getPartitionColumnStatistics(dbName, tableName, partNames, colNames, engine,
        validWriteIdList);
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String catName, String dbName,
      String tableName, List<String> partNames, List<String> colNames, String engine)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.getPartitionColumnStatistics(catName, dbName, tableName, partNames, colNames, engine);
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String catName, String dbName,
      String tableName, List<String> partNames, List<String> colNames, String engine, String validWriteIdList)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.getPartitionColumnStatistics(catName, dbName, tableName, partNames, colNames, engine,
        validWriteIdList);
  }

  @Override
  public boolean deleteColumnStatistics(DeleteColumnStatisticsRequest req) throws TException {
    return delegate.deleteColumnStatistics(req);
  }

  @Override
  public void updateTransactionalStatistics(UpdateTransactionalStatsRequest req) throws TException {
    delegate.updateTransactionalStatistics(req);
  }

  @Override
  public boolean create_role(Role role) throws MetaException, TException {
    return delegate.create_role(role);
  }

  @Override
  public boolean drop_role(String role_name) throws MetaException, TException {
    return delegate.drop_role(role_name);
  }

  @Override
  public List<String> listRoleNames() throws MetaException, TException {
    return delegate.listRoleNames();
  }

  @Override
  public boolean grant_role(String role_name, String user_name, PrincipalType principalType, String grantor,
      PrincipalType grantorType, boolean grantOption) throws MetaException, TException {
    return delegate.grant_role(role_name, user_name, principalType, grantor, grantorType, grantOption);
  }

  @Override
  public boolean revoke_role(String role_name, String user_name, PrincipalType principalType,
      boolean grantOption) throws MetaException, TException {
    return delegate.revoke_role(role_name, user_name, principalType, grantOption);
  }

  @Override
  public List<Role> list_roles(String principalName, PrincipalType principalType)
      throws MetaException, TException {
    return delegate.list_roles(principalName, principalType);
  }

  @Override
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject, String user_name,
      List<String> group_names) throws MetaException, TException {
    return delegate.get_privilege_set(hiveObject, user_name, group_names);
  }

  @Override
  public List<HiveObjectPrivilege> list_privileges(String principal_name, PrincipalType principal_type,
      HiveObjectRef hiveObject) throws MetaException, TException {
    return delegate.list_privileges(principal_name, principal_type, hiveObject);
  }

  @Override
  public boolean grant_privileges(PrivilegeBag privileges) throws MetaException, TException {
    return delegate.grant_privileges(privileges);
  }

  @Override
  public boolean revoke_privileges(PrivilegeBag privileges, boolean grantOption)
      throws MetaException, TException {
    return delegate.revoke_privileges(privileges, grantOption);
  }

  @Override
  public boolean refresh_privileges(HiveObjectRef objToRefresh, String authorizer,
      PrivilegeBag grantPrivileges) throws MetaException, TException {
    return delegate.refresh_privileges(objToRefresh, authorizer, grantPrivileges);
  }

  @Override
  public String getDelegationToken(String owner, String renewerKerberosPrincipalName)
      throws MetaException, TException {
    return delegate.getDelegationToken(owner, renewerKerberosPrincipalName);
  }

  @Override
  public long renewDelegationToken(String tokenStrForm) throws MetaException, TException {
    return delegate.renewDelegationToken(tokenStrForm);
  }

  @Override
  public void cancelDelegationToken(String tokenStrForm) throws MetaException, TException {
    delegate.cancelDelegationToken(tokenStrForm);
  }

  @Override
  public String getTokenStrForm() throws IOException {
    return delegate.getTokenStrForm();
  }

  @Override
  public boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
    return delegate.addToken(tokenIdentifier, delegationToken);
  }

  @Override
  public boolean removeToken(String tokenIdentifier) throws TException {
    return delegate.removeToken(tokenIdentifier);
  }

  @Override
  public String getToken(String tokenIdentifier) throws TException {
    return delegate.getToken(tokenIdentifier);
  }

  @Override
  public List<String> getAllTokenIdentifiers() throws TException {
    return delegate.getAllTokenIdentifiers();
  }

  @Override
  public int addMasterKey(String key) throws MetaException, TException {
    return delegate.addMasterKey(key);
  }

  @Override
  public void updateMasterKey(Integer seqNo, String key)
      throws NoSuchObjectException, MetaException, TException {
    delegate.updateMasterKey(seqNo, key);
  }

  @Override
  public boolean removeMasterKey(Integer keySeq) throws TException {
    return delegate.removeMasterKey(keySeq);
  }

  @Override
  public String[] getMasterKeys() throws TException {
    return delegate.getMasterKeys();
  }

  @Override
  public void createFunction(Function func) throws InvalidObjectException, MetaException, TException {
    delegate.createFunction(func);
  }

  @Override
  public void alterFunction(String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException, TException {
    delegate.alterFunction(dbName, funcName, newFunction);
  }

  @Override
  public void alterFunction(String catName, String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException, TException {
    delegate.alterFunction(catName, dbName, funcName, newFunction);
  }

  @Override
  public void dropFunction(String dbName, String funcName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
    delegate.dropFunction(dbName, funcName);
  }

  @Override
  public void dropFunction(String catName, String dbName, String funcName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
    delegate.dropFunction(catName, dbName, funcName);
  }

  @Override
  public Function getFunction(String dbName, String funcName) throws MetaException, TException {
    return delegate.getFunction(dbName, funcName);
  }

  @Override
  public Function getFunction(String catName, String dbName, String funcName)
      throws MetaException, TException {
    return delegate.getFunction(catName, dbName, funcName);
  }

  @Override
  public List<String> getFunctions(String dbName, String pattern) throws MetaException, TException {
    return delegate.getFunctions(dbName, pattern);
  }

  @Override
  public GetFunctionsResponse getFunctionsRequest(GetFunctionsRequest functionRequest) throws TException {
    return delegate.getFunctionsRequest(functionRequest);
  }

  @Override
  public List<String> getFunctions(String catName, String dbName, String pattern)
      throws MetaException, TException {
    return delegate.getFunctions(catName, dbName, pattern);
  }

  @Override
  public GetAllFunctionsResponse getAllFunctions() throws MetaException, TException {
    return delegate.getAllFunctions();
  }

  @Override
  public GetOpenTxnsResponse getOpenTxns() throws TException {
    return delegate.getOpenTxns();
  }

  @Override
  public ValidTxnList getValidTxns() throws TException {
    return delegate.getValidTxns();
  }

  @Override
  public ValidTxnList getValidTxns(long currentTxn) throws TException {
    return delegate.getValidTxns(currentTxn);
  }

  @Override
  public ValidTxnList getValidTxns(long currentTxn, List<TxnType> excludeTxnTypes) throws TException {
    return delegate.getValidTxns(currentTxn, excludeTxnTypes);
  }

  @Override
  public ValidWriteIdList getValidWriteIds(String fullTableName) throws TException {
    return delegate.getValidWriteIds(fullTableName);
  }

  @Override
  public ValidWriteIdList getValidWriteIds(String fullTableName, Long writeId) throws TException {
    return delegate.getValidWriteIds(fullTableName, writeId);
  }

  @Override
  public List<TableValidWriteIds> getValidWriteIds(List<String> tablesList, String validTxnList)
      throws TException {
    return delegate.getValidWriteIds(tablesList, validTxnList);
  }

  @Override
  public void addWriteIdsToMinHistory(long txnId, Map<String, Long> writeIds) throws TException {
    delegate.addWriteIdsToMinHistory(txnId, writeIds);
  }

  @Override
  public long openTxn(String user) throws TException {
    return delegate.openTxn(user);
  }

  @Override
  public long openTxn(String user, TxnType txnType) throws TException {
    return delegate.openTxn(user, txnType);
  }

  @Override
  public List<Long> replOpenTxn(String replPolicy, List<Long> srcTxnIds, String user, TxnType txnType)
      throws TException {
    return delegate.replOpenTxn(replPolicy, srcTxnIds, user, txnType);
  }

  @Override
  public OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
    return delegate.openTxns(user, numTxns);
  }

  @Override
  public void rollbackTxn(long txnid) throws NoSuchTxnException, TException {
    delegate.rollbackTxn(txnid);
  }

  @Override
  public void rollbackTxn(AbortTxnRequest abortTxnRequest) throws NoSuchTxnException, TException {
    delegate.rollbackTxn(abortTxnRequest);
  }

  @Override
  public void replRollbackTxn(long srcTxnid, String replPolicy, TxnType txnType)
      throws NoSuchTxnException, TException {
    delegate.replRollbackTxn(srcTxnid, replPolicy, txnType);
  }

  @Override
  public void commitTxn(long txnid) throws NoSuchTxnException, TxnAbortedException, TException {
    delegate.commitTxn(txnid);
  }

  @Override
  public void commitTxnWithKeyValue(long txnid, long tableId, String key, String value)
      throws NoSuchTxnException, TxnAbortedException, TException {
    delegate.commitTxnWithKeyValue(txnid, tableId, key, value);
  }

  @Override
  public void commitTxn(CommitTxnRequest rqst) throws NoSuchTxnException, TxnAbortedException, TException {
    delegate.commitTxn(rqst);
  }

  @Override
  public void abortTxns(List<Long> txnids) throws TException {
    delegate.abortTxns(txnids);
  }

  @Override
  public void abortTxns(AbortTxnsRequest abortTxnsRequest) throws TException {
    delegate.abortTxns(abortTxnsRequest);
  }

  @Override
  public long allocateTableWriteId(long txnId, String dbName, String tableName) throws TException {
    return delegate.allocateTableWriteId(txnId, dbName, tableName);
  }

  @Override
  public long allocateTableWriteId(long txnId, String dbName, String tableName, boolean reallocate)
      throws TException {
    return delegate.allocateTableWriteId(txnId, dbName, tableName, reallocate);
  }

  @Override
  public void replTableWriteIdState(String validWriteIdList, String dbName, String tableName,
      List<String> partNames) throws TException {
    delegate.replTableWriteIdState(validWriteIdList, dbName, tableName, partNames);
  }

  @Override
  public List<TxnToWriteId> allocateTableWriteIdsBatch(List<Long> txnIds, String dbName, String tableName)
      throws TException {
    return delegate.allocateTableWriteIdsBatch(txnIds, dbName, tableName);
  }

  @Override
  public List<TxnToWriteId> replAllocateTableWriteIdsBatch(String dbName, String tableName, String replPolicy,
      List<TxnToWriteId> srcTxnToWriteIdList) throws TException {
    return delegate.replAllocateTableWriteIdsBatch(dbName, tableName, replPolicy, srcTxnToWriteIdList);
  }

  @Override
  public long getMaxAllocatedWriteId(String dbName, String tableName) throws TException {
    return delegate.getMaxAllocatedWriteId(dbName, tableName);
  }

  @Override
  public void seedWriteId(String dbName, String tableName, long seedWriteId) throws TException {
    delegate.seedWriteId(dbName, tableName, seedWriteId);
  }

  @Override
  public void seedTxnId(long seedTxnId) throws TException {
    delegate.seedTxnId(seedTxnId);
  }

  @Override
  public GetOpenTxnsInfoResponse showTxns() throws TException {
    return delegate.showTxns();
  }

  @Override
  public LockResponse lock(LockRequest request) throws NoSuchTxnException, TxnAbortedException, TException {
    return delegate.lock(request);
  }

  @Override
  public LockResponse checkLock(long lockid)
      throws NoSuchTxnException, TxnAbortedException, NoSuchLockException, TException {
    return delegate.checkLock(lockid);
  }

  @Override
  public void unlock(long lockid) throws NoSuchLockException, TxnOpenException, TException {
    delegate.unlock(lockid);
  }

  @Override
  public ShowLocksResponse showLocks() throws TException {
    return delegate.showLocks();
  }

  @Override
  public ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
    return delegate.showLocks(showLocksRequest);
  }

  @Override
  public void heartbeat(long txnid, long lockid)
      throws NoSuchLockException, NoSuchTxnException, TxnAbortedException, TException {
    delegate.heartbeat(txnid, lockid);
  }

  @Override
  public HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws TException {
    return delegate.heartbeatTxnRange(min, max);
  }

  @Override
  public void compact(String dbname, String tableName, String partitionName, CompactionType type)
      throws TException {
    delegate.compact(dbname, tableName, partitionName, type);
  }

  @Override
  public void compact(String dbname, String tableName, String partitionName, CompactionType type,
      Map<String, String> tblproperties) throws TException {
    delegate.compact(dbname, tableName, partitionName, type, tblproperties);
  }

  @Override
  public CompactionResponse compact2(String dbname, String tableName, String partitionName,
      CompactionType type, Map<String, String> tblproperties) throws TException {
    return delegate.compact2(dbname, tableName, partitionName, type, tblproperties);
  }

  @Override
  public CompactionResponse compact2(CompactionRequest request) throws TException {
    return delegate.compact2(request);
  }

  @Override
  public ShowCompactResponse showCompactions() throws TException {
    return delegate.showCompactions();
  }

  @Override
  public ShowCompactResponse showCompactions(ShowCompactRequest request) throws TException {
    return delegate.showCompactions(request);
  }

  @Override
  public boolean submitForCleanup(CompactionRequest rqst, long highestWriteId, long txnId) throws TException {
    return delegate.submitForCleanup(rqst, highestWriteId, txnId);
  }

  @Override
  public GetLatestCommittedCompactionInfoResponse getLatestCommittedCompactionInfo(
      GetLatestCommittedCompactionInfoRequest request) throws TException {
    return delegate.getLatestCommittedCompactionInfo(request);
  }

  @Override
  public void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName,
      List<String> partNames) throws TException {
    delegate.addDynamicPartitions(txnId, writeId, dbName, tableName, partNames);
  }

  @Override
  public void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName,
      List<String> partNames, DataOperationType operationType) throws TException {
    delegate.addDynamicPartitions(txnId, writeId, dbName, tableName, partNames, operationType);
  }

  @Override
  public void insertTable(Table table, boolean overwrite) throws MetaException {
    delegate.insertTable(table, overwrite);
  }

  @Override
  public long getLatestTxnIdInConflict(long txnId) throws TException {
    return delegate.getLatestTxnIdInConflict(txnId);
  }

  @Override
  public NotificationEventResponse getNextNotification(long lastEventId, int maxEvents,
      NotificationFilter filter) throws TException {
    return delegate.getNextNotification(lastEventId, maxEvents, filter);
  }

  @Override
  public NotificationEventResponse getNextNotification(NotificationEventRequest request,
      boolean allowGapsInEventIds, NotificationFilter filter) throws TException {
    return delegate.getNextNotification(request, allowGapsInEventIds, filter);
  }

  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
    return delegate.getCurrentNotificationEventId();
  }

  @Override
  public NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst)
      throws TException {
    return delegate.getNotificationEventsCount(rqst);
  }

  @Override
  public FireEventResponse fireListenerEvent(FireEventRequest request) throws TException {
    return delegate.fireListenerEvent(request);
  }

  @Override
  public void addWriteNotificationLog(WriteNotificationLogRequest rqst) throws TException {
    delegate.addWriteNotificationLog(rqst);
  }

  @Override
  public void addWriteNotificationLogInBatch(WriteNotificationLogBatchRequest rqst) throws TException {
    delegate.addWriteNotificationLogInBatch(rqst);
  }

  @Override
  public GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest getPrincRoleReq)
      throws MetaException, TException {
    return delegate.get_principals_in_role(getPrincRoleReq);
  }

  @Override
  public GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
      GetRoleGrantsForPrincipalRequest getRolePrincReq) throws MetaException, TException {
    return delegate.get_role_grants_for_principal(getRolePrincReq);
  }

  @Override
  public AggrStats getAggrColStatsFor(String dbName, String tblName, List<String> colNames,
      List<String> partName, String engine) throws NoSuchObjectException, MetaException, TException {
    return delegate.getAggrColStatsFor(dbName, tblName, colNames, partName, engine);
  }

  @Override
  public AggrStats getAggrColStatsFor(String dbName, String tblName, List<String> colNames,
      List<String> partName, String engine, String writeIdList)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.getAggrColStatsFor(dbName, tblName, colNames, partName, engine, writeIdList);
  }

  @Override
  public AggrStats getAggrColStatsFor(String catName, String dbName, String tblName, List<String> colNames,
      List<String> partNames, String engine) throws NoSuchObjectException, MetaException, TException {
    return delegate.getAggrColStatsFor(catName, dbName, tblName, colNames, partNames, engine);
  }

  @Override
  public AggrStats getAggrColStatsFor(String catName, String dbName, String tblName, List<String> colNames,
      List<String> partNames, String engine, String writeIdList)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.getAggrColStatsFor(catName, dbName, tblName, colNames, partNames, engine, writeIdList);
  }

  @Override
  public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
    return delegate.setPartitionColumnStatistics(request);
  }

  @Override
  public void flushCache() {
    delegate.flushCache();
  }

  @Override
  public Iterable<Map.Entry<Long, ByteBuffer>> getFileMetadata(List<Long> fileIds) throws TException {
    return delegate.getFileMetadata(fileIds);
  }

  @Override
  public Iterable<Map.Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(List<Long> fileIds,
      ByteBuffer sarg, boolean doGetFooters) throws TException {
    return delegate.getFileMetadataBySarg(fileIds, sarg, doGetFooters);
  }

  @Override
  public void clearFileMetadata(List<Long> fileIds) throws TException {
    delegate.clearFileMetadata(fileIds);
  }

  @Override
  public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException {
    delegate.putFileMetadata(fileIds, metadata);
  }

  @Override
  public boolean isSameConfObj(Configuration c) {
    return delegate.isSameConfObj(c);
  }

  @Override
  public boolean cacheFileMetadata(String dbName, String tableName, String partName, boolean allParts)
      throws TException {
    return delegate.cacheFileMetadata(dbName, tableName, partName, allParts);
  }

  @Override
  public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest request)
      throws MetaException, NoSuchObjectException, TException {
    return delegate.getPrimaryKeys(request);
  }

  @Override
  public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest request)
      throws MetaException, NoSuchObjectException, TException {
    return delegate.getForeignKeys(request);
  }

  @Override
  public List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest request)
      throws MetaException, NoSuchObjectException, TException {
    return delegate.getUniqueConstraints(request);
  }

  @Override
  public List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest request)
      throws MetaException, NoSuchObjectException, TException {
    return delegate.getNotNullConstraints(request);
  }

  @Override
  public List<SQLDefaultConstraint> getDefaultConstraints(DefaultConstraintsRequest request)
      throws MetaException, NoSuchObjectException, TException {
    return delegate.getDefaultConstraints(request);
  }

  @Override
  public List<SQLCheckConstraint> getCheckConstraints(CheckConstraintsRequest request)
      throws MetaException, NoSuchObjectException, TException {
    return delegate.getCheckConstraints(request);
  }

  @Override
  public SQLAllTableConstraints getAllTableConstraints(AllTableConstraintsRequest request)
      throws MetaException, NoSuchObjectException, TException {
    return delegate.getAllTableConstraints(request);
  }

  @Override
  public void createTableWithConstraints(Table tTbl, List<SQLPrimaryKey> primaryKeys,
      List<SQLForeignKey> foreignKeys, List<SQLUniqueConstraint> uniqueConstraints,
      List<SQLNotNullConstraint> notNullConstraints, List<SQLDefaultConstraint> defaultConstraints,
      List<SQLCheckConstraint> checkConstraints) throws AlreadyExistsException, InvalidObjectException,
      MetaException, NoSuchObjectException, TException {
    delegate.createTableWithConstraints(tTbl, primaryKeys, foreignKeys, uniqueConstraints, notNullConstraints,
        defaultConstraints, checkConstraints);
  }

  @Override
  public void dropConstraint(String dbName, String tableName, String constraintName)
      throws MetaException, NoSuchObjectException, TException {
    delegate.dropConstraint(dbName, tableName, constraintName);
  }

  @Override
  public void dropConstraint(String catName, String dbName, String tableName, String constraintName)
      throws MetaException, NoSuchObjectException, TException {
    delegate.dropConstraint(catName, dbName, tableName, constraintName);
  }

  @Override
  public void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols)
      throws MetaException, NoSuchObjectException, TException {
    delegate.addPrimaryKey(primaryKeyCols);
  }

  @Override
  public void addForeignKey(List<SQLForeignKey> foreignKeyCols)
      throws MetaException, NoSuchObjectException, TException {
    delegate.addForeignKey(foreignKeyCols);
  }

  @Override
  public void addUniqueConstraint(List<SQLUniqueConstraint> uniqueConstraintCols)
      throws MetaException, NoSuchObjectException, TException {
    delegate.addUniqueConstraint(uniqueConstraintCols);
  }

  @Override
  public void addNotNullConstraint(List<SQLNotNullConstraint> notNullConstraintCols)
      throws MetaException, NoSuchObjectException, TException {
    delegate.addNotNullConstraint(notNullConstraintCols);
  }

  @Override
  public void addDefaultConstraint(List<SQLDefaultConstraint> defaultConstraints)
      throws MetaException, NoSuchObjectException, TException {
    delegate.addDefaultConstraint(defaultConstraints);
  }

  @Override
  public void addCheckConstraint(List<SQLCheckConstraint> checkConstraints)
      throws MetaException, NoSuchObjectException, TException {
    delegate.addCheckConstraint(checkConstraints);
  }

  @Override
  public String getMetastoreDbUuid() throws MetaException, TException {
    return delegate.getMetastoreDbUuid();
  }

  @Override
  public void createResourcePlan(WMResourcePlan resourcePlan, String copyFromName)
      throws InvalidObjectException, MetaException, TException {
    delegate.createResourcePlan(resourcePlan, copyFromName);
  }

  @Override
  public WMFullResourcePlan getResourcePlan(String resourcePlanName, String ns)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.getResourcePlan(resourcePlanName, ns);
  }

  @Override
  public List<WMResourcePlan> getAllResourcePlans(String ns)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.getAllResourcePlans(ns);
  }

  @Override
  public void dropResourcePlan(String resourcePlanName, String ns)
      throws NoSuchObjectException, MetaException, TException {
    delegate.dropResourcePlan(resourcePlanName, ns);
  }

  @Override
  public WMFullResourcePlan alterResourcePlan(String resourcePlanName, String ns,
      WMNullableResourcePlan resourcePlan, boolean canActivateDisabled, boolean isForceDeactivate,
      boolean isReplace) throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    return delegate.alterResourcePlan(resourcePlanName, ns, resourcePlan, canActivateDisabled,
        isForceDeactivate, isReplace);
  }

  @Override
  public WMFullResourcePlan getActiveResourcePlan(String ns) throws MetaException, TException {
    return delegate.getActiveResourcePlan(ns);
  }

  @Override
  public WMValidateResourcePlanResponse validateResourcePlan(String resourcePlanName, String ns)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    return delegate.validateResourcePlan(resourcePlanName, ns);
  }

  @Override
  public void createWMTrigger(WMTrigger trigger) throws InvalidObjectException, MetaException, TException {
    delegate.createWMTrigger(trigger);
  }

  @Override
  public void alterWMTrigger(WMTrigger trigger)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    delegate.alterWMTrigger(trigger);
  }

  @Override
  public void dropWMTrigger(String resourcePlanName, String triggerName, String ns)
      throws NoSuchObjectException, MetaException, TException {
    delegate.dropWMTrigger(resourcePlanName, triggerName, ns);
  }

  @Override
  public List<WMTrigger> getTriggersForResourcePlan(String resourcePlan, String ns)
      throws NoSuchObjectException, MetaException, TException {
    return delegate.getTriggersForResourcePlan(resourcePlan, ns);
  }

  @Override
  public void createWMPool(WMPool pool)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    delegate.createWMPool(pool);
  }

  @Override
  public void alterWMPool(WMNullablePool pool, String poolPath)
      throws NoSuchObjectException, InvalidObjectException, TException {
    delegate.alterWMPool(pool, poolPath);
  }

  @Override
  public void dropWMPool(String resourcePlanName, String poolPath, String ns) throws TException {
    delegate.dropWMPool(resourcePlanName, poolPath, ns);
  }

  @Override
  public void createOrUpdateWMMapping(WMMapping mapping, boolean isUpdate) throws TException {
    delegate.createOrUpdateWMMapping(mapping, isUpdate);
  }

  @Override
  public void dropWMMapping(WMMapping mapping) throws TException {
    delegate.dropWMMapping(mapping);
  }

  @Override
  public void createOrDropTriggerToPoolMapping(String resourcePlanName, String triggerName, String poolPath,
      boolean shouldDrop, String ns) throws AlreadyExistsException, NoSuchObjectException,
      InvalidObjectException, MetaException, TException {
    delegate.createOrDropTriggerToPoolMapping(resourcePlanName, triggerName, poolPath, shouldDrop, ns);
  }

  @Override
  public void createISchema(ISchema schema) throws TException {
    delegate.createISchema(schema);
  }

  @Override
  public void alterISchema(String catName, String dbName, String schemaName, ISchema newSchema)
      throws TException {
    delegate.alterISchema(catName, dbName, schemaName, newSchema);
  }

  @Override
  public ISchema getISchema(String catName, String dbName, String name) throws TException {
    return delegate.getISchema(catName, dbName, name);
  }

  @Override
  public void dropISchema(String catName, String dbName, String name) throws TException {
    delegate.dropISchema(catName, dbName, name);
  }

  @Override
  public void addSchemaVersion(SchemaVersion schemaVersion) throws TException {
    delegate.addSchemaVersion(schemaVersion);
  }

  @Override
  public SchemaVersion getSchemaVersion(String catName, String dbName, String schemaName, int version)
      throws TException {
    return delegate.getSchemaVersion(catName, dbName, schemaName, version);
  }

  @Override
  public SchemaVersion getSchemaLatestVersion(String catName, String dbName, String schemaName)
      throws TException {
    return delegate.getSchemaLatestVersion(catName, dbName, schemaName);
  }

  @Override
  public List<SchemaVersion> getSchemaAllVersions(String catName, String dbName, String schemaName)
      throws TException {
    return delegate.getSchemaAllVersions(catName, dbName, schemaName);
  }

  @Override
  public void dropSchemaVersion(String catName, String dbName, String schemaName, int version)
      throws TException {
    delegate.dropSchemaVersion(catName, dbName, schemaName, version);
  }

  @Override
  public FindSchemasByColsResp getSchemaByCols(FindSchemasByColsRqst rqst) throws TException {
    return delegate.getSchemaByCols(rqst);
  }

  @Override
  public void mapSchemaVersionToSerde(String catName, String dbName, String schemaName, int version,
      String serdeName) throws TException {
    delegate.mapSchemaVersionToSerde(catName, dbName, schemaName, version, serdeName);
  }

  @Override
  public void setSchemaVersionState(String catName, String dbName, String schemaName, int version,
      SchemaVersionState state) throws TException {
    delegate.setSchemaVersionState(catName, dbName, schemaName, version, state);
  }

  @Override
  public void addSerDe(SerDeInfo serDeInfo) throws TException {
    delegate.addSerDe(serDeInfo);
  }

  @Override
  public SerDeInfo getSerDe(String serDeName) throws TException {
    return delegate.getSerDe(serDeName);
  }

  @Override
  public LockResponse lockMaterializationRebuild(String dbName, String tableName, long txnId)
      throws TException {
    return delegate.lockMaterializationRebuild(dbName, tableName, txnId);
  }

  @Override
  public boolean heartbeatLockMaterializationRebuild(String dbName, String tableName, long txnId)
      throws TException {
    return delegate.heartbeatLockMaterializationRebuild(dbName, tableName, txnId);
  }

  @Override
  public void addRuntimeStat(RuntimeStat stat) throws TException {
    delegate.addRuntimeStat(stat);
  }

  @Override
  public List<RuntimeStat> getRuntimeStats(int maxWeight, int maxCreateTime) throws TException {
    return delegate.getRuntimeStats(maxWeight, maxCreateTime);
  }

  @Override
  public GetPartitionsResponse getPartitionsWithSpecs(GetPartitionsRequest request) throws TException {
    return delegate.getPartitionsWithSpecs(request);
  }

  @Override
  public OptionalCompactionInfoStruct findNextCompact(String workerId) throws MetaException, TException {
    return delegate.findNextCompact(workerId);
  }

  @Override
  public OptionalCompactionInfoStruct findNextCompact(FindNextCompactRequest rqst)
      throws MetaException, TException {
    return delegate.findNextCompact(rqst);
  }

  @Override
  public void updateCompactorState(CompactionInfoStruct cr, long txnId) throws TException {
    delegate.updateCompactorState(cr, txnId);
  }

  @Override
  public List<String> findColumnsWithStats(CompactionInfoStruct cr) throws TException {
    return delegate.findColumnsWithStats(cr);
  }

  @Override
  public void markCleaned(CompactionInfoStruct cr) throws MetaException, TException {
    delegate.markCleaned(cr);
  }

  @Override
  public void markCompacted(CompactionInfoStruct cr) throws MetaException, TException {
    delegate.markCompacted(cr);
  }

  @Override
  public void markFailed(CompactionInfoStruct cr) throws MetaException, TException {
    delegate.markFailed(cr);
  }

  @Override
  public void markRefused(CompactionInfoStruct cr) throws MetaException, TException {
    delegate.markRefused(cr);
  }

  @Override
  public boolean updateCompactionMetricsData(CompactionMetricsDataStruct struct)
      throws MetaException, TException {
    return delegate.updateCompactionMetricsData(struct);
  }

  @Override
  public void removeCompactionMetricsData(CompactionMetricsDataRequest request)
      throws MetaException, TException {
    delegate.removeCompactionMetricsData(request);
  }

  @Override
  public void setHadoopJobid(String jobId, long cqId) throws MetaException, TException {
    delegate.setHadoopJobid(jobId, cqId);
  }

  @Override
  public String getServerVersion() throws TException {
    return delegate.getServerVersion();
  }

  @Override
  public ScheduledQuery getScheduledQuery(ScheduledQueryKey scheduleKey) throws TException {
    return delegate.getScheduledQuery(scheduleKey);
  }

  @Override
  public void scheduledQueryMaintenance(ScheduledQueryMaintenanceRequest request)
      throws MetaException, TException {
    delegate.scheduledQueryMaintenance(request);
  }

  @Override
  public ScheduledQueryPollResponse scheduledQueryPoll(ScheduledQueryPollRequest request)
      throws MetaException, TException {
    return delegate.scheduledQueryPoll(request);
  }

  @Override
  public void scheduledQueryProgress(ScheduledQueryProgressInfo info) throws TException {
    delegate.scheduledQueryProgress(info);
  }

  @Override
  public void addReplicationMetrics(ReplicationMetricList replicationMetricList)
      throws MetaException, TException {
    delegate.addReplicationMetrics(replicationMetricList);
  }

  @Override
  public ReplicationMetricList getReplicationMetrics(GetReplicationMetricsRequest replicationMetricsRequest)
      throws MetaException, TException {
    return delegate.getReplicationMetrics(replicationMetricsRequest);
  }

  @Override
  public void createStoredProcedure(StoredProcedure proc)
      throws NoSuchObjectException, MetaException, TException {
    delegate.createStoredProcedure(proc);
  }

  @Override
  public StoredProcedure getStoredProcedure(StoredProcedureRequest request)
      throws MetaException, NoSuchObjectException, TException {
    return delegate.getStoredProcedure(request);
  }

  @Override
  public void dropStoredProcedure(StoredProcedureRequest request)
      throws MetaException, NoSuchObjectException, TException {
    delegate.dropStoredProcedure(request);
  }

  @Override
  public List<String> getAllStoredProcedures(ListStoredProcedureRequest request)
      throws MetaException, TException {
    return delegate.getAllStoredProcedures(request);
  }

  @Override
  public void addPackage(AddPackageRequest request) throws NoSuchObjectException, MetaException, TException {
    delegate.addPackage(request);
  }

  @Override
  public Package findPackage(GetPackageRequest request) throws TException {
    return delegate.findPackage(request);
  }

  @Override
  public List<String> listPackages(ListPackageRequest request) throws TException {
    return delegate.listPackages(request);
  }

  @Override
  public void dropPackage(DropPackageRequest request) throws TException {
    delegate.dropPackage(request);
  }

  @Override
  public List<WriteEventInfo> getAllWriteEventInfo(GetAllWriteEventInfoRequest request) throws TException {
    return delegate.getAllWriteEventInfo(request);
  }

  @Override
  public AbortCompactResponse abortCompactions(AbortCompactionRequest request) throws TException {
    return delegate.abortCompactions(request);
  }
}
