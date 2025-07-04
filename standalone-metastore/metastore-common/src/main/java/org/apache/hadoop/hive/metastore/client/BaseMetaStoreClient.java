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
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.convertToGetPartitionsByNamesRequest;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

public abstract class BaseMetaStoreClient implements IMetaStoreClient {

  // Keep a copy of HiveConf so if Session conf changes, we may need to get a new HMS client.
  protected final Configuration conf;

  public BaseMetaStoreClient(Configuration conf) {
    if (conf == null) {
      conf = MetastoreConf.newMetastoreConf();
      this.conf = conf;
    } else {
      this.conf = new Configuration(conf);
    }
  }

  @Override
  public final void dropCatalog(String catName)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    dropCatalog(catName, true);
  }

  @Override
  public final List<String> getDatabases(String databasePattern) throws MetaException, TException {
    return getDatabases(getDefaultCatalog(conf), databasePattern);
  }

  @Override
  public final List<String> getAllDatabases() throws MetaException, TException {
    return getAllDatabases(getDefaultCatalog(conf));
  }

  @Override
  public final List<String> getTables(String dbName, String tablePattern)
      throws MetaException, TException, UnknownDBException {
    return getTables(getDefaultCatalog(conf), dbName, tablePattern);
  }

  @Override
  public final List<String> getTables(String dbName, String tablePattern, TableType tableType)
      throws MetaException, TException, UnknownDBException {
    return getTables(getDefaultCatalog(conf), dbName, tablePattern, tableType);
  }

  @Override
  public final List<String> getMaterializedViewsForRewriting(String dbName)
      throws MetaException, TException, UnknownDBException {
    return getMaterializedViewsForRewriting(getDefaultCatalog(conf), dbName);
  }

  @Override
  public final List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes)
      throws MetaException, TException, UnknownDBException {
    return getTableMeta(getDefaultCatalog(conf), dbPatterns, tablePatterns, tableTypes);
  }

  @Override
  public final List<String> getAllTables(String dbName) throws MetaException, TException, UnknownDBException {
    return getAllTables(getDefaultCatalog(conf), dbName);
  }

  @Override
  public final List<String> listTableNamesByFilter(String dbName, String filter, short maxTables)
      throws TException, InvalidOperationException, UnknownDBException {
    return listTableNamesByFilter(getDefaultCatalog(conf), dbName, filter, maxTables);
  }

  @Override
  public final void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab)
      throws MetaException, TException, NoSuchObjectException {
    dropTable(getDefaultCatalog(conf), dbname, tableName, deleteData, ignoreUnknownTab, false);
  }

  @Override
  public final void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab,
      boolean ifPurge) throws MetaException, TException, NoSuchObjectException {
    dropTable(getDefaultCatalog(conf), dbname, tableName, deleteData, ignoreUnknownTab, ifPurge);
  }

  @Override
  public final void dropTable(String dbname, String tableName)
      throws MetaException, TException, NoSuchObjectException {
    dropTable(getDefaultCatalog(conf), dbname, tableName, true, true, false);
  }

  @Override
  public final void dropTable(String catName, String dbName, String tableName, boolean deleteData,
      boolean ignoreUnknownTable, boolean ifPurge) throws MetaException, NoSuchObjectException, TException {
    Table table;
    try {
      table = getTable(catName, dbName, tableName);
    } catch (NoSuchObjectException e) {
      if (!ignoreUnknownTable) {
        throw e;
      }
      return;
    }
    dropTable(table, deleteData, ignoreUnknownTable, ifPurge);
  }

  @Override
  public final void truncateTable(String dbName, String tableName, List<String> partNames)
      throws MetaException, TException {
    truncateTable(getDefaultCatalog(conf), dbName, tableName, null, partNames, null, -1, true, null);
  }

  @Override
  public final void truncateTable(TableName table, List<String> partNames) throws TException {
    truncateTable(table.getCat(), table.getDb(), table.getTable(), table.getTableMetaRef(), partNames, null,
        -1, true, null);
  }

  @Override
  public final void truncateTable(String dbName, String tableName, List<String> partNames,
      String validWriteIds, long writeId) throws TException {
    truncateTable(getDefaultCatalog(conf), dbName, tableName, null, partNames, validWriteIds, writeId, true,
        null);
  }

  @Override
  public final void truncateTable(String dbName, String tableName, List<String> partNames,
      String validWriteIds, long writeId, boolean deleteData) throws TException {
    truncateTable(getDefaultCatalog(conf), dbName, tableName, null, partNames, validWriteIds, writeId,
        deleteData, null);
  }

  @Override
  public final boolean tableExists(String databaseName, String tableName)
      throws MetaException, TException, UnknownDBException {
    return tableExists(getDefaultCatalog(conf), databaseName, tableName);
  }

  @Override
  public final Database getDatabase(String databaseName) throws NoSuchObjectException, MetaException, TException {
    return getDatabase(getDefaultCatalog(conf), databaseName);
  }

  @Override
  public final Table getTable(String dbName, String tableName)
      throws MetaException, TException, NoSuchObjectException {
    GetTableRequest req = new GetTableRequest(dbName, tableName);
    req.setCatName(getDefaultCatalog(conf));
    return getTable(req);
  }

  @Override
  public final Table getTable(String dbName, String tableName, boolean getColumnStats, String engine)
      throws MetaException, TException, NoSuchObjectException {
    GetTableRequest req = new GetTableRequest(dbName, tableName);
    req.setCatName(getDefaultCatalog(conf));
    req.setGetColumnStats(getColumnStats);
    if (getColumnStats) {
      req.setEngine(engine);
    }
    return getTable(req);
  }

  @Override
  public final Table getTable(String catName, String dbName, String tableName) throws MetaException, TException {
    GetTableRequest req = new GetTableRequest(dbName, tableName);
    req.setCatName(catName);
    return getTable(req);
  }

  @Override
  public final Table getTable(String catName, String dbName, String tableName, String validWriteIdList,
      boolean getColumnStats, String engine) throws TException {
    GetTableRequest req = new GetTableRequest(dbName, tableName);
    req.setCatName(catName);
    req.setValidWriteIdList(validWriteIdList);
    req.setGetColumnStats(getColumnStats);
    if (getColumnStats) {
      req.setEngine(engine);
    }
    return getTable(req);
  }

  @Override
  public final List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    return getTables(getDefaultCatalog(conf), dbName, tableNames, null);
  }

  @Override
  public final List<Table> getTableObjectsByName(String catName, String dbName, List<String> tableNames)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
    return getTables(catName, dbName, tableNames, null);
  }

  @Override
  public final void updateCreationMetadata(String dbName, String tableName, CreationMetadata cm)
      throws MetaException, TException {
    updateCreationMetadata(getDefaultCatalog(conf), dbName, tableName, cm);
  }

  @Override
  public final Partition appendPartition(String dbName, String tableName, List<String> partVals)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return appendPartition(getDefaultCatalog(conf), dbName, tableName, partVals);
  }

  @Override
  public final Partition appendPartition(String dbName, String tableName, String name)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return appendPartition(getDefaultCatalog(conf), dbName, tableName, name);
  }

  @Override
  public final int add_partitions(List<Partition> partitions)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    return add_partitions(partitions, false, true).size();
  }

  @Override
  public final Partition getPartition(String dbName, String tblName, List<String> partVals)
      throws NoSuchObjectException, MetaException, TException {
    return getPartition(getDefaultCatalog(conf), dbName, tblName, partVals);
  }

  @Override
  public final Partition exchange_partition(Map<String, String> partitionSpecs, String sourceDb,
      String sourceTable, String destdb, String destTableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    String catName = getDefaultCatalog(conf);
    return exchange_partition(partitionSpecs, catName, sourceDb, sourceTable, catName, destdb, destTableName);
  }

  @Override
  public final List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String sourceDb,
      String sourceTable, String destdb, String destTableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
    String catName = getDefaultCatalog(conf);
    return exchange_partitions(partitionSpecs, catName, sourceDb, sourceTable, catName, destdb, destTableName);
  }

  @Override
  public final Partition getPartition(String dbName, String tblName, String name)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    return getPartition(getDefaultCatalog(conf), dbName, tblName, name);
  }

  @Override
  public final Partition getPartitionWithAuthInfo(String dbName, String tableName, List<String> pvals,
      String userName, List<String> groupNames)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException {
    return getPartitionWithAuthInfo(getDefaultCatalog(conf), dbName, tableName, pvals, userName, groupNames);
  }

  @Override
  public final List<Partition> listPartitions(String db_name, String tbl_name, short max_parts)
      throws NoSuchObjectException, MetaException, TException {
    return listPartitions(getDefaultCatalog(conf), db_name, tbl_name, max_parts);
  }

  @Override
  public final PartitionSpecProxy listPartitionSpecs(String dbName, String tableName, int maxParts)
      throws TException {
    return listPartitionSpecs(getDefaultCatalog(conf), dbName, tableName, maxParts);
  }

  @Override
  public final List<Partition> listPartitions(String db_name, String tbl_name, List<String> part_vals,
      short max_parts) throws NoSuchObjectException, MetaException, TException {
    return listPartitions(getDefaultCatalog(conf), db_name, tbl_name, part_vals, max_parts);
  }

  @Override
  public final List<String> listPartitionNames(String db_name, String tbl_name, short max_parts)
      throws NoSuchObjectException, MetaException, TException {
    return listPartitionNames(getDefaultCatalog(conf), db_name, tbl_name, max_parts);
  }

  @Override
  public final List<String> listPartitionNames(String db_name, String tbl_name, List<String> part_vals,
      short max_parts) throws MetaException, TException, NoSuchObjectException {
    if (db_name == null || tbl_name == null) {
      throw new MetaException("DbName/TableName cannot be null");
    }
    GetPartitionNamesPsRequest getPartitionNamesPsRequest = new GetPartitionNamesPsRequest(db_name, tbl_name);
    getPartitionNamesPsRequest.setCatName(getDefaultCatalog(conf));
    getPartitionNamesPsRequest.setPartValues(part_vals);
    getPartitionNamesPsRequest.setMaxParts(HiveMetaStoreClientUtils.shrinkMaxtoShort(max_parts));

    return listPartitionNamesRequest(getPartitionNamesPsRequest).getNames();
  }

  @Override
  public final List<String> listPartitionNames(String catName, String db_name, String tbl_name,
      List<String> part_vals, int max_parts) throws MetaException, TException, NoSuchObjectException {
    if (db_name == null || tbl_name == null) {
      throw new MetaException("DbName/TableName cannot be null");
    }
    GetPartitionNamesPsRequest getPartitionNamesPsRequest = new GetPartitionNamesPsRequest(db_name, tbl_name);
    getPartitionNamesPsRequest.setCatName(catName);
    getPartitionNamesPsRequest.setPartValues(part_vals);
    getPartitionNamesPsRequest.setMaxParts(HiveMetaStoreClientUtils.shrinkMaxtoShort(max_parts));

    return listPartitionNamesRequest(getPartitionNamesPsRequest).getNames();
  }

  @Override
  public final int getNumPartitionsByFilter(String dbName, String tableName, String filter)
      throws MetaException, NoSuchObjectException, TException {
    return getNumPartitionsByFilter(getDefaultCatalog(conf), dbName, tableName, filter);
  }

  @Override
  public final List<Partition> listPartitionsByFilter(String db_name, String tbl_name, String filter,
      short max_parts) throws MetaException, NoSuchObjectException, TException {
    return listPartitionsByFilter(getDefaultCatalog(conf), db_name, tbl_name, filter, max_parts);
  }

  @Override
  public final PartitionSpecProxy listPartitionSpecsByFilter(String db_name, String tbl_name, String filter,
      int max_parts) throws MetaException, NoSuchObjectException, TException {
    return listPartitionSpecsByFilter(getDefaultCatalog(conf), db_name, tbl_name, filter, max_parts);
  }

  @Override
  public final boolean listPartitionsByExpr(String db_name, String tbl_name, byte[] expr,
      String default_partition_name, short max_parts, List<Partition> result) throws TException {
    PartitionsByExprRequest req = new PartitionsByExprRequest(db_name, tbl_name, ByteBuffer.wrap(expr));

    req.setCatName(getDefaultCatalog(conf));
    req.setDefaultPartitionName(default_partition_name);
    if (max_parts >= 0) {
      req.setMaxParts(HiveMetaStoreClientUtils.shrinkMaxtoShort(max_parts));
    }
    // validWriteIdList should be set by each proxy

    return listPartitionsByExpr(req, result);
  }

  @Override
  public final boolean listPartitionsByExpr(String catName, String db_name, String tbl_name, byte[] expr,
      String default_partition_name, int max_parts, List<Partition> result) throws TException {
    PartitionsByExprRequest req = new PartitionsByExprRequest(db_name, tbl_name, ByteBuffer.wrap(expr));

    req.setCatName(catName);
    req.setDefaultPartitionName(default_partition_name);
    if (max_parts >= 0) {
      req.setMaxParts(HiveMetaStoreClientUtils.shrinkMaxtoShort(max_parts));
    }
    // validWriteIdList should be set by each proxy

    return listPartitionsByExpr(req, result);
  }

  @Override
  public final List<Partition> listPartitionsWithAuthInfo(String dbName, String tableName, short maxParts,
      String userName, List<String> groupNames) throws MetaException, TException, NoSuchObjectException {
    return listPartitionsWithAuthInfo(getDefaultCatalog(conf), dbName, tableName, maxParts, userName,
        groupNames);
  }

  @Override
  public final List<Partition> getPartitionsByNames(String dbName, String tableName, List<String> partNames)
      throws NoSuchObjectException, MetaException, TException {
    GetPartitionsByNamesRequest req = convertToGetPartitionsByNamesRequest(
        MetaStoreUtils.prependCatalogToDbName(getDefaultCatalog(conf), dbName, conf), tableName, partNames);
    return getPartitionsByNames(req).getPartitions();
  }

  @Override
  public final List<Partition> listPartitionsWithAuthInfo(String dbName, String tableName,
      List<String> partialPvals, short maxParts, String userName, List<String> groupNames)
      throws MetaException, TException, NoSuchObjectException {
    return listPartitionsWithAuthInfo(getDefaultCatalog(conf), dbName, tableName, partialPvals, maxParts,
        userName, groupNames);
  }

  @Override
  public final void markPartitionForEvent(String db_name, String tbl_name, Map<String, String> partKVs,
      PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException,
      UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {
    markPartitionForEvent(getDefaultCatalog(conf), db_name, tbl_name, partKVs, eventType);
  }

  @Override
  public final boolean isPartitionMarkedForEvent(String db_name, String tbl_name, Map<String, String> partKVs,
      PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException,
      UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {
    return isPartitionMarkedForEvent(getDefaultCatalog(conf), db_name, tbl_name, partKVs, eventType);
  }

  @Override
  public final void createTable(Table tbl) throws AlreadyExistsException, InvalidObjectException,
      MetaException, NoSuchObjectException, TException {
    createTable(new CreateTableRequest(tbl));
  }

  @Override
  public final void alter_table(String databaseName, String tblName, Table table)
      throws InvalidOperationException, MetaException, TException {
    alter_table(getDefaultCatalog(conf), databaseName, tblName, table, null, null);
  }

  @Override
  public final void alter_table(String catName, String dbName, String tblName, Table newTable,
      EnvironmentContext envContext) throws InvalidOperationException, MetaException, TException {
    alter_table(catName, dbName, tblName, newTable, envContext, null);
  }

  @Override
  public final void alter_table(String databaseName, String tblName, Table table, boolean cascade)
      throws InvalidOperationException, MetaException, TException {
    EnvironmentContext environmentContext = new EnvironmentContext();
    if (cascade) {
      environmentContext.putToProperties(StatsSetupConst.CASCADE, StatsSetupConst.TRUE);
    }
    alter_table(getDefaultCatalog(conf), databaseName, tblName, table, environmentContext, null);
  }

  @Override
  public final void alter_table_with_environmentContext(String databaseName, String tblName, Table table,
      EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
    alter_table(getDefaultCatalog(conf), databaseName, tblName, table, environmentContext, null);
  }

  @Override
  public final void dropDatabase(String name)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    DropDatabaseRequest req = new DropDatabaseRequest();
    req.setName(name);
    req.setCatalogName(getDefaultCatalog(conf));
    req.setIgnoreUnknownDb(false);
    req.setDeleteData(true);
    req.setCascade(false);

    dropDatabase(req);
  }

  @Override
  public final void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    DropDatabaseRequest req = new DropDatabaseRequest();
    req.setName(name);
    req.setCatalogName(getDefaultCatalog(conf));
    req.setIgnoreUnknownDb(ignoreUnknownDb);
    req.setDeleteData(deleteData);
    req.setCascade(false);

    dropDatabase(req);
  }

  @Override
  public final void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    DropDatabaseRequest req = new DropDatabaseRequest();
    req.setName(name);
    req.setCatalogName(getDefaultCatalog(conf));
    req.setIgnoreUnknownDb(ignoreUnknownDb);
    req.setDeleteData(deleteData);
    req.setCascade(cascade);

    dropDatabase(req);
  }

  @Override
  public final void alterDatabase(String name, Database db)
      throws NoSuchObjectException, MetaException, TException {
    alterDatabase(getDefaultCatalog(conf), name, db);
  }

  @Override
  public final boolean dropPartition(String db_name, String tbl_name, List<String> part_vals,
      boolean deleteData) throws NoSuchObjectException, MetaException, TException {
    return dropPartition(getDefaultCatalog(conf), db_name, tbl_name, part_vals,
        PartitionDropOptions.instance().deleteData(deleteData));
  }

  @Override
  public final boolean dropPartition(String catName, String db_name, String tbl_name, List<String> part_vals,
      boolean deleteData) throws NoSuchObjectException, MetaException, TException {
    return dropPartition(catName, db_name, tbl_name, part_vals,
        PartitionDropOptions.instance().deleteData(deleteData));
  }

  @Override
  public final boolean dropPartition(String db_name, String tbl_name, List<String> part_vals,
      PartitionDropOptions options) throws NoSuchObjectException, MetaException, TException {
    return dropPartition(getDefaultCatalog(conf), db_name, tbl_name, part_vals, options);
  }

  @Override
  public final List<Partition> dropPartitions(String dbName, String tblName,
      List<Pair<Integer, byte[]>> partExprs, boolean deleteData, boolean ifExists)
      throws NoSuchObjectException, MetaException, TException {
    PartitionDropOptions options = PartitionDropOptions.instance()
        .deleteData(deleteData)
        .ifExists(ifExists);
    return dropPartitions(getDefaultCatalog(conf), dbName, tblName, partExprs, options, null);
  }

  @Override
  public final List<Partition> dropPartitions(String dbName, String tblName,
      List<Pair<Integer, byte[]>> partExprs, PartitionDropOptions options)
      throws NoSuchObjectException, MetaException, TException {
    return dropPartitions(getDefaultCatalog(conf), dbName, tblName, partExprs, options, null);
  }

  @Override
  public final List<Partition> dropPartitions(String catName, String dbName, String tblName,
      List<Pair<Integer, byte[]>> partExprs, PartitionDropOptions options)
      throws NoSuchObjectException, MetaException, TException {
    return dropPartitions(catName, dbName, tblName, partExprs, options, null);
  }

  @Override
  public final boolean dropPartition(String db_name, String tbl_name, String name, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
    return dropPartition(getDefaultCatalog(conf), db_name, tbl_name, name, deleteData);
  }

  @Override
  public final void alter_partition(String dbName, String tblName, Partition newPart)
      throws InvalidOperationException, MetaException, TException {
    alter_partition(getDefaultCatalog(conf), dbName, tblName, newPart, null, null);
  }

  @Override
  public final void alter_partition(String dbName, String tblName, Partition newPart,
      EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
    alter_partition(getDefaultCatalog(conf), dbName, tblName, newPart, environmentContext, null);
  }

  @Override
  public final void alter_partition(String catName, String dbName, String tblName, Partition newPart,
      EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
    alter_partition(catName, dbName, tblName, newPart, environmentContext, null);
  }

  @Override
  public final void alter_partitions(String dbName, String tblName, List<Partition> newParts,
      EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
    alter_partitions(getDefaultCatalog(conf), dbName, tblName, newParts, environmentContext, null, -1);
  }

  @Override
  public final void alter_partitions(String dbName, String tblName, List<Partition> newParts,
      EnvironmentContext environmentContext, String writeIdList, long writeId)
      throws InvalidOperationException, MetaException, TException {
    alter_partitions(getDefaultCatalog(conf), dbName, tblName, newParts, environmentContext, writeIdList,
        writeId);
  }

  @Override
  public final void renamePartition(String dbname, String tableName, List<String> part_vals, Partition newPart)
      throws InvalidOperationException, MetaException, TException {
    renamePartition(getDefaultCatalog(conf), dbname, tableName, part_vals, newPart, null);
  }

  @Override
  public final List<FieldSchema> getFields(String db, String tableName) throws MetaException, TException,
      UnknownTableException, UnknownDBException {
    return getFields(getDefaultCatalog(conf), db, tableName);
  }

  @Override
  public final List<FieldSchema> getSchema(String db, String tableName) throws MetaException, TException,
      UnknownTableException, UnknownDBException {
    return getSchema(getDefaultCatalog(conf), db, tableName);
  }

  @Override
  public final List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames, String engine) throws NoSuchObjectException, MetaException, TException {
    return getTableColumnStatistics(getDefaultCatalog(conf), dbName, tableName, colNames, engine, null);
  }

  @Override
  public final List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames, String engine, String validWriteIdList)
      throws NoSuchObjectException, MetaException, TException {
    return getTableColumnStatistics(getDefaultCatalog(conf), dbName, tableName, colNames, engine,
        validWriteIdList);
  }

  @Override
  public final List<ColumnStatisticsObj> getTableColumnStatistics(String catName, String dbName,
      String tableName, List<String> colNames, String engine)
      throws NoSuchObjectException, MetaException, TException {
    return getTableColumnStatistics(catName, dbName, tableName, colNames, engine, null);
  }

  @Override
  public final Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName, String tableName,
      List<String> partNames, List<String> colNames, String engine)
      throws NoSuchObjectException, MetaException, TException {
    return getPartitionColumnStatistics(getDefaultCatalog(conf), dbName, tableName, partNames,
        colNames, engine, null);
  }

  @Override
  public final Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName,
      String tableName, List<String> partNames, List<String> colNames, String engine, String validWriteIdList)
      throws NoSuchObjectException, MetaException, TException {
    return getPartitionColumnStatistics(getDefaultCatalog(conf), dbName, tableName, partNames, colNames,
        engine, validWriteIdList);
  }

  @Override
  public final Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String catName,
      String dbName, String tableName, List<String> partNames, List<String> colNames, String engine)
      throws NoSuchObjectException, MetaException, TException {
    return getPartitionColumnStatistics(catName, dbName, tableName, partNames, colNames, engine, null);
  }

  @Override
  public final void alterFunction(String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException, TException {
    alterFunction(getDefaultCatalog(conf), dbName, funcName, newFunction);
  }

  @Override
  public final void dropFunction(String dbName, String funcName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
    dropFunction(getDefaultCatalog(conf), dbName, funcName);
  }

  @Override
  public final Function getFunction(String dbName, String funcName) throws MetaException, TException {
    return getFunction(getDefaultCatalog(conf), dbName, funcName);
  }

  @Override
  public final List<String> getFunctions(String dbName, String pattern) throws MetaException, TException {
    return getFunctions(getDefaultCatalog(conf), dbName, pattern);
  }

  @Override
  public final ValidTxnList getValidTxns() throws TException {
    return getValidTxns(0, Arrays.asList(TxnType.READ_ONLY));
  }

  @Override
  public final ValidTxnList getValidTxns(long currentTxn) throws TException {
    return getValidTxns(currentTxn, Arrays.asList(TxnType.READ_ONLY));
  }

  @Override
  public final long openTxn(String user) throws TException {
    return openTxn(user, null);
  }

  @Override
  public final void rollbackTxn(long txnid) throws NoSuchTxnException, TException {
    rollbackTxn(new AbortTxnRequest(txnid));
  }

  @Override
  public final void commitTxn(long txnid) throws NoSuchTxnException, TxnAbortedException, TException {
    commitTxn(new CommitTxnRequest(txnid));
  }

  @Override
  public final void abortTxns(List<Long> txnids) throws TException {
    AbortTxnsRequest abortTxnsRequest = new AbortTxnsRequest(txnids);
    abortTxns(abortTxnsRequest);
  }

  @Override
  public final long allocateTableWriteId(long txnId, String dbName, String tableName) throws TException {
    return allocateTableWriteId(txnId, dbName, tableName, false);
  }

  @Override
  public final ShowCompactResponse showCompactions() throws TException {
    return showCompactions(new ShowCompactRequest());
  }

  @Override
  public final NotificationEventResponse getNextNotification(long lastEventId, int maxEvents,
      NotificationFilter filter) throws TException {
    NotificationEventRequest req = new NotificationEventRequest(lastEventId);
    req.setMaxEvents(maxEvents);
    return getNextNotification(req, false, filter);
  }

  @Override
  public final AggrStats getAggrColStatsFor(String dbName, String tblName, List<String> colNames,
      List<String> partName, String engine) throws NoSuchObjectException, MetaException, TException {
    // writeIdList should be set by each proxy.
    return getAggrColStatsFor(getDefaultCatalog(conf), dbName, tblName, colNames, partName, engine);
  }

  @Override
  public final AggrStats getAggrColStatsFor(String dbName, String tblName, List<String> colNames,
      List<String> partName, String engine, String writeIdList)
      throws NoSuchObjectException, MetaException, TException {
    // We keep two copies of getAggrColStatsFor to adhere Hive.java's writeIdList, which can be null.
    return getAggrColStatsFor(getDefaultCatalog(conf), dbName, tblName, colNames, partName, engine,
        writeIdList);
  }

  @Override
  public final void dropConstraint(String dbName, String tableName, String constraintName)
      throws MetaException, NoSuchObjectException, TException {
    dropConstraint(getDefaultCatalog(conf), dbName, tableName, constraintName);
  }
}
