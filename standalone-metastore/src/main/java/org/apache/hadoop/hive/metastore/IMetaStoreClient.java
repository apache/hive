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


import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.classification.RetrySemantics;
import org.apache.hadoop.hive.metastore.annotation.NoReconnect;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.BasicTxnInfo;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleResponse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.ConfigValSecurityException;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsResp;
import org.apache.hadoop.hive.metastore.api.FindSchemasByColsRqst;
import org.apache.hadoop.hive.metastore.api.FireEventRequest;
import org.apache.hadoop.hive.metastore.api.FireEventResponse;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetAllFunctionsResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.MetadataPpdResult;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionState;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMNullablePool;
import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.ObjectPair;
import org.apache.thrift.TException;

/**
 * Wrapper around hive metastore thrift api
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface IMetaStoreClient {

  /**
   * Returns whether current client is compatible with conf argument or not
   * @return
   */
  boolean isCompatibleWith(Configuration conf);

  /**
   * Set added jars path info to MetaStoreClient.
   * @param addedJars the hive.added.jars.path. It is qualified paths separated by commas.
   */
  void setHiveAddedJars(String addedJars);

  /**
   * Returns true if the current client is using an in process metastore (local metastore).
   *
   * @return
   */
  boolean isLocalMetaStore();

  /**
   *  Tries to reconnect this MetaStoreClient to the MetaStore.
   */
  void reconnect() throws MetaException;

  /**
   * close connection to meta store
   */
  @NoReconnect
  void close();

  /**
   * set meta variable which is open to end users
   */
  void setMetaConf(String key, String value) throws MetaException, TException;

  /**
   * get current meta variable
   */
  String getMetaConf(String key) throws MetaException, TException;

  /**
   * Get the names of all databases in the MetaStore that match the given pattern.
   * @param databasePattern
   * @return List of database names.
   * @throws MetaException
   * @throws TException
   */
  List<String> getDatabases(String databasePattern) throws MetaException, TException;

  /**
   * Get the names of all databases in the MetaStore.
   * @return List of database names.
   * @throws MetaException
   * @throws TException
   */
  List<String> getAllDatabases() throws MetaException, TException;

  /**
   * Get the names of all tables in the specified database that satisfy the supplied
   * table name pattern.
   * @param dbName
   * @param tablePattern
   * @return List of table names.
   * @throws MetaException
   * @throws TException
   * @throws UnknownDBException
   */
  List<String> getTables(String dbName, String tablePattern)
      throws MetaException, TException, UnknownDBException;

  /**
   * Get the names of all tables in the specified database that satisfy the supplied
   * table name pattern and table type (MANAGED_TABLE || EXTERNAL_TABLE || VIRTUAL_VIEW)
   * @param dbName Name of the database to fetch tables in.
   * @param tablePattern pattern to match for table names.
   * @param tableType Type of the table in the HMS store. VIRTUAL_VIEW is for views.
   * @return List of table names.
   * @throws MetaException
   * @throws TException
   * @throws UnknownDBException
   */
  List<String> getTables(String dbName, String tablePattern, TableType tableType)
      throws MetaException, TException, UnknownDBException;

  /**
   * Get materialized views that have rewriting enabled.
   * @param dbName Name of the database to fetch materialized views from.
   * @return List of materialized view names.
   * @throws MetaException
   * @throws TException
   * @throws UnknownDBException
   */
  List<String> getMaterializedViewsForRewriting(String dbName)
      throws MetaException, TException, UnknownDBException;

  /**
   * For quick GetTablesOperation
   */
  List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes)
      throws MetaException, TException, UnknownDBException;

  /**
   * Get the names of all tables in the specified database.
   * @param dbName
   * @return List of table names.
   * @throws MetaException
   * @throws TException
   * @throws UnknownDBException
   */
  List<String> getAllTables(String dbName) throws MetaException, TException, UnknownDBException;

  /**
   * Get a list of table names that match a filter.
   * The filter operators are LIKE, &lt;, &lt;=, &gt;, &gt;=, =, &lt;&gt;
   *
   * In the filter statement, values interpreted as strings must be enclosed in quotes,
   * while values interpreted as integers should not be.  Strings and integers are the only
   * supported value types.
   *
   * The currently supported key names in the filter are:
   * Constants.HIVE_FILTER_FIELD_OWNER, which filters on the tables' owner's name
   *   and supports all filter operators
   * Constants.HIVE_FILTER_FIELD_LAST_ACCESS, which filters on the last access times
   *   and supports all filter operators except LIKE
   * Constants.HIVE_FILTER_FIELD_PARAMS, which filters on the tables' parameter keys and values
   *   and only supports the filter operators = and &lt;&gt;.
   *   Append the parameter key name to HIVE_FILTER_FIELD_PARAMS in the filter statement.
   *   For example, to filter on parameter keys called "retention", the key name in the filter
   *   statement should be Constants.HIVE_FILTER_FIELD_PARAMS + "retention"
   *   Also, = and &lt;&gt; only work for keys that exist in the tables.
   *   E.g., filtering on tables where key1 &lt;&gt; value will only
   *   return tables that have a value for the parameter key1.
   * Some example filter statements include:
   * filter = Constants.HIVE_FILTER_FIELD_OWNER + " like \".*test.*\" and " +
   *   Constants.HIVE_FILTER_FIELD_LAST_ACCESS + " = 0";
   * filter = Constants.HIVE_FILTER_FIELD_OWNER + " = \"test_user\" and (" +
   *   Constants.HIVE_FILTER_FIELD_PARAMS + "retention = \"30\" or " +
   *   Constants.HIVE_FILTER_FIELD_PARAMS + "retention = \"90\")"
   *
   * @param dbName
   *          The name of the database from which you will retrieve the table names
   * @param filter
   *          The filter string
   * @param maxTables
   *          The maximum number of tables returned
   * @return  A list of table names that match the desired filter
   */
  List<String> listTableNamesByFilter(String dbName, String filter, short maxTables)
      throws MetaException, TException, InvalidOperationException, UnknownDBException;


  /**
   * Drop the table.
   *
   * @param dbname
   *          The database for this table
   * @param tableName
   *          The table to drop
   * @param deleteData
   *          Should we delete the underlying data
   * @param ignoreUnknownTab
   *          don't throw if the requested table doesn't exist
   * @throws MetaException
   *           Could not drop table properly.
   * @throws NoSuchObjectException
   *           The table wasn't found.
   * @throws TException
   *           A thrift communication error occurred
   */
  void dropTable(String dbname, String tableName, boolean deleteData,
      boolean ignoreUnknownTab) throws MetaException, TException,
      NoSuchObjectException;

  /**
   * @param ifPurge
   *          completely purge the table (skipping trash) while removing data from warehouse
   * @see #dropTable(String, String, boolean, boolean)
   */
  public void dropTable(String dbname, String tableName, boolean deleteData,
      boolean ignoreUnknownTab, boolean ifPurge) throws MetaException, TException,
      NoSuchObjectException;

  /**
   * Drop the table in the DEFAULT database.
   *
   * @param tableName
   *          The table to drop
   * @param deleteData
   *          Should we delete the underlying data
   * @throws MetaException
   *           Could not drop table properly.
   * @throws UnknownTableException
   *           The table wasn't found.
   * @throws TException
   *           A thrift communication error occurred
   * @throws NoSuchObjectException
   *           The table wasn't found.
   *
   * @deprecated As of release 0.6.0 replaced by {@link #dropTable(String, String, boolean, boolean)}.
   *             This method will be removed in release 0.7.0.
   */
  @Deprecated
  void dropTable(String tableName, boolean deleteData)
      throws MetaException, UnknownTableException, TException, NoSuchObjectException;

  /**
   * @see #dropTable(String, String, boolean, boolean)
   */
  void dropTable(String dbname, String tableName)
      throws MetaException, TException, NoSuchObjectException;

  /**
   * Truncate the table/partitions in the DEFAULT database.
   * @param dbName
   *          The db to which the table to be truncate belongs to
   * @param tableName
   *          The table to truncate
   * @param partNames
   *          List of partitions to truncate. NULL will truncate the whole table/all partitions
   * @throws MetaException
   * @throws TException
   *           Could not truncate table properly.
   */
  void truncateTable(String dbName, String tableName, List<String> partNames) throws MetaException, TException;

  /**
   * Recycles the files recursively from the input path to the cmroot directory either by copying or moving it.
   *
   * @param request Inputs for path of the data files to be recycled to cmroot and
   *                isPurge flag when set to true files which needs to be recycled are not moved to Trash
   * @return Response which is currently void
   */
  CmRecycleResponse recycleDirToCmPath(CmRecycleRequest request) throws MetaException, TException;

  boolean tableExists(String databaseName, String tableName) throws MetaException,
      TException, UnknownDBException;

  /**
   * Check to see if the specified table exists in the DEFAULT database.
   * @param tableName
   * @return TRUE if DEFAULT.tableName exists, FALSE otherwise.
   * @throws MetaException
   * @throws TException
   * @throws UnknownDBException
   * @deprecated As of release 0.6.0 replaced by {@link #tableExists(String, String)}.
   *             This method will be removed in release 0.7.0.
   */
  @Deprecated
  boolean tableExists(String tableName) throws MetaException,
      TException, UnknownDBException;

  /**
   * Get a table object from the DEFAULT database.
   *
   * @param tableName
   *          Name of the table to fetch.
   * @return An object representing the table.
   * @throws MetaException
   *           Could not fetch the table
   * @throws TException
   *           A thrift communication error occurred
   * @throws NoSuchObjectException
   *           In case the table wasn't found.
   * @deprecated As of release 0.6.0 replaced by {@link #getTable(String, String)}.
   *             This method will be removed in release 0.7.0.
   */
  @Deprecated
  Table getTable(String tableName) throws MetaException, TException, NoSuchObjectException;

  /**
   * Get a Database Object
   * @param databaseName  name of the database to fetch
   * @return the database
   * @throws NoSuchObjectException The database does not exist
   * @throws MetaException Could not fetch the database
   * @throws TException A thrift communication error occurred
   */
  Database getDatabase(String databaseName)
      throws NoSuchObjectException, MetaException, TException;


  /**
   * Get a table object.
   *
   * @param dbName
   *          The database the table is located in.
   * @param tableName
   *          Name of the table to fetch.
   * @return An object representing the table.
   * @throws MetaException
   *           Could not fetch the table
   * @throws TException
   *           A thrift communication error occurred
   * @throws NoSuchObjectException
   *           In case the table wasn't found.
   */
  Table getTable(String dbName, String tableName) throws MetaException,
      TException, NoSuchObjectException;

  /**
   *
   * @param dbName
   *          The database the tables are located in.
   * @param tableNames
   *          The names of the tables to fetch
   * @return A list of objects representing the tables.
   *          Only the tables that can be retrieved from the database are returned.  For example,
   *          if none of the requested tables could be retrieved, an empty list is returned.
   *          There is no guarantee of ordering of the returned tables.
   * @throws InvalidOperationException
   *          The input to this operation is invalid (e.g., the list of tables names is null)
   * @throws UnknownDBException
   *          The requested database could not be fetched.
   * @throws TException
   *          A thrift communication error occurred
   * @throws MetaException
   *          Any other errors
   */
  List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
      throws MetaException, InvalidOperationException, UnknownDBException, TException;

  /**
   * Returns the invalidation information for the materialized views given as input.
   */
  Map<String, Materialization> getMaterializationsInvalidationInfo(String dbName, List<String> viewNames)
      throws MetaException, InvalidOperationException, UnknownDBException, TException;

  /**
   * Updates the creation metadata for the materialized view.
   */
  void updateCreationMetadata(String dbName, String tableName, CreationMetadata cm)
      throws MetaException, TException;

  /**
   * @param tableName
   * @param dbName
   * @param partVals
   * @return the partition object
   * @throws InvalidObjectException
   * @throws AlreadyExistsException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#append_partition(java.lang.String,
   *      java.lang.String, java.util.List)
   */
  Partition appendPartition(String tableName, String dbName,
      List<String> partVals) throws InvalidObjectException,
      AlreadyExistsException, MetaException, TException;

  Partition appendPartition(String tableName, String dbName, String name)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException;

  /**
   * Add a partition to the table.
   *
   * @param partition
   *          The partition to add
   * @return The partition added
   * @throws InvalidObjectException
   *           Could not find table to add to
   * @throws AlreadyExistsException
   *           Partition already exists
   * @throws MetaException
   *           Could not add partition
   * @throws TException
   *           Thrift exception
   */
  Partition add_partition(Partition partition)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException;

  /**
   * Add partitions to the table.
   *
   * @param partitions
   *          The partitions to add
   * @throws InvalidObjectException
   *           Could not find table to add to
   * @throws AlreadyExistsException
   *           Partition already exists
   * @throws MetaException
   *           Could not add partition
   * @throws TException
   *           Thrift exception
   */
  int add_partitions(List<Partition> partitions)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException;

  int add_partitions_pspec(PartitionSpecProxy partitionSpec)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException;

  /**
   * Add partitions to the table.
   *
   * @param partitions The partitions to add
   * @param ifNotExists only add partitions if they don't exist
   * @param needResults Whether the results are needed
   * @return the partitions that were added, or null if !needResults
   */
  List<Partition> add_partitions(
      List<Partition> partitions, boolean ifNotExists, boolean needResults)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException;

  /**
   * @param dbName
   * @param tblName
   * @param partVals
   * @return the partition object
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_partition(java.lang.String,
   *      java.lang.String, java.util.List)
   */
  Partition getPartition(String dbName, String tblName,
      List<String> partVals) throws NoSuchObjectException, MetaException, TException;

  /**
   * @param partitionSpecs
   * @param sourceDb
   * @param sourceTable
   * @param destdb
   * @param destTableName
   * @return partition object
   */
  Partition exchange_partition(Map<String, String> partitionSpecs,
      String sourceDb, String sourceTable, String destdb,
      String destTableName) throws MetaException, NoSuchObjectException,
      InvalidObjectException, TException;

  /**
   * With the one partitionSpecs to exchange, multiple partitions could be exchanged.
   * e.g., year=2015/month/day, exchanging partition year=2015 results to all the partitions
   * belonging to it exchanged. This function returns the list of affected partitions.
   * @param partitionSpecs
   * @param sourceDb
   * @param sourceTable
   * @param destdb
   * @param destTableName
   * @return the list of the new partitions
   */
  List<Partition> exchange_partitions(Map<String, String> partitionSpecs,
      String sourceDb, String sourceTable, String destdb,
      String destTableName) throws MetaException, NoSuchObjectException,
      InvalidObjectException, TException;

  /**
   * @param dbName
   * @param tblName
   * @param name - partition name i.e. 'ds=2010-02-03/ts=2010-02-03 18%3A16%3A01'
   * @return the partition object
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_partition(java.lang.String,
   *      java.lang.String, java.util.List)
   */
  Partition getPartition(String dbName, String tblName,
      String name) throws MetaException, UnknownTableException, NoSuchObjectException, TException;


  /**
   * @param dbName
   * @param tableName
   * @param pvals
   * @param userName
   * @param groupNames
   * @return the partition
   * @throws MetaException
   * @throws UnknownTableException
   * @throws NoSuchObjectException
   * @throws TException
   */
  Partition getPartitionWithAuthInfo(String dbName, String tableName,
      List<String> pvals, String userName, List<String> groupNames)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException;

  /**
   * @param tbl_name
   * @param db_name
   * @param max_parts
   * @return the list of partitions
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   */
  List<Partition> listPartitions(String db_name, String tbl_name,
      short max_parts) throws NoSuchObjectException, MetaException, TException;

  public PartitionSpecProxy listPartitionSpecs(String dbName, String tableName, int maxParts)
    throws TException;
  List<Partition> listPartitions(String db_name, String tbl_name,
      List<String> part_vals, short max_parts) throws NoSuchObjectException, MetaException, TException;

  List<String> listPartitionNames(String db_name, String tbl_name,
      short max_parts) throws NoSuchObjectException, MetaException, TException;

  List<String> listPartitionNames(String db_name, String tbl_name,
      List<String> part_vals, short max_parts)
      throws MetaException, TException, NoSuchObjectException;

  public PartitionValuesResponse listPartitionValues(PartitionValuesRequest request)
      throws MetaException, TException, NoSuchObjectException;

  /**
   * Get number of partitions matching specified filter
   * @param dbName the database name
   * @param tableName the table name
   * @param filter the filter string,
   *    for example "part1 = \"p1_abc\" and part2 &lt;= "\p2_test\"". Filtering can
   *    be done only on string partition keys.
   * @return number of partitions
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   */
  public int getNumPartitionsByFilter(String dbName, String tableName,
                                      String filter) throws MetaException, NoSuchObjectException, TException;


    /**
   * Get list of partitions matching specified filter
   * @param db_name the database name
   * @param tbl_name the table name
   * @param filter the filter string,
   *    for example "part1 = \"p1_abc\" and part2 &lt;= "\p2_test\"". Filtering can
   *    be done only on string partition keys.
   * @param max_parts the maximum number of partitions to return,
   *    all partitions are returned if -1 is passed
   * @return list of partitions
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   */
  List<Partition> listPartitionsByFilter(String db_name, String tbl_name,
      String filter, short max_parts) throws MetaException,
         NoSuchObjectException, TException;

  PartitionSpecProxy listPartitionSpecsByFilter(String db_name, String tbl_name,
                                                       String filter, int max_parts) throws MetaException,
         NoSuchObjectException, TException;

  /**
   * Get list of partitions matching specified serialized expression
   * @param db_name the database name
   * @param tbl_name the table name
   * @param expr expression, serialized from ExprNodeDesc
   * @param max_parts the maximum number of partitions to return,
   *    all partitions are returned if -1 is passed
   * @param default_partition_name Default partition name from configuration. If blank, the
   *    metastore server-side configuration is used.
   * @param result the resulting list of partitions
   * @return whether the resulting list contains partitions which may or may not match the expr
   */
  boolean listPartitionsByExpr(String db_name, String tbl_name,
      byte[] expr, String default_partition_name, short max_parts, List<Partition> result)
          throws TException;

  /**
   * @param dbName
   * @param tableName
   * @param s
   * @param userName
   * @param groupNames
   * @return the list of partitions
   * @throws NoSuchObjectException
   */
  List<Partition> listPartitionsWithAuthInfo(String dbName,
      String tableName, short s, String userName, List<String> groupNames)
      throws MetaException, TException, NoSuchObjectException;

  /**
   * Get partitions by a list of partition names.
   * @param db_name database name
   * @param tbl_name table name
   * @param part_names list of partition names
   * @return list of Partition objects
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   */
  List<Partition> getPartitionsByNames(String db_name, String tbl_name,
      List<String> part_names) throws NoSuchObjectException, MetaException, TException;

  /**
   * @param dbName
   * @param tableName
   * @param partialPvals
   * @param s
   * @param userName
   * @param groupNames
   * @return the list of paritions
   * @throws NoSuchObjectException
   */
  List<Partition> listPartitionsWithAuthInfo(String dbName,
      String tableName, List<String> partialPvals, short s, String userName,
      List<String> groupNames) throws MetaException, TException, NoSuchObjectException;

  /**
   * @param db_name
   * @param tbl_name
   * @param partKVs
   * @param eventType
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   * @throws UnknownTableException
   * @throws UnknownDBException
   * @throws UnknownPartitionException
   * @throws InvalidPartitionException
   */
  void markPartitionForEvent(String db_name, String tbl_name, Map<String,String> partKVs,
      PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException,
      UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException;

  /**
   * @param db_name
   * @param tbl_name
   * @param partKVs
   * @param eventType
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   * @throws UnknownTableException
   * @throws UnknownDBException
   * @throws UnknownPartitionException
   * @throws InvalidPartitionException
   */
  boolean isPartitionMarkedForEvent(String db_name, String tbl_name, Map<String,String> partKVs,
      PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException,
      UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException;

  /**
   * @param partVals
   * @throws TException
   * @throws MetaException
   */
  void validatePartitionNameCharacters(List<String> partVals) throws TException, MetaException;

  /**
   * @param tbl
   * @throws AlreadyExistsException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#create_table(org.apache.hadoop.hive.metastore.api.Table)
   */

  void createTable(Table tbl) throws AlreadyExistsException,
      InvalidObjectException, MetaException, NoSuchObjectException, TException;

  void alter_table(String defaultDatabaseName, String tblName,
      Table table) throws InvalidOperationException, MetaException, TException;

  /**
   * Use alter_table_with_environmentContext instead of alter_table with cascade option
   * passed in EnvironmentContext using {@code StatsSetupConst.CASCADE}
   */
  @Deprecated
  void alter_table(String defaultDatabaseName, String tblName, Table table,
      boolean cascade) throws InvalidOperationException, MetaException, TException;

  //wrapper of alter_table_with_cascade
  void alter_table_with_environmentContext(String defaultDatabaseName, String tblName, Table table,
      EnvironmentContext environmentContext) throws InvalidOperationException, MetaException,
      TException;

  void createDatabase(Database db)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException;

  void dropDatabase(String name)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException;

  void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException;

  void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException;

  void alterDatabase(String name, Database db)
      throws NoSuchObjectException, MetaException, TException;

  /**
   * @param db_name
   * @param tbl_name
   * @param part_vals
   * @param deleteData
   *          delete the underlying data or just delete the table in metadata
   * @return true or false
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#drop_partition(java.lang.String,
   *      java.lang.String, java.util.List, boolean)
   */
  boolean dropPartition(String db_name, String tbl_name,
      List<String> part_vals, boolean deleteData) throws NoSuchObjectException,
      MetaException, TException;

  /**
   * Method to dropPartitions() with the option to purge the partition data directly,
   * rather than to move data to trash.
   * @param db_name Name of the database.
   * @param tbl_name Name of the table.
   * @param part_vals Specification of the partitions being dropped.
   * @param options PartitionDropOptions for the operation.
   * @return True (if partitions are dropped), else false.
   * @throws TException
   */
  boolean dropPartition(String db_name, String tbl_name, List<String> part_vals,
                        PartitionDropOptions options) throws TException;

  List<Partition> dropPartitions(String dbName, String tblName,
                                 List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData,
                                 boolean ifExists) throws NoSuchObjectException, MetaException, TException;

  List<Partition> dropPartitions(String dbName, String tblName,
      List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData,
      boolean ifExists, boolean needResults) throws NoSuchObjectException, MetaException, TException;

  /**
   * Generalization of dropPartitions(),
   * @param dbName Name of the database
   * @param tblName Name of the table
   * @param partExprs Partition-specification
   * @param options Boolean options for dropping partitions
   * @return List of Partitions dropped
   * @throws TException On failure
   */
  List<Partition> dropPartitions(String dbName, String tblName,
                                 List<ObjectPair<Integer, byte[]>> partExprs, PartitionDropOptions options) throws TException;

  boolean dropPartition(String db_name, String tbl_name,
      String name, boolean deleteData) throws NoSuchObjectException,
      MetaException, TException;

  /**
   * updates a partition to new partition
   *
   * @param dbName
   *          database of the old partition
   * @param tblName
   *          table name of the old partition
   * @param newPart
   *          new partition
   * @throws InvalidOperationException
   *           if the old partition does not exist
   * @throws MetaException
   *           if error in updating metadata
   * @throws TException
   *           if error in communicating with metastore server
   */
  void alter_partition(String dbName, String tblName, Partition newPart)
      throws InvalidOperationException, MetaException, TException;

  /**
   * updates a partition to new partition
   *
   * @param dbName
   *          database of the old partition
   * @param tblName
   *          table name of the old partition
   * @param newPart
   *          new partition
   * @throws InvalidOperationException
   *           if the old partition does not exist
   * @throws MetaException
   *           if error in updating metadata
   * @throws TException
   *           if error in communicating with metastore server
   */
  void alter_partition(String dbName, String tblName, Partition newPart, EnvironmentContext environmentContext)
      throws InvalidOperationException, MetaException, TException;

  /**
   * updates a list of partitions
   *
   * @param dbName
   *          database of the old partition
   * @param tblName
   *          table name of the old partition
   * @param newParts
   *          list of partitions
   * @throws InvalidOperationException
   *           if the old partition does not exist
   * @throws MetaException
   *           if error in updating metadata
   * @throws TException
   *           if error in communicating with metastore server
   */
  void alter_partitions(String dbName, String tblName, List<Partition> newParts)
      throws InvalidOperationException, MetaException, TException;

  /**
   * updates a list of partitions
   *
   * @param dbName
   *          database of the old partition
   * @param tblName
   *          table name of the old partition
   * @param newParts
   *          list of partitions
   * @param environmentContext
   * @throws InvalidOperationException
   *           if the old partition does not exist
   * @throws MetaException
   *           if error in updating metadata
   * @throws TException
   *           if error in communicating with metastore server
   */
  void alter_partitions(String dbName, String tblName, List<Partition> newParts,
      EnvironmentContext environmentContext)
      throws InvalidOperationException, MetaException, TException;

  /**
   * rename a partition to a new partition
   *
   * @param dbname
   *          database of the old partition
   * @param name
   *          table name of the old partition
   * @param part_vals
   *          values of the old partition
   * @param newPart
   *          new partition
   * @throws InvalidOperationException
   *           if srcFs and destFs are different
   * @throws MetaException
   *          if error in updating metadata
   * @throws TException
   *          if error in communicating with metastore server
   */
  void renamePartition(final String dbname, final String name, final List<String> part_vals, final Partition newPart)
      throws InvalidOperationException, MetaException, TException;

  /**
   * @param db
   * @param tableName
   * @throws UnknownTableException
   * @throws UnknownDBException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_fields(java.lang.String,
   *      java.lang.String)
   */
  List<FieldSchema> getFields(String db, String tableName)
      throws MetaException, TException, UnknownTableException,
      UnknownDBException;

  /**
   * @param db
   * @param tableName
   * @throws UnknownTableException
   * @throws UnknownDBException
   * @throws MetaException
   * @throws TException
   * @see org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Iface#get_schema(java.lang.String,
   *      java.lang.String)
   */
  List<FieldSchema> getSchema(String db, String tableName)
      throws MetaException, TException, UnknownTableException,
      UnknownDBException;

  /**
   * @param name
   *          name of the configuration property to get the value of
   * @param defaultValue
   *          the value to return if property with the given name doesn't exist
   * @return value of the specified configuration property
   * @throws TException
   * @throws ConfigValSecurityException
   */
  String getConfigValue(String name, String defaultValue)
      throws TException, ConfigValSecurityException;

  /**
   *
   * @param name
   *          the partition name e.g. ("ds=2010-03-03/hr=12")
   * @return a list containing the partition col values, in the same order as the name
   * @throws MetaException
   * @throws TException
   */
  List<String> partitionNameToVals(String name)
      throws MetaException, TException;
  /**
   *
   * @param name
   *          the partition name e.g. ("ds=2010-03-03/hr=12")
   * @return a map from the partition col to the value, as listed in the name
   * @throws MetaException
   * @throws TException
   */
  Map<String, String> partitionNameToSpec(String name)
      throws MetaException, TException;

  /**
   * Write table level column statistics to persistent store
   * @param statsObj
   * @return boolean indicating the status of the operation
   * @throws NoSuchObjectException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws TException
   * @throws InvalidInputException
   */

  boolean updateTableColumnStatistics(ColumnStatistics statsObj)
    throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
    InvalidInputException;

  /**
   * Write partition level column statistics to persistent store
   * @param statsObj
   * @return boolean indicating the status of the operation
   * @throws NoSuchObjectException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws TException
   * @throws InvalidInputException
   */

  boolean updatePartitionColumnStatistics(ColumnStatistics statsObj)
   throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
   InvalidInputException;

  /**
   * Get table column statistics given dbName, tableName and multiple colName-s
   * @return ColumnStatistics struct for a given db, table and columns
   */
  List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames) throws NoSuchObjectException, MetaException, TException;

  /**
   * Get partitions column statistics given dbName, tableName, multiple partitions and colName-s
   * @return ColumnStatistics struct for a given db, table and columns
   */
  Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName,
      String tableName,  List<String> partNames, List<String> colNames)
          throws NoSuchObjectException, MetaException, TException;

  /**
   * Delete partition level column statistics given dbName, tableName, partName and colName
   * @param dbName
   * @param tableName
   * @param partName
   * @param colName
   * @return boolean indicating outcome of the operation
   * @throws NoSuchObjectException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws TException
   * @throws InvalidInputException
   */

  boolean deletePartitionColumnStatistics(String dbName, String tableName,
    String partName, String colName) throws NoSuchObjectException, MetaException,
    InvalidObjectException, TException, InvalidInputException;

  /**
   * Delete table level column statistics given dbName, tableName and colName
   * @param dbName
   * @param tableName
   * @param colName
   * @return boolean indicating the outcome of the operation
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws InvalidObjectException
   * @throws TException
   * @throws InvalidInputException
   */
   boolean deleteTableColumnStatistics(String dbName, String tableName, String colName) throws
    NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException;

  /**
   * @param role
   *          role object
   * @return true on success
   * @throws MetaException
   * @throws TException
   */
  boolean create_role(Role role)
      throws MetaException, TException;

  /**
   * @param role_name
   *          role name
   *
   * @return true on success
   * @throws MetaException
   * @throws TException
   */
  boolean drop_role(String role_name) throws MetaException, TException;

  /**
   * list all role names
   * @return list of names
   * @throws TException
   * @throws MetaException
   */
  List<String> listRoleNames() throws MetaException, TException;

  /**
   *
   * @param role_name
   * @param user_name
   * @param principalType
   * @param grantor
   * @param grantorType
   * @param grantOption
   * @return true on success
   * @throws MetaException
   * @throws TException
   */
  boolean grant_role(String role_name, String user_name,
      PrincipalType principalType, String grantor, PrincipalType grantorType,
      boolean grantOption) throws MetaException, TException;

  /**
   * @param role_name
   *          role name
   * @param user_name
   *          user name
   * @param principalType
   *
   * @return true on success
   * @throws MetaException
   * @throws TException
   */
  boolean revoke_role(String role_name, String user_name,
      PrincipalType principalType, boolean grantOption) throws MetaException, TException;

  /**
   *
   * @param principalName
   * @param principalType
   * @return list of roles
   * @throws MetaException
   * @throws TException
   */
  List<Role> list_roles(String principalName, PrincipalType principalType)
      throws MetaException, TException;

  /**
   * Return the privileges that the user, group have directly and indirectly through roles
   * on the given hiveObject
   * @param hiveObject
   * @param user_name
   * @param group_names
   * @return the privilege set
   * @throws MetaException
   * @throws TException
   */
  PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject,
      String user_name, List<String> group_names) throws MetaException,
      TException;

  /**
   * Return the privileges that this principal has directly over the object (not through roles).
   * @param principal_name
   * @param principal_type
   * @param hiveObject
   * @return list of privileges
   * @throws MetaException
   * @throws TException
   */
  List<HiveObjectPrivilege> list_privileges(String principal_name,
      PrincipalType principal_type, HiveObjectRef hiveObject)
      throws MetaException, TException;

  /**
   * @param privileges
   * @return true on success
   * @throws MetaException
   * @throws TException
   */
  boolean grant_privileges(PrivilegeBag privileges)
      throws MetaException, TException;

  /**
   * @param privileges
   * @return true on success
   * @throws MetaException
   * @throws TException
   */
  boolean revoke_privileges(PrivilegeBag privileges, boolean grantOption)
      throws MetaException, TException;

  /**
   * This is expected to be a no-op when in local mode,
   * which means that the implementation will return null.
   * @param owner the intended owner for the token
   * @param renewerKerberosPrincipalName
   * @return the string of the token
   * @throws MetaException
   * @throws TException
   */
  String getDelegationToken(String owner, String renewerKerberosPrincipalName)
      throws MetaException, TException;

  /**
   * @param tokenStrForm
   * @return the new expiration time
   * @throws MetaException
   * @throws TException
   */
  long renewDelegationToken(String tokenStrForm) throws MetaException, TException;

  /**
   * @param tokenStrForm
   * @throws MetaException
   * @throws TException
   */
  void cancelDelegationToken(String tokenStrForm) throws MetaException, TException;

  String getTokenStrForm() throws IOException;

  boolean addToken(String tokenIdentifier, String delegationToken) throws TException;

  boolean removeToken(String tokenIdentifier) throws TException;

  String getToken(String tokenIdentifier) throws TException;

  List<String> getAllTokenIdentifiers() throws TException;

  int addMasterKey(String key) throws MetaException, TException;

  void updateMasterKey(Integer seqNo, String key)
      throws NoSuchObjectException, MetaException, TException;

  boolean removeMasterKey(Integer keySeq) throws TException;

  String[] getMasterKeys() throws TException;

  void createFunction(Function func)
      throws InvalidObjectException, MetaException, TException;

  void alterFunction(String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException, TException;

  void dropFunction(String dbName, String funcName) throws MetaException,
      NoSuchObjectException, InvalidObjectException, InvalidInputException, TException;

  Function getFunction(String dbName, String funcName)
      throws MetaException, TException;

  List<String> getFunctions(String dbName, String pattern)
      throws MetaException, TException;

  GetAllFunctionsResponse getAllFunctions()
          throws MetaException, TException;

  /**
   * Get a structure that details valid transactions.
   * @return list of valid transactions
   * @throws TException
   */
  ValidTxnList getValidTxns() throws TException;

  /**
   * Get a structure that details valid transactions.
   * @param currentTxn The current transaction of the caller. This will be removed from the
   *                   exceptions list so that the caller sees records from his own transaction.
   * @return list of valid transactions and also valid write IDs for each input table.
   * @throws TException
   */
  ValidTxnList getValidTxns(long currentTxn) throws TException;

  /**
   * Get a structure that details valid transactions.
   * @param fullTableName full table name of format <db_name>.<table_name>
   * @return list of valid write ids for the given table
   * @throws TException
   */
  ValidWriteIdList getValidWriteIds(String fullTableName) throws TException;

  /**
   * Get a structure that details valid write ids list for all tables read by current txn.
   * @param currentTxnId current txn ID for which we try to get valid write ids list
   * @param tablesList list of tables (format: <db_name>.<table_name>) read from the current transaction
   *                   for which needs to populate the valid write ids
   * @param validTxnList snapshot of valid txns for the current txn
   * @return list of valid write ids for the given list of tables.
   * @throws TException
   */
  ValidTxnWriteIdList getValidWriteIds(Long currentTxnId, List<String> tablesList, String validTxnList)
          throws TException;

  /**
   * Initiate a transaction.
   * @param user User who is opening this transaction.  This is the Hive user,
   *             not necessarily the OS user.  It is assumed that this user has already been
   *             authenticated and authorized at this point.
   * @return transaction identifier
   * @throws TException
   */
  long openTxn(String user) throws TException;

  /**
   * Initiate a batch of transactions.  It is not guaranteed that the
   * requested number of transactions will be instantiated.  The system has a
   * maximum number instantiated per request, controlled by hive.txn.max
   * .batch.open in hive-site.xml.  If the user requests more than this
   * value, only the configured max will be returned.
   *
   * <p>Increasing the number of transactions requested in the batch will
   * allow applications that stream data into Hive to place more commits in a
   * single file, thus reducing load on the namenode and making reads of the
   * data more efficient.  However, opening more transactions in a batch will
   * also result in readers needing to keep a larger list of open
   * transactions to ignore, potentially slowing their reads.  Users will
   * need to test in their system to understand the optimal number of
   * transactions to request in a batch.
   * </p>
   * @param user User who is opening this transaction.  This is the Hive user,
   *             not necessarily the OS user.  It is assumed that this user has already been
   *             authenticated and authorized at this point.
   * @param numTxns number of requested transactions to open
   * @return list of opened txn ids.  As noted above, this may be less than
   * requested, so the user should check how many were returned rather than
   * optimistically assuming that the result matches the request.
   * @throws TException
   */
  OpenTxnsResponse openTxns(String user, int numTxns) throws TException;

  /**
   * Rollback a transaction.  This will also unlock any locks associated with
   * this transaction.
   * @param txnid id of transaction to be rolled back.
   * @throws NoSuchTxnException if the requested transaction does not exist.
   * Note that this can result from the transaction having timed out and been
   * deleted.
   * @throws TException
   */
  void rollbackTxn(long txnid) throws NoSuchTxnException, TException;

  /**
   * Commit a transaction.  This will also unlock any locks associated with
   * this transaction.
   * @param txnid id of transaction to be committed.
   * @throws NoSuchTxnException if the requested transaction does not exist.
   * This can result fro the transaction having timed out and been deleted by
   * the compactor.
   * @throws TxnAbortedException if the requested transaction has been
   * aborted.  This can result from the transaction timing out.
   * @throws TException
   */
  void commitTxn(long txnid)
      throws NoSuchTxnException, TxnAbortedException, TException;

  /**
   * Abort a list of transactions. This is for use by "ABORT TRANSACTIONS" in the grammar.
   * @throws TException
   */
  void abortTxns(List<Long> txnids) throws TException;

  /**
   * Allocate a per table write ID and associate it with the given transaction.
   * @param txnId id of transaction to which the allocated write ID to be associated.
   * @param dbName name of DB in which the table belongs.
   * @param tableName table to which the write ID to be allocated
   * @throws TException
   */
  long allocateTableWriteId(long txnId, String dbName, String tableName) throws TException;

  /**
   * Allocate a per table write ID and associate it with the given transaction.
   * @param txnIds ids of transaction batchto which the allocated write ID to be associated.
   * @param dbName name of DB in which the table belongs.
   * @param tableName table to which the write ID to be allocated
   * @throws TException
   */
  List<TxnToWriteId> allocateTableWriteIdsBatch(List<Long> txnIds, String dbName, String tableName) throws TException;

  /**
   * Show the list of currently open transactions.  This is for use by "show transactions" in the
   * grammar, not for applications that want to find a list of current transactions to work with.
   * Those wishing the latter should call {@link #getValidTxns()}.
   * @return List of currently opened transactions, included aborted ones.
   * @throws TException
   */
  GetOpenTxnsInfoResponse showTxns() throws TException;

  /**
   * Request a set of locks.  All locks needed for a particular query, DML,
   * or DDL operation should be batched together and requested in one lock
   * call.  This avoids deadlocks.  It also avoids blocking other users who
   * only require some of the locks required by this user.
   *
   * <p>If the operation requires a transaction (INSERT, UPDATE,
   * or DELETE) that transaction id must be provided as part this lock
   * request.  All locks associated with a transaction will be released when
   * that transaction is committed or rolled back.</p>
   * *
   * <p>Once a lock is acquired, {@link #heartbeat(long, long)} must be called
   * on a regular basis to avoid the lock being timed out by the system.</p>
   * @param request The lock request.  {@link LockRequestBuilder} can be used
   *                construct this request.
   * @return a lock response, which will provide two things,
   * the id of the lock (to be used in all further calls regarding this lock)
   * as well as a state of the lock.  If the state is ACQUIRED then the user
   * can proceed.  If it is WAITING the user should wait and call
   * {@link #checkLock(long)} before proceeding.  All components of the lock
   * will have the same state.
   * @throws NoSuchTxnException if the requested transaction does not exist.
   * This can result fro the transaction having timed out and been deleted by
   * the compactor.
   * @throws TxnAbortedException if the requested transaction has been
   * aborted.  This can result from the transaction timing out.
   * @throws TException
   */
  @RetrySemantics.CannotRetry
  LockResponse lock(LockRequest request)
      throws NoSuchTxnException, TxnAbortedException, TException;

  /**
   * Check the status of a set of locks requested via a
   * {@link #lock(org.apache.hadoop.hive.metastore.api.LockRequest)} call.
   * Once a lock is acquired, {@link #heartbeat(long, long)} must be called
   * on a regular basis to avoid the lock being timed out by the system.
   * @param lockid lock id returned by lock().
   * @return a lock response, which will provide two things,
   * the id of the lock (to be used in all further calls regarding this lock)
   * as well as a state of the lock.  If the state is ACQUIRED then the user
   * can proceed.  If it is WAITING the user should wait and call
   * this method again before proceeding.  All components of the lock
   * will have the same state.
   * @throws NoSuchTxnException if the requested transaction does not exist.
   * This can result fro the transaction having timed out and been deleted by
   * the compactor.
   * @throws TxnAbortedException if the requested transaction has been
   * aborted.  This can result from the transaction timing out.
   * @throws NoSuchLockException if the requested lockid does not exist.
   * This can result from the lock timing out and being unlocked by the system.
   * @throws TException
   */
  LockResponse checkLock(long lockid)
    throws NoSuchTxnException, TxnAbortedException, NoSuchLockException,
      TException;

  /**
   * Unlock a set of locks.  This can only be called when the locks are not
   * assocaited with a transaction.
   * @param lockid lock id returned by
   * {@link #lock(org.apache.hadoop.hive.metastore.api.LockRequest)}
   * @throws NoSuchLockException if the requested lockid does not exist.
   * This can result from the lock timing out and being unlocked by the system.
   * @throws TxnOpenException if the locks are are associated with a
   * transaction.
   * @throws TException
   */
  void unlock(long lockid)
      throws NoSuchLockException, TxnOpenException, TException;

  /**
   * Show all currently held and waiting locks.
   * @return List of currently held and waiting locks.
   * @throws TException
   */
  @Deprecated
  ShowLocksResponse showLocks() throws TException;

  /**
   * Show all currently held and waiting locks.
   * @param showLocksRequest SHOW LOCK request
   * @return List of currently held and waiting locks.
   * @throws TException
   */
  ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException;

  /**
   * Send a heartbeat to indicate that the client holding these locks (if
   * any) and that opened this transaction (if one exists) is still alive.
   * The default timeout for transactions and locks is 300 seconds,
   * though it is configurable.  To determine how often to heartbeat you will
   * need to ask your system administrator how the metastore thrift service
   * has been configured.
   * @param txnid the id of the open transaction.  If no transaction is open
   *              (it is a DDL or query) then this can be set to 0.
   * @param lockid the id of the locks obtained.  If no locks have been
   *               obtained then this can be set to 0.
   * @throws NoSuchTxnException if the requested transaction does not exist.
   * This can result fro the transaction having timed out and been deleted by
   * the compactor.
   * @throws TxnAbortedException if the requested transaction has been
   * aborted.  This can result from the transaction timing out.
   * @throws NoSuchLockException if the requested lockid does not exist.
   * This can result from the lock timing out and being unlocked by the system.
   * @throws TException
   */
  void heartbeat(long txnid, long lockid)
    throws NoSuchLockException, NoSuchTxnException, TxnAbortedException,
      TException;

  /**
   * Send heartbeats for a range of transactions.  This is for the streaming ingest client that
   * will have many transactions open at once.  Everyone else should use
   * {@link #heartbeat(long, long)}.
   * @param min minimum transaction id to heartbeat, inclusive
   * @param max maximum transaction id to heartbeat, inclusive
   * @return a pair of lists that tell which transactions in the list did not exist (they may
   * have already been closed) and which were aborted.
   * @throws TException
   */
  HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws TException;

  /**
   * Send a request to compact a table or partition.  This will not block until the compaction is
   * complete.  It will instead put a request on the queue for that table or partition to be
   * compacted.  No checking is done on the dbname, tableName, or partitionName to make sure they
   * refer to valid objects.  It is assumed this has already been done by the caller.
   * @param dbname Name of the database the table is in.  If null, this will be assumed to be
   *               'default'.
   * @param tableName Name of the table to be compacted.  This cannot be null.  If partitionName
   *                  is null, this must be a non-partitioned table.
   * @param partitionName Name of the partition to be compacted
   * @param type Whether this is a major or minor compaction.
   * @throws TException
   */
  @Deprecated
  void compact(String dbname, String tableName, String partitionName,  CompactionType type)
      throws TException;
  @Deprecated
  void compact(String dbname, String tableName, String partitionName, CompactionType type,
               Map<String, String> tblproperties) throws TException;
  /**
   * Send a request to compact a table or partition.  This will not block until the compaction is
   * complete.  It will instead put a request on the queue for that table or partition to be
   * compacted.  No checking is done on the dbname, tableName, or partitionName to make sure they
   * refer to valid objects.  It is assumed this has already been done by the caller.  At most one
   * Compaction can be scheduled/running for any given resource at a time.
   * @param dbname Name of the database the table is in.  If null, this will be assumed to be
   *               'default'.
   * @param tableName Name of the table to be compacted.  This cannot be null.  If partitionName
   *                  is null, this must be a non-partitioned table.
   * @param partitionName Name of the partition to be compacted
   * @param type Whether this is a major or minor compaction.
   * @param tblproperties the list of tblproperties to override for this compact. Can be null.
   * @return id of newly scheduled compaction or id/state of one which is already scheduled/running
   * @throws TException
   */
  CompactionResponse compact2(String dbname, String tableName, String partitionName, CompactionType type,
                              Map<String, String> tblproperties) throws TException;

  /**
   * Get a list of all compactions.
   * @return List of all current compactions.  This includes compactions waiting to happen,
   * in progress, and finished but waiting to clean the existing files.
   * @throws TException
   */
  ShowCompactResponse showCompactions() throws TException;

  /**
   * @deprecated in Hive 1.3.0/2.1.0 - will be removed in 2 releases
   */
  @Deprecated
  void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName, List<String> partNames)
    throws TException;
  /**
   * Send a list of partitions to the metastore to indicate which partitions were loaded
   * dynamically.
   * @param txnId id of the transaction
   * @param writeId table write id for this txn
   * @param dbName database name
   * @param tableName table name
   * @param partNames partition name, as constructed by Warehouse.makePartName
   * @throws TException
   */
  void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName, List<String> partNames,
                            DataOperationType operationType)
    throws TException;

  /**
   * Performs the commit/rollback to the metadata storage for insert operator from external storage handler.
   * @param table table name
   * @param overwrite true if the insert is overwrite
   *
   * @throws MetaException
   */
  void insertTable(Table table, boolean overwrite) throws MetaException;

  /**
   * A filter provided by the client that determines if a given notification event should be
   * returned.
   */
  @InterfaceAudience.LimitedPrivate({"HCatalog"})
  interface NotificationFilter {
    /**
     * Whether a notification event should be accepted
     * @param event
     * @return if true, event will be added to list, if false it will be ignored
     */
    boolean accept(NotificationEvent event);
  }

  /**
   * Get the next set of notifications from the database.
   * @param lastEventId The last event id that was consumed by this reader.  The returned
   *                    notifications will start at the next eventId available after this eventId.
   * @param maxEvents Maximum number of events to return.  If &lt; 1, then all available events will
   *                  be returned.
   * @param filter User provided filter to remove unwanted events.  If null, all events will be
   *               returned.
   * @return list of notifications, sorted by eventId.  It is guaranteed that the events are in
   * the order that the operations were done on the database.
   * @throws TException
   */
  @InterfaceAudience.LimitedPrivate({"HCatalog"})
  NotificationEventResponse getNextNotification(long lastEventId, int maxEvents,
                                                NotificationFilter filter) throws TException;

  /**
   * Get the last used notification event id.
   * @return last used id
   * @throws TException
   */
  @InterfaceAudience.LimitedPrivate({"HCatalog"})
  CurrentNotificationEventId getCurrentNotificationEventId() throws TException;

  /**
   * Get the number of events from given eventID for the input database.
   * @return number of events
   * @throws TException
   */
  @InterfaceAudience.LimitedPrivate({"HCatalog"})
  NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst)
          throws TException;

  /**
   * Request that the metastore fire an event.  Currently this is only supported for DML
   * operations, since the metastore knows when DDL operations happen.
   * @param request
   * @return response, type depends on type of request
   * @throws TException
   */

  @InterfaceAudience.LimitedPrivate({"Apache Hive, HCatalog"})
  FireEventResponse fireListenerEvent(FireEventRequest request) throws TException;

  class IncompatibleMetastoreException extends MetaException {
    IncompatibleMetastoreException(String message) {
      super(message);
    }
  }

  /**
   * get all role-grants for users/roles that have been granted the given role
   * Note that in the returned list of RolePrincipalGrants, the roleName is
   * redundant as it would match the role_name argument of this function
   * @param getPrincRoleReq
   * @return
   * @throws MetaException
   * @throws TException
   */
  GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest getPrincRoleReq)
      throws MetaException, TException;

  /**
   * get all role-grants for roles that have been granted to given principal
   * Note that in the returned list of RolePrincipalGrants, the principal information
   * redundant as it would match the principal information in request
   * @param getRolePrincReq
   * @return
   * @throws MetaException
   * @throws TException
   */
  GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
      GetRoleGrantsForPrincipalRequest getRolePrincReq) throws MetaException, TException;

  public AggrStats getAggrColStatsFor(String dbName, String tblName,
      List<String> colNames, List<String> partName)  throws NoSuchObjectException, MetaException, TException;

  boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request) throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException;

  /**
   * Flush any catalog objects held by the metastore implementation.  Note that this does not
   * flush statistics objects.  This should be called at the beginning of each query.
   */
  void flushCache();

  /**
   * Gets file metadata, as cached by metastore, for respective file IDs.
   * The metadata that is not cached in metastore may be missing.
   */
  Iterable<Entry<Long, ByteBuffer>> getFileMetadata(List<Long> fileIds) throws TException;

  Iterable<Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(
      List<Long> fileIds, ByteBuffer sarg, boolean doGetFooters) throws TException;

  /**
   * Cleares the file metadata cache for respective file IDs.
   */
  void clearFileMetadata(List<Long> fileIds) throws TException;

  /**
   * Adds file metadata for respective file IDs to metadata cache in metastore.
   */
  void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException;

  boolean isSameConfObj(Configuration c);

  boolean cacheFileMetadata(String dbName, String tableName, String partName,
      boolean allParts) throws TException;

  List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest request)
    throws MetaException, NoSuchObjectException, TException;

  List<SQLForeignKey> getForeignKeys(ForeignKeysRequest request) throws MetaException,
    NoSuchObjectException, TException;

  List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest request) throws MetaException,
    NoSuchObjectException, TException;

  List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest request) throws MetaException,
    NoSuchObjectException, TException;

  List<SQLDefaultConstraint> getDefaultConstraints(DefaultConstraintsRequest request) throws MetaException,
      NoSuchObjectException, TException;

  List<SQLCheckConstraint> getCheckConstraints(CheckConstraintsRequest request) throws MetaException,
      NoSuchObjectException, TException;

  void createTableWithConstraints(
    org.apache.hadoop.hive.metastore.api.Table tTbl,
    List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys,
    List<SQLUniqueConstraint> uniqueConstraints,
    List<SQLNotNullConstraint> notNullConstraints,
    List<SQLDefaultConstraint> defaultConstraints,
    List<SQLCheckConstraint> checkConstraints)
    throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException, TException;

  void dropConstraint(String dbName, String tableName, String constraintName) throws
    MetaException, NoSuchObjectException, TException;

  void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols) throws
  MetaException, NoSuchObjectException, TException;

  void addForeignKey(List<SQLForeignKey> foreignKeyCols) throws
  MetaException, NoSuchObjectException, TException;

  void addUniqueConstraint(List<SQLUniqueConstraint> uniqueConstraintCols) throws
  MetaException, NoSuchObjectException, TException;

  void addNotNullConstraint(List<SQLNotNullConstraint> notNullConstraintCols) throws
  MetaException, NoSuchObjectException, TException;

  void addDefaultConstraint(List<SQLDefaultConstraint> defaultConstraints) throws
      MetaException, NoSuchObjectException, TException;

  void addCheckConstraint(List<SQLCheckConstraint> checkConstraints) throws
      MetaException, NoSuchObjectException, TException;

  /**
   * Gets the unique id of the backing database instance used for storing metadata
   * @return unique id of the backing database instance
   * @throws MetaException if HMS is not able to fetch the UUID or if there are multiple UUIDs found in the database
   * @throws TException in case of Thrift errors
   */
  String getMetastoreDbUuid() throws MetaException, TException;

  void createResourcePlan(WMResourcePlan resourcePlan, String copyFromName)
      throws InvalidObjectException, MetaException, TException;

  WMFullResourcePlan getResourcePlan(String resourcePlanName)
    throws NoSuchObjectException, MetaException, TException;

  List<WMResourcePlan> getAllResourcePlans()
      throws NoSuchObjectException, MetaException, TException;

  void dropResourcePlan(String resourcePlanName)
      throws NoSuchObjectException, MetaException, TException;

  WMFullResourcePlan alterResourcePlan(String resourcePlanName, WMNullableResourcePlan resourcePlan,
      boolean canActivateDisabled, boolean isForceDeactivate, boolean isReplace)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException;

  WMFullResourcePlan getActiveResourcePlan() throws MetaException, TException;

  WMValidateResourcePlanResponse validateResourcePlan(String resourcePlanName)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException;

  void createWMTrigger(WMTrigger trigger)
      throws InvalidObjectException, MetaException, TException;

  void alterWMTrigger(WMTrigger trigger)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException;

  void dropWMTrigger(String resourcePlanName, String triggerName)
      throws NoSuchObjectException, MetaException, TException;

  List<WMTrigger> getTriggersForResourcePlan(String resourcePlan)
      throws NoSuchObjectException, MetaException, TException;

  void createWMPool(WMPool pool)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException;

  void alterWMPool(WMNullablePool pool, String poolPath)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException;

  void dropWMPool(String resourcePlanName, String poolPath)
      throws NoSuchObjectException, MetaException, TException;

  void createOrUpdateWMMapping(WMMapping mapping, boolean isUpdate)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException;

  void dropWMMapping(WMMapping mapping)
      throws NoSuchObjectException, MetaException, TException;

  void createOrDropTriggerToPoolMapping(String resourcePlanName, String triggerName,
      String poolPath, boolean shouldDrop) throws AlreadyExistsException, NoSuchObjectException,
      InvalidObjectException, MetaException, TException;

  /**
   * Create a new schema.  This is really a schema container, as there will be specific versions
   * of the schema that have columns, etc.
   * @param schema schema to create
   * @throws AlreadyExistsException if a schema of this name already exists
   * @throws NoSuchObjectException database references by this schema does not exist
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  void createISchema(ISchema schema) throws TException;

  /**
   * Alter an existing schema.
   * @param dbName database the schema is in
   * @param schemaName name of the schema
   * @param newSchema altered schema object
   * @throws NoSuchObjectException no schema with this name could be found
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  void alterISchema(String dbName, String schemaName, ISchema newSchema) throws TException;

  /**
   * Fetch a schema.
   * @param dbName database the schema is in
   * @param name name of the schema
   * @return the schema or null if no such schema
   * @throws NoSuchObjectException no schema matching this name exists
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  ISchema getISchema(String dbName, String name) throws TException;

  /**
   * Drop an existing schema.  If there are schema versions of this, this call will fail.
   * @param dbName database the schema is in
   * @param name name of the schema to drop
   * @throws NoSuchObjectException no schema with this name could be found
   * @throws InvalidOperationException attempt to drop a schema that has versions
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  void dropISchema(String dbName, String name) throws TException;

  /**
   * Add a new version to an existing schema.
   * @param schemaVersion version object to add
   * @throws AlreadyExistsException a version of this schema with the same version id already exists
   * @throws NoSuchObjectException no schema with this name could be found
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  void addSchemaVersion(SchemaVersion schemaVersion) throws TException;

  /**
   * Get a specific version of a schema.
   * @param dbName database the schema is in
   * @param schemaName name of the schema
   * @param version version of the schema
   * @return the schema version or null if no such schema version
   * @throws NoSuchObjectException no schema matching this name and version exists
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  SchemaVersion getSchemaVersion(String dbName, String schemaName, int version) throws TException;

  /**
   * Get the latest version of a schema.
   * @param dbName database the schema is in
   * @param schemaName name of the schema
   * @return latest version of the schema or null if the schema does not exist or there are no
   * version of the schema.
   * @throws NoSuchObjectException no versions of schema matching this name exist
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  SchemaVersion getSchemaLatestVersion(String dbName, String schemaName) throws TException;

  /**
   * Get all the extant versions of a schema.
   * @param dbName database the schema is in
   * @param schemaName name of the schema.
   * @return list of all the schema versions or null if this schema does not exist or has no
   * versions.
   * @throws NoSuchObjectException no versions of schema matching this name exist
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  List<SchemaVersion> getSchemaAllVersions(String dbName, String schemaName) throws TException;

  /**
   * Drop a version of a schema.  Given that versions are supposed to be immutable you should
   * think really hard before you call this method.  It should only be used for schema versions
   * that were added in error and never referenced any data.
   * @param dbName database the schema is in
   * @param schemaName name of the schema
   * @param version version of the schema
   * @throws NoSuchObjectException no matching version of the schema could be found
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  void dropSchemaVersion(String dbName, String schemaName, int version) throws TException;

  /**
   * Find all schema versions that have columns that match a query.
   * @param rqst query, this can include column names, namespaces (actually stored in the
   *             description field in FieldSchema), and types.
   * @return The (possibly empty) list of schema name/version pairs that match.
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  FindSchemasByColsResp getSchemaByCols(FindSchemasByColsRqst rqst) throws TException;

  /**
   * Map a schema version to a serde.  This mapping is one-to-one, thus this will destroy any
   * previous mappings for this schema version.
   * @param dbName database the schema is in
   * @param schemaName name of the schema
   * @param version version of the schema
   * @param serdeName name of the serde
   * @throws NoSuchObjectException no matching version of the schema could be found or no serde
   * of the provided name could be found
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  void mapSchemaVersionToSerde(String dbName, String schemaName, int version, String serdeName) throws TException;

  /**
   * Set the state of a schema version.
   * @param dbName database the schema is in
   * @param schemaName name of the schema
   * @param version version of the schema
   * @param state state to set the schema too
   * @throws NoSuchObjectException no matching version of the schema could be found
   * @throws InvalidOperationException attempt to make a state change that is not valid
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  void setSchemaVersionState(String dbName, String schemaName, int version, SchemaVersionState state) throws TException;

  /**
   * Add a serde.  This is primarily intended for use with SchemaRegistry objects, since serdes
   * are automatically added when needed as part of creating and altering tables and partitions.
   * @param serDeInfo serde to add
   * @throws AlreadyExistsException serde of this name already exists
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  void addSerDe(SerDeInfo serDeInfo) throws TException;

  /**
   * Fetch a serde.  This is primarily intended for use with SchemaRegistry objects, since serdes
   * are automatically fetched along with other information for tables and partitions.
   * @param serDeName name of the serde
   * @return the serde.
   * @throws NoSuchObjectException no serde with this name exists.
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  SerDeInfo getSerDe(String serDeName) throws TException;
}
