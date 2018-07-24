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
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.classification.RetrySemantics;
import org.apache.hadoop.hive.metastore.annotation.NoReconnect;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.CmRecycleRequest;
import org.apache.hadoop.hive.metastore.api.CmRecycleResponse;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
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
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
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
import org.apache.hadoop.hive.metastore.api.TableValidWriteIds;
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
import org.apache.hadoop.hive.metastore.api.WriteNotificationLogRequest;
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
   * Create a new catalog.
   * @param catalog catalog object to create.
   * @throws AlreadyExistsException A catalog of this name already exists.
   * @throws InvalidObjectException There is something wrong with the passed in catalog object.
   * @throws MetaException something went wrong, usually either in the database or trying to
   * create the directory for the catalog.
   * @throws TException general thrift exception.
   */
  void createCatalog(Catalog catalog)
      throws AlreadyExistsException, InvalidObjectException, MetaException, TException;

  /**
   * Alter an existing catalog.
   * @param catalogName the name of the catalog to alter.
   * @param newCatalog the new catalog object.  All relevant details of the catalog should be
   *                   set, don't rely on the system to figure out what you changed and only copy
   *                   that in.
   * @throws NoSuchObjectException no catalog of this name exists
   * @throws InvalidObjectException an attempt was made to make an unsupported change (such as
   * catalog name).
   * @throws MetaException usually indicates a database error
   * @throws TException general thrift exception
   */
  void alterCatalog(String catalogName, Catalog newCatalog)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException;

  /**
   * Get a catalog object.
   * @param catName Name of the catalog to fetch.
   * @return The catalog.
   * @throws NoSuchObjectException no catalog of this name exists.
   * @throws MetaException something went wrong, usually in the database.
   * @throws TException general thrift exception.
   */
  Catalog getCatalog(String catName) throws NoSuchObjectException, MetaException, TException;

  /**
   * Get a list of all catalogs known to the system.
   * @return list of catalog names
   * @throws MetaException something went wrong, usually in the database.
   * @throws TException general thrift exception.
   */
  List<String> getCatalogs() throws MetaException, TException;

  /**
   * Drop a catalog.  Catalogs must be empty to be dropped, there is no cascade for dropping a
   * catalog.
   * @param catName name of the catalog to drop
   * @throws NoSuchObjectException no catalog of this name exists.
   * @throws InvalidOperationException The catalog is not empty and cannot be dropped.
   * @throws MetaException something went wrong, usually in the database.
   * @throws TException general thrift exception.
   */
  void dropCatalog(String catName)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException;

  /**
   * Get the names of all databases in the default catalog that match the given pattern.
   * @param databasePattern pattern for the database name to patch
   * @return List of database names.
   * @throws MetaException error accessing RDBMS.
   * @throws TException thrift transport error
   */
  List<String> getDatabases(String databasePattern) throws MetaException, TException;

  /**
   * Get all databases in a catalog whose names match a pattern.
   * @param catName  catalog name.  Can be null, in which case the default catalog is assumed.
   * @param databasePattern pattern for the database name to match
   * @return list of database names
   * @throws MetaException error accessing RDBMS.
   * @throws TException thrift transport error
   */
  List<String> getDatabases(String catName, String databasePattern)
      throws MetaException, TException;

  /**
   * Get the names of all databases in the MetaStore.
   * @return List of database names in the default catalog.
   * @throws MetaException error accessing RDBMS.
   * @throws TException thrift transport error
   */
  List<String> getAllDatabases() throws MetaException, TException;

  /**
   * Get all databases in a catalog.
   * @param catName catalog name.  Can be null, in which case the default catalog is assumed.
   * @return list of all database names
   * @throws MetaException error accessing RDBMS.
   * @throws TException thrift transport error
   */
  List<String> getAllDatabases(String catName) throws MetaException, TException;

  /**
   * Get the names of all tables in the specified database that satisfy the supplied
   * table name pattern.
   * @param dbName database name.
   * @param tablePattern pattern for table name to conform to
   * @return List of table names.
   * @throws MetaException error fetching information from the RDBMS
   * @throws TException thrift transport error
   * @throws UnknownDBException indicated database to search in does not exist.
   */
  List<String> getTables(String dbName, String tablePattern)
      throws MetaException, TException, UnknownDBException;

  /**
   * Get the names of all tables in the specified database that satisfy the supplied
   * table name pattern.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tablePattern pattern for table name to conform to
   * @return List of table names.
   * @throws MetaException error fetching information from the RDBMS
   * @throws TException general thrift error
   * @throws UnknownDBException indicated database to search in does not exist.
   */
  List<String> getTables(String catName, String dbName, String tablePattern)
      throws MetaException, TException, UnknownDBException;


  /**
   * Get the names of all tables in the specified database that satisfy the supplied
   * table name pattern and table type (MANAGED_TABLE || EXTERNAL_TABLE || VIRTUAL_VIEW)
   * @param dbName Name of the database to fetch tables in.
   * @param tablePattern pattern to match for table names.
   * @param tableType Type of the table in the HMS store. VIRTUAL_VIEW is for views.
   * @return List of table names.
   * @throws MetaException error fetching information from the RDBMS
   * @throws TException thrift transport error
   * @throws UnknownDBException indicated database does not exist.
   */
  List<String> getTables(String dbName, String tablePattern, TableType tableType)
      throws MetaException, TException, UnknownDBException;

  /**
   * Get the names of all tables in the specified database that satisfy the supplied
   * table name pattern and table type (MANAGED_TABLE || EXTERNAL_TABLE || VIRTUAL_VIEW)
   * @param catName catalog name.
   * @param dbName Name of the database to fetch tables in.
   * @param tablePattern pattern to match for table names.
   * @param tableType Type of the table in the HMS store. VIRTUAL_VIEW is for views.
   * @return List of table names.
   * @throws MetaException error fetching information from the RDBMS
   * @throws TException thrift transport error
   * @throws UnknownDBException indicated database does not exist.
   */
  List<String> getTables(String catName, String dbName, String tablePattern, TableType tableType)
      throws MetaException, TException, UnknownDBException;

  /**
   * Get materialized views that have rewriting enabled.  This will use the default catalog.
   * @param dbName Name of the database to fetch materialized views from.
   * @return List of materialized view names.
   * @throws MetaException error fetching from the RDBMS
   * @throws TException thrift transport error
   * @throws UnknownDBException no such database
   */
  List<String> getMaterializedViewsForRewriting(String dbName)
      throws MetaException, TException, UnknownDBException;

  /**
   * Get materialized views that have rewriting enabled.
   * @param catName catalog name.
   * @param dbName Name of the database to fetch materialized views from.
   * @return List of materialized view names.
   * @throws MetaException error fetching from the RDBMS
   * @throws TException thrift transport error
   * @throws UnknownDBException no such database
   */
  List<String> getMaterializedViewsForRewriting(String catName, String dbName)
      throws MetaException, TException, UnknownDBException;

  /**
   * Fetches just table name and comments.  Useful when you need full table name
   * (catalog.database.table) but don't need extra information like partition columns that
   * require additional fetches from the database.
   * @param dbPatterns database pattern to match, or null for all databases
   * @param tablePatterns table pattern to match.
   * @param tableTypes list of table types to fetch.
   * @return list of TableMeta objects with information on matching tables
   * @throws MetaException something went wrong with the fetch from the RDBMS
   * @throws TException thrift transport error
   * @throws UnknownDBException No databases match the provided pattern.
   */
  List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes)
      throws MetaException, TException, UnknownDBException;

  /**
   * Fetches just table name and comments.  Useful when you need full table name
   * (catalog.database.table) but don't need extra information like partition columns that
   * require additional fetches from the database.
   * @param catName catalog to search in.  Search cannot cross catalogs.
   * @param dbPatterns database pattern to match, or null for all databases
   * @param tablePatterns table pattern to match.
   * @param tableTypes list of table types to fetch.
   * @return list of TableMeta objects with information on matching tables
   * @throws MetaException something went wrong with the fetch from the RDBMS
   * @throws TException thrift transport error
   * @throws UnknownDBException No databases match the provided pattern.
   */
  List<TableMeta> getTableMeta(String catName, String dbPatterns, String tablePatterns,
                               List<String> tableTypes)
      throws MetaException, TException, UnknownDBException;

  /**
   * Get the names of all tables in the specified database.
   * @param dbName database name
   * @return List of table names.
   * @throws MetaException something went wrong with the fetch from the RDBMS
   * @throws TException thrift transport error
   * @throws UnknownDBException No databases match the provided pattern.
   */
  List<String> getAllTables(String dbName) throws MetaException, TException, UnknownDBException;

  /**
   * Get the names of all tables in the specified database.
   * @param catName catalog name
   * @param dbName database name
   * @return List of table names.
   * @throws MetaException something went wrong with the fetch from the RDBMS
   * @throws TException thrift transport error
   * @throws UnknownDBException No databases match the provided pattern.
   */
  List<String> getAllTables(String catName, String dbName)
      throws MetaException, TException, UnknownDBException;

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
   * @throws InvalidOperationException invalid filter
   * @throws UnknownDBException no such database
   * @throws TException thrift transport error
   */
  List<String> listTableNamesByFilter(String dbName, String filter, short maxTables)
      throws TException, InvalidOperationException, UnknownDBException;

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
   * @param catName catalog name
   * @param dbName
   *          The name of the database from which you will retrieve the table names
   * @param filter
   *          The filter string
   * @param maxTables
   *          The maximum number of tables returned
   * @return  A list of table names that match the desired filter
   * @throws InvalidOperationException invalid filter
   * @throws UnknownDBException no such database
   * @throws TException thrift transport error
   */
  List<String> listTableNamesByFilter(String catName, String dbName, String filter, int maxTables)
      throws TException, InvalidOperationException, UnknownDBException;

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
   *
   */
  void dropTable(String dbname, String tableName, boolean deleteData,
      boolean ignoreUnknownTab) throws MetaException, TException,
      NoSuchObjectException;

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
   * @param ifPurge
   *          completely purge the table (skipping trash) while removing data from warehouse
   * @throws MetaException
   *           Could not drop table properly.
   * @throws NoSuchObjectException
   *           The table wasn't found.
   * @throws TException
   *           A thrift communication error occurred
   */
  void dropTable(String dbname, String tableName, boolean deleteData,
      boolean ignoreUnknownTab, boolean ifPurge) throws MetaException, TException,
      NoSuchObjectException;

  /**
   * Drop the table.
   *
   * @param dbname
   *          The database for this table
   * @param tableName
   *          The table to drop
   * @throws MetaException
   *           Could not drop table properly.
   * @throws NoSuchObjectException
   *           The table wasn't found.
   * @throws TException
   *           A thrift communication error occurred
   */
  void dropTable(String dbname, String tableName)
      throws MetaException, TException, NoSuchObjectException;

  /**
   * Drop a table.
   * @param catName catalog the table is in.
   * @param dbName database the table is in.
   * @param tableName table name.
   * @param deleteData whether associated data should be deleted.
   * @param ignoreUnknownTable whether a non-existent table name should be ignored
   * @param ifPurge whether dropped data should be immediately removed rather than placed in HDFS
   *               trash.
   * @throws MetaException something went wrong, usually in the RDBMS or storage.
   * @throws NoSuchObjectException No table of this name exists, only thrown if
   * ignoreUnknownTable is false.
   * @throws TException general thrift error.
   */
  void dropTable(String catName, String dbName, String tableName, boolean deleteData,
                 boolean ignoreUnknownTable, boolean ifPurge)
    throws MetaException, NoSuchObjectException, TException;

  /**
   * Drop a table.  Equivalent to
   * {@link #dropTable(String, String, String, boolean, boolean, boolean)} with ifPurge set to
   * false.
   * @param catName catalog the table is in.
   * @param dbName database the table is in.
   * @param tableName table name.
   * @param deleteData whether associated data should be deleted.
   * @param ignoreUnknownTable whether a non-existent table name should be ignored
   * @throws MetaException something went wrong, usually in the RDBMS or storage.
   * @throws NoSuchObjectException No table of this name exists, only thrown if
   * ignoreUnknownTable is false.
   * @throws TException general thrift error.
   */
  default void dropTable(String catName, String dbName, String tableName, boolean deleteData,
                         boolean ignoreUnknownTable)
    throws MetaException, NoSuchObjectException, TException {
    dropTable(catName, dbName, tableName, deleteData, ignoreUnknownTable, false);
  }

  /**
   * Drop a table.  Equivalent to
   * {@link #dropTable(String, String, String, boolean, boolean, boolean)} with deleteData
   * set and ignoreUnknownTable set to true and ifPurge set to false.
   * @param catName catalog the table is in.
   * @param dbName database the table is in.
   * @param tableName table name.
   * @throws MetaException something went wrong, usually in the RDBMS or storage.
   * @throws NoSuchObjectException No table of this name exists, only thrown if
   * ignoreUnknownTable is false.
   * @throws TException general thrift error.
   */
  default void dropTable(String catName, String dbName, String tableName)
      throws MetaException, NoSuchObjectException, TException {
    dropTable(catName, dbName, tableName, true, true, false);
  }

  /**
   * Truncate the table/partitions in the DEFAULT database.
   * @param dbName
   *          The db to which the table to be truncate belongs to
   * @param tableName
   *          The table to truncate
   * @param partNames
   *          List of partitions to truncate. NULL will truncate the whole table/all partitions
   * @throws MetaException Failure in the RDBMS or storage
   * @throws TException Thrift transport exception
   */
  void truncateTable(String dbName, String tableName, List<String> partNames) throws MetaException, TException;

  /**
   * Truncate the table/partitions in the DEFAULT database.
   * @param catName catalog name
   * @param dbName
   *          The db to which the table to be truncate belongs to
   * @param tableName
   *          The table to truncate
   * @param partNames
   *          List of partitions to truncate. NULL will truncate the whole table/all partitions
   * @throws MetaException Failure in the RDBMS or storage
   * @throws TException Thrift transport exception
   */
  void truncateTable(String catName, String dbName, String tableName, List<String> partNames)
      throws MetaException, TException;

  /**
   * Recycles the files recursively from the input path to the cmroot directory either by copying or moving it.
   *
   * @param request Inputs for path of the data files to be recycled to cmroot and
   *                isPurge flag when set to true files which needs to be recycled are not moved to Trash
   * @return Response which is currently void
   */
  CmRecycleResponse recycleDirToCmPath(CmRecycleRequest request) throws MetaException, TException;

  /**
   * Check whether a table exists in the default catalog.
   * @param databaseName database name
   * @param tableName table name
   * @return true if the indicated table exists, false if not
   * @throws MetaException error fetching form the RDBMS
   * @throws TException thrift transport error
   * @throws UnknownDBException the indicated database does not exist.
   */
  boolean tableExists(String databaseName, String tableName)
      throws MetaException, TException, UnknownDBException;

  /**
   * Check whether a table exists.
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @return true if the indicated table exists, false if not
   * @throws MetaException error fetching form the RDBMS
   * @throws TException thrift transport error
   * @throws UnknownDBException the indicated database does not exist.
   */
  boolean tableExists(String catName, String dbName, String tableName)
      throws MetaException, TException, UnknownDBException;

  /**
   * Get a Database Object in the default catalog
   * @param databaseName  name of the database to fetch
   * @return the database
   * @throws NoSuchObjectException The database does not exist
   * @throws MetaException Could not fetch the database
   * @throws TException A thrift communication error occurred
   */
  Database getDatabase(String databaseName)
      throws NoSuchObjectException, MetaException, TException;

  /**
   * Get a database.
   * @param catalogName catalog name.  Can be null, in which case
   * {@link Warehouse#DEFAULT_CATALOG_NAME} will be assumed.
   * @param databaseName database name
   * @return the database object
   * @throws NoSuchObjectException No database with this name exists in the specified catalog
   * @throws MetaException something went wrong, usually in the RDBMS
   * @throws TException general thrift error
   */
  Database getDatabase(String catalogName, String databaseName)
      throws NoSuchObjectException, MetaException, TException;

  /**
   * Get a table object in the default catalog.
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
   * Get a table object.
   * @param catName catalog the table is in.
   * @param dbName database the table is in.
   * @param tableName table name.
   * @return table object.
   * @throws MetaException Something went wrong, usually in the RDBMS.
   * @throws TException general thrift error.
   */
  Table getTable(String catName, String dbName, String tableName) throws MetaException, TException;

  /**
   * Get tables as objects (rather than just fetching their names).  This is more expensive and
   * should only be used if you actually need all the information about the tables.
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
   * Get tables as objects (rather than just fetching their names).  This is more expensive and
   * should only be used if you actually need all the information about the tables.
   * @param catName catalog name
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
  List<Table> getTableObjectsByName(String catName, String dbName, List<String> tableNames)
      throws MetaException, InvalidOperationException, UnknownDBException, TException;

  /**
   * Returns the invalidation information for the materialized views given as input.
   */
  Materialization getMaterializationInvalidationInfo(CreationMetadata cm, String validTxnList)
      throws MetaException, InvalidOperationException, UnknownDBException, TException;

  /**
   * Updates the creation metadata for the materialized view.
   */
  void updateCreationMetadata(String dbName, String tableName, CreationMetadata cm)
      throws MetaException, TException;

  /**
   * Updates the creation metadata for the materialized view.
   */
  void updateCreationMetadata(String catName, String dbName, String tableName, CreationMetadata cm)
      throws MetaException, TException;

  /**
  /**
   * Add a partition to a table and get back the resulting Partition object.  This creates an
   * empty default partition with just the partition values set.
   * @param dbName database name
   * @param tableName table name
   * @param partVals partition values
   * @return the partition object
   * @throws InvalidObjectException no such table
   * @throws AlreadyExistsException a partition with these values already exists
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  Partition appendPartition(String dbName, String tableName, List<String> partVals)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException;

  /**
   * Add a partition to a table and get back the resulting Partition object.  This creates an
   * empty default partition with just the partition values set.
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param partVals partition values
   * @return the partition object
   * @throws InvalidObjectException no such table
   * @throws AlreadyExistsException a partition with these values already exists
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  Partition appendPartition(String catName, String dbName, String tableName, List<String> partVals)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException;

  /**
   * Add a partition to a table and get back the resulting Partition object.  This creates an
   * empty default partition with just the partition value set.
   * @param dbName database name.
   * @param tableName table name.
   * @param name name of the partition, should be in the form partkey=partval.
   * @return new partition object.
   * @throws InvalidObjectException No such table.
   * @throws AlreadyExistsException Partition of this name already exists.
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  Partition appendPartition(String dbName, String tableName, String name)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException;

  /**
   * Add a partition to a table and get back the resulting Partition object.  This creates an
   * empty default partition with just the partition value set.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tableName table name.
   * @param name name of the partition, should be in the form partkey=partval.
   * @return new partition object.
   * @throws InvalidObjectException No such table.
   * @throws AlreadyExistsException Partition of this name already exists.
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  Partition appendPartition(String catName, String dbName, String tableName, String name)
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

  /**
   * Add a partitions using a spec proxy.
   * @param partitionSpec partition spec proxy
   * @return number of partitions that were added
   * @throws InvalidObjectException the partitionSpec is malformed.
   * @throws AlreadyExistsException one or more of the partitions already exist.
   * @throws MetaException error accessing the RDBMS or storage.
   * @throws TException thrift transport error
   */
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
   * Get a partition.
   * @param dbName database name
   * @param tblName table name
   * @param partVals partition values for this partition, must be in the same order as the
   *                 partition keys of the table.
   * @return the partition object
   * @throws NoSuchObjectException no such partition
   * @throws MetaException error access the RDBMS.
   * @throws TException thrift transport error
   */
  Partition getPartition(String dbName, String tblName, List<String> partVals)
      throws NoSuchObjectException, MetaException, TException;

  /**
   * Get a partition.
   * @param catName catalog name
   * @param dbName database name
   * @param tblName table name
   * @param partVals partition values for this partition, must be in the same order as the
   *                 partition keys of the table.
   * @return the partition object
   * @throws NoSuchObjectException no such partition
   * @throws MetaException error access the RDBMS.
   * @throws TException thrift transport error
   */
  Partition getPartition(String catName, String dbName, String tblName, List<String> partVals)
      throws NoSuchObjectException, MetaException, TException;

  /**
   * Move a partition from one table to another
   * @param partitionSpecs key value pairs that describe the partition to be moved.
   * @param sourceDb database of the source table
   * @param sourceTable name of the source table
   * @param destdb database of the destination table
   * @param destTableName name of the destination table
   * @return partition object
   * @throws MetaException error accessing the RDBMS or storage
   * @throws NoSuchObjectException no such table, for either source or destination table
   * @throws InvalidObjectException error in partition specifications
   * @throws TException thrift transport error
   */
  Partition exchange_partition(Map<String, String> partitionSpecs,
      String sourceDb, String sourceTable, String destdb,
      String destTableName) throws MetaException, NoSuchObjectException,
      InvalidObjectException, TException;

  /**
   * Move a partition from one table to another
   * @param partitionSpecs key value pairs that describe the partition to be moved.
   * @param sourceCat catalog of the source table
   * @param sourceDb database of the source table
   * @param sourceTable name of the source table
   * @param destCat catalog of the destination table, for now must the same as sourceCat
   * @param destdb database of the destination table
   * @param destTableName name of the destination table
   * @return partition object
   * @throws MetaException error accessing the RDBMS or storage
   * @throws NoSuchObjectException no such table, for either source or destination table
   * @throws InvalidObjectException error in partition specifications
   * @throws TException thrift transport error
   */
  Partition exchange_partition(Map<String, String> partitionSpecs, String sourceCat,
                               String sourceDb, String sourceTable, String destCat, String destdb,
                               String destTableName) throws MetaException, NoSuchObjectException,
      InvalidObjectException, TException;

  /**
   * With the one partitionSpecs to exchange, multiple partitions could be exchanged.
   * e.g., year=2015/month/day, exchanging partition year=2015 results to all the partitions
   * belonging to it exchanged. This function returns the list of affected partitions.
   * @param partitionSpecs key value pairs that describe the partition(s) to be moved.
   * @param sourceDb database of the source table
   * @param sourceTable name of the source table
   * @param destdb database of the destination table
   * @param destTableName name of the destination table
   * @throws MetaException error accessing the RDBMS or storage
   * @throws NoSuchObjectException no such table, for either source or destination table
   * @throws InvalidObjectException error in partition specifications
   * @throws TException thrift transport error
   * @return the list of the new partitions
   */
  List<Partition> exchange_partitions(Map<String, String> partitionSpecs,
      String sourceDb, String sourceTable, String destdb,
      String destTableName) throws MetaException, NoSuchObjectException,
      InvalidObjectException, TException;

  /**
   * With the one partitionSpecs to exchange, multiple partitions could be exchanged.
   * e.g., year=2015/month/day, exchanging partition year=2015 results to all the partitions
   * belonging to it exchanged. This function returns the list of affected partitions.
   * @param partitionSpecs key value pairs that describe the partition(s) to be moved.
   * @param sourceCat catalog of the source table
   * @param sourceDb database of the source table
   * @param sourceTable name of the source table
   * @param destCat catalog of the destination table, for now must the same as sourceCat
   * @param destdb database of the destination table
   * @param destTableName name of the destination table
   * @throws MetaException error accessing the RDBMS or storage
   * @throws NoSuchObjectException no such table, for either source or destination table
   * @throws InvalidObjectException error in partition specifications
   * @throws TException thrift transport error
   * @return the list of the new partitions
   */
  List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String sourceCat,
                                      String sourceDb, String sourceTable, String destCat,
                                      String destdb, String destTableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, TException;

  /**
   * Get a Partition by name.
   * @param dbName database name.
   * @param tblName table name.
   * @param name - partition name i.e. 'ds=2010-02-03/ts=2010-02-03 18%3A16%3A01'
   * @return the partition object
   * @throws MetaException error access the RDBMS.
   * @throws TException thrift transport error
   */
  Partition getPartition(String dbName, String tblName, String name)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException;

  /**
   * Get a Partition by name.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param name - partition name i.e. 'ds=2010-02-03/ts=2010-02-03 18%3A16%3A01'
   * @return the partition object
   * @throws MetaException error access the RDBMS.
   * @throws TException thrift transport error
   */
  Partition getPartition(String catName, String dbName, String tblName, String name)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException;


  /**
   * Get a Partition along with authorization information.
   * @param dbName database name
   * @param tableName table name
   * @param pvals partition values, must be in the same order as the tables partition keys
   * @param userName name of the calling user
   * @param groupNames groups the call
   * @return the partition
   * @throws MetaException error accessing the RDBMS
   * @throws UnknownTableException no such table
   * @throws NoSuchObjectException no such partition
   * @throws TException thrift transport error
   */
  Partition getPartitionWithAuthInfo(String dbName, String tableName,
      List<String> pvals, String userName, List<String> groupNames)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException;

  /**
   * Get a Partition along with authorization information.
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param pvals partition values, must be in the same order as the tables partition keys
   * @param userName name of the calling user
   * @param groupNames groups the call
   * @return the partition
   * @throws MetaException error accessing the RDBMS
   * @throws UnknownTableException no such table
   * @throws NoSuchObjectException no such partition
   * @throws TException thrift transport error
   */
  Partition getPartitionWithAuthInfo(String catName, String dbName, String tableName,
                                     List<String> pvals, String userName, List<String> groupNames)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException;

  /**
   * Get a list of partittions for a table.
   * @param db_name database name
   * @param tbl_name table name
   * @param max_parts maximum number of parts to return, -1 for all
   * @return the list of partitions
   * @throws NoSuchObjectException No such table.
   * @throws MetaException error accessing RDBMS.
   * @throws TException thrift transport error
   */
  List<Partition> listPartitions(String db_name, String tbl_name, short max_parts)
      throws NoSuchObjectException, MetaException, TException;

  /**
   * Get a list of partittions for a table.
   * @param catName catalog name
   * @param db_name database name
   * @param tbl_name table name
   * @param max_parts maximum number of parts to return, -1 for all
   * @return the list of partitions
   * @throws NoSuchObjectException No such table.
   * @throws MetaException error accessing RDBMS.
   * @throws TException thrift transport error
   */
  List<Partition> listPartitions(String catName, String db_name, String tbl_name, int max_parts)
      throws NoSuchObjectException, MetaException, TException;

  /**
   * Get a list of partitions from a table, returned in the form of PartitionSpecProxy
   * @param dbName database name.
   * @param tableName table name.
   * @param maxParts maximum number of partitions to return, or -1 for all
   * @return a PartitionSpecProxy
   * @throws TException thrift transport error
   */
  PartitionSpecProxy listPartitionSpecs(String dbName, String tableName, int maxParts)
    throws TException;

  /**
   * Get a list of partitions from a table, returned in the form of PartitionSpecProxy
   * @param catName catalog name.
   * @param dbName database name.
   * @param tableName table name.
   * @param maxParts maximum number of partitions to return, or -1 for all
   * @return a PartitionSpecProxy
   * @throws TException thrift transport error
   */
  PartitionSpecProxy listPartitionSpecs(String catName, String dbName, String tableName,
                                        int maxParts) throws TException;

  /**
   * Get a list of partitions based on a (possibly partial) list of partition values.
   * @param db_name database name.
   * @param tbl_name table name.
   * @param part_vals partition values, in order of the table partition keys.  These can be
   *                  partial, or .* to match all values for a particular key.
   * @param max_parts maximum number of partitions to return, or -1 for all.
   * @return list of partitions
   * @throws NoSuchObjectException no such table.
   * @throws MetaException error accessing the database or processing the partition values.
   * @throws TException thrift transport error.
   */
  List<Partition> listPartitions(String db_name, String tbl_name,
      List<String> part_vals, short max_parts) throws NoSuchObjectException, MetaException, TException;

  /**
   * Get a list of partitions based on a (possibly partial) list of partition values.
   * @param catName catalog name.
   * @param db_name database name.
   * @param tbl_name table name.
   * @param part_vals partition values, in order of the table partition keys.  These can be
   *                  partial, or .* to match all values for a particular key.
   * @param max_parts maximum number of partitions to return, or -1 for all.
   * @return list of partitions
   * @throws NoSuchObjectException no such table.
   * @throws MetaException error accessing the database or processing the partition values.
   * @throws TException thrift transport error.
   */
  List<Partition> listPartitions(String catName, String db_name, String tbl_name,
                                 List<String> part_vals, int max_parts)
      throws NoSuchObjectException, MetaException, TException;

  /**
   * List Names of partitions in a table.
   * @param db_name database name.
   * @param tbl_name table name.
   * @param max_parts maximum number of parts of fetch, or -1 to fetch them all.
   * @return list of partition names.
   * @throws NoSuchObjectException No such table.
   * @throws MetaException Error accessing the RDBMS.
   * @throws TException thrift transport error
   */
  List<String> listPartitionNames(String db_name, String tbl_name,
      short max_parts) throws NoSuchObjectException, MetaException, TException;

  /**
   * List Names of partitions in a table.
   * @param catName catalog name.
   * @param db_name database name.
   * @param tbl_name table name.
   * @param max_parts maximum number of parts of fetch, or -1 to fetch them all.
   * @return list of partition names.
   * @throws NoSuchObjectException No such table.
   * @throws MetaException Error accessing the RDBMS.
   * @throws TException thrift transport error
   */
  List<String> listPartitionNames(String catName, String db_name, String tbl_name,
                                  int max_parts) throws NoSuchObjectException, MetaException, TException;

  /**
   * Get a list of partition names matching a partial specification of the partition values.
   * @param db_name database name.
   * @param tbl_name table name.
   * @param part_vals partial list of partition values.  These must be given in the order of the
   *                  partition keys.  If you wish to accept any value for a particular key you
   *                  can pass ".*" for that value in this list.
   * @param max_parts maximum number of partition names to return, or -1 to return all that are
   *                  found.
   * @return list of matching partition names.
   * @throws MetaException error accessing the RDBMS.
   * @throws TException thrift transport error.
   * @throws NoSuchObjectException no such table.
   */
  List<String> listPartitionNames(String db_name, String tbl_name,
      List<String> part_vals, short max_parts)
      throws MetaException, TException, NoSuchObjectException;

  /**
   * Get a list of partition names matching a partial specification of the partition values.
   * @param catName catalog name.
   * @param db_name database name.
   * @param tbl_name table name.
   * @param part_vals partial list of partition values.  These must be given in the order of the
   *                  partition keys.  If you wish to accept any value for a particular key you
   *                  can pass ".*" for that value in this list.
   * @param max_parts maximum number of partition names to return, or -1 to return all that are
   *                  found.
   * @return list of matching partition names.
   * @throws MetaException error accessing the RDBMS.
   * @throws TException thrift transport error.
   * @throws NoSuchObjectException no such table.
   */
  List<String> listPartitionNames(String catName, String db_name, String tbl_name,
                                  List<String> part_vals, int max_parts)
      throws MetaException, TException, NoSuchObjectException;

  /**
   * Get a list of partition values
   * @param request request
   * @return reponse
   * @throws MetaException error accessing RDBMS
   * @throws TException thrift transport error
   * @throws NoSuchObjectException no such table
   */
  PartitionValuesResponse listPartitionValues(PartitionValuesRequest request)
      throws MetaException, TException, NoSuchObjectException;

  /**
   * Get number of partitions matching specified filter
   * @param dbName the database name
   * @param tableName the table name
   * @param filter the filter string,
   *    for example "part1 = \"p1_abc\" and part2 &lt;= "\p2_test\"". Filtering can
   *    be done only on string partition keys.
   * @return number of partitions
   * @throws MetaException error accessing RDBMS or processing the filter
   * @throws NoSuchObjectException no such table
   * @throws TException thrift transport error
   */
  int getNumPartitionsByFilter(String dbName, String tableName,
                               String filter) throws MetaException, NoSuchObjectException, TException;

  /**
   * Get number of partitions matching specified filter
   * @param catName catalog name
   * @param dbName the database name
   * @param tableName the table name
   * @param filter the filter string,
   *    for example "part1 = \"p1_abc\" and part2 &lt;= "\p2_test\"". Filtering can
   *    be done only on string partition keys.
   * @return number of partitions
   * @throws MetaException error accessing RDBMS or processing the filter
   * @throws NoSuchObjectException no such table
   * @throws TException thrift transport error
   */
  int getNumPartitionsByFilter(String catName, String dbName, String tableName,
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
   * @throws MetaException Error accessing the RDBMS or processing the filter.
   * @throws NoSuchObjectException No such table.
   * @throws TException thrift transport error
   */
  List<Partition> listPartitionsByFilter(String db_name, String tbl_name,
      String filter, short max_parts) throws MetaException, NoSuchObjectException, TException;

  /**
   * Get list of partitions matching specified filter
   * @param catName catalog name.
   * @param db_name the database name
   * @param tbl_name the table name
   * @param filter the filter string,
   *    for example "part1 = \"p1_abc\" and part2 &lt;= "\p2_test\"". Filtering can
   *    be done only on string partition keys.
   * @param max_parts the maximum number of partitions to return,
   *    all partitions are returned if -1 is passed
   * @return list of partitions
   * @throws MetaException Error accessing the RDBMS or processing the filter.
   * @throws NoSuchObjectException No such table.
   * @throws TException thrift transport error
   */
  List<Partition> listPartitionsByFilter(String catName, String db_name, String tbl_name,
                                         String filter, int max_parts)
      throws MetaException, NoSuchObjectException, TException;

  /**
   * Get a list of partitions in a PartitionSpec, using a filter to select which partitions to
   * fetch.
   * @param db_name database name
   * @param tbl_name table name
   * @param filter SQL where clause filter
   * @param max_parts maximum number of partitions to fetch, or -1 for all
   * @return PartitionSpec
   * @throws MetaException error accessing RDBMS or processing the filter
   * @throws NoSuchObjectException No table matches the request
   * @throws TException thrift transport error
   */
  PartitionSpecProxy listPartitionSpecsByFilter(String db_name, String tbl_name,
                                                String filter, int max_parts)
      throws MetaException, NoSuchObjectException, TException;

  /**
   * Get a list of partitions in a PartitionSpec, using a filter to select which partitions to
   * fetch.
   * @param catName catalog name
   * @param db_name database name
   * @param tbl_name table name
   * @param filter SQL where clause filter
   * @param max_parts maximum number of partitions to fetch, or -1 for all
   * @return PartitionSpec
   * @throws MetaException error accessing RDBMS or processing the filter
   * @throws NoSuchObjectException No table matches the request
   * @throws TException thrift transport error
   */
  PartitionSpecProxy listPartitionSpecsByFilter(String catName, String db_name, String tbl_name,
                                                String filter, int max_parts)
      throws MetaException, NoSuchObjectException, TException;

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
   * @throws TException thrift transport error or error executing the filter.
   */
  boolean listPartitionsByExpr(String db_name, String tbl_name,
      byte[] expr, String default_partition_name, short max_parts, List<Partition> result)
          throws TException;

  /**
   * Get list of partitions matching specified serialized expression
   * @param catName catalog name
   * @param db_name the database name
   * @param tbl_name the table name
   * @param expr expression, serialized from ExprNodeDesc
   * @param max_parts the maximum number of partitions to return,
   *    all partitions are returned if -1 is passed
   * @param default_partition_name Default partition name from configuration. If blank, the
   *    metastore server-side configuration is used.
   * @param result the resulting list of partitions
   * @return whether the resulting list contains partitions which may or may not match the expr
   * @throws TException thrift transport error or error executing the filter.
   */
  boolean listPartitionsByExpr(String catName, String db_name, String tbl_name, byte[] expr,
                               String default_partition_name, int max_parts, List<Partition> result)
      throws TException;

  /**
   * List partitions, fetching the authorization information along with the partitions.
   * @param dbName database name
   * @param tableName table name
   * @param maxParts maximum number of partitions to fetch, or -1 for all
   * @param userName user to fetch privileges for
   * @param groupNames groups to fetch privileges for
   * @return the list of partitions
   * @throws NoSuchObjectException no partitions matching the criteria were found
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  List<Partition> listPartitionsWithAuthInfo(String dbName,
      String tableName, short maxParts, String userName, List<String> groupNames)
      throws MetaException, TException, NoSuchObjectException;

  /**
   * List partitions, fetching the authorization information along with the partitions.
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param maxParts maximum number of partitions to fetch, or -1 for all
   * @param userName user to fetch privileges for
   * @param groupNames groups to fetch privileges for
   * @return the list of partitions
   * @throws NoSuchObjectException no partitions matching the criteria were found
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
                                             int maxParts, String userName, List<String> groupNames)
      throws MetaException, TException, NoSuchObjectException;

  /**
   * Get partitions by a list of partition names.
   * @param db_name database name
   * @param tbl_name table name
   * @param part_names list of partition names
   * @return list of Partition objects
   * @throws NoSuchObjectException No such partitions
   * @throws MetaException error accessing the RDBMS.
   * @throws TException thrift transport error
   */
  List<Partition> getPartitionsByNames(String db_name, String tbl_name,
      List<String> part_names) throws NoSuchObjectException, MetaException, TException;

  /**
   * Get partitions by a list of partition names.
   * @param catName catalog name
   * @param db_name database name
   * @param tbl_name table name
   * @param part_names list of partition names
   * @return list of Partition objects
   * @throws NoSuchObjectException No such partitions
   * @throws MetaException error accessing the RDBMS.
   * @throws TException thrift transport error
   */
  List<Partition> getPartitionsByNames(String catName, String db_name, String tbl_name,
                                       List<String> part_names)
      throws NoSuchObjectException, MetaException, TException;

  /**
   * List partitions along with privilege information for a user or groups
   * @param dbName database name
   * @param tableName table name
   * @param partialPvals partition values, can be partial
   * @param maxParts maximum number of partitions to fetch, or -1 for all
   * @param userName user to fetch privilege information for
   * @param groupNames group to fetch privilege information for
   * @return the list of partitions
   * @throws NoSuchObjectException no partitions matching the criteria were found
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  List<Partition> listPartitionsWithAuthInfo(String dbName,
      String tableName, List<String> partialPvals, short maxParts, String userName,
      List<String> groupNames) throws MetaException, TException, NoSuchObjectException;

  /**
   * List partitions along with privilege information for a user or groups
   * @param dbName database name
   * @param tableName table name
   * @param partialPvals partition values, can be partial
   * @param maxParts maximum number of partitions to fetch, or -1 for all
   * @param userName user to fetch privilege information for
   * @param groupNames group to fetch privilege information for
   * @return the list of partitions
   * @throws NoSuchObjectException no partitions matching the criteria were found
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
                                             List<String> partialPvals, int maxParts, String userName,
                                             List<String> groupNames)
      throws MetaException, TException, NoSuchObjectException;

  /**
   * Mark an event as having occurred on a partition.
   * @param db_name database name
   * @param tbl_name table name
   * @param partKVs key value pairs that describe the partition
   * @param eventType type of the event
   * @throws MetaException error access the RDBMS
   * @throws NoSuchObjectException never throws this AFAICT
   * @throws TException thrift transport error
   * @throws UnknownTableException no such table
   * @throws UnknownDBException no such database
   * @throws UnknownPartitionException no such partition
   * @throws InvalidPartitionException partition partKVs is invalid
   */
  void markPartitionForEvent(String db_name, String tbl_name, Map<String,String> partKVs,
      PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException,
      UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException;

  /**
   * Mark an event as having occurred on a partition.
   * @param catName catalog name
   * @param db_name database name
   * @param tbl_name table name
   * @param partKVs key value pairs that describe the partition
   * @param eventType type of the event
   * @throws MetaException error access the RDBMS
   * @throws NoSuchObjectException never throws this AFAICT
   * @throws TException thrift transport error
   * @throws UnknownTableException no such table
   * @throws UnknownDBException no such database
   * @throws UnknownPartitionException no such partition
   * @throws InvalidPartitionException partition partKVs is invalid
   */
  void markPartitionForEvent(String catName, String db_name, String tbl_name, Map<String,String> partKVs,
                             PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException,
      UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException;

  /**
   * Determine whether a partition has been marked with a particular event type.
   * @param db_name database name
   * @param tbl_name table name.
   * @param partKVs key value pairs that describe the partition.
   * @param eventType event type
   * @throws MetaException error access the RDBMS
   * @throws NoSuchObjectException never throws this AFAICT
   * @throws TException thrift transport error
   * @throws UnknownTableException no such table
   * @throws UnknownDBException no such database
   * @throws UnknownPartitionException no such partition
   * @throws InvalidPartitionException partition partKVs is invalid
   */
  boolean isPartitionMarkedForEvent(String db_name, String tbl_name, Map<String,String> partKVs,
      PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException,
      UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException;

  /**
   * Determine whether a partition has been marked with a particular event type.
   * @param catName catalog name
   * @param db_name database name
   * @param tbl_name table name.
   * @param partKVs key value pairs that describe the partition.
   * @param eventType event type
   * @throws MetaException error access the RDBMS
   * @throws NoSuchObjectException never throws this AFAICT
   * @throws TException thrift transport error
   * @throws UnknownTableException no such table
   * @throws UnknownDBException no such database
   * @throws UnknownPartitionException no such partition
   * @throws InvalidPartitionException partition partKVs is invalid
   */
  boolean isPartitionMarkedForEvent(String catName, String db_name, String tbl_name, Map<String,String> partKVs,
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

  /**
   * Alter a table
   * @param databaseName database name
   * @param tblName table name
   * @param table new table object, should be complete representation of the table, not just the
   *             things you want to change.
   * @throws InvalidOperationException something is wrong with the new table object or an
   * operation was attempted that is not allowed (such as changing partition columns).
   * @throws MetaException something went wrong, usually in the RDBMS
   * @throws TException general thrift exception
   */
  void alter_table(String databaseName, String tblName, Table table)
      throws InvalidOperationException, MetaException, TException;

  /**
   * Alter a table. Equivalent to
   * {@link #alter_table(String, String, String, Table, EnvironmentContext)} with
   * EnvironmentContext set to null.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param newTable new table object, should be complete representation of the table, not just the
   *                 things you want to change.
   * @throws InvalidOperationException something is wrong with the new table object or an
   * operation was attempted that is not allowed (such as changing partition columns).
   * @throws MetaException something went wrong, usually in the RDBMS
   * @throws TException general thrift exception
   */
  default void alter_table(String catName, String dbName, String tblName, Table newTable)
      throws InvalidOperationException, MetaException, TException {
    alter_table(catName, dbName, tblName, newTable, null);
  }

  /**
   * Alter a table.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param newTable new table object, should be complete representation of the table, not just the
   *                 things you want to change.
   * @param envContext options for the alter.
   * @throws InvalidOperationException something is wrong with the new table object or an
   * operation was attempted that is not allowed (such as changing partition columns).
   * @throws MetaException something went wrong, usually in the RDBMS
   * @throws TException general thrift exception
   */
  void alter_table(String catName, String dbName, String tblName, Table newTable,
                  EnvironmentContext envContext)
      throws InvalidOperationException, MetaException, TException;

  /**
   * @deprecated Use alter_table_with_environmentContext instead of alter_table with cascade option
   * passed in EnvironmentContext using {@code StatsSetupConst.CASCADE}
   */
  @Deprecated
  void alter_table(String defaultDatabaseName, String tblName, Table table,
      boolean cascade) throws InvalidOperationException, MetaException, TException;

  /**
   * Alter a table.
   * @param databaseName database name
   * @param tblName table name
   * @param table new table object, should be complete representation of the table, not just the
   *              things you want to change.
   * @param environmentContext options for the alter.
   * @throws InvalidOperationException something is wrong with the new table object or an
   * operation was attempted that is not allowed (such as changing partition columns).
   * @throws MetaException something went wrong, usually in the RDBMS
   * @throws TException general thrift exception
   */
  void alter_table_with_environmentContext(String databaseName, String tblName, Table table,
      EnvironmentContext environmentContext) throws InvalidOperationException, MetaException,
      TException;

  /**
   * Create a new database.
   * @param db database object.  If the catalog name is null it will be assumed to be
   *           {@link Warehouse#DEFAULT_CATALOG_NAME}.
   * @throws InvalidObjectException There is something wrong with the database object.
   * @throws AlreadyExistsException There is already a database of this name in the specified
   * catalog.
   * @throws MetaException something went wrong, usually in the RDBMS
   * @throws TException general thrift error
   */
  void createDatabase(Database db)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException;

  /**
   * Drop a database.
   * @param name name of the database to drop.
   * @throws NoSuchObjectException No such database exists.
   * @throws InvalidOperationException The database cannot be dropped because it is not empty.
   * @throws MetaException something went wrong, usually either in the RDMBS or in storage.
   * @throws TException general thrift error.
   */
  void dropDatabase(String name)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException;

  /**
   *
   * Drop a database.
   * @param name name of the database to drop.
   * @param deleteData whether to drop the underlying HDFS directory.
   * @param ignoreUnknownDb whether to ignore an attempt to drop a non-existant database
   * @throws NoSuchObjectException No database of this name exists in the specified catalog and
   * ignoreUnknownDb is false.
   * @throws InvalidOperationException The database cannot be dropped because it is not empty.
   * @throws MetaException something went wrong, usually either in the RDMBS or in storage.
   * @throws TException general thrift error.
   */
  void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException;

  /**
   *
   * Drop a database.
   * @param name database name.
   * @param deleteData whether to drop the underlying HDFS directory.
   * @param ignoreUnknownDb whether to ignore an attempt to drop a non-existant database
   * @param cascade whether to drop contained tables, etc.  If this is false and there are
   *                objects still in the database the drop will fail.
   * @throws NoSuchObjectException No database of this name exists in the specified catalog and
   * ignoreUnknownDb is false.
   * @throws InvalidOperationException The database contains objects and cascade is false.
   * @throws MetaException something went wrong, usually either in the RDBMS or storage.
   * @throws TException general thrift error.
   */
  void dropDatabase(String name, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException;

  /**
   * Drop a database.
   * @param catName Catalog name.  This can be null, in which case
   *                {@link Warehouse#DEFAULT_CATALOG_NAME} will be assumed.
   * @param dbName database name.
   * @param deleteData whether to drop the underlying HDFS directory.
   * @param ignoreUnknownDb whether to ignore an attempt to drop a non-existant database
   * @param cascade whether to drop contained tables, etc.  If this is false and there are
   *                objects still in the database the drop will fail.
   * @throws NoSuchObjectException No database of this name exists in the specified catalog and
   * ignoreUnknownDb is false.
   * @throws InvalidOperationException The database contains objects and cascade is false.
   * @throws MetaException something went wrong, usually either in the RDBMS or storage.
   * @throws TException general thrift error.
   */
  void dropDatabase(String catName, String dbName, boolean deleteData, boolean ignoreUnknownDb,
                    boolean cascade)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException;

  /**
   * Drop a database.  Equivalent to
   * {@link #dropDatabase(String, String, boolean, boolean, boolean)} with cascade = false.
   * @param catName Catalog name.  This can be null, in which case
   *                {@link Warehouse#DEFAULT_CATALOG_NAME} will be assumed.
   * @param dbName database name.
   * @param deleteData whether to drop the underlying HDFS directory.
   * @param ignoreUnknownDb whether to ignore an attempt to drop a non-existant database
   * @throws NoSuchObjectException No database of this name exists in the specified catalog and
   * ignoreUnknownDb is false.
   * @throws InvalidOperationException The database contains objects and cascade is false.
   * @throws MetaException something went wrong, usually either in the RDBMS or storage.
   * @throws TException general thrift error.
   */
  default void dropDatabase(String catName, String dbName, boolean deleteData,
                            boolean ignoreUnknownDb)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    dropDatabase(catName, dbName, deleteData, ignoreUnknownDb, false);
  }

  /**
   * Drop a database.  Equivalent to
   * {@link #dropDatabase(String, String, boolean, boolean, boolean)} with deleteData =
   * true, ignoreUnknownDb = false, cascade = false.
   * @param catName Catalog name.  This can be null, in which case
   *                {@link Warehouse#DEFAULT_CATALOG_NAME} will be assumed.
   * @param dbName database name.
   * @throws NoSuchObjectException No database of this name exists in the specified catalog and
   * ignoreUnknownDb is false.
   * @throws InvalidOperationException The database contains objects and cascade is false.
   * @throws MetaException something went wrong, usually either in the RDBMS or storage.
   * @throws TException general thrift error.
   */
  default void dropDatabase(String catName, String dbName)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    dropDatabase(catName, dbName, true, false, false);
  }


  /**
   * Alter a database.
   * @param name database name.
   * @param db new database object.
   * @throws NoSuchObjectException No database of this name exists in the specified catalog.
   * @throws MetaException something went wrong, usually in the RDBMS.
   * @throws TException general thrift error.
   */
  void alterDatabase(String name, Database db)
      throws NoSuchObjectException, MetaException, TException;

  /**
   * Alter a database.
   * @param catName Catalog name.  This can be null, in which case
   *                {@link Warehouse#DEFAULT_CATALOG_NAME} will be assumed.
   * @param dbName database name.
   * @param newDb new database object.
   * @throws NoSuchObjectException No database of this name exists in the specified catalog.
   * @throws MetaException something went wrong, usually in the RDBMS.
   * @throws TException general thrift error.
   */
  void alterDatabase(String catName, String dbName, Database newDb)
      throws NoSuchObjectException, MetaException, TException;

  /**
   * Drop a partition.
   * @param db_name database name
   * @param tbl_name table name
   * @param part_vals partition values, in the same order as the partition keys
   * @param deleteData
   *          delete the underlying data or just delete the partition in metadata
   * @return true or false
   * @throws NoSuchObjectException partition does not exist
   * @throws MetaException error accessing the RDBMS or the storage.
   * @throws TException thrift transport error
   */
  boolean dropPartition(String db_name, String tbl_name,
      List<String> part_vals, boolean deleteData) throws NoSuchObjectException,
      MetaException, TException;

  /**
   * Drop a partition.
   * @param catName catalog name.
   * @param db_name database name
   * @param tbl_name table name
   * @param part_vals partition values, in the same order as the partition keys
   * @param deleteData
   *          delete the underlying data or just delete the partition in metadata
   * @return true or false
   * @throws NoSuchObjectException partition does not exist
   * @throws MetaException error accessing the RDBMS or the storage.
   * @throws TException thrift transport error
   */
  boolean dropPartition(String catName, String db_name, String tbl_name,
                        List<String> part_vals, boolean deleteData) throws NoSuchObjectException,
      MetaException, TException;

  /**
   * Drop a partition with the option to purge the partition data directly,
   * rather than to move data to trash.
   * @param db_name Name of the database.
   * @param tbl_name Name of the table.
   * @param part_vals Specification of the partitions being dropped.
   * @param options PartitionDropOptions for the operation.
   * @return True (if partitions are dropped), else false.
   * @throws NoSuchObjectException partition does not exist
   * @throws MetaException error accessing the RDBMS or the storage.
   * @throws TException thrift transport error.
   */
  boolean dropPartition(String db_name, String tbl_name, List<String> part_vals,
                        PartitionDropOptions options)
      throws NoSuchObjectException, MetaException, TException;

  /**
   * Drop a partition with the option to purge the partition data directly,
   * rather than to move data to trash.
   * @param catName catalog name.
   * @param db_name Name of the database.
   * @param tbl_name Name of the table.
   * @param part_vals Specification of the partitions being dropped.
   * @param options PartitionDropOptions for the operation.
   * @return True (if partitions are dropped), else false.
   * @throws NoSuchObjectException partition does not exist
   * @throws MetaException error accessing the RDBMS or the storage.
   * @throws TException thrift transport error.
   */
  boolean dropPartition(String catName, String db_name, String tbl_name, List<String> part_vals,
                        PartitionDropOptions options)
      throws NoSuchObjectException, MetaException, TException;

  /**
   * Drop partitions based on an expression.
   * @param dbName database name.
   * @param tblName table name.
   * @param partExprs I don't understand this fully, so can't completely explain it.  The second
   *                  half of the object pair is an expression used to determine which partitions
   *                  to drop.  The first half has something to do with archive level, but I
   *                  don't understand what.  I'm also not sure what happens if you pass multiple
   *                  expressions.
   * @param deleteData whether to delete the data as well as the metadata.
   * @param ifExists if true, it is not an error if no partitions match the expression(s).
   * @return list of deleted partitions.
   * @throws NoSuchObjectException No partition matches the expression(s), and ifExists was false.
   * @throws MetaException error access the RDBMS or storage.
   * @throws TException Thrift transport error.
   */
  List<Partition> dropPartitions(String dbName, String tblName,
                                 List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData,
                                 boolean ifExists) throws NoSuchObjectException, MetaException, TException;

  /**
   * Drop partitions based on an expression.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param partExprs I don't understand this fully, so can't completely explain it.  The second
   *                  half of the object pair is an expression used to determine which partitions
   *                  to drop.  The first half has something to do with archive level, but I
   *                  don't understand what.  I'm also not sure what happens if you pass multiple
   *                  expressions.
   * @param deleteData whether to delete the data as well as the metadata.
   * @param ifExists if true, it is not an error if no partitions match the expression(s).
   * @return list of deleted partitions.
   * @throws NoSuchObjectException No partition matches the expression(s), and ifExists was false.
   * @throws MetaException error access the RDBMS or storage.
   * @throws TException Thrift transport error.
   */
  default List<Partition> dropPartitions(String catName, String dbName, String tblName,
                                         List<ObjectPair<Integer, byte[]>> partExprs,
                                         boolean deleteData, boolean ifExists)
      throws NoSuchObjectException, MetaException, TException {
    return dropPartitions(catName, dbName, tblName, partExprs,
        PartitionDropOptions.instance()
            .deleteData(deleteData)
            .ifExists(ifExists));
  }

  /**
   * Drop partitions based on an expression.
   * @param dbName database name.
   * @param tblName table name.
   * @param partExprs I don't understand this fully, so can't completely explain it.  The second
   *                  half of the object pair is an expression used to determine which partitions
   *                  to drop.  The first half has something to do with archive level, but I
   *                  don't understand what.  I'm also not sure what happens if you pass multiple
   *                  expressions.
   * @param deleteData whether to delete the data as well as the metadata.
   * @param ifExists if true, it is not an error if no partitions match the expression(s).
   * @param needResults if true, the list of deleted partitions will be returned, if not, null
   *                    will be returned.
   * @return list of deleted partitions.
   * @throws NoSuchObjectException No partition matches the expression(s), and ifExists was false.
   * @throws MetaException error access the RDBMS or storage.
   * @throws TException Thrift transport error.
   * @deprecated Use {@link #dropPartitions(String, String, String, List, boolean, boolean, boolean)}
   */
  @Deprecated
  List<Partition> dropPartitions(String dbName, String tblName,
      List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData,
      boolean ifExists, boolean needResults) throws NoSuchObjectException, MetaException, TException;

  /**
   * Drop partitions based on an expression.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tblName table name.
   * @param partExprs I don't understand this fully, so can't completely explain it.  The second
   *                  half of the object pair is an expression used to determine which partitions
   *                  to drop.  The first half has something to do with archive level, but I
   *                  don't understand what.  I'm also not sure what happens if you pass multiple
   *                  expressions.
   * @param deleteData whether to delete the data as well as the metadata.
   * @param ifExists if true, it is not an error if no partitions match the expression(s).
   * @param needResults if true, the list of deleted partitions will be returned, if not, null
   *                    will be returned.
   * @return list of deleted partitions, if needResults is true
   * @throws NoSuchObjectException No partition matches the expression(s), and ifExists was false.
   * @throws MetaException error access the RDBMS or storage.
   * @throws TException Thrift transport error.
   */
  default List<Partition> dropPartitions(String catName, String dbName, String tblName,
                                         List<ObjectPair<Integer, byte[]>> partExprs, boolean deleteData,
                                         boolean ifExists, boolean needResults)
      throws NoSuchObjectException, MetaException, TException {
    return dropPartitions(catName, dbName, tblName, partExprs,
        PartitionDropOptions.instance()
            .deleteData(deleteData)
            .ifExists(ifExists)
            .returnResults(needResults));
  }

  /**
   * Generalization of dropPartitions(),
   * @param dbName Name of the database
   * @param tblName Name of the table
   * @param partExprs Partition-specification
   * @param options Boolean options for dropping partitions
   * @return List of Partitions dropped
   * @throws NoSuchObjectException No partition matches the expression(s), and ifExists was false.
   * @throws MetaException error access the RDBMS or storage.
   * @throws TException On failure
   */
  List<Partition> dropPartitions(String dbName, String tblName,
                                 List<ObjectPair<Integer, byte[]>> partExprs,
                                 PartitionDropOptions options)
      throws NoSuchObjectException, MetaException, TException;

  /**
   * Generalization of dropPartitions(),
   * @param catName catalog name
   * @param dbName Name of the database
   * @param tblName Name of the table
   * @param partExprs Partition-specification
   * @param options Boolean options for dropping partitions
   * @return List of Partitions dropped
   * @throws NoSuchObjectException No partition matches the expression(s), and ifExists was false.
   * @throws MetaException error access the RDBMS or storage.
   * @throws TException On failure
   */
  List<Partition> dropPartitions(String catName, String dbName, String tblName,
                                 List<ObjectPair<Integer, byte[]>> partExprs,
                                 PartitionDropOptions options)
      throws NoSuchObjectException, MetaException, TException;

  /**
   * Drop a partition.
   * @param db_name database name.
   * @param tbl_name table name.
   * @param name partition name.
   * @param deleteData whether to delete the data or just the metadata.
   * @return true if the partition was dropped.
   * @throws NoSuchObjectException no such partition.
   * @throws MetaException error accessing the RDBMS or storage
   * @throws TException thrift transport error
   */
  boolean dropPartition(String db_name, String tbl_name,
      String name, boolean deleteData) throws NoSuchObjectException,
      MetaException, TException;

  /**
   * Drop a partition.
   * @param catName catalog name.
   * @param db_name database name.
   * @param tbl_name table name.
   * @param name partition name.
   * @param deleteData whether to delete the data or just the metadata.
   * @return true if the partition was dropped.
   * @throws NoSuchObjectException no such partition.
   * @throws MetaException error accessing the RDBMS or storage
   * @throws TException thrift transport error
   */
  boolean dropPartition(String catName, String db_name, String tbl_name,
                        String name, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException;

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
   * @param catName catalog name
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
  default void alter_partition(String catName, String dbName, String tblName, Partition newPart)
      throws InvalidOperationException, MetaException, TException {
    alter_partition(catName, dbName, tblName, newPart, null);
  }

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
   * updates a partition to new partition
   * @param catName catalog name.
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
  void alter_partition(String catName, String dbName, String tblName, Partition newPart,
                       EnvironmentContext environmentContext)
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
   * @param environmentContext key value pairs to pass to alter function.
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
   * updates a list of partitions
   * @param catName catalog name.
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
  default void alter_partitions(String catName, String dbName, String tblName,
                                List<Partition> newParts)
      throws InvalidOperationException, MetaException, TException {
    alter_partitions(catName, dbName, tblName, newParts, null);
  }

  /**
   * updates a list of partitions
   * @param catName catalog name.
   * @param dbName
   *          database of the old partition
   * @param tblName
   *          table name of the old partition
   * @param newParts
   *          list of partitions
   * @param environmentContext key value pairs to pass to alter function.
   * @throws InvalidOperationException
   *           if the old partition does not exist
   * @throws MetaException
   *           if error in updating metadata
   * @throws TException
   *           if error in communicating with metastore server
   */
  void alter_partitions(String catName, String dbName, String tblName, List<Partition> newParts,
                        EnvironmentContext environmentContext)
      throws InvalidOperationException, MetaException, TException;

  /**
   * rename a partition to a new partition
   *
   * @param dbname
   *          database of the old partition
   * @param tableName
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
  void renamePartition(final String dbname, final String tableName, final List<String> part_vals,
                       final Partition newPart)
      throws InvalidOperationException, MetaException, TException;

  /**
   * rename a partition to a new partition
   * @param catName catalog name.
   * @param dbname
   *          database of the old partition
   * @param tableName
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
  void renamePartition(String catName, String dbname, String tableName, List<String> part_vals,
                       Partition newPart)
      throws InvalidOperationException, MetaException, TException;

  /**
   * Get schema for a table, excluding the partition columns.
   * @param db database name
   * @param tableName table name
   * @return  list of field schemas describing the table's schema
   * @throws UnknownTableException no such table
   * @throws UnknownDBException no such database
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  List<FieldSchema> getFields(String db, String tableName)
      throws MetaException, TException, UnknownTableException,
      UnknownDBException;

  /**
   * Get schema for a table, excluding the partition columns.
   * @param catName catalog name
   * @param db database name
   * @param tableName table name
   * @return  list of field schemas describing the table's schema
   * @throws UnknownTableException no such table
   * @throws UnknownDBException no such database
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  List<FieldSchema> getFields(String catName, String db, String tableName)
      throws MetaException, TException, UnknownTableException,
      UnknownDBException;

  /**
   * Get schema for a table, including the partition columns.
   * @param db database name
   * @param tableName table name
   * @return  list of field schemas describing the table's schema
   * @throws UnknownTableException no such table
   * @throws UnknownDBException no such database
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  List<FieldSchema> getSchema(String db, String tableName)
      throws MetaException, TException, UnknownTableException,
      UnknownDBException;

  /**
   * Get schema for a table, including the partition columns.
   * @param catName catalog name
   * @param db database name
   * @param tableName table name
   * @return  list of field schemas describing the table's schema
   * @throws UnknownTableException no such table
   * @throws UnknownDBException no such database
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  List<FieldSchema> getSchema(String catName, String db, String tableName)
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
   * Get the column statistics for a set of columns in a table.  This should only be used for
   * non-partitioned tables.  For partitioned tables use
   * {@link #getPartitionColumnStatistics(String, String, List, List)}.
   * @param dbName database name
   * @param tableName table name
   * @param colNames list of column names
   * @return list of column statistics objects, one per column
   * @throws NoSuchObjectException no such table
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames) throws NoSuchObjectException, MetaException, TException;

  /**
   * Get the column statistics for a set of columns in a table.  This should only be used for
   * non-partitioned tables.  For partitioned tables use
   * {@link #getPartitionColumnStatistics(String, String, String, List, List)}.
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param colNames list of column names
   * @return list of column statistics objects, one per column
   * @throws NoSuchObjectException no such table
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  List<ColumnStatisticsObj> getTableColumnStatistics(String catName, String dbName, String tableName,
                                                     List<String> colNames)
      throws NoSuchObjectException, MetaException, TException;

  /**
   * Get the column statistics for a set of columns in a partition.
   * @param dbName database name
   * @param tableName table name
   * @param partNames partition names.  Since these are names they should be of the form
   *                  "key1=value1[/key2=value2...]"
   * @param colNames list of column names
   * @return map of columns to statistics
   * @throws NoSuchObjectException no such partition
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName,
      String tableName,  List<String> partNames, List<String> colNames)
          throws NoSuchObjectException, MetaException, TException;

  /**
   * Get the column statistics for a set of columns in a partition.
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param partNames partition names.  Since these are names they should be of the form
   *                  "key1=value1[/key2=value2...]"
   * @param colNames list of column names
   * @return map of columns to statistics
   * @throws NoSuchObjectException no such partition
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
      String catName, String dbName, String tableName,  List<String> partNames, List<String> colNames)
      throws NoSuchObjectException, MetaException, TException;

  /**
   * Delete partition level column statistics given dbName, tableName, partName and colName, or
   * all columns in a partition.
   * @param dbName database name.
   * @param tableName table name.
   * @param partName partition name.
   * @param colName column name, or null for all columns
   * @return boolean indicating outcome of the operation
   * @throws NoSuchObjectException no such partition exists
   * @throws InvalidObjectException error dropping the stats data
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   * @throws InvalidInputException input is invalid or null.
   */
  boolean deletePartitionColumnStatistics(String dbName, String tableName,
    String partName, String colName) throws NoSuchObjectException, MetaException,
    InvalidObjectException, TException, InvalidInputException;

  /**
   * Delete partition level column statistics given dbName, tableName, partName and colName, or
   * all columns in a partition.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tableName table name.
   * @param partName partition name.
   * @param colName column name, or null for all columns
   * @return boolean indicating outcome of the operation
   * @throws NoSuchObjectException no such partition exists
   * @throws InvalidObjectException error dropping the stats data
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   * @throws InvalidInputException input is invalid or null.
   */
  boolean deletePartitionColumnStatistics(String catName, String dbName, String tableName,
                                          String partName, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException;

  /**
   * Delete table level column statistics given dbName, tableName and colName, or all columns in
   * a table.  This should be used for non-partitioned tables.
   * @param dbName database name
   * @param tableName table name
   * @param colName column name, or null to drop stats for all columns
   * @return boolean indicating the outcome of the operation
   * @throws NoSuchObjectException No such table
   * @throws MetaException error accessing the RDBMS
   * @throws InvalidObjectException error dropping the stats
   * @throws TException thrift transport error
   * @throws InvalidInputException bad input, like a null table name.
   */
   boolean deleteTableColumnStatistics(String dbName, String tableName, String colName) throws
    NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException;

  /**
   * Delete table level column statistics given dbName, tableName and colName, or all columns in
   * a table.  This should be used for non-partitioned tables.
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param colName column name, or null to drop stats for all columns
   * @return boolean indicating the outcome of the operation
   * @throws NoSuchObjectException No such table
   * @throws MetaException error accessing the RDBMS
   * @throws InvalidObjectException error dropping the stats
   * @throws TException thrift transport error
   * @throws InvalidInputException bad input, like a null table name.
   */
  boolean deleteTableColumnStatistics(String catName, String dbName, String tableName, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException;

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
   * @param revokePrivileges
   * @param authorizer
   * @param objToRefresh
   * @return true on success
   * @throws MetaException
   * @throws TException
   */
  boolean refresh_privileges(HiveObjectRef objToRefresh, String authorizer, PrivilegeBag grantPrivileges)
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

  /**
   * Create a new function.
   * @param func function specification
   * @throws InvalidObjectException the function object is invalid
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  void createFunction(Function func)
      throws InvalidObjectException, MetaException, TException;

  /**
   * Alter a function.
   * @param dbName database name.
   * @param funcName function name.
   * @param newFunction new function specification.  This should be complete, not just the changes.
   * @throws InvalidObjectException the function object is invalid
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  void alterFunction(String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException, TException;

  /**
   * Alter a function.
   * @param catName catalog name.
   * @param dbName database name.
   * @param funcName function name.
   * @param newFunction new function specification.  This should be complete, not just the changes.
   * @throws InvalidObjectException the function object is invalid
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  void alterFunction(String catName, String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException, TException;

  /**
   * Drop a function.
   * @param dbName database name.
   * @param funcName function name.
   * @throws MetaException error accessing the RDBMS
   * @throws NoSuchObjectException no such function
   * @throws InvalidObjectException not sure when this is thrown
   * @throws InvalidInputException not sure when this is thrown
   * @throws TException thrift transport error
   */
  void dropFunction(String dbName, String funcName) throws MetaException,
      NoSuchObjectException, InvalidObjectException, InvalidInputException, TException;

  /**
   * Drop a function.
   * @param catName catalog name.
   * @param dbName database name.
   * @param funcName function name.
   * @throws MetaException error accessing the RDBMS
   * @throws NoSuchObjectException no such function
   * @throws InvalidObjectException not sure when this is thrown
   * @throws InvalidInputException not sure when this is thrown
   * @throws TException thrift transport error
   */
  void dropFunction(String catName, String dbName, String funcName) throws MetaException,
      NoSuchObjectException, InvalidObjectException, InvalidInputException, TException;

  /**
   * Get a function.
   * @param dbName database name.
   * @param funcName function name.
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  Function getFunction(String dbName, String funcName)
      throws MetaException, TException;

  /**
   * Get a function.
   * @param catName catalog name.
   * @param dbName database name.
   * @param funcName function name.
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  Function getFunction(String catName, String dbName, String funcName)
      throws MetaException, TException;

  /**
   * Get all functions matching a pattern
   * @param dbName database name.
   * @param pattern to match.  This is a java regex pattern.
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  List<String> getFunctions(String dbName, String pattern)
      throws MetaException, TException;

  /**
   * Get all functions matching a pattern
   * @param catName catalog name.
   * @param dbName database name.
   * @param pattern to match.  This is a java regex pattern.
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  List<String> getFunctions(String catName, String dbName, String pattern)
      throws MetaException, TException;

  /**
   * Get all functions in the default catalog.
   * @return list of functions
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  GetAllFunctionsResponse getAllFunctions() throws MetaException, TException;

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
   * @param tablesList list of tables (format: <db_name>.<table_name>) read from the current transaction
   *                   for which needs to populate the valid write ids
   * @param validTxnList snapshot of valid txns for the current txn
   * @return list of valid write ids for the given list of tables.
   * @throws TException
   */
  List<TableValidWriteIds> getValidWriteIds(List<String> tablesList, String validTxnList)
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
   * Initiate a transaction at the target cluster.
   * @param replPolicy The replication policy to uniquely identify the source cluster.
   * @param srcTxnIds The list of transaction ids at the source cluster
   * @param user The user who has fired the repl load command.
   * @return transaction identifiers
   * @throws TException
   */
  List<Long> replOpenTxn(String replPolicy, List<Long> srcTxnIds, String user) throws TException;

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
   * Rollback a transaction.  This will also unlock any locks associated with
   * this transaction.
   * @param srcTxnid id of transaction at source while is rolled back and to be replicated.
   * @param replPolicy the replication policy to identify the source cluster
   * @throws NoSuchTxnException if the requested transaction does not exist.
   * Note that this can result from the transaction having timed out and been
   * deleted.
   * @throws TException
   */
  void replRollbackTxn(long srcTxnid, String replPolicy) throws NoSuchTxnException, TException;

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
   * Commit a transaction.  This will also unlock any locks associated with
   * this transaction.
   * @param rqst Information containing the txn info and write event information
   * of transaction at source which is committed and to be replicated
   * @throws NoSuchTxnException if the requested transaction does not exist.
   * This can result fro the transaction having timed out and been deleted by
   * the compactor.
   * @throws TxnAbortedException if the requested transaction has been
   * aborted.  This can result from the transaction timing out.
   * @throws TException
   */
  void replCommitTxn(CommitTxnRequest rqst)
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
   * Replicate Table Write Ids state to mark aborted write ids and writeid high water mark.
   * @param validWriteIdList Snapshot of writeid list when the table/partition is dumped.
   * @param dbName Database name
   * @param tableName Table which is written.
   * @param partNames List of partitions being written.
   * @throws TException in case of failure to replicate the writeid state
   */
  void replTableWriteIdState(String validWriteIdList, String dbName, String tableName, List<String> partNames)
          throws TException;

  /**
   * Allocate a per table write ID and associate it with the given transaction.
   * @param txnIds ids of transaction batchto which the allocated write ID to be associated.
   * @param dbName name of DB in which the table belongs.
   * @param tableName table to which the write ID to be allocated
   * @throws TException
   */
  List<TxnToWriteId> allocateTableWriteIdsBatch(List<Long> txnIds, String dbName, String tableName) throws TException;

  /**
   * Allocate a per table write ID and associate it with the given transaction. Used by replication load task.
   * @param dbName name of DB in which the table belongs.
   * @param tableName table to which the write ID to be allocated
   * @param replPolicy Used by replication task to identify the source cluster.
   * @param srcTxnToWriteIdList List of txn to write id map sent from the source cluster.
   * @throws TException
   */
  List<TxnToWriteId> replAllocateTableWriteIdsBatch(String dbName, String tableName, String replPolicy,
                                                    List<TxnToWriteId> srcTxnToWriteIdList) throws TException;
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

  /**
   * Add a event related to write operations in an ACID table.
   * @param rqst message containing information for acid write operation.
   * @throws TException
   */
  @InterfaceAudience.LimitedPrivate({"Apache Hive, HCatalog"})
  void addWriteNotificationLog(WriteNotificationLogRequest rqst) throws TException;

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

  /**
   * Get aggregated column stats for a set of partitions.
   * @param dbName database name
   * @param tblName table name
   * @param colNames list of column names
   * @param partName list of partition names (not values).
   * @return aggregated stats for requested partitions
   * @throws NoSuchObjectException no such table
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport exception
   */
  AggrStats getAggrColStatsFor(String dbName, String tblName,
      List<String> colNames, List<String> partName)  throws NoSuchObjectException, MetaException, TException;

  /**
   * Get aggregated column stats for a set of partitions.
   * @param catName catalog name
   * @param dbName database name
   * @param tblName table name
   * @param colNames list of column names
   * @param partNames list of partition names (not values).
   * @return aggregated stats for requested partitions
   * @throws NoSuchObjectException no such table
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport exception
   */
  AggrStats getAggrColStatsFor(String catName, String dbName, String tblName,
                               List<String> colNames, List<String> partNames)
      throws NoSuchObjectException, MetaException, TException;

  /**
   * Set table or partition column statistics.
   * @param request request object, contains all the table, partition, and statistics information
   * @return true if the set was successful.
   * @throws NoSuchObjectException the table, partition, or columns specified do not exist.
   * @throws InvalidObjectException the stats object is not valid.
   * @throws MetaException error accessing the RDBMS.
   * @throws TException thrift transport error.
   * @throws InvalidInputException the input is invalid (eg, a null table name)
   */
  boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException;

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

  /**
   * Get a primary key for a table.
   * @param request Request info
   * @return List of primary key columns
   * @throws MetaException error reading the RDBMS
   * @throws NoSuchObjectException no primary key exists on this table, or maybe no such table
   * @throws TException thrift transport error
   */
  List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest request)
    throws MetaException, NoSuchObjectException, TException;

  /**
   * Get a foreign key for a table.
   * @param request Request info
   * @return List of foreign key columns
   * @throws MetaException error reading the RDBMS
   * @throws NoSuchObjectException no foreign key exists on this table, or maybe no such table
   * @throws TException thrift transport error
   */
  List<SQLForeignKey> getForeignKeys(ForeignKeysRequest request) throws MetaException,
    NoSuchObjectException, TException;

  /**
   * Get a unique constraint for a table.
   * @param request Request info
   * @return List of unique constraint columns
   * @throws MetaException error reading the RDBMS
   * @throws NoSuchObjectException no unique constraint on this table, or maybe no such table
   * @throws TException thrift transport error
   */
  List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest request) throws MetaException,
    NoSuchObjectException, TException;

  /**
   * Get a not null constraint for a table.
   * @param request Request info
   * @return List of not null constraint columns
   * @throws MetaException error reading the RDBMS
   * @throws NoSuchObjectException no not null constraint on this table, or maybe no such table
   * @throws TException thrift transport error
   */
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

  /**
   * Drop a constraint.  This can be used for primary keys, foreign keys, unique constraints, or
   * not null constraints.
   * @param dbName database name
   * @param tableName table name
   * @param constraintName name of the constraint
   * @throws MetaException RDBMS access error
   * @throws NoSuchObjectException no such constraint exists
   * @throws TException thrift transport error
   */
  void dropConstraint(String dbName, String tableName, String constraintName)
      throws MetaException, NoSuchObjectException, TException;

  /**
   * Drop a constraint.  This can be used for primary keys, foreign keys, unique constraints, or
   * not null constraints.
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param constraintName name of the constraint
   * @throws MetaException RDBMS access error
   * @throws NoSuchObjectException no such constraint exists
   * @throws TException thrift transport error
   */
  void dropConstraint(String catName, String dbName, String tableName, String constraintName)
      throws MetaException, NoSuchObjectException, TException;


  /**
   * Add a primary key.
   * @param primaryKeyCols Primary key columns.
   * @throws MetaException error reading or writing to the RDBMS or a primary key already exists
   * @throws NoSuchObjectException no such table exists
   * @throws TException thrift transport error
   */
  void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols) throws
  MetaException, NoSuchObjectException, TException;

  /**
   * Add a foreign key
   * @param foreignKeyCols Foreign key definition
   * @throws MetaException error reading or writing to the RDBMS or foreign key already exists
   * @throws NoSuchObjectException one of the tables in the foreign key does not exist.
   * @throws TException thrift transport error
   */
  void addForeignKey(List<SQLForeignKey> foreignKeyCols) throws
  MetaException, NoSuchObjectException, TException;

  /**
   * Add a unique constraint
   * @param uniqueConstraintCols Unique constraint definition
   * @throws MetaException error reading or writing to the RDBMS or unique constraint already exists
   * @throws NoSuchObjectException no such table
   * @throws TException thrift transport error
   */
  void addUniqueConstraint(List<SQLUniqueConstraint> uniqueConstraintCols) throws
  MetaException, NoSuchObjectException, TException;

  /**
   * Add a not null constraint
   * @param notNullConstraintCols Notnull constraint definition
   * @throws MetaException error reading or writing to the RDBMS or not null constraint already
   * exists
   * @throws NoSuchObjectException no such table
   * @throws TException thrift transport error
   */
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
      throws NoSuchObjectException, InvalidObjectException, TException;

  void dropWMPool(String resourcePlanName, String poolPath)
      throws TException;

  void createOrUpdateWMMapping(WMMapping mapping, boolean isUpdate)
      throws TException;

  void dropWMMapping(WMMapping mapping)
      throws TException;

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
   * @param catName catalog name
   * @param dbName database the schema is in
   * @param schemaName name of the schema
   * @param newSchema altered schema object
   * @throws NoSuchObjectException no schema with this name could be found
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  void alterISchema(String catName, String dbName, String schemaName, ISchema newSchema) throws TException;

  /**
   * Fetch a schema.
   * @param catName catalog name
   * @param dbName database the schema is in
   * @param name name of the schema
   * @return the schema or null if no such schema
   * @throws NoSuchObjectException no schema matching this name exists
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  ISchema getISchema(String catName, String dbName, String name) throws TException;

  /**
   * Drop an existing schema.  If there are schema versions of this, this call will fail.
   * @param catName catalog name
   * @param dbName database the schema is in
   * @param name name of the schema to drop
   * @throws NoSuchObjectException no schema with this name could be found
   * @throws InvalidOperationException attempt to drop a schema that has versions
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  void dropISchema(String catName, String dbName, String name) throws TException;

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
  SchemaVersion getSchemaVersion(String catName, String dbName, String schemaName, int version) throws TException;

  /**
   * Get the latest version of a schema.
   * @param catName catalog name
   * @param dbName database the schema is in
   * @param schemaName name of the schema
   * @return latest version of the schema or null if the schema does not exist or there are no
   * version of the schema.
   * @throws NoSuchObjectException no versions of schema matching this name exist
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  SchemaVersion getSchemaLatestVersion(String catName, String dbName, String schemaName) throws TException;

  /**
   * Get all the extant versions of a schema.
   * @param catName catalog name
   * @param dbName database the schema is in
   * @param schemaName name of the schema.
   * @return list of all the schema versions or null if this schema does not exist or has no
   * versions.
   * @throws NoSuchObjectException no versions of schema matching this name exist
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  List<SchemaVersion> getSchemaAllVersions(String catName, String dbName, String schemaName) throws TException;

  /**
   * Drop a version of a schema.  Given that versions are supposed to be immutable you should
   * think really hard before you call this method.  It should only be used for schema versions
   * that were added in error and never referenced any data.
   * @param catName catalog name
   * @param dbName database the schema is in
   * @param schemaName name of the schema
   * @param version version of the schema
   * @throws NoSuchObjectException no matching version of the schema could be found
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  void dropSchemaVersion(String catName, String dbName, String schemaName, int version) throws TException;

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
   * @param catName catalog name
   * @param dbName database the schema is in
   * @param schemaName name of the schema
   * @param version version of the schema
   * @param serdeName name of the serde
   * @throws NoSuchObjectException no matching version of the schema could be found or no serde
   * of the provided name could be found
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  void mapSchemaVersionToSerde(String catName, String dbName, String schemaName, int version, String serdeName) throws TException;

  /**
   * Set the state of a schema version.
   * @param catName catalog name
   * @param dbName database the schema is in
   * @param schemaName name of the schema
   * @param version version of the schema
   * @param state state to set the schema too
   * @throws NoSuchObjectException no matching version of the schema could be found
   * @throws InvalidOperationException attempt to make a state change that is not valid
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  void setSchemaVersionState(String catName, String dbName, String schemaName, int version, SchemaVersionState state) throws TException;

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

  /**
   * Acquire the materialization rebuild lock for a given view. We need to specify the fully
   * qualified name of the materialized view and the open transaction ID so we can identify
   * uniquely the lock.
   * @param dbName db name for the materialized view
   * @param tableName table name for the materialized view
   * @param txnId transaction id for the rebuild
   * @return the response from the metastore, where the lock id is equal to the txn id and
   * the status can be either ACQUIRED or NOT ACQUIRED
   */
  LockResponse lockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException;

  /**
   * Method to refresh the acquisition of a given materialization rebuild lock.
   * @param dbName db name for the materialized view
   * @param tableName table name for the materialized view
   * @param txnId transaction id for the rebuild
   * @return true if the lock could be renewed, false otherwise
   */
  boolean heartbeatLockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException;

  /** Adds a RuntimeStat for metastore persistence. */
  void addRuntimeStat(RuntimeStat stat) throws TException;

  /** Reads runtime statistics. */
  List<RuntimeStat> getRuntimeStats(int maxWeight, int maxCreateTime) throws TException;

}
