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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.common.classification.RetrySemantics;
import org.apache.hadoop.hive.metastore.annotation.NoReconnect;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.metastore.api.Package;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.thrift.TException;

/**
 * Wrapper around hive metastore thrift api
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface IMetaStoreClient extends AutoCloseable {

  /**
   * Returns whether current client is compatible with conf argument or not
   * @return
   */
  default boolean isCompatibleWith(Configuration configuration) {
    return false;
  }

  /**
   * Set added jars path info to MetaStoreClient.
   * @param addedJars the hive.added.jars.path. It is qualified paths separated by commas.
   */
  default void setHiveAddedJars(String addedJars) {
    throw new UnsupportedOperationException("MetaStore client does not support setting added jars");
  }

  /**
   * Returns true if the current client is using an in process metastore (local metastore).
   *
   * @return
   */
  default boolean isLocalMetaStore(){
    throw new UnsupportedOperationException("MetaStore client does not support checking if metastore is local");
  }

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
  default void setMetaConf(String key, String value) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support setting meta variables");
  }

  /**
   * get current meta variable
   */
  default String getMetaConf(String key) throws TException{
    return "";
  }

  /**
   * Create a new catalog.
   * @param catalog catalog object to create.
   * @throws AlreadyExistsException A catalog of this name already exists.
   * @throws InvalidObjectException There is something wrong with the passed in catalog object.
   * @throws MetaException something went wrong, usually either in the database or trying to
   * create the directory for the catalog.
   * @throws TException general thrift exception.
   */
  default void createCatalog(Catalog catalog)
      throws AlreadyExistsException, InvalidObjectException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support creating catalogs");
  }

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
  default void alterCatalog(String catalogName, Catalog newCatalog)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering catalogs");
  }

  /**
   * Get a catalog object.
   * @param catName Name of the catalog to fetch.
   * @return The catalog.
   * @throws NoSuchObjectException no catalog of this name exists.
   * @throws MetaException something went wrong, usually in the database.
   * @throws TException general thrift exception.
   */
  default Catalog getCatalog(String catName) throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support fetching catalogs");
  }

  /**
   * Get a list of all catalogs known to the system.
   * @return list of catalog names
   * @throws MetaException something went wrong, usually in the database.
   * @throws TException general thrift exception.
   */
  default List<String> getCatalogs() throws MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support fetching catalogs");
  }

  /**
   * Drop a catalog.  Catalogs must be empty to be dropped, there is no cascade for dropping a
   * catalog.
   * @param catName name of the catalog to drop
   * @throws NoSuchObjectException no catalog of this name exists.
   * @throws InvalidOperationException The catalog is not empty and cannot be dropped.
   * @throws MetaException something went wrong, usually in the database.
   * @throws TException general thrift exception.
   */
  default void dropCatalog(String catName)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support dropping catalogs");
  }

  /**
   * Drop a catalog.  Catalogs must be empty to be dropped, there is no cascade for dropping a
   * catalog.
   * @param catName name of the catalog to drop
   * @param ifExists if true, do not throw an error if the catalog does not exist.
   * @throws TException general thrift exception.
   */
  default void dropCatalog(String catName, boolean ifExists) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support dropping catalogs");
  }

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
  default List<String> getTables(String catName, String dbName, String tablePattern, TableType tableType)
      throws MetaException, TException, UnknownDBException  {
    throw new UnsupportedOperationException("MetaStore client does not support fetching tables with table type");
  }

  /**
   * Retrieve all materialized views that have rewriting enabled. This will use the default catalog.
   * @return List of materialized views.
   * @throws MetaException error fetching from the RDBMS
   * @throws TException thrift transport error
   * @throws UnknownDBException no such database
   */
  default List<Table> getAllMaterializedViewObjectsForRewriting()
      throws MetaException, TException, UnknownDBException {
    throw new UnsupportedOperationException("MetaStore client does not support fetching materialized views");
  }

  /**
   * Get the names of all the tables along with extended table metadata
   * @param catName catalog name.
   * @param dbName Name of the database to fetch tables from.
   * @param tablePattern pattern to match the tables names.
   * @param requestedFields An int bitmask to indicate the depth of the returned objects
   * @param limit Maximum size of the result set. &lt;=0 indicates no limit
   * @return List of ExtendedTableInfo that match the input arguments.
   * @throws MetaException Thrown if there is error on fetching from DBMS.
   * @throws TException Thrown if there is a thrift transport exception.
   */
  default List<ExtendedTableInfo> getTablesExt(String catName, String dbName, String tablePattern, int requestedFields,
      int limit) throws MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support fetching extended table info");
  }

  /**
   * Get materialized views that have rewriting enabled.  This will use the default catalog.
   * @param dbName Name of the database to fetch materialized views from.
   * @return List of materialized view names.
   * @throws MetaException error fetching from the RDBMS
   * @throws TException thrift transport error
   * @throws UnknownDBException no such database
   */
  default List<String> getMaterializedViewsForRewriting(String dbName)
      throws MetaException, TException, UnknownDBException {
    throw new UnsupportedOperationException("MetaStore client does not support fetching materialized views");
  }

  /**
   * Get materialized views that have rewriting enabled.
   * @param catName catalog name.
   * @param dbName Name of the database to fetch materialized views from.
   * @return List of materialized view names.
   * @throws MetaException error fetching from the RDBMS
   * @throws TException thrift transport error
   * @throws UnknownDBException no such database
   */
  default List<String> getMaterializedViewsForRewriting(String catName, String dbName)
      throws MetaException, TException, UnknownDBException {
    throw new UnsupportedOperationException("MetaStore client does not support fetching materialized views");
  }

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
  default List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes)
      throws MetaException, TException, UnknownDBException {
    throw new UnsupportedOperationException("MetaStore client does not support fetching table metadata");
  }

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
  default List<TableMeta> getTableMeta(String catName, String dbPatterns, String tablePatterns,
                               List<String> tableTypes)
      throws MetaException, TException, UnknownDBException {
     throw new UnsupportedOperationException("MetaStore client does not support fetching table metadata");
  }

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
  default List<String> listTableNamesByFilter(String dbName, String filter, short maxTables)
      throws TException, InvalidOperationException, UnknownDBException {
     throw new UnsupportedOperationException("MetaStore client does not support listing table names by filter");
  }

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
  default List<String> listTableNamesByFilter(String catName, String dbName, String filter, int maxTables)
      throws TException, InvalidOperationException, UnknownDBException {
     throw new UnsupportedOperationException("MetaStore client does not support listing table names by filter");
  }

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
  void dropTable(String dbname, String tableName, boolean deleteData, boolean ignoreUnknownTab) 
      throws MetaException, TException, NoSuchObjectException;

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
  @Deprecated // TODO: deprecate all methods without a catalog here; a single layer (e.g. Hive.java) should handle current-catalog
  void dropTable(String dbname, String tableName, boolean deleteData,
      boolean ignoreUnknownTab, boolean ifPurge) 
      throws MetaException, TException, NoSuchObjectException;

  void dropTable(Table table, boolean deleteData, boolean ignoreUnknownTab, boolean ifPurge) 
      throws TException;

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
  @Deprecated
  default void truncateTable(String dbName, String tableName, List<String> partNames) throws MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support truncating tables");
  }

  default void truncateTable(TableName table, List<String> partNames) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support truncating tables");
  }

  default void truncateTable(String dbName, String tableName, List<String> partNames,
      String validWriteIds, long writeId) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support truncating tables with write ids");
  }


  default void truncateTable(String dbName, String tableName, List<String> partNames,
      String validWriteIds, long writeId, boolean deleteData) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support truncating tables with write ids");
  }

  default void truncateTable(String catName, String dbName, String tableName, String ref, List<String> partNames,
      String validWriteIds, long writeId, boolean deleteData, EnvironmentContext context) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support truncating tables with write ids");
  }

  /**
   * Recycles the files recursively from the input path to the cmroot directory either by copying or moving it.
   *
   * @param request Inputs for path of the data files to be recycled to cmroot and
   *                isPurge flag when set to true files which needs to be recycled are not moved to Trash
   * @return Response which is currently void
   */
  default CmRecycleResponse recycleDirToCmPath(CmRecycleRequest request) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support recycling directories to cmroot");
  }

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
   * @deprecated use getTable(GetTableRequest getTableRequest)
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
  @Deprecated
  Table getTable(String dbName, String tableName) throws MetaException,
      TException, NoSuchObjectException;

  /**
   * Get a table object in the default catalog.
   * @deprecated use getTable(GetTableRequest getTableRequest)
   * @param dbName
   *          The database the table is located in.
   * @param tableName
   *          Name of the table to fetch.
   * @param getColumnStats
   *          get the column stats, if available, when true
   * @param engine engine sending the request
   * @return An object representing the table.
   * @throws MetaException
   *           Could not fetch the table
   * @throws TException
   *           A thrift communication error occurred
   * @throws NoSuchObjectException
   *           In case the table wasn't found.
   */
  @Deprecated
  Table getTable(String dbName, String tableName, boolean getColumnStats, String engine) throws MetaException,
          TException, NoSuchObjectException;

  /**
   * Get a table object.
   * @deprecated use getTable(GetTableRequest getTableRequest)
   * @param catName catalog the table is in.
   * @param dbName database the table is in.
   * @param tableName table name.
   * @return table object.
   * @throws MetaException Something went wrong, usually in the RDBMS.
   * @throws TException general thrift error.
   */
  @Deprecated
  Table getTable(String catName, String dbName, String tableName) throws MetaException, TException;

  /**
   * Get a table object.
   * @deprecated use getTable(GetTableRequest getTableRequest)
   * @param catName catalog the table is in.
   * @param dbName database the table is in.
   * @param tableName table name.
   * @param validWriteIdList applicable snapshot
   * @param getColumnStats get the column stats, if available, when true
   * @param engine engine sending the request
   * @return table object.
   * @throws MetaException Something went wrong, usually in the RDBMS.
   * @throws TException general thrift error.
   */
  @Deprecated
  Table getTable(String catName, String dbName, String tableName,
                 String validWriteIdList, boolean getColumnStats, String engine) throws TException;

  /**
   *
   * @param getTableRequest request object to query a table in HMS
   * @return An object representing the table.
   * @throws MetaException
   *           Could not fetch the table
   * @throws TException
   *           A thrift communication error occurred
   * @throws NoSuchObjectException
   *           In case the table wasn't found.
   */
  Table getTable(GetTableRequest getTableRequest) throws MetaException, TException, NoSuchObjectException;


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
  default List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support fetching table objects by name");
  }

  /**
   * Get tables as objects (rather than just fetching their names).  This is more expensive and
   * should only be used if you actually need all the information about the tables.
   * @param catName catalog name
   * @param dbName The database the tables are located in.
   * @param tableNames The names of the tables to fetch.
   * @param projectionsSpec The subset of columns that need to be fetched as part of the table object.
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
  default List<Table> getTables(String catName, String dbName, List<String> tableNames, 
    GetProjectionsSpec projectionsSpec) throws MetaException, InvalidOperationException, UnknownDBException, 
      TException {
     throw new UnsupportedOperationException("MetaStore client does not support fetching tables");
  }
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
  default List<Table> getTableObjectsByName(String catName, String dbName, List<String> tableNames)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support fetching table objects by name");
  }

  /**
   * Returns the invalidation information for the materialized views given as input.
   */
  default Materialization getMaterializationInvalidationInfo(CreationMetadata cm, String validTxnList) 
      throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support fetching materialization invalidation " +
        "info");
  }

  /**
   * Updates the creation metadata for the materialized view.
   */
  default void updateCreationMetadata(String dbName, String tableName, CreationMetadata cm)
      throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support updating creation metadata");
  }

  /**
   * Updates the creation metadata for the materialized view.
   */
  default void updateCreationMetadata(String catName, String dbName, String tableName, CreationMetadata cm)
      throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support updating creation metadata");
  }

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
  default Partition appendPartition(String dbName, String tableName, List<String> partVals)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support appending partitions");
  }

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
  default Partition appendPartition(String catName, String dbName, String tableName, List<String> partVals)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support appending partitions");
  }

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
  default Partition appendPartition(String dbName, String tableName, String name)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support appending partitions");
  }

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
  default Partition appendPartition(String catName, String dbName, String tableName, String name)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support appending partitions");
  }

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
  default Partition add_partition(Partition partition)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support adding partitions");
  }

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
  default int add_partitions(List<Partition> partitions)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support adding partitions");
  }

  /**
   * Add a partitions using a spec proxy.
   * @param partitionSpec partition spec proxy
   * @return number of partitions that were added
   * @throws InvalidObjectException the partitionSpec is malformed.
   * @throws AlreadyExistsException one or more of the partitions already exist.
   * @throws MetaException error accessing the RDBMS or storage.
   * @throws TException thrift transport error
   */
  default int add_partitions_pspec(PartitionSpecProxy partitionSpec)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support adding partitions using a spec proxy");
  }

  /**
   * Add partitions to the table.
   *
   * @param partitions The partitions to add
   * @param ifNotExists only add partitions if they don't exist
   * @param needResults Whether the results are needed
   * @return the partitions that were added, or null if !needResults
   */
  default List<Partition> add_partitions(
      List<Partition> partitions, boolean ifNotExists, boolean needResults)
      throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support adding partitions");
  }

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
  default Partition getPartition(String dbName, String tblName, List<String> partVals)
      throws NoSuchObjectException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting partitions");
  }

  /**
   * Get a partition.
   * @param req
   * @return GetPartitionResponse
   * @throws NoSuchObjectException no such partition
   * @throws MetaException error access the RDBMS.
   * @throws TException thrift transport error
   */
  default GetPartitionResponse getPartitionRequest(GetPartitionRequest req)
          throws NoSuchObjectException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting partitions");
  }

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
  default Partition getPartition(String catName, String dbName, String tblName, List<String> partVals)
      throws NoSuchObjectException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting partitions");
  }

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
  default Partition exchange_partition(Map<String, String> partitionSpecs,
      String sourceDb, String sourceTable, String destdb,
      String destTableName) throws MetaException, NoSuchObjectException,
      InvalidObjectException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support exchanging partitions");
  }

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
  default Partition exchange_partition(Map<String, String> partitionSpecs, String sourceCat,
                               String sourceDb, String sourceTable, String destCat, String destdb,
                               String destTableName) throws MetaException, NoSuchObjectException,
      InvalidObjectException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support exchanging partitions");
  }

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
  default List<Partition> exchange_partitions(Map<String, String> partitionSpecs,
      String sourceDb, String sourceTable, String destdb,
      String destTableName) throws MetaException, NoSuchObjectException,
      InvalidObjectException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support exchanging partitions");
  }

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
  default List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String sourceCat,
                                      String sourceDb, String sourceTable, String destCat,
                                      String destdb, String destTableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support exchanging partitions");
  }

  /**
   * Get a Partition by name.
   * @param dbName database name.
   * @param tblName table name.
   * @param name - partition name i.e. 'ds=2010-02-03/ts=2010-02-03 18%3A16%3A01'
   * @return the partition object
   * @throws MetaException error access the RDBMS.
   * @throws TException thrift transport error
   */
  default Partition getPartition(String dbName, String tblName, String name)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting partitions");
  }

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
  default Partition getPartition(String catName, String dbName, String tblName, String name)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting partitions");
  }


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
  default Partition getPartitionWithAuthInfo(String dbName, String tableName,
      List<String> pvals, String userName, List<String> groupNames)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting partitions with auth info");
  }

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
  default Partition getPartitionWithAuthInfo(String catName, String dbName, String tableName,
                                     List<String> pvals, String userName, List<String> groupNames)
      throws MetaException, UnknownTableException, NoSuchObjectException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting partitions with auth info");
  }

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
  default List<Partition> listPartitions(String db_name, String tbl_name, short max_parts)
      throws NoSuchObjectException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partitions");
  }

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
  default List<Partition> listPartitions(String catName, String db_name, String tbl_name, int max_parts)
      throws NoSuchObjectException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partitions");
  }

  /**
   * Get a list of partitions from a table, returned in the form of PartitionSpecProxy
   * @param dbName database name.
   * @param tableName table name.
   * @param maxParts maximum number of partitions to return, or -1 for all
   * @return a PartitionSpecProxy
   * @throws TException thrift transport error
   */
  default PartitionSpecProxy listPartitionSpecs(String dbName, String tableName, int maxParts)
    throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partition specs");
  }

  /**
   * Get a list of partitions from a table, returned in the form of PartitionSpecProxy
   * @param catName catalog name.
   * @param dbName database name.
   * @param tableName table name.
   * @param maxParts maximum number of partitions to return, or -1 for all
   * @return a PartitionSpecProxy
   * @throws TException thrift transport error
   */
  default PartitionSpecProxy listPartitionSpecs(String catName, String dbName, String tableName, int maxParts)
      throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partition specs");
  }

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
  default List<Partition> listPartitions(String db_name, String tbl_name,
      List<String> part_vals, short max_parts) throws NoSuchObjectException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partitions");
  }

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
  default List<Partition> listPartitions(String catName, String db_name, String tbl_name,
                                 List<String> part_vals, int max_parts)
      throws NoSuchObjectException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partitions");
  }

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
  default List<String> listPartitionNames(String db_name, String tbl_name,
      short max_parts) throws NoSuchObjectException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partition names");
  }

  /**
   * List Names of partitions in a table.
   * @param req
   * @return GetPartitionNamesPsResponse
   * @throws NoSuchObjectException No such table.
   * @throws MetaException Error accessing the RDBMS.
   * @throws TException thrift transport error
   */
  default GetPartitionNamesPsResponse listPartitionNamesRequest(GetPartitionNamesPsRequest req)
          throws NoSuchObjectException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partition names");
  }

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
  default List<String> listPartitionNames(String catName, String db_name, String tbl_name, int max_parts)
      throws NoSuchObjectException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partition names");
  }

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
  default List<String> listPartitionNames(String db_name, String tbl_name, List<String> part_vals, short max_parts)
      throws MetaException, TException, NoSuchObjectException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partition names");
  }

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
  default List<String> listPartitionNames(String catName, String db_name, String tbl_name, List<String> part_vals,
      int max_parts) throws MetaException, TException, NoSuchObjectException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partition names");
  }

  /**
   * Get a list of partition names matching the specified filter and return in order if specified.
   * @param request request
   * @return list of matching partition names.
   * @throws MetaException error accessing the RDBMS.
   * @throws TException thrift transport error.
   * @throws NoSuchObjectException  no such table.
   */
  default List<String> listPartitionNames(PartitionsByExprRequest request)
      throws MetaException, TException, NoSuchObjectException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partition names");
  }

  /**
   * Get a list of partition values
   * @param request request
   * @return reponse
   * @throws MetaException error accessing RDBMS
   * @throws TException thrift transport error
   * @throws NoSuchObjectException no such table
   */
  default PartitionValuesResponse listPartitionValues(PartitionValuesRequest request)
      throws MetaException, TException, NoSuchObjectException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partition values");
  }

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
  default int getNumPartitionsByFilter(String dbName, String tableName, String filter)
      throws MetaException, NoSuchObjectException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting number of partitions by filter");
  }

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
  default int getNumPartitionsByFilter(String catName, String dbName, String tableName, String filter)
      throws MetaException, NoSuchObjectException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting number of partitions by filter");
  }


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
  default List<Partition> listPartitionsByFilter(String db_name, String tbl_name, String filter, short max_parts)
      throws MetaException, NoSuchObjectException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partitions by filter");
  }

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
  default List<Partition> listPartitionsByFilter(String catName, String db_name, String tbl_name,
                                         String filter, int max_parts)
      throws MetaException, NoSuchObjectException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partitions by filter");
  }

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
  default PartitionSpecProxy listPartitionSpecsByFilter(String db_name, String tbl_name, String filter, int max_parts)
      throws MetaException, NoSuchObjectException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partition specs by filter");
  }

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
  default PartitionSpecProxy listPartitionSpecsByFilter(String catName, String db_name, String tbl_name,
      String filter, int max_parts) throws MetaException, NoSuchObjectException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partition specs by filter");
  }

  /**
   * Get list of {@link PartitionSpec} matching specified serialized expression.
   * @param req PartitionsByExprRequest object
   * @return whether the resulting list contains partitions which may or may not match the expr
   * @throws TException thrift transport error or error executing the filter.
   */
  default boolean listPartitionsSpecByExpr(PartitionsByExprRequest req, List<PartitionSpec> result) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partition specs by expr");
  }

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
  default boolean listPartitionsByExpr(String db_name, String tbl_name,
      byte[] expr, String default_partition_name, short max_parts, List<Partition> result)
      throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partitions by expr");
  }

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
  default boolean listPartitionsByExpr(String catName, String db_name, String tbl_name, byte[] expr, 
      String default_partition_name, int max_parts, List<Partition> result) throws TException {
     throw new UnsupportedOperationException("tMetaStore client does not support listing partitions by expr");
  }

  /**
   * Get list of partitions matching specified serialized expression
   * @param req PartitionsByExprRequest object
   * @return whether the resulting list contains partitions which may or may not match the expr
   * @throws TException thrift transport error or error executing the filter.
   */
  default boolean listPartitionsByExpr(PartitionsByExprRequest req, List<Partition> result) throws TException{
    throw new UnsupportedOperationException("MetaStore client does not support listing partitions by expr");
  }

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
  default List<Partition> listPartitionsWithAuthInfo(String dbName,
      String tableName, short maxParts, String userName, List<String> groupNames)
      throws MetaException, TException, NoSuchObjectException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partitions with auth info");
  }

  /**
   * List partitions, fetching the authorization information along with the partitions.
   * @param req
   * @return GetPartitionsPsWithAuthResponse
   * @throws NoSuchObjectException no partitions matching the criteria were found
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  default GetPartitionsPsWithAuthResponse listPartitionsWithAuthInfoRequest(GetPartitionsPsWithAuthRequest req)
          throws MetaException, TException, NoSuchObjectException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partitions with auth info");
  }

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
  default List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
                                             int maxParts, String userName, List<String> groupNames)
      throws MetaException, TException, NoSuchObjectException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partitions with auth info");
  }

  /**
   * Get partitions by a list of partition names.
   * @param db_name database name
   * @param tbl_name table name
   * @param part_names list of partition names
   * @return list of Partition objects
   * @throws NoSuchObjectException No such partitions
   * @throws MetaException error accessing the RDBMS.
   * @throws TException thrift transport error
   * @deprecated Use {@link #getPartitionsByNames(GetPartitionsByNamesRequest)} instead
   */
  @Deprecated
  default List<Partition> getPartitionsByNames(String db_name, String tbl_name,
      List<String> part_names) throws NoSuchObjectException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting partitions by names");
  }

  /**
   * Get partitions by a list of partition names.
   * @param req
   * @return PartitionsResponse
   * @throws NoSuchObjectException No such partitions
   * @throws MetaException error accessing the RDBMS.
   * @throws TException thrift transport error
   */
  default PartitionsResponse getPartitionsRequest(PartitionsRequest req)
          throws NoSuchObjectException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting partitions request");
  }

    /**
   * Get partitions by a list of partition names.
   * @param req GetPartitionsByNamesRequest
   * @return list of Partition objects
   * @throws NoSuchObjectException No such partitions
   * @throws MetaException error accessing the RDBMS.
   * @throws TException thrift transport error
   */
  default GetPartitionsByNamesResult getPartitionsByNames(GetPartitionsByNamesRequest req) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting partitions by names");
  }

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
  default List<Partition> listPartitionsWithAuthInfo(String dbName,
      String tableName, List<String> partialPvals, short maxParts, String userName,
      List<String> groupNames) throws MetaException, TException, NoSuchObjectException {
     throw new UnsupportedOperationException("MetaStore client does not support listing partitions with auth info");
  }

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
  default List<Partition> listPartitionsWithAuthInfo(
      String catName, String dbName, String tableName, List<String> partialPvals, int maxParts, String userName,
      List<String> groupNames) throws MetaException, TException, NoSuchObjectException  {
     throw new UnsupportedOperationException("MetaStore client does not support listing partitions with auth info");
  }

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
  default void markPartitionForEvent(String db_name, String tbl_name, Map<String,String> partKVs,
      PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException,
      UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {
    throw new UnsupportedOperationException("MetaStore client does not support marking partition for event");
  }

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
  default void markPartitionForEvent(String catName, String db_name, String tbl_name, Map<String,String> partKVs,
                             PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException,
      UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {
    throw new UnsupportedOperationException("MetaStore client does not support marking partition for event");
  }

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
  default boolean isPartitionMarkedForEvent(String db_name, String tbl_name, Map<String,String> partKVs,
      PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException,
      UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {
     throw new UnsupportedOperationException("MetaStore client does not support checking if partition is marked for event");
  }

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
  default boolean isPartitionMarkedForEvent(String catName, String db_name, String tbl_name, Map<String,String> partKVs,
                                    PartitionEventType eventType) throws MetaException, NoSuchObjectException, TException,
      UnknownTableException, UnknownDBException, UnknownPartitionException, InvalidPartitionException {
     throw new UnsupportedOperationException("MetaStore client does not support checking if partition is marked for event");
  }

  /**
   * @param partVals
   * @throws TException
   * @throws MetaException
   */
  default void validatePartitionNameCharacters(List<String> partVals) throws TException, MetaException {
    throw new UnsupportedOperationException("MetaStore client does not support validating partition name characters");
  }

  /**
   * Dry run that translates table
   *    *
   *    * @param tbl
   *    *          a table object
   *    * @throws HiveException
   */
  default Table getTranslateTableDryrun(Table tbl) throws TException {
    return new Table();
  }

  /**
   * @param tbl
   * @throws AlreadyExistsException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   */
  void createTable(Table tbl) throws AlreadyExistsException,
      InvalidObjectException, MetaException, NoSuchObjectException, TException;

  /**
   * @param request CreateTableRequest
   * @throws AlreadyExistsException
   * @throws InvalidObjectException
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   */
  void createTable(CreateTableRequest request) throws AlreadyExistsException,
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
  default void alter_table(String databaseName, String tblName, Table table)
      throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering table");
  }

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
  default void alter_table(String catName, String dbName, String tblName, Table newTable,
                  EnvironmentContext envContext)
      throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering table");
  }

  /**
   * @deprecated Use alter_table_with_environmentContext instead of alter_table with cascade option
   * passed in EnvironmentContext using {@code StatsSetupConst.CASCADE}
   */
  @Deprecated
  default void alter_table(String defaultDatabaseName, String tblName, Table table, boolean cascade) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering table with cascade option");
  }

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
  @Deprecated
  default void alter_table_with_environmentContext(String databaseName, String tblName, Table table,
      EnvironmentContext environmentContext) throws InvalidOperationException, MetaException,
      TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering table with environment context");
  }

  default void alter_table(String catName, String databaseName, String tblName, Table table,
      EnvironmentContext environmentContext, String validWriteIdList) throws TException {}
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
  @Deprecated
  default void dropDatabase(String catName, String dbName, boolean deleteData, boolean ignoreUnknownDb, boolean cascade)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    DropDatabaseRequest req = new DropDatabaseRequest();
    req.setName(dbName);
    req.setCatalogName(catName);
    req.setIgnoreUnknownDb(ignoreUnknownDb);
    req.setDeleteData(deleteData);
    req.setCascade(cascade);

    dropDatabase(req);
  }

  void dropDatabase(DropDatabaseRequest req) throws TException;

  /**
   * Alter a database.
   * @param name database name.
   * @param db new database object.
   * @throws NoSuchObjectException No database of this name exists in the specified catalog.
   * @throws MetaException something went wrong, usually in the RDBMS.
   * @throws TException general thrift error.
   */
  default void alterDatabase(String name, Database db)
      throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering database");
  }

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
  default void alterDatabase(String catName, String dbName, Database newDb)
      throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering database");
  }

  /**
   * Create a new dataconnector.
   * @param connector object.
   * @throws InvalidObjectException There is something wrong with the dataconnector object.
   * @throws AlreadyExistsException There is already a dataconnector with this name.
   * @throws MetaException something went wrong, usually in the RDBMS
   * @throws TException general thrift error
   */
  default void createDataConnector(DataConnector connector)
      throws InvalidObjectException, AlreadyExistsException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support creating dataconnector");
  }

  /**
   * Drop a dataconnector.
   * @param name name of the dataconnector to drop.
   * @param ifNotExists if specified, drop will not throw an exception if the connector does not exist.
   * @param checkReferences drop only if there are no dbs referencing this connector.
   * @throws NoSuchObjectException No such dataconnector exists.
   * @throws InvalidOperationException The dataconnector cannot be dropped because it is not allowed.
   * @throws MetaException something went wrong, usually either in the RDMBS or in storage.
   * @throws TException general thrift error.
   */
  default void dropDataConnector(String name, boolean ifNotExists, boolean checkReferences)
      throws NoSuchObjectException, InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support dropping dataconnector");
  }

  /**
   * Alter a dataconnector.
   * @param name dataconnector name.
   * @param connector new dataconnector object.
   * @throws NoSuchObjectException No dataconnector with this name exists.
   * @throws MetaException Operation could not be completed, usually in the RDBMS.
   * @throws TException thrift transport layer error.
   */
  default void alterDataConnector(String name, DataConnector connector)
      throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering dataconnector");
  }

  /**
   * Get the dataconnector by name
   * @return DataConnector if there is a match
   * @throws MetaException error complete the operation
   * @throws TException thrift transport error
   */
  default DataConnector getDataConnector(String name)
      throws MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting dataconnector by name");
  }

  /**
   * Get the names of all dataconnectors in the MetaStore.
   * @return List of dataconnector names.
   * @throws MetaException error accessing RDBMS.
   * @throws TException thrift transport error
   */
  default List<String> getAllDataConnectorNames() throws MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting all dataconnector names");
  }

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
  default boolean dropPartition(String db_name, String tbl_name,
      List<String> part_vals, boolean deleteData) throws NoSuchObjectException,
      MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support dropping partition");
  }

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
  default boolean dropPartition(String catName, String db_name, String tbl_name,
                        List<String> part_vals, boolean deleteData) throws NoSuchObjectException,
      MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support dropping partition");
  }

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
  default boolean dropPartition(String db_name, String tbl_name, List<String> part_vals,
                        PartitionDropOptions options)
      throws NoSuchObjectException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support dropping partition");
  }

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
  default boolean dropPartition(String catName, String db_name, String tbl_name, List<String> part_vals,
                        PartitionDropOptions options)
      throws NoSuchObjectException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support dropping partition");
  }

  /**
   * Drop partitions based on an expression.
   * @deprecated since 4.1.0, will be removed in 5.0.0
   * use {@link #dropPartitions(TableName, RequestPartsSpec, PartitionDropOptions, EnvironmentContext)} instead.
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
  @Deprecated
  default List<Partition> dropPartitions(String dbName, String tblName,
                                 List<Pair<Integer, byte[]>> partExprs, boolean deleteData,
                                 boolean ifExists) throws NoSuchObjectException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support dropping partitions by expression");
  }

  /**
   * Drop partitions based on an expression.
   * @deprecated since 4.1.0, will be removed in 5.0.0
   * use {@link #dropPartitions(TableName, RequestPartsSpec, PartitionDropOptions, EnvironmentContext)} instead.
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
  @Deprecated
  default List<Partition> dropPartitions(String catName, String dbName, String tblName,
                                         List<Pair<Integer, byte[]>> partExprs,
                                         boolean deleteData, boolean ifExists)
      throws NoSuchObjectException, MetaException, TException {
    return dropPartitions(catName, dbName, tblName, partExprs,
        PartitionDropOptions.instance()
            .deleteData(deleteData)
            .ifExists(ifExists));
  }
  @Deprecated
  default List<Partition> dropPartitions(String dbName, String tblName,
      List<Pair<Integer, byte[]>> partExprs, boolean deleteData,
      boolean ifExists, boolean needResults) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support dropping partitions by expression");
  }

  /**
   * Drop partitions based on an expression.
   * @deprecated since 4.1.0, will be removed in 5.0.0
   * use {@link #dropPartitions(TableName, RequestPartsSpec, PartitionDropOptions, EnvironmentContext)} instead.
   * (HIVE-28658 Add Iceberg REST Catalog client support)
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
  @Deprecated
  default List<Partition> dropPartitions(String catName, String dbName, String tblName,
                                         List<Pair<Integer, byte[]>> partExprs, boolean deleteData,
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
   * @deprecated since 4.1.0, will be removed in 5.0.0
   * use {@link #dropPartitions(TableName, RequestPartsSpec, PartitionDropOptions, EnvironmentContext)} instead.
   * @param dbName Name of the database
   * @param tblName Name of the table
   * @param partExprs Partition-specification
   * @param options Boolean options for dropping partitions
   * @return List of Partitions dropped
   * @throws NoSuchObjectException No partition matches the expression(s), and ifExists was false.
   * @throws MetaException error access the RDBMS or storage.
   * @throws TException On failure
   */
  @Deprecated
  default List<Partition> dropPartitions(String dbName, String tblName,
                                 List<Pair<Integer, byte[]>> partExprs,
                                 PartitionDropOptions options)
      throws NoSuchObjectException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support dropping partitions by expression");
  }

  /**
   * Generalization of dropPartitions(),
   * @deprecated since 4.1.0, will be removed in 5.0.0
   * use {@link #dropPartitions(TableName, RequestPartsSpec, PartitionDropOptions, EnvironmentContext)} instead.
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
  @Deprecated
  default List<Partition> dropPartitions(String catName, String dbName, String tblName,
                                 List<Pair<Integer, byte[]>> partExprs,
                                 PartitionDropOptions options)
      throws NoSuchObjectException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support dropping partitions by expression");
  }

  /**
   * @deprecated since 4.1.0, will be removed in 5.0.0
   * use {@link #dropPartitions(TableName, RequestPartsSpec, PartitionDropOptions, EnvironmentContext)} instead.
   */
  @Deprecated
  default List<Partition> dropPartitions(String catName, String dbName, String tblName,
      List<Pair<Integer, byte[]>> partExprs, PartitionDropOptions options, EnvironmentContext context)
      throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support dropping partitions by expression");
  }

  /**
   * Drop partitions based on the request partitions specification.
   * @param tableName Name of the table.
   * @param partsSpec Specification of the partitions to drop.
   * @param options Options for dropping partitions.
   * @param context Environment context for the operation.
   * @return List of Partitions dropped.
   * @throws TException thrift transport error.
   */
  default List<Partition> dropPartitions(TableName tableName,
      RequestPartsSpec partsSpec, PartitionDropOptions options, EnvironmentContext context)
      throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support dropping partition");
  }

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
  default boolean dropPartition(String db_name, String tbl_name,
      String name, boolean deleteData) throws NoSuchObjectException,
      MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support dropping partition");
  }

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
  default boolean dropPartition(String catName, String db_name, String tbl_name,
                        String name, boolean deleteData)
      throws NoSuchObjectException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support dropping partition");
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
  default void alter_partition(String dbName, String tblName, Partition newPart)
      throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering partition");
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
  @Deprecated
  default void alter_partition(String dbName, String tblName, Partition newPart, EnvironmentContext environmentContext)
      throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering partition");
  }


  default void alter_partition(String catName, String dbName, String tblName, Partition newPart,
      EnvironmentContext environmentContext, String writeIdList)
      throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering partition");
  }

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
  default void alter_partition(String catName, String dbName, String tblName, Partition newPart,
                       EnvironmentContext environmentContext)
      throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering partition");
  }

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
  @Deprecated
  default void alter_partitions(String dbName, String tblName, List<Partition> newParts)
      throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering partitions");
  }

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
  @Deprecated
  default  void alter_partitions(String dbName, String tblName, List<Partition> newParts,
      EnvironmentContext environmentContext) throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering partitions");
  }

  default  void alter_partitions(String dbName, String tblName, List<Partition> newParts,
                        EnvironmentContext environmentContext,
                        String writeIdList, long writeId) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering partitions");
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
  default void alter_partitions(String catName, String dbName, String tblName, List<Partition> newParts,
                        EnvironmentContext environmentContext,
                        String writeIdList, long writeId)
      throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering partitions");
  }

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
   *           if srcFs and destFs are different, or trying to rename to an already existing partition name
   * @throws MetaException
   *          if error in updating metadata
   * @throws TException
   *          if error in communicating with metastore server
   */
  @Deprecated
  default void renamePartition(final String dbname, final String tableName, final List<String> part_vals,
                       final Partition newPart)
      throws InvalidOperationException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support renaming partition");
  }

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
   *           if srcFs and destFs are different, or trying to rename to an already existing partition name
   * @throws MetaException
   *          if error in updating metadata
   * @throws TException
   *          if error in communicating with metastore server
   */
  default void renamePartition(String catName, String dbname, String tableName, List<String> part_vals, 
                               Partition newPart, String validWriteIds) 
        throws TException {
    renamePartition(catName, dbname, tableName, part_vals, newPart, validWriteIds, 0, false);
  }

  default void renamePartition(String catName, String dbname, String tableName, List<String> part_vals,
                       Partition newPart, String validWriteIds, long txnId, boolean makeCopy)
    throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support renaming partition");
  }

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
  default List<FieldSchema> getFields(String db, String tableName)
      throws MetaException, TException, UnknownTableException,
      UnknownDBException {
     throw new UnsupportedOperationException("MetaStore client does not support getting fields for a table");
  }

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
  default List<FieldSchema> getFields(String catName, String db, String tableName)
      throws MetaException, TException, UnknownTableException,
      UnknownDBException {
     throw new UnsupportedOperationException("MetaStore client does not support getting fields for a table");
  }

  /**
   * Get schema for a table, excluding the partition columns.
   * @param req
   * @return GetFieldsResponse
   * @throws UnknownTableException no such table
   * @throws UnknownDBException no such database
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  default GetFieldsResponse getFieldsRequest(GetFieldsRequest req)
          throws MetaException, TException, UnknownTableException,
          UnknownDBException {
    throw new UnsupportedOperationException("MetaStore client does not support getting fields for a table");
  }

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
  default List<FieldSchema> getSchema(String db, String tableName)
      throws MetaException, TException, UnknownTableException,
      UnknownDBException {
     throw new UnsupportedOperationException("MetaStore client does not support getting schema for a table");
  }

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
  default List<FieldSchema> getSchema(String catName, String db, String tableName)
      throws MetaException, TException, UnknownTableException,
      UnknownDBException {
     throw new UnsupportedOperationException("MetaStore client does not support getting schema for a table");
  }

  /**
   * Get schema for a table, including the partition columns.
   * @param req
   * @return GetSchemaResponse
   * @throws UnknownTableException no such table
   * @throws UnknownDBException no such database
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  default GetSchemaResponse getSchemaRequest(GetSchemaRequest req)
          throws MetaException, TException, UnknownTableException,
          UnknownDBException {
    throw new UnsupportedOperationException("MetaStore client does not support getting schema for a table");
  }

  /**
   * @param name
   *          name of the configuration property to get the value of
   * @param defaultValue
   *          the value to return if property with the given name doesn't exist
   * @return value of the specified configuration property
   * @throws TException
   * @throws ConfigValSecurityException
   */
  default String getConfigValue(String name, String defaultValue)
      throws TException, ConfigValSecurityException {
    return "50";
  }

  /**
   *
   * @param name
   *          the partition name e.g. ("ds=2010-03-03/hr=12")
   * @return a list containing the partition col values, in the same order as the name
   * @throws MetaException
   * @throws TException
   */
  default List<String> partitionNameToVals(String name)
      throws MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support converting partition name to values");
  }
  /**
   *
   * @param name
   *          the partition name e.g. ("ds=2010-03-03/hr=12")
   * @return a map from the partition col to the value, as listed in the name
   * @throws MetaException
   * @throws TException
   */
  default Map<String, String> partitionNameToSpec(String name)
      throws MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support converting partition name to spec");
  }

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
  default boolean updateTableColumnStatistics(ColumnStatistics statsObj)
    throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
    InvalidInputException {
     throw new UnsupportedOperationException("MetaStore client does not support updating table column statistics");
}

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
  default boolean updatePartitionColumnStatistics(ColumnStatistics statsObj)
   throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
   InvalidInputException {
     throw new UnsupportedOperationException("MetaStore client does not support updating partition column statistics");
  }

  /**
   * Get the column statistics for a set of columns in a table.  This should only be used for
   * non-partitioned tables.  For partitioned tables use
   * {@link #getPartitionColumnStatistics(String, String, List, List, String)}.
   * @param dbName database name
   * @param tableName table name
   * @param colNames list of column names
   * @param engine engine sending the request
   * @return list of column statistics objects, one per column
   * @throws NoSuchObjectException no such table
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  default List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames, String engine) throws NoSuchObjectException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting table column statistics");
  }

  default List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames, String engine, String validWriteIdList) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting table column statistics");
  }

  /**
   * Get the column statistics for a set of columns in a table.  This should only be used for
   * non-partitioned tables.  For partitioned tables use
   * {@link #getPartitionColumnStatistics(String, String, String, List, List, String)}.
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param colNames list of column names
   * @param engine engine sending the request
   * @return list of column statistics objects, one per column
   * @throws NoSuchObjectException no such table
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  default List<ColumnStatisticsObj> getTableColumnStatistics(String catName, String dbName, String tableName,
      List<String> colNames, String engine) throws NoSuchObjectException, MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting table column statistics");
  }

  default List<ColumnStatisticsObj> getTableColumnStatistics(String catName, String dbName, String tableName,
      List<String> colNames, String engine, String validWriteIdList) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting table column statistics");
  }
  /**
   * Get the column statistics for a set of columns in a partition.
   * @param dbName database name
   * @param tableName table name
   * @param partNames partition names.  Since these are names they should be of the form
   *                  "key1=value1[/key2=value2...]"
   * @param colNames list of column names
   * @param engine engine sending the request
   * @return map of columns to statistics
   * @throws NoSuchObjectException no such partition
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  default Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName,
      String tableName,  List<String> partNames, List<String> colNames, String engine)
          throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting partition column statistics");
  }

  default Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(String dbName, String tableName, 
    List<String> partNames, List<String> colNames, String engine, String validWriteIdList) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting partition column statistics");
  }

  /**
   * Get the column statistics for a set of columns in a partition.
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param partNames partition names.  Since these are names they should be of the form
   *                  "key1=value1[/key2=value2...]"
   * @param colNames list of column names
   * @param engine engine sending the request
   * @return map of columns to statistics
   * @throws NoSuchObjectException no such partition
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  default Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
      String catName, String dbName, String tableName,  List<String> partNames, List<String> colNames,
      String engine) throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting partition column statistics");
  }

  default Map<String, List<ColumnStatisticsObj>> getPartitionColumnStatistics(
      String catName, String dbName, String tableName,
      List<String> partNames, List<String> colNames,
      String engine, String validWriteIdList) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting partition column statistics");
  }
  /**
   * Delete partition level column statistics given dbName, tableName, partName and colName, or
   * all columns in a partition.
   * @param dbName database name.
   * @param tableName table name.
   * @param partName partition name.
   * @param colName column name, or null for all columns
   * @param engine engine, or null for all engines
   * @return boolean indicating outcome of the operation
   * @throws NoSuchObjectException no such partition exists
   * @throws InvalidObjectException error dropping the stats data
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   * @throws InvalidInputException input is invalid or null.
   * @deprecated Use
   *    {@link IMetaStoreClient#deleteColumnStatistics(org.apache.hadoop.hive.metastore.api.DeleteColumnStatisticsRequest)} instead
   */
  @Deprecated
  default boolean deletePartitionColumnStatistics(String dbName, String tableName,
    String partName, String colName, String engine) throws NoSuchObjectException, MetaException,
    InvalidObjectException, TException, InvalidInputException {
    DeleteColumnStatisticsRequest request = new DeleteColumnStatisticsRequest(dbName, tableName);
    request.setEngine(engine);
    if (colName != null) {
      request.addToCol_names(colName);
    }
    if (partName != null) {
      request.addToPart_names(partName);
    }
    return deleteColumnStatistics(request);
  }

  /**
   * Delete partition level column statistics given dbName, tableName, partName and colName, or
   * all columns in a partition.
   * @param catName catalog name.
   * @param dbName database name.
   * @param tableName table name.
   * @param partName partition name.
   * @param colName column name, or null for all columns
   * @param engine engine, or null for all engines
   * @return boolean indicating outcome of the operation
   * @throws NoSuchObjectException no such partition exists
   * @throws InvalidObjectException error dropping the stats data
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   * @throws InvalidInputException input is invalid or null.
   * @deprecated Use
   *    {@link IMetaStoreClient#deleteColumnStatistics(org.apache.hadoop.hive.metastore.api.DeleteColumnStatisticsRequest)} instead
   */
  @Deprecated
  default boolean deletePartitionColumnStatistics(String catName, String dbName, String tableName,
      String partName, String colName, String engine)
      throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
    DeleteColumnStatisticsRequest request = new DeleteColumnStatisticsRequest(dbName, tableName);
    request.setCat_name(catName);
    request.setEngine(engine);
    if (colName != null) {
      request.addToCol_names(colName);
    }
    if (partName != null) {
      request.addToPart_names(partName);
    }
    return deleteColumnStatistics(request);
  }

  /**
   * Delete table level column statistics given dbName, tableName and colName, or all columns in
   * a table.  This should be used for non-partitioned tables.
   * @param dbName database name
   * @param tableName table name
   * @param colName column name, or null to drop stats for all columns
   * @param engine engine, or null for all engines
   * @return boolean indicating the outcome of the operation
   * @throws NoSuchObjectException No such table
   * @throws MetaException error accessing the RDBMS
   * @throws InvalidObjectException error dropping the stats
   * @throws TException thrift transport error
   * @throws InvalidInputException bad input, like a null table name.
   * @deprecated Use
   *    {@link IMetaStoreClient#deleteColumnStatistics(org.apache.hadoop.hive.metastore.api.DeleteColumnStatisticsRequest)} instead
   */
  @Deprecated
  default boolean deleteTableColumnStatistics(String dbName, String tableName, String colName, String engine) throws
    NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
    DeleteColumnStatisticsRequest request = new DeleteColumnStatisticsRequest(dbName, tableName);
    request.setEngine(engine);
    if (colName != null) {
      request.addToCol_names(colName);
    }
    request.setTableLevel(true);
    return deleteColumnStatistics(request);
  }

  /**
   * Delete table level column statistics given dbName, tableName and colName, or all columns in
   * a table.  This should be used for non-partitioned tables.
   * @param catName catalog name
   * @param dbName database name
   * @param tableName table name
   * @param colName column name, or null to drop stats for all columns
   * @param engine engine, or null for all engines
   * @return boolean indicating the outcome of the operation
   * @throws NoSuchObjectException No such table
   * @throws MetaException error accessing the RDBMS
   * @throws InvalidObjectException error dropping the stats
   * @throws TException thrift transport error
   * @throws InvalidInputException bad input, like a null table name.
   * @deprecated Use
   *    {@link IMetaStoreClient#deleteColumnStatistics(org.apache.hadoop.hive.metastore.api.DeleteColumnStatisticsRequest)} instead
   */
  @Deprecated
  default boolean deleteTableColumnStatistics(String catName, String dbName, String tableName, String colName, String engine)
      throws NoSuchObjectException, MetaException, InvalidObjectException, TException, InvalidInputException {
    DeleteColumnStatisticsRequest request = new DeleteColumnStatisticsRequest(dbName, tableName);
    request.setCat_name(catName);
    request.setEngine(engine);
    if (colName != null) {
      request.addToCol_names(colName);
    }
    request.setTableLevel(true);
    return deleteColumnStatistics(request);
  }

  /**
   * Delete table or partition level column statistics given catName, dbName, tableName, partName and colNames,
   * or all columns in a table or partition.
   * This should be used for tables or partitions
   * @param req the DeleteColumnStatisticsRequest which including
   *            catalog name, database name, table name, partition name(optional),
   *            a list column names(optional), and engine name
   * @return boolean indicating the outcome of the operation
   * @throws TException thrift transport error
   */
  default boolean deleteColumnStatistics(DeleteColumnStatisticsRequest req) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support deleting column statistics");
  }

  default void updateTransactionalStatistics(UpdateTransactionalStatsRequest req) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support updating transactional statistics");
  }

  /**
   * @param role
   *          role object
   * @return true on success
   * @throws MetaException
   * @throws TException
   */
  default boolean create_role(Role role)
      throws MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support creating roles");
  }

  /**
   * @param role_name
   *          role name
   *
   * @return true on success
   * @throws MetaException
   * @throws TException
   */
  default boolean drop_role(String role_name) throws MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support dropping roles");
  }

  /**
   * list all role names
   * @return list of names
   * @throws TException
   * @throws MetaException
   */
  default List<String> listRoleNames() throws MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support listing role names");
  }

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
  default boolean grant_role(String role_name, String user_name,
      PrincipalType principalType, String grantor, PrincipalType grantorType,
      boolean grantOption) throws MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support granting roles");
  }

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
  default boolean revoke_role(String role_name, String user_name,
      PrincipalType principalType, boolean grantOption) throws MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support revoking roles");
  }

  /**
   *
   * @param principalName
   * @param principalType
   * @return list of roles
   * @throws MetaException
   * @throws TException
   */
  default List<Role> list_roles(String principalName, PrincipalType principalType)
      throws MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support listing roles");
  }

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
  default PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject,
      String user_name, List<String> group_names) throws MetaException,
      TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting privilege set");
  }

  /**
   * Return the privileges that this principal has directly over the object (not through roles).
   * @param principal_name
   * @param principal_type
   * @param hiveObject
   * @return list of privileges
   * @throws MetaException
   * @throws TException
   */
  default List<HiveObjectPrivilege> list_privileges(String principal_name,
      PrincipalType principal_type, HiveObjectRef hiveObject)
      throws MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support listing privileges");
  }

  /**
   * @param privileges
   * @return true on success
   * @throws MetaException
   * @throws TException
   */
  default boolean grant_privileges(PrivilegeBag privileges)
      throws MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support granting privileges");
  }

  /**
   * @param privileges
   * @return true on success
   * @throws MetaException
   * @throws TException
   */
  default boolean revoke_privileges(PrivilegeBag privileges, boolean grantOption)
      throws MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support revoking privileges");
  }

  /**
   * @param authorizer
   * @param objToRefresh
   * @return true on success
   * @throws MetaException
   * @throws TException
   */
  default boolean refresh_privileges(HiveObjectRef objToRefresh, String authorizer, PrivilegeBag grantPrivileges)
      throws MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support refreshing privileges");
  }

  /**
   * This is expected to be a no-op when in local mode,
   * which means that the implementation will return null.
   * @param owner the intended owner for the token
   * @param renewerKerberosPrincipalName
   * @return the string of the token
   * @throws MetaException
   * @throws TException
   */
  default String getDelegationToken(String owner, String renewerKerberosPrincipalName)
      throws MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting delegation token");
  }

  /**
   * @param tokenStrForm
   * @return the new expiration time
   * @throws MetaException
   * @throws TException
   */
  default long renewDelegationToken(String tokenStrForm) throws MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support renewing delegation token");
  }

  /**
   * @param tokenStrForm
   * @throws MetaException
   * @throws TException
   */
  default void cancelDelegationToken(String tokenStrForm) throws MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support canceling delegation token");
  }

  default String getTokenStrForm() throws IOException {
    throw new UnsupportedOperationException("MetaStore client does not support getting token string form");
  }

  default boolean addToken(String tokenIdentifier, String delegationToken) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support adding tokens");
  }

  default boolean removeToken(String tokenIdentifier) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support removing tokens");
  }

  default String getToken(String tokenIdentifier) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting tokens");
  }

  default List<String> getAllTokenIdentifiers() throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting all tokens");
  }

  default int addMasterKey(String key) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support adding master keys");
  }

  default void updateMasterKey(Integer seqNo, String key) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support updating master keys");
  }

  default boolean removeMasterKey(Integer keySeq) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support removing master keys");
  }


  default String[] getMasterKeys() throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting master keys");
  }

  /**
   * Create a new function.
   * @param func function specification
   * @throws InvalidObjectException the function object is invalid
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  default void createFunction(Function func)
      throws InvalidObjectException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support creating functions");
  }

  /**
   * Alter a function.
   * @param dbName database name.
   * @param funcName function name.
   * @param newFunction new function specification.  This should be complete, not just the changes.
   * @throws InvalidObjectException the function object is invalid
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  default void alterFunction(String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering functions");
  }

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
  default void alterFunction(String catName, String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering functions");
  }

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
  default void dropFunction(String dbName, String funcName) throws MetaException,
      NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support dropping functions");
  }

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
  default void dropFunction(String catName, String dbName, String funcName) throws MetaException,
      NoSuchObjectException, InvalidObjectException, InvalidInputException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support dropping functions");
  }

  /**
   * Get a function.
   * @param dbName database name.
   * @param funcName function name.
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  default Function getFunction(String dbName, String funcName)
      throws MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting functions");
  }

  /**
   * Get a function.
   * @param catName catalog name.
   * @param dbName database name.
   * @param funcName function name.
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  default Function getFunction(String catName, String dbName, String funcName)
      throws MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting functions");
  }

  /**
   * Get all functions matching a pattern
   * @param dbName database name.
   * @param pattern to match.  This is a java regex pattern.
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  @Deprecated
  default List<String> getFunctions(String dbName, String pattern)
      throws MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting functions");
  }

  /**
   * Get all functions matching a pattern
   * @param functionRequest function request.
   * @throws TException thrift transport error
   */
  default GetFunctionsResponse getFunctionsRequest(GetFunctionsRequest functionRequest)
      throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting functions");
  }
  /**
   * Get all functions matching a pattern
   * @param catName catalog name.
   * @param dbName database name.
   * @param pattern to match.  This is a java regex pattern.
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  @Deprecated
  default List<String> getFunctions(String catName, String dbName, String pattern)
      throws MetaException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting functions");
  }

  /**
   * Get all functions in the default catalog.
   * @return list of functions
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport error
   */
  default GetAllFunctionsResponse getAllFunctions() throws MetaException, TException {
    return new GetAllFunctionsResponse();
  }

  default GetOpenTxnsResponse getOpenTxns() throws TException  {
    throw new UnsupportedOperationException("MetaStore client does not support getting open transactions");
  }

  /**
   * Get a structure that details valid transactions.
   * @return list of valid transactions
   * @throws TException
   */
  default ValidTxnList getValidTxns() throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting valid transactions");
  }

  /**
   * Get a structure that details valid transactions.
   * @param currentTxn The current transaction of the caller. This will be removed from the
   *                   exceptions list so that the caller sees records from his own transaction.
   * @return list of valid transactions and also valid write IDs for each input table.
   * @throws TException
   */
  default ValidTxnList getValidTxns(long currentTxn) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting valid transactions");
  }

  /**
   * Get a structure that details valid transactions.
   * @param currentTxn The current transaction of the caller. This will be removed from the
   *                   exceptions list so that the caller sees records from his own transaction.
   * @param excludeTxnTypes list of transaction types that should be excluded from the valid transaction list.
   * @return list of valid transactions and also valid write IDs for each input table.
   * @throws TException
   */
  default ValidTxnList getValidTxns(long currentTxn, List<TxnType> excludeTxnTypes) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting valid transactions");
  }

  /**
   * Get a structure that details valid write ids.
   * @param fullTableName full table name of format &lt;db_name&gt;.&lt;table_name&gt;
   * @return list of valid write ids for the given table
   * @throws TException
   */
  default ValidWriteIdList getValidWriteIds(String fullTableName) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting valid write ids");
  }

  /**
   * Get a structure that details valid write ids.
   * @param fullTableName full table name of format &lt;db_name&gt;.&lt;table_name&gt;
   * @param writeId The write id to get the corresponding txn
   * @return list of valid write ids for the given table
   * @throws TException
   */
  default ValidWriteIdList getValidWriteIds(String fullTableName, Long writeId) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting valid write ids");
  }

  /**
   * Get a structure that details valid write ids list for all tables read by current txn.
   * @param tablesList list of tables (format: &lt;db_name&gt;.&lt;table_name&gt;) read from the current transaction
   *                   for which needs to populate the valid write ids
   * @param validTxnList snapshot of valid txns for the current txn
   * @return list of valid write ids for the given list of tables.
   * @throws TException
   */
  default List<TableValidWriteIds> getValidWriteIds(List<String> tablesList, String validTxnList)
          throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting valid write ids");
  }

  /**
   * Persists minOpenWriteId list to identify obsolete directories eligible for cleanup
   * @param txnId transaction identifier
   * @param writeIds list of minOpenWriteId
   */
  default void addWriteIdsToMinHistory(long txnId, Map<String, Long> writeIds) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support adding write ids to min history");
  }
    
  /**
   * Initiate a transaction.
   * @param user User who is opening this transaction.  This is the Hive user,
   *             not necessarily the OS user.  It is assumed that this user has already been
   *             authenticated and authorized at this point.
   * @return transaction identifier
   * @throws TException
   */
  default long openTxn(String user) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support opening transactions");
  }

  /**
   * Initiate a transaction with given type.
   * @param user User who is opening this transaction.
   * @param txnType Type of needed transaction.
   * @return transaction identifier
   * @throws TException
   */
  default long openTxn(String user, TxnType txnType) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support opening transactions with type");
  }

  /**
   * Initiate a repl replayed or hive replication transaction (dump/load).
   * @param replPolicy Contains replication policy to uniquely identify the source cluster in case of repl replayed txns
   *                   or database under replication name for hive replication txns
   * @param srcTxnIds The list of transaction ids at the source cluster in case of repl replayed transactions
   *                 or null in case of hive replication transactions.
   * @param user The user who has fired the command.
   *
   * @param txnType Type of transaction to open: REPL_CREATED for repl replayed transactions
   *                                             DEFAULT for hive replication transactions.
   * @return transaction identifiers
   * @throws TException
   */
  default List<Long> replOpenTxn(String replPolicy, List<Long> srcTxnIds, String user, TxnType txnType) 
      throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support opening repl replayed or hive " +
         "replication transactions");
  }

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
  default OpenTxnsResponse openTxns(String user, int numTxns) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support opening transactions in batch");
  }

  /**
   * Rollback a transaction.  This will also unlock any locks associated with
   * this transaction.
   * @param txnid id of transaction to be rolled back.
   * @throws NoSuchTxnException if the requested transaction does not exist.
   * Note that this can result from the transaction having timed out and been
   * deleted.
   * @throws TException
   */
  default void rollbackTxn(long txnid) throws NoSuchTxnException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support rolling back transactions");
  }

  /**
   * Rollback a transaction.  This will also unlock any locks associated with
   * this transaction.
   * @param abortTxnRequest AbortTxnRequest object containing transaction id and
   * error codes.
   * @throws NoSuchTxnException if the requested transaction does not exist.
   * Note that this can result from the transaction having timed out and been
   * deleted.
   * @throws TException
   */
  default void rollbackTxn(AbortTxnRequest abortTxnRequest) throws NoSuchTxnException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support rolling back transactions");
  }

  /**
   * Rollback a transaction.  This will also unlock any locks associated with
   * this transaction.
   * @param srcTxnid id of transaction at source while is rolled back and to be replicated
   *                 or null in case of hive replication transactions
   * @param replPolicy Contains replication policy to uniquely identify the source cluster in case of repl replayed txns
   *                   or database under replication name for hive replication txns
   * @param txnType Type of transaction to Rollback: REPL_CREATED for repl replayed transactions
    *                                                DEFAULT for hive replication transactions.
   * @throws NoSuchTxnException if the requested transaction does not exist.
   * Note that this can result from the transaction having timed out and been
   * deleted.
   * @throws TException
   */
  default void replRollbackTxn(long srcTxnid, String replPolicy, TxnType txnType) throws NoSuchTxnException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support rolling back transactions");
  }


  default ReplayedTxnsForPolicyResult getReplayedTxnsForPolicy(String replPolicy) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting replayed transactions " +
        "for policy");
  }

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
  default void commitTxn(long txnid)
      throws NoSuchTxnException, TxnAbortedException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support committing transactions");
  }

  /**
   * Like commitTxn, but it will atomically store as well a key and a value. This
   * can be useful for example to know if the transaction corresponding to
   * txnid has been committed by later querying with DESCRIBE EXTENDED TABLE.
   * TABLE_PARAMS from the metastore must already have a row with the TBL_ID
   * corresponding to the table in the parameters and PARAM_KEY the same as key
   * in the parameters. The way to update this table is with an ALTER command
   * to overwrite/create the table properties.
   * @param txnid id of transaction to be committed.
   * @param tableId id of the table to associate the key/value with
   * @param key key to be committed. It must start with "_meta". The reason
   *            for this is to prevent important keys being updated, like owner.
   * @param value value to be committed.
   * @throws NoSuchTxnException if the requested transaction does not exist.
   * This can result fro the transaction having timed out and been deleted by
   * the compactor.
   * @throws TxnAbortedException if the requested transaction has been
   * aborted.  This can result from the transaction timing out.
   * @throws IllegalStateException if not exactly one row corresponding to
   * tableId and key are found in TABLE_PARAMS while updating.
   * @throws TException
   */
  default void commitTxnWithKeyValue(long txnid, long tableId,
      String key, String value) throws NoSuchTxnException,
      TxnAbortedException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support committing transactions with key/value");
  }

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
  default void commitTxn(CommitTxnRequest rqst)
          throws NoSuchTxnException, TxnAbortedException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support committing transactions with request");
  }

  /**
   * Abort a list of transactions. This is for use by "ABORT TRANSACTIONS" in the grammar.
   * @throws TException
   */
  default void abortTxns(List<Long> txnids) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support aborting transactions");
  }

  /**
   * Abort a list of transactions with additional information of
   * errorcodes as defined in TxnErrorMsg.java.
   * @param abortTxnsRequest Information containing txnIds and error codes
   * @throws TException
   */
  default void abortTxns(AbortTxnsRequest abortTxnsRequest) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support aborting transactions with request");
  }

  /**
   * Allocate a per table write ID and associate it with the given transaction.
   * @param txnId id of transaction to which the allocated write ID to be associated.
   * @param dbName name of DB in which the table belongs.
   * @param tableName table to which the write ID to be allocated
   * @throws TException
   */
  default long allocateTableWriteId(long txnId, String dbName, String tableName) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support allocating table write IDs");
  }

  /**
   * Allocate a per table write ID and associate it with the given transaction.
   * @param txnId id of transaction to which the allocated write ID to be associated.
   * @param dbName name of DB in which the table belongs.
   * @param tableName table to which the write ID to be allocated
   * @param reallocate should we reallocate already mapped writeId (if true) or reuse (if false)
   * @throws TException
   */
  default long allocateTableWriteId(long txnId, String dbName, String tableName, boolean reallocate) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support allocating table write IDs " +
         "with reallocate option");
  }

  /**
   * Replicate Table Write Ids state to mark aborted write ids and writeid high water mark.
   * @param validWriteIdList Snapshot of writeid list when the table/partition is dumped.
   * @param dbName Database name
   * @param tableName Table which is written.
   * @param partNames List of partitions being written.
   * @throws TException in case of failure to replicate the writeid state
   */
  default void replTableWriteIdState(String validWriteIdList, String dbName, String tableName, List<String> partNames)
          throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support replicating table write IDs state");
  }

  /**
   * Allocate a per table write ID and associate it with the given transaction.
   * @param txnIds ids of transaction batchto which the allocated write ID to be associated.
   * @param dbName name of DB in which the table belongs.
   * @param tableName table to which the write ID to be allocated
   * @throws TException
   */
  default List<TxnToWriteId> allocateTableWriteIdsBatch(List<Long> txnIds, String dbName, String tableName)
      throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support allocating table write IDs in batch");
  }

  /**
   * Allocate a per table write ID and associate it with the given transaction. Used by replication load task.
   * @param dbName name of DB in which the table belongs.
   * @param tableName table to which the write ID to be allocated
   * @param replPolicy Used by replication task to identify the source cluster.
   * @param srcTxnToWriteIdList List of txn to write id map sent from the source cluster.
   * @throws TException
   */
  default List<TxnToWriteId> replAllocateTableWriteIdsBatch(String dbName, String tableName, String replPolicy,
                                                    List<TxnToWriteId> srcTxnToWriteIdList) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support replicating allocating table write " +
         "IDs in batch");
  }

  /**
   * Get the maximum allocated writeId for the given table
   * @param dbName name of DB in which the table belongs.
   * @param tableName table from which the writeId is queried
   * @return the maximum allocated writeId
   * @throws TException
   */
  default long getMaxAllocatedWriteId(String dbName, String tableName) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting maximum allocated write IDs");
  }

  /**
   * Seed an ACID table with the given writeId. If the table already contains writes it will fail.
   * @param dbName name of DB in which the table belongs.
   * @param tableName table to which the writeId will be set
   * @param seedWriteId the start value of writeId
   * @throws TException
   */
  default void seedWriteId(String dbName, String tableName, long seedWriteId) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support seeding write IDs");
  }

  /**
   * Seed or increment the global txnId to the given value.
   * If the actual txnId is greater or equal than the seed value, it wil fail
   * @param seedTxnId The seed value for the next transactions
   * @throws TException
   */
  default void seedTxnId(long seedTxnId) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support seeding transaction IDs");
  }

  /**
   * Show the list of currently open transactions.  This is for use by "show transactions" in the
   * grammar, not for applications that want to find a list of current transactions to work with.
   * Those wishing the latter should call {@link #getValidTxns()}.
   * @return List of currently opened transactions, included aborted ones.
   * @throws TException
   */
  default GetOpenTxnsInfoResponse showTxns() throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support showing transactions");
  }

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
  default LockResponse lock(LockRequest request)
      throws NoSuchTxnException, TxnAbortedException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support locking");
  }

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
  default LockResponse checkLock(long lockid)
    throws NoSuchTxnException, TxnAbortedException, NoSuchLockException,
      TException {
    throw new UnsupportedOperationException("MetaStore client does not support checking locks");
  }

  /**
   * Unlock a set of locks.  This can only be called when the locks are not
   * associated with a transaction.
   * @param lockid lock id returned by
   * {@link #lock(org.apache.hadoop.hive.metastore.api.LockRequest)}
   * @throws NoSuchLockException if the requested lockid does not exist.
   * This can result from the lock timing out and being unlocked by the system.
   * @throws TxnOpenException if the locks are are associated with a
   * transaction.
   * @throws TException
   */
  default void unlock(long lockid)
      throws NoSuchLockException, TxnOpenException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support unlocking locks");
  }

  /**
   * Show all currently held and waiting locks.
   * @param showLocksRequest SHOW LOCK request
   * @return List of currently held and waiting locks.
   * @throws TException
   */
  default ShowLocksResponse showLocks(ShowLocksRequest showLocksRequest) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support showing locks");
  }

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
  default void heartbeat(long txnid, long lockid)
    throws NoSuchLockException, NoSuchTxnException, TxnAbortedException,
      TException {
    throw new UnsupportedOperationException("MetaStore client does not support heartbeating");
  }

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
  default HeartbeatTxnRangeResponse heartbeatTxnRange(long min, long max) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support heartbeating a range of transactions");
  }

  /**
   * Send a request to compact a table or partition.  This will not block until the compaction is
   * complete.  It will instead put a request on the queue for that table or partition to be
   * compacted.  No checking is done on the dbname, tableName, or partitionName to make sure they
   * refer to valid objects.  It is assumed this has already been done by the caller.  At most one
   * Compaction can be scheduled/running for any given resource at a time.
   * @param request The {@link CompactionRequest} object containing the details required to enqueue
   *                a compaction request.
   * @throws TException
   */
  default CompactionResponse compact2(CompactionRequest request) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support compacting tables or partitions");
  }

  /**
   * Get a list of all compactions.
   * @return List of all current compactions. This includes compactions waiting to happen,
   * in progress, and finished but waiting to clean the existing files.
   * @throws TException
   */
  default ShowCompactResponse showCompactions() throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support showing compactions");
  }
  
  /**
   * Get a list of compactions for the given request object.
   */
  default ShowCompactResponse showCompactions(ShowCompactRequest request) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support showing compactions with request");
  }
  
  /**
   * Submit a request for performing cleanup of output directory. This is particularly
   * useful for CTAS when the query fails after write and before creation of table.
   * @return Status of whether the request was successfully submitted. True indicates
   * the request was successfully submitted and false indicates failure of request submitted.
   * @param rqst Request containing the table directory which needs to be cleaned up.
   * @param highestWriteId The highest write ID that was used while writing the table directory.
   * @param txnId The transaction ID of the query.
   * @throws TException
   */
  default boolean submitForCleanup(CompactionRequest rqst, long highestWriteId,
                           long txnId) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support submitting for cleanup");
  }

  /**
   * Get one latest record of SUCCEEDED or READY_FOR_CLEANING compaction for a table/partition.
   * No checking is done on the dbname, tablename, or partitionname to make sure they refer to valid objects.
   * Is is assumed to be done by the caller.
   * Note that partition names should be supplied with the request for a partitioned table; otherwise,
   * no records will be returned.
   * @param request info on which compaction to retrieve
   * @return one latest compaction record for a non partitioned table or one latest record for each
   * partition specified by the request.
   * @throws TException
   */
  default GetLatestCommittedCompactionInfoResponse getLatestCommittedCompactionInfo(GetLatestCommittedCompactionInfoRequest request)
    throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting latest committed compaction info");
  }

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
  default void addDynamicPartitions(long txnId, long writeId, String dbName, String tableName, List<String> partNames,
                            DataOperationType operationType)
    throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support adding dynamic partitions");
  }

  /**
   * Performs the commit/rollback to the metadata storage for insert operator from external storage handler.
   * @param table table name
   * @param overwrite true if the insert is overwrite
   *
   * @throws MetaException
   */
  default void insertTable(Table table, boolean overwrite) throws MetaException {
    throw new UnsupportedOperationException("MetaStore client does not support inserting tables");
  }

  /**
   * Checks if there is a conflicting transaction
   * @param txnId
   * @return latest txnId in conflict
   */
  default long getLatestTxnIdInConflict(long txnId) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting latest transaction id " +
         "in conflict");
  }

  default GetDatabaseObjectsResponse get_databases_req(GetDatabaseObjectsRequest request) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting database objects");
  }

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
  default NotificationEventResponse getNextNotification(long lastEventId, int maxEvents,
                                                NotificationFilter filter) throws TException {
    return new NotificationEventResponse();
  }

  /**
   * Get the next set of notifications from the database.
   * @param request The {@link NotificationEventRequest} request to be sent to the server
   *                to fetch the next set of events.
   * @param allowGapsInEventIds If this flag is true, the returned event ids may contain
   *                            gaps in the event ids. This could happen if on the server
   *                            side some of the events since the requested eventId have
   *                            been garbage collected. If the flag is false, the method
   *                            will throw {@link MetaException} if the returned events
   *                            from the server are not in sequence from the requested
   *                            event id.
   * @param filter User provided filter to remove unwanted events.  If null, all events will be
   *               returned.
   * @return list of notifications, sorted by eventId.  It is guaranteed that the events are in
   * the order that the operations were done on the database.
   * @throws TException
   */
  @InterfaceAudience.LimitedPrivate({"HCatalog"})
  default NotificationEventResponse getNextNotification(NotificationEventRequest request,
      boolean allowGapsInEventIds, NotificationFilter filter) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting next notification with request");
  }

  /**
   * Get the last used notification event id.
   * @return last used id
   * @throws TException
   */
  @InterfaceAudience.LimitedPrivate({"HCatalog"})
  default CurrentNotificationEventId getCurrentNotificationEventId() throws TException {
    return new CurrentNotificationEventId();
  }

  /**
   * Get the number of events from given eventID for the input database.
   * @return number of events
   * @throws TException
   */
  @InterfaceAudience.LimitedPrivate({"HCatalog"})
  default NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst)
          throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting notification events count");
  }

  /**
   * Request that the metastore fire an event.  Currently this is only supported for DML
   * operations, since the metastore knows when DDL operations happen.
   * @param request
   * @return response, type depends on type of request
   * @throws TException
   */

  @InterfaceAudience.LimitedPrivate({"Apache Hive, HCatalog"})
  default FireEventResponse fireListenerEvent(FireEventRequest request) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support firing events");
  }

  /**
   * Add a event related to write operations in an ACID table.
   * @param rqst message containing information for acid write operation.
   * @throws TException
   */
  @InterfaceAudience.LimitedPrivate({"Apache Hive, HCatalog"})
  default void addWriteNotificationLog(WriteNotificationLogRequest rqst) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support adding write notification log");
  }

  /**
   * Add a batch of event related to write operations in an ACID table.
   * @param rqst message containing information for acid write operations.
   * @throws TException
   */
  @InterfaceAudience.LimitedPrivate({"Apache Hive, HCatalog"})
  default void addWriteNotificationLogInBatch(WriteNotificationLogBatchRequest rqst) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support adding write notification log in batch");
  }

  class IncompatibleMetastoreException extends MetaException {
    public IncompatibleMetastoreException(String message) {
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
  default GetPrincipalsInRoleResponse get_principals_in_role(GetPrincipalsInRoleRequest getPrincRoleReq)
      throws MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting principals in role");
  }


  /**
   * get all role-grants for roles that have been granted to given principal
   * Note that in the returned list of RolePrincipalGrants, the principal information
   * redundant as it would match the principal information in request
   * @param getRolePrincReq
   * @return
   * @throws MetaException
   * @throws TException
   */
  default GetRoleGrantsForPrincipalResponse get_role_grants_for_principal(
      GetRoleGrantsForPrincipalRequest getRolePrincReq) throws MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting role grants for principal");
  }

  /**
   * Get aggregated column stats for a set of partitions.
   * @param dbName database name
   * @param tblName table name
   * @param colNames list of column names
   * @param partName list of partition names (not values).
   * @param engine engine sending the request
   * @return aggregated stats for requested partitions
   * @throws NoSuchObjectException no such table
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport exception
   */
  default AggrStats getAggrColStatsFor(String dbName, String tblName,
      List<String> colNames, List<String> partName, String engine) 
      throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting aggregated column stats " +
        "for partitions");
  }

  default AggrStats getAggrColStatsFor(String dbName, String tblName, List<String> colNames, 
      List<String> partName, String engine, String writeIdList) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting aggregated column stats " +
        "for partitions with writeIdList");
  }

  /**
   * Get aggregated column stats for a set of partitions.
   * @param catName catalog name
   * @param dbName database name
   * @param tblName table name
   * @param colNames list of column names
   * @param partNames list of partition names (not values).
   * @param engine engine sending the request
   * @return aggregated stats for requested partitions
   * @throws NoSuchObjectException no such table
   * @throws MetaException error accessing the RDBMS
   * @throws TException thrift transport exception
   */
  default AggrStats getAggrColStatsFor(String catName, String dbName, String tblName,
                               List<String> colNames, List<String> partNames,
                               String engine)
      throws NoSuchObjectException, MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting aggregated column stats for " +
        "partitions with catalog name");
  }

  default AggrStats getAggrColStatsFor(String catName, String dbName, String tblName, List<String> colNames, 
      List<String> partNames, String engine, String writeIdList) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting aggregated column stats for " +
        "partitions with catalog name and writeIdList");
  }
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
  default boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException, InvalidInputException {
     throw new UnsupportedOperationException("MetaStore client does not support setting partition column statistics");
  }

  /**
   * Flush any catalog objects held by the metastore implementation.  Note that this does not
   * flush statistics objects.  This should be called at the beginning of each query.
   */
  default void flushCache() {}

  /**
   * Gets file metadata, as cached by metastore, for respective file IDs.
   * The metadata that is not cached in metastore may be missing.
   */
  default Iterable<Entry<Long, ByteBuffer>> getFileMetadata(List<Long> fileIds) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting file metadata");
  }

  default Iterable<Entry<Long, MetadataPpdResult>> getFileMetadataBySarg(
      List<Long> fileIds, ByteBuffer sarg, boolean doGetFooters) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting file metadata by sarg");
  }

  /**
   * Cleares the file metadata cache for respective file IDs.
   */
  default void clearFileMetadata(List<Long> fileIds) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support clearing file metadata");
  }

  /**
   * Adds file metadata for respective file IDs to metadata cache in metastore.
   */
  default void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support putting file metadata");
  }

  default boolean isSameConfObj(Configuration c) {
     throw new UnsupportedOperationException("MetaStore client does not support checking if the configuration object " +
         "is the same");
  }

  default boolean cacheFileMetadata(String dbName, String tableName, String partName,
      boolean allParts) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support caching file metadata");
  }

  /**
   * Get a primary key for a table.
   * @param request Request info
   * @return List of primary key columns
   * @throws MetaException error reading the RDBMS
   * @throws NoSuchObjectException no primary key exists on this table, or maybe no such table
   * @throws TException thrift transport error
   */
  default List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest request)
    throws MetaException, NoSuchObjectException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting primary keys");
  }

  /**
   * Get a foreign key for a table.
   * @param request Request info
   * @return List of foreign key columns
   * @throws MetaException error reading the RDBMS
   * @throws NoSuchObjectException no foreign key exists on this table, or maybe no such table
   * @throws TException thrift transport error
   */
  default List<SQLForeignKey> getForeignKeys(ForeignKeysRequest request) throws MetaException,
    NoSuchObjectException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting foreign keys");
  }

  /**
   * Get a unique constraint for a table.
   * @param request Request info
   * @return List of unique constraint columns
   * @throws MetaException error reading the RDBMS
   * @throws NoSuchObjectException no unique constraint on this table, or maybe no such table
   * @throws TException thrift transport error
   */
  default List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest request) throws MetaException,
    NoSuchObjectException, TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting unique constraints");
  }

  /**
   * Get a not null constraint for a table.
   * @param request Request info
   * @return List of not null constraint columns
   * @throws MetaException error reading the RDBMS
   * @throws NoSuchObjectException no not null constraint on this table, or maybe no such table
   * @throws TException thrift transport error
   */
  default List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest request) throws MetaException,
    NoSuchObjectException, TException {
    return Collections.emptyList();
  }

  default List<SQLDefaultConstraint> getDefaultConstraints(DefaultConstraintsRequest request) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting default constraints");
  }

  default List<SQLCheckConstraint> getCheckConstraints(CheckConstraintsRequest request) throws TException {
    return Collections.emptyList();
  }

  /**
   * Get all constraints of given table
   * @param request Request info
   * @return all constraints of this table
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws TException
   */
  default SQLAllTableConstraints getAllTableConstraints(AllTableConstraintsRequest request)
      throws MetaException, NoSuchObjectException, TException {
    return new SQLAllTableConstraints();
  }

  default void createTableWithConstraints(
    org.apache.hadoop.hive.metastore.api.Table tTbl,
    List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys,
    List<SQLUniqueConstraint> uniqueConstraints,
    List<SQLNotNullConstraint> notNullConstraints,
    List<SQLDefaultConstraint> defaultConstraints,
    List<SQLCheckConstraint> checkConstraints)
    throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support creating table with constraints");
  }

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
  default void dropConstraint(String dbName, String tableName, String constraintName)
      throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support dropping constraints");
  }

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
  default void dropConstraint(String catName, String dbName, String tableName, String constraintName)
      throws MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support dropping constraints with catalog name");
  }


  /**
   * Add a primary key.
   * @param primaryKeyCols Primary key columns.
   * @throws MetaException error reading or writing to the RDBMS or a primary key already exists
   * @throws NoSuchObjectException no such table exists
   * @throws TException thrift transport error
   */
  default void addPrimaryKey(List<SQLPrimaryKey> primaryKeyCols) throws
  MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support adding primary keys");
  }

  /**
   * Add a foreign key
   * @param foreignKeyCols Foreign key definition
   * @throws MetaException error reading or writing to the RDBMS or foreign key already exists
   * @throws NoSuchObjectException one of the tables in the foreign key does not exist.
   * @throws TException thrift transport error
   */
  default void addForeignKey(List<SQLForeignKey> foreignKeyCols) throws
  MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support adding foreign keys");
  }

  /**
   * Add a unique constraint
   * @param uniqueConstraintCols Unique constraint definition
   * @throws MetaException error reading or writing to the RDBMS or unique constraint already exists
   * @throws NoSuchObjectException no such table
   * @throws TException thrift transport error
   */
  default void addUniqueConstraint(List<SQLUniqueConstraint> uniqueConstraintCols) throws
  MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support adding unique constraints");
  }

  /**
   * Add a not null constraint
   * @param notNullConstraintCols Notnull constraint definition
   * @throws MetaException error reading or writing to the RDBMS or not null constraint already
   * exists
   * @throws NoSuchObjectException no such table
   * @throws TException thrift transport error
   */
  default void addNotNullConstraint(List<SQLNotNullConstraint> notNullConstraintCols) throws
  MetaException, NoSuchObjectException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support adding not null constraints");
  }

  default void addDefaultConstraint(List<SQLDefaultConstraint> defaultConstraints) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support adding default constraints");
  }

  default void addCheckConstraint(List<SQLCheckConstraint> checkConstraints) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support adding check constraints");
  }

  /**
   * Gets the unique id of the backing database instance used for storing metadata
   * @return unique id of the backing database instance
   * @throws MetaException if HMS is not able to fetch the UUID or if there are multiple UUIDs found in the database
   * @throws TException in case of Thrift errors
   */
  default String getMetastoreDbUuid() throws MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting metastore db uuid");
  }

  default void createResourcePlan(WMResourcePlan resourcePlan, String copyFromName) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support creating resource plans");
  }

  default WMFullResourcePlan getResourcePlan(String resourcePlanName, String ns) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting resource plans");
  }

  default List<WMResourcePlan> getAllResourcePlans(String ns) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting all resource plans");
  }

  default void dropResourcePlan(String resourcePlanName, String ns) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support dropping resource plans");
  }

  default WMFullResourcePlan alterResourcePlan(String resourcePlanName, String ns, WMNullableResourcePlan resourcePlan,
      boolean canActivateDisabled, boolean isForceDeactivate, boolean isReplace) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering resource plans");
  }

  default WMFullResourcePlan getActiveResourcePlan(String ns) throws TException {
    return new WMFullResourcePlan();
  }

  default WMValidateResourcePlanResponse validateResourcePlan(String resourcePlanName, String ns) throws TException {
    throw new UnsupportedOperationException("this method is not supported");
  }

  default void createWMTrigger(WMTrigger trigger) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support creating WM triggers");
  }

  default void alterWMTrigger(WMTrigger trigger) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering WM triggers");
  }

  default void dropWMTrigger(String resourcePlanName, String triggerName, String ns) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support dropping WM triggers");
  }

  default List<WMTrigger> getTriggersForResourcePlan(String resourcePlan, String ns) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting WM triggers for resource plans");
  }

  default void createWMPool(WMPool pool) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support creating WM pools");
  }

  default void alterWMPool(WMNullablePool pool, String poolPath) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering WM pools");
  }

  default void dropWMPool(String resourcePlanName, String poolPath, String ns)
      throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support dropping WM pools");
  }

  default void createOrUpdateWMMapping(WMMapping mapping, boolean isUpdate)
      throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support creating or updating WM mappings");
  }

  default void dropWMMapping(WMMapping mapping)
      throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support dropping WM mappings");
  }

  default void createOrDropTriggerToPoolMapping(String resourcePlanName, String triggerName,
      String poolPath, boolean shouldDrop, String ns) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support creating or dropping trigger " +
        "to pool mappings");
  }

  /**
   * Create a new schema.  This is really a schema container, as there will be specific versions
   * of the schema that have columns, etc.
   * @param schema schema to create
   * @throws AlreadyExistsException if a schema of this name already exists
   * @throws NoSuchObjectException database references by this schema does not exist
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  default void createISchema(ISchema schema) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support creating schemas");
  }

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
  default void alterISchema(String catName, String dbName, String schemaName, ISchema newSchema) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support altering schemas");
  }

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
  default ISchema getISchema(String catName, String dbName, String name) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting schemas");
  }

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
  default void dropISchema(String catName, String dbName, String name) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support dropping schemas");
  }

  /**
   * Add a new version to an existing schema.
   * @param schemaVersion version object to add
   * @throws AlreadyExistsException a version of this schema with the same version id already exists
   * @throws NoSuchObjectException no schema with this name could be found
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  default void addSchemaVersion(SchemaVersion schemaVersion) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support adding schema versions");
  }

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
  default SchemaVersion getSchemaVersion(String catName, String dbName, String schemaName, int version)
      throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting schema versions");
  }

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
  default SchemaVersion getSchemaLatestVersion(String catName, String dbName, String schemaName) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting latest schema version");
  }

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
  default List<SchemaVersion> getSchemaAllVersions(String catName, String dbName, String schemaName) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting all schema versions");
  }

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
  default void dropSchemaVersion(String catName, String dbName, String schemaName, int version) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support dropping schema versions");
  }

  /**
   * Find all schema versions that have columns that match a query.
   * @param rqst query, this can include column names, namespaces (actually stored in the
   *             description field in FieldSchema), and types.
   * @return The (possibly empty) list of schema name/version pairs that match.
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  default FindSchemasByColsResp getSchemaByCols(FindSchemasByColsRqst rqst) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting schemas by columns");
  }

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
  default void mapSchemaVersionToSerde(String catName, String dbName, String schemaName, int version, String serdeName) 
      throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support mapping schema versions to serdes");
  }

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
  default void setSchemaVersionState(String catName, String dbName, String schemaName, int version, 
      SchemaVersionState state) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support setting schema version state");
  }

  /**
   * Add a serde.  This is primarily intended for use with SchemaRegistry objects, since serdes
   * are automatically added when needed as part of creating and altering tables and partitions.
   * @param serDeInfo serde to add
   * @throws AlreadyExistsException serde of this name already exists
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  default void addSerDe(SerDeInfo serDeInfo) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support adding serdes");
  }

  /**
   * Fetch a serde.  This is primarily intended for use with SchemaRegistry objects, since serdes
   * are automatically fetched along with other information for tables and partitions.
   * @param serDeName name of the serde
   * @return the serde.
   * @throws NoSuchObjectException no serde with this name exists.
   * @throws MetaException general metastore error
   * @throws TException general thrift error
   */
  default SerDeInfo getSerDe(String serDeName) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting serdes");
  }

  /**
   * Acquire the materialization rebuild lock for a given view. We need to specify the fully
   * qualified name of the materialized view and the open transaction ID so we can identify
   * uniquely the lock.
   * @deprecated use lockMaterializationRebuild(LockMaterializationRebuildRequest rqst)
   * @param dbName db name for the materialized view
   * @param tableName table name for the materialized view
   * @param txnId transaction id for the rebuild
   * @return the response from the metastore, where the lock id is equal to the txn id and
   * the status can be either ACQUIRED or NOT ACQUIRED
   */
  @Deprecated
  default LockResponse lockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException {
    return lockMaterializationRebuild(new LockMaterializationRebuildRequest(Warehouse.DEFAULT_CATALOG_NAME,
        dbName, tableName, txnId));
  }

  /**
   * Acquire the materialization rebuild lock for a given view. We need to specify the fully
   * qualified name of the materialized view and the open transaction ID so we can identify
   * uniquely the lock.
   * @param rqst object of type LockMaterializationRebuildRequest
   * @return the response from the metastore, where the lock id is equal to the txn id and
   * the status can be either ACQUIRED or NOT ACQUIRED
   */
  default LockResponse lockMaterializationRebuild(LockMaterializationRebuildRequest rqst) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support acquiring materialization rebuild lock");
  }


  /**
   * Method to refresh the acquisition of a given materialization rebuild lock.
   * @deprecated use heartbeatLockMaterializationRebuild(LockMaterializationRebuildRequest rqst)
   * @param dbName db name for the materialized view
   * @param tableName table name for the materialized view
   * @param txnId transaction id for the rebuild
   * @return true if the lock could be renewed, false otherwise
   */
  @Deprecated
  default boolean heartbeatLockMaterializationRebuild(String dbName, String tableName, long txnId) throws TException {
    return heartbeatLockMaterializationRebuild(new LockMaterializationRebuildRequest(Warehouse.DEFAULT_CATALOG_NAME,
         dbName, tableName, txnId));
  }

  /**
   * Method to refresh the acquisition of a given materialization rebuild lock.
   * @param rqst object of type LockMaterializationRebuildRequest
   * @return true if the lock could be renewed, false otherwise
   */
  default boolean heartbeatLockMaterializationRebuild(LockMaterializationRebuildRequest rqst)
      throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support heartbeating materialization " +
        "rebuild lock");
  }

  /** Adds a RuntimeStat for metastore persistence. */
  default void addRuntimeStat(RuntimeStat stat) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support adding runtime stats");
  }

  /** Reads runtime statistics. */
  default List<RuntimeStat> getRuntimeStats(int maxWeight, int maxCreateTime) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support reading runtime stats");
  }

  /**
   * Generic Partition request API, providing different ways of filtering and controlling output.
   *
   * The API entry point is getPartitionsWithSpecs(), which is based on a single
   * request/response object model.
   *
   * The request (GetPartitionsRequest) defines any filtering that should be done for partitions
   * as well as the list of fields that should be returned (this is called ProjectionSpec).
   * Projection is simply a list of dot separated strings which represent the fields which should
   * be returned. Projection may also include whitelist or blacklist of parameters to include in
   * the partition. When both blacklist and whitelist are present, the blacklist supersedes the
   * whitelist in case of conflicts.
   *
   * Partition filter spec is the generalization of various types of partition filtering.
   * Partitions can be filtered by names, by values or by partition expressions.
   */
  default GetPartitionsResponse getPartitionsWithSpecs(GetPartitionsRequest request) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting partitions with specs");
  }

  /**
   * Get the next compaction job to do.
   * @param rqst Information about the worker id and version
   * @return next compaction job encapsulated in a {@link CompactionInfoStruct}.
   * @throws MetaException
   * @throws TException
   */
  default OptionalCompactionInfoStruct findNextCompact(FindNextCompactRequest rqst) throws MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support finding next compaction");
  }

  /**
   * Set the compaction highest write id.
   * @param cr compaction job being done.
   * @param txnId transaction id.
   * @throws TException
   */
  default void updateCompactorState(CompactionInfoStruct cr, long txnId) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support updating compactor state");
  }

  /**
   * Get columns.
   * @param cr compaction job.
   * @return
   * @throws TException
   */
  default List<String> findColumnsWithStats(CompactionInfoStruct cr) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support finding columns with stats");
  }

  /**
   * Mark a finished compaction as cleaned.
   * @param cr compaction job.
   * @throws MetaException
   * @throws TException
   */
  default void markCleaned(CompactionInfoStruct cr) throws MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support marking compaction as cleaned");
  }

  /**
   * Mark a finished compaction as compacted.
   * @param cr compaction job.
   * @throws MetaException
   * @throws TException
   */
  default void markCompacted(CompactionInfoStruct cr) throws MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support marking compaction as compacted");
  }

  /**
   * Mark a finished compaction as failed.
   * @param cr compaction job.
   * @throws MetaException
   * @throws TException
   */
  default void markFailed(CompactionInfoStruct cr) throws MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support marking compaction as failed");
  }

  /**
   * Mark a compaction as refused (to run).
   * @param cr compaction job.
   * @throws MetaException
   * @throws TException
   */
  default void markRefused(CompactionInfoStruct cr) throws MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support marking compaction as refused");
  }

  /**
   * Create, update or delete one record in the compaction metrics cache.
   * <p>
   * If the metric is not found in the metrics cache, it will be created.
   * </p>
   * <p>
   * If the metric is found, it will be updated. This operation uses an optimistic locking mechanism, meaning if another
   * operation changed the value of this metric, the update will abort and won't be retried.
   * </p>
   * <p>
   * If the new metric value is below {@link CompactionMetricsDataStruct#getThreshold()}, it will be deleted.
   * </p>
   * @param struct the object that is used for the update, always non-null
   * @return true, if update finished successfully
   * @throws MetaException
   * @throws TException
   */
  default boolean updateCompactionMetricsData(CompactionMetricsDataStruct struct) throws MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support updating compaction metrics data");
  }


  /**
   * Remove records from the compaction metrics cache matching the filter criteria passed in as parameters
   * @param request the request object, that contains the filter parameters, must be non-null
   * @throws MetaException
   * @throws TException
   */
  default void removeCompactionMetricsData(CompactionMetricsDataRequest request) throws MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support removing compaction metrics data");
  }
  /**
   * Set the hadoop id for a compaction.
   * @param jobId mapreduce job id that will do the compaction.
   * @param cqId compaction id.
   * @throws MetaException
   * @throws TException
   */
  default void setHadoopJobid(String jobId, long cqId) throws MetaException, TException {
    throw new UnsupportedOperationException("MetaStore client does not support setting hadoop job id for compaction");
  }

  /**
   * Gets the version string of the metastore server which this client is connected to
   *
   * @return String representation of the version number of Metastore server (eg: 3.1.0-SNAPSHOT)
   */
  default String getServerVersion() throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting server version");
  }

  /**
   * Returns details about a scheduled query by name.
   * 
   * @throws NoSuchObjectException if an object by the given name dosen't exists.
   */
  default ScheduledQuery getScheduledQuery(ScheduledQueryKey scheduleKey) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting scheduled query by key");
  }

  /**
   * Carries out maintenance of scheduled queries (insert/update/drop).
   */
  default void scheduledQueryMaintenance(ScheduledQueryMaintenanceRequest request) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support scheduled query maintenance");
  }

  /**
   * Checks whenever a query is available for execution.
   *
   * @return optionally a scheduled query to be processed.
   */
  default ScheduledQueryPollResponse scheduledQueryPoll(ScheduledQueryPollRequest request) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support scheduled query poll");
  }

  /**
   * Registers the progress a scheduled query being executed.
   */
  default void scheduledQueryProgress(ScheduledQueryProgressInfo info) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support scheduled query progress");
  }

  /**
   * Adds replication metrics for the replication policies.
   * @param replicationMetricList
   * @throws MetaException
   */
  default void addReplicationMetrics(ReplicationMetricList replicationMetricList) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support adding replication metrics");
  }

  default ReplicationMetricList getReplicationMetrics(GetReplicationMetricsRequest
                                                replicationMetricsRequest) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting replication metrics");
  }

  default void createStoredProcedure(StoredProcedure proc) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support creating stored procedures");
  }

  default StoredProcedure getStoredProcedure(StoredProcedureRequest request) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support getting stored procedures");
  }

  default void dropStoredProcedure(StoredProcedureRequest request) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support dropping stored procedures");
  }

  default List<String> getAllStoredProcedures(ListStoredProcedureRequest request) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting all stored procedures");
  }

  default void addPackage(AddPackageRequest request) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support adding packages");
  }

  default Package findPackage(GetPackageRequest request) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support finding packages");
  }

  default List<String> listPackages(ListPackageRequest request) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support listing packages");
  }

  default void dropPackage(DropPackageRequest request) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support dropping packages");
  }

  /**
   * Get acid write events of a specific transaction.
   * @throws TException
   */
  default List<WriteEventInfo> getAllWriteEventInfo(GetAllWriteEventInfoRequest request) throws TException {
     throw new UnsupportedOperationException("MetaStore client does not support getting write events");
  }

  default AbortCompactResponse abortCompactions(AbortCompactionRequest request) throws TException {
    throw new UnsupportedOperationException("MetaStore client does not support aborting compactions");
  }

  /**
   * Sets properties.
   * @param nameSpace the property store namespace
   * @param properties a map keyed by property path mapped to property values
   * @return true if successful, false otherwise
   * @throws TException
   */
  default boolean setProperties(String nameSpace, Map<String, String> properties) throws TException {
    throw new UnsupportedOperationException();
  }

  /**
   * Gets properties.
   * @param nameSpace the property store namespace.
   * @param mapPrefix the map prefix (ala starts-with) to select maps
   * @param mapPredicate predicate expression on properties to further reduce the selected maps
   * @param selection the list of properties to return, null for all
   * @return a map keyed by property map path to maps keyed by property name mapped to property values
   * @throws TException
   */
  default Map<String, Map<String, String>> getProperties(String nameSpace, String mapPrefix, 
      String mapPredicate, String... selection) throws TException {
    throw new UnsupportedOperationException();
  }
}
