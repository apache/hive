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

import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventsCountResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.WMNullablePool;
import org.apache.hadoop.hive.metastore.api.WMNullableResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
import org.apache.hadoop.hive.metastore.api.WMValidateResourcePlanResponse;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.ColStatsObjWithSourceInfo;
import org.apache.thrift.TException;

public interface RawStore extends Configurable {

  /***
   * Annotation to skip retries
   */
  @Target(value = ElementType.METHOD)
  @Retention(value = RetentionPolicy.RUNTIME)
  @interface CanNotRetry {
  }

  void shutdown();

  /**
   * Opens a new one or the one already created Every call of this function must
   * have corresponding commit or rollback function call
   *
   * @return an active transaction
   */

  boolean openTransaction();

  /**
   * if this is the commit of the first open call then an actual commit is
   * called.
   *
   * @return true or false
   */
  @CanNotRetry
  boolean commitTransaction();

  boolean isActiveTransaction();

  /**
   * Rolls back the current transaction if it is active
   */
  @CanNotRetry
  void rollbackTransaction();

  void createDatabase(Database db)
      throws InvalidObjectException, MetaException;

  Database getDatabase(String name)
      throws NoSuchObjectException;

  boolean dropDatabase(String dbname) throws NoSuchObjectException, MetaException;

  boolean alterDatabase(String dbname, Database db) throws NoSuchObjectException, MetaException;

  List<String> getDatabases(String pattern) throws MetaException;

  List<String> getAllDatabases() throws MetaException;

  boolean createType(Type type);

  Type getType(String typeName);

  boolean dropType(String typeName);

  void createTable(Table tbl) throws InvalidObjectException,
      MetaException;

  boolean dropTable(String dbName, String tableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException;

  Table getTable(String dbName, String tableName)
      throws MetaException;

  boolean addPartition(Partition part)
      throws InvalidObjectException, MetaException;

  boolean addPartitions(String dbName, String tblName, List<Partition> parts)
      throws InvalidObjectException, MetaException;

  boolean addPartitions(String dbName, String tblName, PartitionSpecProxy partitionSpec, boolean ifNotExists)
      throws InvalidObjectException, MetaException;

  Partition getPartition(String dbName, String tableName,
      List<String> part_vals) throws MetaException, NoSuchObjectException;

  boolean doesPartitionExist(String dbName, String tableName,
      List<String> part_vals) throws MetaException, NoSuchObjectException;

  boolean dropPartition(String dbName, String tableName,
      List<String> part_vals) throws MetaException, NoSuchObjectException, InvalidObjectException,
      InvalidInputException;

  List<Partition> getPartitions(String dbName,
      String tableName, int max) throws MetaException, NoSuchObjectException;

  void alterTable(String dbname, String name, Table newTable)
      throws InvalidObjectException, MetaException;

  void updateCreationMetadata(String dbname, String tablename, CreationMetadata cm)
      throws MetaException;

  List<String> getTables(String dbName, String pattern)
      throws MetaException;

  List<String> getTables(String dbName, String pattern, TableType tableType)
      throws MetaException;

  List<String> getMaterializedViewsForRewriting(String dbName)
      throws MetaException, NoSuchObjectException;

  List<TableMeta> getTableMeta(
      String dbNames, String tableNames, List<String> tableTypes) throws MetaException;

  /**
   * @param dbname
   *        The name of the database from which to retrieve the tables
   * @param tableNames
   *        The names of the tables to retrieve.
   * @return A list of the tables retrievable from the database
   *          whose names are in the list tableNames.
   *         If there are duplicate names, only one instance of the table will be returned
   * @throws MetaException
   */
  List<Table> getTableObjectsByName(String dbname, List<String> tableNames)
      throws MetaException, UnknownDBException;

  List<String> getAllTables(String dbName) throws MetaException;

  /**
   * Gets a list of tables based on a filter string and filter type.
   * @param dbName
   *          The name of the database from which you will retrieve the table names
   * @param filter
   *          The filter string
   * @param max_tables
   *          The maximum number of tables returned
   * @return  A list of table names that match the desired filter
   * @throws MetaException
   * @throws UnknownDBException
   */
  List<String> listTableNamesByFilter(String dbName,
      String filter, short max_tables) throws MetaException, UnknownDBException;

  List<String> listPartitionNames(String db_name,
      String tbl_name, short max_parts) throws MetaException;

  PartitionValuesResponse listPartitionValues(String db_name, String tbl_name,
                                              List<FieldSchema> cols, boolean applyDistinct, String filter, boolean ascending,
                                              List<FieldSchema> order, long maxParts) throws MetaException;

  List<String> listPartitionNamesByFilter(String db_name,
      String tbl_name, String filter, short max_parts) throws MetaException;

  void alterPartition(String db_name, String tbl_name, List<String> part_vals,
      Partition new_part) throws InvalidObjectException, MetaException;

  void alterPartitions(String db_name, String tbl_name,
      List<List<String>> part_vals_list, List<Partition> new_parts)
      throws InvalidObjectException, MetaException;

  boolean addIndex(Index index)
      throws InvalidObjectException, MetaException;

  Index getIndex(String dbName, String origTableName, String indexName) throws MetaException;

  boolean dropIndex(String dbName, String origTableName, String indexName) throws MetaException;

  List<Index> getIndexes(String dbName,
      String origTableName, int max) throws MetaException;

  List<String> listIndexNames(String dbName,
      String origTableName, short max) throws MetaException;

  void alterIndex(String dbname, String baseTblName, String name, Index newIndex)
      throws InvalidObjectException, MetaException;

  List<Partition> getPartitionsByFilter(
      String dbName, String tblName, String filter, short maxParts)
      throws MetaException, NoSuchObjectException;

  boolean getPartitionsByExpr(String dbName, String tblName,
      byte[] expr, String defaultPartitionName, short maxParts, List<Partition> result)
      throws TException;

  int getNumPartitionsByFilter(String dbName, String tblName, String filter)
    throws MetaException, NoSuchObjectException;

  int getNumPartitionsByExpr(String dbName, String tblName, byte[] expr) throws MetaException, NoSuchObjectException;

  List<Partition> getPartitionsByNames(
      String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException;

  Table markPartitionForEvent(String dbName, String tblName, Map<String,String> partVals, PartitionEventType evtType) throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException;

  boolean isPartitionMarkedForEvent(String dbName, String tblName, Map<String, String> partName, PartitionEventType evtType) throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException;

  boolean addRole(String rowName, String ownerName)
      throws InvalidObjectException, MetaException, NoSuchObjectException;

  boolean removeRole(String roleName) throws MetaException, NoSuchObjectException;

  boolean grantRole(Role role, String userName, PrincipalType principalType,
      String grantor, PrincipalType grantorType, boolean grantOption)
      throws MetaException, NoSuchObjectException, InvalidObjectException;

  boolean revokeRole(Role role, String userName, PrincipalType principalType,
      boolean grantOption) throws MetaException, NoSuchObjectException;

  PrincipalPrivilegeSet getUserPrivilegeSet(String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException;

  PrincipalPrivilegeSet getDBPrivilegeSet (String dbName, String userName,
      List<String> groupNames)  throws InvalidObjectException, MetaException;

  PrincipalPrivilegeSet getTablePrivilegeSet (String dbName, String tableName,
      String userName, List<String> groupNames) throws InvalidObjectException, MetaException;

  PrincipalPrivilegeSet getPartitionPrivilegeSet (String dbName, String tableName,
      String partition, String userName, List<String> groupNames) throws InvalidObjectException, MetaException;

  PrincipalPrivilegeSet getColumnPrivilegeSet (String dbName, String tableName, String partitionName,
      String columnName, String userName, List<String> groupNames) throws InvalidObjectException, MetaException;

  List<HiveObjectPrivilege> listPrincipalGlobalGrants(String principalName,
      PrincipalType principalType);

  List<HiveObjectPrivilege> listPrincipalDBGrants(String principalName,
      PrincipalType principalType, String dbName);

  List<HiveObjectPrivilege> listAllTableGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName);

  List<HiveObjectPrivilege> listPrincipalPartitionGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName, List<String> partValues, String partName);

  List<HiveObjectPrivilege> listPrincipalTableColumnGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName, String columnName);

  List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName, List<String> partValues, String partName, String columnName);

  boolean grantPrivileges (PrivilegeBag privileges)
      throws InvalidObjectException, MetaException, NoSuchObjectException;

  boolean revokePrivileges  (PrivilegeBag privileges, boolean grantOption)
  throws InvalidObjectException, MetaException, NoSuchObjectException;

  org.apache.hadoop.hive.metastore.api.Role getRole(
      String roleName) throws NoSuchObjectException;

  List<String> listRoleNames();

  List<Role> listRoles(String principalName,
      PrincipalType principalType);

  List<RolePrincipalGrant> listRolesWithGrants(String principalName,
                                                      PrincipalType principalType);


  /**
   * Get the role to principal grant mapping for given role
   * @param roleName
   * @return
   */
  List<RolePrincipalGrant> listRoleMembers(String roleName);


  Partition getPartitionWithAuth(String dbName, String tblName,
      List<String> partVals, String user_name, List<String> group_names)
      throws MetaException, NoSuchObjectException, InvalidObjectException;

  List<Partition> getPartitionsWithAuth(String dbName,
      String tblName, short maxParts, String userName, List<String> groupNames)
      throws MetaException, NoSuchObjectException, InvalidObjectException;

  /**
   * Lists partition names that match a given partial specification
   * @param db_name
   *          The name of the database which has the partitions
   * @param tbl_name
   *          The name of the table which has the partitions
   * @param part_vals
   *          A partial list of values for partitions in order of the table's partition keys.
   *          Entries can be empty if you only want to specify latter partitions.
   * @param max_parts
   *          The maximum number of partitions to return
   * @return A list of partition names that match the partial spec.
   * @throws MetaException
   * @throws NoSuchObjectException
   */
  List<String> listPartitionNamesPs(String db_name, String tbl_name,
      List<String> part_vals, short max_parts)
      throws MetaException, NoSuchObjectException;

  /**
   * Lists partitions that match a given partial specification and sets their auth privileges.
   *   If userName and groupNames null, then no auth privileges are set.
   * @param db_name
   *          The name of the database which has the partitions
   * @param tbl_name
   *          The name of the table which has the partitions
   * @param part_vals
   *          A partial list of values for partitions in order of the table's partition keys
   *          Entries can be empty if you need to specify latter partitions.
   * @param max_parts
   *          The maximum number of partitions to return
   * @param userName
   *          The user name for the partition for authentication privileges
   * @param groupNames
   *          The groupNames for the partition for authentication privileges
   * @return A list of partitions that match the partial spec.
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws InvalidObjectException
   */
  List<Partition> listPartitionsPsWithAuth(String db_name, String tbl_name,
      List<String> part_vals, short max_parts, String userName, List<String> groupNames)
      throws MetaException, InvalidObjectException, NoSuchObjectException;

  /** Persists the given column statistics object to the metastore
   * @param colStats object to persist
   * @return Boolean indicating the outcome of the operation
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws InvalidObjectException
   * @throws InvalidInputException
   */
  boolean updateTableColumnStatistics(ColumnStatistics colStats)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  /** Persists the given column statistics object to the metastore
   * @param partVals
   *
   * @param statsObj object to persist
   * @return Boolean indicating the outcome of the operation
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws InvalidObjectException
   * @throws InvalidInputException
   */
  boolean updatePartitionColumnStatistics(ColumnStatistics statsObj,
     List<String> partVals)
     throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  /**
   * Returns the relevant column statistics for a given column in a given table in a given database
   * if such statistics exist.
   *
   * @param dbName name of the database, defaults to current database
   * @param tableName name of the table
   * @param colName names of the columns for which statistics is requested
   * @return Relevant column statistics for the column for the given table
   * @throws NoSuchObjectException
   * @throws MetaException
   *
   */
  ColumnStatistics getTableColumnStatistics(String dbName, String tableName,
    List<String> colName) throws MetaException, NoSuchObjectException;

  /**
   * Returns the relevant column statistics for given columns in given partitions in a given
   * table in a given database if such statistics exist.
   */
  List<ColumnStatistics> getPartitionColumnStatistics(
     String dbName, String tblName, List<String> partNames, List<String> colNames)
      throws MetaException, NoSuchObjectException;

  /**
   * Deletes column statistics if present associated with a given db, table, partition and col. If
   * null is passed instead of a colName, stats when present for all columns associated
   * with a given db, table and partition are deleted.
   *
   * @param dbName
   * @param tableName
   * @param partName
   * @param partVals
   * @param colName
   * @return Boolean indicating the outcome of the operation
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws InvalidObjectException
   * @throws InvalidInputException
   */

  boolean deletePartitionColumnStatistics(String dbName, String tableName,
      String partName, List<String> partVals, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  /**
   * Deletes column statistics if present associated with a given db, table and col. If
   * null is passed instead of a colName, stats when present for all columns associated
   * with a given db and table are deleted.
   *
   * @param dbName
   * @param tableName
   * @param colName
   * @return Boolean indicating the outcome of the operation
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws InvalidObjectException
   * @throws InvalidInputException
   */

  boolean deleteTableColumnStatistics(String dbName, String tableName,
    String colName)
    throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  long cleanupEvents();

  boolean addToken(String tokenIdentifier, String delegationToken);

  boolean removeToken(String tokenIdentifier);

  String getToken(String tokenIdentifier);

  List<String> getAllTokenIdentifiers();

  int addMasterKey(String key) throws MetaException;

  void updateMasterKey(Integer seqNo, String key)
     throws NoSuchObjectException, MetaException;

  boolean removeMasterKey(Integer keySeq);

  String[] getMasterKeys();

  void verifySchema() throws MetaException;

  String getMetaStoreSchemaVersion() throws  MetaException;

  abstract void setMetaStoreSchemaVersion(String version, String comment) throws MetaException;

  void dropPartitions(String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException;

  List<HiveObjectPrivilege> listPrincipalDBGrantsAll(
      String principalName, PrincipalType principalType);

  List<HiveObjectPrivilege> listPrincipalTableGrantsAll(
      String principalName, PrincipalType principalType);

  List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(
      String principalName, PrincipalType principalType);

  List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(
      String principalName, PrincipalType principalType);

  List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(
      String principalName, PrincipalType principalType);

  List<HiveObjectPrivilege> listGlobalGrantsAll();

  List<HiveObjectPrivilege> listDBGrantsAll(String dbName);

  List<HiveObjectPrivilege> listPartitionColumnGrantsAll(
      String dbName, String tableName, String partitionName, String columnName);

  List<HiveObjectPrivilege> listTableGrantsAll(String dbName, String tableName);

  List<HiveObjectPrivilege> listPartitionGrantsAll(
      String dbName, String tableName, String partitionName);

  List<HiveObjectPrivilege> listTableColumnGrantsAll(
      String dbName, String tableName, String columnName);

  /**
   * Register a user-defined function based on the function specification passed in.
   * @param func
   * @throws InvalidObjectException
   * @throws MetaException
   */
  void createFunction(Function func)
      throws InvalidObjectException, MetaException;

  /**
   * Alter function based on new function specs.
   * @param dbName
   * @param funcName
   * @param newFunction
   * @throws InvalidObjectException
   * @throws MetaException
   */
  void alterFunction(String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException;

  /**
   * Drop a function definition.
   * @param dbName
   * @param funcName
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws InvalidObjectException
   * @throws InvalidInputException
   */
  void dropFunction(String dbName, String funcName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException;

  /**
   * Retrieve function by name.
   * @param dbName
   * @param funcName
   * @return
   * @throws MetaException
   */
  Function getFunction(String dbName, String funcName) throws MetaException;

  /**
   * Retrieve all functions.
   * @return
   * @throws MetaException
   */
  List<Function> getAllFunctions() throws MetaException;

  /**
   * Retrieve list of function names based on name pattern.
   * @param dbName
   * @param pattern
   * @return
   * @throws MetaException
   */
  List<String> getFunctions(String dbName, String pattern) throws MetaException;

  AggrStats get_aggr_stats_for(String dbName, String tblName,
    List<String> partNames, List<String> colNames) throws MetaException, NoSuchObjectException;

  /**
   * Get column stats for all partitions of all tables in the database
   *
   * @param dbName
   * @return List of column stats objects for all partitions of all tables in the database
   * @throws MetaException
   * @throws NoSuchObjectException
   */
  List<ColStatsObjWithSourceInfo> getPartitionColStatsForDatabase(String dbName)
      throws MetaException, NoSuchObjectException;

  /**
   * Get the next notification event.
   * @param rqst Request containing information on the last processed notification.
   * @return list of notifications, sorted by eventId
   */
  NotificationEventResponse getNextNotification(NotificationEventRequest rqst);


  /**
   * Add a notification entry.  This should only be called from inside the metastore
   * @param event the notification to add
   */
  void addNotificationEvent(NotificationEvent event);

  /**
   * Remove older notification events.
   * @param olderThan Remove any events older than a given number of seconds
   */
  void cleanNotificationEvents(int olderThan);

  /**
   * Get the last issued notification event id.  This is intended for use by the export command
   * so that users can determine the state of the system at the point of the export,
   * and determine which notification events happened before or after the export.
   * @return
   */
  CurrentNotificationEventId getCurrentNotificationEventId();

  /**
   * Get the number of events corresponding to given database with fromEventId.
   * This is intended for use by the repl commands to track the progress of incremental dump.
   * @return
   */
  public NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst);

  /*
   * Flush any catalog objects held by the metastore implementation.  Note that this does not
   * flush statistics objects.  This should be called at the beginning of each query.
   */
  void flushCache();

  /**
   * @param fileIds List of file IDs from the filesystem.
   * @return File metadata buffers from file metadata cache. The array is fileIds-sized, and
   *         the entries (or nulls, if metadata is not in cache) correspond to fileIds in the list
   */
  ByteBuffer[] getFileMetadata(List<Long> fileIds) throws MetaException;

  /**
   * @param fileIds List of file IDs from the filesystem.
   * @param metadata Metadata buffers corresponding to fileIds in the list.
   * @param type The type; determines the class that can do additiona processing for metadata.
   */
  void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata,
      FileMetadataExprType type) throws MetaException;

  /**
   * @return Whether file metadata cache is supported by this implementation.
   */
  boolean isFileMetadataSupported();

  /**
   * Gets file metadata from cache after applying a format-specific expression that can
   * produce additional information based on file metadata and also filter the file list.
   * @param fileIds List of file IDs from the filesystem.
   * @param expr Format-specific serialized expression applicable to the files' metadatas.
   * @param type Expression type; used to determine the class that handles the metadata.
   * @param metadatas Output parameter; fileIds-sized array to receive the metadatas
   *                  for corresponding files, if any.
   * @param exprResults Output parameter; fileIds-sized array to receive the format-specific
   *                    expression results for the corresponding files.
   * @param eliminated Output parameter; fileIds-sized array to receive the indication of whether
   *                   the corresponding files are entirely eliminated by the expression.
   */
  void getFileMetadataByExpr(List<Long> fileIds, FileMetadataExprType type, byte[] expr,
      ByteBuffer[] metadatas, ByteBuffer[] exprResults, boolean[] eliminated)
          throws MetaException;

  /** Gets file metadata handler for the corresponding type. */
  FileMetadataHandler getFileMetadataHandler(FileMetadataExprType type);

  /**
   * Gets total number of tables.
   */
  @InterfaceStability.Evolving
  int getTableCount() throws MetaException;

  /**
   * Gets total number of partitions.
   */
  @InterfaceStability.Evolving
  int getPartitionCount() throws MetaException;

  /**
   * Gets total number of databases.
   */
  @InterfaceStability.Evolving
  int getDatabaseCount() throws MetaException;

  List<SQLPrimaryKey> getPrimaryKeys(String db_name,
    String tbl_name) throws MetaException;

  /**
   * Get the foreign keys for a table.  All foreign keys for a particular table can be fetched by
   * passing null for the last two arguments.
   * @param parent_db_name Database the table referred to is in.  This can be null to match all
   *                       databases.
   * @param parent_tbl_name Table that is referred to.  This can be null to match all tables.
   * @param foreign_db_name Database the table with the foreign key is in.
   * @param foreign_tbl_name Table with the foreign key.
   * @return List of all matching foreign key columns.  Note that if more than one foreign key
   * matches the arguments the results here will be all mixed together into a single list.
   * @throws MetaException if something goes wrong.
   */
  List<SQLForeignKey> getForeignKeys(String parent_db_name,
    String parent_tbl_name, String foreign_db_name, String foreign_tbl_name)
    throws MetaException;

  List<SQLUniqueConstraint> getUniqueConstraints(String db_name,
    String tbl_name) throws MetaException;

  List<SQLNotNullConstraint> getNotNullConstraints(String db_name,
    String tbl_name) throws MetaException;

  List<String> createTableWithConstraints(Table tbl, List<SQLPrimaryKey> primaryKeys,
    List<SQLForeignKey> foreignKeys, List<SQLUniqueConstraint> uniqueConstraints,
    List<SQLNotNullConstraint> notNullConstraints) throws InvalidObjectException, MetaException;

  void dropConstraint(String dbName, String tableName, String constraintName) throws NoSuchObjectException;

  List<String> addPrimaryKeys(List<SQLPrimaryKey> pks) throws InvalidObjectException, MetaException;

  List<String> addForeignKeys(List<SQLForeignKey> fks) throws InvalidObjectException, MetaException;

  List<String> addUniqueConstraints(List<SQLUniqueConstraint> uks) throws InvalidObjectException, MetaException;

  List<String> addNotNullConstraints(List<SQLNotNullConstraint> nns) throws InvalidObjectException, MetaException;

  /**
   * Gets the unique id of the backing datastore for the metadata
   * @return
   * @throws MetaException
   */
  String getMetastoreDbUuid() throws MetaException;

  void createResourcePlan(WMResourcePlan resourcePlan, String copyFrom, int defaultPoolSize)
      throws AlreadyExistsException, MetaException, InvalidObjectException, NoSuchObjectException;

  WMFullResourcePlan getResourcePlan(String name) throws NoSuchObjectException, MetaException;

  List<WMResourcePlan> getAllResourcePlans() throws MetaException;

  WMFullResourcePlan alterResourcePlan(String name, WMNullableResourcePlan resourcePlan,
      boolean canActivateDisabled, boolean canDeactivate, boolean isReplace)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
          MetaException;

  WMFullResourcePlan getActiveResourcePlan() throws MetaException;

  WMValidateResourcePlanResponse validateResourcePlan(String name)
      throws NoSuchObjectException, InvalidObjectException, MetaException;

  void dropResourcePlan(String name) throws NoSuchObjectException, MetaException;

  void createWMTrigger(WMTrigger trigger)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
          MetaException;

  void alterWMTrigger(WMTrigger trigger)
      throws NoSuchObjectException, InvalidOperationException, MetaException;

  void dropWMTrigger(String resourcePlanName, String triggerName)
      throws NoSuchObjectException, InvalidOperationException, MetaException;

  List<WMTrigger> getTriggersForResourcePlan(String resourcePlanName)
      throws NoSuchObjectException, MetaException;

  void createPool(WMPool pool) throws AlreadyExistsException, NoSuchObjectException,
      InvalidOperationException, MetaException;

  void alterPool(WMNullablePool pool, String poolPath) throws AlreadyExistsException,
      NoSuchObjectException, InvalidOperationException, MetaException;

  void dropWMPool(String resourcePlanName, String poolPath)
      throws NoSuchObjectException, InvalidOperationException, MetaException;

  void createOrUpdateWMMapping(WMMapping mapping, boolean update)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
          MetaException;

  void dropWMMapping(WMMapping mapping)
      throws NoSuchObjectException, InvalidOperationException, MetaException;

  void createWMTriggerToPoolMapping(String resourcePlanName, String triggerName, String poolPath)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
          MetaException;

  void dropWMTriggerToPoolMapping(String resourcePlanName, String triggerName, String poolPath)
      throws NoSuchObjectException, InvalidOperationException, MetaException;
}
