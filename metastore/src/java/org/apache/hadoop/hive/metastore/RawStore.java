/**
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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionEventType;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.model.MDBPrivilege;
import org.apache.hadoop.hive.metastore.model.MGlobalPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionPrivilege;
import org.apache.hadoop.hive.metastore.model.MRoleMap;
import org.apache.hadoop.hive.metastore.model.MTableColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MTablePrivilege;
import org.apache.thrift.TException;

public interface RawStore extends Configurable {

  /***
   * Annotation to skip retries
   */
  @Target(value = ElementType.METHOD)
  @Retention(value = RetentionPolicy.RUNTIME)
  public @interface CanNotRetry {
  }

  public abstract void shutdown();

  /**
   * Opens a new one or the one already created Every call of this function must
   * have corresponding commit or rollback function call
   *
   * @return an active transaction
   */

  public abstract boolean openTransaction();

  /**
   * if this is the commit of the first open call then an actual commit is
   * called.
   *
   * @return true or false
   */
  @CanNotRetry
  public abstract boolean commitTransaction();

  /**
   * Rolls back the current transaction if it is active
   */
  @CanNotRetry
  public abstract void rollbackTransaction();

  public abstract void createDatabase(Database db)
      throws InvalidObjectException, MetaException;

  public abstract Database getDatabase(String name)
      throws NoSuchObjectException;

  public abstract boolean dropDatabase(String dbname) throws NoSuchObjectException, MetaException;

  public abstract boolean alterDatabase(String dbname, Database db) throws NoSuchObjectException, MetaException;

  public abstract List<String> getDatabases(String pattern) throws MetaException;

  public abstract List<String> getAllDatabases() throws MetaException;

  public abstract boolean createType(Type type);

  public abstract Type getType(String typeName);

  public abstract boolean dropType(String typeName);

  public abstract void createTable(Table tbl) throws InvalidObjectException,
      MetaException;

  public abstract boolean dropTable(String dbName, String tableName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException;

  public abstract Table getTable(String dbName, String tableName)
      throws MetaException;

  public abstract boolean addPartition(Partition part)
      throws InvalidObjectException, MetaException;

  public abstract boolean addPartitions(String dbName, String tblName, List<Partition> parts)
      throws InvalidObjectException, MetaException;

  public abstract Partition getPartition(String dbName, String tableName,
      List<String> part_vals) throws MetaException, NoSuchObjectException;

  public abstract boolean doesPartitionExist(String dbName, String tableName,
      List<String> part_vals) throws MetaException, NoSuchObjectException;

  public abstract boolean dropPartition(String dbName, String tableName,
      List<String> part_vals) throws MetaException, NoSuchObjectException, InvalidObjectException,
      InvalidInputException;

  public abstract List<Partition> getPartitions(String dbName,
      String tableName, int max) throws MetaException, NoSuchObjectException;

  public abstract void alterTable(String dbname, String name, Table newTable)
      throws InvalidObjectException, MetaException;

  public List<String> getTables(String dbName, String pattern)
      throws MetaException;

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
  public List<Table> getTableObjectsByName(String dbname, List<String> tableNames)
      throws MetaException, UnknownDBException;

  public List<String> getAllTables(String dbName) throws MetaException;

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
  public abstract List<String> listTableNamesByFilter(String dbName,
      String filter, short max_tables) throws MetaException, UnknownDBException;

  public abstract List<String> listPartitionNames(String db_name,
      String tbl_name, short max_parts) throws MetaException;

  public abstract List<String> listPartitionNamesByFilter(String db_name,
      String tbl_name, String filter, short max_parts) throws MetaException;

  public abstract void alterPartition(String db_name, String tbl_name, List<String> part_vals,
      Partition new_part) throws InvalidObjectException, MetaException;

  public abstract void alterPartitions(String db_name, String tbl_name,
      List<List<String>> part_vals_list, List<Partition> new_parts)
      throws InvalidObjectException, MetaException;

  public abstract boolean addIndex(Index index)
      throws InvalidObjectException, MetaException;

  public abstract Index getIndex(String dbName, String origTableName, String indexName) throws MetaException;

  public abstract boolean dropIndex(String dbName, String origTableName, String indexName) throws MetaException;

  public abstract List<Index> getIndexes(String dbName,
      String origTableName, int max) throws MetaException;

  public abstract List<String> listIndexNames(String dbName,
      String origTableName, short max) throws MetaException;

  public abstract void alterIndex(String dbname, String baseTblName, String name, Index newIndex)
      throws InvalidObjectException, MetaException;

  public abstract List<Partition> getPartitionsByFilter(
      String dbName, String tblName, String filter, short maxParts)
      throws MetaException, NoSuchObjectException;

  public abstract boolean getPartitionsByExpr(String dbName, String tblName,
      byte[] expr, String defaultPartitionName, short maxParts, List<Partition> result)
      throws TException;

  public abstract List<Partition> getPartitionsByNames(
      String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException;

  public abstract Table markPartitionForEvent(String dbName, String tblName, Map<String,String> partVals, PartitionEventType evtType) throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException;

  public abstract boolean isPartitionMarkedForEvent(String dbName, String tblName, Map<String, String> partName, PartitionEventType evtType) throws MetaException, UnknownTableException, InvalidPartitionException, UnknownPartitionException;

  public abstract boolean addRole(String rowName, String ownerName)
      throws InvalidObjectException, MetaException, NoSuchObjectException;

  public abstract boolean removeRole(String roleName) throws MetaException, NoSuchObjectException;

  public abstract boolean grantRole(Role role, String userName, PrincipalType principalType,
      String grantor, PrincipalType grantorType, boolean grantOption)
      throws MetaException, NoSuchObjectException, InvalidObjectException;

  public abstract boolean revokeRole(Role role, String userName, PrincipalType principalType)
      throws MetaException, NoSuchObjectException;

  public abstract PrincipalPrivilegeSet getUserPrivilegeSet(String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException;

  public abstract PrincipalPrivilegeSet getDBPrivilegeSet (String dbName, String userName,
      List<String> groupNames)  throws InvalidObjectException, MetaException;

  public abstract PrincipalPrivilegeSet getTablePrivilegeSet (String dbName, String tableName,
      String userName, List<String> groupNames) throws InvalidObjectException, MetaException;

  public abstract PrincipalPrivilegeSet getPartitionPrivilegeSet (String dbName, String tableName,
      String partition, String userName, List<String> groupNames) throws InvalidObjectException, MetaException;

  public abstract PrincipalPrivilegeSet getColumnPrivilegeSet (String dbName, String tableName, String partitionName,
      String columnName, String userName, List<String> groupNames) throws InvalidObjectException, MetaException;

  public abstract List<MGlobalPrivilege> listPrincipalGlobalGrants(String principalName,
      PrincipalType principalType);

  public abstract List<MDBPrivilege> listPrincipalDBGrants(String principalName,
      PrincipalType principalType, String dbName);

  public abstract List<MTablePrivilege> listAllTableGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName);

  public abstract List<MPartitionPrivilege> listPrincipalPartitionGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName, String partName);

  public abstract List<MTableColumnPrivilege> listPrincipalTableColumnGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName, String columnName);

  public abstract List<MPartitionColumnPrivilege> listPrincipalPartitionColumnGrants(
      String principalName, PrincipalType principalType, String dbName,
      String tableName, String partName, String columnName);

  public abstract boolean grantPrivileges (PrivilegeBag privileges)
      throws InvalidObjectException, MetaException, NoSuchObjectException;

  public abstract boolean revokePrivileges  (PrivilegeBag privileges)
  throws InvalidObjectException, MetaException, NoSuchObjectException;

  public abstract org.apache.hadoop.hive.metastore.api.Role getRole(
      String roleName) throws NoSuchObjectException;

  public List<String> listRoleNames();

  public List<MRoleMap> listRoles(String principalName,
      PrincipalType principalType);


  /**
   * Get the role to principal grant mapping for given role
   * @param roleName
   * @return
   */
  public List<MRoleMap> listRoleMembers(String roleName);


  public abstract Partition getPartitionWithAuth(String dbName, String tblName,
      List<String> partVals, String user_name, List<String> group_names)
      throws MetaException, NoSuchObjectException, InvalidObjectException;

  public abstract List<Partition> getPartitionsWithAuth(String dbName,
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
  public abstract List<String> listPartitionNamesPs(String db_name, String tbl_name,
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
  public abstract List<Partition> listPartitionsPsWithAuth(String db_name, String tbl_name,
      List<String> part_vals, short max_parts, String userName, List<String> groupNames)
      throws MetaException, InvalidObjectException, NoSuchObjectException;

  /** Persists the given column statistics object to the metastore
   * @param partVals
   *
   * @param ColumnStats object to persist
   * @param List of partVals
   * @return Boolean indicating the outcome of the operation
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws InvalidObjectException
   * @throws InvalidInputException
   */
  public abstract boolean updateTableColumnStatistics(ColumnStatistics colStats)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  /** Persists the given column statistics object to the metastore
   * @param partVals
   *
   * @param ColumnStats object to persist
   * @param List of partVals
   * @return Boolean indicating the outcome of the operation
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws InvalidObjectException
   * @throws InvalidInputException
   */
  public abstract boolean updatePartitionColumnStatistics(ColumnStatistics statsObj,
     List<String> partVals)
     throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  /**
   * Returns the relevant column statistics for a given column in a given table in a given database
   * if such statistics exist.
   *
   * @param The name of the database, defaults to current database
   * @param The name of the table
   * @param The name of the column for which statistics is requested
   * @return Relevant column statistics for the column for the given table
   * @throws NoSuchObjectException
   * @throws MetaException
   * @throws InvalidInputException
   *
   */
  public abstract ColumnStatistics getTableColumnStatistics(String dbName, String tableName,
    List<String> colName) throws MetaException, NoSuchObjectException;

  /**
   * Returns the relevant column statistics for given columns in given partitions in a given
   * table in a given database if such statistics exist.
   */
  public abstract List<ColumnStatistics> getPartitionColumnStatistics(
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

  public abstract boolean deletePartitionColumnStatistics(String dbName, String tableName,
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

  public abstract boolean deleteTableColumnStatistics(String dbName, String tableName,
    String colName)
    throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException;

  public abstract long cleanupEvents();

  public abstract boolean addToken(String tokenIdentifier, String delegationToken);

  public abstract boolean removeToken(String tokenIdentifier);

  public abstract String getToken(String tokenIdentifier);

  public abstract List<String> getAllTokenIdentifiers();

  public abstract int addMasterKey(String key) throws MetaException;

  public abstract void updateMasterKey(Integer seqNo, String key)
     throws NoSuchObjectException, MetaException;

  public abstract boolean removeMasterKey(Integer keySeq);

  public abstract String[] getMasterKeys();

  public abstract void verifySchema() throws MetaException;

  public abstract String getMetaStoreSchemaVersion() throws  MetaException;

  public abstract void setMetaStoreSchemaVersion(String version, String comment) throws MetaException;

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
  public void createFunction(Function func)
      throws InvalidObjectException, MetaException;

  /**
   * Alter function based on new function specs.
   * @param dbName
   * @param name
   * @param newFunction
   * @throws InvalidObjectException
   * @throws MetaException
   */
  public void alterFunction(String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException;

  /**
   * Drop a function definition.
   * @param dbName
   * @param functionName
   * @return
   * @throws MetaException
   * @throws NoSuchObjectException
   * @throws InvalidObjectException
   * @throws InvalidInputException
   */
  public void dropFunction(String dbName, String funcName)
      throws MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException;

  /**
   * Retrieve function by name.
   * @param dbName
   * @param functionName
   * @return
   * @throws MetaException
   */
  public Function getFunction(String dbName, String funcName) throws MetaException;

  /**
   * Retrieve list of function names based on name pattern.
   * @param dbName
   * @param pattern
   * @return
   * @throws MetaException
   */
  public List<String> getFunctions(String dbName, String pattern) throws MetaException;

}
