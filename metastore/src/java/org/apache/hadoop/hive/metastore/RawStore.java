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

import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.model.MDBPrivilege;
import org.apache.hadoop.hive.metastore.model.MGlobalPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MPartitionPrivilege;
import org.apache.hadoop.hive.metastore.model.MRoleMap;
import org.apache.hadoop.hive.metastore.model.MTableColumnPrivilege;
import org.apache.hadoop.hive.metastore.model.MTablePrivilege;

public interface RawStore extends Configurable {

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
  public abstract boolean commitTransaction();

  /**
   * Rolls back the current transaction if it is active
   */
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
      throws MetaException;

  public abstract Table getTable(String dbName, String tableName)
      throws MetaException;

  public abstract boolean addPartition(Partition part)
      throws InvalidObjectException, MetaException;

  public abstract Partition getPartition(String dbName, String tableName,
      List<String> part_vals) throws MetaException, NoSuchObjectException;

  public abstract boolean dropPartition(String dbName, String tableName,
      List<String> part_vals) throws MetaException;

  public abstract List<Partition> getPartitions(String dbName,
      String tableName, int max) throws MetaException;

  public abstract void alterTable(String dbname, String name, Table newTable)
      throws InvalidObjectException, MetaException;

  public List<String> getTables(String dbName, String pattern)
      throws MetaException;

  public List<String> getAllTables(String dbName) throws MetaException;

  public abstract List<String> listPartitionNames(String db_name,
      String tbl_name, short max_parts) throws MetaException;

  public abstract List<String> listPartitionNamesByFilter(String db_name,
      String tbl_name, String filter, short max_parts) throws MetaException;

  public abstract void alterPartition(String db_name, String tbl_name,
      Partition new_part) throws InvalidObjectException, MetaException;

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
  
  public abstract Partition getPartitionWithAuth(String dbName, String tblName,
      List<String> partVals, String user_name, List<String> group_names)
      throws MetaException, NoSuchObjectException, InvalidObjectException;

  public abstract List<Partition> getPartitionsWithAuth(String dbName,
      String tblName, short maxParts, String userName, List<String> groupNames)
      throws MetaException, NoSuchObjectException, InvalidObjectException;;

}
