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

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.metastore.MetaDescriptor;
import org.apache.hadoop.hive.metastore.metastore.impl.PrivilegeStoreImpl;
import org.apache.hadoop.hive.metastore.model.MDBPrivilege;
import org.apache.hadoop.hive.metastore.model.MDCPrivilege;

@MetaDescriptor(alias = "privilege", defaultImpl = PrivilegeStoreImpl.class)
public interface PrivilegeStore {
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

  /**
   * Get privileges for a database for a user.
   * @param catName catalog name
   * @param dbName database name
   * @param userName user name
   * @param groupNames list of groups the user is in
   * @return privileges for that user on indicated database
   * @throws InvalidObjectException no such database
   * @throws MetaException error accessing the RDBMS
   */
  PrincipalPrivilegeSet getDBPrivilegeSet (String catName, String dbName, String userName,
      List<String> groupNames)  throws InvalidObjectException, MetaException;

  /**
   * Get privileges for a connector for a user.
   * @param catName catalog name
   * @param connectorName connector name
   * @param userName user name
   * @param groupNames list of groups the user is in
   * @return privileges for that user on indicated connector
   * @throws InvalidObjectException no such database
   * @throws MetaException error accessing the RDBMS
   */
  PrincipalPrivilegeSet getConnectorPrivilegeSet (String catName, String connectorName, String userName,
      List<String> groupNames)  throws InvalidObjectException, MetaException;

  /**
   * Get privileges for a table for a user.
   * @param tableName table name
   * @param userName user name
   * @param groupNames list of groups the user is in
   * @return privileges for that user on indicated table
   * @throws InvalidObjectException no such table
   * @throws MetaException error accessing the RDBMS
   */
  PrincipalPrivilegeSet getTablePrivilegeSet (TableName tableName,
      String userName, List<String> groupNames) throws InvalidObjectException, MetaException;

  /**
   * Get privileges for a partition for a user.
   * @param tableName table name
   * @param partition partition name
   * @param userName user name
   * @param groupNames list of groups the user is in
   * @return privileges for that user on indicated partition
   * @throws InvalidObjectException no such partition
   * @throws MetaException error accessing the RDBMS
   */
  PrincipalPrivilegeSet getPartitionPrivilegeSet (TableName tableName,
      String partition, String userName, List<String> groupNames) throws InvalidObjectException, MetaException;

  /**
   * Get privileges for a column in a table or partition for a user.
   * @param tableName table name
   * @param partitionName partition name, or null for table level column permissions
   * @param columnName column name
   * @param userName user name
   * @param groupNames list of groups the user is in
   * @return privileges for that user on indicated column in the table or partition
   * @throws InvalidObjectException no such table, partition, or column
   * @throws MetaException error accessing the RDBMS
   */
  PrincipalPrivilegeSet getColumnPrivilegeSet (TableName tableName, String partitionName,
      String columnName, String userName, List<String> groupNames) throws InvalidObjectException, MetaException;

  List<HiveObjectPrivilege> listPrincipalGlobalGrants(String principalName,
      PrincipalType principalType);

  /**
   * For a given principal name and type, list the DB Grants
   * @param principalName principal name
   * @param principalType type
   * @param catName catalog name
   * @param dbName database name
   * @return list of privileges for that principal on the specified database.
   */
  List<HiveObjectPrivilege> listPrincipalDBGrants(String principalName,
      PrincipalType principalType, String catName, String dbName);

  /**
   * For a given principal name and type, list the DC Grants
   * @param principalName principal name
   * @param principalType type
   * @param dcName data connector name
   * @return list of privileges for that principal on the specified data connector.
   */
  List<HiveObjectPrivilege> listPrincipalDCGrants(String principalName,
      PrincipalType principalType, String dcName);

  /**
   * For a given principal name and type, list the Table Grants
   * @param principalName principal name
   * @param principalType type
   * @param tableName table name
   * @return list of privileges for that principal on the specified database.
   */
  List<HiveObjectPrivilege> listAllTableGrants(
      String principalName, PrincipalType principalType, TableName tableName);

  /**
   * For a given principal name and type, list the Table Grants
   * @param principalName principal name
   * @param principalType type
   * @param tableName table name
   * @param partName partition name (not value)
   * @return list of privileges for that principal on the specified database.
   */
  List<HiveObjectPrivilege> listPrincipalPartitionGrants(
      String principalName, PrincipalType principalType, TableName tableName,
      List<String> partValues, String partName);

  /**
   * For a given principal name and type, list the Table Grants
   * @param principalName principal name
   * @param principalType type
   * @param tableName table name
   * @param columnName column name
   * @return list of privileges for that principal on the specified database.
   */
  List<HiveObjectPrivilege> listPrincipalTableColumnGrants(
      String principalName, PrincipalType principalType, TableName tableName, String columnName);

  /**
   * For a given principal name and type, list the Table Grants
   * @param principalName principal name
   * @param principalType type
   * @param tableName table name
   * @param partName partition name (not value)
   * @param columnName column name
   * @return list of privileges for that principal on the specified database.
   */
  List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(
      String principalName, PrincipalType principalType, TableName tableName,
      List<String> partValues, String partName, String columnName);

  boolean grantPrivileges (PrivilegeBag privileges)
      throws InvalidObjectException, MetaException, NoSuchObjectException;

  boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption)
      throws InvalidObjectException, MetaException, NoSuchObjectException;

  boolean refreshPrivileges(HiveObjectRef objToRefresh, String authorizer, PrivilegeBag grantPrivileges)
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

  /**
   * List all DB grants for a given principal.
   * @param principalName principal name
   * @param principalType type
   * @return all DB grants for this principal
   */
  List<HiveObjectPrivilege> listPrincipalDBGrantsAll(
      String principalName, PrincipalType principalType);

  /**
   * List all DC grants for a given principal.
   * @param principalName principal name
   * @param principalType type
   * @return all DC grants for this principal
   */
  List<HiveObjectPrivilege> listPrincipalDCGrantsAll(
      String principalName, PrincipalType principalType);

  /**
   * List all Table grants for a given principal
   * @param principalName principal name
   * @param principalType type
   * @return all Table grants for this principal
   */
  List<HiveObjectPrivilege> listPrincipalTableGrantsAll(
      String principalName, PrincipalType principalType);

  /**
   * List all Partition grants for a given principal
   * @param principalName principal name
   * @param principalType type
   * @return all Partition grants for this principal
   */
  List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(
      String principalName, PrincipalType principalType);

  /**
   * List all Table column grants for a given principal
   * @param principalName principal name
   * @param principalType type
   * @return all Table column grants for this principal
   */
  List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(
      String principalName, PrincipalType principalType);

  /**
   * List all Partition column grants for a given principal
   * @param principalName principal name
   * @param principalType type
   * @return all Partition column grants for this principal
   */
  List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(
      String principalName, PrincipalType principalType);

  List<HiveObjectPrivilege> listGlobalGrantsAll();

  /**
   * Find all the privileges for a given database.
   * @param catName catalog name
   * @param dbName database name
   * @return list of all privileges.
   */
  List<HiveObjectPrivilege> listDBGrantsAll(String catName, String dbName);

  /**
   * Find all the privileges for a given data connector.
   * @param dcName data connector name
   * @return list of all privileges.
   */
  List<HiveObjectPrivilege> listDCGrantsAll(String dcName);

  /**
   * Find all of the privileges for a given column in a given partition.
   * @param tableName table name
   * @param partitionName partition name (not value)
   * @param columnName column name
   * @return all privileges on this column in this partition
   */
  List<HiveObjectPrivilege> listPartitionColumnGrantsAll(
      TableName tableName, String partitionName, String columnName);

  /**
   * Find all of the privileges for a given table
   * @param tableName table name
   * @return all privileges on this table
   */
  List<HiveObjectPrivilege> listTableGrantsAll(TableName tableName);

  /**
   * Find all of the privileges for a given partition.
   * @param tableName table name
   * @param partitionName partition name (not value)
   * @return all privileges on this partition
   */
  List<HiveObjectPrivilege> listPartitionGrantsAll(
      TableName tableName, String partitionName);

  /**
   * Find all of the privileges for a given column in a given table.
   * @param tableName table name
   * @param columnName column name
   * @return all privileges on this column in this table
   */
  List<HiveObjectPrivilege> listTableColumnGrantsAll(
      TableName tableName, String columnName);

  List<MDBPrivilege> listDatabaseGrants(String catName, String dbName, String authorizer);

  List<MDCPrivilege> listDataConnectorGrants(String dcName, String authorizer);
}
