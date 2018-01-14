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

import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
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
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMTrigger;
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
import org.apache.thrift.TException;

/**
 * A wrapper around {@link org.apache.hadoop.hive.metastore.ObjectStore}
 * with the ability to control the result of commitTransaction().
 * All other functions simply delegate to an embedded ObjectStore object.
 * Ideally, we should have just extended ObjectStore instead of using
 * delegation.  However, since HiveMetaStore uses a Proxy, this class must
 * not inherit from any other class.
 */
public class DummyRawStoreControlledCommit implements RawStore, Configurable {

  private final ObjectStore objectStore;
  public DummyRawStoreControlledCommit() {
    objectStore = new ObjectStore();
  }

 /**
  * If true, shouldCommit() will simply call delegate commitTransaction() to the
  * underlying ObjectStore.
  * If false, shouldCommit() immediately returns false.
  */
  private static boolean shouldCommitSucceed = true;
  public static void setCommitSucceed(boolean flag) {
    shouldCommitSucceed = flag;
  }

  @Override
  public boolean commitTransaction() {
    if (shouldCommitSucceed) {
      return objectStore.commitTransaction();
    } else {
      return false;
    }
  }

  @Override
  public boolean isActiveTransaction() {
    return false;
  }

  // All remaining functions simply delegate to objectStore

  @Override
  public Configuration getConf() {
    return objectStore.getConf();
  }

  @Override
  public void setConf(Configuration conf) {
    objectStore.setConf(conf);
  }

  @Override
  public void shutdown() {
    objectStore.shutdown();
  }

  @Override
  public boolean openTransaction() {
    return objectStore.openTransaction();
  }

  @Override
  public void rollbackTransaction() {
    objectStore.rollbackTransaction();
  }

  @Override
  public void createDatabase(Database db) throws InvalidObjectException, MetaException {
    objectStore.createDatabase(db);
  }

  @Override
  public Database getDatabase(String dbName) throws NoSuchObjectException {
    return objectStore.getDatabase(dbName);
  }

  @Override
  public boolean dropDatabase(String dbName)
      throws NoSuchObjectException, MetaException {
    return objectStore.dropDatabase(dbName);
  }

  @Override
  public boolean alterDatabase(String dbName, Database db)
      throws NoSuchObjectException, MetaException {

    return objectStore.alterDatabase(dbName, db);
  }

  @Override
  public List<String> getDatabases(String pattern) throws MetaException {
    return objectStore.getDatabases(pattern);
  }

  @Override
  public List<String> getAllDatabases() throws MetaException {
    return objectStore.getAllDatabases();
  }

  @Override
  public boolean createType(Type type) {
    return objectStore.createType(type);
  }

  @Override
  public Type getType(String typeName) {
    return objectStore.getType(typeName);
  }

  @Override
  public boolean dropType(String typeName) {
    return objectStore.dropType(typeName);
  }

  @Override
  public void createTable(Table tbl) throws InvalidObjectException, MetaException {
    objectStore.createTable(tbl);
  }

  @Override
  public boolean dropTable(String dbName, String tableName)
      throws MetaException, NoSuchObjectException,
      InvalidObjectException, InvalidInputException {
    return objectStore.dropTable(dbName, tableName);
  }

  @Override
  public Table getTable(String dbName, String tableName) throws MetaException {
    return objectStore.getTable(dbName, tableName);
  }

  @Override
  public boolean addPartition(Partition part)
      throws InvalidObjectException, MetaException {
    return objectStore.addPartition(part);
  }

  @Override
  public Partition getPartition(String dbName, String tableName, List<String> partVals)
      throws MetaException, NoSuchObjectException {
    return objectStore.getPartition(dbName, tableName, partVals);
  }

  @Override
  public boolean dropPartition(String dbName, String tableName, List<String> partVals)
      throws MetaException, NoSuchObjectException,
      InvalidObjectException, InvalidInputException {
    return objectStore.dropPartition(dbName, tableName, partVals);
  }

  @Override
  public List<Partition> getPartitions(String dbName, String tableName, int max)
      throws MetaException, NoSuchObjectException {
    return objectStore.getPartitions(dbName, tableName, max);
  }

  @Override
  public void alterTable(String dbName, String name, Table newTable)
      throws InvalidObjectException, MetaException {
    objectStore.alterTable(dbName, name, newTable);
  }

  @Override
  public List<String> getTables(String dbName, String pattern) throws MetaException {
    return objectStore.getTables(dbName, pattern);
  }

  @Override
  public List<String> getTables(String dbName, String pattern, TableType tableType) throws MetaException {
    return objectStore.getTables(dbName, pattern, tableType);
  }

  @Override
  public List<String> getMaterializedViewsForRewriting(String dbName)
      throws MetaException, NoSuchObjectException {
    return objectStore.getMaterializedViewsForRewriting(dbName);
  }

  @Override
  public List<TableMeta> getTableMeta(String dbNames, String tableNames, List<String> tableTypes)
      throws MetaException {
    return objectStore.getTableMeta(dbNames, tableNames, tableTypes);
  }

  @Override
  public List<Table> getTableObjectsByName(String dbName, List<String> tableNames)
      throws MetaException, UnknownDBException {
    return objectStore.getTableObjectsByName(dbName, tableNames);
  }

  @Override
  public List<String> getAllTables(String dbName) throws MetaException {
    return objectStore.getAllTables(dbName);
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter,
      short maxTables) throws MetaException, UnknownDBException {
    return objectStore.listTableNamesByFilter(dbName, filter, maxTables);
  }

  @Override
  public List<String> listPartitionNames(String dbName, String tblName, short maxParts)
      throws MetaException {
    return objectStore.listPartitionNames(dbName, tblName, maxParts);
  }

  @Override
  public PartitionValuesResponse listPartitionValues(String db_name, String tbl_name, List<FieldSchema> cols, boolean applyDistinct, String filter, boolean ascending, List<FieldSchema> order, long maxParts) throws MetaException {
    return null;
  }

  @Override
  public List<String> listPartitionNamesByFilter(String dbName, String tblName,
      String filter, short maxParts) throws MetaException {
    return objectStore.listPartitionNamesByFilter(dbName, tblName, filter, maxParts);
  }

  @Override
  public void alterPartition(String dbName, String tblName, List<String> partVals,
      Partition newPart) throws InvalidObjectException, MetaException {
    objectStore.alterPartition(dbName, tblName, partVals, newPart);
  }

  @Override
  public void alterPartitions(String dbName, String tblName,
      List<List<String>> partValsList, List<Partition> newParts)
      throws InvalidObjectException, MetaException {
    objectStore.alterPartitions(dbName, tblName, partValsList, newParts);
  }

  @Override
  public boolean addIndex(Index index) throws InvalidObjectException, MetaException {
    return objectStore.addIndex(index);
  }

  @Override
  public Index getIndex(String dbName, String origTableName, String indexName)
      throws MetaException {
    return objectStore.getIndex(dbName, origTableName, indexName);
  }

  @Override
  public boolean dropIndex(String dbName, String origTableName, String indexName)
      throws MetaException {
    return objectStore.dropIndex(dbName, origTableName, indexName);
  }

  @Override
  public List<Index> getIndexes(String dbName, String origTableName, int max)
      throws MetaException {
    return objectStore.getIndexes(dbName, origTableName, max);
  }

  @Override
  public List<String> listIndexNames(String dbName, String origTableName, short max)
      throws MetaException {
    return objectStore.listIndexNames(dbName, origTableName, max);
  }

  @Override
  public void alterIndex(String dbName, String baseTblName, String name, Index newIndex)
      throws InvalidObjectException, MetaException {
    objectStore.alterIndex(dbName, baseTblName, name, newIndex);
  }

  @Override
  public List<Partition> getPartitionsByFilter(String dbName, String tblName,
      String filter, short maxParts) throws MetaException, NoSuchObjectException {
    return objectStore.getPartitionsByFilter(dbName, tblName, filter, maxParts);
  }

  @Override
  public int getNumPartitionsByFilter(String dbName, String tblName,
                                      String filter) throws MetaException, NoSuchObjectException {
    return objectStore.getNumPartitionsByFilter(dbName, tblName, filter);
  }

  @Override
  public int getNumPartitionsByExpr(String dbName, String tblName,
                                      byte[] expr) throws MetaException, NoSuchObjectException {
    return objectStore.getNumPartitionsByExpr(dbName, tblName, expr);
  }

  @Override
  public List<Partition> getPartitionsByNames(String dbName, String tblName,
      List<String> partNames) throws MetaException, NoSuchObjectException {
    return objectStore.getPartitionsByNames(dbName, tblName, partNames);
  }

  @Override
  public boolean getPartitionsByExpr(String dbName, String tblName, byte[] expr,
      String defaultPartitionName, short maxParts, List<Partition> result) throws TException {
    return objectStore.getPartitionsByExpr(
        dbName, tblName, expr, defaultPartitionName, maxParts, result);
  }

  @Override
  public Table markPartitionForEvent(String dbName, String tblName,
      Map<String, String> partVals, PartitionEventType evtType)
      throws MetaException, UnknownTableException, InvalidPartitionException,
      UnknownPartitionException {
    return objectStore.markPartitionForEvent(dbName, tblName, partVals, evtType);
  }

  @Override
  public boolean isPartitionMarkedForEvent(String dbName, String tblName,
      Map<String, String> partName, PartitionEventType evtType)
      throws MetaException, UnknownTableException, InvalidPartitionException,
      UnknownPartitionException {
    return objectStore.isPartitionMarkedForEvent(dbName, tblName, partName, evtType);
  }

  @Override
  public boolean addRole(String rowName, String ownerName) throws InvalidObjectException,
      MetaException, NoSuchObjectException {
    return objectStore.addRole(rowName, ownerName);
  }

  @Override
  public boolean removeRole(String roleName)
      throws MetaException, NoSuchObjectException {
    return objectStore.removeRole(roleName);
  }

  @Override
  public boolean grantRole(Role role, String userName, PrincipalType principalType,
      String grantor, PrincipalType grantorType, boolean grantOption)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    return objectStore.grantRole(role, userName, principalType, grantor, grantorType,
        grantOption);
  }

  @Override
  public boolean revokeRole(Role role, String userName, PrincipalType principalType, boolean grantOption)
      throws MetaException, NoSuchObjectException {
    return objectStore.revokeRole(role, userName, principalType, grantOption);
  }

  @Override
  public PrincipalPrivilegeSet getUserPrivilegeSet(String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {
    return objectStore.getUserPrivilegeSet(userName, groupNames);
  }

  @Override
  public PrincipalPrivilegeSet getDBPrivilegeSet(String dbName, String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {
    return objectStore.getDBPrivilegeSet(dbName, userName, groupNames);
  }

  @Override
  public PrincipalPrivilegeSet getTablePrivilegeSet(String dbName, String tableName,
      String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    return objectStore.getTablePrivilegeSet(dbName, tableName, userName, groupNames);
  }

  @Override
  public PrincipalPrivilegeSet getPartitionPrivilegeSet(String dbName, String tableName,
      String partition, String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    return objectStore.getPartitionPrivilegeSet(dbName, tableName, partition,
        userName, groupNames);
  }

  @Override
  public PrincipalPrivilegeSet getColumnPrivilegeSet(String dbName, String tableName,
      String partitionName, String columnName, String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    return objectStore.getColumnPrivilegeSet(dbName, tableName, partitionName,
        columnName, userName, groupNames);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalGlobalGrants(String principalName,
      PrincipalType principalType) {
    return objectStore.listPrincipalGlobalGrants(principalName, principalType);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDBGrants(String principalName,
      PrincipalType principalType, String dbName) {
    return objectStore.listPrincipalDBGrants(principalName, principalType, dbName);
  }

  @Override
  public List<HiveObjectPrivilege> listAllTableGrants(String principalName,
      PrincipalType principalType, String dbName, String tableName) {
    return objectStore.listAllTableGrants(principalName, principalType,
        dbName, tableName);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionGrants(String principalName,
      PrincipalType principalType, String dbName, String tableName, List<String> partValues,
      String partName) {
    return objectStore.listPrincipalPartitionGrants(principalName, principalType,
        dbName, tableName, partValues, partName);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableColumnGrants(String principalName,
      PrincipalType principalType, String dbName, String tableName, String columnName) {
    return objectStore.listPrincipalTableColumnGrants(principalName, principalType,
        dbName, tableName, columnName);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(
      String principalName, PrincipalType principalType, String dbName, String tableName,
      List<String> partVals, String partName, String columnName) {
    return objectStore.listPrincipalPartitionColumnGrants(principalName, principalType,
        dbName, tableName, partVals, partName, columnName);
  }

  @Override
  public boolean grantPrivileges(PrivilegeBag privileges) throws InvalidObjectException,
      MetaException, NoSuchObjectException {
    return objectStore.grantPrivileges(privileges);
  }

  @Override
  public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    return objectStore.revokePrivileges(privileges, grantOption);
  }

  @Override
  public Role getRole(String roleName) throws NoSuchObjectException {
    return objectStore.getRole(roleName);
  }

  @Override
  public List<String> listRoleNames() {
    return objectStore.listRoleNames();
  }

  @Override
  public List<Role> listRoles(String principalName, PrincipalType principalType) {
    return objectStore.listRoles(principalName, principalType);
  }

  @Override
  public List<RolePrincipalGrant> listRolesWithGrants(String principalName,
                                                      PrincipalType principalType) {
    return objectStore.listRolesWithGrants(principalName, principalType);
  }

  @Override
  public List<RolePrincipalGrant> listRoleMembers(String roleName) {
    return objectStore.listRoleMembers(roleName);
  }

  @Override
  public Partition getPartitionWithAuth(String dbName, String tblName,
      List<String> partVals, String userName, List<String> groupNames)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    return objectStore.getPartitionWithAuth(dbName, tblName, partVals, userName,
        groupNames);
  }

  @Override
  public List<Partition> getPartitionsWithAuth(String dbName, String tblName,
      short maxParts, String userName, List<String> groupNames)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    return objectStore.getPartitionsWithAuth(dbName, tblName, maxParts, userName,
        groupNames);
  }

  @Override
  public List<String> listPartitionNamesPs(String dbName, String tblName,
      List<String> partVals, short maxParts)
      throws MetaException, NoSuchObjectException {
    return objectStore.listPartitionNamesPs(dbName, tblName, partVals, maxParts);
  }

  @Override
  public List<Partition> listPartitionsPsWithAuth(String dbName, String tblName,
      List<String> partVals, short maxParts, String userName, List<String> groupNames)
      throws MetaException, InvalidObjectException, NoSuchObjectException {
    return objectStore.listPartitionsPsWithAuth(dbName, tblName, partVals, maxParts,
        userName, groupNames);
  }

  @Override
  public long cleanupEvents() {
    return objectStore.cleanupEvents();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDBGrantsAll(
      String principalName, PrincipalType principalType) {
    return objectStore.listPrincipalDBGrantsAll(principalName, principalType);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableGrantsAll(
      String principalName, PrincipalType principalType) {
    return objectStore.listPrincipalTableGrantsAll(principalName, principalType);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(
      String principalName, PrincipalType principalType) {
    return objectStore.listPrincipalPartitionGrantsAll(principalName, principalType);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(
      String principalName, PrincipalType principalType) {
    return objectStore.listPrincipalTableColumnGrantsAll(principalName, principalType);
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(
      String principalName, PrincipalType principalType) {
    return objectStore.listPrincipalPartitionColumnGrantsAll(principalName, principalType);
  }

  @Override
  public List<HiveObjectPrivilege> listGlobalGrantsAll() {
    return objectStore.listGlobalGrantsAll();
  }

  @Override
  public List<HiveObjectPrivilege> listDBGrantsAll(String dbName) {
    return objectStore.listDBGrantsAll(dbName);
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionColumnGrantsAll(String dbName, String tableName,
      String partitionName, String columnName) {
    return objectStore.listPartitionColumnGrantsAll(dbName, tableName, partitionName, columnName);
  }

  @Override
  public List<HiveObjectPrivilege> listTableGrantsAll(String dbName, String tableName) {
    return objectStore.listTableGrantsAll(dbName, tableName);
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionGrantsAll(String dbName, String tableName,
      String partitionName) {
    return objectStore.listPartitionGrantsAll(dbName, tableName, partitionName);
  }

  @Override
  public List<HiveObjectPrivilege> listTableColumnGrantsAll(String dbName, String tableName,
      String columnName) {
    return objectStore.listTableColumnGrantsAll(dbName, tableName, columnName);
  }

  @Override
  public ColumnStatistics getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames) throws MetaException, NoSuchObjectException {
    return objectStore.getTableColumnStatistics(dbName, tableName, colNames);
  }

  @Override
  public boolean deleteTableColumnStatistics(String dbName, String tableName,
      String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException,
      InvalidInputException {
    return objectStore.deleteTableColumnStatistics(dbName, tableName, colName);
  }

  @Override
  public boolean deletePartitionColumnStatistics(String dbName, String tableName,
      String partName, List<String> partVals, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException,
      InvalidInputException {
    return objectStore.deletePartitionColumnStatistics(dbName, tableName, partName,
        partVals, colName);
  }

  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics statsObj)
      throws NoSuchObjectException, MetaException, InvalidObjectException,
      InvalidInputException {
    return objectStore.updateTableColumnStatistics(statsObj);
  }

  @Override
  public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj,
      List<String> partVals)
      throws NoSuchObjectException, MetaException, InvalidObjectException,
      InvalidInputException {
    return objectStore.updatePartitionColumnStatistics(statsObj, partVals);
  }

  @Override
  public boolean addToken(String tokenIdentifier, String delegationToken) {
    return false;
  }

  @Override
  public boolean removeToken(String tokenIdentifier) {
    return false;
  }

  @Override
  public String getToken(String tokenIdentifier) {
    return "";
  }

  @Override
  public List<String> getAllTokenIdentifiers() {
    return new ArrayList<>();
  }

  @Override
  public int addMasterKey(String key) throws MetaException {
    return -1;
  }

  @Override
  public void updateMasterKey(Integer seqNo, String key)
    throws NoSuchObjectException, MetaException {}

  @Override
  public boolean removeMasterKey(Integer keySeq) {
    return false;
  }

  @Override
  public String[] getMasterKeys() {
    return new String[0];
  }

  @Override
  public void verifySchema() throws MetaException {
  }

  @Override
  public String getMetaStoreSchemaVersion() throws MetaException {
    return objectStore.getMetaStoreSchemaVersion();
  }

  @Override
  public void setMetaStoreSchemaVersion(String schemaVersion, String comment) throws MetaException {
    objectStore.setMetaStoreSchemaVersion(schemaVersion, comment);

  }

  @Override
  public List<ColumnStatistics> getPartitionColumnStatistics(String dbName,
      String tblName, List<String> colNames, List<String> partNames)
      throws MetaException, NoSuchObjectException {
    return objectStore.getPartitionColumnStatistics(dbName, tblName  , colNames, partNames);
  }

  @Override
  public boolean doesPartitionExist(String dbName, String tableName,
      List<String> partVals) throws MetaException, NoSuchObjectException {
    return objectStore.doesPartitionExist(dbName, tableName, partVals);
  }

  @Override
  public boolean addPartitions(String dbName, String tblName, List<Partition> parts)
      throws InvalidObjectException, MetaException {
    return objectStore.addPartitions(dbName, tblName, parts);
  }

  @Override
  public boolean addPartitions(String dbName, String tblName, PartitionSpecProxy partitionSpec, boolean ifNotExists) throws InvalidObjectException, MetaException {
    return false;
  }

  @Override
  public void dropPartitions(String dbName, String tblName, List<String> partNames)
      throws MetaException, NoSuchObjectException {
    objectStore.dropPartitions(dbName, tblName, partNames);
  }

  @Override
  public void createFunction(Function func) throws InvalidObjectException,
      MetaException {
    objectStore.createFunction(func);
  }

  @Override
  public void alterFunction(String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException {
    objectStore.alterFunction(dbName, funcName, newFunction);
  }

  @Override
  public void dropFunction(String dbName, String funcName)
      throws MetaException, NoSuchObjectException, InvalidObjectException,
      InvalidInputException {
    objectStore.dropFunction(dbName, funcName);
  }

  @Override
  public Function getFunction(String dbName, String funcName)
      throws MetaException {
    return objectStore.getFunction(dbName, funcName);
  }

  @Override
  public List<Function> getAllFunctions()
          throws MetaException {
    return Collections.emptyList();
  }

  @Override
  public List<String> getFunctions(String dbName, String pattern)
      throws MetaException {
    return objectStore.getFunctions(dbName, pattern);
  }

  @Override
  public AggrStats get_aggr_stats_for(String dbName,
      String tblName, List<String> partNames, List<String> colNames)
      throws MetaException {
    return null;
  }

  @Override
  public NotificationEventResponse getNextNotification(NotificationEventRequest rqst) {
    return objectStore.getNextNotification(rqst);
  }

  @Override
  public void addNotificationEvent(NotificationEvent event) {
    objectStore.addNotificationEvent(event);
  }

  @Override
  public void cleanNotificationEvents(int olderThan) {
    objectStore.cleanNotificationEvents(olderThan);
  }

  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() {
    return objectStore.getCurrentNotificationEventId();
  }

  @Override
  public NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst) {
    return  objectStore.getNotificationEventsCount(rqst);
  }

  @Override
  public void flushCache() {
    objectStore.flushCache();
  }

  @Override
  public ByteBuffer[] getFileMetadata(List<Long> fileIds) {
    return null;
  }

  @Override
  public void putFileMetadata(
      List<Long> fileIds, List<ByteBuffer> metadata, FileMetadataExprType type) {
  }

  @Override
  public boolean isFileMetadataSupported() {
    return false;
  }


  @Override
  public void getFileMetadataByExpr(List<Long> fileIds, FileMetadataExprType type, byte[] expr,
      ByteBuffer[] metadatas, ByteBuffer[] stripeBitsets, boolean[] eliminated) {
  }

  @Override
  public int getTableCount() throws MetaException {
    return objectStore.getTableCount();
  }

  @Override
  public int getPartitionCount() throws MetaException {
    return objectStore.getPartitionCount();
  }

  @Override
  public int getDatabaseCount() throws MetaException {
    return objectStore.getDatabaseCount();
  }

  @Override
  public FileMetadataHandler getFileMetadataHandler(FileMetadataExprType type) {
    return null;
  }

  @Override
  public List<SQLPrimaryKey> getPrimaryKeys(String db_name, String tbl_name)
    throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SQLForeignKey> getForeignKeys(String parent_db_name,
    String parent_tbl_name, String foreign_db_name, String foreign_tbl_name)
    throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SQLUniqueConstraint> getUniqueConstraints(String db_name, String tbl_name)
    throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SQLNotNullConstraint> getNotNullConstraints(String db_name, String tbl_name)
    throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> createTableWithConstraints(Table tbl,
    List<SQLPrimaryKey> primaryKeys, List<SQLForeignKey> foreignKeys,
    List<SQLUniqueConstraint> uniqueConstraints,
    List<SQLNotNullConstraint> notNullConstraints)
    throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void dropConstraint(String dbName, String tableName,
   String constraintName) throws NoSuchObjectException {
   // TODO Auto-generated method stub
  }

  @Override
  public List<String> addPrimaryKeys(List<SQLPrimaryKey> pks)
    throws InvalidObjectException, MetaException {
    return null;
  }

  @Override
  public List<String> addForeignKeys(List<SQLForeignKey> fks)
    throws InvalidObjectException, MetaException {
    return null;
  }

  @Override
  public List<String> addUniqueConstraints(List<SQLUniqueConstraint> uks)
    throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<String> addNotNullConstraints(List<SQLNotNullConstraint> nns)
    throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Map<String, List<ColumnStatisticsObj>> getColStatsForTablePartitions(String dbName,
      String tableName) throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getMetastoreDbUuid() throws MetaException {
    throw new MetaException("Get metastore uuid is not implemented");
  }

  @Override
  public void createResourcePlan(WMResourcePlan resourcePlan, String copyFrom, int defaultPoolSize)
      throws AlreadyExistsException, InvalidObjectException, MetaException, NoSuchObjectException {
    objectStore.createResourcePlan(resourcePlan, copyFrom, defaultPoolSize);
  }

  @Override
  public WMFullResourcePlan getResourcePlan(String name) throws NoSuchObjectException {
    return objectStore.getResourcePlan(name);
  }

  @Override
  public List<WMResourcePlan> getAllResourcePlans() throws MetaException {
    return objectStore.getAllResourcePlans();
  }

  @Override
  public WMFullResourcePlan alterResourcePlan(String name, WMResourcePlan resourcePlan,
      boolean canActivateDisabled, boolean canDeactivate, boolean isReplace)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
          MetaException {
    return objectStore.alterResourcePlan(
      name, resourcePlan, canActivateDisabled, canDeactivate, isReplace);
  }

  @Override
  public WMFullResourcePlan getActiveResourcePlan() throws MetaException {
    return objectStore.getActiveResourcePlan();
  }

  @Override
  public List<String> validateResourcePlan(String name)
      throws NoSuchObjectException, InvalidObjectException, MetaException {
    return objectStore.validateResourcePlan(name);
  }

  @Override
  public void dropResourcePlan(String name) throws NoSuchObjectException, MetaException {
    objectStore.dropResourcePlan(name);
  }

  @Override
  public void createWMTrigger(WMTrigger trigger)
      throws AlreadyExistsException, MetaException, NoSuchObjectException,
          InvalidOperationException {
    objectStore.createWMTrigger(trigger);
  }

  @Override
  public void alterWMTrigger(WMTrigger trigger)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    objectStore.alterWMTrigger(trigger);
  }

  @Override
  public void dropWMTrigger(String resourcePlanName, String triggerName)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    objectStore.dropWMTrigger(resourcePlanName, triggerName);
  }

  @Override
  public List<WMTrigger> getTriggersForResourcePlan(String resourcePlanName)
      throws NoSuchObjectException, MetaException {
    return objectStore.getTriggersForResourcePlan(resourcePlanName);
  }

  @Override
  public void createPool(WMPool pool) throws AlreadyExistsException, NoSuchObjectException,
      InvalidOperationException, MetaException {
    objectStore.createPool(pool);
  }

  @Override
  public void alterPool(WMPool pool, String poolPath) throws AlreadyExistsException,
      NoSuchObjectException, InvalidOperationException, MetaException {
    objectStore.alterPool(pool, poolPath);
  }

  @Override
  public void dropWMPool(String resourcePlanName, String poolPath)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    objectStore.dropWMPool(resourcePlanName, poolPath);
  }

  @Override
  public void createOrUpdateWMMapping(WMMapping mapping, boolean update)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
      MetaException {
    objectStore.createOrUpdateWMMapping(mapping, update);
  }

  @Override
  public void dropWMMapping(WMMapping mapping)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    objectStore.dropWMMapping(mapping);
  }

  @Override
  public void createWMTriggerToPoolMapping(String resourcePlanName, String triggerName,
      String poolPath) throws AlreadyExistsException, NoSuchObjectException,
      InvalidOperationException, MetaException {
    objectStore.createWMTriggerToPoolMapping(resourcePlanName, triggerName, poolPath);
  }

  @Override
  public void dropWMTriggerToPoolMapping(String resourcePlanName, String triggerName,
      String poolPath) throws NoSuchObjectException, InvalidOperationException, MetaException {
    objectStore.dropWMTriggerToPoolMapping(resourcePlanName, triggerName, poolPath);
  }
}
