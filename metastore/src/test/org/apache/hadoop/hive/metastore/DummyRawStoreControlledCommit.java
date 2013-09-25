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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.Database;
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
      throws MetaException {
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
  public List<Partition> getPartitionsByNames(String dbName, String tblName,
      List<String> partNames) throws MetaException, NoSuchObjectException {
    return objectStore.getPartitionsByNames(dbName, tblName, partNames);
  }

  @Override
  public boolean getPartitionsByExpr(String dbName, String tblName, byte[] expr,
      String defaultPartitionName, short maxParts, Set<Partition> result) throws TException {
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
    return objectStore.grantRole(role, userName,  principalType, grantor, grantorType,
        grantOption);
  }

  @Override
  public boolean revokeRole(Role role, String userName, PrincipalType principalType)
      throws MetaException, NoSuchObjectException {
    return objectStore.revokeRole(role, userName, principalType);
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
  public List<MGlobalPrivilege> listPrincipalGlobalGrants(String principalName,
      PrincipalType principalType) {
    return objectStore.listPrincipalGlobalGrants(principalName, principalType);
  }

  @Override
  public List<MDBPrivilege> listPrincipalDBGrants(String principalName,
      PrincipalType principalType, String dbName) {
    return objectStore.listPrincipalDBGrants(principalName,  principalType, dbName);
  }

  @Override
  public List<MTablePrivilege> listAllTableGrants(String principalName,
      PrincipalType principalType, String dbName, String tableName) {
    return objectStore.listAllTableGrants(principalName,  principalType,
        dbName, tableName);
  }

  @Override
  public List<MPartitionPrivilege> listPrincipalPartitionGrants(String principalName,
      PrincipalType principalType, String dbName, String tableName, String partName) {
    return objectStore.listPrincipalPartitionGrants(principalName, principalType,
        dbName, tableName, partName);
  }

  @Override
  public List<MTableColumnPrivilege> listPrincipalTableColumnGrants(String principalName,
      PrincipalType principalType, String dbName, String tableName, String columnName) {
    return objectStore.listPrincipalTableColumnGrants(principalName, principalType,
        dbName, tableName, columnName);
  }

  @Override
  public List<MPartitionColumnPrivilege> listPrincipalPartitionColumnGrants(
      String principalName, PrincipalType principalType, String dbName, String tableName,
      String partName, String columnName) {
    return objectStore.listPrincipalPartitionColumnGrants(principalName, principalType,
        dbName, tableName, partName, columnName);
  }

  @Override
  public boolean grantPrivileges(PrivilegeBag privileges) throws InvalidObjectException,
      MetaException, NoSuchObjectException {
    return objectStore.grantPrivileges(privileges);
  }

  @Override
  public boolean revokePrivileges(PrivilegeBag privileges) throws InvalidObjectException,
      MetaException, NoSuchObjectException {
    return objectStore.revokePrivileges(privileges);
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
  public List<MRoleMap> listRoles(String principalName, PrincipalType principalType) {
    return objectStore.listRoles(principalName, principalType);
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
  public ColumnStatistics getTableColumnStatistics(String dbName, String tableName,
      String colName)
      throws MetaException, NoSuchObjectException, InvalidInputException {
    return objectStore.getTableColumnStatistics(dbName, tableName, colName);
  }

  @Override
  public boolean deleteTableColumnStatistics(String dbName, String tableName,
      String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException,
      InvalidInputException {
    return objectStore.deleteTableColumnStatistics(dbName, tableName, colName);
  }

  public boolean deletePartitionColumnStatistics(String dbName, String tableName,
      String partName, List<String> partVals, String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException,
      InvalidInputException {
    return objectStore.deletePartitionColumnStatistics(dbName, tableName, partName,
        partVals, colName);
  }

  @Override
  public ColumnStatistics getPartitionColumnStatistics(String dbName, String tableName,
      String partName, List<String> partVal, String colName)
      throws MetaException, NoSuchObjectException, InvalidInputException,
      InvalidObjectException  {
    return objectStore.getPartitionColumnStatistics(dbName, tableName, partName,
        partVal, colName);
  }

  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics statsObj)
      throws NoSuchObjectException, MetaException, InvalidObjectException,
      InvalidInputException {
    return objectStore.updateTableColumnStatistics(statsObj);
  }

  public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj,
      List<String> partVals)
      throws NoSuchObjectException, MetaException, InvalidObjectException,
      InvalidInputException {
    return objectStore.updatePartitionColumnStatistics(statsObj, partVals);
  }

  public boolean addToken(String tokenIdentifier, String delegationToken) {
    return false;
  }

  public boolean removeToken(String tokenIdentifier) {
    return false;
  }

  public String getToken(String tokenIdentifier) {
    return "";
  }

  public List<String> getAllTokenIdentifiers() {
    return new ArrayList<String>();
  }

  public int addMasterKey(String key) throws MetaException {
    return -1;
  }

  public void updateMasterKey(Integer seqNo, String key)
    throws NoSuchObjectException, MetaException {}

  public boolean removeMasterKey(Integer keySeq) {
    return false;
  }

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

}
