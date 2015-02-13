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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.NotificationEvent;
import org.apache.hadoop.hive.metastore.api.NotificationEventRequest;
import org.apache.hadoop.hive.metastore.api.NotificationEventResponse;
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
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.thrift.TException;

/**
 *
 * DummyRawStoreForJdoConnection.
 *
 * An implementation of RawStore that verifies the DummyJdoConnectionUrlHook has already been
 * applied when this class's setConf method is called, by checking that the value of the
 * METASTORECONNECTURLKEY ConfVar has been updated.
 *
 * All non-void methods return default values.
 */
public class DummyRawStoreForJdoConnection implements RawStore {

  @Override
  public Configuration getConf() {

    return null;
  }

  @Override
  public void setConf(Configuration arg0) {
    String expected = DummyJdoConnectionUrlHook.newUrl;
    String actual = arg0.get(HiveConf.ConfVars.METASTORECONNECTURLKEY.varname);

    Assert.assertEquals("The expected URL used by JDO to connect to the metastore: " + expected +
        " did not match the actual value when the Raw Store was initialized: " + actual,
        expected, actual);
  }

  @Override
  public void shutdown() {


  }

  @Override
  public boolean openTransaction() {

    return false;
  }

  @Override
  public boolean commitTransaction() {

    return false;
  }

  @Override
  public void rollbackTransaction() {


  }

  @Override
  public void createDatabase(Database db) throws InvalidObjectException, MetaException {


  }

  @Override
  public Database getDatabase(String name) throws NoSuchObjectException {

    return null;
  }

  @Override
  public boolean dropDatabase(String dbname) throws NoSuchObjectException, MetaException {

    return false;
  }

  @Override
  public boolean alterDatabase(String dbname, Database db) throws NoSuchObjectException,
      MetaException {

    return false;
  }

  @Override
  public List<String> getDatabases(String pattern) throws MetaException {

    return Collections.emptyList();
  }

  @Override
  public List<String> getAllDatabases() throws MetaException {

    return Collections.emptyList();
  }

  @Override
  public boolean createType(Type type) {

    return false;
  }

  @Override
  public Type getType(String typeName) {

    return null;
  }

  @Override
  public boolean dropType(String typeName) {

    return false;
  }

  @Override
  public void createTable(Table tbl) throws InvalidObjectException, MetaException {


  }

  @Override
  public boolean dropTable(String dbName, String tableName) throws MetaException {

    return false;
  }

  @Override
  public Table getTable(String dbName, String tableName) throws MetaException {

    return null;
  }

  @Override
  public boolean addPartition(Partition part) throws InvalidObjectException, MetaException {

    return false;
  }

  @Override
  public Partition getPartition(String dbName, String tableName, List<String> part_vals)
      throws MetaException, NoSuchObjectException {

    return null;
  }

  @Override
  public boolean dropPartition(String dbName, String tableName, List<String> part_vals)
      throws MetaException {

    return false;
  }

  @Override
  public List<Partition> getPartitions(String dbName, String tableName, int max)
      throws MetaException {

    return Collections.emptyList();
  }

  @Override
  public void alterTable(String dbname, String name, Table newTable) throws InvalidObjectException,
      MetaException {


  }

  @Override
  public List<String> getTables(String dbName, String pattern) throws MetaException {

    return Collections.emptyList();
  }

  @Override
  public List<Table> getTableObjectsByName(String dbname, List<String> tableNames)
      throws MetaException, UnknownDBException {

    return Collections.emptyList();
  }

  @Override
  public List<String> getAllTables(String dbName) throws MetaException {

    return Collections.emptyList();
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter, short max_tables)
      throws MetaException, UnknownDBException {

    return Collections.emptyList();
  }

  @Override
  public List<String> listPartitionNames(String db_name, String tbl_name, short max_parts)
      throws MetaException {

    return Collections.emptyList();
  }

  @Override
  public List<String> listPartitionNamesByFilter(String db_name, String tbl_name, String filter,
      short max_parts) throws MetaException {

    return Collections.emptyList();
  }

  @Override
  public void alterPartition(String db_name, String tbl_name, List<String> part_vals,
      Partition new_part) throws InvalidObjectException, MetaException {


  }

  @Override
  public void alterPartitions(String db_name, String tbl_name, List<List<String>> part_vals_list,
      List<Partition> new_parts) throws InvalidObjectException, MetaException {


  }


  @Override
  public boolean addIndex(Index index) throws InvalidObjectException, MetaException {

    return false;
  }

  @Override
  public Index getIndex(String dbName, String origTableName, String indexName)
      throws MetaException {

    return null;
  }

  @Override
  public boolean dropIndex(String dbName, String origTableName, String indexName)
      throws MetaException {

    return false;
  }

  @Override
  public List<Index> getIndexes(String dbName, String origTableName, int max)
      throws MetaException {

    return null;
  }

  @Override
  public List<String> listIndexNames(String dbName, String origTableName, short max)
      throws MetaException {

    return Collections.emptyList();
  }

  @Override
  public void alterIndex(String dbname, String baseTblName, String name, Index newIndex)
      throws InvalidObjectException, MetaException {


  }

  @Override
  public List<Partition> getPartitionsByFilter(String dbName, String tblName, String filter,
      short maxParts) throws MetaException, NoSuchObjectException {

    return Collections.emptyList();
  }

  @Override
  public List<Partition> getPartitionsByNames(String dbName, String tblName,
      List<String> partNames) throws MetaException, NoSuchObjectException {

    return Collections.emptyList();
  }

  @Override
  public boolean getPartitionsByExpr(String dbName, String tblName, byte[] expr,
      String defaultPartitionName, short maxParts, List<Partition> result) throws TException {
    return false;
  }

  @Override
  public Table markPartitionForEvent(String dbName, String tblName, Map<String, String> partVals,
      PartitionEventType evtType) throws MetaException, UnknownTableException,
      InvalidPartitionException, UnknownPartitionException {

    return null;
  }

  @Override
  public boolean isPartitionMarkedForEvent(String dbName, String tblName,
      Map<String, String> partName, PartitionEventType evtType) throws MetaException,
      UnknownTableException, InvalidPartitionException, UnknownPartitionException {

    return false;
  }

  @Override
  public boolean addRole(String rowName, String ownerName) throws InvalidObjectException,
      MetaException, NoSuchObjectException {

    return false;
  }

  @Override
  public boolean removeRole(String roleName) throws MetaException, NoSuchObjectException {

    return false;
  }

  @Override
  public boolean grantRole(Role role, String userName, PrincipalType principalType, String grantor,
      PrincipalType grantorType, boolean grantOption) throws MetaException, NoSuchObjectException,
      InvalidObjectException {

    return false;
  }

  @Override
  public boolean revokeRole(Role role, String userName, PrincipalType principalType, boolean grantOption)
      throws MetaException, NoSuchObjectException {

    return false;
  }

  @Override
  public PrincipalPrivilegeSet getUserPrivilegeSet(String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {

    return null;
  }

  @Override
  public PrincipalPrivilegeSet getDBPrivilegeSet(String dbName, String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {

    return null;
  }

  @Override
  public PrincipalPrivilegeSet getTablePrivilegeSet(String dbName, String tableName,
      String userName, List<String> groupNames) throws InvalidObjectException, MetaException {

    return null;
  }

  @Override
  public PrincipalPrivilegeSet getPartitionPrivilegeSet(String dbName, String tableName,
      String partition, String userName, List<String> groupNames) throws InvalidObjectException,
      MetaException {

    return null;
  }

  @Override
  public PrincipalPrivilegeSet getColumnPrivilegeSet(String dbName, String tableName,
      String partitionName, String columnName, String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {

    return null;
  }

  @Override
  public List<MGlobalPrivilege> listPrincipalGlobalGrants(String principalName,
      PrincipalType principalType) {

    return Collections.emptyList();
  }

  @Override
  public List<MDBPrivilege> listPrincipalDBGrants(String principalName,
      PrincipalType principalType, String dbName) {

    return Collections.emptyList();
  }

  @Override
  public List<MTablePrivilege> listAllTableGrants(String principalName,
      PrincipalType principalType, String dbName, String tableName) {

    return Collections.emptyList();
  }

  @Override
  public List<MPartitionPrivilege> listPrincipalPartitionGrants(String principalName,
      PrincipalType principalType, String dbName, String tableName, String partName) {

    return Collections.emptyList();
  }

  @Override
  public List<MTableColumnPrivilege> listPrincipalTableColumnGrants(String principalName,
      PrincipalType principalType, String dbName, String tableName, String columnName) {

    return Collections.emptyList();
  }

  @Override
  public List<MPartitionColumnPrivilege> listPrincipalPartitionColumnGrants(String principalName,
      PrincipalType principalType, String dbName, String tableName, String partName,
      String columnName) {

    return Collections.emptyList();
  }

  @Override
  public boolean grantPrivileges(PrivilegeBag privileges) throws InvalidObjectException,
      MetaException, NoSuchObjectException {

    return false;
  }

  @Override
  public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption)
      throws InvalidObjectException, MetaException, NoSuchObjectException {

    return false;
  }

  @Override
  public Role getRole(String roleName) throws NoSuchObjectException {

    return null;
  }

  @Override
  public List<String> listRoleNames() {

    return Collections.emptyList();
  }

  @Override
  public List<MRoleMap> listRoles(String principalName, PrincipalType principalType) {

    return Collections.emptyList();
  }

  @Override
  public List<MRoleMap> listRoleMembers(String roleName) {
    return Collections.emptyList();
  }

  @Override
  public Partition getPartitionWithAuth(String dbName, String tblName, List<String> partVals,
      String user_name, List<String> group_names) throws MetaException, NoSuchObjectException,
      InvalidObjectException {

    return null;
  }

  @Override
  public List<Partition> getPartitionsWithAuth(String dbName, String tblName, short maxParts,
      String userName, List<String> groupNames) throws MetaException, NoSuchObjectException,
      InvalidObjectException {

    return Collections.emptyList();
  }

  @Override
  public List<String> listPartitionNamesPs(String db_name, String tbl_name, List<String> part_vals,
      short max_parts) throws MetaException, NoSuchObjectException {

    return Collections.emptyList();
  }

  @Override
  public List<Partition> listPartitionsPsWithAuth(String db_name, String tbl_name,
      List<String> part_vals, short max_parts, String userName, List<String> groupNames)
      throws MetaException, InvalidObjectException, NoSuchObjectException {

    return Collections.emptyList();
  }

  @Override
  public long cleanupEvents() {

    return 0;
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
    return null;
  }

  @Override
  public List<String> getAllTokenIdentifiers() {
    return Collections.emptyList();
  }

  @Override
  public int addMasterKey(String key) {
    return 0;
  }

  @Override
  public void updateMasterKey(Integer seqNo, String key) {
  }

  @Override
  public boolean removeMasterKey(Integer keySeq) {
    return false;
  }

  @Override
  public String[] getMasterKeys() {
    return new String[0];
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDBGrantsAll(
      String principalName, PrincipalType principalType) {
    return Collections.emptyList();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableGrantsAll(
      String principalName, PrincipalType principalType) {
    return Collections.emptyList();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(
      String principalName, PrincipalType principalType) {
    return Collections.emptyList();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(
      String principalName, PrincipalType principalType) {
    return Collections.emptyList();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(
      String principalName, PrincipalType principalType) {
    return Collections.emptyList();
  }

  @Override
  public List<HiveObjectPrivilege> listGlobalGrantsAll() {
    return Collections.emptyList();
  }

  @Override
  public List<HiveObjectPrivilege> listDBGrantsAll(String dbName) {
    return Collections.emptyList();
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionColumnGrantsAll(String dbName, String tableName, String partitionName, String columnName) {
    return Collections.emptyList();
  }

  @Override
  public List<HiveObjectPrivilege> listTableGrantsAll(String dbName, String tableName) {
    return Collections.emptyList();
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionGrantsAll(String dbName, String tableName, String partitionName) {
    return Collections.emptyList();
  }

  @Override
  public List<HiveObjectPrivilege> listTableColumnGrantsAll(String dbName, String tableName, String columnName) {
    return Collections.emptyList();
  }

  @Override
  public  ColumnStatistics getTableColumnStatistics(String dbName, String tableName,
      List<String> colName) throws MetaException, NoSuchObjectException {
    return null;
  }

  @Override
  public boolean deleteTableColumnStatistics(String dbName, String tableName,
                                              String colName)
      throws NoSuchObjectException, MetaException, InvalidObjectException {
    return false;
  }


  @Override
  public boolean deletePartitionColumnStatistics(String dbName, String tableName,
    String partName, List<String> partVals, String colName)
    throws NoSuchObjectException, MetaException, InvalidObjectException,
    InvalidInputException {
    return false;

  }

  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics statsObj)
      throws NoSuchObjectException, MetaException, InvalidObjectException {
    return false;
  }

  @Override
  public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj,List<String> partVals)
    throws NoSuchObjectException, MetaException, InvalidObjectException {
    return false;
  }

  @Override
  public void verifySchema() throws MetaException {
  }

  @Override
  public String getMetaStoreSchemaVersion() throws MetaException {
    return null;
  }

  @Override
  public void setMetaStoreSchemaVersion(String version, String comment) throws MetaException {
  }

  @Override
  public List<ColumnStatistics> getPartitionColumnStatistics(String dbName,
      String tblName, List<String> colNames, List<String> partNames)
      throws MetaException, NoSuchObjectException {
    return Collections.emptyList();
  }

  @Override
  public boolean doesPartitionExist(String dbName, String tableName,
      List<String> partVals) throws MetaException, NoSuchObjectException {
    return false;
  }

  @Override
  public boolean addPartitions(String dbName, String tblName, List<Partition> parts)
      throws InvalidObjectException, MetaException {
    return false;
  }

  @Override
  public boolean addPartitions(String dbName, String tblName, PartitionSpecProxy partitionSpec, boolean ifNotExists) throws InvalidObjectException, MetaException {
    return false;
  }

  @Override
  public void dropPartitions(String dbName, String tblName, List<String> partNames) {
  }

  @Override
  public void createFunction(Function func) throws InvalidObjectException,
      MetaException {
  }

  @Override
  public void alterFunction(String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException {
  }

  @Override
  public void dropFunction(String dbName, String funcName)
      throws MetaException, NoSuchObjectException, InvalidObjectException,
      InvalidInputException {
  }

  @Override
  public Function getFunction(String dbName, String funcName)
      throws MetaException {
    return null;
  }

  @Override
  public List<String> getFunctions(String dbName, String pattern)
      throws MetaException {
    return Collections.emptyList();
  }

  @Override
  public AggrStats get_aggr_stats_for(String dbName,
      String tblName, List<String> partNames, List<String> colNames)
      throws MetaException {
    return null;
  }

  @Override
  public NotificationEventResponse getNextNotification(NotificationEventRequest rqst) {
    return null;
  }

  @Override
  public void addNotificationEvent(NotificationEvent event) {

  }

  @Override
  public void cleanNotificationEvents(int olderThan) {

  }

  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() {
    return null;
  }


}


