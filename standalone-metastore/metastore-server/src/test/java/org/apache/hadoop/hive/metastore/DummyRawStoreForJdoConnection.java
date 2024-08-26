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

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.AllTableConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CheckConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.AddPackageRequest;
import org.apache.hadoop.hive.metastore.api.DefaultConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.DropPackageRequest;
import org.apache.hadoop.hive.metastore.api.ForeignKeysRequest;
import org.apache.hadoop.hive.metastore.api.GetPackageRequest;
import org.apache.hadoop.hive.metastore.api.GetPartitionsFilterSpec;
import org.apache.hadoop.hive.metastore.api.GetProjectionsSpec;
import org.apache.hadoop.hive.metastore.api.ISchemaName;
import org.apache.hadoop.hive.metastore.api.ListPackageRequest;
import org.apache.hadoop.hive.metastore.api.ListStoredProcedureRequest;
import org.apache.hadoop.hive.metastore.api.NotNullConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.Package;
import org.apache.hadoop.hive.metastore.api.PrimaryKeysRequest;
import org.apache.hadoop.hive.metastore.api.SQLAllTableConstraints;
import org.apache.hadoop.hive.metastore.api.SchemaVersionDescriptor;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.StoredProcedure;
import org.apache.hadoop.hive.metastore.api.UniqueConstraintsRequest;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Catalog;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DataConnector;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.GetPartitionsFilterSpec;
import org.apache.hadoop.hive.metastore.api.GetProjectionsSpec;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.ISchema;
import org.apache.hadoop.hive.metastore.api.ISchemaName;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.InvalidPartitionException;
import org.apache.hadoop.hive.metastore.api.ListStoredProcedureRequest;
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
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.metastore.api.ReplicationMetricList;
import org.apache.hadoop.hive.metastore.api.GetReplicationMetricsRequest;
import org.apache.hadoop.hive.metastore.api.SQLAllTableConstraints;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.metastore.api.ScheduledQuery;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryKey;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryMaintenanceRequest;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollRequest;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryPollResponse;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryProgressInfo;
import org.apache.hadoop.hive.metastore.api.SchemaVersion;
import org.apache.hadoop.hive.metastore.api.SchemaVersionDescriptor;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StoredProcedure;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.Type;
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
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.client.builder.GetPartitionsArgs;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.model.MTable;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.thrift.TException;
import org.junit.Assert;

/**
 *
 * DummyRawStoreForJdoConnection.
 *
 * An implementation of RawStore that verifies the DummyJdoConnectionUrlHook has already been
 * applied when this class's setConf method is called, by checking that the value of the
 * METASTORE_CONNECT_URL_KEY ConfVar has been updated.
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
    String actual = MetastoreConf.getVar(arg0, MetastoreConf.ConfVars.CONNECT_URL_KEY);

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
  public boolean isActiveTransaction() {
    return false;
  }

  @Override
  public void rollbackTransaction() {
  }

  @Override
  public void createCatalog(Catalog cat) throws MetaException {

  }

  @Override
  public Catalog getCatalog(String catalogName) throws NoSuchObjectException, MetaException {
    return null;
  }

  @Override
  public void alterCatalog(String catName, Catalog cat) throws MetaException,
      InvalidOperationException {

  }

  @Override
  public List<String> getCatalogs() {
    return null;
  }

  @Override
  public void dropCatalog(String catalogName) throws NoSuchObjectException, MetaException {

  }

  @Override
  public void createDatabase(Database db) throws InvalidObjectException, MetaException {


  }

  @Override
  public Database getDatabase(String catName, String name) throws NoSuchObjectException {

    return null;
  }

  @Override
  public boolean dropDatabase(String catName, String dbname) throws NoSuchObjectException, MetaException {

    return false;
  }

  @Override
  public boolean alterDatabase(String catName, String dbname, Database db) throws NoSuchObjectException,
      MetaException {

    return false;
  }

  @Override
  public List<String> getDatabases(String catName, String pattern) throws MetaException {

    return Collections.emptyList();
  }

  @Override
  public List<String> getAllDatabases(String catName) throws MetaException {

    return Collections.emptyList();
  }

  @Override
  public void createDataConnector(DataConnector connector) throws InvalidObjectException, MetaException {

  }

  @Override
  public DataConnector getDataConnector(String name) throws NoSuchObjectException {
    return null;
  }

  @Override
  public boolean dropDataConnector(String connector) throws NoSuchObjectException, MetaException {
    return false;
  }

  @Override
  public boolean alterDataConnector(String dcName, DataConnector connector) throws NoSuchObjectException,
      MetaException {
    return false;
  }

  @Override
  public List<String> getAllDataConnectorNames() throws MetaException {
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
  public boolean dropTable(String catName, String dbName, String tableName) throws MetaException {

    return false;
  }

  @Override
  public Table getTable(String catName, String dbName, String tableName) throws MetaException {

    return null;
  }

  @Override
  public Table getTable(String catalogName, String dbName, String tableName,
                        String  writeIdList) throws MetaException {
    return null;
  }

  @Override
  public Table getTable(String catalogName, String dbName, String tableName, String writeIdList, long tableId)
      throws MetaException {
    return null;
  }

  @Override
  public boolean addPartition(Partition part) throws InvalidObjectException, MetaException {

    return false;
  }

  @Override
  public Partition getPartition(String catName, String dbName, String tableName, List<String> part_vals)
      throws MetaException, NoSuchObjectException {

    return null;
  }

  @Override
  public Partition getPartition(String catName, String dbName, String tableName, List<String> part_vals,
                                String  writeIdList)
      throws MetaException, NoSuchObjectException {
    return null;
  }

  @Override
  public boolean dropPartition(String catName, String dbName, String tableName, List<String> part_vals)
      throws MetaException {

    return false;
  }

  @Override
  public boolean dropPartition(String catName, String dbName, String tableName, String partName)
      throws MetaException {

    return false;
  }

  @Override
  public List<Partition> getPartitions(String catName, String dbName, String tableName, int max)
      throws MetaException {

    return Collections.emptyList();
  }

  @Override
  public List<Partition> getPartitions(String catName, String dbName, String tblName,
      GetPartitionsArgs args) throws MetaException, NoSuchObjectException {
    return Collections.emptyList();
  }

  @Override
  public Map<String, String> getPartitionLocations(String catName, String dbName, String tblName,
      String baseLocationToNotShow, int max) {
    return Collections.emptyMap();
  }

  @Override
  public Table alterTable(String catName, String dbname, String name, Table newTable, String queryValidWriteIds)
      throws InvalidObjectException, MetaException {
    return newTable;
  }

  @Override
  public void updateCreationMetadata(String catName, String dbname, String tablename, CreationMetadata cm)
      throws MetaException {
  }

  @Override
  public List<String> getTables(String catName, String dbName, String pattern) throws MetaException {
    return Collections.emptyList();
  }

  @Override
  public List<String> getTables(String catName, String dbName, String pattern, TableType tableType, int limit) throws MetaException {
    return Collections.emptyList();
  }

  @Override
  public List<Table> getAllMaterializedViewObjectsForRewriting(String catName) throws MetaException {
    return Collections.emptyList();
  }

  @Override
  public List<String> getMaterializedViewsForRewriting(String catName, String dbName)
      throws MetaException, NoSuchObjectException {
    return Collections.emptyList();
  }

  @Override
  public List<TableMeta> getTableMeta(String catName, String dbNames, String tableNames, List<String> tableTypes)
      throws MetaException {
    return Collections.emptyList();
  }

  @Override
  public List<Table> getTableObjectsByName(String catName, String dbname, List<String> tableNames)
      throws MetaException, UnknownDBException {

    return Collections.emptyList();
  }

  @Override
  public List<Table> getTableObjectsByName(String catName, String dbname, List<String> tableNames,
          GetProjectionsSpec projectionSpec, String tablePattern) throws MetaException, UnknownDBException {

    return Collections.emptyList();
  }

  @Override
  public List<String> getAllTables(String catName, String dbName) throws MetaException {

    return Collections.emptyList();
  }

  @Override
  public List<String> listTableNamesByFilter(String catName, String dbName, String filter, short max_tables)
      throws MetaException, UnknownDBException {

    return Collections.emptyList();
  }

  @Override
  public List<String> listPartitionNames(String catName, String db_name, String tbl_name, short max_parts)
      throws MetaException {

    return Collections.emptyList();
  }

  @Override
  public List<String> listPartitionNames(String catName, String dbName, String tblName, String defaultPartName,
      byte[] exprBytes, String order, int maxParts) throws MetaException, NoSuchObjectException {

    return Collections.emptyList();
  }

  @Override
  public List<String> listPartitionNamesByFilter(String catName, String dbName, String tblName,
      GetPartitionsArgs args) throws MetaException, NoSuchObjectException {
    return Collections.emptyList();
  }

  @Override
  public PartitionValuesResponse listPartitionValues(String catName, String db_name,
                                                     String tbl_name, List<FieldSchema> cols,
                                                     boolean applyDistinct, String filter,
                                                     boolean ascending, List<FieldSchema> order,
                                                     long maxParts) throws MetaException {
    return null;
  }

  @Override
  public Partition alterPartition(String catName, String db_name, String tbl_name, List<String> part_vals,
      Partition new_part, String queryValidWriteIds) throws InvalidObjectException, MetaException {
    return new_part;
  }

  @Override
  public List<Partition> alterPartitions(String catName, String db_name, String tbl_name,
      List<List<String>> part_vals_list, List<Partition> new_parts,
      long writeId, String queryValidWriteIds) throws InvalidObjectException, MetaException {
    return new_parts;
  }

  @Override
  public List<Partition> getPartitionsByFilter(String catName, String dbName, String tblName,
                                               String filter, short maxParts)
      throws MetaException, NoSuchObjectException {

    return Collections.emptyList();
  }

  @Override
  public List<Partition> getPartitionsByFilter(String catName, String dbName, String tblName,
       GetPartitionsArgs args)
       throws MetaException, NoSuchObjectException {
    return Collections.emptyList();
  }

  @Override
  public List<Partition> getPartitionSpecsByFilterAndProjection(Table table,
      GetProjectionsSpec projectSpec, GetPartitionsFilterSpec filterSpec)
      throws MetaException, NoSuchObjectException {
    return Collections.emptyList();
  }

  @Override
  public List<Partition> getPartitionsByNames(String catName, String dbName, String tblName,
      List<String> partNames) throws MetaException, NoSuchObjectException {

    return Collections.emptyList();
  }

  @Override
  public List<Partition> getPartitionsByNames(String catName, String dbName, String tblName,
      GetPartitionsArgs args) throws MetaException, NoSuchObjectException {
    return Collections.emptyList();
  }

  @Override
  public boolean getPartitionsByExpr(String catName, String dbName, String tblName, byte[] expr,
      String defaultPartitionName, short maxParts, List<Partition> result) throws TException {
    return false;
  }

  @Override
  public boolean getPartitionsByExpr(String catName, String dbName, String tblName,
      List<Partition> result, GetPartitionsArgs args) throws TException {
    return false;
  }

  @Override
  public int getNumPartitionsByFilter(String catName, String dbName, String tblName, String filter)
    throws MetaException, NoSuchObjectException {
    return -1;
  }

  @Override
  public int getNumPartitionsByExpr(String catName, String dbName, String tblName, byte[] expr)
      throws MetaException, NoSuchObjectException {
    return -1;
  }

  @Override
  public int getNumPartitionsByPs(String catName, String dbName, String tblName, List<String> partVals)
      throws MetaException, NoSuchObjectException {
    return -1;
  }

  @Override
  public Table markPartitionForEvent(String catName, String dbName, String tblName, Map<String, String> partVals,
      PartitionEventType evtType) throws MetaException, UnknownTableException,
      InvalidPartitionException, UnknownPartitionException {

    return null;
  }

  @Override
  public boolean isPartitionMarkedForEvent(String catName, String dbName, String tblName,
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
  public PrincipalPrivilegeSet getDBPrivilegeSet(String catName, String dbName, String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {

    return null;
  }

  @Override
  public PrincipalPrivilegeSet getConnectorPrivilegeSet(String catName, String connectorName, String userName,
      List<String> groupNames) throws InvalidObjectException, MetaException {

    return null;
  }

  @Override
  public PrincipalPrivilegeSet getTablePrivilegeSet(String catName, String dbName, String tableName,
      String userName, List<String> groupNames) throws InvalidObjectException, MetaException {

    return null;
  }

  @Override
  public PrincipalPrivilegeSet getPartitionPrivilegeSet(String catName, String dbName, String tableName,
      String partition, String userName, List<String> groupNames) throws InvalidObjectException,
      MetaException {

    return null;
  }

  @Override
  public PrincipalPrivilegeSet getColumnPrivilegeSet(String catName, String dbName, String tableName,
      String partitionName, String columnName, String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {

    return null;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalGlobalGrants(String principalName,
      PrincipalType principalType) {

    return Collections.emptyList();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDBGrants(String principalName,
      PrincipalType principalType, String catName, String dbName) {

    return Collections.emptyList();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDCGrants(String principalName,
      PrincipalType principalType, String dcName) {

    return Collections.emptyList();
  }

  @Override
  public List<HiveObjectPrivilege> listAllTableGrants(String principalName,
      PrincipalType principalType, String catName, String dbName, String tableName) {

    return Collections.emptyList();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionGrants(String principalName,
      PrincipalType principalType, String catName, String dbName, String tableName, List<String> partValues,
      String partName) {

    return Collections.emptyList();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableColumnGrants(String principalName,
      PrincipalType principalType, String catName, String dbName, String tableName, String columnName) {

    return Collections.emptyList();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(String principalName,
      PrincipalType principalType, String catName, String dbName, String tableName, List<String> partVals,
      String partName, String columnName) {

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
  public boolean refreshPrivileges(HiveObjectRef objToRefresh, String authorizer, PrivilegeBag grantPrivileges)
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
  public List<Role> listRoles(String principalName, PrincipalType principalType) {

    return Collections.emptyList();
  }

  @Override
  public List<RolePrincipalGrant> listRolesWithGrants(String principalName,
                                                      PrincipalType principalType) {
    return Collections.emptyList();
  }

  @Override
  public List<RolePrincipalGrant> listRoleMembers(String roleName) {
    return null;
  }

  @Override
  public Partition getPartitionWithAuth(String catName, String dbName, String tblName, List<String> partVals,
      String user_name, List<String> group_names) throws MetaException, NoSuchObjectException,
      InvalidObjectException {

    return null;
  }

  @Override
  public List<Partition> getPartitionsWithAuth(String catName, String dbName, String tblName, short maxParts,
      String userName, List<String> groupNames) throws MetaException, NoSuchObjectException,
      InvalidObjectException {

    return Collections.emptyList();
  }

  @Override
  public List<String> listPartitionNamesPs(String catName, String db_name, String tbl_name, List<String> part_vals,
      short max_parts) throws MetaException, NoSuchObjectException {

    return Collections.emptyList();
  }

  @Override
  public List<Partition> listPartitionsPsWithAuth(String catName, String db_name, String tbl_name,
      List<String> part_vals, short max_parts, String userName, List<String> groupNames)
      throws MetaException, InvalidObjectException, NoSuchObjectException {

    return Collections.emptyList();
  }

  @Override
  public List<Partition> listPartitionsPsWithAuth(String catName, String db_name, String tbl_name,
       GetPartitionsArgs args)
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
  public List<HiveObjectPrivilege> listPrincipalDCGrantsAll(
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
  public List<HiveObjectPrivilege> listDBGrantsAll(String catName, String dbName) {
    return Collections.emptyList();
  }

  @Override
  public List<HiveObjectPrivilege> listDCGrantsAll(String dcName) {
    return Collections.emptyList();
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionColumnGrantsAll(String catName, String dbName, String tableName, String partitionName, String columnName) {
    return Collections.emptyList();
  }

  @Override
  public List<HiveObjectPrivilege> listTableGrantsAll(String catName, String dbName, String tableName) {
    return Collections.emptyList();
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionGrantsAll(String catName, String dbName, String tableName, String partitionName) {
    return Collections.emptyList();
  }

  @Override
  public List<HiveObjectPrivilege> listTableColumnGrantsAll(String catName, String dbName, String tableName, String columnName) {
    return Collections.emptyList();
  }

  @Override
  public List<ColumnStatistics> getTableColumnStatistics(String catName, String dbName, String tableName,
      List<String> colName) throws MetaException, NoSuchObjectException {
    return null;
  }

  @Override
  public  ColumnStatistics getTableColumnStatistics(String catName, String dbName, String tableName,
      List<String> colName, String engine) throws MetaException, NoSuchObjectException {
    return null;
  }

  @Override
  public ColumnStatistics getTableColumnStatistics(
      String catName, String dbName, String tableName, List<String> colName,
      String engine, String  writeIdList)
      throws MetaException, NoSuchObjectException {
    return null;
  }

  @Override
  public boolean deleteTableColumnStatistics(String catName, String dbName, String tableName,
      String colName, String engine)
      throws NoSuchObjectException, MetaException, InvalidObjectException {
    return false;
  }


  @Override
  public boolean deletePartitionColumnStatistics(String catName, String dbName, String tableName,
    String partName, List<String> partVals, String colName, String engine)
    throws NoSuchObjectException, MetaException, InvalidObjectException,
    InvalidInputException {
    return false;

  }

  @Override
  public Map<String, String> updateTableColumnStatistics(ColumnStatistics statsObj,
      String  validWriteIds, long writeId)
      throws NoSuchObjectException, MetaException, InvalidObjectException {
    return null;
  }

  @Override
  public Map<String, String> updatePartitionColumnStatistics(Table table, MTable mTable, 
      ColumnStatistics statsObj, List<String> partVals, String  validWriteIds, long writeId)
    throws NoSuchObjectException, MetaException, InvalidObjectException {
    return null;
  }

  @Override public Map<String, String> updatePartitionColumnStatistics(ColumnStatistics statsObj, List<String> partVals,
      String validWriteIds, long writeId)
    throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    return null;
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
  public List<List<ColumnStatistics>> getPartitionColumnStatistics(String catName, String dbName,
      String tblName, List<String> colNames, List<String> partNames)
      throws MetaException, NoSuchObjectException {
    return Collections.emptyList();
  }

  @Override
  public List<ColumnStatistics> getPartitionColumnStatistics(String catName, String dbName,
      String tblName, List<String> colNames, List<String> partNames, String engine)
      throws MetaException, NoSuchObjectException {
    return Collections.emptyList();
  }

  @Override
  public List<ColumnStatistics> getPartitionColumnStatistics(
      String catName, String dbName, String tblName, List<String> partNames,
      List<String> colNames, String engine, String  writeIdList)
      throws MetaException, NoSuchObjectException {
    return Collections.emptyList();
  }

  @Override
  public boolean doesPartitionExist(String catName, String dbName, String tableName,
      List<FieldSchema> partKeys, List<String> partVals)
      throws MetaException, NoSuchObjectException {
    return false;
  }

  @Override
  public boolean addPartitions(String catName, String dbName, String tblName, List<Partition> parts)
      throws InvalidObjectException, MetaException {
    return false;
  }

  @Override
  public boolean addPartitions(String catName, String dbName, String tblName, PartitionSpecProxy partitionSpec, boolean ifNotExists) throws InvalidObjectException, MetaException {
    return false;
  }

  @Override
  public void dropPartitions(String catName, String dbName, String tblName, List<String> partNames) {
  }

  @Override
  public void createFunction(Function func) throws InvalidObjectException,
      MetaException {
  }

  @Override
  public void alterFunction(String catName, String dbName, String funcName, Function newFunction)
      throws InvalidObjectException, MetaException {
  }

  @Override
  public void dropFunction(String catName, String dbName, String funcName)
      throws MetaException, NoSuchObjectException, InvalidObjectException,
      InvalidInputException {
  }

  @Override
  public Function getFunction(String catName, String dbName, String funcName)
      throws MetaException {
    return null;
  }

  @Override
  public List<Function> getAllFunctions(String catName)
          throws MetaException {
    return Collections.emptyList();
  }

  @Override
  public List<String> getFunctions(String catName, String dbName, String pattern)
      throws MetaException {
    return Collections.emptyList();
  }

  @Override
  public AggrStats get_aggr_stats_for(String catName, String dbName,
      String tblName, List<String> partNames, List<String> colNames,
      String engine)
      throws MetaException {
    return null;
  }

  @Override
  public AggrStats get_aggr_stats_for(
      String catName, String dbName, String tblName, List<String> partNames,
      List<String> colNames, String engine, String  writeIdList)
      throws MetaException, NoSuchObjectException {
    return null;
  }

  @Override
  public NotificationEventResponse getNextNotification(NotificationEventRequest rqst) {
    return null;
  }

  @Override
  public void addNotificationEvent(NotificationEvent event) throws MetaException {

  }

  @Override
  public void cleanNotificationEvents(int olderThan) {

  }

  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() {
    return null;
  }

  @Override
  public NotificationEventsCountResponse getNotificationEventsCount(NotificationEventsCountRequest rqst) {
    return null;
  }

  @Override
  public void flushCache() {

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
    return 0;
  }

  @Override
  public int getPartitionCount() throws MetaException {
    return 0;
  }

  @Override
  public int getDatabaseCount() throws MetaException {
    return 0;
  }

  @Override
  public FileMetadataHandler getFileMetadataHandler(FileMetadataExprType type) {
    return null;
  }

  @Override
  public List<SQLPrimaryKey> getPrimaryKeys(String catName, String db_name, String tbl_name)
    throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SQLPrimaryKey> getPrimaryKeys(PrimaryKeysRequest request) throws MetaException {
    return null;
  }

  @Override
  public List<SQLForeignKey> getForeignKeys(String catName, String parent_db_name,
    String parent_tbl_name, String foreign_db_name, String foreign_tbl_name)
    throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SQLForeignKey> getForeignKeys(ForeignKeysRequest request) throws MetaException {
    return null;
  }

  @Override
  public List<SQLUniqueConstraint> getUniqueConstraints(String catName, String db_name, String tbl_name)
    throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SQLUniqueConstraint> getUniqueConstraints(UniqueConstraintsRequest request) throws MetaException {
    return null;
  }

  @Override
  public List<SQLNotNullConstraint> getNotNullConstraints(String catName, String db_name, String tbl_name)
    throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SQLNotNullConstraint> getNotNullConstraints(NotNullConstraintsRequest request) throws MetaException {
    return null;
  }

  @Override
  public List<SQLDefaultConstraint> getDefaultConstraints(String catName, String db_name, String tbl_name)
      throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SQLDefaultConstraint> getDefaultConstraints(DefaultConstraintsRequest request) throws MetaException {
    return null;
  }

  @Override
  public List<SQLCheckConstraint> getCheckConstraints(String catName, String db_name, String tbl_name)
      throws MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SQLCheckConstraint> getCheckConstraints(CheckConstraintsRequest request) throws MetaException {
    return null;
  }

  @Override
  public SQLAllTableConstraints getAllTableConstraints(String catName, String dbName, String tblName)
      throws MetaException, NoSuchObjectException {
    return null;
  }

  @Override
  public SQLAllTableConstraints getAllTableConstraints(AllTableConstraintsRequest request)
      throws MetaException, NoSuchObjectException {
    return null;
  }

  @Override
  public SQLAllTableConstraints createTableWithConstraints(Table tbl, SQLAllTableConstraints constraints)
    throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void dropConstraint(String catName, String dbName, String tableName,
  String constraintName, boolean missingOk) throws NoSuchObjectException {
    // TODO Auto-generated method stub
  }

  @Override
  public List<SQLPrimaryKey> addPrimaryKeys(List<SQLPrimaryKey> pks)
    throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SQLForeignKey> addForeignKeys(List<SQLForeignKey> fks)
    throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SQLUniqueConstraint> addUniqueConstraints(List<SQLUniqueConstraint> uks)
    throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SQLNotNullConstraint> addNotNullConstraints(List<SQLNotNullConstraint> nns)
    throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SQLDefaultConstraint> addDefaultConstraints(List<SQLDefaultConstraint> nns)
      throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public List<SQLCheckConstraint> addCheckConstraints(List<SQLCheckConstraint> nns)
      throws InvalidObjectException, MetaException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String getMetastoreDbUuid() throws MetaException {
    throw new MetaException("Get metastore uuid is not implemented");
  }

  @Override
  public void createResourcePlan(
      WMResourcePlan resourcePlan, String copyFrom, int defaultPoolSize) throws MetaException {
  }

  @Override
  public WMFullResourcePlan getResourcePlan(String name, String ns) throws NoSuchObjectException {
    return null;
  }

  @Override
  public List<WMResourcePlan> getAllResourcePlans(String ns) throws MetaException {
    return null;
  }

  @Override
  public WMFullResourcePlan alterResourcePlan(
      String name, String ns, WMNullableResourcePlan resourcePlan, boolean canActivateDisabled, boolean canDeactivate,
      boolean isReplace)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
    return null;
  }

  @Override
  public WMFullResourcePlan getActiveResourcePlan(String ns) throws MetaException {
    return null;
  }

  @Override
  public WMValidateResourcePlanResponse validateResourcePlan(String name, String ns)
      throws NoSuchObjectException, InvalidObjectException, MetaException {
    return null;
  }

  @Override
  public void dropResourcePlan(String name, String ns) throws NoSuchObjectException, MetaException {
  }

  @Override
  public void createWMTrigger(WMTrigger trigger) throws MetaException {
  }

  @Override
  public void alterWMTrigger(WMTrigger trigger)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
  }

  @Override
  public void dropWMTrigger(String resourcePlanName, String triggerName, String ns)
      throws NoSuchObjectException, MetaException {
  }

  @Override
  public List<WMTrigger> getTriggersForResourcePlan(String resourcePlanName, String ns)
      throws NoSuchObjectException, MetaException {
    return null;
  }

  @Override
  public void createPool(WMPool pool) throws AlreadyExistsException, NoSuchObjectException,
      InvalidOperationException, MetaException {
  }

  @Override
  public void alterPool(WMNullablePool pool, String poolPath) throws AlreadyExistsException,
      NoSuchObjectException, InvalidOperationException, MetaException {
  }

  @Override
  public void dropWMPool(String resourcePlanName, String poolPath, String ns)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
  }

  @Override
  public void createOrUpdateWMMapping(WMMapping mapping, boolean update)
      throws AlreadyExistsException, NoSuchObjectException, InvalidOperationException,
      MetaException {
  }

  @Override
  public void dropWMMapping(WMMapping mapping)
      throws NoSuchObjectException, InvalidOperationException, MetaException {
  }

  @Override
  public void createWMTriggerToPoolMapping(String resourcePlanName, String triggerName,
      String poolPath, String ns) throws AlreadyExistsException, NoSuchObjectException,
      InvalidOperationException, MetaException {
  }

  @Override
  public void dropWMTriggerToPoolMapping(String resourcePlanName, String triggerName,
      String poolPath, String ns) throws NoSuchObjectException, InvalidOperationException, MetaException {
  }

  @Override
  public List<MetaStoreServerUtils.ColStatsObjWithSourceInfo> getPartitionColStatsForDatabase(String catName, String dbName)
      throws MetaException, NoSuchObjectException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void createISchema(ISchema schema) throws AlreadyExistsException, MetaException {

  }

  @Override
  public void alterISchema(ISchemaName schemaName, ISchema newSchema) throws NoSuchObjectException,
      MetaException {

  }

  @Override
  public ISchema getISchema(ISchemaName schemaName) throws MetaException {
    return null;
  }

  @Override
  public void dropISchema(ISchemaName schemaName) throws NoSuchObjectException, MetaException {

  }

  @Override
  public void addSchemaVersion(SchemaVersion schemaVersion) throws
      AlreadyExistsException, InvalidObjectException, NoSuchObjectException, MetaException {

  }

  @Override
  public void alterSchemaVersion(SchemaVersionDescriptor version, SchemaVersion newVersion) throws
      NoSuchObjectException, MetaException {

  }

  @Override
  public SchemaVersion getSchemaVersion(SchemaVersionDescriptor version) throws MetaException {
    return null;
  }

  @Override
  public SchemaVersion getLatestSchemaVersion(ISchemaName schemaName) throws MetaException {
    return null;
  }

  @Override
  public List<SchemaVersion> getAllSchemaVersion(ISchemaName schemaName) throws MetaException {
    return null;
  }

  @Override
  public List<SchemaVersion> getSchemaVersionsByColumns(String colName, String colNamespace,
                                                        String type) throws MetaException {
    return null;
  }

  @Override
  public void dropSchemaVersion(SchemaVersionDescriptor version) throws NoSuchObjectException,
      MetaException {

  }

  @Override
  public SerDeInfo getSerDeInfo(String serDeName) throws MetaException {
    return null;
  }

  @Override
  public void addSerde(SerDeInfo serde) throws AlreadyExistsException, MetaException {

  }

  @Override
  public void addRuntimeStat(RuntimeStat stat) throws MetaException {
  }

  @Override
  public List<RuntimeStat> getRuntimeStats(int maxEntries, int maxCreateTime) throws MetaException {
    return Collections.emptyList();
  }

  @Override
  public int deleteRuntimeStats(int maxRetainSecs) throws MetaException {
    return 0;
  }

  @Override
  public List<TableName> getTableNamesWithStats() throws MetaException,
      NoSuchObjectException {
    return null;
  }

  @Override
  public List<TableName> getAllTableNamesForStats() throws MetaException,
      NoSuchObjectException {
    return null;
  }

  @Override
  public Map<String, List<String>> getPartitionColsWithStats(String catName,
      String dbName, String tableName) throws MetaException,
      NoSuchObjectException {
    return null;
  }

  @Override
  public void cleanWriteNotificationEvents(int olderThan) {
  }

  @Override
  public List<WriteEventInfo> getAllWriteEventInfo(long txnId, String dbName, String tableName) throws MetaException {
    return null;
  }

  @Override
  public List<String> isPartOfMaterializedView(String catName, String dbName, String tblName) {
    throw new RuntimeException("unimplemented");
  }

  @Override
  public ScheduledQueryPollResponse scheduledQueryPoll(ScheduledQueryPollRequest request) {
    throw new RuntimeException("unimplemented");
  }

  @Override
  public void scheduledQueryMaintenance(ScheduledQueryMaintenanceRequest request) {
  }

  @Override
  public void scheduledQueryProgress(ScheduledQueryProgressInfo info) {
  }

  @Override
  public void addReplicationMetrics(ReplicationMetricList replicationMetricList) {
    throw new RuntimeException("unimplemented");
  }

  @Override
  public ReplicationMetricList getReplicationMetrics(GetReplicationMetricsRequest replicationMetricsRequest) {
    throw new RuntimeException("unimplemented");
  }

  @Override
  public int deleteReplicationMetrics(int maxRetainSecs) {
    throw new RuntimeException("unimplemented");
  }

  @Override
  public ScheduledQuery getScheduledQuery(ScheduledQueryKey scheduleKey) {
    throw new RuntimeException("unimplemented");
  }

  @Override
  public int deleteScheduledExecutions(int maxRetainSecs) {
    throw new RuntimeException("unimplemented");
  }

  @Override
  public int markScheduledExecutionsTimedOut(int timeoutSecs) throws InvalidOperationException{
    throw new RuntimeException("unimplemented");
  }

  @Override
  public void deleteAllPartitionColumnStatistics(TableName tableName, String w) {
    throw new RuntimeException("unimplemented");
  }

  @Override
  public void createOrUpdateStoredProcedure(StoredProcedure proc) throws NoSuchObjectException, MetaException {
    throw new RuntimeException("unimplemented");
  }

  @Override
  public StoredProcedure getStoredProcedure(String catName, String db, String name) throws MetaException {
    return null;
  }

  @Override
  public void dropStoredProcedure(String catName, String dbName, String funcName) throws MetaException {

  }

  @Override
  public List<String> getAllStoredProcedures(ListStoredProcedureRequest request) {
    return Collections.emptyList();
  }

  @Override
  public void addPackage(AddPackageRequest request) {
    throw new RuntimeException("unimplemented");
  }

  @Override
  public Package findPackage(GetPackageRequest request) {
    return null;
  }

  @Override
  public List<String> listPackages(ListPackageRequest request) {
    return Collections.emptyList();
  }

  @Override
  public void dropPackage(DropPackageRequest request) {
  }

  @Override
  public MTable ensureGetMTable(String catName, String dbName, String tblName) throws NoSuchObjectException {
    return null;
  }

  @Override
  public Map<String, Map<String, String>> updatePartitionColumnStatisticsInBatch(
          Map<String, ColumnStatistics> partColStatsMap,
          Table tbl, List<TransactionalMetaStoreEventListener> listeners,
          String validWriteIds, long writeId)
          throws MetaException, InvalidObjectException {
    return null;
  }
}
