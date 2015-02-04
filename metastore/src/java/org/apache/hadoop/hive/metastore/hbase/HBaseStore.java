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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.hbase;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implementation of RawStore that stores data in HBase
 */
public class HBaseStore implements RawStore {
  static final private Log LOG = LogFactory.getLog(HBaseStore.class.getName());

  // Do not access this directly, call getHBase to make sure it is initialized.
  private HBaseReadWrite hbase = null;
  private Configuration conf;
  private int txnNestLevel = 0;

  public HBaseStore() {
  }

  @Override
  public void shutdown() {
    try {
      if (txnNestLevel != 0) rollbackTransaction();
      getHBase().close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean openTransaction() {
    if (txnNestLevel++ == 0) getHBase().begin();
    return true;
  }

  @Override
  public boolean commitTransaction() {
    if (txnNestLevel-- < 1) getHBase().commit();
    return true;
  }

  @Override
  public void rollbackTransaction() {
    txnNestLevel = 0;
    getHBase().rollback();
  }

  @Override
  public void createDatabase(Database db) throws InvalidObjectException, MetaException {
    try {
      // HiveMetaStore already checks for existence of the database, don't recheck
      getHBase().putDb(db);
    } catch (IOException e) {
      // TODO NOt sure what i should throw here
      LOG.error("Unable to create database ", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    }

  }

  @Override
  public Database getDatabase(String name) throws NoSuchObjectException {
    try {
      Database db = getHBase().getDb(name);
      if (db == null) {
        throw new NoSuchObjectException("Unable to find db " + name);
      }
      return db;
    } catch (IOException e) {
      LOG.error("Unable to get db", e);
      throw new NoSuchObjectException("Error reading db " + e.getMessage());
    }
  }

  @Override
  public boolean dropDatabase(String dbname) throws NoSuchObjectException, MetaException {
    try {
      getHBase().deleteDb(dbname);
      return true;
    } catch (IOException e) {
      LOG.error("Unable to delete db" + e);
      throw new MetaException("Unable to drop database " + dbname);
    }
  }

  @Override
  public boolean alterDatabase(String dbname, Database db) throws NoSuchObjectException,
      MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getDatabases(String pattern) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getAllDatabases() throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean createType(Type type) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Type getType(String typeName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropType(String typeName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createTable(Table tbl) throws InvalidObjectException, MetaException {
    // HiveMetaStore above us checks if the table already exists, so we can blindly store it here.
    try {
      getHBase().putTable(tbl);
    } catch (IOException e) {
      // TODO NOt sure what i should throw here
      LOG.error("Unable to create table ", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    }
  }

  @Override
  public boolean dropTable(String dbName, String tableName) throws MetaException,
      NoSuchObjectException, InvalidObjectException, InvalidInputException {
    try {
      getHBase().deleteTable(dbName, tableName);
      return true;
    } catch (IOException e) {
      LOG.error("Unable to delete db" + e);
      throw new MetaException("Unable to drop table " + tableName(dbName, tableName));
    }
  }

  @Override
  public Table getTable(String dbName, String tableName) throws MetaException {
    try {
      Table table = getHBase().getTable(dbName, tableName);
      if (table == null) {
        LOG.debug("Unable to find table " + tableName(dbName, tableName));
      }
      return table;
    } catch (IOException e) {
      LOG.error("Unable to get table", e);
      throw new MetaException("Error reading table " + e.getMessage());
    }
  }

  @Override
  public boolean addPartition(Partition part) throws InvalidObjectException, MetaException {
    try {
      getHBase().putPartition(part);
      return true;
    } catch (IOException e) {
      // TODO NOt sure what i should throw here
      LOG.error("Unable to add partition", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    }
  }

  @Override
  public boolean addPartitions(String dbName, String tblName, List<Partition> parts) throws
      InvalidObjectException, MetaException {
    try {
      for (Partition part : parts) {
        getHBase().putPartition(part);
      }
      return true;
    } catch (IOException e) {
      // TODO NOt sure what i should throw here
      LOG.error("Unable to add partitions", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    }
  }

  @Override
  public boolean addPartitions(String dbName, String tblName, PartitionSpecProxy partitionSpec,
                               boolean ifNotExists) throws InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Partition getPartition(String dbName, String tableName, List<String> part_vals) throws
      MetaException, NoSuchObjectException {
    try {
      Partition part = getHBase().getPartition(dbName, tableName, part_vals);
      if (part == null) {
        throw new NoSuchObjectException("Unable to find partition " +
            partName(dbName, tableName, part_vals));
      }
      return part;
    } catch (IOException e) {
      LOG.error("Unable to get partition", e);
      throw new MetaException("Error reading partition " + e.getMessage());
    }
  }

  @Override
  public boolean doesPartitionExist(String dbName, String tableName, List<String> part_vals) throws
      MetaException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropPartition(String dbName, String tableName, List<String> part_vals) throws
      MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    try {
      getHBase().deletePartition(dbName, tableName, part_vals);
      return true;
    } catch (IOException e) {
      LOG.error("Unable to delete db" + e);
      throw new MetaException("Unable to drop partition " + partName(dbName, tableName, part_vals));
    }
  }

  @Override
  public List<Partition> getPartitions(String dbName, String tableName, int max) throws
      MetaException, NoSuchObjectException {
    try {
      return getHBase().scanPartitionsInTable(dbName, tableName, max);
    } catch (IOException e) {
      LOG.error("Unable to get partitions", e);
      throw new MetaException("Error scanning partitions");
    }
  }

  @Override
  public void alterTable(String dbname, String name, Table newTable) throws InvalidObjectException,
      MetaException {
    // HiveMetaStore above us has already confirmed the table exists, I'm not rechecking
    try {
      getHBase().putTable(newTable);
    } catch (IOException e) {
      LOG.error("Unable to alter table " + tableName(dbname, name), e);
      throw new MetaException("Unable to alter table " + tableName(dbname, name));
    }
  }

  @Override
  public List<String> getTables(String dbName, String pattern) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Table> getTableObjectsByName(String dbname, List<String> tableNames) throws
      MetaException, UnknownDBException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getAllTables(String dbName) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter, short max_tables) throws
      MetaException, UnknownDBException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> listPartitionNames(String db_name, String tbl_name, short max_parts) throws
      MetaException {
    try {
      List<Partition> parts = getHBase().scanPartitionsInTable(db_name, tbl_name, max_parts);
      if (parts == null) return null;
      List<String> names = new ArrayList<String>(parts.size());
      Table table = getHBase().getTable(db_name, tbl_name);
      for (Partition p : parts) {
        names.add(partName(table, p));
      }
      return names;
    } catch (IOException e) {
      LOG.error("Unable to get partitions", e);
      throw new MetaException("Error scanning partitions");
    }
  }

  @Override
  public List<String> listPartitionNamesByFilter(String db_name, String tbl_name, String filter,
                                                 short max_parts) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartition(String db_name, String tbl_name, List<String> part_vals,
                             Partition new_part) throws InvalidObjectException, MetaException {

  }

  @Override
  public void alterPartitions(String db_name, String tbl_name, List<List<String>> part_vals_list,
                              List<Partition> new_parts) throws InvalidObjectException,
      MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addIndex(Index index) throws InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Index getIndex(String dbName, String origTableName, String indexName) throws
      MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean dropIndex(String dbName, String origTableName, String indexName) throws
      MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Index> getIndexes(String dbName, String origTableName, int max) throws MetaException {
    // TODO - Index not currently supported.  But I need to return an empty list or else drop
    // table cores.
    return new ArrayList<Index>();
  }

  @Override
  public List<String> listIndexNames(String dbName, String origTableName, short max) throws
      MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterIndex(String dbname, String baseTblName, String name, Index newIndex) throws
      InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<Partition> getPartitionsByFilter(String dbName, String tblName, String filter,
                                               short maxParts) throws MetaException,
      NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean getPartitionsByExpr(String dbName, String tblName, byte[] expr,
                                     String defaultPartitionName, short maxParts,
                                     List<Partition> result) throws TException {
    // TODO for now just return all partitions, need to add real expression parsing later.
    result.addAll(getPartitions(dbName, tblName, maxParts));
    return true;
  }

  @Override
  public List<Partition> getPartitionsByNames(String dbName, String tblName,
                                              List<String> partNames) throws MetaException,
      NoSuchObjectException {
    List<Partition> parts = new ArrayList<Partition>();
    for (String partName : partNames) {
      parts.add(getPartition(dbName, tblName, partNameToVals(partName)));
    }
    return parts;
  }

  @Override
  public Table markPartitionForEvent(String dbName, String tblName, Map<String, String> partVals,
                                     PartitionEventType evtType) throws MetaException,
      UnknownTableException, InvalidPartitionException, UnknownPartitionException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean isPartitionMarkedForEvent(String dbName, String tblName,
                                           Map<String, String> partName,
                                           PartitionEventType evtType) throws MetaException,
      UnknownTableException, InvalidPartitionException, UnknownPartitionException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addRole(String roleName, String ownerName) throws InvalidObjectException,
      MetaException, NoSuchObjectException {
    int now = (int)(System.currentTimeMillis()/1000);
    Role role = new Role(roleName, now, ownerName);
    try {
      if (getHBase().getRole(roleName) != null) {
        throw new InvalidObjectException("Role " + roleName + " already exists");
      }
      getHBase().putRole(role);
      return true;
    } catch (IOException e) {
      // TODO NOt sure what i should throw here
      LOG.error("Unable to create role ", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    }
  }

  @Override
  public boolean removeRole(String roleName) throws MetaException, NoSuchObjectException {
    try {
      getHBase().deleteRole(roleName);
      return true;
    } catch (IOException e) {
      LOG.error("Unable to delete role" + e);
      throw new MetaException("Unable to drop role " + roleName);
    }
  }

  @Override
  public boolean grantRole(Role role, String userName, PrincipalType principalType, String grantor,
                           PrincipalType grantorType, boolean grantOption) throws MetaException,
      NoSuchObjectException, InvalidObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean revokeRole(Role role, String userName, PrincipalType principalType,
                            boolean grantOption) throws MetaException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PrincipalPrivilegeSet getUserPrivilegeSet(String userName, List<String> groupNames) throws
      InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PrincipalPrivilegeSet getDBPrivilegeSet(String dbName, String userName,
                                                 List<String> groupNames) throws
      InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PrincipalPrivilegeSet getTablePrivilegeSet(String dbName, String tableName,
                                                    String userName, List<String> groupNames) throws
      InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PrincipalPrivilegeSet getPartitionPrivilegeSet(String dbName, String tableName,
                                                        String partition, String userName,
                                                        List<String> groupNames) throws
      InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public PrincipalPrivilegeSet getColumnPrivilegeSet(String dbName, String tableName,
                                                     String partitionName, String columnName,
                                                     String userName,
                                                     List<String> groupNames) throws
      InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<MGlobalPrivilege> listPrincipalGlobalGrants(String principalName,
                                                          PrincipalType principalType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<MDBPrivilege> listPrincipalDBGrants(String principalName, PrincipalType principalType,
                                                  String dbName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<MTablePrivilege> listAllTableGrants(String principalName, PrincipalType principalType,
                                                  String dbName, String tableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<MPartitionPrivilege> listPrincipalPartitionGrants(String principalName,
                                                                PrincipalType principalType,
                                                                String dbName, String tableName,
                                                                String partName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<MTableColumnPrivilege> listPrincipalTableColumnGrants(String principalName,
                                                                    PrincipalType principalType,
                                                                    String dbName, String tableName,
                                                                    String columnName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<MPartitionColumnPrivilege> listPrincipalPartitionColumnGrants(String principalName,
                                                                            PrincipalType principalType,
                                                                            String dbName,
                                                                            String tableName,
                                                                            String partName,
                                                                            String columnName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean grantPrivileges(PrivilegeBag privileges) throws InvalidObjectException,
      MetaException, NoSuchObjectException {
    // TODO
    return true;
  }

  @Override
  public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption) throws
      InvalidObjectException, MetaException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Role getRole(String roleName) throws NoSuchObjectException {
    try {
      Role role = getHBase().getRole(roleName);
      if (role == null) {
        throw new NoSuchObjectException("Unable to find role " + roleName);
      }
      return role;
    } catch (IOException e) {
      LOG.error("Unable to get role", e);
      throw new NoSuchObjectException("Error reading table " + e.getMessage());
    }
  }

  @Override
  public List<String> listRoleNames() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<MRoleMap> listRoles(String principalName, PrincipalType principalType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<MRoleMap> listRoleMembers(String roleName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Partition getPartitionWithAuth(String dbName, String tblName, List<String> partVals,
                                        String user_name, List<String> group_names) throws
      MetaException, NoSuchObjectException, InvalidObjectException {
    Partition p = getPartition(dbName, tblName, partVals);
    // TODO check that user is authorized to see these partitions
    return p;
  }

  @Override
  public List<Partition> getPartitionsWithAuth(String dbName, String tblName, short maxParts,
                                               String userName, List<String> groupNames) throws
      MetaException, NoSuchObjectException, InvalidObjectException {
    List<Partition> parts = getPartitions(dbName, tblName, maxParts);
    // TODO check that user is authorized;
    return parts;
  }

  @Override
  public List<String> listPartitionNamesPs(String db_name, String tbl_name, List<String> part_vals,
                                           short max_parts) throws MetaException,
      NoSuchObjectException {
    throw new UnsupportedOperationException();
  }


  @Override
  public List<Partition> listPartitionsPsWithAuth(String db_name, String tbl_name,
                                                  List<String> part_vals, short max_parts,
                                                  String userName, List<String> groupNames) throws
      MetaException, InvalidObjectException, NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics colStats) throws
      NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    try {
      getHBase().updateStatistics(colStats.getStatsDesc().getDbName(),
          colStats.getStatsDesc().getTableName(), null, null, colStats);
      return true;
    } catch (IOException e) {
      LOG.error("Unable to update column statistics", e);
      throw new MetaException("Failed to update column statistics, " + e.getMessage());
    }
  }

  @Override
  public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj,
                                                 List<String> partVals) throws
      NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    try {
      getHBase().updateStatistics(statsObj.getStatsDesc().getDbName(),
          statsObj.getStatsDesc().getTableName(), statsObj.getStatsDesc().getPartName(),
          partVals, statsObj);
      return true;
    } catch (IOException e) {
      LOG.error("Unable to update column statistics", e);
      throw new MetaException("Failed to update column statistics, " + e.getMessage());
    }
  }

  @Override
  public ColumnStatistics getTableColumnStatistics(String dbName, String tableName,
                                                   List<String> colName) throws MetaException,
      NoSuchObjectException {
    try {
      return getHBase().getTableStatistics(dbName, tableName, colName);
    } catch (IOException e) {
      LOG.error("Unable to fetch column statistics", e);
      throw new MetaException("Failed to fetch column statistics, " + e.getMessage());
    }
  }

  @Override
  public List<ColumnStatistics> getPartitionColumnStatistics(String dbName, String tblName,
                                                             List<String> partNames,
                                                             List<String> colNames) throws
      MetaException, NoSuchObjectException {
    List<List<String>> partVals = new ArrayList<List<String>>(partNames.size());
    for (String partName : partNames) partVals.add(partNameToVals(partName));
    try {
      return getHBase().getPartitionStatistics(dbName, tblName, partNames, partVals, colNames);
    } catch (IOException e) {
      LOG.error("Unable to fetch column statistics", e);
      throw new MetaException("Failed fetching column statistics, " + e.getMessage());
    }
  }

  @Override
  public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName,
                                                 List<String> partVals, String colName) throws
      NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    // NOP, stats will be deleted along with the partition when it is dropped.
    return true;
  }

  @Override
  public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName) throws
      NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    // NOP, stats will be deleted along with the table when it is dropped.
    return true;
  }

  @Override
  public long cleanupEvents() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addToken(String tokenIdentifier, String delegationToken) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeToken(String tokenIdentifier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getToken(String tokenIdentifier) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getAllTokenIdentifiers() {
    throw new UnsupportedOperationException();
  }

  @Override
  public int addMasterKey(String key) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void updateMasterKey(Integer seqNo, String key) throws NoSuchObjectException,
      MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean removeMasterKey(Integer keySeq) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String[] getMasterKeys() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void verifySchema() throws MetaException {
    try {
      getHBase().createTablesIfNotExist();
    } catch (IOException e) {
      LOG.fatal("Unable to verify schema ", e);
      throw new MetaException("Unable to verify schema");
    }
  }

  @Override
  public String getMetaStoreSchemaVersion() throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setMetaStoreSchemaVersion(String version, String comment) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropPartitions(String dbName, String tblName, List<String> partNames) throws
      MetaException, NoSuchObjectException {
    try {
      for (String partName : partNames) {
        dropPartition(dbName, tblName, partNameToVals(partName));
      }
    } catch (Exception e) {
      LOG.error("Unable to drop partitions", e);
      throw new NoSuchObjectException("Failure dropping partitions, " + e.getMessage());
    }
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDBGrantsAll(String principalName,
                                                            PrincipalType principalType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableGrantsAll(String principalName,
                                                               PrincipalType principalType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(String principalName,
                                                                   PrincipalType principalType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(String principalName,
                                                                     PrincipalType principalType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(String principalName,
                                                                         PrincipalType principalType) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listGlobalGrantsAll() {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listDBGrantsAll(String dbName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionColumnGrantsAll(String dbName, String tableName,
                                                                String partitionName,
                                                                String columnName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listTableGrantsAll(String dbName, String tableName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionGrantsAll(String dbName, String tableName,
                                                          String partitionName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveObjectPrivilege> listTableColumnGrantsAll(String dbName, String tableName,
                                                            String columnName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void createFunction(Function func) throws InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterFunction(String dbName, String funcName, Function newFunction) throws
      InvalidObjectException, MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropFunction(String dbName, String funcName) throws MetaException,
      NoSuchObjectException, InvalidObjectException, InvalidInputException {
    throw new UnsupportedOperationException();
  }

  @Override
  public Function getFunction(String dbName, String funcName) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<String> getFunctions(String dbName, String pattern) throws MetaException {
    throw new UnsupportedOperationException();
  }

  @Override
  public AggrStats get_aggr_stats_for(String dbName, String tblName, List<String> partNames,
                                      List<String> colNames) throws MetaException,
      NoSuchObjectException {
    throw new UnsupportedOperationException();
  }

  @Override
  public NotificationEventResponse getNextNotification(NotificationEventRequest rqst) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addNotificationEvent(NotificationEvent event) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void cleanNotificationEvents(int olderThan) {
    throw new UnsupportedOperationException();
  }

  @Override
  public CurrentNotificationEventId getCurrentNotificationEventId() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void flushCache() {
    getHBase().flushCatalogCache();
  }

  @Override
  public void setConf(Configuration configuration) {
    conf = configuration;
  }

  @Override
  public Configuration getConf() {
    return conf;

  }

  private HBaseReadWrite getHBase() {
    if (hbase == null) hbase = HBaseReadWrite.getInstance(conf);
    return hbase;
  }

  private String tableName(String dbName, String tableName) {
    return dbName + "." + tableName;
  }

  private String partName(String dbName, String tableName, List<String> partVals) {
    return tableName(dbName, tableName) + StringUtils.join(partVals, ':');
  }

  private String partName(Table table, Partition part) {
    return partName(table, part.getValues());
  }

  static String partName(Table table, List<String> partVals) {
    List<FieldSchema> partCols = table.getPartitionKeys();
    StringBuilder builder = new StringBuilder();
    if (partCols.size() != partVals.size()) {
      throw new RuntimeException("Woh bad, different number of partition cols and vals!");
    }
    for (int i = 0; i < partCols.size(); i++) {
      if (i != 0) builder.append('/');
      builder.append(partCols.get(i).getName());
      builder.append('=');
      builder.append(partVals.get(i));
    }
    return builder.toString();
  }

  private List<String> partNameToVals(String name) {
    if (name == null) return null;
    List<String> vals = new ArrayList<String>();
    String[] kvp = name.split("/");
    for (String kv : kvp) {
      vals.add(kv.substring(kv.indexOf('=') + 1));
    }
    return vals;
  }
}
