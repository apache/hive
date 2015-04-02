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
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.PartFilterExprUtil;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
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
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.hbase.HBaseFilterPlanUtil.PlanResult;
import org.apache.hadoop.hive.metastore.hbase.HBaseFilterPlanUtil.ScanPlan;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.hbase.AggregateStatsCache.AggrColStatsCached;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Implementation of RawStore that stores data in HBase
 */
public class HBaseStore implements RawStore {
  static final private Log LOG = LogFactory.getLog(HBaseStore.class.getName());

  // Do not access this directly, call getHBase to make sure it is initialized.
  private HBaseReadWrite hbase = null;
  private Configuration conf;
  private int txnNestLevel = 0;
  private PartitionExpressionProxy expressionProxy = null;

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
      throw new MetaException("Unable to drop database " + e.getMessage());
    }
  }

  @Override
  public boolean alterDatabase(String dbname, Database db) throws NoSuchObjectException,
      MetaException {
    // ObjectStore fetches the old db before updating it, but I can't see the possible value of
    // that since the caller will have needed to call getDatabase to have the db object.
    try {
      getHBase().putDb(db);
      return true;
    } catch (IOException e) {
      LOG.error("Unable to alter database ", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    }
  }

  @Override
  public List<String> getDatabases(String pattern) throws MetaException {
    try {
      List<Database> dbs = getHBase().scanDatabases(likeToRegex(pattern));
      List<String> dbNames = new ArrayList<String>(dbs.size());
      for (Database db : dbs) dbNames.add(db.getName());
      return dbNames;
    } catch (IOException e) {
      LOG.error("Unable to get databases ", e);
      throw new MetaException("Unable to get databases, " + e.getMessage());
    }
  }

  @Override
  public List<String> getAllDatabases() throws MetaException {
    return getDatabases(null);
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
      throw new MetaException("Unable to drop table " + tableNameForErrorMsg(dbName, tableName));
    }
  }

  @Override
  public Table getTable(String dbName, String tableName) throws MetaException {
    try {
      Table table = getHBase().getTable(dbName, tableName);
      if (table == null) {
        LOG.debug("Unable to find table " + tableNameForErrorMsg(dbName, tableName));
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
      LOG.error("Unable to add partition", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    }
  }

  @Override
  public boolean addPartitions(String dbName, String tblName, List<Partition> parts)
      throws InvalidObjectException, MetaException {
    try {
      getHBase().putPartitions(parts);
      return true;
    } catch (IOException e) {
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
            partNameForErrorMsg(dbName, tableName, part_vals));
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
    try {
      return getHBase().getPartition(dbName, tableName, part_vals) != null;
    } catch (IOException e) {
      LOG.error("Unable to get partition", e);
      throw new MetaException("Error reading partition " + e.getMessage());
    }
  }

  @Override
  public boolean dropPartition(String dbName, String tableName, List<String> part_vals) throws
      MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    try {
      getHBase().deletePartition(dbName, tableName, part_vals);
      return true;
    } catch (IOException e) {
      LOG.error("Unable to delete db" + e);
      throw new MetaException("Unable to drop partition " + partNameForErrorMsg(dbName, tableName,
          part_vals));
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
    try {
      Table oldTable = getHBase().getTable(dbname, name);
      getHBase().replaceTable(oldTable, newTable);
    } catch (IOException e) {
      LOG.error("Unable to alter table " + tableNameForErrorMsg(dbname, name), e);
      throw new MetaException("Unable to alter table " + tableNameForErrorMsg(dbname, name));
    }
  }

  @Override
  public List<String> getTables(String dbName, String pattern) throws MetaException {
    try {
      List<Table> tables = getHBase().scanTables(dbName, likeToRegex(pattern));
      List<String> tableNames = new ArrayList<String>(tables.size());
      for (Table table : tables) tableNames.add(table.getTableName());
      return tableNames;
    } catch (IOException e) {
      LOG.error("Unable to get tables ", e);
      throw new MetaException("Unable to get tables, " + e.getMessage());
    }
  }

  @Override
  public List<Table> getTableObjectsByName(String dbname, List<String> tableNames) throws
      MetaException, UnknownDBException {
    try {
      return getHBase().getTables(dbname, tableNames);
    } catch (IOException e) {
      LOG.error("Unable to get tables ", e);
      throw new MetaException("Unable to get tables, " + e.getMessage());
    }
  }

  @Override
  public List<String> getAllTables(String dbName) throws MetaException {
    return getTables(dbName, null);
  }

  @Override
  public List<String> listTableNamesByFilter(String dbName, String filter, short max_tables) throws
      MetaException, UnknownDBException {
    // TODO needs to wait until we support pushing filters into HBase.
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
        names.add(buildExternalPartName(table, p));
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
    // TODO needs to wait until we support pushing filters into HBase.
    throw new UnsupportedOperationException();
  }

  @Override
  public void alterPartition(String db_name, String tbl_name, List<String> part_vals,
                             Partition new_part) throws InvalidObjectException, MetaException {
    try {
      Partition oldPart = getHBase().getPartition(db_name, tbl_name, part_vals);
      getHBase().replacePartition(oldPart, new_part);
    } catch (IOException e) {
      LOG.error("Unable to add partition", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    }
  }

  @Override
  public void alterPartitions(String db_name, String tbl_name, List<List<String>> part_vals_list,
                              List<Partition> new_parts) throws InvalidObjectException,
      MetaException {
    try {
      List<Partition> oldParts = getHBase().getPartitions(db_name, tbl_name, part_vals_list);
      getHBase().replacePartitions(oldParts, new_parts);
    } catch (IOException e) {
      LOG.error("Unable to add partition", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    }
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
    final ExpressionTree exprTree = (filter != null && !filter.isEmpty()) ? PartFilterExprUtil
        .getFilterParser(filter).tree : ExpressionTree.EMPTY_TREE;
    List<Partition> result = new ArrayList<Partition>();
    getPartitionsByExprInternal(dbName, tblName, exprTree, maxParts, result);
    return result;
  }

  @Override
  public boolean getPartitionsByExpr(String dbName, String tblName, byte[] expr,
                                     String defaultPartitionName, short maxParts,
                                     List<Partition> result) throws TException {
    final ExpressionTree exprTree = PartFilterExprUtil.makeExpressionTree(expressionProxy, expr);
    // TODO: investigate if there should be any role for defaultPartitionName in this
    // implementation. direct sql code path in ObjectStore does not use it.

    return getPartitionsByExprInternal(dbName, tblName, exprTree, maxParts, result);
  }

  private boolean getPartitionsByExprInternal(String dbName, String tblName,
      ExpressionTree exprTree, short maxParts, List<Partition> result) throws MetaException,
      NoSuchObjectException {

    Table table = getTable(dbName, tblName);
    if (table == null) {
      throw new NoSuchObjectException("Unable to find table " + dbName + "." + tblName);
    }
    String firstPartitionColumn = table.getPartitionKeys().get(0).getName();
    // general hbase filter plan from expression tree
    PlanResult planRes = HBaseFilterPlanUtil.getFilterPlan(exprTree, firstPartitionColumn);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Hbase Filter Plan generated : " + planRes.plan);
    }

    // results from scans need to be merged as there can be overlapping results between
    // the scans. Use a map of list of partition values to partition for this.
    Map<List<String>, Partition> mergedParts = new HashMap<List<String>, Partition>();
    for (ScanPlan splan : planRes.plan.getPlans()) {
      try {
        List<Partition> parts = getHBase().scanPartitions(dbName, tblName,
            splan.getStartRowSuffix(), splan.getEndRowSuffix(), null, -1);
        boolean reachedMax = false;
        for (Partition part : parts) {
          mergedParts.put(part.getValues(), part);
          if (mergedParts.size() == maxParts) {
            reachedMax = true;
            break;
          }
        }
        if (reachedMax) {
          break;
        }
      } catch (IOException e) {
        LOG.error("Unable to get partitions", e);
        throw new MetaException("Error scanning partitions" + tableNameForErrorMsg(dbName, tblName)
            + ": " + e);
      }
    }
    for (Entry<List<String>, Partition> mp : mergedParts.entrySet()) {
      result.add(mp.getValue());
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Matched partitions " + result);
    }

    // return true if there might be some additional partitions that don't match filter conditions
    // being returned
    return !planRes.hasUnsupportedCondition;
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

  /*
   * The design for roles.  Roles are a pain because of their hierarchical nature.  When a user
   * comes in and we need to be able to determine all roles he is a part of, we do not want to
   * have to walk the hierarchy in the database.  This means we need to flatten the role map for
   * each user.  But we also have to track how the roles are connected for each user, in case one
   * role is revoked from another (e.g. if role1 is included in role2 but then revoked
   * from it and user1 was granted both role2 and role1 we cannot remove user1 from role1
   * because he was granted that separately).
   *
   * We want to optimize for the read case and put the cost on grant and revoke of roles, since
   * we assume that is the less common case.  So we lay out the roles data as follows:
   *
   * There is a ROLES table that records each role, plus what other principals have been granted
   * into it, along with the info on grantor, etc.
   *
   * There is a USER_TO_ROLES table that contains the mapping of each user to every role he is a
   * part of.
   *
   * This makes determining what roles a user participates in very quick, as USER_TO_ROLE is a
   * simple list for each user.  It makes granting users into roles expensive, and granting roles
   * into roles very expensive.  Each time a user is granted into a role, we need to walk the
   * hierarchy in the role table (which means moving through that table multiple times) to
   * determine every role the user participates in.  Each a role is granted into another role
   * this hierarchical walk must be done for every principal in the role being granted into.  To
   * mitigate this pain somewhat whenever doing these mappings we cache the entire ROLES table in
   * memory since we assume it is not large.
   *
   * On a related note, whenever a role is dropped we must walk not only all these role tables
   * above (equivalent to a role being revoked from another role, since we have to rebuilding
   * mappings for any users in roles that contained that role and any users directly in that
   * role), but we also have to remove all the privileges associated with that role directly.
   * That means a walk of the DBS table and of the TBLS table.
   */

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
      LOG.error("Unable to create role ", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    }
  }

  @Override
  public boolean removeRole(String roleName) throws MetaException, NoSuchObjectException {
    try {
      Set<String> usersInRole = getHBase().findAllUsersInRole(roleName);
      getHBase().deleteRole(roleName);
      getHBase().removeRoleGrants(roleName);
      for (String user : usersInRole) {
        getHBase().buildRoleMapForUser(user);
      }
      return true;
    } catch (IOException e) {
      LOG.error("Unable to delete role" + e);
      throw new MetaException("Unable to drop role " + roleName);
    }
  }

  @Override
  public boolean grantRole(Role role, String userName, PrincipalType principalType, String grantor,
                           PrincipalType grantorType, boolean grantOption)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    try {
      Set<String> usersToRemap = findUsersToRemapRolesFor(role, userName, principalType);
      HbaseMetastoreProto.RoleGrantInfo.Builder builder =
          HbaseMetastoreProto.RoleGrantInfo.newBuilder();
      if (userName != null) builder.setPrincipalName(userName);
      if (principalType != null) {
        builder.setPrincipalType(HBaseUtils.convertPrincipalTypes(principalType));
      }
      builder.setAddTime((int)(System.currentTimeMillis() / 1000));
      if (grantor != null) builder.setGrantor(grantor);
      if (grantorType != null) {
        builder.setGrantorType(HBaseUtils.convertPrincipalTypes(grantorType));
      }
      builder.setGrantOption(grantOption);

      getHBase().addPrincipalToRole(role.getRoleName(), builder.build());
      for (String user : usersToRemap) {
        getHBase().buildRoleMapForUser(user);
      }
      return true;
    } catch (IOException e) {
      LOG.error("Unable to grant role", e);
      throw new MetaException("Unable to grant role " + e.getMessage());
    }
  }

  @Override
  public boolean revokeRole(Role role, String userName, PrincipalType principalType,
                            boolean grantOption) throws MetaException, NoSuchObjectException {
    // This can have a couple of different meanings.  If grantOption is true, then this is only
    // revoking the grant option, the role itself doesn't need to be removed.  If it is false
    // then we need to remove the userName from the role altogether.
    try {
      if (grantOption) {
        // If this is a grant only change, we don't need to rebuild the user mappings.
        getHBase().dropPrincipalFromRole(role.getRoleName(), userName, principalType, grantOption);
      } else {
        Set<String> usersToRemap = findUsersToRemapRolesFor(role, userName, principalType);
        getHBase().dropPrincipalFromRole(role.getRoleName(), userName, principalType, grantOption);
        for (String user : usersToRemap) {
          getHBase().buildRoleMapForUser(user);
        }
      }
      return true;
    } catch (IOException e) {
      LOG.error("Unable to revoke role " + role.getRoleName() + " from " + userName, e);
      throw new MetaException("Unable to revoke role " + e.getMessage());
    }
  }

  @Override
  public PrincipalPrivilegeSet getUserPrivilegeSet(String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    try {
      PrincipalPrivilegeSet pps = new PrincipalPrivilegeSet();
      PrincipalPrivilegeSet global = getHBase().getGlobalPrivs();
      if (global == null) return null;
      List<PrivilegeGrantInfo> pgi;
      if (global.getUserPrivileges() != null) {
        pgi = global.getUserPrivileges().get(userName);
        if (pgi != null) {
          pps.putToUserPrivileges(userName, pgi);
        }
      }

      if (global.getRolePrivileges() != null) {
        List<String> roles = getHBase().getUserRoles(userName);
        if (roles != null) {
          for (String role : roles) {
            pgi = global.getRolePrivileges().get(role);
            if (pgi != null) {
              pps.putToRolePrivileges(role, pgi);
            }
          }
        }
      }
      return pps;
    } catch (IOException e) {
      LOG.error("Unable to get db privileges for user", e);
      throw new MetaException("Unable to get db privileges for user, " + e.getMessage());
    }
  }

  @Override
  public PrincipalPrivilegeSet getDBPrivilegeSet(String dbName, String userName,
                                                 List<String> groupNames)
      throws InvalidObjectException, MetaException {
    try {
      PrincipalPrivilegeSet pps = new PrincipalPrivilegeSet();
      Database db = getHBase().getDb(dbName);
      if (db.getPrivileges() != null) {
        List<PrivilegeGrantInfo> pgi;
        // Find the user privileges for this db
        if (db.getPrivileges().getUserPrivileges() != null) {
          pgi = db.getPrivileges().getUserPrivileges().get(userName);
          if (pgi != null) {
            pps.putToUserPrivileges(userName, pgi);
          }
        }

        if (db.getPrivileges().getRolePrivileges() != null) {
          List<String> roles = getHBase().getUserRoles(userName);
          if (roles != null) {
            for (String role : roles) {
              pgi = db.getPrivileges().getRolePrivileges().get(role);
              if (pgi != null) {
                pps.putToRolePrivileges(role, pgi);
              }
            }
          }
        }
      }
      return pps;
    } catch (IOException e) {
      LOG.error("Unable to get db privileges for user", e);
      throw new MetaException("Unable to get db privileges for user, " + e.getMessage());
    }
  }

  @Override
  public PrincipalPrivilegeSet getTablePrivilegeSet(String dbName, String tableName,
                                                    String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    try {
      PrincipalPrivilegeSet pps = new PrincipalPrivilegeSet();
      Table table = getHBase().getTable(dbName, tableName);
      List<PrivilegeGrantInfo> pgi;
      if (table.getPrivileges() != null) {
        if (table.getPrivileges().getUserPrivileges() != null) {
          pgi = table.getPrivileges().getUserPrivileges().get(userName);
          if (pgi != null) {
            pps.putToUserPrivileges(userName, pgi);
          }
        }

        if (table.getPrivileges().getRolePrivileges() != null) {
          List<String> roles = getHBase().getUserRoles(userName);
          if (roles != null) {
            for (String role : roles) {
              pgi = table.getPrivileges().getRolePrivileges().get(role);
              if (pgi != null) {
                pps.putToRolePrivileges(role, pgi);
              }
            }
          }
        }
      }
      return pps;
    } catch (IOException e) {
      LOG.error("Unable to get db privileges for user", e);
      throw new MetaException("Unable to get db privileges for user, " + e.getMessage());
    }
  }

  @Override
  public PrincipalPrivilegeSet getPartitionPrivilegeSet(String dbName, String tableName,
                                                        String partition, String userName,
                                                        List<String> groupNames) throws
      InvalidObjectException, MetaException {
    // We don't support partition privileges
    return null;
  }

  @Override
  public PrincipalPrivilegeSet getColumnPrivilegeSet(String dbName, String tableName,
                                                     String partitionName, String columnName,
                                                     String userName,
                                                     List<String> groupNames) throws
      InvalidObjectException, MetaException {
    // We don't support column level privileges
    return null;
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalGlobalGrants(String principalName,
                                                             PrincipalType principalType) {
    List<PrivilegeGrantInfo> grants;
    List<HiveObjectPrivilege> privileges = new ArrayList<HiveObjectPrivilege>();
    try {
      PrincipalPrivilegeSet pps = getHBase().getGlobalPrivs();
      if (pps == null) return privileges;
      Map<String, List<PrivilegeGrantInfo>> map;
      switch (principalType) {
        case USER:
          map = pps.getUserPrivileges();
          break;

        case ROLE:
          map = pps.getRolePrivileges();
          break;

        default:
          throw new RuntimeException("Unknown or unsupported principal type " +
              principalType.toString());
      }
      if (map == null) return privileges;
      grants = map.get(principalName);

      if (grants == null || grants.size() == 0) return privileges;
      for (PrivilegeGrantInfo pgi : grants) {
        privileges.add(new HiveObjectPrivilege(new HiveObjectRef(HiveObjectType.GLOBAL, null,
            null, null, null), principalName, principalType, pgi));
      }
      return privileges;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDBGrants(String principalName,
                                                         PrincipalType principalType,
                                                         String dbName) {
    List<PrivilegeGrantInfo> grants;
    List<HiveObjectPrivilege> privileges = new ArrayList<HiveObjectPrivilege>();
    try {
      Database db = getHBase().getDb(dbName);
      if (db == null) return privileges;
      PrincipalPrivilegeSet pps = db.getPrivileges();
      if (pps == null) return privileges;
      Map<String, List<PrivilegeGrantInfo>> map;
      switch (principalType) {
        case USER:
          map = pps.getUserPrivileges();
          break;

        case ROLE:
          map = pps.getRolePrivileges();
          break;

        default:
          throw new RuntimeException("Unknown or unsupported principal type " +
              principalType.toString());
      }
      if (map == null) return privileges;
      grants = map.get(principalName);

      if (grants == null || grants.size() == 0) return privileges;
      for (PrivilegeGrantInfo pgi : grants) {
        privileges.add(new HiveObjectPrivilege(new HiveObjectRef(HiveObjectType.DATABASE, dbName,
         null, null, null), principalName, principalType, pgi));
      }
      return privileges;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<HiveObjectPrivilege> listAllTableGrants(String principalName,
                                                      PrincipalType principalType,
                                                      String dbName,
                                                      String tableName) {
    List<PrivilegeGrantInfo> grants;
    List<HiveObjectPrivilege> privileges = new ArrayList<HiveObjectPrivilege>();
    try {
      Table table = getHBase().getTable(dbName, tableName);
      if (table == null) return privileges;
      PrincipalPrivilegeSet pps = table.getPrivileges();
      if (pps == null) return privileges;
      Map<String, List<PrivilegeGrantInfo>> map;
      switch (principalType) {
        case USER:
          map = pps.getUserPrivileges();
          break;

        case ROLE:
          map = pps.getRolePrivileges();
          break;

        default:
          throw new RuntimeException("Unknown or unsupported principal type " +
              principalType.toString());
      }
      if (map == null) return privileges;
      grants = map.get(principalName);

      if (grants == null || grants.size() == 0) return privileges;
      for (PrivilegeGrantInfo pgi : grants) {
        privileges.add(new HiveObjectPrivilege(new HiveObjectRef(HiveObjectType.TABLE, dbName,
            tableName, null, null), principalName, principalType, pgi));
      }
      return privileges;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionGrants(String principalName,
                                                                PrincipalType principalType,
                                                                String dbName,
                                                                String tableName,
                                                                List<String> partValues,
                                                                String partName) {
    // We don't support partition grants
    return new ArrayList<HiveObjectPrivilege>();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableColumnGrants(String principalName,
                                                                  PrincipalType principalType,
                                                                  String dbName, String tableName,
                                                                  String columnName) {
    // We don't support column grants
    return new ArrayList<HiveObjectPrivilege>();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrants(String principalName,
                                                                      PrincipalType principalType,
                                                                      String dbName,
                                                                      String tableName,
                                                                      List<String> partVals,
                                                                      String partName,
                                                                      String columnName) {
    // We don't support column grants
    return new ArrayList<HiveObjectPrivilege>();
  }

  @Override
  public boolean grantPrivileges(PrivilegeBag privileges)
      throws InvalidObjectException, MetaException, NoSuchObjectException {
    for (HiveObjectPrivilege priv : privileges.getPrivileges()) {
      // Locate the right object to deal with
     PrivilegeInfo privilegeInfo = findPrivilegeToGrantOrRevoke(priv);

      // Now, let's see if we've already got this privilege
      for (PrivilegeGrantInfo info : privilegeInfo.grants) {
        if (info.getPrivilege().equals(priv.getGrantInfo().getPrivilege())) {
          throw new InvalidObjectException(priv.getPrincipalName() + " already has " +
              priv.getGrantInfo().getPrivilege() + " on " + privilegeInfo.typeErrMsg);
        }
      }
      privilegeInfo.grants.add(priv.getGrantInfo());

      writeBackGrantOrRevoke(priv, privilegeInfo);
    }
    return true;
  }

  @Override
  public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption) throws
      InvalidObjectException, MetaException, NoSuchObjectException {
    for (HiveObjectPrivilege priv : privileges.getPrivileges()) {
      PrivilegeInfo privilegeInfo = findPrivilegeToGrantOrRevoke(priv);

      for (int i = 0; i < privilegeInfo.grants.size(); i++) {
        if (privilegeInfo.grants.get(i).getPrivilege().equals(priv.getGrantInfo().getPrivilege())) {
          if (grantOption) privilegeInfo.grants.get(i).setGrantOption(false);
          else privilegeInfo.grants.remove(i);
          break;
        }
      }
      writeBackGrantOrRevoke(priv, privilegeInfo);
    }
    return true;
  }

  private static class PrivilegeInfo {
    Database db;
    Table table;
    List<PrivilegeGrantInfo> grants;
    String typeErrMsg;
    PrincipalPrivilegeSet privSet;
  }

  private PrivilegeInfo findPrivilegeToGrantOrRevoke(HiveObjectPrivilege privilege)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    PrivilegeInfo result = new PrivilegeInfo();
    switch (privilege.getHiveObject().getObjectType()) {
      case GLOBAL:
        try {
          result.privSet = createOnNull(getHBase().getGlobalPrivs());
        } catch (IOException e) {
          LOG.error("Unable to fetch global privileges", e);
          throw new MetaException("Unable to fetch global privileges, " + e.getMessage());
        }
        result.typeErrMsg = "global";
        break;

      case DATABASE:
        result.db = getDatabase(privilege.getHiveObject().getDbName());
        result.typeErrMsg = "database " + result.db.getName();
        result.privSet = createOnNull(result.db.getPrivileges());
        break;

      case TABLE:
        result.table = getTable(privilege.getHiveObject().getDbName(),
            privilege.getHiveObject().getObjectName());
        result.typeErrMsg = "table " + result.table.getTableName();
        result.privSet = createOnNull(result.table.getPrivileges());
        break;

      case PARTITION:
      case COLUMN:
        throw new RuntimeException("HBase metastore does not support partition or column " +
            "permissions");

      default:
        throw new RuntimeException("Woah bad, unknown object type " +
            privilege.getHiveObject().getObjectType());
    }

    // Locate the right PrivilegeGrantInfo
    Map<String, List<PrivilegeGrantInfo>> grantInfos;
    switch (privilege.getPrincipalType()) {
      case USER:
        grantInfos = result.privSet.getUserPrivileges();
        result.typeErrMsg = "user";
        break;

      case GROUP:
        throw new RuntimeException("HBase metastore does not support group permissions");

      case ROLE:
        grantInfos = result.privSet.getRolePrivileges();
        result.typeErrMsg = "role";
        break;

      default:
        throw new RuntimeException("Woah bad, unknown principal type " +
            privilege.getPrincipalType());
    }

    // Find the requested name in the grantInfo
    result.grants = grantInfos.get(privilege.getPrincipalName());
    if (result.grants == null) {
      // Means we don't have any grants for this user yet.
      result.grants = new ArrayList<PrivilegeGrantInfo>();
      grantInfos.put(privilege.getPrincipalName(), result.grants);
    }
    return result;
  }

  private PrincipalPrivilegeSet createOnNull(PrincipalPrivilegeSet pps) {
    // If this is the first time a user has been granted a privilege set will be null.
    if (pps == null) {
      pps = new PrincipalPrivilegeSet();
    }
    if (pps.getUserPrivileges() == null) {
      pps.setUserPrivileges(new HashMap<String, List<PrivilegeGrantInfo>>());
    }
    if (pps.getRolePrivileges() == null) {
      pps.setRolePrivileges(new HashMap<String, List<PrivilegeGrantInfo>>());
    }
    return pps;
  }

  private void writeBackGrantOrRevoke(HiveObjectPrivilege priv, PrivilegeInfo pi)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    // Now write it back
    switch (priv.getHiveObject().getObjectType()) {
      case GLOBAL:
        try {
          getHBase().putGlobalPrivs(pi.privSet);
        } catch (IOException e) {
          LOG.error("Unable to write global privileges", e);
          throw new MetaException("Unable to write global privileges, " + e.getMessage());
        }
        break;

      case DATABASE:
        pi.db.setPrivileges(pi.privSet);
        alterDatabase(pi.db.getName(), pi.db);
        break;

      case TABLE:
        pi.table.setPrivileges(pi.privSet);
        alterTable(pi.table.getDbName(), pi.table.getTableName(), pi.table);
        break;

      default:
        throw new RuntimeException("Dude, you missed the second switch!");
    }
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
    try {
      List<Role> roles = getHBase().scanRoles();
      List<String> roleNames = new ArrayList<String>(roles.size());
      for (Role role : roles) roleNames.add(role.getRoleName());
      return roleNames;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<Role> listRoles(String principalName, PrincipalType principalType) {
    List<Role> roles = new ArrayList<Role>();
    try {
      roles.addAll(getHBase().getPrincipalDirectRoles(principalName, principalType));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    // Add the public role if this is a user
    if (principalType == PrincipalType.USER) {
      roles.add(new Role(HiveMetaStore.PUBLIC, 0, null));
    }
    return roles;
  }

  @Override
  public List<RolePrincipalGrant> listRolesWithGrants(String principalName,
                                                      PrincipalType principalType) {
    try {
      List<Role> roles = listRoles(principalName, principalType);
      List<RolePrincipalGrant> rpgs = new ArrayList<RolePrincipalGrant>(roles.size());
      for (Role role : roles) {
        HbaseMetastoreProto.RoleGrantInfoList grants = getHBase().getRolePrincipals(role.getRoleName());
        if (grants != null) {
          for (HbaseMetastoreProto.RoleGrantInfo grant : grants.getGrantInfoList()) {
            if (grant.getPrincipalType() == HBaseUtils.convertPrincipalTypes(principalType) &&
                grant.getPrincipalName().equals(principalName)) {
              rpgs.add(new RolePrincipalGrant(role.getRoleName(), principalName, principalType,
                  grant.getGrantOption(), (int) grant.getAddTime(), grant.getGrantor(),
                  HBaseUtils.convertPrincipalTypes(grant.getGrantorType())));
            }
          }
        }
      }
      return rpgs;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<RolePrincipalGrant> listRoleMembers(String roleName) {
    try {
      HbaseMetastoreProto.RoleGrantInfoList gil = getHBase().getRolePrincipals(roleName);
      List<RolePrincipalGrant> roleMaps = new ArrayList<RolePrincipalGrant>(gil.getGrantInfoList().size());
      for (HbaseMetastoreProto.RoleGrantInfo giw : gil.getGrantInfoList()) {
        roleMaps.add(new RolePrincipalGrant(roleName, giw.getPrincipalName(),
            HBaseUtils.convertPrincipalTypes(giw.getPrincipalType()),
            giw.getGrantOption(), (int)giw.getAddTime(), giw.getGrantor(),
            HBaseUtils.convertPrincipalTypes(giw.getGrantorType())));
      }
      return roleMaps;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Partition getPartitionWithAuth(String dbName, String tblName, List<String> partVals,
                                        String user_name, List<String> group_names)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    // We don't do authorization checks for partitions.
    return getPartition(dbName, tblName, partVals);
  }

  @Override
  public List<Partition> getPartitionsWithAuth(String dbName, String tblName, short maxParts,
                                               String userName, List<String> groupNames)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    // We don't do authorization checks for partitions.
    return getPartitions(dbName, tblName, maxParts);
  }

  @Override
  public List<String> listPartitionNamesPs(String db_name, String tbl_name, List<String> part_vals,
                                           short max_parts)
      throws MetaException, NoSuchObjectException {
    List<Partition> parts =
        listPartitionsPsWithAuth(db_name, tbl_name, part_vals, max_parts, null, null);
    List<String> partNames = new ArrayList<String>(parts.size());
    for (Partition part : parts) {
      partNames.add(buildExternalPartName(db_name, tbl_name, part.getValues()));
    }
    return partNames;
  }


  @Override
  public List<Partition> listPartitionsPsWithAuth(String db_name, String tbl_name,
                                                  List<String> part_vals, short max_parts,
                                                  String userName, List<String> groupNames)
      throws MetaException, NoSuchObjectException {
    // We don't handle auth info with partitions
    try {
      return getHBase().scanPartitions(db_name, tbl_name, part_vals, max_parts);
    } catch (IOException e) {
      LOG.error("Unable to list partition names", e);
      throw new MetaException("Failed to list part names, " + e.getMessage());
    }
  }

  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics colStats)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
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
  public boolean updatePartitionColumnStatistics(ColumnStatistics statsObj, List<String> partVals)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    try {
      getHBase().updateStatistics(statsObj.getStatsDesc().getDbName(),
          statsObj.getStatsDesc().getTableName(), statsObj.getStatsDesc().getPartName(), partVals,
          statsObj);
      return true;
    } catch (IOException e) {
      LOG.error("Unable to update column statistics", e);
      throw new MetaException("Failed to update column statistics, " + e.getMessage());
    }
  }

  @Override
  public ColumnStatistics getTableColumnStatistics(String dbName, String tableName,
      List<String> colName) throws MetaException, NoSuchObjectException {
    try {
      return getHBase().getTableStatistics(dbName, tableName, colName);
    } catch (IOException e) {
      LOG.error("Unable to fetch column statistics", e);
      throw new MetaException("Failed to fetch column statistics, " + e.getMessage());
    }
  }

  @Override
  public List<ColumnStatistics> getPartitionColumnStatistics(String dbName, String tblName,
      List<String> partNames, List<String> colNames) throws MetaException, NoSuchObjectException {
    List<List<String>> partVals = new ArrayList<List<String>>(partNames.size());
    for (String partName : partNames) {
      partVals.add(partNameToVals(partName));
    }
    try {
      return getHBase().getPartitionStatistics(dbName, tblName, partNames, partVals, colNames);
    } catch (IOException e) {
      LOG.error("Unable to fetch column statistics", e);
      throw new MetaException("Failed fetching column statistics, " + e.getMessage());
    }
  }

  @Override
  public boolean deletePartitionColumnStatistics(String dbName, String tableName, String partName,
      List<String> partVals, String colName) throws NoSuchObjectException, MetaException,
      InvalidObjectException, InvalidInputException {
    // NOP, stats will be deleted along with the partition when it is dropped.
    return true;
  }

  @Override
  public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName) throws
      NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    // NOP, stats will be deleted along with the table when it is dropped.
    return true;
  }

  /**
   * Return aggregated statistics for each column in the colNames list aggregated over partitions in
   * the partNames list
   *
   */
  @Override
  public AggrStats get_aggr_stats_for(String dbName, String tblName, List<String> partNames,
      List<String> colNames) throws MetaException, NoSuchObjectException {
    List<List<String>> partVals = new ArrayList<List<String>>(partNames.size());
    for (String partName : partNames) {
      partVals.add(partNameToVals(partName));
    }
    try {
      return getHBase().getAggrStats(dbName, tblName, partNames, partVals, colNames);
    } catch (IOException e) {
      LOG.error("Unable to fetch aggregate column statistics", e);
      throw new MetaException("Failed fetching aggregate column statistics, " + e.getMessage());
    }
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
      if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_IN_TEST)) {
        getHBase().createTablesIfNotExist();
      }
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
    List<HiveObjectPrivilege> privileges = new ArrayList<HiveObjectPrivilege>();
    try {
      List<Database> dbs = getHBase().scanDatabases(null);
      for (Database db : dbs) {
        List<PrivilegeGrantInfo> grants;

        PrincipalPrivilegeSet pps = db.getPrivileges();
        if (pps == null) continue;
        Map<String, List<PrivilegeGrantInfo>> map;
        switch (principalType) {
          case USER:
            map = pps.getUserPrivileges();
            break;

          case ROLE:
            map = pps.getRolePrivileges();
            break;

          default:
            throw new RuntimeException("Unknown or unsupported principal type " +
                principalType.toString());
        }

        if (map == null) continue;
        grants = map.get(principalName);
        if (grants == null || grants.size() == 0) continue;
        for (PrivilegeGrantInfo pgi : grants) {
          privileges.add(new HiveObjectPrivilege(new HiveObjectRef(HiveObjectType.DATABASE,
              db.getName(), null, null, null), principalName, principalType, pgi));
        }
      }
      return privileges;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableGrantsAll(String principalName,
                                                               PrincipalType principalType) {
    List<HiveObjectPrivilege> privileges = new ArrayList<HiveObjectPrivilege>();
    try {
      List<Table> tables = getHBase().scanTables(null, null);
      for (Table table : tables) {
        List<PrivilegeGrantInfo> grants;

        PrincipalPrivilegeSet pps = table.getPrivileges();
        if (pps == null) continue;
        Map<String, List<PrivilegeGrantInfo>> map;
        switch (principalType) {
          case USER:
            map = pps.getUserPrivileges();
            break;

          case ROLE:
            map = pps.getRolePrivileges();
            break;

          default:
            throw new RuntimeException("Unknown or unsupported principal type " +
                principalType.toString());
        }

        if (map == null) continue;
        grants = map.get(principalName);
        if (grants == null || grants.size() == 0) continue;
        for (PrivilegeGrantInfo pgi : grants) {
          privileges.add(new HiveObjectPrivilege(new HiveObjectRef(HiveObjectType.TABLE,
              table.getDbName(), table.getTableName(), null, null), principalName, principalType,
              pgi));
        }
      }
      return privileges;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionGrantsAll(String principalName,
                                                                   PrincipalType principalType) {
    return new ArrayList<HiveObjectPrivilege>();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableColumnGrantsAll(String principalName,
                                                                     PrincipalType principalType) {
    return new ArrayList<HiveObjectPrivilege>();
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalPartitionColumnGrantsAll(String principalName,
                                                                         PrincipalType principalType) {
    return new ArrayList<HiveObjectPrivilege>();
  }

  @Override
  public List<HiveObjectPrivilege> listGlobalGrantsAll() {
    List<HiveObjectPrivilege> privileges = new ArrayList<HiveObjectPrivilege>();
    try {
      PrincipalPrivilegeSet pps = getHBase().getGlobalPrivs();
      if (pps != null) {
        for (Map.Entry<String, List<PrivilegeGrantInfo>> e : pps.getUserPrivileges().entrySet()) {
          for (PrivilegeGrantInfo pgi : e.getValue()) {
            privileges.add(new HiveObjectPrivilege(new HiveObjectRef(HiveObjectType.GLOBAL, null,
                null, null, null), e.getKey(), PrincipalType.USER, pgi));
          }
        }
        for (Map.Entry<String, List<PrivilegeGrantInfo>> e : pps.getRolePrivileges().entrySet()) {
          for (PrivilegeGrantInfo pgi : e.getValue()) {
            privileges.add(new HiveObjectPrivilege(new HiveObjectRef(HiveObjectType.GLOBAL, null,
                null, null, null), e.getKey(), PrincipalType.ROLE, pgi));
          }
        }
      }
      return privileges;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<HiveObjectPrivilege> listDBGrantsAll(String dbName) {
    List<HiveObjectPrivilege> privileges = new ArrayList<HiveObjectPrivilege>();
    try {
      Database db = getHBase().getDb(dbName);
      PrincipalPrivilegeSet pps = db.getPrivileges();
      if (pps != null) {
        for (Map.Entry<String, List<PrivilegeGrantInfo>> e : pps.getUserPrivileges().entrySet()) {
          for (PrivilegeGrantInfo pgi : e.getValue()) {
            privileges.add(new HiveObjectPrivilege(new HiveObjectRef(HiveObjectType.DATABASE, dbName,
                null, null, null), e.getKey(), PrincipalType.USER, pgi));
          }
        }
        for (Map.Entry<String, List<PrivilegeGrantInfo>> e : pps.getRolePrivileges().entrySet()) {
          for (PrivilegeGrantInfo pgi : e.getValue()) {
            privileges.add(new HiveObjectPrivilege(new HiveObjectRef(HiveObjectType.DATABASE, dbName,
                null, null, null), e.getKey(), PrincipalType.ROLE, pgi));
          }
        }
      }
      return privileges;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionColumnGrantsAll(String dbName, String tableName,
                                                                String partitionName,
                                                                String columnName) {
    return new ArrayList<HiveObjectPrivilege>();
  }

  @Override
  public List<HiveObjectPrivilege> listTableGrantsAll(String dbName, String tableName) {
    List<HiveObjectPrivilege> privileges = new ArrayList<HiveObjectPrivilege>();
    try {
      Table table = getHBase().getTable(dbName, tableName);
      PrincipalPrivilegeSet pps = table.getPrivileges();
      if (pps != null) {
        for (Map.Entry<String, List<PrivilegeGrantInfo>> e : pps.getUserPrivileges().entrySet()) {
          for (PrivilegeGrantInfo pgi : e.getValue()) {
            privileges.add(new HiveObjectPrivilege(new HiveObjectRef(HiveObjectType.TABLE, dbName,
                tableName, null, null), e.getKey(), PrincipalType.USER, pgi));
          }
        }
        for (Map.Entry<String, List<PrivilegeGrantInfo>> e : pps.getRolePrivileges().entrySet()) {
          for (PrivilegeGrantInfo pgi : e.getValue()) {
            privileges.add(new HiveObjectPrivilege(new HiveObjectRef(HiveObjectType.TABLE, dbName,
                tableName, null, null), e.getKey(), PrincipalType.ROLE, pgi));
          }
        }
      }
      return privileges;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<HiveObjectPrivilege> listPartitionGrantsAll(String dbName, String tableName,
                                                          String partitionName) {
    return new ArrayList<HiveObjectPrivilege>();
  }

  @Override
  public List<HiveObjectPrivilege> listTableColumnGrantsAll(String dbName, String tableName,
                                                            String columnName) {
    return new ArrayList<HiveObjectPrivilege>();
  }

  @Override
  public void createFunction(Function func) throws InvalidObjectException, MetaException {
    try {
      getHBase().putFunction(func);
    } catch (IOException e) {
      LOG.error("Unable to create function", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    }
  }

  @Override
  public void alterFunction(String dbName, String funcName, Function newFunction) throws
      InvalidObjectException, MetaException {
    try {
      getHBase().putFunction(newFunction);
    } catch (IOException e) {
      LOG.error("Unable to alter function ", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    }
  }

  @Override
  public void dropFunction(String dbName, String funcName) throws MetaException,
      NoSuchObjectException, InvalidObjectException, InvalidInputException {
    try {
      getHBase().deleteFunction(dbName, funcName);
    } catch (IOException e) {
      LOG.error("Unable to delete function" + e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    }
  }

  @Override
  public Function getFunction(String dbName, String funcName) throws MetaException {
    try {
      return getHBase().getFunction(dbName, funcName);
    } catch (IOException e) {
      LOG.error("Unable to get function" + e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    }
  }

  @Override
  public List<String> getFunctions(String dbName, String pattern) throws MetaException {
    try {
      List<Function> funcs = getHBase().scanFunctions(dbName, likeToRegex(pattern));
      List<String> funcNames = new ArrayList<String>(funcs.size());
      for (Function func : funcs) funcNames.add(func.getFunctionName());
      return funcNames;
    } catch (IOException e) {
      LOG.error("Unable to get functions" + e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    }
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
    // initialize expressionProxy. Also re-initialize it if
    // setConf is being called with new configuration object (though that
    // is not expected to happen, doing it just for safety)
    if(expressionProxy == null || conf != configuration) {
      expressionProxy = PartFilterExprUtil.createExpressionProxy(configuration);
    }
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

  // This is for building error messages only.  It does not look up anything in the metastore.
  private String tableNameForErrorMsg(String dbName, String tableName) {
    return dbName + "." + tableName;
  }

  // This is for building error messages only.  It does not look up anything in the metastore as
  // they may just throw another error.
  private String partNameForErrorMsg(String dbName, String tableName, List<String> partVals) {
    return tableNameForErrorMsg(dbName, tableName) + "." + StringUtils.join(partVals, ':');
  }

  private String buildExternalPartName(Table table, Partition part) {
    return buildExternalPartName(table, part.getValues());
  }

  private String buildExternalPartName(String dbName, String tableName, List<String> partVals)
      throws MetaException {
    return buildExternalPartName(getTable(dbName, tableName), partVals);
  }

  private Set<String> findUsersToRemapRolesFor(Role role, String principalName, PrincipalType type)
      throws IOException, NoSuchObjectException {
    Set<String> usersToRemap;
    switch (type) {
      case USER:
        // In this case it's just the user being added to the role that we need to remap for.
        usersToRemap = new HashSet<String>();
        usersToRemap.add(principalName);
        break;

      case ROLE:
        // In this case we need to remap for all users in the containing role (not the role being
        // granted into the containing role).
        usersToRemap = getHBase().findAllUsersInRole(role.getRoleName());
        break;

      default:
        throw new RuntimeException("Unknown principal type " + type);

    }
    return usersToRemap;
  }

  /**
   * Build a partition name for external use.  Necessary since HBase itself doesn't store
   * partition names.
   * @param table  table object
   * @param partVals partition values.
   * @return
   */
  static String buildExternalPartName(Table table, List<String> partVals) {
    List<String> partCols = new ArrayList<String>();
    for (FieldSchema pc : table.getPartitionKeys()) partCols.add(pc.getName());
    return FileUtils.makePartName(partCols, partVals);
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

  private String likeToRegex(String like) {
    if (like == null) return null;
    // Convert Hive's strange like syntax to Java regex.  Per
    // https://cwiki.apache.org/confluence/display/Hive/LanguageManual+DDL#LanguageManualDDL-Show
    // the supported syntax is that * means Java .* and | means 'or'
    // This implementation leaves other regular expression syntax alone, which means people can
    // use it, even though it wouldn't work on RDBMS backed metastores.
    return like.replace("*", ".*");
  }
}
