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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheLoader;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.FileMetadataHandler;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.PartFilterExprUtil;
import org.apache.hadoop.hive.metastore.PartitionExpressionProxy;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AggrStats;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CurrentNotificationEventId;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.FileMetadataExprType;
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
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.Type;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownPartitionException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.hbase.HBaseFilterPlanUtil.PlanResult;
import org.apache.hadoop.hive.metastore.hbase.HBaseFilterPlanUtil.ScanPlan;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hive.common.util.HiveStringUtils;
import org.apache.thrift.TException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Implementation of RawStore that stores data in HBase
 */
public class HBaseStore implements RawStore {
  static final private Logger LOG = LoggerFactory.getLogger(HBaseStore.class.getName());

  // Do not access this directly, call getHBase to make sure it is initialized.
  private HBaseReadWrite hbase = null;
  private Configuration conf;
  private int txnNestLevel = 0;
  private PartitionExpressionProxy expressionProxy = null;
  private Map<FileMetadataExprType, FileMetadataHandler> fmHandlers;

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
    if (txnNestLevel++ <= 0) {
      LOG.debug("Opening HBase transaction");
      getHBase().begin();
      txnNestLevel = 1;
    }
    return true;
  }

  @Override
  public boolean commitTransaction() {
    if (--txnNestLevel == 0) {
      LOG.debug("Committing HBase transaction");
      getHBase().commit();
    }
    return true;
  }

  @Override
  public void rollbackTransaction() {
    txnNestLevel = 0;
    LOG.debug("Rolling back HBase transaction");
    getHBase().rollback();
  }

  @Override
  public void createDatabase(Database db) throws InvalidObjectException, MetaException {
    boolean commit = false;
    openTransaction();
    try {
      Database dbCopy = db.deepCopy();
      dbCopy.setName(HiveStringUtils.normalizeIdentifier(dbCopy.getName()));
      // HiveMetaStore already checks for existence of the database, don't recheck
      getHBase().putDb(dbCopy);
      commit = true;
    } catch (IOException e) {
      LOG.error("Unable to create database ", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }

  }

  @Override
  public Database getDatabase(String name) throws NoSuchObjectException {
    boolean commit = false;
    openTransaction();
    try {
      Database db = getHBase().getDb(HiveStringUtils.normalizeIdentifier(name));
      if (db == null) {
        throw new NoSuchObjectException("Unable to find db " + name);
      }
      commit = true;
      return db;
    } catch (IOException e) {
      LOG.error("Unable to get db", e);
      throw new NoSuchObjectException("Error reading db " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean dropDatabase(String dbname) throws NoSuchObjectException, MetaException {
    boolean commit = false;
    openTransaction();
    try {
      getHBase().deleteDb(HiveStringUtils.normalizeIdentifier(dbname));
      commit = true;
      return true;
    } catch (IOException e) {
      LOG.error("Unable to delete db" + e);
      throw new MetaException("Unable to drop database " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean alterDatabase(String dbname, Database db) throws NoSuchObjectException,
      MetaException {
    // ObjectStore fetches the old db before updating it, but I can't see the possible value of
    // that since the caller will have needed to call getDatabase to have the db object.
    boolean commit = false;
    openTransaction();
    try {
      Database dbCopy = db.deepCopy();
      dbCopy.setName(HiveStringUtils.normalizeIdentifier(dbCopy.getName()));
      getHBase().putDb(dbCopy);
      commit = true;
      return true;
    } catch (IOException e) {
      LOG.error("Unable to alter database ", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<String> getDatabases(String pattern) throws MetaException {
    boolean commit = false;
    openTransaction();
    try {
      List<Database> dbs = getHBase().scanDatabases(
          pattern==null?null:HiveStringUtils.normalizeIdentifier(likeToRegex(pattern)));
      List<String> dbNames = new ArrayList<String>(dbs.size());
      for (Database db : dbs) dbNames.add(db.getName());
      commit = true;
      return dbNames;
    } catch (IOException e) {
      LOG.error("Unable to get databases ", e);
      throw new MetaException("Unable to get databases, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<String> getAllDatabases() throws MetaException {
    return getDatabases(null);
  }

  @Override
  public int getDatabaseCount() throws MetaException {
    try {
      return getHBase().getDatabaseCount();
    } catch (IOException e) {
      LOG.error("Unable to get database count", e);
      throw new MetaException("Error scanning databases");
    }
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
    boolean commit = false;
    openTransaction();
    // HiveMetaStore above us checks if the table already exists, so we can blindly store it here.
    try {
      Table tblCopy = tbl.deepCopy();
      tblCopy.setDbName(HiveStringUtils.normalizeIdentifier(tblCopy.getDbName()));
      tblCopy.setTableName(HiveStringUtils.normalizeIdentifier(tblCopy.getTableName()));
      normalizeColumnNames(tblCopy);
      getHBase().putTable(tblCopy);
      commit = true;
    } catch (IOException e) {
      LOG.error("Unable to create table ", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  private void normalizeColumnNames(Table tbl) {
    if (tbl.getSd().getCols() != null) {
      tbl.getSd().setCols(normalizeFieldSchemaList(tbl.getSd().getCols()));
    }
    if (tbl.getPartitionKeys() != null) {
      tbl.setPartitionKeys(normalizeFieldSchemaList(tbl.getPartitionKeys()));
    }
  }

  private List<FieldSchema> normalizeFieldSchemaList(List<FieldSchema> fieldschemas) {
    List<FieldSchema> ret = new ArrayList<>();
    for (FieldSchema fieldSchema : fieldschemas) {
      ret.add(new FieldSchema(fieldSchema.getName().toLowerCase(), fieldSchema.getType(),
          fieldSchema.getComment()));
    }
    return ret;
  }

  @Override
  public boolean dropTable(String dbName, String tableName) throws MetaException,
      NoSuchObjectException, InvalidObjectException, InvalidInputException {
    boolean commit = false;
    openTransaction();
    try {
      getHBase().deleteTable(HiveStringUtils.normalizeIdentifier(dbName),
          HiveStringUtils.normalizeIdentifier(tableName));
      commit = true;
      return true;
    } catch (IOException e) {
      LOG.error("Unable to delete db" + e);
      throw new MetaException("Unable to drop table " + tableNameForErrorMsg(dbName, tableName));
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public Table getTable(String dbName, String tableName) throws MetaException {
    boolean commit = false;
    openTransaction();
    try {
      Table table = getHBase().getTable(HiveStringUtils.normalizeIdentifier(dbName),
          HiveStringUtils.normalizeIdentifier(tableName));
      if (table == null) {
        LOG.debug("Unable to find table " + tableNameForErrorMsg(dbName, tableName));
      }
      commit = true;
      return table;
    } catch (IOException e) {
      LOG.error("Unable to get table", e);
      throw new MetaException("Error reading table " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean addPartition(Partition part) throws InvalidObjectException, MetaException {
    boolean commit = false;
    openTransaction();
    try {
      Partition partCopy = part.deepCopy();
      partCopy.setDbName(HiveStringUtils.normalizeIdentifier(part.getDbName()));
      partCopy.setTableName(HiveStringUtils.normalizeIdentifier(part.getTableName()));
      getHBase().putPartition(partCopy);
      commit = true;
      return true;
    } catch (IOException e) {
      LOG.error("Unable to add partition", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean addPartitions(String dbName, String tblName, List<Partition> parts)
      throws InvalidObjectException, MetaException {
    boolean commit = false;
    openTransaction();
    try {
      List<Partition> partsCopy = new ArrayList<Partition>();
      for (int i=0;i<parts.size();i++) {
        Partition partCopy = parts.get(i).deepCopy();
        partCopy.setDbName(HiveStringUtils.normalizeIdentifier(partCopy.getDbName()));
        partCopy.setTableName(HiveStringUtils.normalizeIdentifier(partCopy.getTableName()));
        partsCopy.add(i, partCopy);
      }
      getHBase().putPartitions(partsCopy);
      commit = true;
      return true;
    } catch (IOException e) {
      LOG.error("Unable to add partitions", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
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
    boolean commit = false;
    openTransaction();
    try {
      Partition part = getHBase().getPartition(HiveStringUtils.normalizeIdentifier(dbName),
          HiveStringUtils.normalizeIdentifier(tableName), part_vals);
      if (part == null) {
        throw new NoSuchObjectException("Unable to find partition " +
            partNameForErrorMsg(dbName, tableName, part_vals));
      }
      commit = true;
      return part;
    } catch (IOException e) {
      LOG.error("Unable to get partition", e);
      throw new MetaException("Error reading partition " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean doesPartitionExist(String dbName, String tableName, List<String> part_vals) throws
      MetaException, NoSuchObjectException {
    boolean commit = false;
    openTransaction();
    try {
      boolean exists = getHBase().getPartition(HiveStringUtils.normalizeIdentifier(dbName),
          HiveStringUtils.normalizeIdentifier(tableName), part_vals) != null;
      commit = true;
      return exists;
    } catch (IOException e) {
      LOG.error("Unable to get partition", e);
      throw new MetaException("Error reading partition " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean dropPartition(String dbName, String tableName, List<String> part_vals) throws
      MetaException, NoSuchObjectException, InvalidObjectException, InvalidInputException {
    boolean commit = false;
    openTransaction();
    try {
      dbName = HiveStringUtils.normalizeIdentifier(dbName);
      tableName = HiveStringUtils.normalizeIdentifier(tableName);
      getHBase().deletePartition(dbName, tableName, HBaseUtils.getPartitionKeyTypes(
          getTable(dbName, tableName).getPartitionKeys()), part_vals);
      // Drop any cached stats that reference this partitions
      getHBase().getStatsCache().invalidate(dbName, tableName,
          buildExternalPartName(dbName, tableName, part_vals));
      commit = true;
      return true;
    } catch (IOException e) {
      LOG.error("Unable to delete db" + e);
      throw new MetaException("Unable to drop partition " + partNameForErrorMsg(dbName, tableName,
          part_vals));
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<Partition> getPartitions(String dbName, String tableName, int max) throws
      MetaException, NoSuchObjectException {
    boolean commit = false;
    openTransaction();
    try {
      List<Partition> parts = getHBase().scanPartitionsInTable(HiveStringUtils.normalizeIdentifier(dbName),
          HiveStringUtils.normalizeIdentifier(tableName), max);
      commit = true;
      return parts;
    } catch (IOException e) {
      LOG.error("Unable to get partitions", e);
      throw new MetaException("Error scanning partitions");
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public void alterTable(String dbName, String tableName, Table newTable) throws InvalidObjectException,
      MetaException {
    boolean commit = false;
    openTransaction();
    try {
      Table newTableCopy = newTable.deepCopy();
      newTableCopy.setDbName(HiveStringUtils.normalizeIdentifier(newTableCopy.getDbName()));
      List<String> oldPartTypes = getTable(dbName, tableName).getPartitionKeys()==null?
          null:HBaseUtils.getPartitionKeyTypes(getTable(dbName, tableName).getPartitionKeys());
      newTableCopy.setTableName(HiveStringUtils.normalizeIdentifier(newTableCopy.getTableName()));
      getHBase().replaceTable(getHBase().getTable(HiveStringUtils.normalizeIdentifier(dbName),
          HiveStringUtils.normalizeIdentifier(tableName)), newTableCopy);
      if (newTable.getPartitionKeys() != null && newTable.getPartitionKeys().size() > 0
          && !tableName.equals(newTable.getTableName())) {
        // They renamed the table, so we need to change each partition as well, since it changes
        // the key.
        try {
          List<Partition> oldParts = getPartitions(dbName, tableName, -1);
          List<Partition> newParts = new ArrayList<>(oldParts.size());
          for (Partition oldPart : oldParts) {
            Partition newPart = oldPart.deepCopy();
            newPart.setTableName(newTable.getTableName());
            newParts.add(newPart);
          }
          getHBase().replacePartitions(oldParts, newParts, oldPartTypes);
        } catch (NoSuchObjectException e) {
          LOG.debug("No partitions found for old table so not worrying about it");
        }

      }
      commit = true;
    } catch (IOException e) {
      LOG.error("Unable to alter table " + tableNameForErrorMsg(dbName, tableName), e);
      throw new MetaException("Unable to alter table " + tableNameForErrorMsg(dbName, tableName));
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<String> getTables(String dbName, String pattern) throws MetaException {
    boolean commit = false;
    openTransaction();
    try {
      List<String> tableNames = getTableNamesInTx(dbName, pattern);
      commit = true;
      return tableNames;
    } catch (IOException e) {
      LOG.error("Unable to get tables ", e);
      throw new MetaException("Unable to get tables, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<String> getTables(String dbName, String pattern, TableType tableType) throws MetaException {
    throw new UnsupportedOperationException();
  }

  private List<String> getTableNamesInTx(String dbName, String pattern) throws IOException {
    List<Table> tables = getHBase().scanTables(HiveStringUtils.normalizeIdentifier(dbName),
        pattern==null?null:HiveStringUtils.normalizeIdentifier(likeToRegex(pattern)));
    List<String> tableNames = new ArrayList<String>(tables.size());
    for (Table table : tables) tableNames.add(table.getTableName());
    return tableNames;
  }

  @Override
  public List<TableMeta> getTableMeta(String dbNames, String tableNames, List<String> tableTypes)
      throws MetaException {
    boolean commit = false;
    openTransaction();
    try {
      List<TableMeta> metas = new ArrayList<>();
      for (String dbName : getDatabases(dbNames)) {
        for (Table table : getTableObjectsByName(dbName, getTableNamesInTx(dbName, tableNames))) {
          if (tableTypes == null || tableTypes.contains(table.getTableType())) {
            TableMeta metaData = new TableMeta(
              table.getDbName(), table.getTableName(), table.getTableType());
            metaData.setComments(table.getParameters().get("comment"));
            metas.add(metaData);
          }
        }
      }
      commit = true;
      return metas;
    } catch (Exception e) {
      LOG.error("Unable to get tables ", e);
      throw new MetaException("Unable to get tables, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<Table> getTableObjectsByName(String dbname, List<String> tableNames) throws
      MetaException, UnknownDBException {
    boolean commit = false;
    openTransaction();
    try {
      List<String> normalizedTableNames = new ArrayList<String>(tableNames.size());
      for (String tableName : tableNames) {
        normalizedTableNames.add(HiveStringUtils.normalizeIdentifier(tableName));
      }
      List<Table> tables = getHBase().getTables(HiveStringUtils.normalizeIdentifier(dbname),
          normalizedTableNames);
      commit = true;
      return tables;
    } catch (IOException e) {
      LOG.error("Unable to get tables ", e);
      throw new MetaException("Unable to get tables, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<String> getAllTables(String dbName) throws MetaException {
    return getTables(dbName, null);
  }

  @Override
  public int getTableCount() throws MetaException {
    try {
      return getHBase().getTableCount();
    } catch (IOException e) {
      LOG.error("Unable to get table count", e);
      throw new MetaException("Error scanning tables");
    }
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
    boolean commit = false;
    openTransaction();
    try {
      List<Partition> parts = getHBase().scanPartitionsInTable(HiveStringUtils.normalizeIdentifier(db_name),
          HiveStringUtils.normalizeIdentifier(tbl_name), max_parts);
      if (parts == null) return null;
      List<String> names = new ArrayList<String>(parts.size());
      Table table = getHBase().getTable(HiveStringUtils.normalizeIdentifier(db_name),
          HiveStringUtils.normalizeIdentifier(tbl_name));
      for (Partition p : parts) {
        names.add(buildExternalPartName(table, p));
      }
      commit = true;
      return names;
    } catch (IOException e) {
      LOG.error("Unable to get partitions", e);
      throw new MetaException("Error scanning partitions");
    } finally {
      commitOrRoleBack(commit);
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
    boolean commit = false;
    openTransaction();
    try {
      Partition new_partCopy = new_part.deepCopy();
      new_partCopy.setDbName(HiveStringUtils.normalizeIdentifier(new_partCopy.getDbName()));
      new_partCopy.setTableName(HiveStringUtils.normalizeIdentifier(new_partCopy.getTableName()));
      Partition oldPart = getHBase().getPartition(HiveStringUtils.normalizeIdentifier(db_name),
          HiveStringUtils.normalizeIdentifier(tbl_name), part_vals);
      getHBase().replacePartition(oldPart, new_partCopy, HBaseUtils.getPartitionKeyTypes(
          getTable(db_name, tbl_name).getPartitionKeys()));
      // Drop any cached stats that reference this partitions
      getHBase().getStatsCache().invalidate(HiveStringUtils.normalizeIdentifier(db_name),
          HiveStringUtils.normalizeIdentifier(tbl_name),
          buildExternalPartName(db_name, tbl_name, part_vals));
      commit = true;
    } catch (IOException e) {
      LOG.error("Unable to add partition", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public void alterPartitions(String db_name, String tbl_name, List<List<String>> part_vals_list,
                              List<Partition> new_parts) throws InvalidObjectException,
      MetaException {
    boolean commit = false;
    openTransaction();
    try {
      List<Partition> new_partsCopy = new ArrayList<Partition>();
      for (int i=0;i<new_parts.size();i++) {
        Partition newPartCopy = new_parts.get(i).deepCopy();
        newPartCopy.setDbName(HiveStringUtils.normalizeIdentifier(newPartCopy.getDbName()));
        newPartCopy.setTableName(HiveStringUtils.normalizeIdentifier(newPartCopy.getTableName()));
        new_partsCopy.add(i, newPartCopy);
      }
      List<Partition> oldParts = getHBase().getPartitions(HiveStringUtils.normalizeIdentifier(db_name),
          HiveStringUtils.normalizeIdentifier(tbl_name),
          HBaseUtils.getPartitionKeyTypes(getTable(HiveStringUtils.normalizeIdentifier(db_name),
          HiveStringUtils.normalizeIdentifier(tbl_name)).getPartitionKeys()), part_vals_list);
      getHBase().replacePartitions(oldParts, new_partsCopy, HBaseUtils.getPartitionKeyTypes(
          getTable(db_name, tbl_name).getPartitionKeys()));
      for (List<String> part_vals : part_vals_list) {
        getHBase().getStatsCache().invalidate(HiveStringUtils.normalizeIdentifier(db_name),
            HiveStringUtils.normalizeIdentifier(tbl_name),
            buildExternalPartName(db_name, tbl_name, part_vals));
      }
      commit = true;
    } catch (IOException e) {
      LOG.error("Unable to add partition", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean addIndex(Index index) throws InvalidObjectException, MetaException {
    boolean commit = false;
    openTransaction();
    try {
      index.setDbName(HiveStringUtils.normalizeIdentifier(index.getDbName()));
      index.setOrigTableName(HiveStringUtils.normalizeIdentifier(index.getOrigTableName()));
      index.setIndexName(HiveStringUtils.normalizeIdentifier(index.getIndexName()));
      index.setIndexTableName(HiveStringUtils.normalizeIdentifier(index.getIndexTableName()));
      getHBase().putIndex(index);
      commit = true;
    } catch (IOException e) {
      LOG.error("Unable to create index ", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
    return commit;
  }

  @Override
  public Index getIndex(String dbName, String origTableName, String indexName) throws
      MetaException {
    boolean commit = false;
    openTransaction();
    try {
      Index index = getHBase().getIndex(HiveStringUtils.normalizeIdentifier(dbName),
          HiveStringUtils.normalizeIdentifier(origTableName),
          HiveStringUtils.normalizeIdentifier(indexName));
      if (index == null) {
        LOG.debug("Unable to find index " + indexNameForErrorMsg(dbName, origTableName, indexName));
      }
      commit = true;
      return index;
    } catch (IOException e) {
      LOG.error("Unable to get index", e);
      throw new MetaException("Error reading index " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean dropIndex(String dbName, String origTableName, String indexName) throws
      MetaException {
    boolean commit = false;
    openTransaction();
    try {
      getHBase().deleteIndex(HiveStringUtils.normalizeIdentifier(dbName),
          HiveStringUtils.normalizeIdentifier(origTableName),
          HiveStringUtils.normalizeIdentifier(indexName));
      commit = true;
      return true;
    } catch (IOException e) {
      LOG.error("Unable to delete index" + e);
      throw new MetaException("Unable to drop index "
          + indexNameForErrorMsg(dbName, origTableName, indexName));
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<Index> getIndexes(String dbName, String origTableName, int max) throws MetaException {
    boolean commit = false;
    openTransaction();
    try {
      List<Index> indexes = getHBase().scanIndexes(HiveStringUtils.normalizeIdentifier(dbName),
          HiveStringUtils.normalizeIdentifier(origTableName), max);
      commit = true;
      return indexes;
    } catch (IOException e) {
      LOG.error("Unable to get indexes", e);
      throw new MetaException("Error scanning indexxes");
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<String> listIndexNames(String dbName, String origTableName, short max) throws
      MetaException {
    boolean commit = false;
    openTransaction();
    try {
      List<Index> indexes = getHBase().scanIndexes(HiveStringUtils.normalizeIdentifier(dbName),
          HiveStringUtils.normalizeIdentifier(origTableName), max);
      if (indexes == null) return null;
      List<String> names = new ArrayList<String>(indexes.size());
      for (Index index : indexes) {
        names.add(index.getIndexName());
      }
      commit = true;
      return names;
    } catch (IOException e) {
      LOG.error("Unable to get indexes", e);
      throw new MetaException("Error scanning indexes");
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public void alterIndex(String dbname, String baseTblName, String name, Index newIndex) throws
      InvalidObjectException, MetaException {
    boolean commit = false;
    openTransaction();
    try {
      Index newIndexCopy = newIndex.deepCopy();
      newIndexCopy.setDbName(HiveStringUtils.normalizeIdentifier(newIndexCopy.getDbName()));
      newIndexCopy.setOrigTableName(
          HiveStringUtils.normalizeIdentifier(newIndexCopy.getOrigTableName()));
      newIndexCopy.setIndexName(HiveStringUtils.normalizeIdentifier(newIndexCopy.getIndexName()));
      getHBase().replaceIndex(getHBase().getIndex(HiveStringUtils.normalizeIdentifier(dbname),
          HiveStringUtils.normalizeIdentifier(baseTblName),
          HiveStringUtils.normalizeIdentifier(name)), newIndexCopy);
      commit = true;
    } catch (IOException e) {
      LOG.error("Unable to alter index " + indexNameForErrorMsg(dbname, baseTblName, name), e);
      throw new MetaException("Unable to alter index "
          + indexNameForErrorMsg(dbname, baseTblName, name));
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<Partition> getPartitionsByFilter(String dbName, String tblName, String filter,
                                               short maxParts) throws MetaException,
      NoSuchObjectException {
    final ExpressionTree exprTree = (filter != null && !filter.isEmpty()) ? PartFilterExprUtil
        .getFilterParser(filter).tree : ExpressionTree.EMPTY_TREE;
    List<Partition> result = new ArrayList<Partition>();
    boolean commit = false;
    openTransaction();
    try {
      getPartitionsByExprInternal(HiveStringUtils.normalizeIdentifier(dbName),
          HiveStringUtils.normalizeIdentifier(tblName), exprTree, maxParts, result);
      return result;
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean getPartitionsByExpr(String dbName, String tblName, byte[] expr,
                                     String defaultPartitionName, short maxParts,
                                     List<Partition> result) throws TException {
    final ExpressionTree exprTree = PartFilterExprUtil.makeExpressionTree(expressionProxy, expr);
    dbName = HiveStringUtils.normalizeIdentifier(dbName);
    tblName = HiveStringUtils.normalizeIdentifier(tblName);
    Table table = getTable(dbName, tblName);
    boolean commit = false;
    openTransaction();
    try {
      if (exprTree == null) {
        List<String> partNames = new LinkedList<String>();
        boolean hasUnknownPartitions = getPartitionNamesPrunedByExprNoTxn(
            table, expr, defaultPartitionName, maxParts, partNames);
        result.addAll(getPartitionsByNames(dbName, tblName, partNames));
        return hasUnknownPartitions;
      } else {
        return getPartitionsByExprInternal(dbName, tblName, exprTree, maxParts, result);
      }
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public int getNumPartitionsByFilter(String dbName, String tblName, String filter)
    throws MetaException, NoSuchObjectException {
    final ExpressionTree exprTree = (filter != null && !filter.isEmpty()) ? PartFilterExprUtil
      .getFilterParser(filter).tree : ExpressionTree.EMPTY_TREE;
    List<Partition> result = new ArrayList<Partition>();
    boolean commit = false;
    openTransaction();
    try {
      return getPartitionsByFilter(dbName, tblName, filter, Short.MAX_VALUE).size();
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public int getNumPartitionsByExpr(String dbName, String tblName, byte[] expr)
      throws MetaException, NoSuchObjectException {
    final ExpressionTree exprTree = PartFilterExprUtil.makeExpressionTree(expressionProxy, expr);
    List<Partition> result = new ArrayList<Partition>();
    boolean commit = false;
    openTransaction();
    try {
      getPartitionsByExprInternal(dbName, tblName, exprTree, Short.MAX_VALUE, result);
      return result.size();
    } finally {
      commitOrRoleBack(commit);
    }
  }

  /**
   * Gets the partition names from a table, pruned using an expression.
   * @param table Table.
   * @param expr Expression.
   * @param defaultPartName Default partition name from job config, if any.
   * @param maxParts Maximum number of partition names to return.
   * @param result The resulting names.
   * @return Whether the result contains any unknown partitions.
   * @throws NoSuchObjectException
   */
  private boolean getPartitionNamesPrunedByExprNoTxn(Table table, byte[] expr,
      String defaultPartName, short maxParts, List<String> result) throws MetaException, NoSuchObjectException {
    List<Partition> parts = getPartitions(
        table.getDbName(), table.getTableName(), maxParts);
    for (Partition part : parts) {
      result.add(Warehouse.makePartName(table.getPartitionKeys(), part.getValues()));
    }
    List<String> columnNames = new ArrayList<String>();
    List<PrimitiveTypeInfo> typeInfos = new ArrayList<PrimitiveTypeInfo>();
    for (FieldSchema fs : table.getPartitionKeys()) {
      columnNames.add(fs.getName());
      typeInfos.add(TypeInfoFactory.getPrimitiveTypeInfo(fs.getType()));
    }
    if (defaultPartName == null || defaultPartName.isEmpty()) {
      defaultPartName = HiveConf.getVar(getConf(), HiveConf.ConfVars.DEFAULTPARTITIONNAME);
    }
    return expressionProxy.filterPartitionsByExpr(
        columnNames, typeInfos, expr, defaultPartName, result);
  }

  private boolean getPartitionsByExprInternal(String dbName, String tblName,
      ExpressionTree exprTree, short maxParts, List<Partition> result) throws MetaException,
      NoSuchObjectException {

    dbName = HiveStringUtils.normalizeIdentifier(dbName);
    tblName = HiveStringUtils.normalizeIdentifier(tblName);
    Table table = getTable(dbName, tblName);
    if (table == null) {
      throw new NoSuchObjectException("Unable to find table " + dbName + "." + tblName);
    }
    // general hbase filter plan from expression tree
    PlanResult planRes = HBaseFilterPlanUtil.getFilterPlan(exprTree, table.getPartitionKeys());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Hbase Filter Plan generated : " + planRes.plan);
    }

    // results from scans need to be merged as there can be overlapping results between
    // the scans. Use a map of list of partition values to partition for this.
    Map<List<String>, Partition> mergedParts = new HashMap<List<String>, Partition>();
    for (ScanPlan splan : planRes.plan.getPlans()) {
      try {
        List<Partition> parts = getHBase().scanPartitions(dbName, tblName,
            splan.getStartRowSuffix(dbName, tblName, table.getPartitionKeys()),
            splan.getEndRowSuffix(dbName, tblName, table.getPartitionKeys()),
            splan.getFilter(table.getPartitionKeys()), -1);
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
  public int getPartitionCount() throws MetaException {
    try {
      return getHBase().getPartitionCount();
    } catch (IOException e) {
      LOG.error("Unable to get partition count", e);
      throw new MetaException("Error scanning partitions");
    }
  }

  @Override
  public boolean addRole(String roleName, String ownerName) throws InvalidObjectException,
      MetaException, NoSuchObjectException {
    int now = (int)(System.currentTimeMillis()/1000);
    Role role = new Role(roleName, now, ownerName);
    boolean commit = false;
    openTransaction();
    try {
      if (getHBase().getRole(roleName) != null) {
        throw new InvalidObjectException("Role " + roleName + " already exists");
      }
      getHBase().putRole(role);
      commit = true;
      return true;
    } catch (IOException e) {
      LOG.error("Unable to create role ", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean removeRole(String roleName) throws MetaException, NoSuchObjectException {
    boolean commit = false;
    openTransaction();
    try {
      Set<String> usersInRole = getHBase().findAllUsersInRole(roleName);
      getHBase().deleteRole(roleName);
      getHBase().removeRoleGrants(roleName);
      for (String user : usersInRole) {
        getHBase().buildRoleMapForUser(user);
      }
      commit = true;
      return true;
    } catch (IOException e) {
      LOG.error("Unable to delete role" + e);
      throw new MetaException("Unable to drop role " + roleName);
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean grantRole(Role role, String userName, PrincipalType principalType, String grantor,
                           PrincipalType grantorType, boolean grantOption)
      throws MetaException, NoSuchObjectException, InvalidObjectException {
    boolean commit = false;
    openTransaction();
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
      commit = true;
      return true;
    } catch (IOException e) {
      LOG.error("Unable to grant role", e);
      throw new MetaException("Unable to grant role " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean revokeRole(Role role, String userName, PrincipalType principalType,
                            boolean grantOption) throws MetaException, NoSuchObjectException {
    boolean commit = false;
    openTransaction();
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
      commit = true;
      return true;
    } catch (IOException e) {
      LOG.error("Unable to revoke role " + role.getRoleName() + " from " + userName, e);
      throw new MetaException("Unable to revoke role " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public PrincipalPrivilegeSet getUserPrivilegeSet(String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    boolean commit = false;
    openTransaction();
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
      commit = true;
      return pps;
    } catch (IOException e) {
      LOG.error("Unable to get db privileges for user", e);
      throw new MetaException("Unable to get db privileges for user, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public PrincipalPrivilegeSet getDBPrivilegeSet(String dbName, String userName,
                                                 List<String> groupNames)
      throws InvalidObjectException, MetaException {
    boolean commit = false;
    openTransaction();
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
      commit = true;
      return pps;
    } catch (IOException e) {
      LOG.error("Unable to get db privileges for user", e);
      throw new MetaException("Unable to get db privileges for user, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public PrincipalPrivilegeSet getTablePrivilegeSet(String dbName, String tableName,
                                                    String userName, List<String> groupNames)
      throws InvalidObjectException, MetaException {
    boolean commit = false;
    openTransaction();
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
      commit = true;
      return pps;
    } catch (IOException e) {
      LOG.error("Unable to get db privileges for user", e);
      throw new MetaException("Unable to get db privileges for user, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
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
    boolean commit = false;
    openTransaction();
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
      commit = true;
      return privileges;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDBGrants(String principalName,
                                                         PrincipalType principalType,
                                                         String dbName) {
    List<PrivilegeGrantInfo> grants;
    List<HiveObjectPrivilege> privileges = new ArrayList<HiveObjectPrivilege>();
    boolean commit = false;
    openTransaction();
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
      commit = true;
      return privileges;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<HiveObjectPrivilege> listAllTableGrants(String principalName,
                                                      PrincipalType principalType,
                                                      String dbName,
                                                      String tableName) {
    List<PrivilegeGrantInfo> grants;
    List<HiveObjectPrivilege> privileges = new ArrayList<HiveObjectPrivilege>();
    boolean commit = false;
    openTransaction();
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
      commit = true;
      return privileges;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      commitOrRoleBack(commit);
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
    boolean commit = false;
    openTransaction();
    try {
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
      commit = true;
      return true;
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean revokePrivileges(PrivilegeBag privileges, boolean grantOption) throws
      InvalidObjectException, MetaException, NoSuchObjectException {
    boolean commit = false;
    openTransaction();
    try {
      for (HiveObjectPrivilege priv : privileges.getPrivileges()) {
        PrivilegeInfo privilegeInfo = findPrivilegeToGrantOrRevoke(priv);

        for (int i = 0; i < privilegeInfo.grants.size(); i++) {
          if (privilegeInfo.grants.get(i).getPrivilege().equals(
              priv.getGrantInfo().getPrivilege())) {
            if (grantOption) privilegeInfo.grants.get(i).setGrantOption(false);
            else privilegeInfo.grants.remove(i);
            break;
          }
        }
        writeBackGrantOrRevoke(priv, privilegeInfo);
      }
      commit = true;
      return true;
    } finally {
      commitOrRoleBack(commit);
    }
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
    boolean commit = false;
    openTransaction();
    try {
      Role role = getHBase().getRole(roleName);
      if (role == null) {
        throw new NoSuchObjectException("Unable to find role " + roleName);
      }
      commit = true;
      return role;
    } catch (IOException e) {
      LOG.error("Unable to get role", e);
      throw new NoSuchObjectException("Error reading table " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<String> listRoleNames() {
    boolean commit = false;
    openTransaction();
    try {
      List<Role> roles = getHBase().scanRoles();
      List<String> roleNames = new ArrayList<String>(roles.size());
      for (Role role : roles) roleNames.add(role.getRoleName());
      commit = true;
      return roleNames;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<Role> listRoles(String principalName, PrincipalType principalType) {
    List<Role> roles = new ArrayList<Role>();
    boolean commit = false;
    openTransaction();
    try {
      try {
        roles.addAll(getHBase().getPrincipalDirectRoles(principalName, principalType));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      // Add the public role if this is a user
      if (principalType == PrincipalType.USER) {
        roles.add(new Role(HiveMetaStore.PUBLIC, 0, null));
      }
      commit = true;
      return roles;
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<RolePrincipalGrant> listRolesWithGrants(String principalName,
                                                      PrincipalType principalType) {
    boolean commit = false;
    openTransaction();
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
      commit = true;
      return rpgs;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<RolePrincipalGrant> listRoleMembers(String roleName) {
    boolean commit = false;
    openTransaction();
    try {
      HbaseMetastoreProto.RoleGrantInfoList gil = getHBase().getRolePrincipals(roleName);
      List<RolePrincipalGrant> roleMaps = new ArrayList<RolePrincipalGrant>(gil.getGrantInfoList().size());
      for (HbaseMetastoreProto.RoleGrantInfo giw : gil.getGrantInfoList()) {
        roleMaps.add(new RolePrincipalGrant(roleName, giw.getPrincipalName(),
            HBaseUtils.convertPrincipalTypes(giw.getPrincipalType()),
            giw.getGrantOption(), (int)giw.getAddTime(), giw.getGrantor(),
            HBaseUtils.convertPrincipalTypes(giw.getGrantorType())));
      }
      commit = true;
      return roleMaps;
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      commitOrRoleBack(commit);
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
      partNames.add(buildExternalPartName(HiveStringUtils.normalizeIdentifier(db_name),
          HiveStringUtils.normalizeIdentifier(tbl_name), part.getValues()));
    }
    return partNames;
  }


  @Override
  public List<Partition> listPartitionsPsWithAuth(String db_name, String tbl_name,
                                                  List<String> part_vals, short max_parts,
                                                  String userName, List<String> groupNames)
      throws MetaException, NoSuchObjectException {
    // We don't handle auth info with partitions
    boolean commit = false;
    openTransaction();
    try {
      List<Partition> parts = getHBase().scanPartitions(HiveStringUtils.normalizeIdentifier(db_name),
          HiveStringUtils.normalizeIdentifier(tbl_name), part_vals, max_parts);
      commit = true;
      return parts;
    } catch (IOException e) {
      LOG.error("Unable to list partition names", e);
      throw new MetaException("Failed to list part names, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics colStats) throws
      NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean commit = false;
    openTransaction();
    try {
      //update table properties
      List<ColumnStatisticsObj> statsObjs = colStats.getStatsObj();
      List<String> colNames = new ArrayList<>();
      for (ColumnStatisticsObj statsObj:statsObjs) {
        colNames.add(statsObj.getColName());
      }
      String dbName = colStats.getStatsDesc().getDbName();
      String tableName = colStats.getStatsDesc().getTableName();
      Table newTable = getTable(dbName, tableName);
      Table newTableCopy = newTable.deepCopy();
      StatsSetupConst.setColumnStatsState(newTableCopy.getParameters(), colNames);
      getHBase().replaceTable(newTable, newTableCopy);

      getHBase().updateStatistics(colStats.getStatsDesc().getDbName(),
          colStats.getStatsDesc().getTableName(), null, colStats);

      commit = true;
      return true;
    } catch (IOException e) {
      LOG.error("Unable to update column statistics", e);
      throw new MetaException("Failed to update column statistics, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean updatePartitionColumnStatistics(ColumnStatistics colStats,
                                                 List<String> partVals) throws
      NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    boolean commit = false;
    openTransaction();
    try {
      // update partition properties
      String db_name = colStats.getStatsDesc().getDbName();
      String tbl_name = colStats.getStatsDesc().getTableName();
      Partition oldPart = getHBase().getPartition(db_name, tbl_name, partVals);
      Partition new_partCopy = oldPart.deepCopy();
      List<String> colNames = new ArrayList<>();
      List<ColumnStatisticsObj> statsObjs = colStats.getStatsObj();
      for (ColumnStatisticsObj statsObj : statsObjs) {
        colNames.add(statsObj.getColName());
      }
      StatsSetupConst.setColumnStatsState(new_partCopy.getParameters(), colNames);
      getHBase().replacePartition(oldPart, new_partCopy,
          HBaseUtils.getPartitionKeyTypes(getTable(db_name, tbl_name).getPartitionKeys()));

      getHBase().updateStatistics(colStats.getStatsDesc().getDbName(),
          colStats.getStatsDesc().getTableName(), partVals, colStats);
      // We need to invalidate aggregates that include this partition
      getHBase().getStatsCache().invalidate(colStats.getStatsDesc().getDbName(),
          colStats.getStatsDesc().getTableName(), colStats.getStatsDesc().getPartName());

      commit = true;
      return true;
    } catch (IOException e) {
      LOG.error("Unable to update column statistics", e);
      throw new MetaException("Failed to update column statistics, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public ColumnStatistics getTableColumnStatistics(String dbName, String tableName,
                                                   List<String> colName) throws MetaException,
      NoSuchObjectException {
    boolean commit = false;
    openTransaction();
    try {
      ColumnStatistics cs = getHBase().getTableStatistics(dbName, tableName, colName);
      commit = true;
      return cs;
    } catch (IOException e) {
      LOG.error("Unable to fetch column statistics", e);
      throw new MetaException("Failed to fetch column statistics, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<ColumnStatistics> getPartitionColumnStatistics(String dbName, String tblName,
      List<String> partNames, List<String> colNames) throws MetaException, NoSuchObjectException {
    List<List<String>> partVals = new ArrayList<List<String>>(partNames.size());
    for (String partName : partNames) {
      partVals.add(partNameToVals(partName));
    }
    boolean commit = false;
    openTransaction();
    try {
      List<ColumnStatistics> cs =
          getHBase().getPartitionStatistics(dbName, tblName, partNames, partVals, colNames);
      commit = true;
      return cs;
    } catch (IOException e) {
      LOG.error("Unable to fetch column statistics", e);
      throw new MetaException("Failed fetching column statistics, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
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
    boolean commit = false;
    boolean hasAnyStats = false;
    openTransaction();
    try {
      AggrStats aggrStats = new AggrStats();
      aggrStats.setPartsFound(0);
      for (String colName : colNames) {
        try {
          AggrStats oneCol =
              getHBase().getStatsCache().get(dbName, tblName, partNames, colName);
          if (oneCol.getColStatsSize() > 0) {
            assert oneCol.getColStatsSize() == 1;
            aggrStats.setPartsFound(oneCol.getPartsFound());
            aggrStats.addToColStats(oneCol.getColStats().get(0));
            hasAnyStats = true;
          }
        } catch (CacheLoader.InvalidCacheLoadException e) {
          LOG.debug("Found no stats for column " + colName);
          // This means we have no stats at all for this column for these partitions, so just
          // move on.
        }
      }
      commit = true;
      if (!hasAnyStats) {
        // Set the required field.
        aggrStats.setColStats(new ArrayList<ColumnStatisticsObj>());
      }
      return aggrStats;
    } catch (IOException e) {
      LOG.error("Unable to fetch aggregate column statistics", e);
      throw new MetaException("Failed fetching aggregate column statistics, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public long cleanupEvents() {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean addToken(String tokenIdentifier, String delegationToken) {
    boolean commit = false;
    openTransaction();
    try {
      getHBase().putDelegationToken(tokenIdentifier, delegationToken);
      commit = true;
      return commit; // See HIVE-11302, for now always returning true
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean removeToken(String tokenIdentifier) {
    boolean commit = false;
    openTransaction();
    try {
      getHBase().deleteDelegationToken(tokenIdentifier);
      commit = true;
      return commit; // See HIVE-11302, for now always returning true
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public String getToken(String tokenIdentifier) {
    boolean commit = false;
    openTransaction();
    try {
      String token = getHBase().getDelegationToken(tokenIdentifier);
      commit = true;
      return token;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<String> getAllTokenIdentifiers() {
    boolean commit = false;
    openTransaction();
    try {
      List<String> ids = getHBase().scanDelegationTokenIdentifiers();
      commit = true;
      return ids;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public int addMasterKey(String key) throws MetaException {
    boolean commit = false;
    openTransaction();
    try {
      long seq = getHBase().getNextSequence(HBaseReadWrite.MASTER_KEY_SEQUENCE);
      getHBase().putMasterKey((int) seq, key);
      commit = true;
      return (int)seq;
    } catch (IOException e) {
      LOG.error("Unable to add master key", e);
      throw new MetaException("Failed adding master key, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public void updateMasterKey(Integer seqNo, String key) throws NoSuchObjectException,
      MetaException {
    boolean commit = false;
    openTransaction();
    try {
      if (getHBase().getMasterKey(seqNo) == null) {
        throw new NoSuchObjectException("No key found with keyId: " + seqNo);
      }
      getHBase().putMasterKey(seqNo, key);
      commit = true;
    } catch (IOException e) {
      LOG.error("Unable to update master key", e);
      throw new MetaException("Failed updating master key, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public boolean removeMasterKey(Integer keySeq) {
    boolean commit = false;
    openTransaction();
    try {
      getHBase().deleteMasterKey(keySeq);
      commit = true;
      return true;  // See HIVE-11302, for now always returning true
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public String[] getMasterKeys() {
    boolean commit = false;
    openTransaction();
    try {
      List<String> keys = getHBase().scanMasterKeys();
      commit = true;
      return keys.toArray(new String[keys.size()]);
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public void verifySchema() throws MetaException {

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
    boolean commit = false;
    openTransaction();
    try {
      for (String partName : partNames) {
        dropPartition(dbName, tblName, partNameToVals(partName));
      }
      commit = true;
    } catch (Exception e) {
      LOG.error("Unable to drop partitions", e);
      throw new NoSuchObjectException("Failure dropping partitions, " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalDBGrantsAll(String principalName,
                                                            PrincipalType principalType) {
    List<HiveObjectPrivilege> privileges = new ArrayList<HiveObjectPrivilege>();
    boolean commit = false;
    openTransaction();
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
      commit = true;
      return privileges;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<HiveObjectPrivilege> listPrincipalTableGrantsAll(String principalName,
                                                               PrincipalType principalType) {
    List<HiveObjectPrivilege> privileges = new ArrayList<HiveObjectPrivilege>();
    boolean commit = false;
    openTransaction();
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
      commit = true;
      return privileges;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      commitOrRoleBack(commit);
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
    boolean commit = false;
    openTransaction();
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
      commit = true;
      return privileges;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<HiveObjectPrivilege> listDBGrantsAll(String dbName) {
    List<HiveObjectPrivilege> privileges = new ArrayList<HiveObjectPrivilege>();
    boolean commit = false;
    openTransaction();
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
      commit = true;
      return privileges;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      commitOrRoleBack(commit);
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
    boolean commit = false;
    openTransaction();
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
      commit = true;
      return privileges;
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      commitOrRoleBack(commit);
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
    boolean commit = false;
    openTransaction();
    try {
      getHBase().putFunction(func);
      commit = true;
    } catch (IOException e) {
      LOG.error("Unable to create function", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public void alterFunction(String dbName, String funcName, Function newFunction) throws
      InvalidObjectException, MetaException {
    boolean commit = false;
    openTransaction();
    try {
      getHBase().putFunction(newFunction);
      commit = true;
    } catch (IOException e) {
      LOG.error("Unable to alter function ", e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public void dropFunction(String dbName, String funcName) throws MetaException,
      NoSuchObjectException, InvalidObjectException, InvalidInputException {
    boolean commit = false;
    openTransaction();
    try {
      getHBase().deleteFunction(dbName, funcName);
      commit = true;
    } catch (IOException e) {
      LOG.error("Unable to delete function" + e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public Function getFunction(String dbName, String funcName) throws MetaException {
    boolean commit = false;
    openTransaction();
    try {
      Function func = getHBase().getFunction(dbName, funcName);
      commit = true;
      return func;
    } catch (IOException e) {
      LOG.error("Unable to get function" + e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<Function> getAllFunctions() throws MetaException {
    boolean commit = false;
    openTransaction();
    try {
      List<Function> funcs = getHBase().scanFunctions(null, ".*");
      commit = true;
      return funcs;
    } catch (IOException e) {
      LOG.error("Unable to get functions" + e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<String> getFunctions(String dbName, String pattern) throws MetaException {
    boolean commit = false;
    openTransaction();
    try {
      List<Function> funcs = getHBase().scanFunctions(dbName, likeToRegex(pattern));
      List<String> funcNames = new ArrayList<String>(funcs.size());
      for (Function func : funcs) funcNames.add(func.getFunctionName());
      commit = true;
      return funcNames;
    } catch (IOException e) {
      LOG.error("Unable to get functions" + e);
      throw new MetaException("Unable to read from or write to hbase " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
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
    // TODO: why not re-intialize HBaseReadWrite?
    Configuration oldConf = conf;
    conf = configuration;
    if (expressionProxy != null && conf != oldConf) {
      LOG.warn("Unexpected setConf when we were already configured");
    }
    if (expressionProxy == null || conf != oldConf) {
      expressionProxy = PartFilterExprUtil.createExpressionProxy(conf);
    }
    if (conf != oldConf) {
      fmHandlers = HiveMetaStore.createHandlerMap();
      configureFileMetadataHandlers(fmHandlers.values());
    }
  }

  private void configureFileMetadataHandlers(Collection<FileMetadataHandler> fmHandlers) {
    for (FileMetadataHandler fmh : fmHandlers) {
      fmh.configure(conf, expressionProxy, getHBase());
    }
  }

  @Override
  public FileMetadataHandler getFileMetadataHandler(FileMetadataExprType type) {
    return fmHandlers.get(type);
  }

  @Override
  public Configuration getConf() {
    return conf;

  }

  private HBaseReadWrite getHBase() {
    if (hbase == null) {
      HBaseReadWrite.setConf(conf);
      hbase = HBaseReadWrite.getInstance();
    }
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

  // This is for building error messages only.  It does not look up anything in the metastore as
  // they may just throw another error.
  private String indexNameForErrorMsg(String dbName, String origTableName, String indexName) {
    return tableNameForErrorMsg(dbName, origTableName) + "." + indexName;
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

  private static List<String> partNameToVals(String name) {
    if (name == null) return null;
    List<String> vals = new ArrayList<String>();
    String[] kvp = name.split("/");
    for (String kv : kvp) {
      vals.add(FileUtils.unescapePathName(kv.substring(kv.indexOf('=') + 1)));
    }
    return vals;
  }

  static List<List<String>> partNameListToValsList(List<String> partNames) {
    List<List<String>> valLists = new ArrayList<List<String>>(partNames.size());
    for (String partName : partNames) {
      valLists.add(partNameToVals(partName));
    }
    return valLists;
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

  private void commitOrRoleBack(boolean commit) {
    if (commit) {
      LOG.debug("Committing transaction");
      commitTransaction();
    } else {
      LOG.debug("Rolling back transaction");
      rollbackTransaction();
    }
  }

  @VisibleForTesting HBaseReadWrite backdoor() {
    return getHBase();
  }

  @Override
  public boolean isFileMetadataSupported() {
    return true;
  }

  @Override
  public ByteBuffer[] getFileMetadata(List<Long> fileIds) throws MetaException {
    openTransaction();
    boolean commit = true;
    try {
      return getHBase().getFileMetadata(fileIds);
    } catch (IOException e) {
      commit = false;
      LOG.error("Unable to get file metadata", e);
      throw new MetaException("Error reading file metadata " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public void getFileMetadataByExpr(List<Long> fileIds, FileMetadataExprType type, byte[] expr,
      ByteBuffer[] metadatas, ByteBuffer[] results, boolean[] eliminated) throws MetaException {
    FileMetadataHandler fmh = fmHandlers.get(type);
    boolean commit = true;
    try {
      fmh.getFileMetadataByExpr(fileIds, expr, metadatas, results, eliminated);
    } catch (IOException e) {
      LOG.error("Unable to get file metadata by expr", e);
      commit = false;
      throw new MetaException("Error reading file metadata by expr" + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public void putFileMetadata(List<Long> fileIds, List<ByteBuffer> metadata,
      FileMetadataExprType type) throws MetaException {
    openTransaction();
    boolean commit = false;
    try {
      ByteBuffer[][] addedVals = null;
      ByteBuffer[] addedCols = null;
      if (type != null) {
        FileMetadataHandler fmh = fmHandlers.get(type);
        addedCols = fmh.createAddedCols();
        if (addedCols != null) {
          addedVals = fmh.createAddedColVals(metadata);
        }
      }
      getHBase().storeFileMetadata(fileIds, metadata, addedCols, addedVals);
      commit = true;
    } catch (IOException | InterruptedException e) {
      LOG.error("Unable to store file metadata", e);
      throw new MetaException("Error storing file metadata " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<SQLPrimaryKey> getPrimaryKeys(String db_name, String tbl_name) throws MetaException {
    boolean commit = false;
    openTransaction();
    try {
      List<SQLPrimaryKey> pk = getHBase().getPrimaryKey(db_name, tbl_name);
      commit = true;
      return pk;
    } catch (IOException e) {
      LOG.error("Unable to get primary key", e);
      throw new MetaException("Error reading db " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public List<SQLForeignKey> getForeignKeys(String parent_db_name, String parent_tbl_name,
                                            String foreign_db_name, String foreign_tbl_name)
      throws MetaException {
    boolean commit = false;
    openTransaction();
    try {
      List<SQLForeignKey> fks = getHBase().getForeignKeys(foreign_db_name, foreign_tbl_name);
      if (fks == null || fks.size() == 0) return null;
      List<SQLForeignKey> result = new ArrayList<>(fks.size());
      for (SQLForeignKey fkcol : fks) {
        if ((parent_db_name == null || fkcol.getPktable_db().equals(parent_db_name)) &&
            (parent_tbl_name == null || fkcol.getPktable_name().equals(parent_tbl_name))) {
          result.add(fkcol);
        }
      }
      commit = true;
      return result;
    } catch (IOException e) {
      LOG.error("Unable to get foreign key", e);
      throw new MetaException("Error reading db " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public void createTableWithConstraints(Table tbl, List<SQLPrimaryKey> primaryKeys,
                                         List<SQLForeignKey> foreignKeys)
      throws InvalidObjectException, MetaException {
    boolean commit = false;
    openTransaction();
    try {
      createTable(tbl);
      if (primaryKeys != null) addPrimaryKeys(primaryKeys);
      if (foreignKeys != null) addForeignKeys(foreignKeys);
      commit = true;
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public void dropConstraint(String dbName, String tableName, String constraintName)
      throws NoSuchObjectException {
    // This is something of pain, since we have to search both primary key and foreign key to see
    // which they want to drop.
    boolean commit = false;
    openTransaction();
    try {
      List<SQLPrimaryKey> pk = getHBase().getPrimaryKey(dbName, tableName);
      if (pk != null && pk.size() > 0 && pk.get(0).getPk_name().equals(constraintName)) {
        getHBase().deletePrimaryKey(dbName, tableName);
        commit = true;
        return;
      }

      List<SQLForeignKey> fks = getHBase().getForeignKeys(dbName, tableName);
      if (fks != null && fks.size() > 0) {
        List<SQLForeignKey> newKeyList = new ArrayList<>(fks.size());
        // Make a new list of keys that excludes all columns from the constraint we're dropping.
        for (SQLForeignKey fkcol : fks) {
          if (!fkcol.getFk_name().equals(constraintName)) newKeyList.add(fkcol);
        }
        // If we've dropped only one foreign key out of many keys, than update so that we still
        // have the existing keys.  Otherwise drop the foreign keys all together.
        if (newKeyList.size() > 0) getHBase().putForeignKeys(newKeyList);
        else getHBase().deleteForeignKeys(dbName, tableName);
        commit = true;
        return;
      }

      commit = true;
      throw new NoSuchObjectException("Unable to find constraint named " + constraintName +
        " on table " + tableNameForErrorMsg(dbName, tableName));
    } catch (IOException e) {
      LOG.error("Error fetching primary key for table " + tableNameForErrorMsg(dbName, tableName), e);
      throw new NoSuchObjectException("Error fetching primary key for table " +
          tableNameForErrorMsg(dbName, tableName) + " : " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public void addPrimaryKeys(List<SQLPrimaryKey> pks) throws InvalidObjectException, MetaException {
    boolean commit = false;
    openTransaction();
    try {
      List<SQLPrimaryKey> currentPk =
          getHBase().getPrimaryKey(pks.get(0).getTable_db(), pks.get(0).getTable_name());
      if (currentPk != null) {
        throw new MetaException(" Primary key already exists for: " +
            tableNameForErrorMsg(pks.get(0).getTable_db(), pks.get(0).getTable_name()));
      }
      getHBase().putPrimaryKey(pks);
      commit = true;
    } catch (IOException e) {
      LOG.error("Error writing primary key", e);
      throw new MetaException("Error writing primary key: " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }

  @Override
  public void addForeignKeys(List<SQLForeignKey> fks) throws InvalidObjectException, MetaException {
    boolean commit = false;
    openTransaction();
    try {
      // Fetch the existing keys (if any) and add in these new ones
      List<SQLForeignKey> existing =
          getHBase().getForeignKeys(fks.get(0).getFktable_db(), fks.get(0).getFktable_name());
      if (existing == null) existing = new ArrayList<>(fks.size());
      existing.addAll(fks);
      getHBase().putForeignKeys(existing);
      commit = true;
    } catch (IOException e) {
      LOG.error("Error writing foreign keys", e);
      throw new MetaException("Error writing foreign keys: " + e.getMessage());
    } finally {
      commitOrRoleBack(commit);
    }
  }
}
