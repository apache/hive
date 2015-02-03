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

package org.apache.hadoop.hive.ql.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.TableStatsRequest;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.thrift.TException;

public class SessionHiveMetaStoreClient extends HiveMetaStoreClient implements IMetaStoreClient {

  SessionHiveMetaStoreClient(HiveConf conf) throws MetaException {
    super(conf);
  }

  SessionHiveMetaStoreClient(HiveConf conf, HiveMetaHookLoader hookLoader) throws MetaException {
    super(conf, hookLoader);
  }

  private Warehouse wh = null;

  private Warehouse getWh() throws MetaException {
    if (wh == null) {
      wh = new Warehouse(conf);
    }
    return wh;
  }

  @Override
  protected void create_table_with_environment_context(
      org.apache.hadoop.hive.metastore.api.Table tbl, EnvironmentContext envContext)
      throws AlreadyExistsException, InvalidObjectException,
      MetaException, NoSuchObjectException, TException {

    if (tbl.isTemporary()) {
      createTempTable(tbl, envContext);
      return;
    }
    // non-temp tables should use underlying client.
    super.create_table_with_environment_context(tbl, envContext);
  }

  @Override
  protected void drop_table_with_environment_context(String dbname, String name,
      boolean deleteData, EnvironmentContext envContext) throws MetaException, TException,
      NoSuchObjectException, UnsupportedOperationException {
    // First try temp table
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbname, name);
    if (table != null) {
      try {
        deleteTempTableColumnStatsForTable(dbname, name);
      } catch (NoSuchObjectException err){
        // No stats to delete, forgivable error.
        LOG.info(err);
      }
      dropTempTable(table, deleteData, envContext);
      return;
    }

    // Try underlying client
    super.drop_table_with_environment_context(dbname,  name, deleteData, envContext);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Table getTable(String dbname, String name) throws MetaException,
  TException, NoSuchObjectException {
    // First check temp tables
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbname, name);
    if (table != null) {
      return deepCopy(table);  // Original method used deepCopy(), do the same here.
    }

    // Try underlying client
    return super.getTable(dbname, name);
  }

  @Override
  public List<String> getAllTables(String dbName) throws MetaException {
    List<String> tableNames = super.getAllTables(dbName);

    // May need to merge with list of temp tables
    Map<String, Table> tables = getTempTablesForDatabase(dbName);
    if (tables == null || tables.size() == 0) {
      return tableNames;
    }

    // Get list of temp table names
    Set<String> tempTableNames = tables.keySet();

    // Merge and sort result
    Set<String> allTableNames = new HashSet<String>(tableNames.size() + tempTableNames.size());
    allTableNames.addAll(tableNames);
    allTableNames.addAll(tempTableNames);
    tableNames = new ArrayList<String>(allTableNames);
    Collections.sort(tableNames);
    return tableNames;
  }

  @Override
  public List<String> getTables(String dbName, String tablePattern) throws MetaException {
    List<String> tableNames = super.getTables(dbName, tablePattern);

    // May need to merge with list of temp tables
    dbName = dbName.toLowerCase();
    tablePattern = tablePattern.toLowerCase();
    Map<String, Table> tables = getTempTablesForDatabase(dbName);
    if (tables == null || tables.size() == 0) {
      return tableNames;
    }
    tablePattern = tablePattern.replaceAll("\\*", ".*");
    Pattern pattern = Pattern.compile(tablePattern);
    Matcher matcher = pattern.matcher("");
    Set<String> combinedTableNames = new HashSet<String>();
    for (String tableName : tables.keySet()) {
      if (matcher == null) {
        matcher = pattern.matcher(tableName);
      } else {
        matcher.reset(tableName);
      }
      if (matcher.matches()) {
        combinedTableNames.add(tableName);
      }
    }

    // Combine/sort temp and normal table results
    combinedTableNames.addAll(tableNames);
    tableNames = new ArrayList<String>(combinedTableNames);
    Collections.sort(tableNames);
    return tableNames;
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Table> getTableObjectsByName(String dbName,
      List<String> tableNames)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {

    dbName = dbName.toLowerCase();
    if (SessionState.get().getTempTables().size() == 0) {
      // No temp tables, just call underlying client
      return super.getTableObjectsByName(dbName, tableNames);
    }

    List<org.apache.hadoop.hive.metastore.api.Table> tables =
        new ArrayList<org.apache.hadoop.hive.metastore.api.Table>();
    for (String tableName : tableNames) {
      try {
        org.apache.hadoop.hive.metastore.api.Table table = getTable(dbName, tableName);
        if (table != null) {
          tables.add(table);
        }
      } catch (NoSuchObjectException err) {
        // Ignore error, just return the valid tables that are found.
      }
    }
    return tables;
  }

  @Override
  public boolean tableExists(String databaseName, String tableName) throws MetaException,
  TException, UnknownDBException {
    // First check temp tables
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(databaseName, tableName);
    if (table != null) {
      return true;
    }

    // Try underlying client
    return super.tableExists(databaseName, tableName);
  }

  @Override
  public List<FieldSchema> getSchema(String dbName, String tableName)
      throws MetaException, TException, UnknownTableException,
      UnknownDBException {
    // First check temp tables
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tableName);
    if (table != null) {
      return deepCopyFieldSchemas(table.getSd().getCols());
    }

    // Try underlying client
    return super.getSchema(dbName, tableName);
  }

  @Override
  public void alter_table(String dbname, String tbl_name, org.apache.hadoop.hive.metastore.api.Table new_tbl,
      boolean cascade) throws InvalidOperationException, MetaException, TException {
    org.apache.hadoop.hive.metastore.api.Table old_tbl = getTempTable(dbname, tbl_name);
    if (old_tbl != null) {
      //actually temp table does not support partitions, cascade is not applicable here
      alterTempTable(dbname, tbl_name, old_tbl, new_tbl, null);
      return;
    }
    super.alter_table(dbname, tbl_name, new_tbl, cascade);
  }

  @Override
  public void alter_table(String dbname, String tbl_name, org.apache.hadoop.hive.metastore.api.Table new_tbl,
      EnvironmentContext envContext) throws InvalidOperationException, MetaException, TException {
    // First try temp table
    org.apache.hadoop.hive.metastore.api.Table old_tbl = getTempTable(dbname, tbl_name);
    if (old_tbl != null) {
      alterTempTable(dbname, tbl_name, old_tbl, new_tbl, envContext);
      return;
    }

    // Try underlying client
    super.alter_table(dbname,  tbl_name,  new_tbl, envContext);
  }

  @Override
  public PrincipalPrivilegeSet get_privilege_set(HiveObjectRef hiveObject,
      String userName, List<String> groupNames) throws MetaException,
      TException {
    // If caller is looking for temp table, handle here. Otherwise pass on to underlying client.
    if (hiveObject.getObjectType() == HiveObjectType.TABLE) {
      org.apache.hadoop.hive.metastore.api.Table table =
          getTempTable(hiveObject.getDbName(), hiveObject.getObjectName());
      if (table != null) {
        return deepCopy(table.getPrivileges());
      }
    }

    return super.get_privilege_set(hiveObject, userName, groupNames);
  }

  /** {@inheritDoc} */
  @Override
  public boolean updateTableColumnStatistics(ColumnStatistics statsObj)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
      InvalidInputException {
    String dbName = statsObj.getStatsDesc().getDbName().toLowerCase();
    String tableName = statsObj.getStatsDesc().getTableName().toLowerCase();
    if (getTempTable(dbName, tableName) != null) {
      return updateTempTableColumnStats(dbName, tableName, statsObj);
    }
    return super.updateTableColumnStatistics(statsObj);
  }

  /** {@inheritDoc} */
  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames) throws NoSuchObjectException, MetaException, TException,
      InvalidInputException, InvalidObjectException {
    if (getTempTable(dbName, tableName) != null) {
      return getTempTableColumnStats(dbName, tableName, colNames);
    }
    return super.getTableColumnStatistics(dbName, tableName, colNames);
  }

  /** {@inheritDoc} */
  @Override
  public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
      InvalidInputException {
    if (getTempTable(dbName, tableName) != null) {
      return deleteTempTableColumnStats(dbName, tableName, colName);
    }
    return super.deleteTableColumnStatistics(dbName, tableName, colName);
  }

  private void createTempTable(org.apache.hadoop.hive.metastore.api.Table tbl,
      EnvironmentContext envContext) throws AlreadyExistsException, InvalidObjectException,
      MetaException, NoSuchObjectException, TException {

    SessionState ss = SessionState.get();
    if (ss == null) {
      throw new MetaException("No current SessionState, cannot create temporary table"
          + tbl.getDbName() + "." + tbl.getTableName());
    }

    // We may not own the table object, create a copy
    tbl = deepCopyAndLowerCaseTable(tbl);

    String dbName = tbl.getDbName();
    String tblName = tbl.getTableName();
    Map<String, Table> tables = getTempTablesForDatabase(dbName);
    if (tables != null && tables.containsKey(tblName)) {
      throw new MetaException("Temporary table " + dbName + "." + tblName + " already exists");
    }

    // Create temp table directory
    Warehouse wh = getWh();
    Path tblPath = wh.getDnsPath(new Path(tbl.getSd().getLocation()));
    if (tblPath == null) {
      throw new MetaException("Temp table path not set for " + tbl.getTableName());
    } else {
      if (!wh.isDir(tblPath)) {
        if (!wh.mkdirs(tblPath, true)) {
          throw new MetaException(tblPath
              + " is not a directory or unable to create one");
        }
      }
      // Make sure location string is in proper format
      tbl.getSd().setLocation(tblPath.toString());
    }

    // Add temp table info to current session
    Table tTable = new Table(tbl);
    if (tables == null) {
      tables = new HashMap<String, Table>();
      ss.getTempTables().put(dbName, tables);
    }
    tables.put(tblName, tTable);
  }

  private org.apache.hadoop.hive.metastore.api.Table getTempTable(String dbName, String tableName) {
    Map<String, Table> tables = getTempTablesForDatabase(dbName.toLowerCase());
    if (tables != null) {
      Table table = tables.get(tableName.toLowerCase());
      if (table != null) {
        return table.getTTable();
      }
    }
    return null;
  }

  private void alterTempTable(String dbname, String tbl_name,
      org.apache.hadoop.hive.metastore.api.Table oldt,
      org.apache.hadoop.hive.metastore.api.Table newt,
      EnvironmentContext envContext) throws InvalidOperationException, MetaException, TException {
    dbname = dbname.toLowerCase();
    tbl_name = tbl_name.toLowerCase();
    boolean shouldDeleteColStats = false;

    // Disallow changing temp table location
    if (!newt.getSd().getLocation().equals(oldt.getSd().getLocation())) {
      throw new MetaException("Temp table location cannot be changed");
    }

    org.apache.hadoop.hive.metastore.api.Table newtCopy = deepCopyAndLowerCaseTable(newt);
    MetaStoreUtils.updateUnpartitionedTableStatsFast(newtCopy,
        getWh().getFileStatusesForSD(newtCopy.getSd()), false, true);
    Table newTable = new Table(newtCopy);
    String newDbName = newTable.getDbName();
    String newTableName = newTable.getTableName();
    if (!newDbName.equals(oldt.getDbName()) || !newTableName.equals(oldt.getTableName())) {
      // Table was renamed.

      // Do not allow temp table rename if the new name already exists as a temp table
      if (getTempTable(newDbName, newTableName) != null) {
        throw new MetaException("Cannot rename temporary table to " + newTableName
            + " - temporary table already exists with the same name");
      }

      // Remove old temp table entry, and add new entry to list of temp tables.
      // Note that for temp tables there is no need to rename directories
      Map<String, Table> tables = getTempTablesForDatabase(dbname);
      if (tables == null || tables.remove(tbl_name) == null) {
        throw new MetaException("Could not find temp table entry for " + dbname + "." + tbl_name);
      }
      shouldDeleteColStats = true;

      tables = getTempTablesForDatabase(newDbName);
      if (tables == null) {
        tables = new HashMap<String, Table>();
        SessionState.get().getTempTables().put(newDbName, tables);
      }
      tables.put(newTableName, newTable);
    } else {
      if (haveTableColumnsChanged(oldt, newt)) {
        shouldDeleteColStats = true;
      }
      getTempTablesForDatabase(dbname).put(tbl_name, newTable);
    }

    if (shouldDeleteColStats) {
      try {
        deleteTempTableColumnStatsForTable(dbname, tbl_name);
      } catch (NoSuchObjectException err){
        // No stats to delete, forgivable error.
        LOG.info(err);
      }
    }
  }

  private static boolean haveTableColumnsChanged(org.apache.hadoop.hive.metastore.api.Table oldt,
      org.apache.hadoop.hive.metastore.api.Table newt) {
    List<FieldSchema> oldCols = oldt.getSd().getCols();
    List<FieldSchema> newCols = newt.getSd().getCols();
    if (oldCols.size() != newCols.size()) {
      return true;
    }
    Iterator<FieldSchema> oldColsIter = oldCols.iterator();
    Iterator<FieldSchema> newColsIter = newCols.iterator();
    while (oldColsIter.hasNext()) {
      // Don't use FieldSchema.equals() since it also compares comments,
      // which is unnecessary for this method.
      if (!fieldSchemaEqualsIgnoreComment(oldColsIter.next(), newColsIter.next())) {
        return true;
      }
    }
    return false;
  }

  private static boolean fieldSchemaEqualsIgnoreComment(FieldSchema left, FieldSchema right) {
    // Just check name/type for equality, don't compare comment
    if (!left.getName().equals(right.getName())) {
      return true;
    }
    if (!left.getType().equals(right.getType())) {
      return true;
    }
    return false;
  }

  private void dropTempTable(org.apache.hadoop.hive.metastore.api.Table table, boolean deleteData,
      EnvironmentContext envContext) throws MetaException, TException,
      NoSuchObjectException, UnsupportedOperationException {

    String dbName = table.getDbName().toLowerCase();
    String tableName = table.getTableName().toLowerCase();

    // Determine the temp table path
    Path tablePath = null;
    String pathStr = table.getSd().getLocation();
    if (pathStr != null) {
      try {
        tablePath = new Path(table.getSd().getLocation());
        if (!getWh().isWritable(tablePath.getParent())) {
          throw new MetaException("Table metadata not deleted since " + tablePath.getParent() +
              " is not writable by " + conf.getUser());
        }
      } catch (IOException err) {
        MetaException metaException =
            new MetaException("Error checking temp table path for " + table.getTableName());
        metaException.initCause(err);
        throw metaException;
      }
    }

    // Remove table entry from SessionState
    Map<String, Table> tables = getTempTablesForDatabase(dbName);
    if (tables == null || tables.remove(tableName) == null) {
      throw new MetaException("Could not find temp table entry for " + dbName + "." + tableName);
    }

    // Delete table data
    if (deleteData && !MetaStoreUtils.isExternalTable(table)) {
      try {
        boolean ifPurge = false;
        if (envContext != null){
          ifPurge = Boolean.parseBoolean(envContext.getProperties().get("ifPurge"));
        }
        getWh().deleteDir(tablePath, true, ifPurge);
      } catch (Exception err) {
        LOG.error("Failed to delete temp table directory: " + tablePath, err);
        // Forgive error
      }
    }
  }

  private org.apache.hadoop.hive.metastore.api.Table deepCopyAndLowerCaseTable(
      org.apache.hadoop.hive.metastore.api.Table tbl) {
    org.apache.hadoop.hive.metastore.api.Table newCopy = deepCopy(tbl);
    newCopy.setDbName(newCopy.getDbName().toLowerCase());
    newCopy.setTableName(newCopy.getTableName().toLowerCase());
    return newCopy;
  }

  private Map<String, Table> getTempTablesForDatabase(String dbName) {
    SessionState ss = SessionState.get();
    if (ss == null) {
      LOG.debug("No current SessionState, skipping temp tables");
      return null;
    }
    return ss.getTempTables().get(dbName);
  }

  private Map<String, ColumnStatisticsObj> getTempTableColumnStatsForTable(String dbName,
      String tableName) {
    SessionState ss = SessionState.get();
    if (ss == null) {
      LOG.debug("No current SessionState, skipping temp tables");
      return null;
    }
    String lookupName = StatsUtils.getFullyQualifiedTableName(dbName.toLowerCase(),
        tableName.toLowerCase());
    return ss.getTempTableColStats().get(lookupName);
  }

  private static List<ColumnStatisticsObj> copyColumnStatisticsObjList(Map<String, ColumnStatisticsObj> csoMap) {
    List<ColumnStatisticsObj> retval = new ArrayList<ColumnStatisticsObj>(csoMap.size());
    for (ColumnStatisticsObj cso : csoMap.values()) {
      retval.add(new ColumnStatisticsObj(cso));
    }
    return retval;
  }

  private List<ColumnStatisticsObj> getTempTableColumnStats(String dbName, String tableName,
      List<String> colNames) {
    Map<String, ColumnStatisticsObj> tableColStats =
        getTempTableColumnStatsForTable(dbName, tableName);
    List<ColumnStatisticsObj> retval = new ArrayList<ColumnStatisticsObj>();

    if (tableColStats != null) {
      for (String colName : colNames) {
        colName = colName.toLowerCase();
        if (tableColStats.containsKey(colName)) {
          retval.add(new ColumnStatisticsObj(tableColStats.get(colName)));
        }
      }
    }
    return retval;
  }

  private boolean updateTempTableColumnStats(String dbName, String tableName,
      ColumnStatistics colStats) throws MetaException {
    SessionState ss = SessionState.get();
    if (ss == null) {
      throw new MetaException("No current SessionState, cannot update temporary table stats for "
          + dbName + "." + tableName);
    }
    Map<String, ColumnStatisticsObj> ssTableColStats =
        getTempTableColumnStatsForTable(dbName, tableName);
    if (ssTableColStats == null) {
      // Add new entry for this table
      ssTableColStats = new HashMap<String, ColumnStatisticsObj>();
      ss.getTempTableColStats().put(
          StatsUtils.getFullyQualifiedTableName(dbName, tableName),
          ssTableColStats);
    }
    mergeColumnStats(ssTableColStats, colStats);
    return true;
  }

  private static void mergeColumnStats(Map<String, ColumnStatisticsObj> oldStats,
      ColumnStatistics newStats) {
    List<ColumnStatisticsObj> newColList = newStats.getStatsObj();
    if (newColList != null) {
      for (ColumnStatisticsObj colStat : newColList) {
        // This is admittedly a bit simple, StatsObjectConverter seems to allow
        // old stats attributes to be kept if the new values do not overwrite them.
        oldStats.put(colStat.getColName().toLowerCase(), colStat);
      }
    }
  }

  private boolean deleteTempTableColumnStatsForTable(String dbName, String tableName)
      throws NoSuchObjectException {
    Map<String, ColumnStatisticsObj> deletedEntry =
        getTempTableColumnStatsForTable(dbName, tableName);
    if (deletedEntry != null) {
      SessionState.get().getTempTableColStats().remove(
          StatsUtils.getFullyQualifiedTableName(dbName, tableName));
    } else {
      throw new NoSuchObjectException("Column stats doesn't exist for db=" + dbName +
          " temp table=" + tableName);
    }
    return true;
  }

  private boolean deleteTempTableColumnStats(String dbName, String tableName, String columnName)
      throws NoSuchObjectException {
    ColumnStatisticsObj deletedEntry = null;
    Map<String, ColumnStatisticsObj> ssTableColStats =
        getTempTableColumnStatsForTable(dbName, tableName);
    if (ssTableColStats != null) {
      deletedEntry = ssTableColStats.remove(columnName.toLowerCase());
    }
    if (deletedEntry == null) {
      throw new NoSuchObjectException("Column stats doesn't exist for db=" + dbName +
          " temp table=" + tableName);
    }
    return true;
  }
}
