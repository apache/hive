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

package org.apache.hadoop.hive.ql.metadata;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.tuple.Pair;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.io.HdfsUtils;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.PartFilterExprUtil;
import org.apache.hadoop.hive.metastore.PartitionDropOptions;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PartitionListComposingSpec;
import org.apache.hadoop.hive.metastore.api.PartitionSpec;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRequest;
import org.apache.hadoop.hive.metastore.api.PartitionValuesResponse;
import org.apache.hadoop.hive.metastore.api.PartitionValuesRow;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.SetPartitionsStatsRequest;
import org.apache.hadoop.hive.metastore.api.TableMeta;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.UnknownTableException;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.parser.ExpressionTree;
import org.apache.hadoop.hive.metastore.partition.spec.PartitionSpecProxy;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.Warehouse.getCatalogQualifiedTableName;
import static org.apache.hadoop.hive.metastore.Warehouse.makePartName;
import static org.apache.hadoop.hive.metastore.Warehouse.makeSpecFromName;
import static org.apache.hadoop.hive.metastore.Warehouse.makeValsFromName;
import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.compareFieldColumns;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getColumnNamesForTable;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getPvals;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.isExternalTable;

/**
 * todo: This need review re: thread safety.  Various places (see callsers of
 * {@link SessionState#setCurrentSessionState(SessionState)}) pass SessionState to forked threads.
 * Currently it looks like those threads only read metadata but this is fragile.
 * Also, maps (in SessionState) where tempt table metadata is stored are concurrent and so
 * any put/get crosses a memory barrier and so does using most {@code java.util.concurrent.*}
 * so the readers of the objects in these maps should have the most recent view of the object.
 * But again, could be fragile.
 */
public class SessionHiveMetaStoreClient extends HiveMetaStoreClient implements IMetaStoreClient {
  private static final Logger LOG = LoggerFactory.getLogger(SessionHiveMetaStoreClient.class);

  SessionHiveMetaStoreClient(Configuration conf, Boolean allowEmbedded) throws MetaException {
    super(conf, null, allowEmbedded);
  }

  SessionHiveMetaStoreClient(
      Configuration conf, HiveMetaHookLoader hookLoader, Boolean allowEmbedded) throws MetaException {
    super(conf, hookLoader, allowEmbedded);
  }

  private Warehouse wh = null;

  private Warehouse getWh() throws MetaException {
    if (wh == null) {
      wh = new Warehouse(conf);
    }
    return wh;
  }

  // TODO CAT - a number of these need to be updated.  Don't bother with deprecated methods as
  // this is just an internal class.  Wait until we're ready to move all the catalog stuff up
  // into ql.

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
  protected void drop_table_with_environment_context(String catName, String dbname, String name,
      boolean deleteData, EnvironmentContext envContext) throws MetaException, TException,
      NoSuchObjectException, UnsupportedOperationException {
    // First try temp table
    // TODO CAT - I think the right thing here is to always put temp tables in the current
    // catalog.  But we don't yet have a notion of current catalog, so we'll have to hold on
    // until we do.
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbname, name);
    if (table != null) {
      try {
        deleteTempTableColumnStatsForTable(dbname, name);
      } catch (NoSuchObjectException err){
        // No stats to delete, forgivable error.
        LOG.info(err.getMessage());
      }
      dropTempTable(table, deleteData, envContext);
      return;
    }

    // Try underlying client
    super.drop_table_with_environment_context(catName, dbname,  name, deleteData, envContext);
  }

  @Override
  public void truncateTable(String dbName, String tableName, List<String> partNames) throws MetaException, TException {
    // First try temp table
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tableName);
    if (table != null) {
      truncateTempTable(table);
      return;
    }
    // Try underlying client
    super.truncateTable(dbName, tableName, partNames);
  }

  @Override
  public void truncateTable(String dbName, String tableName,
      List<String> partNames, String validWriteIds, long writeId)
      throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tableName);
    if (table != null) {
      truncateTempTable(table);
      return;
    }
    super.truncateTable(dbName, tableName, partNames, validWriteIds, writeId);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Table getTable(String dbname, String name) throws MetaException,
  TException, NoSuchObjectException {
    return getTable(dbname, name, false, null);
  }

  @Override
  public org.apache.hadoop.hive.metastore.api.Table getTable(String dbname, String name,
  boolean getColStats, String engine) throws MetaException,
  TException, NoSuchObjectException {
    // First check temp tables
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbname, name);
    if (table != null) {
      return deepCopy(table);  // Original method used deepCopy(), do the same here.
    }
    // Try underlying client
    return super.getTable(getDefaultCatalog(conf), dbname, name, getColStats, engine);
  }

  // Need to override this one too or dropTable breaks because it doesn't find the table when checks
  // before the drop.
  @Override
  public org.apache.hadoop.hive.metastore.api.Table getTable(String catName, String dbName,
                                                             String tableName) throws TException {
    return getTable(catName, dbName, tableName, false, null);
  }

  // Need to override this one too or dropTable breaks because it doesn't find the table when checks
  // before the drop.
  @Override
  public org.apache.hadoop.hive.metastore.api.Table getTable(String catName, String dbName,
          String tableName, boolean getColStats, String engine)
          throws TException {
    if (!DEFAULT_CATALOG_NAME.equals(catName)) {
      return super.getTable(catName, dbName, tableName, getColStats, engine);
    } else {
      return getTable(dbName, tableName, getColStats, engine);
    }
  }

  @Override
  public List<String> getAllTables(String dbName) throws MetaException {
    List<String> tableNames = super.getAllTables(dbName);

    // May need to merge with list of temp tables
    Map<String, Table> tables = getTempTablesForDatabase(dbName, "?");
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
    Map<String, Table> tables = getTempTablesForDatabase(dbName, tablePattern);
    if (tables == null || tables.size() == 0) {
      return tableNames;
    }
    tablePattern = tablePattern.replaceAll("\\*", ".*");
    Pattern pattern = Pattern.compile(tablePattern);
    Matcher matcher = pattern.matcher("");
    Set<String> combinedTableNames = new HashSet<String>();
    for (String tableName : tables.keySet()) {
      matcher.reset(tableName);
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
  public List<String> getTables(String dbname, String tablePattern, TableType tableType) throws MetaException {
    List<String> tableNames = super.getTables(dbname, tablePattern, tableType);

    if (tableType == TableType.MANAGED_TABLE || tableType == TableType.EXTERNAL_TABLE) {
      // May need to merge with list of temp tables
      dbname = dbname.toLowerCase();
      tablePattern = tablePattern.toLowerCase();
      Map<String, Table> tables = getTempTablesForDatabase(dbname, tablePattern);
      if (tables == null || tables.size() == 0) {
        return tableNames;
      }
      tablePattern = tablePattern.replaceAll("\\*", ".*");
      Pattern pattern = Pattern.compile(tablePattern);
      Matcher matcher = pattern.matcher("");
      Set<String> combinedTableNames = new HashSet<String>();
      combinedTableNames.addAll(tableNames);
      for (Entry<String, Table> tableData : tables.entrySet()) {
        matcher.reset(tableData.getKey());
        if (matcher.matches()) {
          if (tableData.getValue().getTableType() == tableType) {
            // If tableType is the same that we are requesting,
            // add table the the list
            combinedTableNames.add(tableData.getKey());
          } else {
            // If tableType is not the same that we are requesting,
            // remove it in case it was added before, as temp table
            // overrides original table
            combinedTableNames.remove(tableData.getKey());
          }
        }
      }
      // Combine/sort temp and normal table results
      tableNames = new ArrayList<>(combinedTableNames);
      Collections.sort(tableNames);
    }

    return tableNames;
  }

  @Override
  public List<TableMeta> getTableMeta(String dbPatterns, String tablePatterns, List<String> tableTypes)
      throws MetaException {
    List<TableMeta> tableMetas = super.getTableMeta(dbPatterns, tablePatterns, tableTypes);
    Map<String, Map<String, Table>> tmpTables = getTempTables("dbPatterns='" + dbPatterns +
        "' tablePatterns='" + tablePatterns + "'");
    if (tmpTables.isEmpty()) {
      return tableMetas;
    }

    List<Matcher> dbPatternList = new ArrayList<>();
    for (String element : dbPatterns.split("\\|")) {
      dbPatternList.add(Pattern.compile(element.replaceAll("\\*", ".*")).matcher(""));
    }
    List<Matcher> tblPatternList = new ArrayList<>();
    for (String element : tablePatterns.split("\\|")) {
      tblPatternList.add(Pattern.compile(element.replaceAll("\\*", ".*")).matcher(""));
    }
    for (Map.Entry<String, Map<String, Table>> outer : tmpTables.entrySet()) {
      if (!matchesAny(outer.getKey(), dbPatternList)) {
        continue;
      }
      for (Map.Entry<String, Table> inner : outer.getValue().entrySet()) {
        Table table = inner.getValue();
        String tableName = table.getTableName();
        String typeString = table.getTableType().name();
        if (tableTypes != null && !tableTypes.contains(typeString)) {
          continue;
        }
        if (!matchesAny(inner.getKey(), tblPatternList)) {
          continue;
        }
        TableMeta tableMeta = new TableMeta(table.getDbName(), tableName, typeString);
        tableMeta.setComments(table.getProperty("comment"));
        tableMetas.add(tableMeta);
      }
    }
    return tableMetas;
  }

  private boolean matchesAny(String string, List<Matcher> matchers) {
    for (Matcher matcher : matchers) {
      if (matcher.reset(string).matches()) {
        return true;
      }
    }
    return matchers.isEmpty();
  }

  @Override
  public List<org.apache.hadoop.hive.metastore.api.Table> getTableObjectsByName(String dbName,
      List<String> tableNames)
      throws MetaException, InvalidOperationException, UnknownDBException, TException {

    dbName = dbName.toLowerCase();
    if (SessionState.get() == null || SessionState.get().getTempTables().size() == 0) {
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

  @Deprecated
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
  public void alter_table(String catName, String dbName, String tbl_name,
      org.apache.hadoop.hive.metastore.api.Table new_tbl,
      EnvironmentContext envContext, String validWriteIds)
      throws InvalidOperationException, MetaException, TException {
    org.apache.hadoop.hive.metastore.api.Table old_tbl = getTempTable(dbName, tbl_name);
    if (old_tbl != null) {
      //actually temp table does not support partitions, cascade is not applicable here
      alterTempTable(dbName, tbl_name, old_tbl, new_tbl, null);
      return;
    }
    super.alter_table(catName, dbName, tbl_name, new_tbl, envContext, validWriteIds);
  }

  @Override
  public void alter_table(String dbname, String tbl_name,
      org.apache.hadoop.hive.metastore.api.Table new_tbl) throws InvalidOperationException,
      MetaException, TException {
    org.apache.hadoop.hive.metastore.api.Table old_tbl = getTempTable(dbname, tbl_name);
    if (old_tbl != null) {
      // actually temp table does not support partitions, cascade is not
      // applicable here
      alterTempTable(dbname, tbl_name, old_tbl, new_tbl, null);
      return;
    }
    super.alter_table(dbname, tbl_name, new_tbl);
  }

  @Override
  public void alter_table_with_environmentContext(String dbname, String tbl_name,
      org.apache.hadoop.hive.metastore.api.Table new_tbl, EnvironmentContext envContext)
      throws InvalidOperationException, MetaException, TException {
    // First try temp table
    org.apache.hadoop.hive.metastore.api.Table old_tbl = getTempTable(dbname, tbl_name);
    if (old_tbl != null) {
      alterTempTable(dbname, tbl_name, old_tbl, new_tbl, envContext);
      return;
    }

    // Try underlying client
    super.alter_table_with_environmentContext(dbname, tbl_name, new_tbl, envContext);
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
  public boolean setPartitionColumnStatistics(SetPartitionsStatsRequest request)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
      InvalidInputException {
    if (request.getColStatsSize() == 1) {
      ColumnStatistics colStats = request.getColStatsIterator().next();
      ColumnStatisticsDesc desc = colStats.getStatsDesc();
      String dbName = desc.getDbName().toLowerCase();
      String tableName = desc.getTableName().toLowerCase();
      if (getTempTable(dbName, tableName) != null) {
        return updateTempTableColumnStats(dbName, tableName, colStats);
      }
    }
    return super.setPartitionColumnStatistics(request);
  }

  /** {@inheritDoc} */
  @Override
  public List<ColumnStatisticsObj> getTableColumnStatistics(String dbName, String tableName,
      List<String> colNames, String engine) throws NoSuchObjectException, MetaException, TException,
      InvalidInputException, InvalidObjectException {
    if (getTempTable(dbName, tableName) != null) {
      return getTempTableColumnStats(dbName, tableName, colNames);
    }
    return super.getTableColumnStatistics(dbName, tableName, colNames, engine);
  }

  /** {@inheritDoc} */
  @Override
  public boolean deleteTableColumnStatistics(String dbName, String tableName, String colName, String engine)
      throws NoSuchObjectException, InvalidObjectException, MetaException, TException,
      InvalidInputException {
    if (getTempTable(dbName, tableName) != null) {
      return deleteTempTableColumnStats(dbName, tableName, colName);
    }
    return super.deleteTableColumnStatistics(dbName, tableName, colName, engine);
  }

  private void createTempTable(org.apache.hadoop.hive.metastore.api.Table tbl,
      EnvironmentContext envContext) throws AlreadyExistsException, InvalidObjectException,
      MetaException, NoSuchObjectException, TException {

    boolean isVirtualTable = tbl.getTableName().startsWith(SemanticAnalyzer.VALUES_TMP_TABLE_NAME_PREFIX);

    SessionState ss = SessionState.get();
    if (ss == null) {
      throw new MetaException("No current SessionState, cannot create temporary table: "
          + Warehouse.getQualifiedName(tbl));
    }

    // We may not own the table object, create a copy
    tbl = deepCopyAndLowerCaseTable(tbl);

    String dbName = tbl.getDbName();
    String tblName = tbl.getTableName();
    Map<String, Table> tables = getTempTablesForDatabase(dbName, tblName);
    if (tables != null && tables.containsKey(tblName)) {
      throw new MetaException(
          "Temporary table " + StatsUtils.getFullyQualifiedTableName(dbName, tblName) + " already exists");
    }

    // Create temp table directory
    Warehouse wh = getWh();
    if (tbl.getSd().getLocation() == null) {
      // Temp tables that do not go through SemanticAnalyzer may not have location set - do it here.
      // For example export of acid tables generates a query plan that creates a temp table.
      tbl.getSd().setLocation(SessionState.generateTempTableLocation(conf));
    }
    Path tblPath = wh.getDnsPath(new Path(tbl.getSd().getLocation()));
    if (tblPath == null) {
      throw new MetaException("Temp table path not set for " + tbl.getTableName());
    } else {
      if (!wh.isDir(tblPath)) {
        if (!wh.mkdirs(tblPath)) {
          throw new MetaException(tblPath
              + " is not a directory or unable to create one");
        }
      }
      // Make sure location string is in proper format
      tbl.getSd().setLocation(tblPath.toString());
    }

    // Add temp table info to current session
    Table tTable = new Table(tbl);
    if (!isVirtualTable) {
      StatsSetupConst.setStatsStateForCreateTable(tbl.getParameters(), getColumnNamesForTable(tbl),
          StatsSetupConst.TRUE);
    }
    if (tables == null) {
      tables = new HashMap<String, Table>();
      ss.getTempTables().put(dbName, tables);
    }
    tables.put(tblName, tTable);
    createPartitionedTempTable(tbl);
  }

  private org.apache.hadoop.hive.metastore.api.Table getTempTable(String dbName, String tableName)
      throws MetaException {
    if (dbName == null) {
      throw new MetaException("Db name cannot be null");
    }
    if (tableName == null) {
      throw new MetaException("Table name cannot be null");
    }
    Map<String, Table> tables = getTempTablesForDatabase(dbName.toLowerCase(),
        tableName.toLowerCase());
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
      Map<String, Table> tables = getTempTablesForDatabase(dbname, tbl_name);
      if (tables == null || tables.remove(tbl_name) == null) {
        throw new MetaException("Could not find temp table entry for " + dbname + "." + tbl_name);
      }
      shouldDeleteColStats = true;

      tables = getTempTablesForDatabase(newDbName, tbl_name);
      if (tables == null) {
        tables = new HashMap<String, Table>();
        SessionState.get().getTempTables().put(newDbName, tables);
      }
      tables.put(newTableName, newTable);
    } else {
      if (haveTableColumnsChanged(oldt, newt)) {
        shouldDeleteColStats = true;
      }
      getTempTablesForDatabase(dbname, tbl_name).put(tbl_name, newTable);
    }

    if (shouldDeleteColStats) {
      try {
        deleteTempTableColumnStatsForTable(dbname, tbl_name);
      } catch (NoSuchObjectException err){
        // No stats to delete, forgivable error.
        LOG.info(err.getMessage());
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

  private boolean needToUpdateStats(Map<String,String> props, EnvironmentContext environmentContext) {
    if (null == props) {
      return false;
    }
    boolean statsPresent = false;
    for (String stat : StatsSetupConst.SUPPORTED_STATS) {
      String statVal = props.get(stat);
      if (statVal != null && Long.parseLong(statVal) > 0) {
        statsPresent = true;
        //In the case of truncate table, we set the stats to be 0.
        props.put(stat, "0");
      }
    }
    //first set basic stats to true
    StatsSetupConst.setBasicStatsState(props, StatsSetupConst.TRUE);
    environmentContext.putToProperties(StatsSetupConst.STATS_GENERATED, StatsSetupConst.TASK);
    //then invalidate column stats
    StatsSetupConst.clearColumnStatsState(props);
    return statsPresent;
  }

  private void truncateTempTable(org.apache.hadoop.hive.metastore.api.Table table) throws MetaException, TException {

    boolean isAutopurge = "true".equalsIgnoreCase(table.getParameters().get("auto.purge"));
    try {
      // this is not transactional
      Path location = new Path(table.getSd().getLocation());

      FileSystem fs = location.getFileSystem(conf);
      HadoopShims.HdfsEncryptionShim shim
              = ShimLoader.getHadoopShims().createHdfsEncryptionShim(fs, conf);
      if (!shim.isPathEncrypted(location)) {
        HdfsUtils.HadoopFileStatus status = new HdfsUtils.HadoopFileStatus(conf, fs, location);
        FileStatus targetStatus = fs.getFileStatus(location);
        String targetGroup = targetStatus == null ? null : targetStatus.getGroup();
        FileUtils.moveToTrash(fs, location, conf, isAutopurge);
        fs.mkdirs(location);
        HdfsUtils.setFullFileStatus(conf, status, targetGroup, fs, location, false);
      } else {
        FileStatus[] statuses = fs.listStatus(location, FileUtils.HIDDEN_FILES_PATH_FILTER);
        if ((statuses != null) && (statuses.length > 0)) {
          boolean success = Hive.trashFiles(fs, statuses, conf, isAutopurge);
          if (!success) {
            throw new HiveException("Error in deleting the contents of " + location.toString());
          }
        }
      }

      EnvironmentContext environmentContext = new EnvironmentContext();
      if (needToUpdateStats(table.getParameters(), environmentContext)) {
        alter_table_with_environmentContext(table.getDbName(), table.getTableName(), table, environmentContext);
      }
    } catch (Exception e) {
      throw new MetaException(e.getMessage());
    }
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
              " is not writable by " + SecurityUtils.getUser());
        }
      } catch (IOException err) {
        MetaException metaException =
            new MetaException("Error checking temp table path for " + table.getTableName());
        metaException.initCause(err);
        throw metaException;
      }
    }

    // Remove table entry from SessionState
    Map<String, Table> tables = getTempTablesForDatabase(dbName, tableName);
    if (tables == null || tables.remove(tableName) == null) {
      throw new MetaException(
          "Could not find temp table entry for " + StatsUtils.getFullyQualifiedTableName(dbName, tableName));
    }
    removePartitionedTempTable(table);

    // Delete table data
    if (deleteData && !isExternalTable(table)) {
      try {
        boolean ifPurge = false;
        if (envContext != null){
          ifPurge = Boolean.parseBoolean(envContext.getProperties().get("ifPurge"));
        }
        getWh().deleteDir(tablePath, true, ifPurge, false);
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

  /**
   * @param dbName actual database name
   * @param tblName actual table name or search pattern (for error message)
   */
  public static Map<String, Table> getTempTablesForDatabase(String dbName,
      String tblName) {
    return getTempTables(Warehouse.getQualifiedName(dbName, tblName)).
        get(dbName);
  }

  private static Map<String, Map<String, Table>> getTempTables(String msg) {
    SessionState ss = SessionState.get();
    if (ss == null) {
      LOG.debug("No current SessionState, skipping temp tables for " + msg);
      return Collections.emptyMap();
    }
    return ss.getTempTables();
  }

  private Map<String, ColumnStatisticsObj> getTempTableColumnStatsForTable(String dbName,
      String tableName) {
    SessionState ss = SessionState.get();
    if (ss == null) {
      LOG.debug("No current SessionState, skipping temp tables for " +
          Warehouse.getQualifiedName(dbName, tableName));
      return null;
    }
    String lookupName = StatsUtils.getFullyQualifiedTableName(dbName.toLowerCase(),
        tableName.toLowerCase());
    return ss.getTempTableColStats().get(lookupName);
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
          + StatsUtils.getFullyQualifiedTableName(dbName, tableName));
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

    List<String> colNames = new ArrayList<>();
    for (ColumnStatisticsObj obj : colStats.getStatsObj()) {
      colNames.add(obj.getColName());
    }
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tableName);
    StatsSetupConst.setColumnStatsState(table.getParameters(), colNames);
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

  /**
   * Hive.loadPartition() calls this.
   * @param partition
   *          The partition to add
   * @return the partition added
   */
  @Override
  public org.apache.hadoop.hive.metastore.api.Partition add_partition(
      org.apache.hadoop.hive.metastore.api.Partition partition) throws TException {
    // First try temp table
    if (partition == null) {
      throw new MetaException("Partition cannot be null");
    }
    org.apache.hadoop.hive.metastore.api.Table table =
        getTempTable(partition.getDbName(), partition.getTableName());
    if (table == null) {
      //(assume) not a temp table - Try underlying client
      return super.add_partition(partition);
    }
    TempTable tt = getPartitionedTempTable(table);
    checkPartitionProperties(partition);
    Path partitionLocation = getPartitionLocation(table, partition, false);
    Partition result = tt.addPartition(deepCopy(partition));
    createAndSetLocationForAddedPartition(result, partitionLocation);
    return result;
  }

  /**
   * Loading Dynamic Partitions calls this. The partitions which are loaded, must belong to the
   * same table.
   * @param partitions the new partitions to be added, must be not null
   * @return number of partitions that were added
   * @throws TException
   */
  @Override
  public int add_partitions(List<Partition> partitions) throws TException {
    if (partitions == null || partitions.contains(null)) {
      throw new MetaException("Partitions cannot be null");
    }
    if (partitions.isEmpty()) {
      return 0;
    }

    List<Partition> addedPartitions = add_partitions(partitions, false, true);
    if (addedPartitions != null) {
      return addedPartitions.size();
    }

    return super.add_partitions(partitions);
  }

  @Override
  public int add_partitions_pspec(PartitionSpecProxy partitionSpec) throws TException {
    if (partitionSpec == null) {
      throw new MetaException("PartitionSpec cannot be null.");
    }
    if (partitionSpec.size() == 0) {
      return 0;
    }

    org.apache.hadoop.hive.metastore.api.Table table =
        getTempTable(partitionSpec.getDbName(), partitionSpec.getTableName());
    if (table == null) {
      return super.add_partitions_pspec(partitionSpec);
    }
    assertTempTablePartitioned(table);
    PartitionSpecProxy.PartitionIterator partitionIterator = partitionSpec.getPartitionIterator();
    List<Partition> partitionsToAdd = new ArrayList<>(partitionSpec.size());
    while (partitionIterator.hasNext()) {
      partitionsToAdd.add(partitionIterator.next());
    }

    List<Partition> addedPartitions = addPartitionsToTempTable(partitionsToAdd, partitionSpec.getDbName(),
        partitionSpec.getTableName(), false);
    if (addedPartitions != null) {
      return addedPartitions.size();
    }

    return super.add_partitions_pspec(partitionSpec);
  }

  @Override
  public List<Partition> add_partitions(List<Partition> partitions, boolean ifNotExists, boolean needResults)
      throws TException {
    if (partitions == null || partitions.contains(null)) {
      throw new MetaException("Partitions cannot be null");
    }

    if (partitions.isEmpty()) {
      return needResults ? new ArrayList<>() : null;
    }

    List<Partition> addedPartitions = addPartitionsToTempTable(partitions, null, null, ifNotExists);
    if (addedPartitions != null) {
      return needResults ? addedPartitions : null;
    }

    return super.add_partitions(partitions, ifNotExists, needResults);
  }


  @Override
  public Partition getPartition(String catName, String dbName, String tblName, String name) throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tblName);
    if (table == null) {
      return super.getPartition(catName, dbName, tblName, name);
    }
    TempTable tt = getPartitionedTempTable(table);
    Partition partition = tt.getPartition(name);
    if (partition == null) {
      throw new NoSuchObjectException("Partition with name " + name + " for table " + tblName + " in database " +
              dbName + " is not found.");
    }

    return deepCopy(partition);
  }

  @Override
  public Partition getPartition(String catName, String dbName, String tblName,
                                List<String> partVals) throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tblName);
    if (table == null) {
      return super.getPartition(catName, dbName, tblName, partVals);
    }
    TempTable tt = getPartitionedTempTable(table);
    Partition partition = tt.getPartition(partVals);
    if (partition == null) {
      throw new NoSuchObjectException("Partition with partition values " +
              (partVals != null ? Arrays.toString(partVals.toArray()) : "null")+
              " for table " + tblName + " in database " + dbName + " is not found.");
    }
    return deepCopy(partition);
  }

  /**
   * @param partialPvals partition values, can be partial.  This really means that missing values
   *                    are represented by empty str.
   * @param maxParts maximum number of partitions to fetch, or -1 for all
   */
  @Override
  public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName,
      String tableName, List<String> partialPvals, int maxParts, String userName,
      List<String> groupNames) throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tableName);
    if (table == null) {
      //(assume) not a temp table - Try underlying client
      return super.listPartitionsWithAuthInfo(catName, dbName, tableName, partialPvals, maxParts, userName,
          groupNames);
    }
    TempTable tt = getPartitionedTempTable(table);
    List<Partition> parts = tt.listPartitionsByPartitionValsWithAuthInfo(partialPvals, userName, groupNames);
    return getPartitionsForMaxParts(tableName, parts, maxParts);
  }

  @Override
  public List<Partition> listPartitionsWithAuthInfo(String catName, String dbName, String tableName,
      int maxParts, String userName, List<String> groupNames)
      throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tableName);
    if (table == null) {
      return super.listPartitionsWithAuthInfo(catName, dbName, tableName, maxParts, userName, groupNames);
    }
    TempTable tt = getPartitionedTempTable(table);
    List<Partition> partitions = tt.listPartitionsWithAuthInfo(userName, groupNames);
    return getPartitionsForMaxParts(tableName, partitions, maxParts);
  }

  @Override
  public List<String> listPartitionNames(String catName, String dbName, String tblName,
      int maxParts) throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tblName);
    if (table == null) {
      return super.listPartitionNames(catName, dbName, tblName, maxParts);
    }
    TempTable tt = getPartitionedTempTable(table);
    List<Partition> partitions = tt.listPartitions();
    List<String> result = new ArrayList<>();
    for (int i = 0; i < ((maxParts < 0 || maxParts > partitions.size()) ? partitions.size() : maxParts); i++) {
      result.add(makePartName(table.getPartitionKeys(), partitions.get(i).getValues()));
    }
    Collections.sort(result);
    return result;
  }

  @Override
  public List<String> listPartitionNames(String catName, String dbName, String tblName,
      List<String> partVals, int maxParts) throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tblName);
    if (table == null) {
      return super.listPartitionNames(catName, dbName, tblName, partVals, maxParts);
    }
    TempTable tt = getPartitionedTempTable(table);
    List<Partition> partitions = tt.getPartitionsByPartitionVals(partVals);
    List<String> result = new ArrayList<>();
    for (int i = 0; i < ((maxParts < 0 || maxParts > partitions.size()) ? partitions.size() : maxParts); i++) {
      result.add(makePartName(table.getPartitionKeys(), partitions.get(i).getValues()));
    }
    Collections.sort(result);
    return result;
  }

  @Override
  public List<Partition> listPartitions(String catName, String dbName, String tblName, int maxParts)
      throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tblName);
    if (table == null) {
      return super.listPartitions(catName, dbName, tblName, maxParts);
    }
    TempTable tt = getPartitionedTempTable(table);
    return getPartitionsForMaxParts(tblName, tt.listPartitions(), maxParts);
  }

  @Override
  public List<Partition> listPartitions(String catName, String dbName, String tblName,
      List<String> partVals, int maxParts) throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tblName);
    if (table == null) {
      return super.listPartitions(catName, dbName, tblName, partVals, maxParts);
    }
    TempTable tt = getPartitionedTempTable(table);
    return getPartitionsForMaxParts(tblName, tt.getPartitionsByPartitionVals(partVals), maxParts);
  }

  @Override
  public PartitionSpecProxy listPartitionSpecs(String catName, String dbName, String tableName,
      int maxParts) throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tableName);
    if (table == null) {
      return super.listPartitionSpecs(catName, dbName, tableName, maxParts);
    }
    TempTable tt = getPartitionedTempTable(table);
    return getPartitionSpecProxy(table, tt.listPartitions(), maxParts);
  }

  @Override
  public boolean listPartitionsByExpr(String catName, String dbName, String tblName, byte[] expr,
      String defaultPartitionName, int maxParts, List<Partition> result) throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tblName);
    if (table == null) {
      return super.listPartitionsByExpr(catName, dbName, tblName, expr, defaultPartitionName, maxParts, result);
    }
    assert result != null;
    result.addAll(getPartitionsForMaxParts(tblName, getPartitionedTempTable(table).listPartitionsByFilter(
        generateJDOFilter(table, expr, defaultPartitionName)), maxParts));
    return result.isEmpty();
  }

  @Override
  public List<Partition> getPartitionsByNames(String catName, String dbName, String tblName,
                                              List<String> partNames, boolean getColStats, String engine) throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tblName);
    if (table == null) {
      //(assume) not a temp table - Try underlying client
      return super.getPartitionsByNames(catName, dbName, tblName, partNames, getColStats, engine);
    }
    TempTable tt = getPartitionedTempTable(table);
    List<Partition> partitions = tt.getPartitionsByNames(partNames);

    return deepCopyPartitions(partitions);
  }

  @Override
  public Partition getPartitionWithAuthInfo(String catName, String dbName, String tableName,
                                            List<String> pvals, String userName,
                                            List<String> groupNames) throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tableName);
    if (table == null) {
      return super.getPartitionWithAuthInfo(catName, dbName, tableName, pvals, userName, groupNames);
    }
    TempTable tt = getPartitionedTempTable(table);
    Partition partition = tt.getPartitionWithAuthInfo(pvals, userName, groupNames);
    if (partition == null) {
      throw new NoSuchObjectException("Partition with partition values " +
              (pvals != null ? Arrays.toString(pvals.toArray()) : "null") +
              " for table " + tableName + " in database " + dbName + "and for user " +
              userName + " and group names " + (groupNames != null ? Arrays.toString(groupNames.toArray()) : "null") +
              " is not found.");
    }
    return deepCopy(partition);
  }

  @Override
  public boolean dropPartition(String dbName, String tableName, List<String> partVals) throws TException {
    return dropPartition(getDefaultCatalog(conf), dbName, tableName, partVals,
        PartitionDropOptions.instance().deleteData(true));
  }

  @Override
  public boolean dropPartition(String catName, String dbName, String tblName, List<String> partVals,
      PartitionDropOptions options) throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tblName);
    if (table == null) {
      return super.dropPartition(catName, dbName, tblName, partVals, options);
    }
    if (partVals == null || partVals.isEmpty() || partVals.contains(null)) {
      throw new MetaException("Partition values cannot be null, empty or contain null values");
    }
    TempTable tt = getPartitionedTempTable(table);
    if (tt == null) {
      throw new IllegalStateException("TempTable not found for " + getCatalogQualifiedTableName(table));
    }
    Partition droppedPartition = tt.dropPartition(partVals);
    boolean result = droppedPartition != null ? true : false;
    boolean purgeData = options != null ? options.purgeData : true;
    boolean deleteData = options != null ? options.deleteData : true;
    if (deleteData && !tt.isExternal()) {
      result &= deletePartitionLocation(droppedPartition, purgeData);
    }

    return result;
  }

  @Override
  public boolean dropPartition(String catName, String dbName, String tableName, String partitionName,
      boolean deleteData) throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tableName);
    if (table == null) {
      return super.dropPartition(catName, dbName, tableName, partitionName, deleteData);
    }
    TempTable tt = getPartitionedTempTable(table);
    Partition droppedPartition = tt.dropPartition(partitionName);
    boolean result = droppedPartition != null ? true : false;
    if (deleteData && !tt.isExternal()) {
      result &= deletePartitionLocation(droppedPartition, true);
    }
    return result;
  }

  @Override
  public List<Partition> dropPartitions(String catName, String dbName, String tblName,
      List<Pair<Integer, byte[]>> partExprs, PartitionDropOptions options) throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tblName);
    if (table == null) {
      return super.dropPartitions(catName, dbName, tblName, partExprs, options);
    }
    TempTable tt = getPartitionedTempTable(table);
    List<Partition> result = new ArrayList<>();
    for (Pair<Integer, byte[]> pair : partExprs) {
      byte[] expr = pair.getRight();
      String filter = generateJDOFilter(table, expr, conf.get(HiveConf.ConfVars.DEFAULTPARTITIONNAME.varname));
      List<Partition> partitions = tt.listPartitionsByFilter(filter);
      for (Partition p : partitions) {
        Partition droppedPartition = tt.dropPartition(p.getValues());
        if (droppedPartition != null) {
          result.add(droppedPartition);
          boolean purgeData = options != null ? options.purgeData : true;
          boolean deleteData = options != null ? options.deleteData : true;
          if (deleteData && !tt.isExternal()) {
            deletePartitionLocation(droppedPartition, purgeData);
          }
        }
      }
    }
    return result;
  }

  @Override
  public Partition exchange_partition(Map<String, String> partitionSpecs, String sourceCatName,
      String sourceDbName, String sourceTableName, String destCatName, String destDbName, String destTableName)
      throws TException {
    org.apache.hadoop.hive.metastore.api.Table sourceTempTable = getTempTable(sourceDbName, sourceTableName);
    org.apache.hadoop.hive.metastore.api.Table destTempTable = getTempTable(destDbName, destTableName);
    if (sourceTempTable == null && destTempTable == null) {
      return super
          .exchange_partition(partitionSpecs, sourceCatName, sourceDbName, sourceTableName, destCatName, destDbName,
              destTableName);
    } else if (sourceTempTable != null && destTempTable != null) {
      TempTable sourceTT = getPartitionedTempTable(sourceTempTable);
      TempTable destTT = getPartitionedTempTable(destTempTable);
      List<Partition> partitions = exchangePartitions(partitionSpecs, sourceTempTable, sourceTT, destTempTable, destTT);
      if (!partitions.isEmpty()) {
        return partitions.get(0);
      }
    }
    throw new MetaException("Exchanging partitions between temporary and non-temporary tables is not supported.");
  }

  @Override
  public List<Partition> exchange_partitions(Map<String, String> partitionSpecs, String sourceCatName,
      String sourceDbName, String sourceTableName, String destCatName, String destDbName, String destTableName)
      throws TException {
    org.apache.hadoop.hive.metastore.api.Table sourceTempTable = getTempTable(sourceDbName, sourceTableName);
    org.apache.hadoop.hive.metastore.api.Table destTempTable = getTempTable(destDbName, destTableName);
    if (sourceTempTable == null && destTempTable == null) {
      return super
          .exchange_partitions(partitionSpecs, sourceCatName, sourceDbName, sourceTableName, destCatName, destDbName,
              destTableName);
    } else if (sourceTempTable != null && destTempTable != null) {
      return exchangePartitions(partitionSpecs, sourceTempTable, getPartitionedTempTable(sourceTempTable),
          destTempTable, getPartitionedTempTable(destTempTable));
    }
    throw new MetaException("Exchanging partitions between temporary and non-temporary tables is not supported.");
  }

  @Override
  public void alter_partition(String catName, String dbName, String tblName, Partition newPart,
      EnvironmentContext environmentContext) throws TException {
    alter_partition(catName, dbName, tblName, newPart, environmentContext, null);
  }

  @Override
  public void alter_partition(String catName, String dbName, String tblName, Partition newPart,
      EnvironmentContext environmentContext, String writeIdList)
      throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tblName);
    if (table == null) {
      super.alter_partition(catName, dbName, tblName, newPart, environmentContext, writeIdList);
      return;
    }
    TempTable tt = getPartitionedTempTable(table);
    tt.alterPartition(newPart);
  }

  @Override
  public void alter_partitions(String catName, String dbName, String tblName, List<Partition> newParts,
      EnvironmentContext environmentContext, String writeIdList, long writeId) throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tblName);
    if (table == null) {
      super.alter_partitions(catName, dbName, tblName, newParts, environmentContext, writeIdList, writeId);
      return;
    }
    TempTable tt = getPartitionedTempTable(table);
    tt.alterPartitions(newParts);
  }

  @Override
  public void renamePartition(String catName, String dbname, String tableName, List<String> partitionVals,
      Partition newPart, String validWriteIds) throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbname, tableName);
    if (table == null) {
      super.renamePartition(catName, dbname, tableName, partitionVals, newPart, validWriteIds);
      return;
    }
    TempTable tt = getPartitionedTempTable(table);
    tt.renamePartition(partitionVals, newPart);
  }

  @Override
  public Partition appendPartition(String catName, String dbName, String tableName, List<String> partVals)
      throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tableName);
    if (table == null) {
      return super.appendPartition(catName, dbName, tableName, partVals);
    }
    if (partVals == null || partVals.isEmpty()) {
      throw new MetaException("The partition values must be not null or empty.");
    }
    assertTempTablePartitioned(table);
    Partition partition = new PartitionBuilder().inTable(table).setValues(partVals).build(conf);
    return appendPartitionToTempTable(table, partition);
  }

  @Override
  public Partition appendPartition(String catName, String dbName, String tableName, String partitionName)
      throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tableName);
    if (table == null) {
      return super.appendPartition(catName, dbName, tableName, partitionName);
    }
    if (partitionName == null || partitionName.isEmpty()) {
      throw new MetaException("The partition must be not null or empty.");
    }
    assertTempTablePartitioned(table);
    Map<String, String> specFromName = makeSpecFromName(partitionName);
    if (specFromName == null || specFromName.isEmpty()) {
      throw new InvalidObjectException("Invalid partition name " + partitionName);
    }
    List<String> pVals = new ArrayList<>();
    for (FieldSchema field : table.getPartitionKeys()) {
      String val = specFromName.get(field.getName());
      if (val == null) {
        throw new InvalidObjectException("Partition name " + partitionName + " and table partition keys " + Arrays
            .toString(table.getPartitionKeys().toArray()) + " does not match");
      }
      pVals.add(val);
    }
    Partition partition = new PartitionBuilder().inTable(table).setValues(pVals).build(conf);
    return appendPartitionToTempTable(table, partition);
  }

  @Override
  public List<Partition> listPartitionsByFilter(String catName, String dbName, String tableName,
      String filter, int maxParts) throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tableName);
    if (table == null) {
      return super.listPartitionsByFilter(catName, dbName, tableName, filter, maxParts);
    }
    return getPartitionsForMaxParts(tableName, getPartitionedTempTable(table).listPartitionsByFilter(
        generateJDOFilter(table, filter)), maxParts);
  }

  @Override
  public int getNumPartitionsByFilter(String catName, String dbName, String tableName, String filter)
      throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tableName);
    if (table == null) {
      return super.getNumPartitionsByFilter(catName, dbName, tableName, filter);
    }
    return getPartitionedTempTable(table).getNumPartitionsByFilter(generateJDOFilter(table, filter));
  }

  @Override
  public PartitionSpecProxy listPartitionSpecsByFilter(String catName, String dbName, String tblName,
      String filter, int maxParts) throws TException {
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tblName);
    if (table == null) {
      return super.listPartitionSpecsByFilter(catName, dbName, tblName, filter, maxParts);
    }
    return getPartitionSpecProxy(table, getPartitionedTempTable(table).listPartitionsByFilter(generateJDOFilter(table,
        filter)), maxParts);
  }

  @Override
  public PartitionValuesResponse listPartitionValues(PartitionValuesRequest request) throws TException {
    if (request == null || request.getPartitionKeys() == null || request.getPartitionKeys().isEmpty()) {
      return super.listPartitionValues(request);
    }
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(request.getDbName(), request.getTblName());
    if (table == null) {
      return super.listPartitionValues(request);
    }
    TempTable tt = getPartitionedTempTable(table);
    List<Partition> partitions = request.isSetFilter() ?
        tt.listPartitionsByFilter(generateJDOFilter(table, request.getFilter())) :
        tt.listPartitions();
    List<String> partitionNames = new ArrayList<>();
    for (Partition p : partitions) {
      partitionNames.add(makePartName(table.getPartitionKeys(), p.getValues()));
    }
    if (partitionNames.isEmpty() && partitions.isEmpty()) {
      throw new MetaException("Cannot obtain list of partition by filter:\"" + request.getFilter() +
          "\" for " + getCatalogQualifiedTableName(table));
    }
    if (request.isSetAscending()) {
      if (request.isAscending()) {
        Collections.sort(partitionNames);
      } else {
        Collections.sort(partitionNames, Collections.reverseOrder());
      }
    }
    PartitionValuesResponse response = new PartitionValuesResponse();
    response.setPartitionValues(new ArrayList<>(partitionNames.size()));
    for (String partName : partitionNames) {
      ArrayList<String> vals = new ArrayList<>(Collections.nCopies(table.getPartitionKeysSize(), null));
      PartitionValuesRow row = new PartitionValuesRow();
      makeValsFromName(partName, vals);
      vals.forEach(row::addToRow);
      response.addToPartitionValues(row);
    }
    return response;
  }

  private PartitionSpecProxy getPartitionSpecProxy(org.apache.hadoop.hive.metastore.api.Table table,
      List<Partition> partitions, int maxParts) throws MetaException {
    List<PartitionSpec> partitionSpecs;
    PartitionSpec partitionSpec = new PartitionSpec();
    PartitionListComposingSpec partitionListComposingSpec = new PartitionListComposingSpec(new ArrayList<>());
    for (int i = 0; i < ((maxParts < 0 || maxParts > partitions.size()) ? partitions.size() : maxParts); i++) {
      partitionListComposingSpec.addToPartitions(deepCopy(partitions.get(i)));
    }
    partitionSpec.setCatName(table.getCatName());
    partitionSpec.setDbName(table.getDbName());
    partitionSpec.setTableName(table.getTableName());
    partitionSpec.setRootPath(table.getSd().getLocation());
    partitionSpec.setPartitionList(partitionListComposingSpec);
    partitionSpecs = Arrays.asList(partitionSpec);

    return PartitionSpecProxy.Factory.get(partitionSpecs);
  }

  private List<Partition> getPartitionsForMaxParts(String tableName, List<Partition> parts, int maxParts) {
    List<Partition> matchedParts = new ArrayList<>();
    for(int i = 0; i < ((maxParts < 0 || maxParts > parts.size()) ? parts.size() : maxParts); i++) {
      matchedParts.add(deepCopy(parts.get(i)));
    }
    return matchedParts;
  }

  private String generateJDOFilter(org.apache.hadoop.hive.metastore.api.Table table, String filter)
      throws MetaException {
    ExpressionTree exprTree = org.apache.commons.lang3.StringUtils.isNotEmpty(filter)
        ? PartFilterExprUtil.getFilterParser(filter).tree : ExpressionTree.EMPTY_TREE;
    return generateJDOFilter(table, exprTree);
  }

  private String generateJDOFilter(org.apache.hadoop.hive.metastore.api.Table table, byte[] expr,
      String defaultPartitionName) throws MetaException {
    ExpressionTree expressionTree = PartFilterExprUtil
        .makeExpressionTree(PartFilterExprUtil.createExpressionProxy(conf), expr, defaultPartitionName);
    return generateJDOFilter(table, expressionTree == null ? ExpressionTree.EMPTY_TREE : expressionTree);
  }

  private String generateJDOFilter(org.apache.hadoop.hive.metastore.api.Table table, ExpressionTree exprTree)
      throws MetaException {

    ExpressionTree.FilterBuilder filterBuilder = new ExpressionTree.FilterBuilder(true);
    Map<String, Object> params = new HashMap<>();
    exprTree.generateJDOFilterFragment(conf, table, params, filterBuilder);
    StringBuilder stringBuilder = new StringBuilder(filterBuilder.getFilter());
    // replace leading &&
    stringBuilder.replace(0, 4, "");
    params.entrySet().stream().forEach(e -> {
      int index = stringBuilder.indexOf(e.getKey());
      stringBuilder.replace(index, index + e.getKey().length(), "\"" + e.getValue().toString() + "\"");
    });
    return stringBuilder.toString();
  }

  private Partition appendPartitionToTempTable(org.apache.hadoop.hive.metastore.api.Table table, Partition partition)
      throws MetaException, AlreadyExistsException {
    TempTable tt = getPartitionedTempTable(table);
    if (tt == null) {
      throw new IllegalStateException("TempTable not found for " + getCatalogQualifiedTableName(table));
    }
    Path partitionLocation = getPartitionLocation(table, partition, false);
    partition = tt.addPartition(deepCopy(partition));
    createAndSetLocationForAddedPartition(partition, partitionLocation);
    return partition;
  }

  private List<Partition> exchangePartitions(Map<String, String> partitionSpecs,
      org.apache.hadoop.hive.metastore.api.Table sourceTable, TempTable sourceTempTable,
      org.apache.hadoop.hive.metastore.api.Table destTable, TempTable destTempTable) throws TException {
    if (partitionSpecs == null || partitionSpecs.isEmpty()) {
      throw new MetaException("PartitionSpecs cannot be null or empty.");
    }
    List<String> partitionVals = getPvals(sourceTable.getPartitionKeys(), partitionSpecs);
    if (partitionVals.stream().allMatch(String::isEmpty)) {
      throw new MetaException("Invalid partition key & values; keys " +
          Arrays.toString(sourceTable.getPartitionKeys().toArray()) + ", values " +
          Arrays.toString(partitionVals.toArray()));
    }
    List<Partition> partitionsToExchange = sourceTempTable
        .getPartitionsByPartitionVals(partitionVals);
    if (partitionSpecs == null) {
      throw new MetaException("The partition specs must be not null.");
    }
    if (partitionsToExchange.isEmpty()) {
      throw new MetaException(
          "No partition is found with the values " + partitionSpecs + " for the table " + sourceTable.getTableName());
    }

    boolean sameColumns = compareFieldColumns(sourceTable.getSd().getCols(), destTable.getSd().getCols());
    boolean samePartitions = compareFieldColumns(sourceTable.getPartitionKeys(), destTable.getPartitionKeys());
    if (!(sameColumns && samePartitions)) {
      throw new MetaException("The tables have different schemas. Their partitions cannot be exchanged.");
    }
    // Check if any of the partitions already exists in the destTable
    for (Partition partition : partitionsToExchange) {
      String partToExchangeName = makePartName(destTable.getPartitionKeys(), partition.getValues());
      if (destTempTable.getPartition(partToExchangeName) != null) {
        throw new MetaException(
            "The partition " + partToExchangeName + " already exists in the table " + destTable.getTableName());
      }
    }

    List<Partition> result = new ArrayList<>();
    for (Partition partition : partitionsToExchange) {
      Partition destPartition = new Partition(partition);
      destPartition.setCatName(destTable.getCatName());
      destPartition.setDbName(destTable.getDbName());
      destPartition.setTableName(destTable.getTableName());
      // the destPartition is created from the original partition, therefore all it's properties are copied, including
      // the location. We must force the rewrite of the location (getPartitionLocation(forceRewrite=true))
      destPartition.getSd().setLocation(getPartitionLocation(destTable, destPartition, true).toString());
      wh.renameDir(new Path(partition.getSd().getLocation()), new Path(destPartition.getSd().getLocation()), false);
      destPartition = destTempTable.addPartition(destPartition);
      dropPartition(sourceTable.getDbName(), sourceTable.getTableName(), partition.getValues());
      result.add(destPartition);
    }
    return result;
  }

  private TempTable getPartitionedTempTable(org.apache.hadoop.hive.metastore.api.Table t) throws MetaException {
    String qualifiedTableName = Warehouse.
        getQualifiedName(t.getDbName().toLowerCase(), t.getTableName().toLowerCase());
    SessionState ss = SessionState.get();
    if (ss == null) {
      LOG.warn("No current SessionState, skipping temp partitions for " + qualifiedTableName);
      return null;
    }
    assertTempTablePartitioned(t);
    TempTable tt = ss.getTempPartitions().get(qualifiedTableName);
    if (tt == null) {
      throw new IllegalStateException("TempTable not found for " +
          getCatalogQualifiedTableName(t));
    }
    return tt;
  }
  private void removePartitionedTempTable(org.apache.hadoop.hive.metastore.api.Table t) {
    String qualifiedTableName = Warehouse.
        getQualifiedName(t.getDbName().toLowerCase(), t.getTableName().toLowerCase());
    SessionState ss = SessionState.get();
    if (ss == null) {
      LOG.warn("No current SessionState, skipping temp partitions for " + qualifiedTableName);
      return;
    }
    ss.getTempPartitions().remove(Warehouse.getQualifiedName(t));
  }

  private void createPartitionedTempTable(org.apache.hadoop.hive.metastore.api.Table t) {
    if(t.getPartitionKeysSize() <= 0) {
      //do nothing as it's not a partitioned table
      return;
    }
    String qualifiedTableName = Warehouse.
        getQualifiedName(t.getDbName().toLowerCase(), t.getTableName().toLowerCase());
    SessionState ss = SessionState.get();
    if (ss == null) {
      LOG.warn("No current SessionState, skipping temp partitions for " + qualifiedTableName);
      return;
    }
    TempTable tt = new TempTable(t);
    if(ss.getTempPartitions().putIfAbsent(qualifiedTableName, tt) != null) {
      throw new IllegalStateException("TempTable for " + qualifiedTableName + " already exists");
    }
  }

  /**
   * Create the directory for partition and set it to the partition.
   *
   * @param partition         instance of the partition, must be not null
   * @param partitionLocation the location of the partition
   * @throws MetaException if the target directory already exists
   */
  private void createAndSetLocationForAddedPartition(Partition partition, Path partitionLocation) throws MetaException {

    if (partitionLocation != null) {
      partition.getSd().setLocation(partitionLocation.toString());

      // Check to see if the directory already exists before calling
      // mkdirs() because if the file system is read-only, mkdirs will
      // throw an exception even if the directory already exists.
      if (!getWh().isDir(partitionLocation)) {
        if (!getWh().mkdirs(partitionLocation)) {
          throw new MetaException(partitionLocation
              + " is not a directory or unable to create one");
        }
      }
    }
  }

  /**
   * Checking the validity of some partition properties (values, storage descriptor, columns, serdeinfo).
   *
   * @param partition an instance of the partition, must be not null
   * @throws MetaException if some check is failing
   */
  private void checkPartitionProperties(Partition partition) throws MetaException {
    if (partition.getDbName() == null) {
      throw new MetaException("Database name cannot be null. " + partition);
    }
    if (partition.getTableName() == null) {
      throw new MetaException("Table name cannot be null. " + partition);
    }
    if (partition.getValues() == null) {
      throw new MetaException("Partition values cannot be null. " + partition);
    }
    if (partition.getSd() == null) {
      throw new MetaException("Storage descriptor for partition cannot be null. " + partition);
    }
    if (partition.getSd().getCols() == null) {
      // not sure this is correct, but it is possible to add a partition without column information.
      return;
    }

    for (FieldSchema schema : partition.getSd().getCols()) {
      if (schema.getType() == null) {
        throw new MetaException("Storage descriptor column type for partition cannot be null. " + partition);
      }
      if (schema.getName() == null) {
        throw new MetaException("Storage descriptor column name for partition cannot be null. " + partition);
      }
    }
    if (partition.getSd().getSerdeInfo() == null) {
      throw new MetaException("Storage descriptor serde info for partition cannot be null. " + partition);
    }
  }

  /**
   * Get the partition location. If the partition location is not set the location of the parent table will be used.
   *
   * @param table     the parent table, must be not null
   * @param partition instance of the partition, must be not null
   * @param forceOverwrite force recalculation of location based on table/partition name
   * @return location of partition
   * @throws MetaException if the partition location cannot be specified or the location is invalid.
   */
  private Path getPartitionLocation(org.apache.hadoop.hive.metastore.api.Table table, Partition partition,
      boolean forceOverwrite)
      throws MetaException {
    Path partLocation = null;
    String partLocationStr = null;
    if (partition.getSd() != null) {
      partLocationStr = partition.getSd().getLocation();
    }
    if (partLocationStr == null || partLocationStr.isEmpty() || forceOverwrite) {
      // set default location if not specified and this is
      // a physical table partition (not a view)
      if (table.getSd().getLocation() != null) {
        partLocation =
            new Path(table.getSd().getLocation(), makePartName(table.getPartitionKeys(), partition.getValues()));
      }
    } else {
      if (table.getSd().getLocation() == null) {
        throw new MetaException("Cannot specify location for a view partition");
      }
      try {
        partLocation = getWh().getDnsPath(new Path(partLocationStr));
      } catch (IllegalArgumentException e) {
        throw new MetaException("Partition path is invalid. " + e.getLocalizedMessage());
      }
    }
    return partLocation;
  }

  /**
   * Try to add the partitions to an already existing temporary table.
   *
   * @param partitions  The partitions to add. It must contain at least one item.
   * @param dbName Name of the database, can be null
   * @param tableName Name of the table, can be null
   * @param ifNotExists only add partition if they don't exist
   * @return the partitions that were added to the temp table. If the temp table is not found, null.
   * @throws MetaException
   */
  private List<Partition> addPartitionsToTempTable(List<Partition> partitions, String dbName, String tableName,
      boolean ifNotExists) throws MetaException, AlreadyExistsException {
    if (!validatePartitions(partitions, dbName, tableName)) {
      return null;
    }
    if (dbName == null) {
      dbName = partitions.get(0).getDbName();
    }
    if (tableName == null) {
      tableName = partitions.get(0).getTableName();
    }
    org.apache.hadoop.hive.metastore.api.Table table = getTempTable(dbName, tableName);
    TempTable tt = getPartitionedTempTable(table);
    if (tt == null) {
      throw new IllegalStateException("TempTable not found for " + getCatalogQualifiedTableName(table));
    }
    List<Partition> result = tt.addPartitions(deepCopyPartitions(partitions), ifNotExists);
    for (Partition p : result) {
      createAndSetLocationForAddedPartition(p, getPartitionLocation(table, p, false));
    }
    return result;
  }

  /**
   * Validate various partition and table properties (dbName, tableName, partitionValues, table key size).
   * @param partitions the partition, must be not null
   * @param dbName name of the database, must be not null
   * @param tableName name of the table, must be not null
   * @return true, if all the validation passed. false, if the associated temporary table cannot be found
   * @throws MetaException some validations can throw it.
   */
  private boolean validatePartitions(List<Partition> partitions, String dbName, String tableName) throws MetaException {
    Set<List<String>> partitionVals = new HashSet<>();
    // do some basic validation
    for (Partition p : partitions) {
      if (p.getDbName() == null) {
        throw new MetaException("Database for partition cannot be null. " + p.toString());
      }
      if (p.getTableName() == null) {
        throw new MetaException("Table for partition cannot be null. " + p.toString());
      }

      // check that all new partitions are belonging to tables located in the same database
      if (dbName != null && !dbName.equals(p.getDbName())) {
        throw new MetaException("Partition tables doesn't belong to the same database "
            + Arrays.toString(partitions.toArray()));
      } else {
        dbName = p.getDbName();
      }
      // check if all new partitions are part of the same table
      if (tableName != null && !tableName.equals(p.getTableName())) {
        throw new MetaException("New partitions doesn't belong to the same table "
            + Arrays.toString(partitions.toArray()));
      } else {
        tableName = p.getTableName();
      }

      // validate that new partitions belonging to the same table doesn't contain duplicate partition values
      if (!partitionVals.contains(p.getValues())) {
        partitionVals.add(p.getValues());
      } else {
        throw new MetaException("Partition values contains duplicate entries. "
            + Arrays.toString(partitions.toArray()));
      }

      // check if all the tables are tmp tables
      org.apache.hadoop.hive.metastore.api.Table table = getTempTable(p.getDbName(), p.getTableName());
      if (table == null) {
        return false;
      }

      checkPartitionProperties(p);
      // validate partition location
      getPartitionLocation(table, p, false);
    }
    return true;
  }

  /**
   * Check partition properties of temporary table.
   * @param table table instance, must be not null.
   * @throws MetaException if check fails.
   */
  private void assertTempTablePartitioned(org.apache.hadoop.hive.metastore.api.Table table) throws MetaException {
    if(table.getPartitionKeysSize() <= 0) {
      throw new MetaException(getCatalogQualifiedTableName(table) + " is not partitioned");
    }
  }

  /**
   * Delete the directory where the partition resides.
   * @param partition instance of partition, must be not null
   * @param purgeData purge the data
   * @return true if delete was successful
   * @throws MetaException if delete fails
   */
  private boolean deletePartitionLocation(Partition partition, boolean purgeData) throws MetaException {
    String location = partition.getSd().getLocation();
    if (location != null) {
      Path path = getWh().getDnsPath(new Path(location));
      try {
        do {
          if (!getWh().deleteDir(path, true, purgeData, false)) {
            throw new MetaException("Unable to delete partition at " + location);
          }
          path = path.getParent();
        } while (getWh().isEmptyDir(path));
      } catch (IOException e) {
        throw new MetaException("Unable to delete partition at " + path.toString());
      }
      return true;
    }
    return false;
  }
}
