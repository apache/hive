package org.apache.hadoop.hive.ql.metadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaHookLoader;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.ql.session.SessionState;
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
    Table newTable = new Table(deepCopyAndLowerCaseTable(newt));
    dbname = dbname.toLowerCase();
    tbl_name = tbl_name.toLowerCase();

    // Disallow changing temp table location
    if (!newt.getSd().getLocation().equals(oldt.getSd().getLocation())) {
      throw new MetaException("Temp table location cannot be changed");
    }

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

      tables = getTempTablesForDatabase(newDbName);
      if (tables == null) {
        tables = new HashMap<String, Table>();
        SessionState.get().getTempTables().put(newDbName, tables);
      }
      tables.put(newTableName, newTable);
    } else {
      getTempTablesForDatabase(dbname).put(tbl_name, newTable);
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
        getWh().deleteDir(tablePath, true);
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
}
