/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.metastore.handler;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.Batchable;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropDatabaseRequest;
import org.apache.hadoop.hive.metastore.api.DropPackageRequest;
import org.apache.hadoop.hive.metastore.api.DropTableRequest;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.ListPackageRequest;
import org.apache.hadoop.hive.metastore.api.ListStoredProcedureRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.ResourceUri;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.PreDropDatabaseEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HIVE_IN_TEST;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.checkTableDataShouldBeDeleted;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.isDbReplicationTarget;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

@RequestHandler(requestBody = DropDatabaseRequest.class, supportAsync = true, metricAlias = "drop_database_req")
public class DropDatabaseHandler
    extends AbstractRequestHandler<DropDatabaseRequest, DropDatabaseHandler.DropDatabaseResult> {

  private static final Logger LOG = LoggerFactory.getLogger(DropDatabaseHandler.class);
  private String name;
  private String catalogName;
  private Database db;
  private List<Table> tables;
  private List<Function> functions;
  private List<String> procedures;
  private List<String> packages;
  private AtomicReference<String> progress;
  private DropDatabaseResult result;

  DropDatabaseHandler(IHMSHandler handler, DropDatabaseRequest request) {
    super(handler, request.isAsyncDrop(), request);
  }

  public DropDatabaseResult execute() throws TException, IOException {
    boolean success = false;
    Map<String, String> transactionalListenerResponses = Collections.emptyMap();
    RawStore rs = handler.getMS();
    rs.openTransaction();
    try {
      if (MetaStoreUtils.isDatabaseRemote(db)) {
        if (rs.dropDatabase(catalogName, name)) {
          success = rs.commitTransaction();
        }
        return result;
      }

      int j = functions.size();
      List<Path> partitionPaths = new ArrayList<>();
      // drop any functions before dropping db
      for (Iterator<Function> iter = functions.iterator(); iter.hasNext(); j--) {
        progress.set("Dropping functions from the database, " + j + " functions left");
        Function func = iter.next();
        rs.dropFunction(catalogName, name, func.getFunctionName());
      }

      j = procedures.size();
      for (Iterator<String> iter = procedures.iterator(); iter.hasNext(); j--) {
        progress.set("Dropping procedures from the database, " + j + " procedures left");
        String procName = iter.next();
        rs.dropStoredProcedure(catalogName, name, procName);
      }

      j = packages.size();
      for (Iterator<String> iter = packages.iterator(); iter.hasNext(); j--) {
        progress.set("Dropping packages from the database, " + j + " packages left");
        String pkgName = iter.next();
        rs.dropPackage(new DropPackageRequest(catalogName, name, pkgName));
      }

      j = tables.size();
      for (Iterator<Table> tablesToDrop = sortTablesToDrop().iterator(); tablesToDrop.hasNext(); j--) {
        progress.set("Dropping tables from the database, " + j + " tables left");
        checkInterrupted();
        Table table = tablesToDrop.next();
        boolean isSoftDelete = TxnUtils.isTableSoftDeleteEnabled(table, request.isSoftDelete());
        boolean tableDataShouldBeDeleted = checkTableDataShouldBeDeleted(table, request.isDeleteData())
            && !isSoftDelete;

        EnvironmentContext context = null;
        if (isSoftDelete) {
          context = new EnvironmentContext();
          context.putToProperties(hive_metastoreConstants.TXN_ID, String.valueOf(request.getTxnId()));
          request.setDeleteManagedDir(false);
        }
        DropTableRequest dropRequest = new DropTableRequest(name, table.getTableName());
        dropRequest.setCatalogName(catalogName);
        dropRequest.setEnvContext(context);
        // Drop the table but not its data
        dropRequest.setDeleteData(false);
        dropRequest.setDropPartitions(true);
        dropRequest.setAsyncDrop(false);
        DropTableHandler dropTable = AbstractRequestHandler.offer(handler, dropRequest);
        if (tableDataShouldBeDeleted
            && dropTable.success()) {
          DropTableHandler.DropTableResult dropTableResult = dropTable.getResult();
          if (dropTableResult.partPaths() != null) {
            partitionPaths.addAll(dropTableResult.partPaths());
          }
        }
      }

      if (rs.dropDatabase(catalogName, name)) {
        if (!handler.getTransactionalListeners().isEmpty()) {
          checkInterrupted();
          DropDatabaseEvent dropEvent = new DropDatabaseEvent(db, true, handler, isDbReplicationTarget(db));
          EnvironmentContext context = null;
          if (!request.isDeleteManagedDir()) {
            context = new EnvironmentContext();
            context.putToProperties(hive_metastoreConstants.TXN_ID, String.valueOf(request.getTxnId()));
          }
          dropEvent.setEnvironmentContext(context);
          transactionalListenerResponses =
              MetaStoreListenerNotifier.notifyEvent(handler.getTransactionalListeners(),
                  EventMessage.EventType.DROP_DATABASE, dropEvent);
        }
        success = rs.commitTransaction();
      }
      result.setSuccess(success);
      result.setPartitionPaths(partitionPaths);
    } finally {
      if (!success) {
        rs.rollbackTransaction();
      }
      if (!handler.getListeners().isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(handler.getListeners(),
            EventMessage.EventType.DROP_DATABASE,
            new DropDatabaseEvent(db, success, handler, isDbReplicationTarget(db)),
            null,
            transactionalListenerResponses, rs);
      }
    }
    return result;
  }

  @Override
  protected void beforeExecute() throws TException, IOException {
    if (request.getName() == null) {
      throw new MetaException("Database name cannot be null.");
    }
    this.name = normalizeIdentifier(request.getName());
    this.catalogName = normalizeIdentifier(
        request.isSetCatalogName() ? request.getCatalogName() : MetaStoreUtils.getDefaultCatalog(handler.getConf()));

    RawStore rs = handler.getMS();
    db = rs.getDatabase(catalogName, name);
    if (!MetastoreConf.getBoolVar(handler.getConf(), HIVE_IN_TEST) && ReplChangeManager.isSourceOfReplication(db)) {
      throw new InvalidOperationException("can not drop a database which is a source of replication");
    }

    List<String> tableNames = defaultEmptyList(rs.getAllTables(catalogName, name));
    functions = defaultEmptyList(rs.getFunctionsRequest(catalogName, name, null, false));
    ListStoredProcedureRequest procedureRequest = new ListStoredProcedureRequest(catalogName);
    procedureRequest.setDbName(name);
    procedures = defaultEmptyList(rs.getAllStoredProcedures(procedureRequest));
    ListPackageRequest pkgRequest = new ListPackageRequest(catalogName);
    pkgRequest.setDbName(name);
    packages = defaultEmptyList(rs.listPackages(pkgRequest));

    if (!request.isCascade()) {
      if (!tableNames.isEmpty()) {
        throw new InvalidOperationException(
            "Database " + db.getName() + " is not empty. One or more tables exist.");
      }
      if (!functions.isEmpty()) {
        throw new InvalidOperationException(
            "Database " + db.getName() + " is not empty. One or more functions exist.");
      }
      if (!procedures.isEmpty()) {
        throw new InvalidOperationException(
            "Database " + db.getName() + " is not empty. One or more stored procedures exist.");
      }
      if (!packages.isEmpty()) {
        throw new InvalidOperationException(
            "Database " + db.getName() + " is not empty. One or more packages exist.");
      }
    }
    Path path = new Path(db.getLocationUri()).getParent();
    if (!handler.getWh().isWritable(path)) {
      throw new MetaException("Database not dropped since its external warehouse location " + path +
          " is not writable by " + SecurityUtils.getUser());
    }
    path = handler.getWh().getDatabaseManagedPath(db).getParent();
    if (!handler.getWh().isWritable(path)) {
      throw new MetaException("Database not dropped since its managed warehouse location " + path +
          " is not writable by " + SecurityUtils.getUser());
    }

    result = new DropDatabaseResult(db);
    checkFuncPathToCm();
    // check the permission of table path to be deleted
    checkTablePathPermission(rs, tableNames);
    progress = new AtomicReference<>(
        String.format("Starting to drop the database with %d tables, %d functions, %d procedures and %d packages.",
            tables.size(), functions.size(), procedures.size(), packages.size()));

    ((HMSHandler) handler).firePreEvent(new PreDropDatabaseEvent(db, handler));
  }

  private void checkFuncPathToCm() {
    List<Path> funcNeedCmPaths = new ArrayList<>();
    if (ReplChangeManager.isSourceOfReplication(db)) {
      for (Function func : functions) {
        // if copy of jar to change management fails we fail the metastore transaction, since the
        // user might delete the jars on HDFS externally after dropping the function, hence having
        // a copy is required to allow incremental replication to work correctly.
        if (func.getResourceUris() != null && !func.getResourceUris().isEmpty()) {
          for (ResourceUri uri : func.getResourceUris()) {
            if (uri.getUri().toLowerCase().startsWith("hdfs:")) {
              funcNeedCmPaths.add(new Path(uri.getUri()));
            }
          }
        }
      }
    }
    result.setFunctionCmPaths(funcNeedCmPaths);
  }

  private void checkTablePathPermission(RawStore rs, List<String> tableNames) throws MetaException {
    int tableBatchSize = MetastoreConf.getIntVar(handler.getConf(), MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX);
    Warehouse wh = handler.getWh();
    Path databasePath = wh.getDnsPath(wh.getDatabasePath(db));
    List<Path> tablePaths = new ArrayList<>();
    this.tables = Batchable.runBatched(tableBatchSize, tableNames, new Batchable<>() {
      @Override
      public List<Table> run(List<String> input) throws Exception {
        List<Table> tabs = rs.getTableObjectsByName(catalogName, name, input);
        for (Table table : tabs) {
          Path tblPathToDelete = null;
          // If the table is not external and it might not be in a subdirectory of the database
          // add it's locations to the list of paths to delete
          boolean isSoftDelete = TxnUtils.isTableSoftDeleteEnabled(table, request.isSoftDelete());
          boolean tableDataShouldBeDeleted = checkTableDataShouldBeDeleted(table,
              request.isDeleteData()) && !isSoftDelete;
          if (table.getSd().getLocation() != null) {
            if (table.getTableType().equals(TableType.MATERIALIZED_VIEW.toString()) && !isSoftDelete) {
              Path materializedViewPath = wh.getDnsPath(new Path(table.getSd().getLocation()));
              if (!FileUtils.isSubdirectory(databasePath.toString(), materializedViewPath.toString()) ||
                  request.isSoftDelete()) {
                tblPathToDelete = materializedViewPath;
              }
            } else if (tableDataShouldBeDeleted) {
              boolean isManagedTable = table.getTableType().equals(TableType.MANAGED_TABLE.toString());
              if (!isManagedTable || request.isSoftDelete()) {
                tblPathToDelete = wh.getDnsPath(new Path(table.getSd().getLocation()));
              }
            }
          }
          if (tblPathToDelete != null) {
            if (!wh.isWritable(tblPathToDelete.getParent())) {
              throw new MetaException("Database metadata not deleted since table: " +
                  table.getTableName() + " has a parent location " + tblPathToDelete.getParent() +
                  " which is not writable by " + SecurityUtils.getUser());
            }
            tablePaths.add(tblPathToDelete);
          }
        }
        return tabs;
      }
    });
    result.setTablePaths(tablePaths);
  }

  private <T> List<T> defaultEmptyList(List<T> list) {
    if (list == null) {
      return Collections.emptyList();
    }
    return list;
  }

  private List<Table> sortTablesToDrop() {
    // drop the materialized view first
    List<Table> materializedTables = new ArrayList<>();
    List<Table> normalTables = new ArrayList<>();
    for (Table table : tables) {
      if (table.getTableType().equals(TableType.MATERIALIZED_VIEW.toString())) {
        materializedTables.add(table);
      } else {
        normalTables.add(table);
      }
    }
    materializedTables.addAll(normalTables);
    return materializedTables;
  }

  @Override
  protected String getMessagePrefix() {
    return "DropDatabaseHandler [" + id + "] -  Drop database " + name + ":";
  }

  @Override
  protected String getRequestProgress() {
    if (progress == null) {
      return getMessagePrefix() + " hasn't started yet";
    }
    return progress.get();
  }

  public static class DropDatabaseResult implements Result {
    private boolean success;
    private List<Path> tablePaths;
    private List<Path> partitionPaths;
    private List<Path> functionCmPaths;
    private final Database database;

    public DropDatabaseResult(Database db) {
      this.database = db;
    }

    public boolean success() {
      return success;
    }

    public void setSuccess(boolean success) {
      this.success = success;
    }

    public List<Path> getTablePaths() {
      return tablePaths;
    }

    public void setTablePaths(List<Path> tablePaths) {
      this.tablePaths = tablePaths;
    }

    public List<Path> getPartitionPaths() {
      return partitionPaths;
    }

    public void setPartitionPaths(List<Path> partitionPaths) {
      this.partitionPaths = partitionPaths;
    }

    public List<Path> getFunctionCmPaths() {
      return functionCmPaths;
    }

    public void setFunctionCmPaths(List<Path> functionCmPaths) {
      this.functionCmPaths = functionCmPaths;
    }

    public Database getDatabase() {
      return database;
    }

    @Override
    public Result shrinkIfNecessary() {
      DropDatabaseResult result = new DropDatabaseResult(null);
      result.setSuccess(result.success);
      return result;
    }
  }

  @Override
  protected void afterExecute(DropDatabaseResult result) throws MetaException, IOException {
    try {
      Warehouse wh = handler.getWh();
      if (result != null && result.success()) {
        for (Path funcCmPath : result.getFunctionCmPaths()) {
          wh.addToChangeManagement(funcCmPath);
        }
        if (request.isDeleteData()) {
          Database db = result.getDatabase();
          // Delete the data in the partitions which have other locations
          List<Path> pathsToDelete = new ArrayList<>();
          if (result.getPartitionPaths() != null) {
            pathsToDelete.addAll(result.getPartitionPaths());
          }
          pathsToDelete.addAll(result.getTablePaths());
          for (Path pathToDelete : pathsToDelete) {
            try {
              wh.deleteDir(pathToDelete, false, db);
            } catch (Exception e) {
              LOG.error("Failed to delete directory: {}", pathToDelete, e);
            }
          }
          Path path = (db.getManagedLocationUri() != null) ?
              new Path(db.getManagedLocationUri()) : wh.getDatabaseManagedPath(db);
          if (request.isDeleteManagedDir()) {
            try {
              Boolean deleted = UserGroupInformation.getLoginUser().doAs((PrivilegedExceptionAction<Boolean>)
                  () -> wh.deleteDir(path, true, db));
              if (!deleted) {
                LOG.error("Failed to delete database's managed warehouse directory: {}", path);
              }
            } catch (Exception e) {
              LOG.error("Failed to delete database's managed warehouse directory: {}", path, e);
            }
          }
          try {
            Boolean deleted = UserGroupInformation.getCurrentUser().doAs((PrivilegedExceptionAction<Boolean>)
                () -> wh.deleteDir(new Path(db.getLocationUri()), true, db));
            if (!deleted) {
              LOG.error("Failed to delete database external warehouse directory: {}", db.getLocationUri());
            }
          } catch (Exception e) {
            LOG.error("Failed to delete the database external warehouse directory: {}", db.getLocationUri(), e);
          }
        }
      }
    } finally {
      tables = null;
      functions = null;
      procedures = null;
      packages = null;
      db = null;
      super.afterExecute(result);
    }
  }
}
