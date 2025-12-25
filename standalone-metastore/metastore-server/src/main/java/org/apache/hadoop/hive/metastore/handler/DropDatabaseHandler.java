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
import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.thrift.TException;

import static org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars.HIVE_IN_TEST;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.checkTableDataShouldBeDeleted;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.isDbReplicationTarget;

public class DropDatabaseHandler
    extends AbstractOperationHandler<DropDatabaseRequest, DropDatabaseHandler.DropDatabaseResult> {
  private Database db;
  private List<Table> tables;
  private List<Function> functions;
  private List<String> procedures;
  private List<String> packages;
  private AtomicReference<String> progress;
  private DropDatabaseResult result;

  DropDatabaseHandler(IHMSHandler handler, DropDatabaseRequest request)
      throws TException, IOException {
    super(handler, request.isAsyncDrop(), request);
  }

  public DropDatabaseResult execute() throws TException, IOException {
    boolean success = false;

    List<Path> partitionPaths = new ArrayList<>();
    Map<String, String> transactionalListenerResponses = Collections.emptyMap();
    RawStore rs = handler.getMS();
    rs.openTransaction();
    try {
      ((HMSHandler) handler).firePreEvent(new PreDropDatabaseEvent(db, handler));
      if (MetaStoreUtils.isDatabaseRemote(db)) {
        if (rs.dropDatabase(db.getCatalogName(), db.getName())) {
          success = rs.commitTransaction();
        }
        return result;
      }
      // drop any functions before dropping db
      for (int i = 0, j = functions.size(); i < functions.size(); i++, j--) {
        progress.set("Dropping functions from the database, " + j + " functions left");
        Function func = functions.get(i);
        rs.dropFunction(request.getCatalogName(), request.getName(), func.getFunctionName());
      }

      for (int i = 0, j = procedures.size(); i < procedures.size(); i++, j--) {
        progress.set("Dropping procedures from the database, " + j + " procedures left");
        String procName = procedures.get(i);
        rs.dropStoredProcedure(request.getCatalogName(), request.getName(), procName);
      }

      for (int i = 0, j = packages.size(); i < packages.size(); i++, j--) {
        progress.set("Dropping packages from the database, " + j + " packages left");
        String pkgName = packages.get(i);
        rs.dropPackage(new DropPackageRequest(request.getCatalogName(), request.getName(), pkgName));
      }

      for (int i = 0, j = tables.size(); i < tables.size(); i++, j--) {
        checkInterrupted();
        progress.set("Dropping tables from the database, " + j + " tables left");
        Table table = tables.get(i);
        boolean isSoftDelete = TxnUtils.isTableSoftDeleteEnabled(table, request.isSoftDelete());
        boolean tableDataShouldBeDeleted = checkTableDataShouldBeDeleted(table, request.isDeleteData())
            && !isSoftDelete;

        EnvironmentContext context = null;
        if (isSoftDelete) {
          context = new EnvironmentContext();
          context.putToProperties(hive_metastoreConstants.TXN_ID, String.valueOf(request.getTxnId()));
          request.setDeleteManagedDir(false);
        }
        DropTableRequest dropRequest = new DropTableRequest(request.getName(), table.getTableName());
        dropRequest.setCatalogName(request.getCatalogName());
        dropRequest.setEnvContext(context);
        // Drop the table but not its data
        dropRequest.setDeleteData(false);
        dropRequest.setDropPartitions(true);
        AbstractOperationHandler<DropTableRequest, DropTableHandler.DropTableResult> asyncOp =
            AbstractOperationHandler.offer(handler, dropRequest);
        DropTableHandler.DropTableResult result = asyncOp.getResult();
        if (tableDataShouldBeDeleted
            && result.success()
            && result.partPaths() != null) {
          partitionPaths.addAll(result.partPaths());
        }
      }

      if (rs.dropDatabase(request.getCatalogName(), request.getName())) {
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
    RawStore rs = handler.getMS();
    String catalogName =
        request.isSetCatalogName() ? request.getCatalogName() : MetaStoreUtils.getDefaultCatalog(handler.getConf());
    request.setCatalogName(catalogName);
    db = rs.getDatabase(request.getCatalogName(), request.getName());
    if (!MetastoreConf.getBoolVar(handler.getConf(), HIVE_IN_TEST) && ReplChangeManager.isSourceOfReplication(db)) {
      throw new InvalidOperationException("can not drop a database which is a source of replication");
    }

    List<String> tables = defaultEmptyList(rs.getAllTables(request.getCatalogName(), request.getName()));
    functions = defaultEmptyList(rs.getFunctionsRequest(request.getCatalogName(), request.getName(), null, false));
    ListStoredProcedureRequest procedureRequest = new ListStoredProcedureRequest(request.getCatalogName());
    procedureRequest.setDbName(request.getName());
    procedures = defaultEmptyList(rs.getAllStoredProcedures(procedureRequest));
    ListPackageRequest pkgRequest = new ListPackageRequest(request.getCatalogName());
    pkgRequest.setDbName(request.getName());
    packages = defaultEmptyList(rs.listPackages(pkgRequest));

    if (!request.isCascade()) {
      if (!tables.isEmpty()) {
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
    addFuncPathToCm();
    // check the permission of table path to be deleted
    checkTablePathPermission(rs, tables);
    progress = new AtomicReference<>(
        String.format("Starting to drop the database with %d tables, %d functions, %d procedures and %d packages.",
            tables.size(), functions.size(), procedures.size(), packages.size()));
  }

  private void addFuncPathToCm() {
    boolean needsCm = ReplChangeManager.isSourceOfReplication(db);
    List<Path> funcNeedCmPaths = new ArrayList<>();
    for (Function func : functions) {
      // if copy of jar to change management fails we fail the metastore transaction, since the
      // user might delete the jars on HDFS externally after dropping the function, hence having
      // a copy is required to allow incremental replication to work correctly.
      if (func.getResourceUris() != null && !func.getResourceUris().isEmpty()) {
        for (ResourceUri uri : func.getResourceUris()) {
          if (uri.getUri().toLowerCase().startsWith("hdfs:") && needsCm) {
            funcNeedCmPaths.add(new Path(uri.getUri()));
          }
        }
      }
    }
    result.setFunctionCmPaths(funcNeedCmPaths);
  }

  private void checkTablePathPermission(RawStore rs, List<String> tables) throws MetaException {
    int tableBatchSize = MetastoreConf.getIntVar(handler.getConf(), MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX);
    Warehouse wh = handler.getWh();
    Path databasePath = wh.getDnsPath(wh.getDatabasePath(db));
    List<Path> tablePaths = new ArrayList<>();
    this.tables = Batchable.runBatched(tableBatchSize, new ArrayList<>(tables), new Batchable<>() {
      @Override
      public List<Table> run(List<String> input) throws Exception {
        List<Table> tables = rs.getTableObjectsByName(request.getCatalogName(), request.getName(), input);
        for (Table table : tables) {
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
        return tables;
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

  @Override
  protected String getMessagePrefix() {
    return "DropDatabaseHandler [" + id + "] -  Drop database " + request.getName() + ":";
  }

  @Override
  protected String getProgress() {
    if (progress == null) {
      return getMessagePrefix() + " hasn't started yet";
    }
    return progress.get();
  }

  public static class DropDatabaseResult {
    private boolean success;
    private List<Path> tablePaths;
    private List<Path> partitionPaths;
    private List<Path> functionCmPaths;
    private final Database database;

    public DropDatabaseResult(Database db) {
      this.database = db;
    }

    public boolean isSuccess() {
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
  }

  @Override
  protected void afterExecute() {
    super.afterExecute();
    tables = null;
    functions = null;
    procedures = null;
    packages = null;
  }
}
