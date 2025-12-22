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
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.ListPackageRequest;
import org.apache.hadoop.hive.metastore.api.ListStoredProcedureRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
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
  private Warehouse wh;
  private RawStore rs;
  private Database db;
  private boolean isReplicated;
  private List<String> tables;
  private List<String> functions;
  private List<String> procedures;
  private List<String> packages;
  private AtomicReference<String> progress;
  
  DropDatabaseHandler(IHMSHandler handler, DropDatabaseRequest request)
      throws TException, IOException {
    super(handler, request.isAsyncDrop(), request);
  }

  public DropDatabaseResult execute() throws TException {
    boolean success = false;
    List<Path> tablePaths = new ArrayList<>();
    List<Path> partitionPaths = new ArrayList<>();
    Map<String, String> transactionalListenerResponses = Collections.emptyMap();
    DropDatabaseResult result = new DropDatabaseResult(db);
    rs.openTransaction();
    try {
      ((HMSHandler) handler).firePreEvent(new PreDropDatabaseEvent(db, handler));
      if (MetaStoreUtils.isDatabaseRemote(db)) {
        if (rs.dropDatabase(db.getCatalogName(), db.getName())) {
          success = rs.commitTransaction();
        }
        return result;
      }
      Path databasePath = wh.getDnsPath(wh.getDatabasePath(db));
      String catPrependedName =
          MetaStoreUtils.prependCatalogToDbName(request.getCatalogName(), request.getName(), handler.getConf());
      // drop any functions before dropping db
      for (int i = 0, j = functions.size(); i < functions.size(); i++, j--) {
        progress.set("Dropping functions from the database, " + j + " functions left");
        String funcName = functions.get(i);
        handler.drop_function(catPrependedName, funcName);
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

      final int tableBatchSize = MetastoreConf.getIntVar(handler.getConf(), MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX);
      // First pass will drop the materialized views
      List<String> materializedViewNames = rs.getTables(request.getCatalogName(), request.getName(),
          ".*", TableType.MATERIALIZED_VIEW, -1);
      AtomicInteger numTables = new AtomicInteger(materializedViewNames == null ? 0 : materializedViewNames.size());
      Batchable.runBatched(tableBatchSize, materializedViewNames, new Batchable<String, Void>() {
        @Override
        public List<Void> run(List<String> input) throws Exception {
          checkInterrupted();
          progress.set("Dropping materialized views from the database, " + numTables.get() + " views left");
          List<Table> materializedViews = rs.getTableObjectsByName(request.getCatalogName(), request.getName(), input);
          for (Table materializedView : materializedViews) {
            boolean isSoftDelete = TxnUtils.isTableSoftDeleteEnabled(materializedView, request.isSoftDelete());

            if (materializedView.getSd().getLocation() != null && !isSoftDelete) {
              Path materializedViewPath = wh.getDnsPath(new Path(materializedView.getSd().getLocation()));

              if (!FileUtils.isSubdirectory(databasePath.toString(), materializedViewPath.toString())
                  || request.isSoftDelete()) {
                if (!wh.isWritable(materializedViewPath.getParent())) {
                  throw new MetaException("Database metadata not deleted since table: " +
                      materializedView.getTableName() + " has a parent location " + materializedViewPath.getParent() +
                      " which is not writable by " + SecurityUtils.getUser());
                }
                tablePaths.add(materializedViewPath);
              }
            }
            EnvironmentContext context = null;
            if (isSoftDelete) {
              context = new EnvironmentContext();
              context.putToProperties(hive_metastoreConstants.TXN_ID, String.valueOf(request.getTxnId()));
            }
            // Drop the materialized view but not its data
            handler.drop_table_with_environment_context(
                request.getName(), materializedView.getTableName(), false, context);

            // Remove from all tables
            tables.remove(materializedView.getTableName());
          }
          numTables.set(numTables.get() - input.size());
          return Collections.emptyList();
        }
      });

      // drop tables before dropping db
      numTables.set(tables.size());
      Batchable.runBatched(tableBatchSize, new ArrayList<>(tables), new Batchable<String, Void>() {
        @Override
        public List<Void> run(List<String> input) throws Exception {
          checkInterrupted();
          progress.set("Dropping tables from the database, " + numTables.get() + " tables left");
          List<Table> tables = rs.getTableObjectsByName(request.getCatalogName(), request.getName(), input);
          for (Table table : tables) {
            // If the table is not external and it might not be in a subdirectory of the database
            // add it's locations to the list of paths to delete
            boolean isSoftDelete = TxnUtils.isTableSoftDeleteEnabled(table, request.isSoftDelete());
            boolean tableDataShouldBeDeleted = checkTableDataShouldBeDeleted(table, request.isDeleteData())
                && !isSoftDelete;

            boolean isManagedTable = table.getTableType().equals(TableType.MANAGED_TABLE.toString());
            if (table.getSd().getLocation() != null && tableDataShouldBeDeleted) {
              Path tablePath = wh.getDnsPath(new Path(table.getSd().getLocation()));
              if (!isManagedTable || request.isSoftDelete()) {
                if (!wh.isWritable(tablePath.getParent())) {
                  throw new MetaException(
                      "Database metadata not deleted since table: " + table.getTableName() + " has a parent location "
                          + tablePath.getParent() + " which is not writable by " + SecurityUtils.getUser());
                }
                tablePaths.add(tablePath);
              }
            }

            EnvironmentContext context = null;
            if (isSoftDelete) {
              context = new EnvironmentContext();
              context.putToProperties(hive_metastoreConstants.TXN_ID, String.valueOf(request.getTxnId()));
              request.setDeleteManagedDir(false);
            }
            DropTableRequest dropRequest = new DropTableRequest(table.getDbName(), table.getTableName());
            dropRequest.setCatalogName(table.getCatName());
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
          numTables.set(numTables.get() - input.size());
          return Collections.emptyList();
        }
      });

      if (rs.dropDatabase(request.getCatalogName(), request.getName())) {
        if (!handler.getTransactionalListeners().isEmpty()) {
          checkInterrupted();
          DropDatabaseEvent dropEvent = new DropDatabaseEvent(db, true, handler, isReplicated);
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
      result.setTablePaths(tablePaths);
      result.setPartitionPaths(partitionPaths);
    } finally {
      if (!success) {
        rs.rollbackTransaction();
      }
      if (!handler.getListeners().isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(handler.getListeners(),
            EventMessage.EventType.DROP_DATABASE,
            new DropDatabaseEvent(db, success, handler, isReplicated),
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
    wh = handler.getWh();
    rs = handler.getMS();
    String catalogName =
        request.isSetCatalogName() ? request.getCatalogName() : MetaStoreUtils.getDefaultCatalog(handler.getConf());
    request.setCatalogName(catalogName);
    db = rs.getDatabase(request.getCatalogName(), request.getName());
    isReplicated = isDbReplicationTarget(db);
    if (!MetastoreConf.getBoolVar(handler.getConf(), HIVE_IN_TEST) && ReplChangeManager.isSourceOfReplication(db)) {
      throw new InvalidOperationException("can not drop a database which is a source of replication");
    }

    tables = defaultEmptyList(rs.getAllTables(request.getCatalogName(), request.getName()));
    functions = defaultEmptyList(rs.getFunctionsRequest(request.getCatalogName(), request.getName(), "*", true));
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
    if (!wh.isWritable(path)) {
      throw new MetaException("Database not dropped since its external warehouse location " + path +
          " is not writable by " + SecurityUtils.getUser());
    }
    path = wh.getDatabaseManagedPath(db).getParent();
    if (!wh.isWritable(path)) {
      throw new MetaException("Database not dropped since its managed warehouse location " + path +
          " is not writable by " + SecurityUtils.getUser());
    }
    progress = new AtomicReference<>(
        String.format("Starting to drop the database with %d tables, %d functions, %d procedures and %d packages.",
            tables.size(), functions.size(), procedures.size(), packages.size()));
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

    public Database getDatabase() {
      return database;
    }
  }

  @Override
  void destroy() {
    tables = null;
    functions = null;
    procedures = null;
    packages = null;
  }
}
