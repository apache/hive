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

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.PreDropTableEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.thrift.TException;

import static org.apache.hadoop.hive.metastore.HMSHandler.checkTableDataShouldBeDeleted;
import static org.apache.hadoop.hive.metastore.HMSHandler.isDbReplicationTarget;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

public class AsyncDropTableHandler
    extends AbstractAsyncOperationHandler<DropTableRequest, AsyncDropTableHandler.DropTableResult> {
  private Table tbl;
  private Path tblPath;
  private TableName tableName;
  private boolean tableDataShouldBeDeleted;
  private AtomicReference<String> progress;

  AsyncDropTableHandler(IHMSHandler handler, boolean async, DropTableRequest request) {
    super(handler, async, request);
  }

  public DropTableResult dropTable() throws TException {
    boolean success = false;
    List<Path> partPaths = null;
    Map<String, String> transactionalListenerResponses = Collections.emptyMap();
    Database db = null;
    boolean isReplicated = false;
    RawStore ms = handler.getMS();
    try {
      ms.openTransaction();
      String catName = tableName.getCat();
      String dbname = tableName.getDb();
      String name = tableName.getTable();
      // HIVE-25282: Drop/Alter table in REMOTE db should fail
      db = ms.getDatabase(catName, dbname);
      if (MetaStoreUtils.isDatabaseRemote(db)) {
        throw new MetaException("Drop table in REMOTE database " + db.getName() + " is not allowed");
      }
      isReplicated = isDbReplicationTarget(db);

      checkInterrupted();
      // Check if table is part of a materialized view.
      // If it is, it cannot be dropped.
      List<String> isPartOfMV = ms.isPartOfMaterializedView(catName, dbname, name);
      if (!isPartOfMV.isEmpty()) {
        throw new MetaException(String.format("Cannot drop table as it is used in the following materialized" +
            " views %s%n", isPartOfMV));
      }

      ((HMSHandler) handler).firePreEvent(new PreDropTableEvent(tbl, request.isDeleteData(), handler));

      // Drop the partitions and get a list of locations which need to be deleted
      if (request.isDropPartitions()) {
        checkInterrupted();
        List<String> locations = ms.dropAllPartitionsAndGetLocations(tableName,
            tblPath != null ? handler.getWh().getDnsPath(tblPath).toString() : null, progress);
        partPaths = locations.stream().map(Path::new).toList();
      }
      // Drop any constraints on the table
      ms.dropConstraint(catName, dbname, name, null, true);

      checkInterrupted();
      if (!ms.dropTable(catName, dbname, name)) {
        throw new MetaException("Unable to drop table " + tableName);
      } else {
        progress.set("Notifying transaction listeners");
        checkInterrupted();
        if (!handler.getTransactionalListeners().isEmpty()) {
          transactionalListenerResponses =
              MetaStoreListenerNotifier.notifyEvent(handler.getTransactionalListeners(),
                  EventMessage.EventType.DROP_TABLE,
                  new DropTableEvent(tbl, true, request.isDeleteData(), handler, isReplicated),
                  request.getEnvContext());
        }
        success = ms.commitTransaction();
      }
      return new DropTableResult(tbl, success, tableDataShouldBeDeleted,
          partPaths, ReplChangeManager.shouldEnableCm(db, tbl));
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }
      if (!handler.getListeners().isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(handler.getListeners(), EventMessage.EventType.DROP_TABLE,
            new DropTableEvent(tbl, success, request.isDeleteData(), handler, isReplicated), request.getEnvContext(),
            transactionalListenerResponses, ms);
      }
    }
  }

  public DropTableResult execute() throws TException, IOException {
    // drop any partitions
    String catName = normalizeIdentifier(
        request.isSetCatalogName() ? request.getCatalogName() : getDefaultCatalog(handler.getConf()));
    String name = normalizeIdentifier(request.getTableName());
    String dbname = normalizeIdentifier(request.getDbName());
    tableName = new TableName(catName, dbname, name);
    progress = new AtomicReference<>("Starting to drop the table: " + tableName);
    GetTableRequest req = new GetTableRequest(request.getDbName(), request.getTableName());
    req.setCatName(catName);
    tbl = handler.get_table_core(req);
    if (tbl == null) {
      throw new NoSuchObjectException(tableName + " doesn't exist");
    }
    if (tbl.getSd() == null) {
      throw new MetaException("Table metadata is corrupted");
    }
    tableDataShouldBeDeleted = checkTableDataShouldBeDeleted(tbl, request.isDeleteData());
    if (tbl.getSd().getLocation() != null) {
      tblPath = new Path(tbl.getSd().getLocation());
    }
    if (tableDataShouldBeDeleted && tblPath != null) {
      if (!handler.getWh().isWritable(tblPath.getParent())) {
        throw new MetaException(tableName + " not deleted since " + tblPath.getParent() +
            " is not writable by " + SecurityUtils.getUser());
      }
    }
    return dropTable();
  }

  @Override
  public String getLogMessagePrefix() {
    return "AsyncDropTableHandler [" + id + "] -  Drop table " + tableName + ":";
  }

  @Override
  public String getOperationProgress() {
    if (progress == null) {
      return "AsyncDropTableHandler [" + id + "] hasn't started yet";
    }
    return progress.get();
  }

  @Override
  void destroy() {
    super.destroy();
    tbl = null;
    tblPath = null;
  }

  public record DropTableResult(Table table,
                                boolean success,
                                boolean tableDataShouldBeDeleted,
                                List<Path> partPaths,
                                boolean shouldEnableCm) {

  }
}
