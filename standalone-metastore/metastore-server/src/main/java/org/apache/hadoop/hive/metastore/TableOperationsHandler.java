/*
 *
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

package org.apache.hadoop.hive.metastore;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropTableRequest;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TableOpResp;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.PreDropTableEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.SecurityUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.ExceptionHandler.handleException;
import static org.apache.hadoop.hive.metastore.HMSHandler.checkTableDataShouldBeDeleted;
import static org.apache.hadoop.hive.metastore.HMSHandler.isDbReplicationTarget;
import static org.apache.hadoop.hive.metastore.HMSHandler.isMustPurge;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

public class TableOperationsHandler<T extends TBase> {
  private static final Logger LOG = LoggerFactory.getLogger(TableOperationsHandler.class);
  private static final Map<String, TableOperationsHandler<?>> OPID_TO_HANDLER = new ConcurrentHashMap<>();
  private static final ScheduledExecutorService OPID_CLEANER = Executors.newScheduledThreadPool(1, r -> {
    Thread thread = new Thread(r);
    thread.setDaemon(true);
    thread.setName("TableOperationsHandler-cleaner");
    return thread;
  });

  private volatile StringBuffer state = new StringBuffer("Preparing context");
  private String id;
  private final IHMSHandler handler_;
  private final T request_;
  private final ExecutorService executor_;
  private final boolean async;
  private Future<?> future;
  private volatile Object result;
  private String messageFormat;
  private final AtomicBoolean aborted = new AtomicBoolean();

  private TableOperationsHandler(String id) {
    this(null, false, null);
    this.id = id;
  }

  private TableOperationsHandler(IHMSHandler handler, boolean async, T request) {
    this.id = UUID.randomUUID().toString();
    this.handler_ = handler;
    this.request_ = request;
    this.async = async;
    if (async) {
      this.executor_ = Executors.newFixedThreadPool(1, r -> {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        thread.setName("TableOperationsHandler " + id);
        return thread;
      });
    } else {
      this.executor_ = MoreExecutors.newDirectExecutorService();
    }
    this.runDropOperation();
    OPID_TO_HANDLER.put(id, this);
  }

  public List<Path> dropTable() throws TException, IOException {
    DropTableRequest dropReq = (DropTableRequest) request_;
    boolean success = false;
    boolean tableDataShouldBeDeleted = true;
    Path tblPath = null;
    List<Path> partPaths = null;
    Table tbl = null;
    Map<String, String> transactionalListenerResponses = Collections.emptyMap();
    Database db = null;
    boolean isReplicated = false;
    RawStore ms = handler_.getMS();
    try {
      ms.openTransaction();
      String name = normalizeIdentifier(dropReq.getTableName());
      String dbname = normalizeIdentifier(dropReq.getDbName());
      String catName =
          normalizeIdentifier(dropReq.isSetCatalogName() ? dropReq.getCatalogName() : getDefaultCatalog(handler_.getConf()));
      TableName tableName = new TableName(catName, dbname, name);

      checkInterrupted();
      // HIVE-25282: Drop/Alter table in REMOTE db should fail
      db = ms.getDatabase(catName, dbname);
      if (MetaStoreUtils.isDatabaseRemote(db)) {
        throw new MetaException("Drop table in REMOTE database " + db.getName() + " is not allowed");
      }
      isReplicated = isDbReplicationTarget(db);

      // drop any partitions
      GetTableRequest req = new GetTableRequest(dbname, name);
      req.setCatName(catName);
      tbl = handler_.get_table_core(req);
      if (tbl == null) {
        throw new NoSuchObjectException(tableName + " doesn't exist");
      }

      if (tbl.getSd() == null) {
        throw new MetaException("Table metadata is corrupted");
      }

      checkInterrupted();
      // Check if table is part of a materialized view.
      // If it is, it cannot be dropped.
      List<String> isPartOfMV = ms.isPartOfMaterializedView(catName, dbname, name);
      if (!isPartOfMV.isEmpty()) {
        throw new MetaException(String.format("Cannot drop table as it is used in the following materialized" +
            " views %s%n", isPartOfMV));
      }

      ((HMSHandler) handler_).firePreEvent(new PreDropTableEvent(tbl, dropReq.isDeleteData(), handler_));

      tableDataShouldBeDeleted = checkTableDataShouldBeDeleted(tbl, dropReq.isDeleteData());
      if (tbl.getSd().getLocation() != null) {
        tblPath = new Path(tbl.getSd().getLocation());
      }
      if (tableDataShouldBeDeleted && tblPath != null) {
        if (!handler_.getWh().isWritable(tblPath.getParent())) {
          throw new MetaException(tableName + " not deleted since " +
              tblPath.getParent() + " is not writable by " +
              SecurityUtils.getUser());
        }
      }
      state = new StringBuffer();
      // Drop the partitions and get a list of locations which need to be deleted
      if (tbl.getPartitionKeysSize() > 0) {
        checkInterrupted();
        List<String> locations = ms.dropAllPartitionsAndGetLocations(tableName,
            tblPath != null ? handler_.getWh().getDnsPath(tblPath).toString() : null, state);
        partPaths = locations.stream().map(Path::new).collect(Collectors.toList());
      }
      // Drop any constraints on the table
      ms.dropConstraint(catName, dbname, name, null, true);

      checkInterrupted();
      state = new StringBuffer("Dropping table");
      if (!ms.dropTable(catName, dbname, name)) {
        throw new MetaException("Unable to drop table " + tableName);
      } else {
        state = new StringBuffer("Notifying transaction listeners");
        if (!handler_.getTransactionalListeners().isEmpty()) {
          transactionalListenerResponses =
              MetaStoreListenerNotifier.notifyEvent(handler_.getTransactionalListeners(),
                  EventMessage.EventType.DROP_TABLE,
                  new DropTableEvent(tbl, true, dropReq.isDeleteData(),
                      handler_, isReplicated),
                  dropReq.getEnvContext());
        }
        checkInterrupted();
        success = ms.commitTransaction();
      }
    } finally {
      try {
        if (!success) {
          ms.rollbackTransaction();
        } else if (tableDataShouldBeDeleted) {
          boolean ifPurge = isMustPurge(dropReq.getEnvContext(), tbl);
          // Data needs deletion. Check if trash may be skipped.
          // Delete the data in the partitions which have other locations
          state = new StringBuffer("Deleting partition directories: " + (partPaths != null ? partPaths.size() : 0));
          ((HMSHandler) handler_).deletePartitionData(partPaths, ifPurge, ReplChangeManager.shouldEnableCm(db, tbl));
          // Delete the data in the table
          state = new StringBuffer("Deleting table directory: " + tblPath);
          ((HMSHandler) handler_).deleteTableData(tblPath, ifPurge, ReplChangeManager.shouldEnableCm(db, tbl));
        }

        if (!handler_.getListeners().isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(handler_.getListeners(), EventMessage.EventType.DROP_TABLE,
              new DropTableEvent(tbl, success, dropReq.isDeleteData(), handler_, isReplicated), dropReq.getEnvContext(),
              transactionalListenerResponses, ms);
        }
      } finally {
        OPID_CLEANER.schedule(() -> OPID_TO_HANDLER.remove(id), 1, TimeUnit.HOURS);
      }
    }
    return partPaths;
  }

  public TableOpResp toTableOpResp() throws TException {
    if (future == null) {
      throw new IllegalStateException(String.format(messageFormat, " hasn't started yet"));
    }
    try {
      result = async ? future.get(100, TimeUnit.MILLISECONDS) : future.get();
    } catch (TimeoutException e) {
      // No Op, return to the caller since long polling timeout has expired
      LOG.trace(String.format(messageFormat, " Long polling timed out"));
    } catch (CancellationException e) {
      // The background operation thread was cancelled
      LOG.trace(String.format(messageFormat, "The background operation was cancelled"));
    } catch (ExecutionException | InterruptedException e) {
      // No op, we will deal with this exception later
      LOG.error(String.format(messageFormat, "failed"), e);
      if (e.getCause() instanceof Exception && !aborted.get()) {
        throw handleException((Exception) e.getCause()).throwIfInstance(TException.class).defaultMetaException();
      }
      String errorMsg = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
      throw new MetaException(String.format(messageFormat, "failed") + " with " + errorMsg);
    }

    TableOpResp resp = new TableOpResp(id);
    if (future.isDone()) {
      resp.setFinished(true);
      resp.setMessage(String.format(messageFormat, future.isCancelled() ? "canceled" : "done"));
    } else {
      resp.setMessage(String.format(messageFormat, "in-progress, state - " + state));
    }
    return resp;
  }

  static Optional<TableOperationsHandler<?>>
      ofCache(String opId, boolean shouldCancel) throws TException {
    TableOperationsHandler<?> tableOp = null;
    if (opId != null) {
      tableOp = OPID_TO_HANDLER.get(opId);
      if (tableOp == null && !shouldCancel) {
        throw new MetaException("Couldn't find the drop operation by " + opId);
      }
      if (shouldCancel) {
        if (tableOp != null) {
          tableOp.cancelOperation();
        } else {
          tableOp = new TableOperationsHandler<TBase>(opId) {
            @Override
            public TableOpResp toTableOpResp() throws TException {
              TableOpResp resp = new TableOpResp(opId);
              resp.setMessage("Drop operation has been canceled");
              resp.setFinished(true);
              return resp;
            }
          };
        }
      }
    }
    return Optional.ofNullable(tableOp);
  }

  static <T extends TBase> TableOperationsHandler<?>
      ofNew(IHMSHandler handler, boolean async, T req) {
    return new TableOperationsHandler<>(handler, async, req);
  }

  private void runDropOperation() {
    if (request_ instanceof DropTableRequest) {
      DropTableRequest dropReq = (DropTableRequest) request_;
      this.messageFormat =
          "Drop on table " + dropReq.getDbName() + "." + dropReq.getTableName() + ": %s";
      this.future = executor_.submit(this::dropTable);
    }
    this.executor_.shutdown();
  }

  public void cancelOperation() {
    if (!future.isDone()) {
      LOG.warn("Drop operation: {} is still running, but a close signal is triggered", id);
      future.cancel(true);
      aborted.set(true);
    }
    executor_.shutdownNow();
  }

  public List<Path> getDropTableResult() throws TException {
    if (!(request_ instanceof DropTableRequest)) {
      throw new IllegalStateException("Current operation " + id + "is not a drop table operation");
    }
    TableOpResp resp = toTableOpResp();
    if (!resp.isFinished()) {
      throw new IllegalStateException("Result is un-available as the operation " + id + " is still running");
    }
    return (List<Path>) result;
  }

  public void checkInterrupted() throws MetaException {
    if (aborted.get()) {
      String errorMessage = "FAILED: drop table " + id + " has been interrupted";
      throw new MetaException(errorMessage);
    }
  }

  @VisibleForTesting
  public static boolean containsOp(String opId) {
    return OPID_TO_HANDLER.containsKey(opId);
  }
}
