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

package org.apache.hadoop.hive.metastore.handler;

import java.io.IOException;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.IMetaStoreMetadataTransformer;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.TransactionalMetaStoreEventListener;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.CreateTableRequest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.InvalidInputException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.SQLAllTableConstraints;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.events.AddCheckConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AddDefaultConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AddForeignKeyEvent;
import org.apache.hadoop.hive.metastore.events.AddNotNullConstraintEvent;
import org.apache.hadoop.hive.metastore.events.AddPrimaryKeyEvent;
import org.apache.hadoop.hive.metastore.events.AddUniqueConstraintEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.PreCreateTableEvent;
import org.apache.hadoop.hive.metastore.events.UpdateTableColumnStatEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.common.AcidConstants.DELTA_DIGITS;
import static org.apache.hadoop.hive.common.AcidConstants.SOFT_DELETE_PATH_SUFFIX;
import static org.apache.hadoop.hive.common.AcidConstants.SOFT_DELETE_TABLE;
import static org.apache.hadoop.hive.common.AcidConstants.SOFT_DELETE_TABLE_PATTERN;
import static org.apache.hadoop.hive.metastore.Warehouse.getCatalogQualifiedTableName;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.CTAS_LEGACY_CONFIG;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_CTAS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_CTLT;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.isDbReplicationTarget;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;
import static org.apache.hadoop.hive.metastore.utils.StringUtils.normalizeIdentifier;

@RequestHandler(requestBody = CreateTableRequest.class)
public class CreateTableHandler
    extends AbstractRequestHandler<CreateTableRequest, CreateTableHandler.CreateTableResult> {
  private static final Logger LOG = LoggerFactory.getLogger(CreateTableHandler.class);
  private SQLAllTableConstraints constraints;
  private RawStore rs;
  private Table tbl;
  private Database db;
  private Warehouse wh;
  private ColumnStatistics colStats;
  private EnvironmentContext envContext;

  CreateTableHandler(IHMSHandler handler, CreateTableRequest request) {
    super(handler, false, request);
  }

  @Override
  protected CreateTableResult execute() throws TException, IOException {
    Map<String, String> transactionalListenerResponses = Collections.emptyMap();
    Path tblPath = null;
    boolean success = false, madeDir = false;
    boolean isReplicated;
    try {
      rs.openTransaction();
      db = rs.getDatabase(tbl.getCatName(), tbl.getDbName());
      isReplicated = isDbReplicationTarget(db);

      ((HMSHandler)handler).firePreEvent(new PreCreateTableEvent(tbl, db, handler));

      if (!TableType.VIRTUAL_VIEW.toString().equals(tbl.getTableType())) {
        if (tbl.getSd().getLocation() == null
            || tbl.getSd().getLocation().isEmpty()) {
          tblPath = wh.getDefaultTablePath(db, tbl.getTableName() + getTableSuffix(tbl),
              MetaStoreUtils.isExternalTable(tbl));
        } else {
          if (!MetaStoreUtils.isExternalTable(tbl) && !MetaStoreUtils.isNonNativeTable(tbl)) {
            LOG.warn("Location: " + tbl.getSd().getLocation()
                + " specified for non-external table:" + tbl.getTableName());
          }
          tblPath = wh.getDnsPath(new Path(tbl.getSd().getLocation()));
          // ignore suffix if it's already there (direct-write CTAS)
          if (!tblPath.getName().matches("(.*)" + SOFT_DELETE_TABLE_PATTERN)) {
            tblPath = new Path(tblPath + getTableSuffix(tbl));
          }
        }
        tbl.getSd().setLocation(tblPath.toString());
      }

      if (tblPath != null) {
        if (!wh.isDir(tblPath)) {
          if (!wh.mkdirs(tblPath)) {
            throw new MetaException(tblPath
                + " is not a directory or unable to create one");
          }
          madeDir = true;
        }
      }

      MetaStoreServerUtils.updateTableStatsForCreateTable(wh, db, tbl,
          request.getEnvContext(), handler.getConf(), tblPath, madeDir);

      // set create time
      long time = System.currentTimeMillis() / 1000;
      tbl.setCreateTime((int) time);
      if (tbl.getParameters() == null ||
          tbl.getParameters().get(hive_metastoreConstants.DDL_TIME) == null) {
        tbl.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(time));
      }
      if (CollectionUtils.isEmpty(constraints.getPrimaryKeys())
          && CollectionUtils.isEmpty(constraints.getForeignKeys())
          && CollectionUtils.isEmpty(constraints.getUniqueConstraints())
          && CollectionUtils.isEmpty(constraints.getNotNullConstraints())
          && CollectionUtils.isEmpty(constraints.getDefaultConstraints())
          && CollectionUtils.isEmpty(constraints.getCheckConstraints())) {
        rs.createTable(tbl);
      } else {
        // Set constraint name if null before sending to listener
        constraints = rs.createTableWithConstraints(tbl, constraints);
      }

      List<TransactionalMetaStoreEventListener> transactionalListeners = handler.getTransactionalListeners();
      if (!transactionalListeners.isEmpty()) {
        transactionalListenerResponses = MetaStoreListenerNotifier.notifyEvent(transactionalListeners,
            EventMessage.EventType.CREATE_TABLE, new CreateTableEvent(tbl, true, handler, isReplicated), envContext);
        if (CollectionUtils.isNotEmpty(constraints.getPrimaryKeys())) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners, EventMessage.EventType.ADD_PRIMARYKEY,
              new AddPrimaryKeyEvent(constraints.getPrimaryKeys(), true, handler), envContext);
        }
        if (CollectionUtils.isNotEmpty(constraints.getForeignKeys())) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners, EventMessage.EventType.ADD_FOREIGNKEY,
              new AddForeignKeyEvent(constraints.getForeignKeys(), true, handler), envContext);
        }
        if (CollectionUtils.isNotEmpty(constraints.getUniqueConstraints())) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners, EventMessage.EventType.ADD_UNIQUECONSTRAINT,
              new AddUniqueConstraintEvent(constraints.getUniqueConstraints(), true, handler), envContext);
        }
        if (CollectionUtils.isNotEmpty(constraints.getNotNullConstraints())) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners, EventMessage.EventType.ADD_NOTNULLCONSTRAINT,
              new AddNotNullConstraintEvent(constraints.getNotNullConstraints(), true, handler), envContext);
        }
        if (CollectionUtils.isNotEmpty(constraints.getCheckConstraints())) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners, EventMessage.EventType.ADD_CHECKCONSTRAINT,
              new AddCheckConstraintEvent(constraints.getCheckConstraints(), true, handler), envContext);
        }
        if (CollectionUtils.isNotEmpty(constraints.getDefaultConstraints())) {
          MetaStoreListenerNotifier.notifyEvent(transactionalListeners, EventMessage.EventType.ADD_DEFAULTCONSTRAINT,
              new AddDefaultConstraintEvent(constraints.getDefaultConstraints(), true, handler), envContext);
        }
      }
      success = rs.commitTransaction();
    } finally {
      if (!success) {
        rs.rollbackTransaction();
        if (madeDir) {
          wh.deleteDir(tblPath, false, ReplChangeManager.shouldEnableCm(db, tbl));
        }
      }
    }
    return new CreateTableResult(success, transactionalListenerResponses);
  }

  private String getTableSuffix(Table tbl) {
    return tbl.isSetTxnId() && tbl.getParameters() != null
        && Boolean.parseBoolean(tbl.getParameters().get(SOFT_DELETE_TABLE)) ?
        SOFT_DELETE_PATH_SUFFIX + String.format(DELTA_DIGITS, tbl.getTxnId()) : "";
  }

  @Override
  protected void afterExecute(CreateTableResult result) throws TException, IOException {
    boolean success = result != null && result.success;
    List<MetaStoreEventListener> listeners = handler.getListeners();
    if (!listeners.isEmpty()) {
      boolean isReplicated = isDbReplicationTarget(db);
      MetaStoreListenerNotifier.notifyEvent(listeners, EventMessage.EventType.CREATE_TABLE,
          new CreateTableEvent(tbl, success, handler, isReplicated), envContext,
          result != null ? result.transactionalListenerResponses : Collections.emptyMap(), rs);
      if (CollectionUtils.isNotEmpty(constraints.getPrimaryKeys())) {
        MetaStoreListenerNotifier.notifyEvent(listeners, EventMessage.EventType.ADD_PRIMARYKEY,
            new AddPrimaryKeyEvent(constraints.getPrimaryKeys(), success, handler), envContext);
      }
      if (CollectionUtils.isNotEmpty(constraints.getForeignKeys())) {
        MetaStoreListenerNotifier.notifyEvent(listeners, EventMessage.EventType.ADD_FOREIGNKEY,
            new AddForeignKeyEvent(constraints.getForeignKeys(), success, handler), envContext);
      }
      if (CollectionUtils.isNotEmpty(constraints.getUniqueConstraints())) {
        MetaStoreListenerNotifier.notifyEvent(listeners, EventMessage.EventType.ADD_UNIQUECONSTRAINT,
            new AddUniqueConstraintEvent(constraints.getUniqueConstraints(), success, handler), envContext);
      }
      if (CollectionUtils.isNotEmpty(constraints.getNotNullConstraints())) {
        MetaStoreListenerNotifier.notifyEvent(listeners, EventMessage.EventType.ADD_NOTNULLCONSTRAINT,
            new AddNotNullConstraintEvent(constraints.getNotNullConstraints(), success, handler), envContext);
      }
      if (CollectionUtils.isNotEmpty(constraints.getDefaultConstraints())) {
        MetaStoreListenerNotifier.notifyEvent(listeners, EventMessage.EventType.ADD_DEFAULTCONSTRAINT,
            new AddDefaultConstraintEvent(constraints.getDefaultConstraints(), success, handler), envContext);
      }
      if (CollectionUtils.isNotEmpty(constraints.getCheckConstraints())) {
        MetaStoreListenerNotifier.notifyEvent(listeners, EventMessage.EventType.ADD_CHECKCONSTRAINT,
            new AddCheckConstraintEvent(constraints.getCheckConstraints(), success, handler), envContext);
      }
    }

    if (success && colStats != null) {
      // If the table has column statistics, update it into the metastore. We need a valid
      // writeId list to update column statistics for a transactional table. But during bootstrap
      // replication, where we use this feature, we do not have a valid writeId list which was
      // used to update the stats. But we know for sure that the writeId associated with the
      // stats was valid then (otherwise stats update would have failed on the source). So, craft
      // a valid transaction list with only that writeId and use it to update the stats.
      long writeId = tbl.getWriteId();
      String validWriteIds = null;
      if (writeId > 0) {
        ValidWriteIdList validWriteIdList =
            new ValidReaderWriteIdList(TableName.getDbTable(tbl.getDbName(),
                tbl.getTableName()),
                new long[0], new BitSet(), writeId);
        validWriteIds = validWriteIdList.toString();
      }
      updateTableColumnStatsInternal(colStats, validWriteIds, tbl.getWriteId());
    }
  }

  private boolean updateTableColumnStatsInternal(ColumnStatistics colStats,
      String validWriteIds, long writeId)
      throws NoSuchObjectException, MetaException, InvalidObjectException, InvalidInputException {
    normalizeColStatsInput(colStats);
    Map<String, String> parameters = null;
    rs.openTransaction();
    boolean committed = false;
    try {
      parameters = rs.updateTableColumnStatistics(colStats, validWriteIds, writeId);
      if (parameters != null) {
        Table tableObj = rs.getTable(colStats.getStatsDesc().getCatName(),
            colStats.getStatsDesc().getDbName(),
            colStats.getStatsDesc().getTableName(), validWriteIds);
        if (!handler.getTransactionalListeners().isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(handler.getTransactionalListeners(),
              EventMessage.EventType.UPDATE_TABLE_COLUMN_STAT,
              new UpdateTableColumnStatEvent(colStats, tableObj, parameters,
                  writeId, handler));
        }
        if (!handler.getListeners().isEmpty()) {
          MetaStoreListenerNotifier.notifyEvent(handler.getListeners(),
              EventMessage.EventType.UPDATE_TABLE_COLUMN_STAT,
              new UpdateTableColumnStatEvent(colStats, tableObj, parameters,
                  writeId, handler));
        }
      }
      committed = rs.commitTransaction();
    } finally {
      if (!committed) {
        rs.rollbackTransaction();
      }
    }

    return parameters != null;
  }

  private void normalizeColStatsInput(ColumnStatistics colStats) throws MetaException {
    // TODO: is this really needed? this code is propagated from HIVE-1362 but most of it is useless.
    ColumnStatisticsDesc statsDesc = colStats.getStatsDesc();
    statsDesc.setCatName(statsDesc.isSetCatName() ? statsDesc.getCatName().toLowerCase() :
        getDefaultCatalog(handler.getConf()));
    statsDesc.setDbName(statsDesc.getDbName().toLowerCase());
    statsDesc.setTableName(statsDesc.getTableName().toLowerCase());
    statsDesc.setPartName(statsDesc.getPartName());
    long time = System.currentTimeMillis() / 1000;
    statsDesc.setLastAnalyzed(time);

    for (ColumnStatisticsObj statsObj : colStats.getStatsObj()) {
      statsObj.setColName(statsObj.getColName().toLowerCase());
      statsObj.setColType(statsObj.getColType().toLowerCase());
    }
    colStats.setStatsDesc(statsDesc);
    colStats.setStatsObj(colStats.getStatsObj());
  }

  @Override
  protected void beforeExecute() throws TException, IOException {
    this.tbl = request.getTable();
    this.rs = handler.getMS();
    this.wh = handler.getWh();
    this.envContext = request.getEnvContext();
    // To preserve backward compatibility throw MetaException in case of null database
    if (tbl.getDbName() == null) {
      throw new MetaException("Null database name is not allowed");
    }
    if (!MetaStoreUtils.validateName(tbl.getTableName(), handler.getConf())) {
      throw new InvalidObjectException(tbl.getTableName()
          + " is not a valid object name");
    }
    if (!MetaStoreUtils.validateTblStorage(tbl.getSd())) {
      throw new InvalidObjectException(tbl.getTableName()
          + " location must not be root path");
    }
    if (!tbl.isSetCatName()) {
      tbl.setCatName(getDefaultCatalog(handler.getConf()));
    }

    this.db = rs.getDatabase(tbl.getCatName(), tbl.getDbName());
    if (MetaStoreUtils.isDatabaseRemote(db)) {
      // HIVE-24425: Create table in REMOTE db should fail
      throw new MetaException("Create table in REMOTE database " + db.getName() + " is not allowed");
    }

    if (rs.getTable(tbl.getCatName(), tbl.getDbName(), tbl.getTableName()) != null) {
      throw new AlreadyExistsException("Table " + getCatalogQualifiedTableName(tbl)
          + " already exists");
    }

    tbl.setDbName(normalizeIdentifier(tbl.getDbName()));
    tbl.setTableName(normalizeIdentifier(tbl.getTableName()));
    IMetaStoreMetadataTransformer transformer = handler.getMetadataTransformer();
    if (transformer != null) {
      tbl = transformer.transformCreateTable(tbl,
          request.getProcessorCapabilities(), request.getProcessorIdentifier());
    }

    Map<String, String> params = tbl.getParameters();
    if (params != null) {
      params.remove(TABLE_IS_CTAS);
      params.remove(TABLE_IS_CTLT);
      if (MetaStoreServerUtils.getBooleanEnvProp(request.getEnvContext(), CTAS_LEGACY_CONFIG) &&
          TableType.MANAGED_TABLE.toString().equals(tbl.getTableType())) {
        params.put("EXTERNAL", "TRUE");
        tbl.setTableType(TableType.EXTERNAL_TABLE.toString());
      }
    }

    String validate = MetaStoreServerUtils.validateTblColumns(tbl.getSd().getCols());
    if (validate != null) {
      throw new InvalidObjectException("Invalid column " + validate);
    }
    if (tbl.getPartitionKeys() != null) {
      validate = MetaStoreServerUtils.validateTblColumns(tbl.getPartitionKeys());
      if (validate != null) {
        throw new InvalidObjectException("Invalid partition column " + validate);
      }
    }
    if (tbl.isSetId()) {
      LOG.debug("Id shouldn't be set but table {}.{} has the Id set to {}. Id is ignored.", tbl.getDbName(),
          tbl.getTableName(), tbl.getId());
      tbl.unsetId();
    }
    SkewedInfo skew = tbl.getSd().getSkewedInfo();
    if (skew != null) {
      validate = MetaStoreServerUtils.validateSkewedColNames(skew.getSkewedColNames());
      if (validate != null) {
        throw new InvalidObjectException("Invalid skew column " + validate);
      }
      validate = MetaStoreServerUtils.validateSkewedColNamesSubsetCol(
          skew.getSkewedColNames(), tbl.getSd().getCols());
      if (validate != null) {
        throw new InvalidObjectException("Invalid skew column " + validate);
      }
    }

    colStats = tbl.getColStats();
    tbl.unsetColStats();

    constraints = new SQLAllTableConstraints();
    constraints.setPrimaryKeys(request.getPrimaryKeys());
    constraints.setForeignKeys(request.getForeignKeys());
    constraints.setUniqueConstraints(request.getUniqueConstraints());
    constraints.setDefaultConstraints(request.getDefaultConstraints());
    constraints.setCheckConstraints(request.getCheckConstraints());
    constraints.setNotNullConstraints(request.getNotNullConstraints());

    String catName = tbl.getCatName();
    // Check that constraints have catalog name properly set first
    if (CollectionUtils.isNotEmpty(constraints.getPrimaryKeys())
        && !constraints.getPrimaryKeys().get(0).isSetCatName()) {
      constraints.getPrimaryKeys().forEach(constraint -> constraint.setCatName(catName));
    }
    if (CollectionUtils.isNotEmpty(constraints.getForeignKeys())
        && !constraints.getForeignKeys().get(0).isSetCatName()) {
      constraints.getForeignKeys().forEach(constraint -> constraint.setCatName(catName));
    }
    if (CollectionUtils.isNotEmpty(constraints.getUniqueConstraints())
        && !constraints.getUniqueConstraints().get(0).isSetCatName()) {
      constraints.getUniqueConstraints().forEach(constraint -> constraint.setCatName(catName));
    }
    if (CollectionUtils.isNotEmpty(constraints.getNotNullConstraints())
        && !constraints.getNotNullConstraints().get(0).isSetCatName()) {
      constraints.getNotNullConstraints().forEach(constraint -> constraint.setCatName(catName));
    }
    if (CollectionUtils.isNotEmpty(constraints.getDefaultConstraints())
        && !constraints.getDefaultConstraints().get(0).isSetCatName()) {
      constraints.getDefaultConstraints().forEach(constraint -> constraint.setCatName(catName));
    }
    if (CollectionUtils.isNotEmpty(constraints.getCheckConstraints())
        && !constraints.getCheckConstraints().get(0).isSetCatName()) {
      constraints.getCheckConstraints().forEach(constraint -> constraint.setCatName(catName));
    }
  }

  @Override
  protected String getMessagePrefix() {
    return "CreateTableHandler [" + id + "] -  create table for " +
        TableName.getQualified(tbl.getCatName(), tbl.getDbName(), tbl.getTableName()) + ":";
  }

  @Override
  protected String getRequestProgress() {
    return "Creating table";
  }

  public record CreateTableResult(boolean success, Map<String, String> transactionalListenerResponses)
      implements Result {

  }
}
