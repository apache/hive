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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore.handler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.AcidConstants;
import org.apache.hadoop.hive.common.AcidMetaDataFile;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.HMSHandler;
import org.apache.hadoop.hive.metastore.IHMSHandler;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.RawStore;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TruncateTableRequest;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.PreAlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.PreAlterTableEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionsEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.metastore.PartitionsIterable;
import org.apache.hadoop.hive.metastore.metastore.iface.TableStore;
import org.apache.hadoop.hive.metastore.txn.TxnUtils;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.HdfsUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.thrift.TException;

import static org.apache.hadoop.hive.metastore.ExceptionHandler.newMetaException;
import static org.apache.hadoop.hive.metastore.client.ThriftHiveMetaStoreClient.TRUNCATE_SKIP_DATA_DELETION;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.isDbReplicationTarget;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.CAT_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.DB_NAME;
import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.parseDbName;

@RequestHandler(requestBody = TruncateTableRequest.class)
public class TruncateTableHandler
    extends AbstractRequestHandler<TruncateTableRequest, TruncateTableHandler.TruncateTableResult>  {
  private String catName;
  private String dbName;
  private Warehouse wh;
  private RawStore ms;
  private Table table;
  private List<String> reqPartNames;
  private Table newTable;
  private boolean isPartitioned;

  TruncateTableHandler(IHMSHandler handler, TruncateTableRequest request) {
    super(handler, false, request);
  }

  @Override
  protected void beforeExecute() throws TException, IOException {
    String[] parsedDbName = parseDbName(request.getDbName(), handler.getConf());
    this.catName = parsedDbName[CAT_NAME];
    this.dbName = parsedDbName[DB_NAME];
    GetTableRequest getTableRequest = new GetTableRequest(dbName, request.getTableName());
    getTableRequest.setCatName(catName);
    this.table = handler.get_table_core(getTableRequest);
    this.isPartitioned = 0 != table.getPartitionKeysSize();

    this.ms = handler.getMS();
    if (isPartitioned) {
      this.reqPartNames = request.getPartNames();
    }
    this.wh = handler.getWh();

    if (!isPartitioned) {
      newTable = table.deepCopy();
      updateStatsForTruncate(newTable.getParameters(), new EnvironmentContext());
      // TODO: this should actually pass thru and set writeId for txn stats.
      if (request.getWriteId() > 0) {
        newTable.setWriteId(request.getWriteId());
      }
      ((HMSHandler) handler).firePreEvent(new PreAlterTableEvent(table, newTable, handler));
    }
  }

  @Override
  protected TruncateTableResult execute() throws TException, IOException {
    boolean isSkipTrash = false, needCmRecycle = false;
    boolean skipDataDeletion = Optional.ofNullable(request.getEnvironmentContext())
        .map(EnvironmentContext::getProperties)
        .map(prop -> prop.get(TRUNCATE_SKIP_DATA_DELETION))
        .map(Boolean::parseBoolean)
        .orElse(false);

    boolean success;
    if (isPartitioned) {
      success = truncatePartitionedTable(isSkipTrash, needCmRecycle, skipDataDeletion);
    } else {
      if (TxnUtils.isTransactionalTable(table) || !skipDataDeletion) {
        if (!skipDataDeletion) {
          isSkipTrash = MetaStoreUtils.isSkipTrash(table.getParameters());
          Database db = handler.get_database_core(catName, dbName);
          needCmRecycle = ReplChangeManager.shouldEnableCm(db, table);
        }
        Path location = new Path(table.getSd().getLocation());
        if (!skipDataDeletion) {
          truncateDataFiles(location, isSkipTrash, needCmRecycle);
        } else {
          addTruncateBaseFile(location, request.getWriteId(),
              handler.getConf(), AcidMetaDataFile.DataFormat.TRUNCATED);
        }
      }
      success = alterTableStatsForTruncate();
    }
    return new TruncateTableResult(success);
  }

  private PartitionsIterable newPartitionBatches(int batchSize) throws MetaException {
    TableName qualifiedTable = new TableName(catName, dbName, request.getTableName());
    if (reqPartNames != null) {
      return PartitionsIterable.create(
          ms.unwrap(TableStore.class), qualifiedTable, batchSize, reqPartNames);
    }
    return PartitionsIterable.create(ms, qualifiedTable, batchSize);
  }

  private boolean truncatePartitionedTable(boolean isSkipTrash, boolean needCmRecycle, boolean skipDataDeletion)
      throws TException, IOException {
    if (reqPartNames != null && reqPartNames.isEmpty()) {
      return true;
    }
    int batchSize = MetastoreConf.getIntVar(handler.getConf(),
        MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX);
    PartitionsIterable partitionBatches = newPartitionBatches(batchSize);

    boolean truncatePartitionData = TxnUtils.isTransactionalTable(table) || !skipDataDeletion;
    if (truncatePartitionData && !skipDataDeletion) {
      isSkipTrash = MetaStoreUtils.isSkipTrash(table.getParameters());
      Database db = handler.get_database_core(catName, dbName);
      needCmRecycle = ReplChangeManager.shouldEnableCm(db, table);
    }

    boolean success = true;
    for (List<Partition> partitions : partitionBatches) {
      if (partitions.isEmpty()) {
        continue;
      }
      EnvironmentContext environmentContext = new EnvironmentContext();
      List<Partition> newPartitions = buildTruncatedPartitions(partitions, environmentContext);
      if (truncatePartitionData) {
        truncatePartitions(partitions, isSkipTrash, needCmRecycle, skipDataDeletion);
      }
      if (!alterPartitions(partitions, newPartitions, environmentContext)) {
        success = false;
        break;
      }
    }
    return success;
  }

  private void truncatePartitions(List<Partition> partitions, boolean isSkipTrash,
      boolean needCmRecycle, boolean skipDataDeletion) throws IOException, MetaException {
    for (Partition partition : partitions) {
      Path location = new Path(partition.getSd().getLocation());
      if (!skipDataDeletion) {
        truncateDataFiles(location, isSkipTrash, needCmRecycle);
      } else {
        addTruncateBaseFile(location, request.getWriteId(),
            handler.getConf(), AcidMetaDataFile.DataFormat.TRUNCATED);
      }
    }
  }

  private List<Partition> buildTruncatedPartitions(List<Partition> partitions,
      EnvironmentContext environmentContext) throws TException {
    List<Partition> newPartitions = new ArrayList<>(partitions.size());
    for (Partition partition : partitions) {
      Partition newPart = partition.deepCopy();
      updateStatsForTruncate(newPart.getParameters(), environmentContext);
      if (request.getWriteId() > 0) {
        newPart.setWriteId(request.getWriteId());
      }
      newPart.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(System
          .currentTimeMillis() / 1000));
      ((HMSHandler) handler).firePreEvent(new PreAlterPartitionEvent(dbName, request.getTableName(), table,
          partition.getValues(), newPart, handler));
      newPartitions.add(newPart);
    }
    return newPartitions;
  }

  private boolean alterPartitions(List<Partition> partitions, List<Partition> newPartitions,
      EnvironmentContext environmentContext) throws TException {
    boolean success = false;
    ms.openTransaction();
    try {
      List<List<String>> partValsList = new ArrayList<>(newPartitions.size());
      for (Partition newPartition : newPartitions) {
        partValsList.add(newPartition.getValues());
      }
      ms.alterPartitions(catName, dbName, request.getTableName(), partValsList, newPartitions,
          request.getWriteId(), request.getValidWriteIdList());
      notifyTruncatePartListeners(handler.getTransactionalListeners(),
          partitions, newPartitions, true, environmentContext);
      success = ms.commitTransaction();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }
      notifyTruncatePartListeners(handler.getListeners(), partitions, newPartitions, success, environmentContext);
    }
    return success;
  }

  private void notifyTruncatePartListeners(List<? extends MetaStoreEventListener> listeners,
      List<Partition> oldPartitions,
      List<Partition> newPartitions, boolean success,
      EnvironmentContext environmentContext) throws TException {
    if (listeners.isEmpty() || oldPartitions.isEmpty()) {
      return;
    }
    boolean mergeEvents = MetastoreConf.getBoolVar(handler.getConf(),
        MetastoreConf.ConfVars.NOTIFICATION_ALTER_PARTITIONS_V2_ENABLED);
    if (mergeEvents) {
      MetaStoreListenerNotifier.notifyEvent(listeners,
          EventMessage.EventType.ALTER_PARTITIONS,
          new AlterPartitionsEvent(oldPartitions, newPartitions, table, true, success, handler),
          environmentContext);
    } else {
      for (int i = 0; i < oldPartitions.size(); i++) {
        Partition newPart = newPartitions.get(i);
        MetaStoreListenerNotifier.notifyEvent(listeners,
            EventMessage.EventType.ALTER_PARTITION,
            new AlterPartitionEvent(oldPartitions.get(i), newPart, table, true, success,
                newPart.getWriteId(), handler),
            environmentContext);
      }
    }
  }

  private void updateStatsForTruncate(Map<String,String> props, EnvironmentContext environmentContext) {
    if (null == props) {
      return;
    }
    for (String stat : StatsSetupConst.SUPPORTED_STATS) {
      String statVal = props.get(stat);
      if (statVal != null) {
        //In the case of truncate table, we set the stats to be 0.
        props.put(stat, "0");
      }
    }
    //first set basic stats to true
    StatsSetupConst.setBasicStatsState(props, StatsSetupConst.TRUE);
    environmentContext.putToProperties(StatsSetupConst.STATS_GENERATED, StatsSetupConst.TASK);
    environmentContext.putToProperties(StatsSetupConst.DO_NOT_POPULATE_QUICK_STATS, StatsSetupConst.TRUE);
    //then invalidate column stats
    StatsSetupConst.clearColumnStatsState(props);
  }

  private boolean alterTableStatsForTruncate() throws TException {
    boolean isReplicated = isDbReplicationTarget(ms.getDatabase(catName, dbName));
    boolean success = false;
    ms.openTransaction();
    try {
      if (!handler.getTransactionalListeners().isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(handler.getTransactionalListeners(), EventMessage.EventType.ALTER_TABLE,
            new AlterTableEvent(table, newTable, true, true, request.getWriteId(), handler, isReplicated));
      }
      ms.alterTable(catName, dbName, request.getTableName(), newTable, request.getValidWriteIdList());
      success = ms.commitTransaction();
    } finally {
      if (!success) {
        ms.rollbackTransaction();
      }
      MetaStoreListenerNotifier.notifyEvent(handler.getListeners(),
          EventMessage.EventType.ALTER_TABLE,
          new AlterTableEvent(table, newTable, true, success,
              request.getWriteId(), handler, isReplicated));
    }
    return success;
  }

  private void truncateDataFiles(Path location, boolean isSkipTrash, boolean needCmRecycle)
      throws IOException, MetaException {
    FileSystem fs = location.getFileSystem(handler.getConf());

    if (!HdfsUtils.isPathEncrypted(handler.getConf(), fs.getUri(), location) &&
        !FileUtils.pathHasSnapshotSubDir(location, fs)) {
      HdfsUtils.HadoopFileStatus status = new HdfsUtils.HadoopFileStatus(handler.getConf(), fs, location);
      FileStatus targetStatus = fs.getFileStatus(location);
      String targetGroup = targetStatus == null ? null : targetStatus.getGroup();

      wh.deleteDir(location, isSkipTrash, needCmRecycle);
      fs.mkdirs(location);
      HdfsUtils.setFullFileStatus(handler.getConf(), status, targetGroup, fs, location, false);
    } else {
      FileStatus[] statuses = fs.listStatus(location, FileUtils.HIDDEN_FILES_PATH_FILTER);
      if (statuses == null || statuses.length == 0) {
        return;
      }
      for (final FileStatus status : statuses) {
        wh.deleteDir(status.getPath(), isSkipTrash, needCmRecycle);
      }
    }
  }

  /**
   * Add an empty baseDir with a truncate metadatafile.
   * @param location partition or table directory
   * @param writeId allocated writeId
   * @throws MetaException
   */
  public static void addTruncateBaseFile(Path location, long writeId, Configuration conf,
      AcidMetaDataFile.DataFormat dataFormat) throws MetaException {
    if (location == null) {
      return;
    }

    Path basePath = new Path(location, AcidConstants.baseDir(writeId));
    try {
      FileSystem fs = location.getFileSystem(conf);
      fs.mkdirs(basePath);
      // We can not leave the folder empty, otherwise it will be skipped at some file listing in AcidUtils
      // No need for a data file, a simple metadata is enough
      AcidMetaDataFile.writeToFile(fs, basePath, dataFormat);
    } catch (Exception e) {
      throw newMetaException(e);
    }
  }

  @Override
  public String toString() {
    return "TruncateTableHandler [" + id + "] -  truncate table for " +
        TableName.getQualified(catName, dbName, table.getTableName()) + ":";
  }

  public record TruncateTableResult(boolean success) implements Result {

  }
}
