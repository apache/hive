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
import org.apache.hadoop.hive.metastore.IHMSHandler;
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
import org.apache.hadoop.hive.metastore.client.builder.GetPartitionsArgs;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionsEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
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
  private List<Partition> partitions;

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

    this.ms = handler.getMS();
    if (request.getPartNames() == null) {
      if (0 != table.getPartitionKeysSize()) {
        this.partitions = ms.getPartitions(catName, dbName,
            request.getTableName(), GetPartitionsArgs.getAllPartitions());
      }
    } else {
      this.partitions = ms.getPartitionsByNames(catName, dbName,
          request.getTableName(), request.getPartNames());
    }
    this.wh = handler.getWh();
  }

  @Override
  protected TruncateTableResult execute() throws TException, IOException {
    boolean isSkipTrash = false, needCmRecycle = false;
    boolean skipDataDeletion = Optional.ofNullable(request.getEnvironmentContext())
        .map(EnvironmentContext::getProperties)
        .map(prop -> prop.get(TRUNCATE_SKIP_DATA_DELETION))
        .map(Boolean::parseBoolean)
        .orElse(false);

    if (TxnUtils.isTransactionalTable(table) || !skipDataDeletion) {
      if (!skipDataDeletion) {
        isSkipTrash = MetaStoreUtils.isSkipTrash(table.getParameters());
        Database db = handler.get_database_core(catName, dbName);
        needCmRecycle = ReplChangeManager.shouldEnableCm(db, table);
      }
      List<Path> locations = new ArrayList<>();
      if (0 != table.getPartitionKeysSize()) {
        for (Partition partition : partitions) {
          locations.add(new Path(partition.getSd().getLocation()));
        }
      } else {
        locations.add(new Path(table.getSd().getLocation()));
      }
      // This is not transactional
      for (Path location : locations) {
        if (!skipDataDeletion) {
          truncateDataFiles(location, isSkipTrash, needCmRecycle);
        } else {
          // For Acid tables we don't need to delete the old files, only write an empty baseDir.
          // Compaction and cleaner will take care of the rest
          addTruncateBaseFile(location, request.getWriteId(),
              handler.getConf(), AcidMetaDataFile.DataFormat.TRUNCATED);
        }
      }
    }
    // Alter the table/partition stats and also notify truncate table event
    alterTableStatsForTruncate();
    return new TruncateTableResult(true);
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

  private void alterPartitionsForTruncate() throws TException {
    EnvironmentContext environmentContext = new EnvironmentContext();
    if (partitions.isEmpty()) {
      return;
    }
    List<List<String>> partValsList = new ArrayList<>();
    for (Partition partition: partitions) {
      updateStatsForTruncate(partition.getParameters(), environmentContext);
      if (request.getWriteId() > 0) {
        partition.setWriteId(request.getWriteId());
      }
      partition.putToParameters(hive_metastoreConstants.DDL_TIME, Long.toString(System
          .currentTimeMillis() / 1000));
      partValsList.add(partition.getValues());
    }
    ms.alterPartitions(catName, dbName, request.getTableName(), partValsList, partitions,
        request.getWriteId(), request.getValidWriteIdList());
    if (handler.getTransactionalListeners() != null && !handler.getTransactionalListeners().isEmpty()) {
      boolean shouldSendSingleEvent = MetastoreConf.getBoolVar(handler.getConf(),
          MetastoreConf.ConfVars.NOTIFICATION_ALTER_PARTITIONS_V2_ENABLED);
      if (shouldSendSingleEvent) {
        MetaStoreListenerNotifier.notifyEvent(handler.getTransactionalListeners(),
            EventMessage.EventType.ALTER_PARTITIONS,
            new AlterPartitionsEvent(partitions, partitions, table, true, true, handler), environmentContext);
      } else {
        for (Partition partition : partitions) {
          MetaStoreListenerNotifier.notifyEvent(handler.getTransactionalListeners(),
              EventMessage.EventType.ALTER_PARTITION,
              new AlterPartitionEvent(partition, partition, table, true, true, partition.getWriteId(), handler),
              environmentContext);
        }
      }
    }
    if (handler.getListeners() != null && !handler.getListeners().isEmpty()) {
      boolean shouldSendSingleEvent = MetastoreConf.getBoolVar(handler.getConf(),
          MetastoreConf.ConfVars.NOTIFICATION_ALTER_PARTITIONS_V2_ENABLED);
      if (shouldSendSingleEvent) {
        MetaStoreListenerNotifier.notifyEvent(handler.getListeners(), EventMessage.EventType.ALTER_PARTITIONS,
            new AlterPartitionsEvent(partitions, partitions, table, true, true, handler), environmentContext);
      } else {
        for (Partition partition : partitions) {
          MetaStoreListenerNotifier.notifyEvent(handler.getListeners(), EventMessage.EventType.ALTER_PARTITION,
              new AlterPartitionEvent(partition, partition, table, true, true, partition.getWriteId(), handler),
              environmentContext);
        }
      }
    }
  }

  private void alterTableStatsForTruncate() throws TException{
    if (0 != table.getPartitionKeysSize()) {
      alterPartitionsForTruncate();
    } else {
      EnvironmentContext environmentContext = new EnvironmentContext();
      updateStatsForTruncate(table.getParameters(), environmentContext);
      boolean isReplicated = isDbReplicationTarget(ms.getDatabase(catName, dbName));
      if (!handler.getTransactionalListeners().isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(handler.getTransactionalListeners(),
            EventMessage.EventType.ALTER_TABLE,
            new AlterTableEvent(table, table, true, true,
                request.getWriteId(), handler, isReplicated));
      }

      if (!handler.getListeners().isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(handler.getListeners(),
            EventMessage.EventType.ALTER_TABLE,
            new AlterTableEvent(table, table, true, true,
                request.getWriteId(), handler, isReplicated));
      }
      // TODO: this should actually pass thru and set writeId for txn stats.
      if (request.getWriteId() > 0) {
        table.setWriteId(request.getWriteId());
      }
      ms.alterTable(catName, dbName, request.getTableName(), table,
          request.getValidWriteIdList());
    }
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
  protected String getMessagePrefix() {
    return "TruncateTableHandler [" + id + "] -  truncate table for " +
        TableName.getQualified(catName, dbName, table.getTableName()) + ":";
  }

  public record TruncateTableResult(boolean success) implements Result {

  }
}
