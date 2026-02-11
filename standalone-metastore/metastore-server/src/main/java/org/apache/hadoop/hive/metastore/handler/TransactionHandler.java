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

import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.MetaStoreListenerNotifier;
import org.apache.hadoop.hive.metastore.ReplChangeManager;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AbortCompactResponse;
import org.apache.hadoop.hive.metastore.api.AbortCompactionRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.ClientCapabilities;
import org.apache.hadoop.hive.metastore.api.ClientCapability;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionInfoStruct;
import org.apache.hadoop.hive.metastore.api.CompactionMetricsDataRequest;
import org.apache.hadoop.hive.metastore.api.CompactionMetricsDataStruct;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.FindNextCompactRequest;
import org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoRequest;
import org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsRequest;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.InsertEventRequestData;
import org.apache.hadoop.hive.metastore.api.LockMaterializationRebuildRequest;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MaxAllocatedTableWriteIdRequest;
import org.apache.hadoop.hive.metastore.api.MaxAllocatedTableWriteIdResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.OptionalCompactionInfoStruct;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ReplTblWriteIdStateRequest;
import org.apache.hadoop.hive.metastore.api.ReplayedTxnsForPolicyResult;
import org.apache.hadoop.hive.metastore.api.SeedTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.SeedTxnIdRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.api.UpdateTransactionalStatsRequest;
import org.apache.hadoop.hive.metastore.api.WriteEventInfo;
import org.apache.hadoop.hive.metastore.api.WriteNotificationLogBatchRequest;
import org.apache.hadoop.hive.metastore.api.WriteNotificationLogBatchResponse;
import org.apache.hadoop.hive.metastore.api.WriteNotificationLogRequest;
import org.apache.hadoop.hive.metastore.api.WriteNotificationLogResponse;
import org.apache.hadoop.hive.metastore.events.AbortTxnEvent;
import org.apache.hadoop.hive.metastore.events.AcidWriteEvent;
import org.apache.hadoop.hive.metastore.events.AllocWriteIdEvent;
import org.apache.hadoop.hive.metastore.events.BatchAcidWriteEvent;
import org.apache.hadoop.hive.metastore.events.CommitCompactionEvent;
import org.apache.hadoop.hive.metastore.events.CommitTxnEvent;
import org.apache.hadoop.hive.metastore.events.OpenTxnEvent;
import org.apache.hadoop.hive.metastore.messaging.EventMessage;
import org.apache.hadoop.hive.metastore.txn.CompactionMetricsDataConverter;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.FilterUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.getDefaultCatalog;

// Collect all Transaction and locking methods together
public abstract class TransactionHandler extends BaseHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TransactionHandler.class);

  protected TransactionHandler(String name, Configuration conf) {
    super(name, conf);
  }

  @Override
  public GetOpenTxnsResponse get_open_txns() throws TException {
    return getTxnHandler().getOpenTxns();
  }

  @Override
  public GetOpenTxnsResponse get_open_txns_req(GetOpenTxnsRequest getOpenTxnsRequest) throws TException {
    return getTxnHandler().getOpenTxns(getOpenTxnsRequest.getExcludeTxnTypes());
  }

  // Transaction and locking methods
  @Override
  public GetOpenTxnsInfoResponse get_open_txns_info() throws TException {
    return getTxnHandler().getOpenTxnsInfo();
  }

  @Override
  public OpenTxnsResponse open_txns(OpenTxnRequest rqst) throws TException {
    OpenTxnsResponse response = getTxnHandler().openTxns(rqst);
    List<Long> txnIds = response.getTxn_ids();
    boolean isHiveReplTxn = rqst.isSetReplPolicy() && TxnType.DEFAULT.equals(rqst.getTxn_type());
    if (txnIds != null && listeners != null && !listeners.isEmpty() && !isHiveReplTxn) {
      MetaStoreListenerNotifier.notifyEvent(listeners, EventMessage.EventType.OPEN_TXN,
          new OpenTxnEvent(txnIds, this));
    }
    return response;
  }

  @Override
  public void abort_txn(AbortTxnRequest rqst) throws TException {
    getTxnHandler().abortTxn(rqst);
    boolean isHiveReplTxn = rqst.isSetReplPolicy() && TxnType.DEFAULT.equals(rqst.getTxn_type());
    if (listeners != null && !listeners.isEmpty() && !isHiveReplTxn) {
      // Not adding dbsUpdated to AbortTxnEvent because
      // only DbNotificationListener cares about it, and this is already
      // handled with transactional listeners in TxnHandler.
      MetaStoreListenerNotifier.notifyEvent(listeners, EventMessage.EventType.ABORT_TXN,
          new AbortTxnEvent(rqst.getTxnid(), this));
    }
  }

  @Override
  public void abort_txns(AbortTxnsRequest rqst) throws TException {
    getTxnHandler().abortTxns(rqst);
    if (listeners != null && !listeners.isEmpty()) {
      for (Long txnId : rqst.getTxn_ids()) {
        // See above abort_txn() note about not adding dbsUpdated.
        MetaStoreListenerNotifier.notifyEvent(listeners, EventMessage.EventType.ABORT_TXN,
            new AbortTxnEvent(txnId, this));
      }
    }
  }

  @Override
  public AbortCompactResponse abort_Compactions(AbortCompactionRequest rqst) throws TException {
    return getTxnHandler().abortCompactions(rqst);
  }

  @Override
  public long get_latest_txnid_in_conflict(long txnId) throws MetaException {
    return getTxnHandler().getLatestTxnIdInConflict(txnId);
  }

  @Override
  public void commit_txn(CommitTxnRequest rqst) throws TException {
    boolean isReplayedReplTxn = TxnType.REPL_CREATED.equals(rqst.getTxn_type());
    boolean isHiveReplTxn = rqst.isSetReplPolicy() && TxnType.DEFAULT.equals(rqst.getTxn_type());
    // in replication flow, the write notification log table will be updated here.
    if (rqst.isSetWriteEventInfos() && isReplayedReplTxn) {
      assert (rqst.isSetReplPolicy());
      long targetTxnId = getTxnHandler().getTargetTxnId(rqst.getReplPolicy(), rqst.getTxnid());
      if (targetTxnId < 0) {
        //looks like a retry
        return;
      }
      for (WriteEventInfo writeEventInfo : rqst.getWriteEventInfos()) {
        String[] filesAdded = ReplChangeManager.getListFromSeparatedString(writeEventInfo.getFiles());
        List<String> partitionValue = null;
        Partition ptnObj = null;
        String root;
        Table tbl = getTblObject(writeEventInfo.getDatabase(), writeEventInfo.getTable(), null);

        if (writeEventInfo.getPartition() != null && !writeEventInfo.getPartition().isEmpty()) {
          partitionValue = Warehouse.getPartValuesFromPartName(writeEventInfo.getPartition());
          ptnObj = getPartitionObj(writeEventInfo.getDatabase(), writeEventInfo.getTable(), partitionValue, tbl);
          root = ptnObj.getSd().getLocation();
        } else {
          root = tbl.getSd().getLocation();
        }

        InsertEventRequestData insertData = new InsertEventRequestData();
        insertData.setReplace(true);

        // The files in the commit txn message during load will have files with path corresponding to source
        // warehouse. Need to transform them to target warehouse using table or partition object location.
        for (String file : filesAdded) {
          String[] decodedPath = ReplChangeManager.decodeFileUri(file);
          String name = (new Path(decodedPath[0])).getName();
          Path newPath = FileUtils.getTransformedPath(name, decodedPath[3], root);
          insertData.addToFilesAdded(newPath.toUri().toString());
          insertData.addToSubDirectoryList(decodedPath[3]);
          try {
            insertData.addToFilesAddedChecksum(ReplChangeManager.checksumFor(newPath, newPath.getFileSystem(conf)));
          } catch (IOException e) {
            LOG.error("failed to get checksum for the file " + newPath + " with error: " + e.getMessage());
            throw new TException(e.getMessage());
          }
        }

        WriteNotificationLogRequest wnRqst = new WriteNotificationLogRequest(targetTxnId,
            writeEventInfo.getWriteId(), writeEventInfo.getDatabase(), writeEventInfo.getTable(), insertData);
        if (partitionValue != null) {
          wnRqst.setPartitionVals(partitionValue);
        }
        addTxnWriteNotificationLog(tbl, ptnObj, wnRqst);
      }
    }
    getTxnHandler().commitTxn(rqst);
    if (listeners != null && !listeners.isEmpty() && !isHiveReplTxn) {
      MetaStoreListenerNotifier.notifyEvent(listeners, EventMessage.EventType.COMMIT_TXN,
          new CommitTxnEvent(rqst.getTxnid(), this));
      Optional<CompactionInfo> compactionInfo = getTxnHandler().getCompactionByTxnId(rqst.getTxnid());
      if (compactionInfo.isPresent()) {
        MetaStoreListenerNotifier.notifyEvent(listeners, EventMessage.EventType.COMMIT_COMPACTION,
            new CommitCompactionEvent(rqst.getTxnid(), compactionInfo.get(), this));
      }
    }
  }

  @Override
  public void repl_tbl_writeid_state(ReplTblWriteIdStateRequest rqst) throws TException {
    getTxnHandler().replTableWriteIdState(rqst);
  }

  @Override
  public GetValidWriteIdsResponse get_valid_write_ids(GetValidWriteIdsRequest rqst) throws TException {
    return getTxnHandler().getValidWriteIds(rqst);
  }

  @Override
  public void add_write_ids_to_min_history(long txnId, Map<String, Long> validWriteIds) throws TException {
    getTxnHandler().addWriteIdsToMinHistory(txnId, validWriteIds);
  }

  @Override
  public void set_hadoop_jobid(String jobId, long cqId) throws MetaException {
    getTxnHandler().setHadoopJobId(jobId, cqId);
  }

  @Deprecated
  @Override
  public OptionalCompactionInfoStruct find_next_compact(String workerId) throws MetaException{
    return CompactionInfo.compactionInfoToOptionalStruct(
        getTxnHandler().findNextToCompact(workerId));
  }

  @Override
  public OptionalCompactionInfoStruct find_next_compact2(FindNextCompactRequest rqst) throws MetaException{
    return CompactionInfo.compactionInfoToOptionalStruct(
        getTxnHandler().findNextToCompact(rqst));
  }

  @Override
  public void mark_cleaned(CompactionInfoStruct cr) throws MetaException {
    getTxnHandler().markCleaned(CompactionInfo.compactionStructToInfo(cr));
  }

  @Override
  public void mark_compacted(CompactionInfoStruct cr) throws MetaException {
    getTxnHandler().markCompacted(CompactionInfo.compactionStructToInfo(cr));
  }

  @Override
  public void mark_failed(CompactionInfoStruct cr) throws MetaException {
    getTxnHandler().markFailed(CompactionInfo.compactionStructToInfo(cr));
  }

  @Override
  public void mark_refused(CompactionInfoStruct cr) throws MetaException {
    getTxnHandler().markRefused(CompactionInfo.compactionStructToInfo(cr));
  }

  @Override
  public boolean update_compaction_metrics_data(CompactionMetricsDataStruct struct) throws MetaException, TException {
    return getTxnHandler().updateCompactionMetricsData(CompactionMetricsDataConverter.structToData(struct));
  }

  @Override
  public void remove_compaction_metrics_data(CompactionMetricsDataRequest request)
      throws MetaException, TException {
    getTxnHandler().removeCompactionMetricsData(request.getDbName(), request.getTblName(), request.getPartitionName(),
        CompactionMetricsDataConverter.thriftCompactionMetricType2DbType(request.getType()));
  }

  @Override
  public List<String> find_columns_with_stats(CompactionInfoStruct cr) throws MetaException {
    return getTxnHandler().findColumnsWithStats(CompactionInfo.compactionStructToInfo(cr));
  }

  @Override
  public void update_compactor_state(CompactionInfoStruct cr, long highWaterMark) throws MetaException {
    getTxnHandler().updateCompactorState(
        CompactionInfo.compactionStructToInfo(cr), highWaterMark);
  }

  @Override
  public GetLatestCommittedCompactionInfoResponse get_latest_committed_compaction_info(
      GetLatestCommittedCompactionInfoRequest rqst) throws MetaException {
    if (rqst.getDbname() == null || rqst.getTablename() == null) {
      throw new MetaException("Database name and table name cannot be null.");
    }
    GetLatestCommittedCompactionInfoResponse response = getTxnHandler().getLatestCommittedCompactionInfo(rqst);
    return FilterUtils.filterCommittedCompactionInfoStructIfEnabled(isServerFilterEnabled, filterHook,
        getDefaultCatalog(conf), rqst.getDbname(), rqst.getTablename(), response);
  }

  @Override
  public AllocateTableWriteIdsResponse allocate_table_write_ids(
      AllocateTableWriteIdsRequest rqst) throws TException {
    AllocateTableWriteIdsResponse response = getTxnHandler().allocateTableWriteIds(rqst);
    if (listeners != null && !listeners.isEmpty()) {
      MetaStoreListenerNotifier.notifyEvent(listeners, EventMessage.EventType.ALLOC_WRITE_ID,
          new AllocWriteIdEvent(response.getTxnToWriteIds(), rqst.getDbName(),
              rqst.getTableName(), this));
    }
    return response;
  }

  @Override
  public MaxAllocatedTableWriteIdResponse get_max_allocated_table_write_id(MaxAllocatedTableWriteIdRequest rqst)
      throws MetaException {
    return getTxnHandler().getMaxAllocatedTableWriteId(rqst);
  }

  @Override
  public void seed_write_id(SeedTableWriteIdsRequest rqst) throws MetaException {
    getTxnHandler().seedWriteId(rqst);
  }

  @Override
  public void seed_txn_id(SeedTxnIdRequest rqst) throws MetaException {
    getTxnHandler().seedTxnId(rqst);
  }

  private void addTxnWriteNotificationLog(Table tableObj, Partition ptnObj, WriteNotificationLogRequest rqst)
      throws MetaException {
    String partition = ""; //Empty string is an invalid partition name. Can be used for non partitioned table.
    if (ptnObj != null) {
      partition = Warehouse.makePartName(tableObj.getPartitionKeys(), rqst.getPartitionVals());
    }
    AcidWriteEvent event = new AcidWriteEvent(partition, tableObj, ptnObj, rqst);
    getTxnHandler().addWriteNotificationLog(event);
    if (listeners != null && !listeners.isEmpty()) {
      MetaStoreListenerNotifier.notifyEvent(listeners, EventMessage.EventType.ACID_WRITE, event);
    }
  }

  private Table getTblObject(String db, String table, String catalog) throws TException {
    GetTableRequest req = new GetTableRequest(db, table);
    if (catalog != null) {
      req.setCatName(catalog);
    }
    req.setCapabilities(new ClientCapabilities(Lists.newArrayList(ClientCapability.TEST_CAPABILITY, ClientCapability.INSERT_ONLY_TABLES)));
    return get_table_req(req).getTable();
  }

  private Partition getPartitionObj(String db, String table, List<String> partitionVals, Table tableObj)
      throws TException {
    if (tableObj.isSetPartitionKeys() && !tableObj.getPartitionKeys().isEmpty()) {
      return get_partition(db, table, partitionVals);
    }
    return null;
  }

  @Override
  public WriteNotificationLogResponse add_write_notification_log(WriteNotificationLogRequest rqst)
      throws TException {
    Table tableObj = getTblObject(rqst.getDb(), rqst.getTable(), null);
    Partition ptnObj = getPartitionObj(rqst.getDb(), rqst.getTable(), rqst.getPartitionVals(), tableObj);
    addTxnWriteNotificationLog(tableObj, ptnObj, rqst);
    return new WriteNotificationLogResponse();
  }

  @Override
  public WriteNotificationLogBatchResponse add_write_notification_log_in_batch(
      WriteNotificationLogBatchRequest batchRequest) throws TException {
    if (batchRequest.getRequestList().size() == 0) {
      return new WriteNotificationLogBatchResponse();
    }

    Table tableObj = getTblObject(batchRequest.getDb(), batchRequest.getTable(), batchRequest.getCatalog());
    BatchAcidWriteEvent event = new BatchAcidWriteEvent();
    List<String> partNameList = new ArrayList<>();
    List<Partition> ptnObjList;

    Map<String, WriteNotificationLogRequest> rqstMap = new HashMap<>();
    if (tableObj.getPartitionKeys().size() != 0) {
      // partitioned table
      for (WriteNotificationLogRequest rqst : batchRequest.getRequestList()) {
        String partition = Warehouse.makePartName(tableObj.getPartitionKeys(), rqst.getPartitionVals());
        partNameList.add(partition);
        // This is used to ignore those request for which the partition does not exists.
        rqstMap.put(partition, rqst);
      }
      ptnObjList = getMS().getPartitionsByNames(tableObj.getCatName(), tableObj.getDbName(),
          tableObj.getTableName(), partNameList);
    } else {
      ptnObjList = new ArrayList<>();
      for (WriteNotificationLogRequest ignored : batchRequest.getRequestList()) {
        ptnObjList.add(null);
      }
    }

    int idx = 0;
    for (Partition partObject : ptnObjList) {
      String partition = ""; //Empty string is an invalid partition name. Can be used for non partitioned table.
      WriteNotificationLogRequest request;
      if (partObject != null) {
        partition = Warehouse.makePartName(tableObj.getPartitionKeys(), partObject.getValues());
        request = rqstMap.get(partition);
      } else {
        // for non partitioned table, we can get serially from the list.
        request = batchRequest.getRequestList().get(idx++);
      }
      event.addNotification(partition, tableObj, partObject, request);
      if (listeners != null && !listeners.isEmpty()) {
        MetaStoreListenerNotifier.notifyEvent(listeners, EventMessage.EventType.BATCH_ACID_WRITE,
            new BatchAcidWriteEvent(partition, tableObj, partObject, request));
      }
    }

    getTxnHandler().addWriteNotificationLog(event);
    return new WriteNotificationLogBatchResponse();
  }

  @Override
  public LockResponse lock(LockRequest rqst) throws TException {
    return getTxnHandler().lock(rqst);
  }

  @Override
  public LockResponse check_lock(CheckLockRequest rqst) throws TException {
    return getTxnHandler().checkLock(rqst);
  }

  @Override
  public void unlock(UnlockRequest rqst) throws TException {
    getTxnHandler().unlock(rqst);
  }

  @Override
  public ShowLocksResponse show_locks(ShowLocksRequest rqst) throws TException {
    return getTxnHandler().showLocks(rqst);
  }

  @Override
  public void heartbeat(HeartbeatRequest ids) throws TException {
    getTxnHandler().heartbeat(ids);
  }

  @Override
  public HeartbeatTxnRangeResponse heartbeat_txn_range(HeartbeatTxnRangeRequest rqst)
      throws TException {
    return getTxnHandler().heartbeatTxnRange(rqst);
  }
  @Deprecated
  @Override
  public void compact(CompactionRequest rqst) throws TException {
    compact2(rqst);
  }
  @Override
  public CompactionResponse compact2(CompactionRequest rqst) throws TException {
    return getTxnHandler().compact(rqst);
  }

  @Override
  public ShowCompactResponse show_compact(ShowCompactRequest rqst) throws TException {
    ShowCompactResponse response = getTxnHandler().showCompact(rqst);
    response.setCompacts(FilterUtils.filterCompactionsIfEnabled(isServerFilterEnabled,
        filterHook, getDefaultCatalog(conf), response.getCompacts()));
    return response;
  }

  @Override
  public boolean submit_for_cleanup(CompactionRequest rqst, long highestWriteId, long txnId)
      throws TException {
    return getTxnHandler().submitForCleanup(rqst, highestWriteId, txnId);
  }

  @Override
  public void add_dynamic_partitions(AddDynamicPartitions rqst) throws TException {
    getTxnHandler().addDynamicPartitions(rqst);
  }

  @Override
  public ReplayedTxnsForPolicyResult get_replayed_txns_for_policy(String policyName) throws MetaException {
    Exception ex = null;
    ReplayedTxnsForPolicyResult ret = null;
    try {
      startFunction("get_replayed_txns_for_policy");
      ret = getTxnHandler().getReplayedTxnsForPolicy(policyName);
    } catch (Exception e) {
      ex = e;
      throw new MetaException("Failed to get replayed txns details for policy " + e.getMessage());
    } finally {
      endFunction("get_replayed_txns_for_policy", ret != null, ex);
    }
    return ret;
  }

  @Override
  public LockResponse get_lock_materialization_rebuild(String dbName, String tableName, long txnId)
      throws TException {
    return getTxnHandler().lockMaterializationRebuild(dbName, tableName, txnId);
  }

  @Deprecated
  @Override
  public boolean heartbeat_lock_materialization_rebuild(String dbName, String tableName, long txnId)
      throws TException {
    return getTxnHandler().heartbeatLockMaterializationRebuild(dbName, tableName, txnId);
  }

  @Override
  public void update_transaction_statistics(UpdateTransactionalStatsRequest req) throws TException {
    getTxnHandler().updateTransactionStatistics(req);
  }

  @Override
  public Materialization get_materialization_invalidation_info(final CreationMetadata cm,
      final String validTxnList) throws MetaException {
    return getTxnHandler().getMaterializationInvalidationInfo(cm, validTxnList);
  }

  @Override
  public LockResponse get_lock_materialization_rebuild_req(LockMaterializationRebuildRequest rqst)
      throws TException {
    return getTxnHandler().lockMaterializationRebuild(rqst);
  }

  @Override
  public boolean heartbeat_lock_materialization_rebuild_req(LockMaterializationRebuildRequest rqst)
      throws TException {
    return getTxnHandler().heartbeatLockMaterializationRebuild(rqst);
  }
}
