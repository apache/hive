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
package org.apache.hadoop.hive.metastore.txn;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.classification.RetrySemantics;
import org.apache.hadoop.hive.metastore.api.AbortCompactResponse;
import org.apache.hadoop.hive.metastore.api.AbortCompactionRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.AbortTxnsRequest;
import org.apache.hadoop.hive.metastore.api.AddDynamicPartitions;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.AllocateTableWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.CheckLockRequest;
import org.apache.hadoop.hive.metastore.api.CommitTxnRequest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionResponse;
import org.apache.hadoop.hive.metastore.api.CreationMetadata;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FindNextCompactRequest;
import org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoRequest;
import org.apache.hadoop.hive.metastore.api.GetLatestCommittedCompactionInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsInfoResponse;
import org.apache.hadoop.hive.metastore.api.GetOpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.GetValidWriteIdsResponse;
import org.apache.hadoop.hive.metastore.api.HeartbeatRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeRequest;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.Materialization;
import org.apache.hadoop.hive.metastore.api.MaxAllocatedTableWriteIdRequest;
import org.apache.hadoop.hive.metastore.api.MaxAllocatedTableWriteIdResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchCompactionException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.OpenTxnRequest;
import org.apache.hadoop.hive.metastore.api.OpenTxnsResponse;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.ReplTblWriteIdStateRequest;
import org.apache.hadoop.hive.metastore.api.SeedTableWriteIdsRequest;
import org.apache.hadoop.hive.metastore.api.SeedTxnIdRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactRequest;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.TxnType;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.api.UpdateTransactionalStatsRequest;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionInfo;
import org.apache.hadoop.hive.metastore.txn.entities.CompactionMetricsData;
import org.apache.hadoop.hive.metastore.txn.entities.MetricsInfo;
import org.apache.hadoop.hive.metastore.txn.jdbc.MultiDataSourceJdbcResource;
import org.apache.hadoop.hive.metastore.txn.retry.SqlRetry;
import org.apache.hadoop.hive.metastore.txn.retry.SqlRetryException;
import org.apache.hadoop.hive.metastore.txn.retry.SqlRetryHandler;
import org.springframework.transaction.annotation.Transactional;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A handler to answer transaction related calls that come into the metastore
 * server.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface TxnStore extends Configurable {
  /**
   * Prefix for key when committing with a key/value.
   */
  String TXN_KEY_START = "_meta";



  enum MUTEX_KEY {
    Initiator, Cleaner, HouseKeeper, TxnCleaner,
    CompactionScheduler, MaterializationRebuild
  }
  // Compactor states (Should really be enum)
  String INITIATED_RESPONSE = "initiated";
  String WORKING_RESPONSE = "working";
  String CLEANING_RESPONSE = "ready for cleaning";
  String FAILED_RESPONSE = "failed";
  String SUCCEEDED_RESPONSE = "succeeded";
  String DID_NOT_INITIATE_RESPONSE = "did not initiate";
  String REFUSED_RESPONSE = "refused";
  String ABORTED_RESPONSE = "aborted";

  char INITIATED_STATE = 'i';
  char WORKING_STATE = 'w';
  char READY_FOR_CLEANING = 'r';
  char FAILED_STATE = 'f';
  char SUCCEEDED_STATE = 's';
  char DID_NOT_INITIATE = 'a';
  char REFUSED_STATE = 'c';

  char ABORTED_STATE = 'x';

  // Compactor types
  char MAJOR_TYPE = 'a';
  char MINOR_TYPE = 'i';
  char REBALANCE_TYPE = 'r';
  char ABORT_TXN_CLEANUP_TYPE = 'c';

  String[] COMPACTION_STATES = new String[] {INITIATED_RESPONSE, WORKING_RESPONSE, CLEANING_RESPONSE, FAILED_RESPONSE,
      SUCCEEDED_RESPONSE, DID_NOT_INITIATE_RESPONSE, REFUSED_RESPONSE };

  int TIMED_OUT_TXN_ABORT_BATCH_SIZE = 50000;

  String POOL_TX = "txnhandler";
  String POOL_MUTEX = "mutex";
  String POOL_COMPACTOR = "compactor";


  /**
   * @return Returns the {@link SqlRetryHandler} instance used by {@link TxnStore}.
   */
  SqlRetryHandler getRetryHandler();

  /**
   * @return Returns the {@link MultiDataSourceJdbcResource} instance used by {@link TxnStore}.
   */
  MultiDataSourceJdbcResource getJdbcResourceHolder();
  
  /**
   * Get information about open transactions.  This gives extensive information about the
   * transactions rather than just the list of transactions.  This should be used when the need
   * is to see information about the transactions (e.g. show transactions).
   * @return information about open transactions
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_TX)
  @RetrySemantics.ReadOnly
  GetOpenTxnsInfoResponse getOpenTxnsInfo() throws MetaException;

  /**
   * Get list of valid transactions.  This gives just the list of transactions that are open.
   * @return list of open transactions, as well as a high watermark.
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_TX)
  @RetrySemantics.ReadOnly
  GetOpenTxnsResponse getOpenTxns() throws MetaException;

  /**
   * Get list of valid transactions.  This gives just the list of transactions that are open.
   * @param excludeTxnTypes : excludes this type of txns while getting the open txns
   * @return list of open transactions, as well as a high watermark.
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_TX)
  @RetrySemantics.ReadOnly
  GetOpenTxnsResponse getOpenTxns(List<TxnType> excludeTxnTypes) throws MetaException;

  /**
   * Get the count for open transactions.
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_TX)
  @RetrySemantics.ReadOnly
  void countOpenTxns() throws MetaException;

  /**
   * Open a set of transactions
   * @param rqst request to open transactions
   * @return information on opened transactions
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(value = POOL_TX, noRollbackFor = SqlRetryException.class)
  @RetrySemantics.Idempotent
  OpenTxnsResponse openTxns(OpenTxnRequest rqst) throws MetaException;

  @SqlRetry(lockInternally = true)
  @Transactional(POOL_TX)
  @RetrySemantics.Idempotent
  long getTargetTxnId(String replPolicy, long sourceTxnId) throws MetaException;

  /**
   * Abort (rollback) a transaction.
   * @param rqst info on transaction to abort
   * @throws NoSuchTxnException
   * @throws MetaException
   */
  @SqlRetry(lockInternally = true)
  @Transactional(POOL_TX)
  @RetrySemantics.Idempotent
  void abortTxn(AbortTxnRequest rqst) throws NoSuchTxnException, MetaException, TxnAbortedException;

  /**
   * Abort (rollback) a list of transactions in one request.
   * @param rqst info on transactions to abort
   * @throws NoSuchTxnException
   * @throws MetaException
   */
  @SqlRetry(lockInternally = true)
  @Transactional(POOL_TX)
  @RetrySemantics.Idempotent
  void abortTxns(AbortTxnsRequest rqst) throws NoSuchTxnException, MetaException;

  /**
   * Commit a transaction
   * @param rqst info on transaction to commit
   * @throws NoSuchTxnException
   * @throws TxnAbortedException
   * @throws MetaException
   */
  @SqlRetry(lockInternally = true)
  @Transactional(value = POOL_TX, noRollbackFor = TxnAbortedException.class)
  @RetrySemantics.Idempotent("No-op if already committed")
  void commitTxn(CommitTxnRequest rqst)
    throws NoSuchTxnException, TxnAbortedException,  MetaException;

  /**
   * Replicate Table Write Ids state to mark aborted write ids and writeid high water mark.
   * @param rqst info on table/partitions and writeid snapshot to replicate.
   * @throws MetaException in case of failure
   */
  @SqlRetry(lockInternally = true)
  @Transactional(POOL_TX)
  @RetrySemantics.Idempotent("No-op if already replicated the writeid state")
  void replTableWriteIdState(ReplTblWriteIdStateRequest rqst) throws MetaException;

  @Transactional(POOL_TX)
  void updateTransactionStatistics(UpdateTransactionalStatsRequest req) throws MetaException;

  /**
   * Get invalidation info for the materialization. Currently, the materialization information
   * only contains information about whether there was update/delete operations on the source
   * tables used by the materialization since it was created.
   * @param cm creation metadata for the materialization
   * @param validTxnList valid transaction list for snapshot taken for current query
   * @throws MetaException
   */
  @Transactional(POOL_TX)
  @RetrySemantics.Idempotent
  Materialization getMaterializationInvalidationInfo(
          final CreationMetadata cm, final String validTxnList)
          throws MetaException;

  @RetrySemantics.ReadOnly
  long getTxnIdForWriteId(String dbName, String tblName, long writeId)
      throws MetaException;

  @SqlRetry
  @Transactional(POOL_TX)
  @RetrySemantics.ReadOnly
  long getLatestTxnIdInConflict(long txnid) throws MetaException;

  @SqlRetry(lockInternally = true)
  @Transactional(POOL_TX)
  LockResponse lockMaterializationRebuild(String dbName, String tableName, long txnId)
      throws MetaException;

  @SqlRetry(lockInternally = true)
  @Transactional(POOL_TX)
  boolean heartbeatLockMaterializationRebuild(String dbName, String tableName, long txnId)
      throws MetaException;

  @SqlRetry(lockInternally = true)
  @Transactional(POOL_TX)
  long cleanupMaterializationRebuildLocks(ValidTxnList validTxnList, long timeout)
      throws MetaException;

  /**
   * Gets the list of valid write ids for the given table wrt to current txn
   * @param rqst info on transaction and list of table names associated with given transaction
   * @throws NoSuchTxnException
   * @throws MetaException
   */
  @SqlRetry  
  @Transactional(POOL_TX)
  @RetrySemantics.ReadOnly
  GetValidWriteIdsResponse getValidWriteIds(GetValidWriteIdsRequest rqst)
          throws NoSuchTxnException,  MetaException;

  @SqlRetry
  @Transactional(POOL_TX)
  @RetrySemantics.SafeToRetry
  void addWriteIdsToMinHistory(long txnId, Map<String, Long> minOpenWriteIds) throws MetaException;

  /**
   * Allocate a write ID for the given table and associate it with a transaction
   * @param rqst info on transaction and table to allocate write id
   * @throws NoSuchTxnException
   * @throws TxnAbortedException
   * @throws MetaException
   */
  @SqlRetry(lockInternally = true, retryOnDuplicateKey = true)
  @Transactional(POOL_TX)
  AllocateTableWriteIdsResponse allocateTableWriteIds(AllocateTableWriteIdsRequest rqst)
    throws NoSuchTxnException, TxnAbortedException, MetaException;

  /**
   * Reads the maximum allocated writeId for the given table
   * @param rqst table for which the maximum writeId is requested
   * @return the maximum allocated writeId
   */
  @SqlRetry
  @Transactional(POOL_TX)
  MaxAllocatedTableWriteIdResponse getMaxAllocatedTableWrited(MaxAllocatedTableWriteIdRequest rqst)
      throws MetaException;

  /**
   * Called on conversion of existing table to full acid.  Sets initial write ID to a high
   * enough value so that we can assign unique ROW__IDs to data in existing files.
   */
  @SqlRetry
  @Transactional(POOL_TX)
  void seedWriteId(SeedTableWriteIdsRequest rqst) throws MetaException;

  /**
   * Sets the next txnId to the given value.
   * If the actual txnId is greater it will throw an exception.
   * @param rqst
   */
  @SqlRetry(lockInternally = true)
  @Transactional(POOL_TX)
  void seedTxnId(SeedTxnIdRequest rqst) throws MetaException;

  /**
   * Obtain a lock.
   * @param rqst information on the lock to obtain.  If the requester is part of a transaction
   *             the txn information must be included in the lock request.
   * @return info on the lock, including whether it was obtained.
   * @throws NoSuchTxnException
   * @throws TxnAbortedException
   * @throws MetaException
   */
  @RetrySemantics.CannotRetry
  LockResponse lock(LockRequest rqst)
    throws NoSuchTxnException, TxnAbortedException, MetaException;

  /**
   * Check whether a lock has been obtained.  This is used after {@link #lock} returned a wait
   * state.
   * @param rqst info on the lock to check
   * @return info on the state of the lock
   * @throws NoSuchTxnException
   * @throws NoSuchLockException
   * @throws TxnAbortedException
   * @throws MetaException
   */
  @Transactional(POOL_TX)
  @RetrySemantics.SafeToRetry
  LockResponse checkLock(CheckLockRequest rqst)
    throws NoSuchTxnException, NoSuchLockException, TxnAbortedException, MetaException;

  /**
   * Unlock a lock.  It is not legal to call this if the caller is part of a txn.  In that case
   * the txn should be committed or aborted instead.  (Note someday this will change since
   * multi-statement transactions will allow unlocking in the transaction.)
   * @param rqst lock to unlock
   * @throws NoSuchLockException
   * @throws TxnOpenException
   * @throws MetaException
   */
  @Transactional(POOL_TX)
  @RetrySemantics.Idempotent
  void unlock(UnlockRequest rqst)
    throws NoSuchLockException, TxnOpenException, MetaException;

  /**
   * Get information on current locks.
   * @param rqst lock information to retrieve
   * @return lock information.
   * @throws MetaException
   */
  @Transactional(POOL_TX)
  @RetrySemantics.ReadOnly
  ShowLocksResponse showLocks(ShowLocksRequest rqst) throws MetaException;

  /**
   * Send a heartbeat for a lock or a transaction
   * @param ids lock and/or txn id to heartbeat
   * @throws NoSuchTxnException
   * @throws NoSuchLockException
   * @throws TxnAbortedException
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_TX)
  @RetrySemantics.SafeToRetry
  void heartbeat(HeartbeatRequest ids)
    throws NoSuchTxnException,  NoSuchLockException, TxnAbortedException, MetaException;

  /**
   * Heartbeat a group of transactions together
   * @param rqst set of transactions to heartbat
   * @return info on txns that were heartbeated
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_TX)
  @RetrySemantics.SafeToRetry
  HeartbeatTxnRangeResponse heartbeatTxnRange(HeartbeatTxnRangeRequest rqst)
    throws MetaException;

  /**
   * Submit a compaction request into the queue.  This is called when a user manually requests a
   * compaction.
   * @param rqst information on what to compact
   * @return id of the compaction that has been started or existing id if this resource is already scheduled
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_TX)
  @RetrySemantics.Idempotent
  CompactionResponse compact(CompactionRequest rqst) throws MetaException;

  @SqlRetry
  @Transactional(POOL_TX)
  @RetrySemantics.SafeToRetry
  boolean submitForCleanup(CompactionRequest rqst, long highestWriteId, long txnId) throws MetaException;

  /**
   * Show list of current compactions.
   * @param rqst info on which compactions to show
   * @return compaction information
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_TX)
  @RetrySemantics.ReadOnly
  ShowCompactResponse showCompact(ShowCompactRequest rqst) throws MetaException;

  /**
   * Abort (rollback) a compaction.
   *
   * @param rqst info on compaction to abort
   * @return
   * @throws NoSuchCompactionException
   * @throws MetaException
   */
  @Transactional(POOL_TX)
  @RetrySemantics.Idempotent
  AbortCompactResponse abortCompactions(AbortCompactionRequest rqst) throws NoSuchCompactionException, MetaException;

  /**
   * Get one latest record of SUCCEEDED or READY_FOR_CLEANING compaction for a table/partition.
   * No checking is done on the dbname, tablename, or partitionname to make sure they refer to valid objects.
   * Is is assumed to be done by the caller.
   * Note that partition names should be supplied with the request for a partitioned table; otherwise,
   * no records will be returned.
   * @param rqst info on which compaction to retrieve
   * @return one latest compaction record for a non partitioned table or one latest record for each
   * partition specified by the request.
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_TX)
  @RetrySemantics.ReadOnly
  GetLatestCommittedCompactionInfoResponse getLatestCommittedCompactionInfo(
      GetLatestCommittedCompactionInfoRequest rqst) throws MetaException;

  /**
   * Add information on a set of dynamic partitions that participated in a transaction.
   * @param rqst dynamic partition info.
   * @throws NoSuchTxnException
   * @throws TxnAbortedException
   * @throws MetaException
   */
  @SqlRetry(lockInternally = true)
  @Transactional(POOL_TX)
  @RetrySemantics.SafeToRetry
  void addDynamicPartitions(AddDynamicPartitions rqst)
      throws NoSuchTxnException,  TxnAbortedException, MetaException;

  /**
   * Clean up corresponding records in metastore tables.
   * @param type Hive object type
   * @param db database object
   * @param table table object
   * @param partitionIterator partition iterator
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_TX)
  @RetrySemantics.Idempotent
  default void cleanupRecords(HiveObjectType type, Database db, Table table, 
      Iterator<Partition> partitionIterator) throws MetaException {
    cleanupRecords(type, db, table, partitionIterator, false);
  }

  @SqlRetry
  @Transactional(POOL_TX)
  @RetrySemantics.Idempotent
  void cleanupRecords(HiveObjectType type, Database db, Table table, 
      Iterator<Partition> partitionIterator, boolean keepTxnToWriteIdMetaData) throws MetaException;

  @SqlRetry
  @Transactional(POOL_TX)
  @RetrySemantics.Idempotent
  void cleanupRecords(HiveObjectType type, Database db, Table table,
      Iterator<Partition> partitionIterator, long txnId) throws MetaException;

  @SqlRetry
  @Transactional(POOL_TX)
  @RetrySemantics.Idempotent
  void onRename(String oldCatName, String oldDbName, String oldTabName, String oldPartName,
      String newCatName, String newDbName, String newTabName, String newPartName)
      throws MetaException;

  /**
   * Timeout transactions and/or locks.  This should only be called by the compactor.
   */
  @Transactional(POOL_TX)
  @RetrySemantics.Idempotent
  void performTimeOuts();

  /**
   * This will look through the completed_txn_components table and look for partitions or tables
   * that may be ready for compaction.  Also, look through txns and txn_components tables for
   * aborted transactions that we should add to the list.
   * @param abortedThreshold  number of aborted queries forming a potential compaction request.
   * @param abortedTimeThreshold age of an aborted txn in milliseconds that will trigger a
   *                             potential compaction request.
   * @return list of CompactionInfo structs.  These will not have id, type,
   * or runAs set since these are only potential compactions not actual ones.
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.ReadOnly
  Set<CompactionInfo> findPotentialCompactions(int abortedThreshold, long abortedTimeThreshold) throws MetaException;

  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.ReadOnly
  Set<CompactionInfo> findPotentialCompactions(int abortedThreshold, long abortedTimeThreshold, long lastChecked)
      throws MetaException;

  /**
   * This updates COMPACTION_QUEUE.  Set runAs username for the case where the request was
   * generated by the user and so the worker must set this value later.  Sets highestWriteId so that
   * cleaner doesn't clean above what compactor has processed.  Updates TXN_COMPONENTS so that
   * we know where {@code compactionTxnId} was writing to in case it aborts.
   * @param compactionTxnId - txnid in which Compactor is running
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.Idempotent
  void updateCompactorState(CompactionInfo ci, long compactionTxnId) throws MetaException;

  /**
   * This will grab the next compaction request off of
   * the queue, and assign it to the worker.
   * @deprecated Replaced by
   *     {@link TxnStore#findNextToCompact(org.apache.hadoop.hive.metastore.api.FindNextCompactRequest)}
   * @param workerId id of the worker calling this, will be recorded in the db
   * @return an info element for this compaction request, or null if there is no work to do now.
   */
  @Deprecated
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.ReadOnly
  CompactionInfo findNextToCompact(String workerId) throws MetaException;

  /**
   * This will grab the next compaction request off of the queue, and assign it to the worker.
   * @param rqst request to find next compaction to run
   * @return an info element for next compaction in the queue, or null if there is no work to do now.
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.ReadOnly
  CompactionInfo findNextToCompact(FindNextCompactRequest rqst) throws MetaException;

  /**
   * This will mark an entry in the queue as compacted
   * and put it in the ready to clean state.
   * @param info info on the compaction entry to mark as compacted.
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.SafeToRetry
  void markCompacted(CompactionInfo info) throws MetaException;

  /**
   * Find entries in the queue that are ready to
   * be cleaned.
   * @param minOpenTxnWaterMark Minimum open txnId
   * @param retentionTime Milliseconds to delay the cleaner
   * @return information on the entry in the queue.
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.ReadOnly
  List<CompactionInfo> findReadyToClean(long minOpenTxnWaterMark, long retentionTime) throws MetaException;

  /**
   * Find the aborted entries in TXN_COMPONENTS which can be used to
   * clean directories belonging to transactions in aborted state.
   * @param abortedTimeThreshold Age of table/partition's oldest aborted transaction involving a given table
   *                            or partition that will trigger cleanup.
   * @param abortedThreshold Number of aborted transactions involving a given table or partition
   *                         that will trigger cleanup.
   * @return Information of potential abort items that needs to be cleaned.
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.ReadOnly
  List<CompactionInfo> findReadyToCleanAborts(long abortedTimeThreshold, int abortedThreshold) throws MetaException;

  /**
   * Sets the cleaning start time for a particular compaction
   *
   * @param info info on the compaction entry
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.CannotRetry
  void markCleanerStart(CompactionInfo info) throws MetaException;

  /**
   * Removes the cleaning start time for a particular compaction
   *
   * @param info info on the compaction entry
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.CannotRetry
  void clearCleanerStart(CompactionInfo info) throws MetaException;

  /**
   * This will remove an entry from the queue after
   * it has been compacted.
   *
   * @param info info on the compaction entry to remove
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.CannotRetry
  void markCleaned(CompactionInfo info) throws MetaException;

  /**
   * Mark a compaction entry as failed.  This will move it to the compaction history queue with a
   * failed status.  It will NOT clean up aborted transactions in the table/partition associated
   * with this compaction.
   * @param info information on the compaction that failed.
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.CannotRetry
  void markFailed(CompactionInfo info) throws MetaException;

  /**
   * Mark a compaction as refused (to run). This can happen if a manual compaction is requested by the user,
   * but for some reason, the table is not suitable for compation.
   * @param info compaction job.
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  void markRefused(CompactionInfo info) throws MetaException;

  /**
   * Stores the value of {@link CompactionInfo#retryRetention} and {@link CompactionInfo#errorMessage} fields
   * of the CompactionInfo either by inserting or updating the fields in the HMS database.
   * @param info The {@link CompactionInfo} object holding the values.
   * @throws MetaException
   */
  @SqlRetry(lockInternally = true)
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.CannotRetry
  void setCleanerRetryRetentionTimeOnError(CompactionInfo info) throws MetaException;

  /**
   * Clean up entries from TXN_TO_WRITE_ID table less than min_uncommited_txnid as found by
   * min(max(TXNS.txn_id), min(WRITE_SET.WS_COMMIT_ID), min(Aborted TXNS.txn_id)).
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.SafeToRetry
  void cleanTxnToWriteIdTable() throws MetaException;

  /**
   * De-duplicate entries from COMPLETED_TXN_COMPONENTS table.
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.SafeToRetry
  void removeDuplicateCompletedTxnComponents() throws MetaException;

  /**
   * Clean up aborted or committed transactions from txns that have no components in txn_components.  The reason such
   * txns exist can be that no work was done in this txn (e.g. Streaming opened TransactionBatch and
   * abandoned it w/o doing any work) or due to {@link #markCleaned(CompactionInfo)} being called,
   * or the delete from the txns was delayed because of TXN_OPENTXN_TIMEOUT window.
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.SafeToRetry
  void cleanEmptyAbortedAndCommittedTxns() throws MetaException;

  /**
   * This will take all entries assigned to workers
   * on a host return them to INITIATED state.  The initiator should use this at start up to
   * clean entries from any workers that were in the middle of compacting when the metastore
   * shutdown.  It does not reset entries from worker threads on other hosts as those may still
   * be working.
   * @param hostname Name of this host.  It is assumed this prefixes the thread's worker id,
   *                 so that like hostname% will match the worker id.
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.Idempotent
  void revokeFromLocalWorkers(String hostname) throws MetaException;

  /**
   * This call will return all compaction queue
   * entries assigned to a worker but over the timeout back to the initiated state.
   * This should be called by the initiator on start up and occasionally when running to clean up
   * after dead threads.  At start up {@link #revokeFromLocalWorkers(String)} should be called
   * first.
   * @param timeout number of milliseconds since start time that should elapse before a worker is
   *                declared dead.
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.Idempotent
  void revokeTimedoutWorkers(long timeout) throws MetaException;

  /**
   * Queries metastore DB directly to find columns in the table which have statistics information.
   * If {@code ci} includes partition info then per partition stats info is examined, otherwise
   * table level stats are examined.
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.ReadOnly
  List<String> findColumnsWithStats(CompactionInfo ci) throws MetaException;

  /**
   * For any given compactable entity (partition, table if not partitioned) the history of compactions
   * may look like "sssfffaaasffss", for example.  The idea is to retain the tail (most recent) of the
   * history such that a configurable number of each type of state is present.  Any other entries
   * can be purged.  This scheme has advantage of always retaining the last failure/success even if
   * it's not recent.
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.SafeToRetry
  void purgeCompactionHistory() throws MetaException;

  /**
   * WriteSet tracking is used to ensure proper transaction isolation.  This method deletes the
   * transaction metadata once it becomes unnecessary.
   */
  @Transactional(POOL_TX)
  @RetrySemantics.SafeToRetry
  void performWriteSetGC() throws MetaException;

  /**
   * Determine if there are enough consecutive failures compacting a table or partition that no
   * new automatic compactions should be scheduled.  User initiated compactions do not do this
   * check.
   * @param ci  Table or partition to check.
   * @return true if it is ok to compact, false if there have been too many failures.
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.ReadOnly
  boolean checkFailedCompactions(CompactionInfo ci) throws MetaException;

  @VisibleForTesting
  @Transactional(POOL_TX)
  int getNumLocks() throws SQLException, MetaException;

  @VisibleForTesting
  long setTimeout(long milliseconds);

  @VisibleForTesting
  long getOpenTxnTimeOutMillis();

  @VisibleForTesting
  void setOpenTxnTimeOutMillis(long openTxnTimeOutMillis);

  @RetrySemantics.Idempotent
  MutexAPI getMutexAPI();

  /**
   * This is primarily designed to provide coarse-grained mutex support to operations running
   * inside the Metastore (of which there could be several instances).  The initial goal is to
   * ensure that various sub-processes of the Compactor don't step on each other.
   *
   * In RDMBS world each {@code LockHandle} uses a java.sql.Connection so use it sparingly.
   */
  interface MutexAPI {
    /**
     * The {@code key} is name of the lock. Will acquire an exclusive lock or block.  It returns
     * a handle which must be used to release the lock.  Each invocation returns a new handle.
     */
    @SqlRetry(lockInternally = true)
    LockHandle acquireLock(String key) throws MetaException;

    /**
     * Same as {@link #acquireLock(String)} but takes an already existing handle as input.  This
     * will associate the lock on {@code key} with the same handle.  All locks associated with
     * the same handle will be released together.
     * @param handle not NULL
     */
    void acquireLock(String key, LockHandle handle) throws MetaException;
    interface LockHandle extends AutoCloseable {
      /**
       * Releases all locks associated with this handle.
       */
      void releaseLocks();

      /**
       * Returns the value of the last update time persisted during the appropriate lock release call.
       */
      Long getLastUpdateTime();

      /**
       * Releases all locks associated with this handle, and persist the value of the last update time.
       */
      void releaseLocks(Long timestamp);
    }
  }

  /**
   * Once a {@link java.util.concurrent.ThreadPoolExecutor} Worker submits a job to the cluster,
   * it calls this to update the metadata.
   * @param id {@link CompactionInfo#id}
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.Idempotent
  void setHadoopJobId(String hadoopJobId, long id) throws MetaException;

  /**
   * Add the ACID write event information to writeNotificationLog table.
   * @param acidWriteEvent
   */
  @SqlRetry(lockInternally = true, retryOnDuplicateKey = true)
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.Idempotent
  void addWriteNotificationLog(ListenerEvent acidWriteEvent) throws MetaException;

  /**
   * Return the currently seen minimum open transaction ID.
   * @return minimum transaction ID
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.Idempotent
  long findMinOpenTxnIdForCleaner() throws MetaException;

  /**
   * Returns the compaction running in the transaction txnId
   * @param txnId transaction Id
   * @return compaction info
   * @throws MetaException ex
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.ReadOnly
  Optional<CompactionInfo> getCompactionByTxnId(long txnId) throws MetaException;

  /**
   * Returns the smallest txnid that could be seen in open state across all active transactions in
   * the system or -1 if there are no active transactions.
   * @return transaction ID
   * @deprecated remove when min_history_level table is dropped
   */
  @RetrySemantics.ReadOnly
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @Deprecated
  long findMinTxnIdSeenOpen() throws MetaException;

  /**
   * Returns ACID metadata related metrics info.
   * @return metrics info object
   */
  @SqlRetry
  @Transactional(POOL_TX)
  @RetrySemantics.ReadOnly
  MetricsInfo getMetricsInfo() throws MetaException;

  /**
   * Returns ACID metrics related info for a specific resource and metric type. If no record is found matching the
   * filter criteria, null will be returned.
   * @param dbName name of database, non-null
   * @param tblName name of the table, non-null
   * @param partitionName name of the partition, can be null
   * @param type type of the delta metric, non-null
   * @return instance of delta metrics info, can be null
   * @throws MetaException
   */
  @RetrySemantics.ReadOnly
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  CompactionMetricsData getCompactionMetricsData(String dbName, String tblName, String partitionName,
                                                 CompactionMetricsData.MetricType type) throws MetaException;

  /**
   * Remove records from the compaction metrics cache matching the filter criteria passed in as parameters
   * @param dbName name of the database, non-null
   * @param tblName name of the table, non-null
   * @param partitionName name of the partition, non-null
   * @param type type of the delta metric, non-null
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.SafeToRetry
  void removeCompactionMetricsData(String dbName, String tblName, String partitionName,
      CompactionMetricsData.MetricType type) throws MetaException;

  /**
   * Returns the top ACID metrics from each type {@link CompactionMetricsData.MetricType}
   * @param limit number of returned records for each type
   * @return list of metrics, always non-null
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.ReadOnly
  List<CompactionMetricsData> getTopCompactionMetricsDataPerType(int limit)
      throws MetaException;

  /**
   * Create, update or delete one record in the compaction metrics cache.
   * <p>
   * If the metric is not found in the metrics cache, it will be created.
   * </p>
   * <p>
   * If the metric is found, it will be updated. This operation uses an optimistic locking mechanism, meaning if another
   * operation changed the value of this metric, the update will abort and won't be retried.
   * </p>
   * <p>
   * If the new metric value is below {@link CompactionMetricsData#getThreshold()}, it will be deleted.
   * </p>
   * @param data the object that is used for the operation
   * @return true, if update finished successfully
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_COMPACTOR)
  @RetrySemantics.Idempotent
  boolean updateCompactionMetricsData(CompactionMetricsData data) throws MetaException;

}
