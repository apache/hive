/**
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
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.common.classification.RetrySemantics;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.*;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * A handler to answer transaction related calls that come into the metastore
 * server.
 *
 * Note on log messages:  Please include txnid:X and lockid info using
 * {@link org.apache.hadoop.hive.common.JavaUtils#txnIdToString(long)}
 * and {@link org.apache.hadoop.hive.common.JavaUtils#lockIdToString(long)} in all messages.
 * The txnid:X and lockid:Y matches how Thrift object toString() methods are generated,
 * so keeping the format consistent makes grep'ing the logs much easier.
 *
 * Note on HIVE_LOCKS.hl_last_heartbeat.
 * For locks that are part of transaction, we set this 0 (would rather set it to NULL but
 * Currently the DB schema has this NOT NULL) and only update/read heartbeat from corresponding
 * transaction in TXNS.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface TxnStore {

  enum MUTEX_KEY {Initiator, Cleaner, HouseKeeper, CompactionHistory, CheckLock,
    WriteSetCleaner, CompactionScheduler}
  // Compactor states (Should really be enum)
  String INITIATED_RESPONSE = "initiated";
  String WORKING_RESPONSE = "working";
  String CLEANING_RESPONSE = "ready for cleaning";
  String FAILED_RESPONSE = "failed";
  String SUCCEEDED_RESPONSE = "succeeded";
  String ATTEMPTED_RESPONSE = "attempted";

  public static final int TIMED_OUT_TXN_ABORT_BATCH_SIZE = 50000;

  public void setConf(HiveConf conf);

  /**
   * Get information about open transactions.  This gives extensive information about the
   * transactions rather than just the list of transactions.  This should be used when the need
   * is to see information about the transactions (e.g. show transactions).
   * @return information about open transactions
   * @throws MetaException
   */
  @RetrySemantics.ReadOnly
  public GetOpenTxnsInfoResponse getOpenTxnsInfo() throws MetaException;

  /**
   * Get list of valid transactions.  This gives just the list of transactions that are open.
   * @return list of open transactions, as well as a high water mark.
   * @throws MetaException
   */
  @RetrySemantics.ReadOnly
  public GetOpenTxnsResponse getOpenTxns() throws MetaException;

  /**
   * Get the count for open transactions.
   * @throws MetaException
   */
  @RetrySemantics.ReadOnly
  public void countOpenTxns() throws MetaException;

  /**
   * Open a set of transactions
   * @param rqst request to open transactions
   * @return information on opened transactions
   * @throws MetaException
   */
  @RetrySemantics.Idempotent
  public OpenTxnsResponse openTxns(OpenTxnRequest rqst) throws MetaException;

  /**
   * Abort (rollback) a transaction.
   * @param rqst info on transaction to abort
   * @throws NoSuchTxnException
   * @throws MetaException
   */
  @RetrySemantics.Idempotent
  public void abortTxn(AbortTxnRequest rqst) throws NoSuchTxnException, MetaException, TxnAbortedException;

  /**
   * Abort (rollback) a list of transactions in one request.
   * @param rqst info on transactions to abort
   * @throws NoSuchTxnException
   * @throws MetaException
   */
  @RetrySemantics.Idempotent
  public void abortTxns(AbortTxnsRequest rqst) throws NoSuchTxnException, MetaException;

  /**
   * Commit a transaction
   * @param rqst info on transaction to commit
   * @throws NoSuchTxnException
   * @throws TxnAbortedException
   * @throws MetaException
   */
  @RetrySemantics.Idempotent
  public void commitTxn(CommitTxnRequest rqst)
    throws NoSuchTxnException, TxnAbortedException,  MetaException;

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
  public LockResponse lock(LockRequest rqst)
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
  @RetrySemantics.SafeToRetry
  public LockResponse checkLock(CheckLockRequest rqst)
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
  @RetrySemantics.Idempotent
  public void unlock(UnlockRequest rqst)
    throws NoSuchLockException, TxnOpenException, MetaException;

  /**
   * Get information on current locks.
   * @param rqst lock information to retrieve
   * @return lock information.
   * @throws MetaException
   */
  @RetrySemantics.ReadOnly
  public ShowLocksResponse showLocks(ShowLocksRequest rqst) throws MetaException;

  /**
   * Send a heartbeat for a lock or a transaction
   * @param ids lock and/or txn id to heartbeat
   * @throws NoSuchTxnException
   * @throws NoSuchLockException
   * @throws TxnAbortedException
   * @throws MetaException
   */
  @RetrySemantics.SafeToRetry
  public void heartbeat(HeartbeatRequest ids)
    throws NoSuchTxnException,  NoSuchLockException, TxnAbortedException, MetaException;

  /**
   * Heartbeat a group of transactions together
   * @param rqst set of transactions to heartbat
   * @return info on txns that were heartbeated
   * @throws MetaException
   */
  @RetrySemantics.SafeToRetry
  public HeartbeatTxnRangeResponse heartbeatTxnRange(HeartbeatTxnRangeRequest rqst)
    throws MetaException;

  /**
   * Submit a compaction request into the queue.  This is called when a user manually requests a
   * compaction.
   * @param rqst information on what to compact
   * @return id of the compaction that has been started or existing id if this resource is already scheduled
   * @throws MetaException
   */
  @RetrySemantics.Idempotent
  public CompactionResponse compact(CompactionRequest rqst) throws MetaException;

  /**
   * Show list of current compactions
   * @param rqst info on which compactions to show
   * @return compaction information
   * @throws MetaException
   */
  @RetrySemantics.ReadOnly
  public ShowCompactResponse showCompact(ShowCompactRequest rqst) throws MetaException;

  /**
   * Add information on a set of dynamic partitions that participated in a transaction.
   * @param rqst dynamic partition info.
   * @throws NoSuchTxnException
   * @throws TxnAbortedException
   * @throws MetaException
   */
  @RetrySemantics.SafeToRetry
  public void addDynamicPartitions(AddDynamicPartitions rqst)
      throws NoSuchTxnException,  TxnAbortedException, MetaException;

  /**
   * Clean up corresponding records in metastore tables
   * @param type Hive object type
   * @param db database object
   * @param table table object
   * @param partitionIterator partition iterator
   * @throws MetaException
   */
  @RetrySemantics.Idempotent
  public void cleanupRecords(HiveObjectType type, Database db, Table table,
                             Iterator<Partition> partitionIterator) throws MetaException;

  /**
   * Timeout transactions and/or locks.  This should only be called by the compactor.
   */
  @RetrySemantics.Idempotent
  public void performTimeOuts();

  /**
   * This will look through the completed_txn_components table and look for partitions or tables
   * that may be ready for compaction.  Also, look through txns and txn_components tables for
   * aborted transactions that we should add to the list.
   * @param maxAborted Maximum number of aborted queries to allow before marking this as a
   *                   potential compaction.
   * @return list of CompactionInfo structs.  These will not have id, type,
   * or runAs set since these are only potential compactions not actual ones.
   */
  @RetrySemantics.ReadOnly
  public Set<CompactionInfo> findPotentialCompactions(int maxAborted) throws MetaException;

  /**
   * Sets the user to run as.  This is for the case
   * where the request was generated by the user and so the worker must set this value later.
   * @param cq_id id of this entry in the queue
   * @param user user to run the jobs as
   */
  @RetrySemantics.Idempotent
  public void setRunAs(long cq_id, String user) throws MetaException;

  /**
   * This will grab the next compaction request off of
   * the queue, and assign it to the worker.
   * @param workerId id of the worker calling this, will be recorded in the db
   * @return an info element for this compaction request, or null if there is no work to do now.
   */
  @RetrySemantics.ReadOnly
  public CompactionInfo findNextToCompact(String workerId) throws MetaException;

  /**
   * This will mark an entry in the queue as compacted
   * and put it in the ready to clean state.
   * @param info info on the compaction entry to mark as compacted.
   */
  @RetrySemantics.SafeToRetry
  public void markCompacted(CompactionInfo info) throws MetaException;

  /**
   * Find entries in the queue that are ready to
   * be cleaned.
   * @return information on the entry in the queue.
   */
  @RetrySemantics.ReadOnly
  public List<CompactionInfo> findReadyToClean() throws MetaException;

  /**
   * This will remove an entry from the queue after
   * it has been compacted.
   * 
   * @param info info on the compaction entry to remove
   */
  @RetrySemantics.CannotRetry
  public void markCleaned(CompactionInfo info) throws MetaException;

  /**
   * Mark a compaction entry as failed.  This will move it to the compaction history queue with a
   * failed status.  It will NOT clean up aborted transactions in the table/partition associated
   * with this compaction.
   * @param info information on the compaction that failed.
   * @throws MetaException
   */
  @RetrySemantics.CannotRetry
  public void markFailed(CompactionInfo info) throws MetaException;

  /**
   * Clean up aborted transactions from txns that have no components in txn_components.  The reson such
   * txns exist can be that now work was done in this txn (e.g. Streaming opened TransactionBatch and
   * abandoned it w/o doing any work) or due to {@link #markCleaned(CompactionInfo)} being called.
   */
  @RetrySemantics.SafeToRetry
  public void cleanEmptyAbortedTxns() throws MetaException;

  /**
   * This will take all entries assigned to workers
   * on a host return them to INITIATED state.  The initiator should use this at start up to
   * clean entries from any workers that were in the middle of compacting when the metastore
   * shutdown.  It does not reset entries from worker threads on other hosts as those may still
   * be working.
   * @param hostname Name of this host.  It is assumed this prefixes the thread's worker id,
   *                 so that like hostname% will match the worker id.
   */
  @RetrySemantics.Idempotent
  public void revokeFromLocalWorkers(String hostname) throws MetaException;

  /**
   * This call will return all compaction queue
   * entries assigned to a worker but over the timeout back to the initiated state.
   * This should be called by the initiator on start up and occasionally when running to clean up
   * after dead threads.  At start up {@link #revokeFromLocalWorkers(String)} should be called
   * first.
   * @param timeout number of milliseconds since start time that should elapse before a worker is
   *                declared dead.
   */
  @RetrySemantics.Idempotent
  public void revokeTimedoutWorkers(long timeout) throws MetaException;

  /**
   * Queries metastore DB directly to find columns in the table which have statistics information.
   * If {@code ci} includes partition info then per partition stats info is examined, otherwise
   * table level stats are examined.
   * @throws MetaException
   */
  @RetrySemantics.ReadOnly
  public List<String> findColumnsWithStats(CompactionInfo ci) throws MetaException;

  /**
   * Record the highest txn id that the {@code ci} compaction job will pay attention to.
   */
  @RetrySemantics.Idempotent
  public void setCompactionHighestTxnId(CompactionInfo ci, long highestTxnId) throws MetaException;

  /**
   * For any given compactable entity (partition, table if not partitioned) the history of compactions
   * may look like "sssfffaaasffss", for example.  The idea is to retain the tail (most recent) of the
   * history such that a configurable number of each type of state is present.  Any other entries
   * can be purged.  This scheme has advantage of always retaining the last failure/success even if
   * it's not recent.
   * @throws MetaException
   */
  @RetrySemantics.SafeToRetry
  public void purgeCompactionHistory() throws MetaException;

  /**
   * WriteSet tracking is used to ensure proper transaction isolation.  This method deletes the 
   * transaction metadata once it becomes unnecessary.  
   */
  @RetrySemantics.SafeToRetry
  public void performWriteSetGC();

  /**
   * Determine if there are enough consecutive failures compacting a table or partition that no
   * new automatic compactions should be scheduled.  User initiated compactions do not do this
   * check.
   * @param ci  Table or partition to check.
   * @return true if it is ok to compact, false if there have been too many failures.
   * @throws MetaException
   */
  @RetrySemantics.ReadOnly
  public boolean checkFailedCompactions(CompactionInfo ci) throws MetaException;

  @VisibleForTesting
  public int numLocksInLockTable() throws SQLException, MetaException;

  @VisibleForTesting
  long setTimeout(long milliseconds);

  @RetrySemantics.Idempotent
  public MutexAPI getMutexAPI();

  /**
   * This is primarily designed to provide coarse grained mutex support to operations running
   * inside the Metastore (of which there could be several instances).  The initial goal is to 
   * ensure that various sub-processes of the Compactor don't step on each other.
   * 
   * In RDMBS world each {@code LockHandle} uses a java.sql.Connection so use it sparingly.
   */
  public static interface MutexAPI {
    /**
     * The {@code key} is name of the lock. Will acquire and exclusive lock or block.  It retuns
     * a handle which must be used to release the lock.  Each invocation returns a new handle.
     */
    public LockHandle acquireLock(String key) throws MetaException;

    /**
     * Same as {@link #acquireLock(String)} but takes an already existing handle as input.  This 
     * will associate the lock on {@code key} with the same handle.  All locks associated with
     * the same handle will be released together.
     * @param handle not NULL
     */
    public void acquireLock(String key, LockHandle handle) throws MetaException;
    public static interface LockHandle {
      /**
       * Releases all locks associated with this handle.
       */
      public void releaseLocks();
    }
  }

  /**
   * Once a {@link java.util.concurrent.ThreadPoolExecutor.Worker} submits a job to the cluster,
   * it calls this to update the metadata.
   * @param id {@link CompactionInfo#id}
   */
  @RetrySemantics.Idempotent
  public void setHadoopJobId(String hadoopJobId, long id);
}
