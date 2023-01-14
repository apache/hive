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

package org.apache.hive.streaming;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.AbortTxnRequest;
import org.apache.hadoop.hive.metastore.api.DataOperationType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HeartbeatTxnRangeResponse;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnToWriteId;
import org.apache.hadoop.hive.metastore.txn.TxnErrorMsg;
import org.apache.hadoop.hive.ql.lockmgr.DbTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Streaming transaction to use most of the times. Will query the
 * metastore to get the transaction ids and the writer ids and then
 * will commit them.
 */
public class TransactionBatch extends AbstractStreamingTransaction {
  private static final Logger LOG = LoggerFactory.getLogger(
      TransactionBatch.class.getName());
  private static final int DEFAULT_HEARTBEAT_INTERVAL = 60 * 1000;
  protected Set<String> createdPartitions = null;
  private String username;
  private HiveStreamingConnection conn;
  private ScheduledExecutorService scheduledExecutorService;
  private String partNameForLock = null;
  private LockRequest lockRequest = null;
  // heartbeats can only be sent for open transactions.
  // there is a race between committing/aborting a transaction and heartbeat.
  // Example: If a heartbeat is sent for committed txn, exception will be thrown.
  // Similarly if we don't send a heartbeat, metastore server might abort a txn
  // for missed heartbeat right before commit txn call.
  // This lock is used to mutex commit/abort and heartbeat calls
  private final ReentrantLock transactionLock = new ReentrantLock();
  // min txn id is incremented linearly within a transaction batch.
  // keeping minTxnId atomic as it is shared with heartbeat thread
  private final AtomicLong minTxnId;
  // max txn id does not change for a transaction batch
  private final long maxTxnId;

  private String agentInfo;
  private int numTxns;

  /**
   * Id of the table from the streaming connection.
   */
  private final long tableId;
  /**
   * Tracks the state of each transaction.
   */
  private HiveStreamingConnection.TxnState[] txnStatus;
  /**
   * ID of the last txn used by {@link #beginNextTransactionImpl()}.
   */
  private long lastTxnUsed;

  /**
   * Represents a batch of transactions acquired from MetaStore.
   *
   * @param conn - hive streaming connection
   * @throws StreamingException if failed to create new RecordUpdater for batch
   */
  public TransactionBatch(HiveStreamingConnection conn) throws StreamingException {
    boolean success = false;
    try {
      if (conn.isPartitionedTable() && !conn.isDynamicPartitioning()) {
        List<FieldSchema> partKeys = conn.getTable().getPartitionKeys();
        partNameForLock = Warehouse.makePartName(partKeys, conn.getStaticPartitionValues());
      }
      this.conn = conn;
      this.username = conn.getUsername();
      this.recordWriter = conn.getRecordWriter();
      this.agentInfo = conn.getAgentInfo();
      this.numTxns = conn.getTransactionBatchSize();
      this.tableId = conn.getTable().getTTable().getId();

      List<Long> txnIds = openTxnImpl(username, numTxns);
      txnToWriteIds = allocateWriteIdsImpl(txnIds);
      assert (txnToWriteIds.size() == numTxns);

      txnStatus = new HiveStreamingConnection.TxnState[numTxns];
      for (int i = 0; i < txnStatus.length; i++) {
        assert (txnToWriteIds.get(i).getTxnId() == txnIds.get(i));
        txnStatus[i] = HiveStreamingConnection.TxnState.OPEN; //Open matches Metastore state
      }
      this.state = HiveStreamingConnection.TxnState.INACTIVE;

      // initialize record writer with connection and write id info
      recordWriter.init(conn, txnToWriteIds.get(0).getWriteId(),
          txnToWriteIds.get(numTxns - 1).getWriteId(), conn.getStatementId());
      this.minTxnId = new AtomicLong(txnIds.get(0));
      this.maxTxnId = txnIds.get(txnIds.size() - 1);
      setupHeartBeatThread();
      success = true;
    } catch (TException e) {
      throw new StreamingException(conn.toString(), e);
    } finally {
      //clean up if above throws
      markDead(success);
    }
  }

  private static class HeartbeatRunnable implements Runnable {
    private final HiveStreamingConnection conn;
    private final AtomicLong minTxnId;
    private final long maxTxnId;
    private final ReentrantLock transactionLock;
    private final AtomicBoolean isTxnClosed;

    HeartbeatRunnable(final HiveStreamingConnection conn, final AtomicLong minTxnId, final long maxTxnId,
        final ReentrantLock transactionLock, final AtomicBoolean isTxnClosed) {
      this.conn = conn;
      this.minTxnId = minTxnId;
      this.maxTxnId = maxTxnId;
      this.transactionLock = transactionLock;
      this.isTxnClosed = isTxnClosed;
    }

    @Override
    public void run() {
      transactionLock.lock();
      try {
        if (minTxnId.get() > 0) {
          HeartbeatTxnRangeResponse resp = conn.getHeatbeatMSC().heartbeatTxnRange(minTxnId.get(), maxTxnId);
          if (!resp.getAborted().isEmpty() || !resp.getNosuch().isEmpty()) {
            LOG.error("Heartbeat failure: {}", resp.toString());
            isTxnClosed.set(true);
          } else {
            LOG.info("Heartbeat sent for range: [{}-{}]", minTxnId.get(), maxTxnId);
          }
        }
      } catch (TException e) {
        LOG.warn("Failure to heartbeat for transaction range: [" + minTxnId.get() + "-" + maxTxnId + "]", e);
      } finally {
        transactionLock.unlock();
      }
    }
  }

  private void setupHeartBeatThread() {
    // start heartbeat thread
    ThreadFactory threadFactory = new ThreadFactoryBuilder().setDaemon(true)
        .setNameFormat("HiveStreamingConnection-Heartbeat-Thread")
        .build();
    this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(threadFactory);
    long heartBeatInterval;
    long initialDelay;
    try {
      // if HIVE_TXN_TIMEOUT is defined, heartbeat interval will be HIVE_TXN_TIMEOUT/2
      heartBeatInterval = DbTxnManager.getHeartbeatInterval(conn.getConf());
    } catch (LockException e) {
      heartBeatInterval = DEFAULT_HEARTBEAT_INTERVAL;
    }
    // to introduce some randomness and to avoid hammering the metastore at the same time (same logic as DbTxnManager)
    initialDelay = (long) (heartBeatInterval * 0.75 * Math.random());
    LOG.info("Starting heartbeat thread with interval: {} ms initialDelay: {} ms for agentInfo: {}",
        heartBeatInterval, initialDelay, conn.getAgentInfo());
    Runnable runnable = new HeartbeatRunnable(conn, minTxnId, maxTxnId, transactionLock, isTxnClosed);
    this.scheduledExecutorService.scheduleWithFixedDelay(runnable, initialDelay, heartBeatInterval, TimeUnit
        .MILLISECONDS);
  }

  private List<Long> openTxnImpl(final String user, final int numTxns) throws TException {
    return conn.getMSC().openTxns(user, numTxns).getTxn_ids();
  }

  private List<TxnToWriteId> allocateWriteIdsImpl(final List<Long> txnIds) throws TException {
    return conn.getMSC().allocateTableWriteIdsBatch(txnIds, conn.getDatabase(),
        conn.getTable().getTableName());
  }

  @Override
  public String toString() {
    if (txnToWriteIds == null || txnToWriteIds.isEmpty()) {
      return "{}";
    }
    StringBuilder sb = new StringBuilder(" TxnStatus[");
    for (HiveStreamingConnection.TxnState state : txnStatus) {
      //'state' should not be null - future proofing
      sb.append(state == null ? "N" : state);
    }
    sb.append("] LastUsed ").append(JavaUtils.txnIdToString(lastTxnUsed));
    return "TxnId/WriteIds=[" + txnToWriteIds.get(0).getTxnId()
        + "/" + txnToWriteIds.get(0).getWriteId()
        + "..."
        + txnToWriteIds.get(txnToWriteIds.size() - 1).getTxnId()
        + "/" + txnToWriteIds.get(txnToWriteIds.size() - 1).getWriteId()
        + "] on connection = " + conn + "; " + sb;
  }

  public void beginNextTransaction() throws StreamingException {
    checkIsClosed();
    beginNextTransactionImpl();
  }

  private void beginNextTransactionImpl() throws StreamingException {
    beginNextTransactionImpl("No more transactions available in" +
        " next batch for connection: " + conn + " user: " + username);
    lastTxnUsed = getCurrentTxnId();
    lockRequest = createLockRequest(conn, partNameForLock, username, getCurrentTxnId(), agentInfo);
    createdPartitions = Sets.newHashSet();
    try {
      LockResponse res = conn.getMSC().lock(lockRequest);
      if (res.getState() != LockState.ACQUIRED) {
        throw new TransactionError("Unable to acquire lock on " + conn);
      }
    } catch (TException e) {
      throw new TransactionError("Unable to acquire lock on " + conn, e);
    }
  }

  public void commit(Set<String> partitions, String key, String value)
      throws StreamingException {
    checkIsClosed();
    boolean success = false;
    try {
      commitImpl(partitions, key, value);
      success = true;
    } finally {
      markDead(success);
    }
  }

  private void commitImpl(Set<String> partitions, String key, String value)
      throws StreamingException {
    try {
      if ((key == null && value != null) || (key != null && value == null)) {
        throw new StreamingException(String.format(
            "If key is set, the value should be as well and vice versa,"
                + " key, value = %s, %s", key, value));
      }
      recordWriter.flush();
      TxnToWriteId txnToWriteId = txnToWriteIds.get(currentTxnIndex);
      if (conn.isDynamicPartitioning()) {
        List<String> partNames = new ArrayList<>(recordWriter.getPartitions());
        createdPartitions.addAll(partNames);
        if (partitions != null) {
          partNames.addAll(partitions);
        }
        if (!partNames.isEmpty()) {
          conn.getMSC().addDynamicPartitions(txnToWriteId.getTxnId(),
              txnToWriteId.getWriteId(), conn.getDatabase(),
              conn.getTable().getTableName(), partNames,
              DataOperationType.INSERT);
        }
      }

      // If it is the last transaction in the batch, then close the files and add write events.
      // We need to close the writer as file checksum can't be obtained on the opened file.
      if ((currentTxnIndex + 1) >= txnToWriteIds.size()) {
        // Replication doesn't work if txn batch size > 1 and the last txn is aborted as write events
        // are ignored by abort txn event causing data loss. However, if the last txn in the batch is
        // committed, then data gets replicated making eventually consistent, but doesn't guarantee
        // point-in-time consistency.
        recordWriter.close();

        // Add write notification events if it is enabled.
        conn.addWriteNotificationEvents();
      }

      transactionLock.lock();
      try {
        if (key != null) {
          conn.getMSC().commitTxnWithKeyValue(txnToWriteId.getTxnId(),
              tableId, key, value);
        } else {
          conn.getMSC().commitTxn(txnToWriteId.getTxnId());
        }
        // increment the min txn id so that heartbeat thread will heartbeat only from the next open transaction.
        // the current transaction is going to committed or fail, so don't need heartbeat for current transaction.
        if (currentTxnIndex + 1 < txnToWriteIds.size()) {
          minTxnId.set(txnToWriteIds.get(currentTxnIndex + 1).getTxnId());
        } else {
          // exhausted the batch, no longer have to heartbeat for current txn batch
          minTxnId.set(-1);
        }
      } finally {
        transactionLock.unlock();
      }
      state = HiveStreamingConnection.TxnState.COMMITTED;
      txnStatus[currentTxnIndex] = HiveStreamingConnection.TxnState.COMMITTED;
    } catch (NoSuchTxnException e) {
      throw new TransactionError("Invalid transaction id : "
          + getCurrentTxnId(), e);
    } catch (TxnAbortedException e) {
      throw new TransactionError("Aborted transaction "
          + "cannot be committed", e);
    } catch (TException e) {
      throw new TransactionError("Unable to commitTransaction transaction"
          + getCurrentTxnId(), e);
    }
  }

  public void abort() throws StreamingException {
    if (isTxnClosed.get()) {
      /*
       * isDead is only set internally by this class.  {@link #markDead(boolean)} will abort all
       * remaining txns, so make this no-op to make sure that a well-behaved client that calls abortTransaction()
       * error doesn't get misleading errors
       */
      return;
    }
    abort(false);
  }

  private void abort(final boolean abortAllRemaining) throws StreamingException {
    abortImpl(abortAllRemaining);
  }

  private void abortImpl(boolean abortAllRemaining) throws StreamingException {
    if (minTxnId == null) {
      return;
    }

    transactionLock.lock();
    try {
      if (abortAllRemaining) {
        // we are aborting all txns in the current batch, so no need to heartbeat
        minTxnId.set(-1);
        //when last txn finished (abortTransaction/commitTransaction) the currentTxnIndex is pointing at that txn
        //so we need to start from next one, if any.  Also if batch was created but
        //fetchTransactionBatch() was never called, we want to start with first txn
        int minOpenTxnIndex = Math.max(currentTxnIndex +
            (state == HiveStreamingConnection.TxnState.ABORTED
                || state == HiveStreamingConnection.TxnState.COMMITTED
                ? 1 : 0), 0);
        for (currentTxnIndex = minOpenTxnIndex;
             currentTxnIndex < txnToWriteIds.size(); currentTxnIndex++) {
          AbortTxnRequest abortTxnRequest = new AbortTxnRequest(txnToWriteIds.get(currentTxnIndex).getTxnId());
          abortTxnRequest.setErrorCode(TxnErrorMsg.ABORT_ROLLBACK.getErrorCode());
          conn.getMSC().rollbackTxn(abortTxnRequest);
          txnStatus[currentTxnIndex] = HiveStreamingConnection.TxnState.ABORTED;
        }
        currentTxnIndex--; //since the loop left it == txnToWriteIds.size()
      } else {
        // we are aborting only the current transaction, so move the min range for heartbeat or disable heartbeat
        // if the current txn is last in the batch.
        if (currentTxnIndex + 1 < txnToWriteIds.size()) {
          minTxnId.set(txnToWriteIds.get(currentTxnIndex + 1).getTxnId());
        } else {
          // exhausted the batch, no longer have to heartbeat
          minTxnId.set(-1);
        }
        long currTxnId = getCurrentTxnId();
        if (currTxnId > 0) {
          AbortTxnRequest abortTxnRequest = new AbortTxnRequest(currTxnId);
          abortTxnRequest.setErrorCode(TxnErrorMsg.ABORT_ROLLBACK.getErrorCode());
          conn.getMSC().rollbackTxn(abortTxnRequest);
          txnStatus[currentTxnIndex] = HiveStreamingConnection.TxnState.ABORTED;
        }
      }
      state = HiveStreamingConnection.TxnState.ABORTED;
    } catch (NoSuchTxnException e) {
      throw new TransactionError("Unable to abort invalid transaction id : "
          + getCurrentTxnId(), e);
    } catch (TException e) {
      throw new TransactionError("Unable to abort transaction id : "
          + getCurrentTxnId(), e);
    } finally {
      transactionLock.unlock();
    }
  }

  /**
   * Close the TransactionBatch.  This will abort any still open txns in this batch.
   *
   * @throws StreamingException - failure when closing transaction batch
   */
  public void close() throws StreamingException {
    if (isClosed()) {
      return;
    }
    isTxnClosed.set(true); //also ensures that heartbeat() is no-op since client is likely doing it async
    try {
      abort(true); //abort all remaining txns
    } catch (Exception ex) {
      LOG.error("Fatal error on " + toString() + "; cause " + ex.getMessage(), ex);
      throw new StreamingException("Unable to abort", ex);
    }
    try {
      closeImpl();
    } catch (Exception ex) {
      LOG.error("Fatal error on " + toString() + "; cause " + ex.getMessage(), ex);
      throw new StreamingException("Unable to close", ex);
    }
  }

  private void closeImpl() throws StreamingException {
    state = HiveStreamingConnection.TxnState.INACTIVE;
    recordWriter.close();
    if (scheduledExecutorService != null) {
      scheduledExecutorService.shutdownNow();
    }
  }

  private static LockRequest createLockRequest(final HiveStreamingConnection connection,
      String partNameForLock, String user, long txnId, String agentInfo) {
    LockRequestBuilder requestBuilder = new LockRequestBuilder(agentInfo);
    requestBuilder.setUser(user);
    requestBuilder.setTransactionId(txnId);

    LockComponentBuilder lockCompBuilder = new LockComponentBuilder()
        .setDbName(connection.getDatabase())
        .setTableName(connection.getTable().getTableName())
        .setSharedRead()
        .setOperationType(DataOperationType.INSERT);
    if (connection.isDynamicPartitioning()) {
      lockCompBuilder.setIsDynamicPartitionWrite(true);
    }
    if (partNameForLock != null && !partNameForLock.isEmpty()) {
      lockCompBuilder.setPartitionName(partNameForLock);
    }
    requestBuilder.addLockComponent(lockCompBuilder.build());

    return requestBuilder.build();
  }

  /**
   * @return the list of created partitions.
   */
  @Override
  public Set<String> getPartitions() {
    return createdPartitions;
  }
}
