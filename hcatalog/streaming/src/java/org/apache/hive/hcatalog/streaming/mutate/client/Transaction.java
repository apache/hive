/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.hcatalog.streaming.mutate.client;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hive.hcatalog.streaming.TransactionBatch.TxnState;
import org.apache.hive.hcatalog.streaming.mutate.client.lock.Lock;
import org.apache.hive.hcatalog.streaming.mutate.client.lock.LockException;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Transaction {

  private static final Logger LOG = LoggerFactory.getLogger(Transaction.class);

  private final Lock lock;
  private final IMetaStoreClient metaStoreClient;
  private final long transactionId;

  private TxnState state;

  Transaction(IMetaStoreClient metaStoreClient, Lock.Options lockOptions) throws TransactionException {
    this(metaStoreClient, new Lock(metaStoreClient, lockOptions));
  }

  /** Visible for testing only. */
  Transaction(IMetaStoreClient metaStoreClient, Lock lock) throws TransactionException {
    this.metaStoreClient = metaStoreClient;
    this.lock = lock;
    transactionId = open(lock.getUser());
  }

  public long getTransactionId() {
    return transactionId;
  }

  public TxnState getState() {
    return state;
  }

  /**
   * Begin the transaction. Acquires a {@link Lock} for the transaction and {@link AcidTable AcidTables}.
   */
  public void begin() throws TransactionException {
    try {
      lock.acquire(transactionId);
    } catch (LockException e) {
      throw new TransactionException("Unable to acquire lock for transaction: " + transactionId, e);
    }
    state = TxnState.OPEN;
    LOG.debug("Begin. Transaction id: {}", transactionId);
  }

  /** Commits the transaction. Releases the {@link Lock}. */
  public void commit() throws TransactionException {
    try {
      lock.release();
    } catch (LockException e) {
      // This appears to leave the remove transaction in an inconsistent state but the heartbeat is now
      // cancelled and it will eventually time out
      throw new TransactionException("Unable to release lock: " + lock + " for transaction: " + transactionId, e);
    }
    try {
      metaStoreClient.commitTxn(transactionId);
      state = TxnState.COMMITTED;
    } catch (NoSuchTxnException e) {
      throw new TransactionException("Invalid transaction id: " + transactionId, e);
    } catch (TxnAbortedException e) {
      throw new TransactionException("Aborted transaction cannot be committed: " + transactionId, e);
    } catch (TException e) {
      throw new TransactionException("Unable to commit transaction: " + transactionId, e);
    }
    LOG.debug("Committed. Transaction id: {}", transactionId);
  }

  /** Aborts the transaction. Releases the {@link Lock}. */
  public void abort() throws TransactionException {
    try {
      lock.release();
    } catch (LockException e) {
      // This appears to leave the remove transaction in an inconsistent state but the heartbeat is now
      // cancelled and it will eventually time out
      throw new TransactionException("Unable to release lock: " + lock + " for transaction: " + transactionId, e);
    }
    try {
      metaStoreClient.rollbackTxn(transactionId);
      state = TxnState.ABORTED;
    } catch (NoSuchTxnException e) {
      throw new TransactionException("Unable to abort invalid transaction id : " + transactionId, e);
    } catch (TException e) {
      throw new TransactionException("Unable to abort transaction id : " + transactionId, e);
    }
    LOG.debug("Aborted. Transaction id: {}", transactionId);
  }

  @Override
  public String toString() {
    return "Transaction [transactionId=" + transactionId + ", state=" + state + "]";
  }

  private long open(String user) throws TransactionException {
    long transactionId = -1;
    try {
      transactionId = metaStoreClient.openTxn(user);
      state = TxnState.INACTIVE;
    } catch (TException e) {
      throw new TransactionException("Unable to open transaction for user: " + user, e);
    }
    LOG.debug("Opened transaction with id: {}", transactionId);
    return transactionId;
  }

}
