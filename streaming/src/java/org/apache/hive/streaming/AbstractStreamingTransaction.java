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

import org.apache.hadoop.hive.metastore.api.TxnToWriteId;

import java.io.InputStream;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Common methods for the implementing classes.
 */
abstract class AbstractStreamingTransaction
    implements StreamingTransaction {

  /**
   * This variable should be initialized by the children.
   */
  protected RecordWriter recordWriter;

  /**
   * This variable should be initialized by the children.
   */
  protected List<TxnToWriteId> txnToWriteIds;


  /**
   * once any operation on this batch encounters a system exception
   * (e.g. IOException on write) it's safest to assume that we can't write to the
   * file backing this batch any more.  This guards important public methods
   */
  protected final AtomicBoolean isTxnClosed = new AtomicBoolean(false);

  protected int currentTxnIndex = -1;
  protected HiveStreamingConnection.TxnState state;


  protected void checkIsClosed() throws StreamingException {
    if (isTxnClosed.get()) {
      throw new StreamingException("Transaction" + toString() + " is closed()");
    }
  }

  protected void beginNextTransactionImpl(String errorMessage)
      throws StreamingException{
    state = HiveStreamingConnection.TxnState.INACTIVE; //clear state from previous txn
    if ((currentTxnIndex + 1) >= txnToWriteIds.size()) {
      throw new InvalidTransactionState(errorMessage);
    }
    currentTxnIndex++;
    state = HiveStreamingConnection.TxnState.OPEN;
  }

  public void write(final byte[] record) throws StreamingException {
    checkIsClosed();
    boolean success = false;
    try {
      recordWriter.write(getCurrentWriteId(), record);
      success = true;
    } catch (SerializationError ex) {
      //this exception indicates that a {@code record} could not be parsed and the
      //caller can decide whether to drop it or send it to dead letter queue.
      //rolling back the txn and retrying won't help since the tuple will be exactly the same
      //when it's replayed.
      success = true;
      throw ex;
    } finally {
      markDead(success);
    }
  }

  public void write(final InputStream inputStream) throws StreamingException {
    checkIsClosed();
    boolean success = false;
    try {
      recordWriter.write(getCurrentWriteId(), inputStream);
      success = true;
    } catch (SerializationError ex) {
      //this exception indicates that a {@code record} could not be parsed and the
      //caller can decide whether to drop it or send it to dead letter queue.
      //rolling back the txn and retrying won'table help since the tuple will be exactly the same
      //when it's replayed.
      success = true;
      throw ex;
    } finally {
      markDead(success);
    }
  }

  /**
   * A transaction batch opens a single HDFS file and writes multiple transaction to it.  If there is any issue
   * with the write, we can't continue to write to the same file any as it may be corrupted now (at the tail).
   * This ensures that a client can't ignore these failures and continue to write.
   */
  protected void markDead(boolean success) throws StreamingException {
    if (success) {
      return;
    }
    close();
  }

  public long getCurrentWriteId() {
    if (currentTxnIndex >= 0) {
      return txnToWriteIds.get(currentTxnIndex).getWriteId();
    }
    return -1L;
  }

  public int remainingTransactions() {
    if (currentTxnIndex >= 0) {
      return txnToWriteIds.size() - currentTxnIndex - 1;
    }
    return txnToWriteIds.size();
  }

  public boolean isClosed() {
    return isTxnClosed.get();
  }

  public HiveStreamingConnection.TxnState getCurrentTransactionState() {
    return state;
  }

  public long getCurrentTxnId() {
    if (currentTxnIndex >= 0) {
      return txnToWriteIds.get(currentTxnIndex).getTxnId();
    }
    return -1L;
  }

  @Override
  public List<TxnToWriteId> getTxnToWriteIds() {
    return txnToWriteIds;
  }

  public void commit() throws StreamingException {
    commit(null);
  }
  public void commit(Set<String> partitions) throws StreamingException {
    commit(partitions, null, null);
  }
}
