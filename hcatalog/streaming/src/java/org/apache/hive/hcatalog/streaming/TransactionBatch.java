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

package org.apache.hive.hcatalog.streaming;


import java.util.Collection;

/**
 * Represents a set of Transactions returned by Hive. Supports opening, writing to
 * and commiting/aborting each transaction. The interface is designed to ensure
 * transactions in a batch are used up sequentially. To stream to the same HiveEndPoint
 * concurrently, create separate StreamingConnections.
 *
 * Note on thread safety: At most 2 threads can run through a given TransactionBatch at the same
 * time.  One thread may call {@link #heartbeat()} and the other all other methods.
 * Violating this may result in "out of sequence response".
 *
 */
public interface TransactionBatch  {
  enum TxnState {
    INACTIVE("I"), OPEN("O"), COMMITTED("C"), ABORTED("A");

    private final String code;
    TxnState(String code) {
      this.code = code;
    };
    public String toString() {
      return code;
    }
  }

  /**
   * Activate the next available transaction in the current transaction batch
   * @throws StreamingException if not able to switch to next Txn
   * @throws InterruptedException if call in interrupted
   */
  public void beginNextTransaction() throws StreamingException, InterruptedException;

  /**
   * Get Id of currently open transaction
   * @return transaction id
   */
  public Long getCurrentTxnId();

  /**
   * get state of current transaction
   */
  public TxnState getCurrentTransactionState();

  /**
   * Commit the currently open transaction
   * @throws StreamingException if there are errors committing
   * @throws InterruptedException if call in interrupted
   */
  public void commit() throws StreamingException, InterruptedException;

  /**
   * Abort the currently open transaction
   * @throws StreamingException if there are errors
   * @throws InterruptedException if call in interrupted
   */
  public void abort() throws StreamingException, InterruptedException;

  /**
   * Remaining transactions are the ones that are not committed or aborted or open.
   * Current open transaction is not considered part of remaining txns.
   * @return number of transactions remaining this batch.
   */
  public int remainingTransactions();


  /**
   *  Write record using RecordWriter
   * @param record  the data to be written
   * @throws StreamingException if there are errors when writing
   * @throws InterruptedException if call in interrupted
   */
  public void write(byte[] record) throws StreamingException, InterruptedException;

  /**
   *  Write records using RecordWriter
   * @throws StreamingException if there are errors when writing
   * @throws InterruptedException if call in interrupted
   */
  public void write(Collection<byte[]> records) throws StreamingException, InterruptedException;


  /**
   * Issues a heartbeat to hive metastore on the current and remaining txn ids
   * to keep them from expiring
   * @throws StreamingException if there are errors
   */
  public void heartbeat() throws StreamingException;

  /**
   * Close the TransactionBatch
   * @throws StreamingException if there are errors closing batch
   * @throws InterruptedException if call in interrupted
   */
  public void close() throws StreamingException, InterruptedException;
  public boolean isClosed();
}
