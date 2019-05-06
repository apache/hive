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

import javax.annotation.Nullable;
import java.io.InputStream;
import java.util.List;
import java.util.Set;

/**
 * Common interface for transaction in HiveStreamingConnection.
 */
public interface StreamingTransaction {
  /**
   * get ready for the next transaction.
   * @throws StreamingException
   */
  void beginNextTransaction() throws StreamingException;

  /**
   * commit transaction.
   * @throws StreamingException
   */
  void commit() throws StreamingException;

  /**
   * Commit transaction and sent to the metastore the created partitions
   * in the process.
   * @param partitions to commit.
   * @throws StreamingException
   */
  void commit(@Nullable Set<String> partitions) throws StreamingException;

  /**
   * Commits atomically together with a key and a value.
   * @param partitions to commit.
   * @param key to commit.
   * @param value to commit.
   * @throws StreamingException
   */
  void commit(@Nullable Set<String> partitions, @Nullable String key,
      @Nullable String value) throws StreamingException;

  /**
   * Abort a transaction.
   * @throws StreamingException
   */
  void abort() throws StreamingException;

  /**
   * Write data withing a transaction. This expectects beginNextTransaction
   * to have been called before this and commit to be called after.
   * @param record bytes to write.
   * @throws StreamingException
   */
  void write(byte[] record) throws StreamingException;

  /**
   * Write data within a transaction.
   * @param stream stream to write.
   * @throws StreamingException
   */
  void write(InputStream stream) throws StreamingException;

  /**
   * Free/close resources used by the streaming transaction.
   * @throws StreamingException
   */
  void close() throws StreamingException;

  /**
   * @return true if closed.
   */
  boolean isClosed();

  /**
   * @return the state of the current transaction.
   */
  HiveStreamingConnection.TxnState getCurrentTransactionState();

  /**
   * @return remaining number of transactions
   */
  int remainingTransactions();

  /**
   * @return the current transaction id being used
   */
  long getCurrentTxnId();

  /**
   * @return the current write id being used
   */
  long getCurrentWriteId();

  /**
   * Get the partitions that were used in this transaction. They may have
   * been created
   * @return list of partitions
   */
  Set<String> getPartitions();

  /**
   * @return get the paris for transaction ids &lt;--&gt; write ids
   */
  List<TxnToWriteId> getTxnToWriteIds();
}
