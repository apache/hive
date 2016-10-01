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

package org.apache.hive.hcatalog.streaming;

import org.apache.hadoop.security.UserGroupInformation;

/**
 * Represents a connection to a HiveEndPoint. Used to acquire transaction batches.
 * Note: the expectation is that there is at most 1 TransactionBatch outstanding for any given
 * StreamingConnection.  Violating this may result in "out of sequence response".
 */
public interface StreamingConnection {

  /**
   * Acquires a new batch of transactions from Hive.

   * @param numTransactionsHint is a hint from client indicating how many transactions client needs.
   * @param writer  Used to write record. The same writer instance can
   *                      be shared with another TransactionBatch (to the same endpoint)
   *                      only after the first TransactionBatch has been closed.
   *                      Writer will be closed when the TransactionBatch is closed.
   * @return
   * @throws ConnectionError
   * @throws InvalidPartition
   * @throws StreamingException
   * @return a batch of transactions
   */
  public TransactionBatch fetchTransactionBatch(int numTransactionsHint,
                                                RecordWriter writer)
          throws ConnectionError, StreamingException, InterruptedException;

  /**
   * Close connection
   */
  public void close();

  /**
   * @return UserGroupInformation associated with this connection or {@code null} if there is none
   */
  UserGroupInformation getUserGroupInformation();
}
