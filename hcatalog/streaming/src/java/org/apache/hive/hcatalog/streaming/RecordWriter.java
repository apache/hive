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


public interface RecordWriter {

  /** Writes using a hive RecordUpdater
   *
   * @param transactionId the ID of the Txn in which the write occurs
   * @param record the record to be written
   */
  public void write(long transactionId, byte[] record) throws StreamingException;

  /** Flush records from buffer. Invoked by TransactionBatch.commit() */
  public void flush() throws StreamingException;

  /** Clear bufferred writes. Invoked by TransactionBatch.abort() */
  public void clear() throws StreamingException;

  /** Acquire a new RecordUpdater. Invoked when
   * StreamingConnection.fetchTransactionBatch() is called */
  public void newBatch(Long minTxnId, Long maxTxnID) throws StreamingException;

  /** Close the RecordUpdater. Invoked by TransactionBatch.close() */
  public void closeBatch() throws StreamingException;
}
