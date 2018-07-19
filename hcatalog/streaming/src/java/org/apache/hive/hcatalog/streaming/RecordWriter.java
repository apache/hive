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


/**
 * @deprecated as of Hive 3.0.0, replaced by {@link org.apache.hive.streaming.RecordWriter}
 */
@Deprecated
public interface RecordWriter {

  /** Writes using a hive RecordUpdater
   *
   * @param writeId the write ID of the table mapping to Txn in which the write occurs
   * @param record the record to be written
   */
  void write(long writeId, byte[] record) throws StreamingException;

  /** Flush records from buffer. Invoked by TransactionBatch.commit() */
  void flush() throws StreamingException;

  /** Clear bufferred writes. Invoked by TransactionBatch.abort() */
  void clear() throws StreamingException;

  /** Acquire a new RecordUpdater. Invoked when
   * StreamingConnection.fetchTransactionBatch() is called */
  void newBatch(Long minWriteId, Long maxWriteID) throws StreamingException;

  /** Close the RecordUpdater. Invoked by TransactionBatch.close() */
  void closeBatch() throws StreamingException;
}
