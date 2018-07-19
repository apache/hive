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


import java.io.InputStream;

import java.util.Set;

public interface RecordWriter {

  /**
   * Initialize record writer.
   *
   * @param connection - streaming connection
   * @param minWriteId - min write id
   * @param maxWriteID - max write id
   * @throws StreamingException - thrown when initialization failed
   */
  void init(StreamingConnection connection, long minWriteId, long maxWriteID) throws StreamingException;

  /**
   * Writes using a hive RecordUpdater.
   *
   * @param writeId - the write ID of the table mapping to Txn in which the write occurs
   * @param record  - the record to be written
   * @throws StreamingException - thrown when write fails
   */
  void write(long writeId, byte[] record) throws StreamingException;

  /**
   * Writes using a hive RecordUpdater. The specified input stream will be automatically closed
   * by the API after reading all the records out of it.
   *
   * @param writeId     - the write ID of the table mapping to Txn in which the write occurs
   * @param inputStream - the record to be written
   * @throws StreamingException - thrown when write fails
   */
  void write(long writeId, InputStream inputStream) throws StreamingException;

  /**
   * Flush records from buffer. Invoked by TransactionBatch.commitTransaction()
   *
   * @throws StreamingException - thrown when flush fails
   */
  void flush() throws StreamingException;

  /**
   * Close the RecordUpdater. Invoked by TransactionBatch.close()
   *
   * @throws StreamingException - thrown when record writer cannot be closed.
   */
  void close() throws StreamingException;

  /**
   * Get the set of partitions that were added by the record writer.
   *
   * @return - set of partitions
   */
  Set<String> getPartitions();
}
