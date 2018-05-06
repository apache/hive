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

import org.apache.hadoop.hive.conf.HiveConf;

public interface StreamingConnection extends ConnectionInfo, PartitionHandler {
  /**
   * Returns hive configuration object used during connection creation.
   *
   * @return - hive conf
   */
  HiveConf getHiveConf();

  /**
   * Begin a transaction for writing.
   *
   * @throws StreamingException - if there are errors when beginning transaction
   */
  void beginTransaction() throws StreamingException;

  /**
   * Write record using RecordWriter.
   *
   * @param record - the data to be written
   * @throws StreamingException - if there are errors when writing
   */
  void write(byte[] record) throws StreamingException;

  /**
   * Write record using RecordWriter.
   *
   * @param inputStream - input stream of records
   * @throws StreamingException - if there are errors when writing
   */
  void write(InputStream inputStream) throws StreamingException;

  /**
   * Commit a transaction to make the writes visible for readers.
   *
   * @throws StreamingException - if there are errors when committing the open transaction
   */
  void commitTransaction() throws StreamingException;

  /**
   * Manually abort the opened transaction.
   *
   * @throws StreamingException - if there are errors when aborting the transaction
   */
  void abortTransaction() throws StreamingException;

  /**
   * Closes streaming connection.
   */
  void close();

  /**
   * Gets stats about the streaming connection.
   *
   * @return - connection stats
   */
  ConnectionStats getConnectionStats();
}
