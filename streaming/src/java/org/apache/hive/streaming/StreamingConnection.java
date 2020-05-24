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
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;

import javax.annotation.Nullable;

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
   * Commits the transaction together with a key value atomically.
   * @param partitions - extra partitions to commit.
   * @param key - key to commit.
   * @param value - value to commit.
   * @throws StreamingException - if there are errors when committing
   * the open transaction.
   */
  default void commitTransaction(@Nullable Set<String> partitions,
      @Nullable String key, @Nullable String value) throws StreamingException {
    throw new UnsupportedOperationException();
  }

    /**
     * Commit a transaction to make the writes visible for readers. Include
     * other partitions that may have been added independently.
     *
     * @param partitions - extra partitions to commit.
     * @throws StreamingException - if there are errors when committing the open transaction.
     */
  default void commitTransaction(@Nullable Set<String> partitions)
      throws StreamingException {
    throw new UnsupportedOperationException();
  }

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

  /**
   * Get the partitions used during the streaming. This partitions haven't
   * been committed to the metastore.
   * @return partitions.
   */
  default Set<String> getPartitions() {
    throw new UnsupportedOperationException();
  }

  /**
   * Returns the file that would be used by the writer to write the rows.
   * given the parameters
   * @param partitionValues partition values
   * @param bucketId bucket id
   * @param minWriteId min write Id
   * @param maxWriteId max write Id
   * @param statementId statement Id
   * @return the location of the file.
   * @throws StreamingException when the path is not found
   */
  default Path getDeltaFileLocation(List<String> partitionValues,
      Integer bucketId, Long minWriteId, Long maxWriteId, Integer statementId)
      throws StreamingException {
    throw new UnsupportedOperationException();
  }

  /**
   * Adds the information of delta directory under which bucket files are created by streaming write.
   * Hive replication uses this information to log write events.
   * @param partitionValues partition values
   * @param writeDir Delta directory under which bucket files are written by streaming
   */
  default void addWriteDirectoryInfo(List<String> partitionValues, Path writeDir) {
  }

  /**
   * Add Write notification events if it is enabled.
   * @throws StreamingException File operation errors or HMS errors.
   */
  default void addWriteNotificationEvents() throws StreamingException {
  }
}
