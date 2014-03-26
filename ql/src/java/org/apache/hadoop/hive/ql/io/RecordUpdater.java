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

package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import java.io.IOException;

/**
 * API for supporting updating records.
 */
public interface RecordUpdater {

  /**
   * Insert a new record into the table.
   * @param currentTransaction the transaction id of the current transaction.
   * @param row the row of data to insert
   * @throws IOException
   */
  void insert(long currentTransaction,
              Object row) throws IOException;

  /**
   * Update an old record with a new set of values.
   * @param currentTransaction the current transaction id
   * @param originalTransaction the row's original transaction id
   * @param rowId the original row id
   * @param row the new values for the row
   * @throws IOException
   */
  void update(long currentTransaction,
              long originalTransaction,
              long rowId,
              Object row) throws IOException;

  /**
   * Delete a row from the table.
   * @param currentTransaction the current transaction id
   * @param originalTransaction the rows original transaction id
   * @param rowId the original row id
   * @throws IOException
   */
  void delete(long currentTransaction,
              long originalTransaction,
              long rowId) throws IOException;

  /**
   * Flush the current set of rows to the underlying file system, so that
   * they are available to readers. Most implementations will need to write
   * additional state information when this is called, so it should only be
   * called during streaming when a transaction is finished, but the
   * RecordUpdater can't be closed yet.
   * @throws IOException
   */
  void flush() throws IOException;

  /**
   * Close this updater. No further calls are legal after this.
   * @param abort Can the data since the last flush be discarded?
   * @throws IOException
   */
  void close(boolean abort) throws IOException;

  /**
   * Returns the statistics information
   * @return SerDeStats
   */
  SerDeStats getStats();
}
