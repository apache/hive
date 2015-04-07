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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.metastore.hbase;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hbase.client.HTableInterface;

import java.io.IOException;
import java.util.List;

/**
 * A connection to HBase.  Separated out as an interface so we can slide different transaction
 * managers between our code and HBase.
 */
public interface HBaseConnection extends Configurable {

  /**
   * Connects to HBase.  This must be called after {@link #setConf} has been called.
   * @throws IOException
   */
  void connect() throws IOException;

  /**
   * Close the connection.  No further operations are possible after this is done.
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * Begin a transaction.
   * @throws IOException
   */
  void beginTransaction() throws IOException;

  /**
   * Commit a transaction
   * @throws IOException indicates the commit has failed
   */
  void commitTransaction() throws IOException;

  /**
   * Rollback a transaction
   * @throws IOException
   */
  void rollbackTransaction() throws IOException;

  /**
   * Flush commits.  A no-op for transaction implementations since they will write at commit time.
   * @param htab Table to flush
   * @throws IOException
   */
  void flush(HTableInterface htab) throws IOException;

  /**
   * Create a new table
   * @param tableName name of the table
   * @param columnFamilies name of the column families in the table
   * @throws IOException
   */
  void createHBaseTable(String tableName, List<byte[]> columnFamilies) throws IOException;

  /**
   * Fetch an existing HBase table.
   * @param tableName name of the table
   * @return table handle
   * @throws IOException
   */
  HTableInterface getHBaseTable(String tableName) throws IOException;

  /**
   * Fetch an existing HBase table and force a connection to it.  This should be used only in
   * cases where you want to assure that the table exists (ie at install).
   * @param tableName name of the table
   * @param force if true, force a connection by fetching a non-existant key
   * @return table handle
   * @throws IOException
   */
  HTableInterface getHBaseTable(String tableName, boolean force) throws IOException;

}
