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
package org.apache.hcatalog.hbase.snapshot;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.List;

/**
 * This interface provides APIs for implementing revision management.
 */
public interface RevisionManager {
  /**
   * Version property required by HBase to use this interface
   * for CoprocessorProtocol / RPC.
   */
  public static final long VERSION = 1L; // do not change

  /**
   * Initialize the revision manager.
   */
  @Deprecated
  public void initialize(Configuration conf);

  /**
   * Opens the revision manager.
   *
   * @throws IOException
   */
  @Deprecated
  public void open() throws IOException;

  /**
   * Closes the revision manager.
   *
   * @throws IOException
   */
  @Deprecated
  public void close() throws IOException;

  /**
   * Setup revision management for a newly created hbase table.
   * @param table the hbase table name
   * @param columnFamilies the column families in the table
   */
  @Deprecated
  public void createTable(String table, List<String> columnFamilies) throws IOException;

  /**
   * Remove table data from revision manager for a dropped table.
   * @param table the hbase table name
   */
  @Deprecated
  public void dropTable(String table) throws IOException;

  /**
   * Start the write transaction.
   *
   * @param table
   * @param families
   * @return a new Transaction
   * @throws IOException
   */
  @Deprecated
  public Transaction beginWriteTransaction(String table, List<String> families)
    throws IOException;

  /**
   * Start the write transaction.
   *
   * @param table
   * @param families
   * @param keepAlive
   * @return a new Transaction
   * @throws IOException
   */
  @Deprecated
  public Transaction beginWriteTransaction(String table,
                       List<String> families, Long keepAlive) throws IOException;

  /**
   * Commit the write transaction.
   *
   * @param transaction
   * @throws IOException
   */
  @Deprecated
  public void commitWriteTransaction(Transaction transaction)
    throws IOException;

  /**
   * Abort the write transaction.
   *
   * @param transaction
   * @throws IOException
   */
  @Deprecated
  public void abortWriteTransaction(Transaction transaction)
    throws IOException;

  /**
   * Get the list of aborted Transactions for a column family
   *
   * @param table the table name
   * @param columnFamily the column family name
   * @return a list of aborted WriteTransactions
   * @throws java.io.IOException
   */
  @Deprecated
  public List<FamilyRevision> getAbortedWriteTransactions(String table,
      String columnFamily) throws IOException;

  /**
   * Create the latest snapshot of the table.
   *
   * @param tableName
   * @return a new snapshot
   * @throws IOException
   */
  @Deprecated
  public TableSnapshot createSnapshot(String tableName) throws IOException;

  /**
   * Create the snapshot of the table using the revision number.
   *
   * @param tableName
   * @param revision
   * @return a new snapshot
   * @throws IOException
   */
  @Deprecated
  public TableSnapshot createSnapshot(String tableName, Long revision)
    throws IOException;

  /**
   * Extends the expiration of a transaction by the time indicated by keep alive.
   *
   * @param transaction
   * @throws IOException
   */
  @Deprecated
  public void keepAlive(Transaction transaction) throws IOException;

}
