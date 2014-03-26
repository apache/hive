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
package org.apache.hadoop.hive.ql.lockmgr;

import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryPlan;

/**
 * An interface that allows Hive to manage transactions.  All classes
 * implementing this should extend {@link HiveTxnManagerImpl} rather than
 * implementing this directly.
 */
public interface HiveTxnManager {

  /**
   * Open a new transaction.
   * @param user Hive user who is opening this transaction.
   * @throws LockException if a transaction is already open.
   */
  void openTxn(String user) throws LockException;

  /**
   * Get the lock manager.  This must be used rather than instantiating an
   * instance of the lock manager directly as the transaction manager will
   * choose which lock manager to instantiate.
   * @return the instance of the lock manager
   * @throws LockException if there is an issue obtaining the lock manager.
   */
  HiveLockManager getLockManager() throws LockException;

  /**
   * Acquire all of the locks needed by a query.  If used with a query that
   * requires transactions, this should be called after {@link #openTxn(String)}.
   * A list of acquired locks will be stored in the
   * {@link org.apache.hadoop.hive.ql.Context} object and can be retrieved
   * via {@link org.apache.hadoop.hive.ql.Context#getHiveLocks}.
   * @param plan query plan
   * @param ctx Context for this query
   * @param username name of the user for this query
   * @throws LockException if there is an error getting the locks
   */
  void acquireLocks(QueryPlan plan, Context ctx, String username) throws LockException;

  /**
   * Commit the current transaction.  This will release all locks obtained in
   * {@link #acquireLocks(org.apache.hadoop.hive.ql.QueryPlan,
   * org.apache.hadoop.hive.ql.Context, java.lang.String)}.
   * @throws LockException if there is no current transaction or the
   * transaction has already been committed or aborted.
   */
  void commitTxn() throws LockException;

  /**
   * Abort the current transaction.  This will release all locks obtained in
   * {@link #acquireLocks(org.apache.hadoop.hive.ql.QueryPlan,
   * org.apache.hadoop.hive.ql.Context, java.lang.String)}.
   * @throws LockException if there is no current transaction or the
   * transaction has already been committed or aborted.
   */
  void rollbackTxn() throws LockException;

  /**
   * Send a heartbeat to the transaction management storage so other Hive
   * clients know that the transaction and locks held by this client are
   * still valid.  For implementations that do not require heartbeats this
   * can be a no-op.
   * @throws LockException If current transaction exists or the transaction
   * has already been committed or aborted.
   */
  void heartbeat() throws LockException;

  /**
   * Get the transactions that are currently valid.  The resulting
   * {@link ValidTxnList} object is a thrift object and can
   * be  passed to  the processing
   * tasks for use in the reading the data.  This call should be made once up
   * front by the planner and should never be called on the backend,
   * as this will violate the isolation level semantics.
   * @return list of valid transactions.
   * @throws LockException
   */
  ValidTxnList getValidTxns() throws LockException;

  /**
   * This call closes down the transaction manager.  All open transactions
   * are aborted.  If no transactions are open but locks are held those locks
   * are released.  This method should be called if processing of a session
   * is being halted in an abnormal way.  It avoids locks and transactions
   * timing out.
   */
  void closeTxnManager();

  /**
   * Indicate whether this lock manager supports the use of <code>lock
   * <i>database</i></code> or <code>lock <i>table</i></code>.
   * @return
   */
  boolean supportsExplicitLock();

  /**
   * Indicate whether this transaction manager returns information about locks in the new format
   * for show locks or the old one.
   * @return true if the new format should be used.
   */
  boolean useNewShowLocksFormat();
}
