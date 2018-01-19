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
package org.apache.hadoop.hive.ql.lockmgr;

import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver.LockedDriverState;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.LockDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.LockTableDesc;
import org.apache.hadoop.hive.ql.plan.UnlockDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.UnlockTableDesc;

import java.util.List;

/**
 * An interface that allows Hive to manage transactions.  All classes
 * implementing this should extend {@link HiveTxnManagerImpl} rather than
 * implementing this directly.
 */
public interface HiveTxnManager {

  /**
   * Open a new transaction.
   * @param ctx Context for this query
   * @param user Hive user who is opening this transaction.
   * @return The new transaction id
   * @throws LockException if a transaction is already open.
   */
  long openTxn(Context ctx, String user) throws LockException;

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
   * requires transactions, this should be called after {@link #openTxn(Context, String)}.
   * A list of acquired locks will be stored in the
   * {@link org.apache.hadoop.hive.ql.Context} object and can be retrieved
   * via {@link org.apache.hadoop.hive.ql.Context#getHiveLocks}.
   *
   * @param plan query plan
   * @param ctx Context for this query
   * @param username name of the user for this query
   * @throws LockException if there is an error getting the locks.  Use {@link LockException#getCanonicalErrorMsg()}
   * to get more info on how to handle the exception.
   */
  void acquireLocks(QueryPlan plan, Context ctx, String username) throws LockException;

  /**
   * Acquire all of the locks needed by a query.  If used with a query that
   * requires transactions, this should be called after {@link #openTxn(Context, String)}.
   * A list of acquired locks will be stored in the
   * {@link org.apache.hadoop.hive.ql.Context} object and can be retrieved
   * via {@link org.apache.hadoop.hive.ql.Context#getHiveLocks}.
   * @param plan query plan
   * @param ctx Context for this query
   * @param username name of the user for this query
   * @param lDrvState the state to inform if the query cancelled or not
   * @throws LockException if there is an error getting the locks
   */
   void acquireLocks(QueryPlan plan, Context ctx, String username, LockedDriverState lDrvState) throws LockException;

  /**
   * Release specified locks.
   * Transaction aware TxnManagers, which has {@code supportsAcid() == true},
   * will track locks internally and ignore this parameter
   * @param hiveLocks The list of locks to be released.
   */
  void releaseLocks(List<HiveLock> hiveLocks) throws LockException;

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
   * Get the name for currently installed transaction manager.
   * @return transaction manager name
   */
  String getTxnManagerName();

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
   * This function is called to lock the table when explicit lock command is
   * issued on a table.
   * @param hiveDB    an object to communicate with the metastore
   * @param lockTbl   table locking info, such as table name, locking mode
   * @return 0 if the locking succeeds, 1 otherwise.
   * @throws HiveException
   */
  int lockTable(Hive hiveDB, LockTableDesc lockTbl) throws HiveException;

  /**
   * This function is called to unlock the table when explicit unlock command is
   * issued on a table.
   * @param hiveDB    an object to communicate with the metastore
   * @param unlockTbl table unlocking info, such as table name
   * @return 0 if the locking succeeds, 1 otherwise.
   * @throws HiveException
   */
  int unlockTable(Hive hiveDB, UnlockTableDesc unlockTbl) throws HiveException;

  /**
   * This function is called to lock the database when explicit lock command is
   * issued on a database.
   * @param hiveDB    an object to communicate with the metastore
   * @param lockDb    database locking info, such as database name, locking mode
   * @return 0 if the locking succeeds, 1 otherwise.
   * @throws HiveException
   */
  int lockDatabase(Hive hiveDB, LockDatabaseDesc lockDb) throws HiveException;

  /**
   * This function is called to unlock the database when explicit unlock command
   * is issued on a database.
   * @param hiveDB    an object to communicate with the metastore
   * @param unlockDb  database unlocking info, such as database name
   * @return 0 if the locking succeeds, 1 otherwise.
   * @throws HiveException
   */
  int unlockDatabase(Hive hiveDB, UnlockDatabaseDesc unlockDb) throws HiveException;

  /**
   * Indicate whether this transaction manager returns information about locks in the new format
   * for show locks or the old one.
   * @return true if the new format should be used.
   */
  boolean useNewShowLocksFormat();

  /**
   * Indicate whether this transaction manager supports ACID operations
   * @return true if this transaction manager does ACID
   */
  boolean supportsAcid();

  /**
   * For resources that support MVCC, the state of the DB must be recorded for the duration of the
   * operation/transaction.  Returns {@code true} if current statment needs to do this.
   */
  boolean recordSnapshot(QueryPlan queryPlan);

  boolean isImplicitTransactionOpen();
  
  boolean isTxnOpen();
  /**
   * if {@code isTxnOpen()}, returns the currently active transaction ID
   */
  long getCurrentTxnId();

  /**
   * Should be though of more as a unique write operation ID in a given txn (at QueryPlan level).
   * Each statement writing data within a multi statement txn should have a unique WriteId.
   * Even a single statement, (e.g. Merge, multi-insert may generates several writes).
   */
  int getWriteIdAndIncrement();
}
