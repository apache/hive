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
package org.apache.hadoop.hive.metastore.txn;

import org.apache.hadoop.hive.common.classification.RetrySemantics;
import org.apache.hadoop.hive.metastore.api.LockRequest;
import org.apache.hadoop.hive.metastore.api.LockResponse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchLockException;
import org.apache.hadoop.hive.metastore.api.NoSuchTxnException;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.TxnAbortedException;
import org.apache.hadoop.hive.metastore.api.TxnOpenException;
import org.apache.hadoop.hive.metastore.api.UnlockRequest;
import org.apache.hadoop.hive.metastore.txn.retry.SqlRetry;
import org.springframework.transaction.annotation.Transactional;

import static org.apache.hadoop.hive.metastore.txn.TxnStore.POOL_TX;

public interface TxnLockManager {

  @SqlRetry(lockInternally = true)
  @Transactional(POOL_TX)
  long enqueueLock(LockRequest rqst) throws NoSuchTxnException, TxnAbortedException, MetaException;

  /**
   * Check whether a lock has been obtained.  This is used after {@link #enqueueLock(LockRequest)} returned a wait
   * state.
   * @param extLockId 
   * @param txnId Transaction id
   * @param zeroWaitReadEnabled
   * @param isExclusiveCTAS
   * @return info on the state of the lock
   * @throws NoSuchTxnException
   * @throws NoSuchLockException
   * @throws TxnAbortedException
   * @throws MetaException
   */
  @SqlRetry(lockInternally = true)
  @Transactional(value = POOL_TX, noRollbackFor = {TxnAbortedException.class})
  LockResponse checkLock(long extLockId, long txnId, boolean zeroWaitReadEnabled, boolean isExclusiveCTAS)
      throws NoSuchTxnException, NoSuchLockException, TxnAbortedException, MetaException;

  /**
   * Unlock a lock.  It is not legal to call this if the caller is part of a txn.  In that case
   * the txn should be committed or aborted instead.  (Note someday this will change since
   * multi-statement transactions will allow unlocking in the transaction.)
   * @param rqst lock to unlock
   * @throws TxnOpenException
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_TX)
  @RetrySemantics.Idempotent
  void unlock(UnlockRequest rqst)
      throws TxnOpenException, MetaException;

  /**
   * Get information on current locks.
   * @param rqst lock information to retrieve
   * @return lock information.
   * @throws MetaException
   */
  @SqlRetry
  @Transactional(POOL_TX)
  @RetrySemantics.ReadOnly
  ShowLocksResponse showLocks(ShowLocksRequest rqst) throws MetaException;
  
}
