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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * An implementation of HiveLockManager for use with {@link org.apache.hadoop.hive.ql.lockmgr.DbTxnManager}.
 * Note, this lock manager is not meant to stand alone.  It cannot be used
 * without the DbTxnManager.
 */
public class DbLockManager implements HiveLockManager{

  static final private String CLASS_NAME = DbLockManager.class.getName();
  static final private Log LOG = LogFactory.getLog(CLASS_NAME);

  private static final long MAX_SLEEP = 15000;
  private HiveLockManagerCtx context;
  private Set<DbHiveLock> locks;
  private IMetaStoreClient client;
  private long nextSleep = 50;

  DbLockManager(IMetaStoreClient client) {
    locks = new HashSet<>();
    this.client = client;
  }

  @Override
  public void setContext(HiveLockManagerCtx ctx) throws LockException {
    context = ctx;
  }

  @Override
  public HiveLock lock(HiveLockObject key, HiveLockMode mode,
                       boolean keepAlive) throws LockException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<HiveLock> lock(List<HiveLockObj> objs, boolean keepAlive) throws
      LockException {
    throw new UnsupportedOperationException();
  }

  /**
   * Send a lock request to the metastore.  This is intended for use by
   * {@link DbTxnManager}.
   * @param lock lock request
   * @param isBlocking if true, will block until locks have been acquired
   * @throws LockException
   * @return the result of the lock attempt
   */
  LockState lock(LockRequest lock, String queryId, boolean isBlocking, List<HiveLock> acquiredLocks) throws LockException {
    try {
      LOG.info("Requesting: queryId=" + queryId + " " + lock);
      LockResponse res = client.lock(lock);
      //link lockId to queryId
      LOG.info("Response to queryId=" + queryId + " " + res);
      if(!isBlocking) {
        if(res.getState() == LockState.WAITING) {
          return LockState.WAITING;
        }
      }
      while (res.getState() == LockState.WAITING) {
        backoff();
        res = client.checkLock(res.getLockid());

      }
      DbHiveLock hl = new DbHiveLock(res.getLockid());
      locks.add(hl);
      if (res.getState() != LockState.ACQUIRED) {
        throw new LockException(ErrorMsg.LOCK_CANNOT_BE_ACQUIRED.getMsg());
      }
      acquiredLocks.add(hl);

      Metrics metrics = MetricsFactory.getInstance();
      if (metrics != null) {
        try {
          metrics.incrementCounter(MetricsConstant.METASTORE_HIVE_LOCKS);
        } catch (Exception e) {
          LOG.warn("Error Reporting hive client metastore lock operation to Metrics system", e);
        }
      }

      return res.getState();
    } catch (NoSuchTxnException e) {
      LOG.error("Metastore could not find txnid " + lock.getTxnid());
      throw new LockException(ErrorMsg.TXNMGR_NOT_INSTANTIATED.getMsg(), e);
    } catch (TxnAbortedException e) {
      LOG.error("Transaction " + JavaUtils.txnIdToString(lock.getTxnid()) + " already aborted.");
      throw new LockException(e, ErrorMsg.TXN_ABORTED, JavaUtils.txnIdToString(lock.getTxnid()));
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(),
          e);
    }
  }
  /**
   * Used to make another attempt to acquire a lock (in Waiting state)
   * @param extLockId
   * @return result of the attempt
   * @throws LockException
   */
  LockState checkLock(long extLockId) throws LockException {
    try {
      return client.checkLock(extLockId).getState();
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(),
        e);
    }
  }

  @Override
  public void unlock(HiveLock hiveLock) throws LockException {
    long lockId = ((DbHiveLock)hiveLock).lockId;
    try {
      LOG.debug("Unlocking " + hiveLock);
      client.unlock(lockId);
      boolean removed = locks.remove(hiveLock);
      Metrics metrics = MetricsFactory.getInstance();
      if (metrics != null) {
        try {
          metrics.decrementCounter(MetricsConstant.METASTORE_HIVE_LOCKS);
        } catch (Exception e) {
          LOG.warn("Error Reporting hive client metastore unlock operation to Metrics system", e);
        }
      }
      LOG.debug("Removed a lock " + removed);
    } catch (NoSuchLockException e) {
      LOG.error("Metastore could find no record of lock " + JavaUtils.lockIdToString(lockId));
      throw new LockException(e, ErrorMsg.LOCK_NO_SUCH_LOCK, JavaUtils.lockIdToString(lockId));
    } catch (TxnOpenException e) {
      throw new RuntimeException("Attempt to unlock lock " + JavaUtils.lockIdToString(lockId) +
          "associated with an open transaction, " + e.getMessage(), e);
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(),
          e);
    }
  }

  @Override
  public void releaseLocks(List<HiveLock> hiveLocks) {
    for (HiveLock lock : hiveLocks) {
      try {
        unlock(lock);
      } catch (LockException e) {
        // Not sure why this method doesn't throw any exceptions,
        // but since the interface doesn't allow it we'll just swallow them and
        // move on.
      }
    }
  }

  @Override
  public List<HiveLock> getLocks(boolean verifyTablePartitions,
                                 boolean fetchData) throws LockException {
    return new ArrayList<HiveLock>(locks);
  }

  @Override
  public List<HiveLock> getLocks(HiveLockObject key,
                                 boolean verifyTablePartitions,
                                 boolean fetchData) throws LockException {
    throw new UnsupportedOperationException();
  }

  public ShowLocksResponse getLocks() throws LockException {
    try {
      return client.showLocks();
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(), e);
    }
  }

    @Override
  public void close() throws LockException {
    for (HiveLock lock : locks) {
      unlock(lock);
    }
    locks.clear();
  }

  @Override
  public void prepareRetry() throws LockException {
    // NOP
  }

  @Override
  public void refresh() {
    // NOP
  }

  static class DbHiveLock extends HiveLock {

    long lockId;

    DbHiveLock(long id) {
      lockId = id;
    }

    @Override
    public HiveLockObject getHiveLockObject() {
      throw new UnsupportedOperationException();
    }

    @Override
    public HiveLockMode getHiveLockMode() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object other) {
      if (other instanceof DbHiveLock) {
        return lockId == ((DbHiveLock)other).lockId;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return (int)(lockId % Integer.MAX_VALUE);
    }
    @Override
    public String toString() {
      return JavaUtils.lockIdToString(lockId);
    }
  }

  /**
   * Clear the memory of the locks in this object.  This won't clear the locks from the database.
   * It is for use with
   * {@link #DbLockManager(org.apache.hadoop.hive.metastore.IMetaStoreClient).commitTxn} and
   * {@link #DbLockManager(org.apache.hadoop.hive.metastore.IMetaStoreClient).rollbackTxn}.
   */
  void clearLocalLockRecords() {
    locks.clear();
  }

  // Sleep before we send checkLock again, but do it with a back off
  // off so we don't sit and hammer the metastore in a tight loop
  private void backoff() {
    nextSleep *= 2;
    if (nextSleep > MAX_SLEEP) nextSleep = MAX_SLEEP;
    try {
      Thread.sleep(nextSleep);
    } catch (InterruptedException e) {
    }
  }
}
