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
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
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
  private HiveMetaStoreClient client;
  private long nextSleep = 50;

  DbLockManager(HiveMetaStoreClient client) {
    locks = new HashSet<DbHiveLock>();
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
   * @throws LockException
   */
  List<HiveLock> lock(LockRequest lock) throws LockException {
    try {
      LOG.debug("Requesting lock");
      LockResponse res = client.lock(lock);
      while (res.getState() == LockState.WAITING) {
        backoff();
        res = client.checkLock(res.getLockid());

      }
      DbHiveLock hl = new DbHiveLock(res.getLockid());
      locks.add(hl);
      if (res.getState() != LockState.ACQUIRED) {
        throw new LockException(ErrorMsg.LOCK_CANNOT_BE_ACQUIRED.getMsg());
      }
      List<HiveLock> locks = new ArrayList<HiveLock>(1);
      locks.add(hl);
      return locks;
    } catch (NoSuchTxnException e) {
      LOG.error("Metastore could not find txnid " + lock.getTxnid());
      throw new LockException(ErrorMsg.TXNMGR_NOT_INSTANTIATED.getMsg(), e);
    } catch (TxnAbortedException e) {
      LOG.error("Transaction " + lock.getTxnid() + " already aborted.");
      throw new LockException(ErrorMsg.TXN_ABORTED.getMsg(), e);
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(),
          e);
    }
  }

  @Override
  public void unlock(HiveLock hiveLock) throws LockException {
    long lockId = ((DbHiveLock)hiveLock).lockId;
    try {
      LOG.debug("Unlocking id:" + lockId);
      client.unlock(lockId);
      boolean removed = locks.remove((DbHiveLock)hiveLock);
      LOG.debug("Removed a lock " + removed);
    } catch (NoSuchLockException e) {
      LOG.error("Metastore could find no record of lock " + lockId);
      throw new LockException(ErrorMsg.LOCK_NO_SUCH_LOCK.getMsg(), e);
    } catch (TxnOpenException e) {
      throw new RuntimeException("Attempt to unlock lock " + lockId +
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
