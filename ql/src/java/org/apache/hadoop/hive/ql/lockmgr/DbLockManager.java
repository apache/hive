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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.common.metrics.common.Metrics;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.thrift.TException;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * An implementation of HiveLockManager for use with {@link org.apache.hadoop.hive.ql.lockmgr.DbTxnManager}.
 * Note, this lock manager is not meant to stand alone.  It cannot be used
 * without the DbTxnManager.
 */
public class DbLockManager implements HiveLockManager{

  static final private String CLASS_NAME = DbLockManager.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  private long MAX_SLEEP;
  private Set<DbHiveLock> locks;
  private IMetaStoreClient client;
  private long nextSleep = 50;
  private final HiveConf conf;

  DbLockManager(IMetaStoreClient client, HiveConf conf) {
    locks = new HashSet<>();
    this.client = client;
    this.conf = conf;
  }

  @Override
  public void setContext(HiveLockManagerCtx ctx) throws LockException {
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
    Objects.requireNonNull(queryId, "queryId cannot be null");
    nextSleep = 50;
    /*
     * get from conf to pick up changes; make sure not to set too low and kill the metastore
     * MAX_SLEEP is the max time each backoff() will wait for, thus the total time to wait for
     * successful lock acquisition is approximately (see backoff()) maxNumWaits * MAX_SLEEP.
     */
    MAX_SLEEP = Math.max(15000, conf.getTimeVar(HiveConf.ConfVars.HIVE_LOCK_SLEEP_BETWEEN_RETRIES, TimeUnit.MILLISECONDS));
    int maxNumWaits = Math.max(0, conf.getIntVar(HiveConf.ConfVars.HIVE_LOCK_NUMRETRIES));
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
      int numRetries = 0;
      long startRetry = System.currentTimeMillis();
      while (res.getState() == LockState.WAITING && numRetries++ < maxNumWaits) {
        backoff();
        res = client.checkLock(res.getLockid());

      }
      long retryDuration = System.currentTimeMillis() - startRetry;
      DbHiveLock hl = new DbHiveLock(res.getLockid());
      locks.add(hl);
      if (res.getState() != LockState.ACQUIRED) {
        if(res.getState() == LockState.WAITING) {
          /**
           * the {@link #unlock(HiveLock)} here is more about future proofing when support for
           * multi-statement txns is added.  In that case it's reasonable for the client
           * to retry this part of txn or try something else w/o aborting the whole txn.
           * Also for READ_COMMITTED (when and if that is supported).
           */
          unlock(hl);//remove the locks in Waiting state
          LockException le = new LockException(null, ErrorMsg.LOCK_ACQUIRE_TIMEDOUT,
            lock.toString(), Long.toString(retryDuration), res.toString());
          if(conf.getBoolVar(HiveConf.ConfVars.TXN_MGR_DUMP_LOCK_STATE_ON_ACQUIRE_TIMEOUT)) {
            showLocksNewFormat(le.getMessage());
          }
          throw le;
        }
        throw new LockException(ErrorMsg.LOCK_CANNOT_BE_ACQUIRED.getMsg() + " " + res);
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
      LOG.error("Metastore could not find " + JavaUtils.txnIdToString(lock.getTxnid()));
      throw new LockException(e, ErrorMsg.TXN_NO_SUCH_TRANSACTION, JavaUtils.txnIdToString(lock.getTxnid()));
    } catch (TxnAbortedException e) {
      LOG.error("Transaction " + JavaUtils.txnIdToString(lock.getTxnid()) + " already aborted.");
      throw new LockException(e, ErrorMsg.TXN_ABORTED, JavaUtils.txnIdToString(lock.getTxnid()));
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(),
          e);
    }
  }
  private void showLocksNewFormat(String preamble) throws LockException {
    ShowLocksResponse rsp = getLocks();

    // write the results in the file
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024*2);
    DataOutputStream os = new DataOutputStream(baos);
    try {
      DDLTask.dumpLockInfo(os, rsp);
      os.flush();
      LOG.info(baos.toString());
    }
    catch(IOException ex) {
      LOG.error("Dumping lock info for " + preamble + " failed: " + ex.getMessage(), ex);
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
   * {@link #DbLockManager(org.apache.hadoop.hive.metastore.IMetaStoreClient, org.apache.hadoop.hive.conf.HiveConf)} .commitTxn} and
   * {@link #DbLockManager(org.apache.hadoop.hive.metastore.IMetaStoreClient, org.apache.hadoop.hive.conf.HiveConf)} .rollbackTxn}.
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
