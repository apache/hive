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
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.Driver.LockedDriverState;
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
 * Note, this lock manager is not meant to be stand alone.  It cannot be used without the DbTxnManager.
 * See {@link DbTxnManager#getMS()} for important concurrency/metastore access notes.
 */
public final class DbLockManager implements HiveLockManager{

  static final private String CLASS_NAME = DbLockManager.class.getName();
  static final private Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  private long MAX_SLEEP;
  //longer term we should always have a txn id and then we won't need to track locks here at all
  private Set<DbHiveLock> locks;
  private long nextSleep = 50;
  private final HiveConf conf;
  private final DbTxnManager txnManager;

  DbLockManager(HiveConf conf, DbTxnManager txnManager) {
    locks = new HashSet<>();
    this.conf = conf;
    this.txnManager = txnManager;
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
  public List<HiveLock> lock(List<HiveLockObj> objs, boolean keepAlive, LockedDriverState lDrvState) throws
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
      LockResponse res = txnManager.getMS().lock(lock);
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
        res = txnManager.getMS().checkLock(res.getLockid());
      }
      long retryDuration = System.currentTimeMillis() - startRetry;
      DbHiveLock hl = new DbHiveLock(res.getLockid(), queryId, lock.getTxnid());
      if(locks.size() > 0) {
        boolean logMsg = false;
        for(DbHiveLock l : locks) {
          if(l.txnId != hl.txnId) {
            //locks from different transactions detected (or from transaction and read-only query in autocommit)
            logMsg = true;
            break;
          }
          else if(l.txnId == 0) {
            if(!l.queryId.equals(hl.queryId)) {
              //here means no open transaction, but different queries
              logMsg = true;
              break;
            }
          }
        }
        if(logMsg) {
          LOG.warn("adding new DbHiveLock(" + hl + ") while we are already tracking locks: " + locks);
        }
      }
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
      LockException le = new LockException(e, ErrorMsg.TXN_ABORTED, JavaUtils.txnIdToString(lock.getTxnid()), e.getMessage());
      LOG.error(le.getMessage());
      throw le;
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
      return txnManager.getMS().checkLock(extLockId).getState();
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(),
        e);
    }
  }

  @Override
  public void unlock(HiveLock hiveLock) throws LockException {
    long lockId = ((DbHiveLock)hiveLock).lockId;
    boolean removed = false;
    try {
      LOG.debug("Unlocking " + hiveLock);
      txnManager.getMS().unlock(lockId);
      //important to remove after unlock() in case it fails
      removed = locks.remove(hiveLock);
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
      //if metastore has no record of this lock, it most likely timed out; either way
      //there is no point tracking it here any longer
      removed = locks.remove(hiveLock);
      LOG.error("Metastore could find no record of lock " + JavaUtils.lockIdToString(lockId));
      throw new LockException(e, ErrorMsg.LOCK_NO_SUCH_LOCK, JavaUtils.lockIdToString(lockId));
    } catch (TxnOpenException e) {
      throw new RuntimeException("Attempt to unlock lock " + JavaUtils.lockIdToString(lockId) +
          "associated with an open transaction, " + e.getMessage(), e);
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(),
          e);
    }
    finally {
      if(removed) {
        LOG.debug("Removed a lock " + hiveLock);
      }
    }
  }

  @Override
  public void releaseLocks(List<HiveLock> hiveLocks) {
    LOG.info("releaseLocks: " + hiveLocks);
    for (HiveLock lock : hiveLocks) {
      try {
        unlock(lock);
      } catch (LockException e) {
        // Not sure why this method doesn't throw any exceptions,
        // but since the interface doesn't allow it we'll just swallow them and
        // move on.
        //This OK-ish since releaseLocks() is only called for RO/AC queries; it
        //would be really bad to eat exceptions here for write operations
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
    return getLocks(new ShowLocksRequest());
  }

  public ShowLocksResponse getLocks(ShowLocksRequest showLocksRequest) throws LockException {
    try {
      return txnManager.getMS().showLocks(showLocksRequest);
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
    String queryId;
    long txnId;

    DbHiveLock(long id) {
      lockId = id;
    }
    DbHiveLock(long id, String queryId, long txnId) {
      lockId = id;
      this.queryId = queryId;
      this.txnId = txnId;
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
      return JavaUtils.lockIdToString(lockId) + " queryId=" + queryId + " " + JavaUtils.txnIdToString(txnId);
    }
  }

  /**
   * Clear the memory of the locks in this object.  This won't clear the locks from the database.
   * It is for use with
   * {@link #DbLockManager(HiveConf, DbTxnManager)} .commitTxn} and
   * {@link #DbLockManager(HiveConf, DbTxnManager)} .rollbackTxn}.
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
