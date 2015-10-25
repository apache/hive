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
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.LockComponentBuilder;
import org.apache.hadoop.hive.metastore.LockRequestBuilder;
import org.apache.hadoop.hive.metastore.api.*;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.Entity;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.thrift.TException;

import java.util.ArrayList;
import java.util.List;

/**
 * An implementation of HiveTxnManager that stores the transactions in the
 * metastore database.
 */
public class DbTxnManager extends HiveTxnManagerImpl {

  static final private String CLASS_NAME = DbTxnManager.class.getName();
  static final private Log LOG = LogFactory.getLog(CLASS_NAME);

  private DbLockManager lockMgr = null;
  private IMetaStoreClient client = null;
  /**
   * The Metastore NEXT_TXN_ID.NTXN_NEXT is initialized to 1; it contains the next available
   * transaction id.  Thus is 1 is first transaction id.
   */
  private long txnId = 0;
  /**
   * assigns a unique monotonically increasing ID to each statement
   * which is part of an open transaction.  This is used by storage
   * layer (see {@link org.apache.hadoop.hive.ql.io.AcidUtils#deltaSubdir(long, long, int)})
   * to keep apart multiple writes of the same data within the same transaction
   * Also see {@link org.apache.hadoop.hive.ql.io.AcidOutputFormat.Options}
   */
  private int statementId = -1;

  DbTxnManager() {
  }

  @Override
  void setHiveConf(HiveConf conf) {
    super.setHiveConf(conf);
    if (!conf.getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY)) {
      throw new RuntimeException(ErrorMsg.DBTXNMGR_REQUIRES_CONCURRENCY.getMsg());
    }
  }

  @Override
  public long openTxn(String user) throws LockException {
    init();
    if(isTxnOpen()) {
      throw new LockException("Transaction already opened. " + JavaUtils.txnIdToString(txnId));
    }
    try {
      txnId = client.openTxn(user);
      statementId = 0;
      LOG.debug("Opened " + JavaUtils.txnIdToString(txnId));
      return txnId;
    } catch (TException e) {
      throw new LockException(e, ErrorMsg.METASTORE_COMMUNICATION_FAILED);
    }
  }

  @Override
  public HiveLockManager getLockManager() throws LockException {
    init();
    if (lockMgr == null) {
      lockMgr = new DbLockManager(client);
    }
    return lockMgr;
  }

  @Override
  public void acquireLocks(QueryPlan plan, Context ctx, String username) throws LockException {
    acquireLocks(plan, ctx, username, true);
  }

  /**
   * This is for testing only.  Normally client should call {@link #acquireLocks(org.apache.hadoop.hive.ql.QueryPlan, org.apache.hadoop.hive.ql.Context, String)}
   * @param isBlocking if false, the method will return immediately; thus the locks may be in LockState.WAITING
   * @return null if no locks were needed
   */
  LockState acquireLocks(QueryPlan plan, Context ctx, String username, boolean isBlocking) throws LockException {
    init();
        // Make sure we've built the lock manager
    getLockManager();

    boolean atLeastOneLock = false;

    LockRequestBuilder rqstBuilder = new LockRequestBuilder();
    //link queryId to txnId
    LOG.info("Setting lock request transaction to " + JavaUtils.txnIdToString(txnId) + " for queryId=" + plan.getQueryId());
    rqstBuilder.setTransactionId(txnId)
        .setUser(username);

    // For each source to read, get a shared lock
    for (ReadEntity input : plan.getInputs()) {
      if (!input.needsLock() || input.isUpdateOrDelete()) {
        // We don't want to acquire readlocks during update or delete as we'll be acquiring write
        // locks instead.
        continue;
      }
      LockComponentBuilder compBuilder = new LockComponentBuilder();
      compBuilder.setShared();

      Table t = null;
      switch (input.getType()) {
        case DATABASE:
          compBuilder.setDbName(input.getDatabase().getName());
          break;

        case TABLE:
          t = input.getTable();
          compBuilder.setDbName(t.getDbName());
          compBuilder.setTableName(t.getTableName());
          break;

        case PARTITION:
        case DUMMYPARTITION:
          compBuilder.setPartitionName(input.getPartition().getName());
          t = input.getPartition().getTable();
          compBuilder.setDbName(t.getDbName());
          compBuilder.setTableName(t.getTableName());
          break;

        default:
          // This is a file or something we don't hold locks for.
          continue;
      }
      LockComponent comp = compBuilder.build();
      LOG.debug("Adding lock component to lock request " + comp.toString());
      rqstBuilder.addLockComponent(comp);
      atLeastOneLock = true;
    }

    // For each source to write to, get the appropriate lock type.  If it's
    // an OVERWRITE, we need to get an exclusive lock.  If it's an insert (no
    // overwrite) than we need a shared.  If it's update or delete then we
    // need a SEMI-SHARED.
    for (WriteEntity output : plan.getOutputs()) {
      if (output.getType() == Entity.Type.DFS_DIR || output.getType() ==
          Entity.Type.LOCAL_DIR) {
        // We don't lock files or directories.
        continue;
      }
      LockComponentBuilder compBuilder = new LockComponentBuilder();
      Table t = null;
      LOG.debug("output is null " + (output == null));
      switch (output.getWriteType()) {
        case DDL_EXCLUSIVE:
        case INSERT_OVERWRITE:
          compBuilder.setExclusive();
          break;

        case INSERT:
        case DDL_SHARED:
          compBuilder.setShared();
          break;

        case UPDATE:
        case DELETE:
          compBuilder.setSemiShared();
          break;

        case DDL_NO_LOCK:
          continue; // No lock required here

        default:
          throw new RuntimeException("Unknown write type " +
              output.getWriteType().toString());

      }
      switch (output.getType()) {
        case DATABASE:
          compBuilder.setDbName(output.getDatabase().getName());
          break;

        case TABLE:
        case DUMMYPARTITION:   // in case of dynamic partitioning lock the table
          t = output.getTable();
          compBuilder.setDbName(t.getDbName());
          compBuilder.setTableName(t.getTableName());
          break;

        case PARTITION:
          compBuilder.setPartitionName(output.getPartition().getName());
          t = output.getPartition().getTable();
          compBuilder.setDbName(t.getDbName());
          compBuilder.setTableName(t.getTableName());
          break;

        default:
          // This is a file or something we don't hold locks for.
          continue;
      }
      LockComponent comp = compBuilder.build();
      LOG.debug("Adding lock component to lock request " + comp.toString());
      rqstBuilder.addLockComponent(comp);
      atLeastOneLock = true;
    }

    // Make sure we need locks.  It's possible there's nothing to lock in
    // this operation.
    if (!atLeastOneLock) {
      LOG.debug("No locks needed for queryId" + plan.getQueryId());
      return null;
    }

    List<HiveLock> locks = new ArrayList<HiveLock>(1);
    if(isTxnOpen()) {
      statementId++;
    }
    LockState lockState = lockMgr.lock(rqstBuilder.build(), plan.getQueryId(), isBlocking, locks);
    ctx.setHiveLocks(locks);
    return lockState;
  }

  @Override
  public void commitTxn() throws LockException {
    if (!isTxnOpen()) {
      throw new RuntimeException("Attempt to commit before opening a transaction");
    }
    try {
      lockMgr.clearLocalLockRecords();
      LOG.debug("Committing txn " + JavaUtils.txnIdToString(txnId));
      client.commitTxn(txnId);
    } catch (NoSuchTxnException e) {
      LOG.error("Metastore could not find " + JavaUtils.txnIdToString(txnId));
      throw new LockException(e, ErrorMsg.TXN_NO_SUCH_TRANSACTION, JavaUtils.txnIdToString(txnId));
    } catch (TxnAbortedException e) {
      LOG.error("Transaction " + JavaUtils.txnIdToString(txnId) + " aborted");
      throw new LockException(e, ErrorMsg.TXN_ABORTED, JavaUtils.txnIdToString(txnId));
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(),
          e);
    } finally {
      txnId = 0;
      statementId = -1;
    }
  }

  @Override
  public void rollbackTxn() throws LockException {
    if (!isTxnOpen()) {
      throw new RuntimeException("Attempt to rollback before opening a transaction");
    }
    try {
      lockMgr.clearLocalLockRecords();
      LOG.debug("Rolling back " + JavaUtils.txnIdToString(txnId));
      client.rollbackTxn(txnId);
    } catch (NoSuchTxnException e) {
      LOG.error("Metastore could not find " + JavaUtils.txnIdToString(txnId));
      throw new LockException(e, ErrorMsg.TXN_NO_SUCH_TRANSACTION, JavaUtils.txnIdToString(txnId));
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(),
          e);
    } finally {
      txnId = 0;
      statementId = -1;
    }
  }

  @Override
  public void heartbeat() throws LockException {
    List<HiveLock> locks;
    if(isTxnOpen()) {
      // Create one dummy lock so we can go through the loop below, though we only
      //really need txnId
      DbLockManager.DbHiveLock dummyLock = new DbLockManager.DbHiveLock(0L);
      locks = new ArrayList<>(1);
      locks.add(dummyLock);
    }
    else {
      locks = lockMgr.getLocks(false, false);
    }
    if(LOG.isInfoEnabled()) {
      StringBuilder sb = new StringBuilder("Sending heartbeat for ")
        .append(JavaUtils.txnIdToString(txnId)).append(" and");
      for(HiveLock lock : locks) {
        sb.append(" ").append(lock.toString());
      }
      LOG.info(sb.toString());
    }
    if(!isTxnOpen() && locks.isEmpty()) {
      // No locks, no txn, we outta here.
      return;
    }
    for (HiveLock lock : locks) {
      long lockId = ((DbLockManager.DbHiveLock)lock).lockId;
      try {
        client.heartbeat(txnId, lockId);
      } catch (NoSuchLockException e) {
        LOG.error("Unable to find lock " + JavaUtils.lockIdToString(lockId));
        throw new LockException(e, ErrorMsg.LOCK_NO_SUCH_LOCK, JavaUtils.lockIdToString(lockId));
      } catch (NoSuchTxnException e) {
        LOG.error("Unable to find transaction " + JavaUtils.txnIdToString(txnId));
        throw new LockException(e, ErrorMsg.TXN_NO_SUCH_TRANSACTION, JavaUtils.txnIdToString(txnId));
      } catch (TxnAbortedException e) {
        LOG.error("Transaction aborted " + JavaUtils.txnIdToString(txnId));
        throw new LockException(e, ErrorMsg.TXN_ABORTED, JavaUtils.txnIdToString(txnId));
      } catch (TException e) {
        throw new LockException(
            ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg() + "(" + JavaUtils.txnIdToString(txnId)
              + "," + lock.toString() + ")", e);
      }
    }
  }

  @Override
  public ValidTxnList getValidTxns() throws LockException {
    init();
    try {
      return client.getValidTxns(txnId);
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(),
          e);
    }
  }

  @Override
  public boolean supportsExplicitLock() {
    return false;
  }

  @Override
  public boolean useNewShowLocksFormat() {
    return true;
  }

  @Override
  public boolean supportsAcid() {
    return true;
  }

  @Override
  protected void destruct() {
    try {
      if (isTxnOpen()) rollbackTxn();
      if (lockMgr != null) lockMgr.close();
    } catch (Exception e) {
      LOG.error("Caught exception " + e.getClass().getName() + " with message <" + e.getMessage()
      + ">, swallowing as there is nothing we can do with it.");
      // Not much we can do about it here.
    }
  }

  private void init() throws LockException {
    if (client == null) {
      if (conf == null) {
        throw new RuntimeException("Must call setHiveConf before any other " +
            "methods.");
      }
      try {
        Hive db = Hive.get(conf);
        client = db.getMSC();
      } catch (MetaException e) {
        throw new LockException(ErrorMsg.METASTORE_COULD_NOT_INITIATE.getMsg(), e);
      } catch (HiveException e) {
        throw new LockException(ErrorMsg.METASTORE_COULD_NOT_INITIATE.getMsg(), e);
      }
    }
  }
  @Override
  public boolean isTxnOpen() {
    return txnId > 0;
  }
  @Override
  public long getCurrentTxnId() {
    return txnId;
  }
  @Override
  public int getStatementId() {
    return statementId;
  }
}
