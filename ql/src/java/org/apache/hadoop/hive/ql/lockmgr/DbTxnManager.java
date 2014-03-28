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
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
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
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.thrift.TException;

import java.util.List;

/**
 * An implementation of HiveTxnManager that stores the transactions in the
 * metastore database.
 */
public class DbTxnManager extends HiveTxnManagerImpl {

  static final private String CLASS_NAME = DbTxnManager.class.getName();
  static final private Log LOG = LogFactory.getLog(CLASS_NAME);

  private DbLockManager lockMgr = null;
  private HiveMetaStoreClient client = null;
  private long txnId = 0;

  DbTxnManager() {
  }

  @Override
  public void openTxn(String user) throws LockException {
    init();
    try {
      txnId = client.openTxn(user);
      LOG.debug("Opened txn " + txnId);
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(),
          e);
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
    init();
        // Make sure we've built the lock manager
    getLockManager();

    boolean atLeastOneLock = false;

    LockRequestBuilder rqstBuilder = new LockRequestBuilder();
    LOG.debug("Setting lock request transaction to " + txnId);
    rqstBuilder.setTransactionId(txnId)
        .setUser(username);

    // For each source to read, get a shared lock
    for (ReadEntity input : plan.getInputs()) {
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
          t = output.getTable();
          compBuilder.setDbName(t.getDbName());
          compBuilder.setTableName(t.getTableName());
          break;

        case PARTITION:
        case DUMMYPARTITION:
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
    if (!atLeastOneLock) return;

    List<HiveLock> locks = lockMgr.lock(rqstBuilder.build());
    ctx.setHiveLocks(locks);
  }

  @Override
  public void commitTxn() throws LockException {
    if (txnId == 0) {
      throw new RuntimeException("Attempt to commit before opening a " +
          "transaction");
    }
    try {
      LOG.debug("Committing txn " + txnId);
      client.commitTxn(txnId);
    } catch (NoSuchTxnException e) {
      LOG.error("Metastore could not find txn " + txnId);
      throw new LockException(ErrorMsg.TXN_NO_SUCH_TRANSACTION.getMsg() , e);
    } catch (TxnAbortedException e) {
      LOG.error("Transaction " + txnId + " aborted");
      throw new LockException(ErrorMsg.TXN_ABORTED.getMsg(), e);
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(),
          e);
    } finally {
      txnId = 0;
    }
  }

  @Override
  public void rollbackTxn() throws LockException {
    if (txnId == 0) {
      throw new RuntimeException("Attempt to rollback before opening a " +
          "transaction");
    }
    try {
      LOG.debug("Rolling back txn " + txnId);
      client.rollbackTxn(txnId);
    } catch (NoSuchTxnException e) {
      LOG.error("Metastore could not find txn " + txnId);
      throw new LockException(ErrorMsg.TXN_NO_SUCH_TRANSACTION.getMsg() , e);
    } catch (TException e) {
      throw new LockException(ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(),
          e);
    } finally {
      txnId = 0;
    }
  }

  @Override
  public void heartbeat() throws LockException {
    LOG.debug("Heartbeating lock and transaction " + txnId);
    List<HiveLock> locks = lockMgr.getLocks(false, false);
    if (locks.size() == 0) {
      if (txnId == 0) {
        // No locks, no txn, we outta here.
        return;
      } else {
        // Create one dummy lock so we can go through the loop below
        DbLockManager.DbHiveLock dummyLock = new DbLockManager.DbHiveLock(0L);
        locks.add(dummyLock);
      }
    }
    for (HiveLock lock : locks) {
      long lockId = ((DbLockManager.DbHiveLock)lock).lockId;
      try {
        client.heartbeat(txnId, lockId);
      } catch (NoSuchLockException e) {
        LOG.error("Unable to find lock " + lockId);
        throw new LockException(ErrorMsg.LOCK_NO_SUCH_LOCK.getMsg(), e);
      } catch (NoSuchTxnException e) {
        LOG.error("Unable to find transaction " + txnId);
        throw new LockException(ErrorMsg.TXN_NO_SUCH_TRANSACTION.getMsg(), e);
      } catch (TxnAbortedException e) {
        LOG.error("Transaction aborted " + txnId);
        throw new LockException(ErrorMsg.TXN_ABORTED.getMsg(), e);
      } catch (TException e) {
        throw new LockException(
            ErrorMsg.METASTORE_COMMUNICATION_FAILED.getMsg(), e);
      }
    }
  }

  @Override
  public ValidTxnList getValidTxns() throws LockException {
    init();
    try {
      return client.getValidTxns();
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
  protected void destruct() {
    try {
      if (txnId > 0) rollbackTxn();
      if (lockMgr != null) lockMgr.close();
    } catch (Exception e) {
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
        client = new HiveMetaStoreClient(conf);
      } catch (MetaException e) {
        throw new LockException(ErrorMsg.METASTORE_COULD_NOT_INITIATE.getMsg(),
            e);
      }
    }
  }

}
