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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.common.ValidTxnList;
import org.apache.hadoop.hive.common.ValidReadTxnList;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver.LockedDriverState;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.*;

/**
 * An implementation of {@link HiveTxnManager} that does not support
 * transactions.  This provides default Hive behavior.
 */
class DummyTxnManager extends HiveTxnManagerImpl {
  static final private Logger LOG =
      LoggerFactory.getLogger(DummyTxnManager.class.getName());

  private HiveLockManager lockMgr;

  private HiveLockManagerCtx lockManagerCtx;

  @Override
  public long openTxn(Context ctx, String user) throws LockException {
    // No-op
    return 0L;
  }
  @Override
  public boolean isTxnOpen() {
    return false;
  }
  @Override
  public long getCurrentTxnId() {
    return 0L;
  }

  @Override
  public int getWriteIdAndIncrement() {
    return 0;
  }
  @Override
  public HiveLockManager getLockManager() throws LockException {
    if (lockMgr == null) {
      boolean supportConcurrency =
          conf.getBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY);
      if (supportConcurrency) {
        String lockMgrName =
            conf.getVar(HiveConf.ConfVars.HIVE_LOCK_MANAGER);
        if ((lockMgrName == null) || (lockMgrName.isEmpty())) {
          throw new LockException(ErrorMsg.LOCKMGR_NOT_SPECIFIED.getMsg());
        }

        try {
          LOG.info("Creating lock manager of type " + lockMgrName);
          lockMgr = (HiveLockManager)ReflectionUtils.newInstance(
              conf.getClassByName(lockMgrName), conf);
          lockManagerCtx = new HiveLockManagerCtx(conf);
          lockMgr.setContext(lockManagerCtx);
        } catch (Exception e) {
          // set hiveLockMgr to null just in case this invalid manager got set to
          // next query's ctx.
          if (lockMgr != null) {
            try {
              lockMgr.close();
            } catch (LockException e1) {
              //nothing can do here
            }
            lockMgr = null;
          }
          throw new LockException(ErrorMsg.LOCKMGR_NOT_INITIALIZED.getMsg() +
              e.getMessage());
        }
      } else {
        LOG.info("Concurrency mode is disabled, not creating a lock manager");
        return null;
      }
    }
    // Force a re-read of the configuration file.  This is done because
    // different queries in the session may be using the same lock manager.
    lockManagerCtx.setConf(conf);
    lockMgr.refresh();
    return lockMgr;
  }

  @Override
  public void acquireLocks(QueryPlan plan, Context ctx, String username) throws LockException {
    acquireLocks(plan,ctx,username,null);
  }

  @Override
  public void acquireLocks(QueryPlan plan, Context ctx, String username, LockedDriverState lDrvState) throws LockException {
    // Make sure we've built the lock manager
    getLockManager();

    // If the lock manager is still null, then it means we aren't using a
    // lock manager
    if (lockMgr == null) {
      return;
    }

    List<HiveLockObj> lockObjects = new ArrayList<HiveLockObj>();

    // Sort all the inputs, outputs.
    // If a lock needs to be acquired on any partition, a read lock needs to be acquired on all
    // its parents also
    for (ReadEntity input : plan.getInputs()) {
      if (!input.needsLock()) {
        continue;
      }
      LOG.debug("Adding " + input.getName() + " to list of lock inputs");
      if (input.getType() == ReadEntity.Type.DATABASE) {
        lockObjects.addAll(getLockObjects(plan, input.getDatabase(), null,
            null, HiveLockMode.SHARED));
      } else  if (input.getType() == ReadEntity.Type.TABLE) {
        lockObjects.addAll(getLockObjects(plan, null, input.getTable(), null,
            HiveLockMode.SHARED));
      } else {
        lockObjects.addAll(getLockObjects(plan, null, null,
            input.getPartition(),
            HiveLockMode.SHARED));
      }
    }

    for (WriteEntity output : plan.getOutputs()) {
      HiveLockMode lockMode = getWriteEntityLockMode(output);
      if (lockMode == null) {
        continue;
      }
      LOG.debug("Adding " + output.getName() + " to list of lock outputs");
      List<HiveLockObj> lockObj = null;
      if (output.getType() == WriteEntity.Type.DATABASE) {
        lockObjects.addAll(getLockObjects(plan, output.getDatabase(), null, null, lockMode));
      } else if (output.getTyp() == WriteEntity.Type.TABLE) {
        lockObj = getLockObjects(plan, null, output.getTable(), null,lockMode);
      } else if (output.getTyp() == WriteEntity.Type.PARTITION) {
        lockObj = getLockObjects(plan, null, null, output.getPartition(), lockMode);
      }
      // In case of dynamic queries, it is possible to have incomplete dummy partitions
      else if (output.getTyp() == WriteEntity.Type.DUMMYPARTITION) {
        lockObj = getLockObjects(plan, null, null, output.getPartition(),
            HiveLockMode.SHARED);
      }

      if(lockObj != null) {
        lockObjects.addAll(lockObj);
        ctx.getOutputLockObjects().put(output, lockObj);
      }
    }

    if (lockObjects.isEmpty() && !ctx.isNeedLockMgr()) {
      return;
    }

    dedupLockObjects(lockObjects);
    List<HiveLock> hiveLocks = lockMgr.lock(lockObjects, false, lDrvState);

    if (hiveLocks == null) {
      throw new LockException(ErrorMsg.LOCK_CANNOT_BE_ACQUIRED.getMsg());
    } else {
      ctx.setHiveLocks(hiveLocks);
    }
  }

  @Override
  public void releaseLocks(List<HiveLock> hiveLocks) throws LockException {
    // If there's no lock manager, it essentially means we didn't acquire locks in the first place,
    // thus no need to release locks
    if (lockMgr != null) {
      lockMgr.releaseLocks(hiveLocks);
    }
  }

  @Override
  public void commitTxn() throws LockException {
    // No-op
  }

  @Override
  public void rollbackTxn() throws LockException {
    // No-op
  }

  @Override
  public void heartbeat() throws LockException {
    // No-op
  }

  @Override
  public ValidTxnList getValidTxns() throws LockException {
    return new ValidReadTxnList();
  }

  @Override
  public String getTxnManagerName() {
    return DummyTxnManager.class.getName();
  }

  @Override
  public boolean supportsExplicitLock() {
    return true;
  }

  @Override
  public boolean useNewShowLocksFormat() {
    return false;
  }

  @Override
  public boolean supportsAcid() {
    return false;
  }


  @Override
  protected void destruct() {
    if (lockMgr != null) {
      try {
        lockMgr.close();
      } catch (LockException e) {
        // Not much I can do about it.
        LOG.warn("Got exception when closing lock manager " + e.getMessage());
      }
    }
  }

  /**
   * Dedup the list of lock objects so that there is only one lock per table/partition.
   * If there is both a shared and exclusive lock for the same object, this will deduped
   * to just a single exclusive lock.  Package level so that the unit tests
   * can access it.  Not intended for use outside this class.
   * @param lockObjects
   */
  static void dedupLockObjects(List<HiveLockObj> lockObjects) {
    Map<String, HiveLockObj> lockMap = new HashMap<String, HiveLockObj>();
    for (HiveLockObj lockObj : lockObjects) {
      String lockName = lockObj.getName();
      HiveLockObj foundLock = lockMap.get(lockName);
      if (foundLock == null || lockObj.getMode() == HiveLockMode.EXCLUSIVE) {
        lockMap.put(lockName, lockObj);
      }
    }
    // copy set of deduped locks back to original list
    lockObjects.clear();
    for (HiveLockObj lockObj : lockMap.values()) {
      lockObjects.add(lockObj);
    }
  }

  private HiveLockMode getWriteEntityLockMode (WriteEntity we) {
    HiveLockMode lockMode = we.isComplete() ? HiveLockMode.EXCLUSIVE : HiveLockMode.SHARED;
    //but the writeEntity is complete in DDL operations, instead DDL sets the writeType, so
    //we use it to determine its lockMode, and first we check if the writeType was set
    WriteEntity.WriteType writeType = we.getWriteType();
    if (writeType == null) {
      return lockMode;
    }
    switch (writeType) {
      case DDL_EXCLUSIVE:
        return HiveLockMode.EXCLUSIVE;
      case DDL_SHARED:
        return HiveLockMode.SHARED;
      case DDL_NO_LOCK:
        return null;
      default: //other writeTypes related to DMLs
        return lockMode;
    }
  }

  private List<HiveLockObj> getLockObjects(QueryPlan plan, Database db,
                                           Table t, Partition p,
                                           HiveLockMode mode)
      throws LockException {
    List<HiveLockObj> locks = new LinkedList<HiveLockObj>();

    HiveLockObject.HiveLockObjectData lockData =
      new HiveLockObject.HiveLockObjectData(plan.getQueryId(),
                             String.valueOf(System.currentTimeMillis()),
                             "IMPLICIT",
                             plan.getQueryStr(),
                             conf);

    if (db != null) {
      locks.add(new HiveLockObj(new HiveLockObject(db.getName(), lockData),
          mode));
      return locks;
    }

    if (t != null) {
      locks.add(new HiveLockObj(new HiveLockObject(t, lockData), mode));
      mode = HiveLockMode.SHARED;
      locks.add(new HiveLockObj(new HiveLockObject(t.getDbName(), lockData), mode));
      return locks;
    }

    if (p != null) {
      if (!(p instanceof DummyPartition)) {
        locks.add(new HiveLockObj(new HiveLockObject(p, lockData), mode));
      }

      // All the parents are locked in shared mode
      mode = HiveLockMode.SHARED;

      // For dummy partitions, only partition name is needed
      String name = p.getName();

      if (p instanceof DummyPartition) {
        name = p.getName().split("@")[2];
      }

      String partialName = "";
      String[] partns = name.split("/");
      int len = p instanceof DummyPartition ? partns.length : partns.length - 1;
      Map<String, String> partialSpec = new LinkedHashMap<String, String>();
      for (int idx = 0; idx < len; idx++) {
        String partn = partns[idx];
        partialName += partn;
        String[] nameValue = partn.split("=");
        assert(nameValue.length == 2);
        partialSpec.put(nameValue[0], nameValue[1]);
        try {
          locks.add(new HiveLockObj(
                      new HiveLockObject(new DummyPartition(p.getTable(), p.getTable().getDbName()
                                                            + "/" + org.apache.hadoop.hive.metastore.utils.MetaStoreUtils.encodeTableName(p.getTable().getTableName())
                                                            + "/" + partialName,
                                                              partialSpec), lockData), mode));
          partialName += "/";
        } catch (HiveException e) {
          throw new LockException(e.getMessage());
        }
      }

      locks.add(new HiveLockObj(new HiveLockObject(p.getTable(), lockData), mode));
      locks.add(new HiveLockObj(new HiveLockObject(p.getTable().getDbName(), lockData), mode));
    }
    return locks;
  }


}
