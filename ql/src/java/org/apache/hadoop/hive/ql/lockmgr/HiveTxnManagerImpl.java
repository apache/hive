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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.Driver.LockedDriverState;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject.HiveLockObjectData;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.LockDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.LockTableDesc;
import org.apache.hadoop.hive.ql.plan.UnlockDatabaseDesc;
import org.apache.hadoop.hive.ql.plan.UnlockTableDesc;

/**
 * An implementation HiveTxnManager that includes internal methods that all
 * transaction managers need to implement but that we don't want to expose to
 * outside.
 */
abstract class HiveTxnManagerImpl implements HiveTxnManager, Configurable {

  protected HiveConf conf;

  void setHiveConf(HiveConf c) {
    setConf(c);
  }

  @Override
  public void setConf(Configuration c) {
    conf = (HiveConf) c;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  abstract protected void destruct();

  @Override
  public void closeTxnManager() {
    destruct();
  }

  @Override
  public void acquireLocks(QueryPlan plan, Context ctx, String username, LockedDriverState lDrvState) throws LockException {
    acquireLocks(plan, ctx, username);
  }

  @Override
  protected void finalize() throws Throwable {
    destruct();
  }
  @Override
  public int lockTable(Hive db, LockTableDesc lockTbl) throws HiveException {
    HiveLockManager lockMgr = getAndCheckLockManager();

    HiveLockMode mode = HiveLockMode.valueOf(lockTbl.getMode());
    String tabName = lockTbl.getTableName();
    Table  tbl = db.getTable(tabName);
    if (tbl == null) {
      throw new HiveException("Table " + tabName + " does not exist ");
    }

    Map<String, String> partSpec = lockTbl.getPartSpec();
    HiveLockObjectData lockData =
        new HiveLockObjectData(lockTbl.getQueryId(),
            String.valueOf(System.currentTimeMillis()),
            "EXPLICIT",
            lockTbl.getQueryStr(),
            conf);

    if (partSpec == null) {
      HiveLock lck = lockMgr.lock(new HiveLockObject(tbl, lockData), mode, true);
      if (lck == null) {
        return 1;
      }
      return 0;
    }

    Partition par = db.getPartition(tbl, partSpec, false);
    if (par == null) {
      throw new HiveException("Partition " + partSpec + " for table " +
          tabName + " does not exist");
    }
    HiveLock lck = lockMgr.lock(new HiveLockObject(par, lockData), mode, true);
    if (lck == null) {
      return 1;
    }
    return 0;
  }

  @Override
  public int unlockTable(Hive hiveDB, UnlockTableDesc unlockTbl) throws HiveException {
    HiveLockManager lockMgr = getAndCheckLockManager();

    String tabName = unlockTbl.getTableName();
    HiveLockObject obj = HiveLockObject.createFrom(hiveDB, tabName,
        unlockTbl.getPartSpec());

    List<HiveLock> locks = lockMgr.getLocks(obj, false, false);
    if ((locks == null) || (locks.isEmpty())) {
      throw new HiveException("Table " + tabName + " is not locked ");
    }
    Iterator<HiveLock> locksIter = locks.iterator();
    while (locksIter.hasNext()) {
      HiveLock lock = locksIter.next();
      lockMgr.unlock(lock);
    }

    return 0;
  }

  @Override
  public int lockDatabase(Hive hiveDB, LockDatabaseDesc lockDb) throws HiveException {
    HiveLockManager lockMgr = getAndCheckLockManager();

    HiveLockMode mode = HiveLockMode.valueOf(lockDb.getMode());
    String dbName = lockDb.getDatabaseName();

    Database dbObj = hiveDB.getDatabase(dbName);
    if (dbObj == null) {
      throw new HiveException("Database " + dbName + " does not exist ");
    }

    HiveLockObjectData lockData =
        new HiveLockObjectData(lockDb.getQueryId(),
            String.valueOf(System.currentTimeMillis()),
            "EXPLICIT", lockDb.getQueryStr(), conf);

    HiveLock lck = lockMgr.lock(new HiveLockObject(dbObj.getName(), lockData), mode, true);
    if (lck == null) {
      return 1;
    }
    return 0;
  }

  @Override
  public int unlockDatabase(Hive hiveDB, UnlockDatabaseDesc unlockDb) throws HiveException {
    HiveLockManager lockMgr = getAndCheckLockManager();

    String dbName = unlockDb.getDatabaseName();

    Database dbObj = hiveDB.getDatabase(dbName);
    if (dbObj == null) {
      throw new HiveException("Database " + dbName + " does not exist ");
    }
    HiveLockObject obj = new HiveLockObject(dbObj.getName(), null);

    List<HiveLock> locks = lockMgr.getLocks(obj, false, false);
    if ((locks == null) || (locks.isEmpty())) {
      throw new HiveException("Database " + dbName + " is not locked ");
    }

    for (HiveLock lock: locks) {
      lockMgr.unlock(lock);

    }
    return 0;
  }

  /**
   * Gets the lock manager and verifies if the explicit lock is supported
   * @return  the lock manager
   * @throws HiveException
   */
  protected HiveLockManager getAndCheckLockManager() throws HiveException {
    HiveLockManager lockMgr = getLockManager();
    if (lockMgr == null) {
      throw new HiveException("LockManager cannot be acquired");
    }

    if (!supportsExplicitLock()) {
      throw new HiveException(ErrorMsg.LOCK_REQUEST_UNSUPPORTED,
          conf.getVar(HiveConf.ConfVars.HIVE_TXN_MANAGER));
    }

    return lockMgr;
  }
  @Override
  public boolean recordSnapshot(QueryPlan queryPlan) {
    return false;
  }
  @Override
  public boolean isImplicitTransactionOpen() {
    return true;
  }

}
