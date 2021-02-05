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

package org.apache.hadoop.hive.ql.ddl.table.lock.show;

import org.apache.hadoop.hive.ql.ddl.DDLOperationContext;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lockmgr.DbLockManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockMode;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject.HiveLockObjectData;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.api.ShowLocksRequest;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ddl.DDLOperation;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Operation process showing the locks.
 */
public class ShowLocksOperation extends DDLOperation<ShowLocksDesc> {
  public ShowLocksOperation(DDLOperationContext context, ShowLocksDesc desc) {
    super(context, desc);
  }

  @Override
  public int execute() throws HiveException {
    Context ctx = context.getContext();
    HiveTxnManager txnManager = ctx.getHiveTxnManager();
    HiveLockManager lockMgr = txnManager.getLockManager();

    if (desc.isNewFormat()) {
      return showLocksNewFormat(lockMgr);
    } else {
      return showLocksOldFormat(lockMgr);
    }
  }

  private int showLocksOldFormat(HiveLockManager lockMgr) throws HiveException {
    if (lockMgr == null) {
      throw new HiveException("show Locks LockManager not specified");
    }

    // write the results in the file
    try (DataOutputStream os = ShowUtils.getOutputStream(new Path(desc.getResFile()), context)) {
      List<HiveLock> locks = getLocksForOldFormat(lockMgr);
      writeLocksInOldFormat(os, locks);
    } catch (IOException e) {
      LOG.warn("show function: ", e);
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString(), e);
    }

    return 0;
  }

  private List<HiveLock> getLocksForOldFormat(HiveLockManager lockMgr) throws LockException, HiveException {
    List<HiveLock> locks = null;
    if (desc.getTableName() == null) {
      // TODO should be doing security check here. Users should not be able to see each other's locks.
      locks = lockMgr.getLocks(false, desc.isExt());
    } else {
      HiveLockObject lockObject = HiveLockObject.createFrom(context.getDb(), desc.getTableName(), desc.getPartSpec());
      locks = lockMgr.getLocks(lockObject, true, desc.isExt());
    }
    Collections.sort(locks, new Comparator<HiveLock>() {
      @Override
      public int compare(HiveLock o1, HiveLock o2) {
        int cmp = o1.getHiveLockObject().getName().compareTo(o2.getHiveLockObject().getName());
        if (cmp != 0) {
          return cmp;
        }

        if (o1.getHiveLockMode() == o2.getHiveLockMode()) {
          return 0;
        }

        // EXCLUSIVE locks occur before SHARED locks
        return (o1.getHiveLockMode() == HiveLockMode.EXCLUSIVE) ? -1 : +1;
      }
    });
    return locks;
  }

  private void writeLocksInOldFormat(DataOutputStream os, List<HiveLock> locks) throws IOException {
    for (HiveLock lock : locks) {
      os.writeBytes(lock.getHiveLockObject().getDisplayName());
      os.write(Utilities.tabCode);
      os.writeBytes(lock.getHiveLockMode().toString());

      if (desc.isExt()) {
        HiveLockObjectData lockData = lock.getHiveLockObject().getData();
        if (lockData != null) {
          os.write(Utilities.newLineCode);
          os.writeBytes("LOCK_QUERYID:" + lockData.getQueryId());
          os.write(Utilities.newLineCode);
          os.writeBytes("LOCK_TIME:" + lockData.getLockTime());
          os.write(Utilities.newLineCode);
          os.writeBytes("LOCK_MODE:" + lockData.getLockMode());
          os.write(Utilities.newLineCode);
          os.writeBytes("LOCK_QUERYSTRING:" + lockData.getQueryStr());
        }
      }

      os.write(Utilities.newLineCode);
    }
  }

  private int showLocksNewFormat(HiveLockManager lockMgr) throws HiveException {
    ShowLocksResponse response = getLocksForNewFormat(lockMgr);

    // write the results in the file
    try (DataOutputStream os = ShowUtils.getOutputStream(new Path(desc.getResFile()), context)) {
      dumpLockInfo(os, response);
    } catch (IOException e) {
      LOG.warn("show function: ", e);
      return 1;
    } catch (Exception e) {
      throw new HiveException(e.toString());
    }

    return 0;
  }

  private ShowLocksResponse getLocksForNewFormat(HiveLockManager lockMgr) throws HiveException, LockException {
    if (!(lockMgr instanceof DbLockManager)) {
      throw new HiveException("New lock format only supported with db lock manager.");
    }

    ShowLocksRequest request = new ShowLocksRequest();
    if (desc.getDbName() == null && desc.getTableName() != null) {
      request.setDbname(SessionState.get().getCurrentDatabase());
    } else {
      request.setDbname(desc.getDbName());
    }
    request.setTablename(desc.getTableName());
    if (desc.getPartSpec() != null) {
      List<String> keyList = new ArrayList<String>();
      List<String> valList = new ArrayList<String>();
      for (String partKey : desc.getPartSpec().keySet()) {
        String partVal = desc.getPartSpec().get(partKey);
        keyList.add(partKey);
        valList.add(partVal);
      }
      String partName = FileUtils.makePartName(keyList, valList);
      request.setPartname(partName);
    }

    return ((DbLockManager)lockMgr).getLocks(request);
  }

  public static void dumpLockInfo(DataOutputStream os, ShowLocksResponse response) throws IOException {
    SessionState sessionState = SessionState.get();
    // Write a header for CliDriver
    if (!sessionState.isHiveServerQuery()) {
      os.writeBytes("Lock ID");
      os.write(Utilities.tabCode);
      os.writeBytes("Database");
      os.write(Utilities.tabCode);
      os.writeBytes("Table");
      os.write(Utilities.tabCode);
      os.writeBytes("Partition");
      os.write(Utilities.tabCode);
      os.writeBytes("State");
      os.write(Utilities.tabCode);
      os.writeBytes("Blocked By");
      os.write(Utilities.tabCode);
      os.writeBytes("Type");
      os.write(Utilities.tabCode);
      os.writeBytes("Transaction ID");
      os.write(Utilities.tabCode);
      os.writeBytes("Last Heartbeat");
      os.write(Utilities.tabCode);
      os.writeBytes("Acquired At");
      os.write(Utilities.tabCode);
      os.writeBytes("User");
      os.write(Utilities.tabCode);
      os.writeBytes("Hostname");
      os.write(Utilities.tabCode);
      os.writeBytes("Agent Info");
      os.write(Utilities.newLineCode);
    }

    List<ShowLocksResponseElement> locks = response.getLocks();
    if (locks != null) {
      for (ShowLocksResponseElement lock : locks) {
        if (lock.isSetLockIdInternal()) {
          os.writeBytes(Long.toString(lock.getLockid()) + "." + Long.toString(lock.getLockIdInternal()));
        } else {
          os.writeBytes(Long.toString(lock.getLockid()));
        }
        os.write(Utilities.tabCode);
        os.writeBytes(lock.getDbname());
        os.write(Utilities.tabCode);
        os.writeBytes((lock.getTablename() == null) ? "NULL" : lock.getTablename());
        os.write(Utilities.tabCode);
        os.writeBytes((lock.getPartname() == null) ? "NULL" : lock.getPartname());
        os.write(Utilities.tabCode);
        os.writeBytes(lock.getState().toString());
        os.write(Utilities.tabCode);
        if (lock.isSetBlockedByExtId()) {//both "blockedby" are either there or not
          os.writeBytes(Long.toString(lock.getBlockedByExtId()) + "." + Long.toString(lock.getBlockedByIntId()));
        } else {
          os.writeBytes("            "); //12 chars - try to keep cols aligned
        }
        os.write(Utilities.tabCode);
        os.writeBytes(lock.getType().toString());
        os.write(Utilities.tabCode);
        os.writeBytes((lock.getTxnid() == 0) ? "NULL" : Long.toString(lock.getTxnid()));
        os.write(Utilities.tabCode);
        os.writeBytes(Long.toString(lock.getLastheartbeat()));
        os.write(Utilities.tabCode);
        os.writeBytes((lock.getAcquiredat() == 0) ? "NULL" : Long.toString(lock.getAcquiredat()));
        os.write(Utilities.tabCode);
        os.writeBytes(lock.getUser());
        os.write(Utilities.tabCode);
        os.writeBytes(lock.getHostname());
        os.write(Utilities.tabCode);
        os.writeBytes(lock.getAgentInfo() == null ? "NULL" : lock.getAgentInfo());
        os.write(Utilities.newLineCode);
      }
    }
  }
}
