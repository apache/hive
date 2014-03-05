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
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject.HiveLockObjectData;
import org.apache.hadoop.hive.ql.metadata.*;

import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * shared lock manager for dedicated hive server. all locks are managed in memory
 */
public class EmbeddedLockManager implements HiveLockManager {

  private static final Log LOG = LogFactory.getLog("EmbeddedHiveLockManager");

  private final Node root = new Node();

  private HiveLockManagerCtx ctx;

  private int sleepTime = 1000;
  private int numRetriesForLock = 0;
  private int numRetriesForUnLock = 0;

  public EmbeddedLockManager() {
  }

  public void setContext(HiveLockManagerCtx ctx) throws LockException {
    this.ctx = ctx;
    refresh();
  }

  public HiveLock lock(HiveLockObject key, HiveLockMode mode, boolean keepAlive)
      throws LockException {
    return lock(key, mode, numRetriesForLock, sleepTime);
  }

  public List<HiveLock> lock(List<HiveLockObj> objs, boolean keepAlive) throws LockException {
    return lock(objs, numRetriesForLock, sleepTime);
  }

  public void unlock(HiveLock hiveLock) throws LockException {
    unlock(hiveLock, numRetriesForUnLock, sleepTime);
  }

  public void releaseLocks(List<HiveLock> hiveLocks) {
    releaseLocks(hiveLocks, numRetriesForUnLock, sleepTime);
  }

  public List<HiveLock> getLocks(boolean verifyTablePartitions, boolean fetchData)
      throws LockException {
    return getLocks(verifyTablePartitions, fetchData, ctx.getConf());
  }

  public List<HiveLock> getLocks(HiveLockObject key, boolean verifyTablePartitions,
      boolean fetchData) throws LockException {
    return getLocks(key, verifyTablePartitions, fetchData, ctx.getConf());
  }

  public void prepareRetry() {
  }

  public void refresh() {
    HiveConf conf = ctx.getConf();
    sleepTime = conf.getIntVar(HiveConf.ConfVars.HIVE_LOCK_SLEEP_BETWEEN_RETRIES) * 1000;
    numRetriesForLock = conf.getIntVar(HiveConf.ConfVars.HIVE_LOCK_NUMRETRIES);
    numRetriesForUnLock = conf.getIntVar(HiveConf.ConfVars.HIVE_UNLOCK_NUMRETRIES);
  }

  public HiveLock lock(HiveLockObject key, HiveLockMode mode, int numRetriesForLock, int sleepTime)
      throws LockException {
    for (int i = 0; i <= numRetriesForLock; i++) {
      if (i > 0) {
        sleep(sleepTime);
      }
      HiveLock lock = lockPrimitive(key, mode);
      if (lock != null) {
        return lock;
      }
    }
    return null;
  }

  private void sleep(int sleepTime) {
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      // ignore
    }
  }

  public List<HiveLock> lock(List<HiveLockObj> objs, int numRetriesForLock, int sleepTime)
      throws LockException {
    sortLocks(objs);
    for (int i = 0; i <= numRetriesForLock; i++) {
      if (i > 0) {
        sleep(sleepTime);
      }
      List<HiveLock> locks = lockPrimitive(objs, numRetriesForLock, sleepTime);
      if (locks != null) {
        return locks;
      }
    }
    return null;
  }

  private HiveLock lockPrimitive(HiveLockObject key, HiveLockMode mode) throws LockException {
    if (root.lock(key.getPaths(), key.getData(), mode == HiveLockMode.EXCLUSIVE)) {
      return new SimpleHiveLock(key, mode);
    }
    return null;
  }

  private List<HiveLock> lockPrimitive(List<HiveLockObj> objs, int numRetriesForLock,
      int sleepTime) throws LockException {
    List<HiveLock> locks = new ArrayList<HiveLock>();
    for (HiveLockObj obj : objs) {
      HiveLock lock = lockPrimitive(obj.getObj(), obj.getMode());
      if (lock == null) {
        releaseLocks(locks, numRetriesForLock, sleepTime);
        return null;
      }
      locks.add(lock);
    }
    return locks;
  }

  private void sortLocks(List<HiveLockObj> objs) {
    Collections.sort(objs, new Comparator<HiveLockObj>() {
      public int compare(HiveLockObj o1, HiveLockObj o2) {
        int cmp = o1.getName().compareTo(o2.getName());
        if (cmp == 0) {
          if (o1.getMode() == o2.getMode()) {
            return cmp;
          }
          // EXCLUSIVE locks occur before SHARED locks
          if (o1.getMode() == HiveLockMode.EXCLUSIVE) {
            return -1;
          }
          return +1;
        }
        return cmp;
      }
    });
  }

  public void unlock(HiveLock hiveLock, int numRetriesForUnLock, int sleepTime)
      throws LockException {
    String[] paths = hiveLock.getHiveLockObject().getPaths();
    HiveLockObjectData data = hiveLock.getHiveLockObject().getData();
    for (int i = 0; i <= numRetriesForUnLock; i++) {
      if (i > 0) {
        sleep(sleepTime);
      }
      if (root.unlock(paths, data)) {
        return;
      }
    }
    throw new LockException("Failed to release lock " + hiveLock);
  }

  public void releaseLocks(List<HiveLock> hiveLocks, int numRetriesForUnLock, int sleepTime) {
    for (HiveLock locked : hiveLocks) {
      try {
        unlock(locked, numRetriesForUnLock, sleepTime);
      } catch (LockException e) {
        LOG.info(e);
      }
    }
  }

  public List<HiveLock> getLocks(boolean verifyTablePartitions, boolean fetchData, HiveConf conf)
      throws LockException {
    return root.getLocks(verifyTablePartitions, fetchData, conf);
  }

  public List<HiveLock> getLocks(HiveLockObject key, boolean verifyTablePartitions,
      boolean fetchData, HiveConf conf) throws LockException {
    return root.getLocks(key.getPaths(), verifyTablePartitions, fetchData, conf);
  }

  // from ZooKeeperHiveLockManager
  private HiveLockObject verify(boolean verify, String[] names, HiveLockObjectData data,
      HiveConf conf) throws LockException {
    if (!verify) {
      return new HiveLockObject(names, data);
    }
    String database = names[0];
    String table = names[1];

    try {
      Hive db = Hive.get(conf);
      Table tab = db.getTable(database, table, false);
      if (tab == null) {
        return null;
      }
      if (names.length == 2) {
        return new HiveLockObject(tab, data);
      }
      Map<String, String> partSpec = new HashMap<String, String>();
      for (int indx = 2; indx < names.length; indx++) {
        String[] partVals = names[indx].split("=");
        partSpec.put(partVals[0], partVals[1]);
      }

      Partition partn;
      try {
        partn = db.getPartition(tab, partSpec, false);
      } catch (HiveException e) {
        partn = null;
      }

      if (partn == null) {
        return new HiveLockObject(new DummyPartition(tab, null, partSpec), data);
      }

      return new HiveLockObject(partn, data);
    } catch (Exception e) {
      throw new LockException(e);
    }
  }

  public void close() {
    root.lock.lock();
    try {
      root.datas = null;
      root.children = null;
    } finally {
      root.lock.unlock();
    }
  }

  private class Node {

    private boolean exclusive;
    private Map<String, Node> children;
    private Map<String, HiveLockObjectData> datas;
    private final ReentrantLock lock = new ReentrantLock();

    public Node() {
    }

    public void set(HiveLockObjectData data, boolean exclusive) {
      this.exclusive = exclusive;
      if (datas == null) {
        datas = new HashMap<String, HiveLockObjectData>(3);
      }
      datas.put(data.getQueryId(), data);
    }

    public boolean lock(String[] paths, HiveLockObjectData data, boolean exclusive) {
      return lock(paths, 0, data, exclusive);
    }

    public boolean unlock(String[] paths, HiveLockObjectData data) {
      return unlock(paths, 0, data);
    }

    private List<HiveLock> getLocks(boolean verify, boolean fetchData, HiveConf conf)
        throws LockException {
      if (!root.hasChild()) {
        return Collections.emptyList();
      }
      List<HiveLock> locks = new ArrayList<HiveLock>();
      getLocks(new Stack<String>(), verify, fetchData, locks, conf);
      return locks;
    }

    private List<HiveLock> getLocks(String[] paths, boolean verify, boolean fetchData,
        HiveConf conf) throws LockException {
      if (!root.hasChild()) {
        return Collections.emptyList();
      }
      List<HiveLock> locks = new ArrayList<HiveLock>();
      getLocks(paths, 0, verify, fetchData, locks, conf);
      return locks;
    }

    private boolean lock(String[] paths, int index, HiveLockObjectData data, boolean exclusive) {
      if (!lock.tryLock()) {
        return false;
      }
      try {
        if (index == paths.length) {
          if (this.exclusive || exclusive && hasLock()) {
            return false;
          }
          set(data, exclusive);
          return true;
        }
        Node child;
        if (children == null) {
          children = new HashMap<String, Node>(3);
          children.put(paths[index], child = new Node());
        } else {
          child = children.get(paths[index]);
          if (child == null) {
            children.put(paths[index], child = new Node());
          }
        }
        return child.lock(paths, index + 1, data, exclusive);
      } finally {
        lock.unlock();
      }
    }

    private boolean unlock(String[] paths, int index, HiveLockObjectData data) {
      if (!lock.tryLock()) {
        return false;
      }
      try {
        if (index == paths.length) {
          if (hasLock()) {
            datas.remove(data.getQueryId());
          }
          return true;
        }
        Node child = children == null ? null : children.get(paths[index]);
        if (child == null) {
          return true; // should not happen
        }
        if (child.unlock(paths, index + 1, data)) {
          if (!child.hasLock() && !child.hasChild()) {
            children.remove(paths[index]);
          }
          return true;
        }
        return false;
      } finally {
        lock.unlock();
      }
    }

    private void getLocks(Stack<String> names, boolean verify,
        boolean fetchData, List<HiveLock> locks, HiveConf conf) throws LockException {
      lock.lock();
      try {
        if (hasLock()) {
          getLocks(names.toArray(new String[names.size()]), verify, fetchData, locks, conf);
        }
        if (children != null) {
          for (Map.Entry<String, Node> entry : children.entrySet()) {
            names.push(entry.getKey());
            entry.getValue().getLocks(names, verify, fetchData, locks, conf);
            names.pop();
          }
        }
      } finally {
        lock.unlock();
      }
    }

    private void getLocks(String[] paths, int index, boolean verify,
        boolean fetchData, List<HiveLock> locks, HiveConf conf) throws LockException {
      lock.lock();
      try {
        if (index == paths.length) {
          getLocks(paths, verify, fetchData, locks, conf);
          return;
        }
        Node child = children.get(paths[index]);
        if (child != null) {
          child.getLocks(paths, index + 1, verify, fetchData, locks, conf);
        }
      } finally {
        lock.unlock();
      }
    }

    private void getLocks(String[] paths, boolean verify, boolean fetchData, List<HiveLock> locks,
        HiveConf conf) throws LockException {
      HiveLockMode lockMode = getLockMode();
      if (fetchData) {
        for (HiveLockObjectData data : datas.values()) {
          HiveLockObject lock = verify(verify, paths, data, conf);
          if (lock != null) {
            locks.add(new SimpleHiveLock(lock, lockMode));
          }
        }
      } else {
        HiveLockObject lock = verify(verify, paths, null, conf);
        if (lock != null) {
          locks.add(new SimpleHiveLock(lock, lockMode));
        }
      }
    }

    private HiveLockMode getLockMode() {
      return exclusive ? HiveLockMode.EXCLUSIVE : HiveLockMode.SHARED;
    }

    private boolean hasLock() {
      return datas != null && !datas.isEmpty();
    }

    private boolean hasChild() {
      return children != null && !children.isEmpty();
    }
  }

  private static class SimpleHiveLock extends HiveLock {

    private final HiveLockObject lockObj;
    private final HiveLockMode lockMode;

    public SimpleHiveLock(HiveLockObject lockObj, HiveLockMode lockMode) {
      this.lockObj = lockObj;
      this.lockMode = lockMode;
    }

    @Override
    public HiveLockObject getHiveLockObject() {
      return lockObj;
    }

    @Override
    public HiveLockMode getHiveLockMode() {
      return lockMode;
    }

    @Override
    public String toString() {
      return lockMode + "=" + lockObj.getDisplayName() + "(" + lockObj.getData() + ")";
    }

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof SimpleHiveLock)) {
        return false;
      }

      SimpleHiveLock simpleLock = (SimpleHiveLock) o;
      return lockObj.equals(simpleLock.getHiveLockObject()) &&
          lockMode == simpleLock.getHiveLockMode();
    }
  }
}
