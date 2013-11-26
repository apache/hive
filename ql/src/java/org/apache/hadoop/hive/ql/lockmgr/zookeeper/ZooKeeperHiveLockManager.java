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

package org.apache.hadoop.hive.ql.lockmgr.zookeeper;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManagerCtx;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockMode;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObj;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject.HiveLockObjectData;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

import com.google.common.annotations.VisibleForTesting;

public class ZooKeeperHiveLockManager implements HiveLockManager {
  HiveLockManagerCtx ctx;
  public static final Log LOG = LogFactory.getLog("ZooKeeperHiveLockManager");
  static final private LogHelper console = new LogHelper(LOG);

  private ZooKeeper zooKeeper;

  // All the locks are created under this parent
  private String    parent;

  private int sessionTimeout;
  private String quorumServers;

  private int sleepTime;
  private int numRetriesForLock;
  private int numRetriesForUnLock;

  private static String clientIp;

  static {
    clientIp = "UNKNOWN";
    try {
      InetAddress clientAddr = InetAddress.getLocalHost();
      clientIp = clientAddr.getHostAddress();
    } catch (Exception e1) {
    }
  }

  public ZooKeeperHiveLockManager() {
  }

  /**
   * @param conf  The hive configuration
   * Get the quorum server address from the configuration. The format is:
   * host1:port, host2:port..
   **/
  @VisibleForTesting
  static String getQuorumServers(HiveConf conf) {
    String[] hosts = conf.getVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM).split(",");
    String port = conf.getVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT);
    StringBuilder quorum = new StringBuilder();
    for(int i=0; i<hosts.length; i++) {
      quorum.append(hosts[i].trim());
      if (!hosts[i].contains(":")) {
        // if the hostname doesn't contain a port, add the configured port to hostname
        quorum.append(":");
        quorum.append(port);
      }

      if (i != hosts.length-1)
        quorum.append(",");
    }

    return quorum.toString();
  }

  /**
   * @param ctx  The lock manager context (containing the Hive configuration file)
   * Start the ZooKeeper client based on the zookeeper cluster specified in the conf.
   **/
  public void setContext(HiveLockManagerCtx ctx) throws LockException {
    this.ctx = ctx;
    HiveConf conf = ctx.getConf();
    sessionTimeout = conf.getIntVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT);
    quorumServers = ZooKeeperHiveLockManager.getQuorumServers(conf);

    sleepTime = conf.getIntVar(HiveConf.ConfVars.HIVE_LOCK_SLEEP_BETWEEN_RETRIES) * 1000;
    numRetriesForLock = conf.getIntVar(HiveConf.ConfVars.HIVE_LOCK_NUMRETRIES);
    numRetriesForUnLock = conf.getIntVar(HiveConf.ConfVars.HIVE_UNLOCK_NUMRETRIES);

    try {
      renewZookeeperInstance(sessionTimeout, quorumServers);
      parent = conf.getVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_NAMESPACE);

      try {
        zooKeeper.create("/" +  parent, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } catch (KeeperException e) {
        // ignore if the parent already exists
        if (e.code() != KeeperException.Code.NODEEXISTS) {
          LOG.warn("Unexpected ZK exception when creating parent node /" + parent, e);
        }
      }

    } catch (Exception e) {
      LOG.error("Failed to create ZooKeeper object: ", e);
      throw new LockException(ErrorMsg.ZOOKEEPER_CLIENT_COULD_NOT_BE_INITIALIZED.getMsg());
    }
  }

  @Override
  public void refresh() {
    HiveConf conf = ctx.getConf();
    sleepTime = conf.getIntVar(HiveConf.ConfVars.HIVE_LOCK_SLEEP_BETWEEN_RETRIES) * 1000;
    numRetriesForLock = conf.getIntVar(HiveConf.ConfVars.HIVE_LOCK_NUMRETRIES);
    numRetriesForUnLock = conf.getIntVar(HiveConf.ConfVars.HIVE_UNLOCK_NUMRETRIES);
  }

  private void renewZookeeperInstance(int sessionTimeout, String quorumServers)
      throws InterruptedException, IOException {
    if (zooKeeper != null) {
      return;
    }

    zooKeeper = new ZooKeeper(quorumServers, sessionTimeout, new DummyWatcher());
  }

  /**
   * @param key    object to be locked
   * Get the name of the last string. For eg. if you need to lock db/T/ds=1=/hr=1,
   * the last name would be db/T/ds=1/hr=1
   **/
  private static String getLastObjectName(String parent, HiveLockObject key) {
    return "/" + parent + "/" + key.getName();
  }

  /**
   * @param key    object to be locked
   * Get the list of names for all the parents.
   * For eg: if you need to lock db/T/ds=1/hr=1, the following list will be returned:
   * {db, db/T, db/T/ds=1, db/T/ds=1/hr=1}
   **/
  private List<String> getObjectNames(HiveLockObject key) {
    List<String> parents = new ArrayList<String>();
    String   curParent   = "/" + parent + "/";
    String[] names       = key.getName().split("/");

    for (String name : names) {
      curParent = curParent + name;
      parents.add(curParent);
      curParent = curParent + "/";
    }
    return parents;
  }

  /**
   * @param  lockObjects  List of objects and the modes of the locks requested
   * @param  keepAlive    Whether the lock is to be persisted after the statement
   *
   * Acuire all the locks. Release all the locks and return null if any lock
   * could not be acquired.
   **/
  public List<HiveLock> lock(List<HiveLockObj> lockObjects,
      boolean keepAlive) throws LockException
  {
    // Sort the objects first. You are guaranteed that if a partition is being locked,
    // the table has already been locked

    Collections.sort(lockObjects, new Comparator<HiveLockObj>() {

    @Override
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

    // walk the list and acquire the locks - if any lock cant be acquired, release all locks, sleep
    // and retry
    HiveLockObj prevLockObj = null;
    List<HiveLock> hiveLocks = new ArrayList<HiveLock>();

    for (HiveLockObj lockObject : lockObjects) {
      // No need to acquire a lock twice on the same object
      // It is ensured that EXCLUSIVE locks occur before SHARED locks on the same object
      if ((prevLockObj != null) && (prevLockObj.getName().equals(lockObject.getName()))) {
        prevLockObj = lockObject;
        continue;
      }

      HiveLock lock = null;
      try {
        lock = lock(lockObject.getObj(), lockObject.getMode(), keepAlive, true);
      } catch (LockException e) {
        console.printError("Error in acquireLocks..." );
        LOG.error("Error in acquireLocks...", e);
        lock = null;
      }

      if (lock == null) {
        releaseLocks(hiveLocks);
        return null;
      }

      hiveLocks.add(lock);
      prevLockObj = lockObject;
    }

    return hiveLocks;

  }

  /**
   * @param hiveLocks
   *          list of hive locks to be released Release all the locks specified. If some of the
   *          locks have already been released, ignore them
   **/
  public void releaseLocks(List<HiveLock> hiveLocks) {
    if (hiveLocks != null) {
      int len = hiveLocks.size();
      for (int pos = len-1; pos >= 0; pos--) {
        HiveLock hiveLock = hiveLocks.get(pos);
        try {
          LOG.info(" about to release lock for " + hiveLock.getHiveLockObject().getName());
          unlock(hiveLock);
        } catch (LockException e) {
          // The lock may have been released. Ignore and continue
          LOG.warn("Error when releasing lock", e);
        }
      }
    }
  }

  /**
   * @param key
   *          The object to be locked
   * @param mode
   *          The mode of the lock
   * @param keepAlive
   *          Whether the lock is to be persisted after the statement Acuire the
   *          lock. Return null if a conflicting lock is present.
   **/
  public ZooKeeperHiveLock lock(HiveLockObject key, HiveLockMode mode,
      boolean keepAlive) throws LockException {
    return lock(key, mode, keepAlive, false);
  }

  /**
   * @param name
   *          The name of the zookeeper child
   * @param data
   *          The data for the zookeeper child
   * @param mode
   *          The mode in which the child needs to be created
   * @throws KeeperException
   * @throws InterruptedException
   **/
  private String createChild(String name, byte[] data, CreateMode mode)
      throws KeeperException, InterruptedException {
    return zooKeeper.create(name, data, Ids.OPEN_ACL_UNSAFE, mode);
  }

  private String getLockName(String parent, HiveLockMode mode) {
    return parent + "/" + "LOCK-" + mode + "-";
  }

  private ZooKeeperHiveLock lock (HiveLockObject key, HiveLockMode mode,
      boolean keepAlive, boolean parentCreated) throws LockException {
    int tryNum = 1;
    ZooKeeperHiveLock ret = null;

    do {
      try {
        if (tryNum > 1) {
          Thread.sleep(sleepTime);
          prepareRetry();
        }
        ret = lockPrimitive(key, mode, keepAlive, parentCreated);
        if (ret != null) {
          break;
        }
        tryNum++;
      } catch (Exception e1) {
        tryNum++;
        if (e1 instanceof KeeperException) {
          KeeperException e = (KeeperException) e1;
          switch (e.code()) {
          case CONNECTIONLOSS:
          case OPERATIONTIMEOUT:
            LOG.warn("Possibly transient ZooKeeper exception: ", e);
            break;
          default:
            LOG.error("Serious Zookeeper exception: ", e);
            break;
          }
        }
        if (tryNum >= numRetriesForLock) {
          throw new LockException(e1);
        }
      }
    } while (tryNum < numRetriesForLock);

    return ret;
  }

  private ZooKeeperHiveLock lockPrimitive(HiveLockObject key,
      HiveLockMode mode, boolean keepAlive, boolean parentCreated)
      throws KeeperException, InterruptedException {
    String res;

    // If the parents have already been created, create the last child only
    List<String> names = new ArrayList<String>();
    String lastName;

    HiveLockObjectData lockData = key.getData();
    lockData.setClientIp(clientIp);

    if (parentCreated) {
      lastName = getLastObjectName(parent, key);
      names.add(lastName);
    } else {
      names = getObjectNames(key);
      lastName = names.get(names.size() - 1);
    }

    // Create the parents first
    for (String name : names) {
      try {
        res = createChild(name, new byte[0], CreateMode.PERSISTENT);
      } catch (KeeperException e) {
        if (e.code() != KeeperException.Code.NODEEXISTS) {
          //if the exception is not 'NODEEXISTS', re-throw it
          throw e;
        }
      }
    }

    res = createChild(getLockName(lastName, mode), key.getData().toString()
        .getBytes(), keepAlive ? CreateMode.PERSISTENT_SEQUENTIAL
        : CreateMode.EPHEMERAL_SEQUENTIAL);

    int seqNo = getSequenceNumber(res, getLockName(lastName, mode));
    if (seqNo == -1) {
      zooKeeper.delete(res, -1);
      return null;
    }

    List<String> children = zooKeeper.getChildren(lastName, false);

    String exLock = getLockName(lastName, HiveLockMode.EXCLUSIVE);
    String shLock = getLockName(lastName, HiveLockMode.SHARED);

    for (String child : children) {
      child = lastName + "/" + child;

      // Is there a conflicting lock on the same object with a lower sequence
      // number
      int childSeq = seqNo;
      if (child.startsWith(exLock)) {
        childSeq = getSequenceNumber(child, exLock);
      }
      if ((mode == HiveLockMode.EXCLUSIVE) && child.startsWith(shLock)) {
        childSeq = getSequenceNumber(child, shLock);
      }

      if ((childSeq >= 0) && (childSeq < seqNo)) {
        zooKeeper.delete(res, -1);
        console.printError("conflicting lock present for "
            + key.getDisplayName() + " mode " + mode);
        return null;
      }
    }

    return new ZooKeeperHiveLock(res, key, mode);
  }

  /* Remove the lock specified */
  public void unlock(HiveLock hiveLock) throws LockException {
    unlockWithRetry(ctx.getConf(), zooKeeper, hiveLock, parent);
  }

  private void unlockWithRetry(HiveConf conf, ZooKeeper zkpClient,
      HiveLock hiveLock, String parent) throws LockException {

    int tryNum = 0;
    do {
      try {
        tryNum++;
        if (tryNum > 1) {
          Thread.sleep(sleepTime);
          prepareRetry();
        }
        unlockPrimitive(conf, zkpClient, hiveLock, parent);
        break;
      } catch (Exception e) {
        if (tryNum >= numRetriesForUnLock) {
          throw new LockException(e);
        }
      }
    } while (tryNum < numRetriesForUnLock);

    return;
  }

  /* Remove the lock specified */
  @VisibleForTesting
  static void unlockPrimitive(HiveConf conf, ZooKeeper zkpClient,
                             HiveLock hiveLock, String parent) throws LockException {
    ZooKeeperHiveLock zLock = (ZooKeeperHiveLock)hiveLock;
    try {
      // can throw KeeperException.NoNodeException, which might mean something is wrong
      zkpClient.delete(zLock.getPath(), -1);

      // Delete the parent node if all the children have been deleted
      HiveLockObject obj = zLock.getHiveLockObject();
      String name  = getLastObjectName(parent, obj);

      try {
        List<String> children = zkpClient.getChildren(name, false);
        if (children == null || children.isEmpty()) {
          zkpClient.delete(name, -1);
        }
      } catch (KeeperException.NoNodeException e) {
        LOG.debug("Node " + name + " previously deleted when attempting to delete.");
      }
    } catch (Exception e) {
      LOG.error("Failed to release ZooKeeper lock: ", e);
      throw new LockException(e);
    }
  }

  /* Release all locks - including PERSISTENT locks */
  public static void releaseAllLocks(HiveConf conf) throws Exception {
    ZooKeeper zkpClient = null;
    try {
      int sessionTimeout = conf.getIntVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT);
      String quorumServers = getQuorumServers(conf);
      Watcher dummWatcher = new DummyWatcher();
      zkpClient = new ZooKeeper(quorumServers, sessionTimeout, dummWatcher);
      String parent = conf.getVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_NAMESPACE);
      List<HiveLock> locks = getLocks(conf, zkpClient, null, parent, false, false);
      Exception lastExceptionGot = null;
      if (locks != null) {
        for (HiveLock lock : locks) {
          try {
            unlockPrimitive(conf, zkpClient, lock, parent);
          } catch (Exception e) {
            lastExceptionGot = e;
          }
        }
      }

      // if we got exception during doing the unlock, rethrow it here
      if(lastExceptionGot != null) {
        throw lastExceptionGot;
      }
    } catch (Exception e) {
      LOG.error("Failed to release all locks: ", e);
      throw new Exception(ErrorMsg.ZOOKEEPER_CLIENT_COULD_NOT_BE_INITIALIZED.getMsg());
    } finally {
      if (zkpClient != null) {
        zkpClient.close();
        zkpClient = null;
      }
    }
  }

  /* Get all locks */
  public List<HiveLock> getLocks(boolean verifyTablePartition, boolean fetchData)
    throws LockException {
    return getLocks(ctx.getConf(), zooKeeper, null, parent, verifyTablePartition, fetchData);
  }

  /* Get all locks for a particular object */
  public List<HiveLock> getLocks(HiveLockObject key, boolean verifyTablePartitions,
                                 boolean fetchData) throws LockException {
    return getLocks(ctx.getConf(), zooKeeper, key, parent, verifyTablePartitions, fetchData);
  }

  /**
   * @param conf        Hive configuration
   * @param zkpClient   The ZooKeeper client
   * @param key         The object to be compared against - if key is null, then get all locks
   **/
  private static List<HiveLock> getLocks(HiveConf conf, ZooKeeper zkpClient,
      HiveLockObject key, String parent, boolean verifyTablePartition, boolean fetchData)
      throws LockException {
    List<HiveLock> locks = new ArrayList<HiveLock>();
    List<String> children;
    boolean recurse = true;
    String commonParent;

    try {
      if (key != null) {
        commonParent = "/" + parent + "/" + key.getName();
        children = zkpClient.getChildren(commonParent, false);
        recurse = false;
      }
      else {
        commonParent = "/" + parent;
        children = zkpClient.getChildren(commonParent, false);
      }
    } catch (Exception e) {
      // no locks present
      return locks;
    }

    Queue<String> childn = new LinkedList<String>();
    if (children != null && !children.isEmpty()) {
      for (String child : children) {
        childn.add(commonParent + "/" + child);
      }
    }

    while (true) {
      String curChild = childn.poll();
      if (curChild == null) {
        return locks;
      }

      if (recurse) {
        try {
          children = zkpClient.getChildren(curChild, false);
          for (String child : children) {
            childn.add(curChild + "/" + child);
          }
        } catch (Exception e) {
          // nothing to do
        }
      }

      HiveLockMode mode = getLockMode(conf, curChild);
      if (mode == null) {
        continue;
      }

      HiveLockObjectData data = null;
      // set the lock object with a dummy data, and then do a set if needed.
      HiveLockObject obj = getLockObject(conf, curChild, mode, data, parent, verifyTablePartition);
      if (obj == null) {
        continue;
      }

      if ((key == null) ||
          (obj.getName().equals(key.getName()))) {

        if (fetchData) {
          try {
            data = new HiveLockObjectData(new String(zkpClient.getData(curChild, new DummyWatcher(), null)));
            data.setClientIp(clientIp);
          } catch (Exception e) {
            LOG.error("Error in getting data for " + curChild, e);
            // ignore error
          }
        }
        obj.setData(data);
        HiveLock lck = (HiveLock)(new ZooKeeperHiveLock(curChild, obj, mode));
        locks.add(lck);
      }
    }
  }

  /** Remove all redundant nodes **/
  private void removeAllRedundantNodes() {
    try {
      renewZookeeperInstance(sessionTimeout, quorumServers);
      checkRedundantNode("/" + parent);
      if (zooKeeper != null) {
        zooKeeper.close();
        zooKeeper = null;
      }
    } catch (Exception e) {
      LOG.warn("Exception while removing all redundant nodes", e);
    }
  }

  private void checkRedundantNode(String node) {
    try {
      // Nothing to do if it is a lock mode
      if (getLockMode(ctx.getConf(), node) != null) {
        return;
      }

      List<String> children = zooKeeper.getChildren(node, false);
      for (String child : children) {
        checkRedundantNode(node + "/" + child);
      }

      children = zooKeeper.getChildren(node, false);
      if ((children == null) || (children.isEmpty()))
      {
        zooKeeper.delete(node, -1);
      }
    } catch (Exception e) {
      LOG.warn("Error in checkRedundantNode for node " + node, e);
    }
  }

  /* Release all transient locks, by simply closing the client */
  public void close() throws LockException {
    try {

      if (zooKeeper != null) {
        zooKeeper.close();
        zooKeeper = null;
      }

      if (HiveConf.getBoolVar(ctx.getConf(), HiveConf.ConfVars.HIVE_ZOOKEEPER_CLEAN_EXTRA_NODES)) {
        removeAllRedundantNodes();
      }

    } catch (Exception e) {
      LOG.error("Failed to close zooKeeper client: " + e);
      throw new LockException(e);
    }
  }

  /**
   * Get the sequence number from the path. The sequence number is always at the end of the path.
   **/
  private int getSequenceNumber(String resPath, String path) {
    String tst = resPath.substring(path.length());
    try {
      return (new Integer(tst)).intValue();
    } catch (Exception e) {
      return -1; // invalid number
    }
  }

  /**
   * Get the object from the path of the lock.
   * The object may correspond to a table, a partition or a parent to a partition.
   * For eg: if Table T is partitioned by ds, hr and ds=1/hr=1 is a valid partition,
   * the lock may also correspond to T@ds=1, which is not a valid object
   * @param verifyTablePartition
   **/
  private static HiveLockObject getLockObject(HiveConf conf, String path,
    HiveLockMode mode, HiveLockObjectData data,
    String parent, boolean verifyTablePartition)
      throws LockException {
    try {
      Hive db = Hive.get(conf);
      int indx = path.lastIndexOf("LOCK-" + mode.toString());
      String objName = path.substring(("/" + parent + "/").length(), indx-1);
      String[] names = objName.split("/");

      if (names.length < 2) {
        return null;
      }

      if (!verifyTablePartition) {
        return new HiveLockObject(names, data);
      }

      // do not throw exception if table does not exist
      Table tab = db.getTable(names[0], names[1], false);
      if (tab == null) {
        return null;
      }

      if (names.length == 2) {
        return new HiveLockObject(tab, data);
      }

      Map<String, String> partSpec = new HashMap<String, String>();
      for (indx = 2; indx < names.length; indx++) {
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
        return new HiveLockObject(new DummyPartition(tab, path, partSpec), data);
      }

      return new HiveLockObject(partn, data);
    } catch (Exception e) {
      LOG.error("Failed to create ZooKeeper object: " + e);
      throw new LockException(e);
    }
  }

  private static Pattern shMode = Pattern.compile("^.*-(SHARED)-([0-9]+)$");
  private static Pattern exMode = Pattern.compile("^.*-(EXCLUSIVE)-([0-9]+)$");

  /* Get the mode of the lock encoded in the path */
  private static HiveLockMode getLockMode(HiveConf conf, String path) {

    Matcher shMatcher = shMode.matcher(path);
    Matcher exMatcher = exMode.matcher(path);

    if (shMatcher.matches()) {
      return HiveLockMode.SHARED;
    }

    if (exMatcher.matches()) {
      return HiveLockMode.EXCLUSIVE;
    }

    return null;
  }

  public static class DummyWatcher implements Watcher {
    public void process(org.apache.zookeeper.WatchedEvent event)  {
    }
  }

  @Override
  public void prepareRetry() throws LockException {
    try {
      if (zooKeeper != null && zooKeeper.getState() == ZooKeeper.States.CLOSED) {
        // Reconnect if the connection is closed.
        zooKeeper = null;
      }
      renewZookeeperInstance(sessionTimeout, quorumServers);
    } catch (Exception e) {
      throw new LockException(e);
    }
  }

}
