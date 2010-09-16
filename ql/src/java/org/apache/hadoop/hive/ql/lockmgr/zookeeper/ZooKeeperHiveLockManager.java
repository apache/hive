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

import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.zookeeper.KeeperException;

import org.apache.hadoop.hive.ql.parse.ErrorMsg;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManager;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockManagerCtx;
import org.apache.hadoop.hive.ql.lockmgr.HiveLock;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockObject;
import org.apache.hadoop.hive.ql.lockmgr.HiveLockMode;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.DummyPartition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;

public class ZooKeeperHiveLockManager implements HiveLockManager {
  HiveLockManagerCtx ctx;
  public static final Log LOG = LogFactory.getLog("ZooKeeperHiveLockManager");
  static final private LogHelper console = new LogHelper(LOG);

  private ZooKeeper zooKeeper;

  // All the locks are created under this parent
  private String    parent;

  public ZooKeeperHiveLockManager() {
  }

  /**
   * @param conf  The hive configuration
   * Get the quorum server address from the configuration. The format is:
   * host1:port, host2:port..
   **/
  private static String getQuorumServers(HiveConf conf) {
    String hosts = conf.getVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_QUORUM);
    String port = conf.getVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_CLIENT_PORT);
    return hosts + ":" + port;
  }

  /**
   * @param ctx  The lock manager context (containing the Hive configuration file)
   * Start the ZooKeeper client based on the zookeeper cluster specified in the conf.
   **/
  public void setContext(HiveLockManagerCtx ctx) throws LockException {
    this.ctx = ctx;
    HiveConf conf = ctx.getConf();
    int sessionTimeout = conf.getIntVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT);
    String quorumServers = ZooKeeperHiveLockManager.getQuorumServers(conf);

    try {
      if (zooKeeper != null) {
        zooKeeper.close();
      }

      zooKeeper = new ZooKeeper(quorumServers, sessionTimeout, new DummyWatcher());
      parent = conf.getVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_NAMESPACE);

      try {
        String par = zooKeeper.create("/" +  parent, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } catch (KeeperException e) {
        // ignore if the parent already exists
      }


    } catch (Exception e) {
      LOG.error("Failed to create ZooKeeper object: " + e);
      throw new LockException(ErrorMsg.ZOOKEEPER_CLIENT_COULD_NOT_BE_INITIALIZED.getMsg());
    }
  }

  /**
   * Since partition names can contain "/", which need all the parent directories to be created by ZooKeeper,
   * replace "/" by a dummy name to ensure a single hierarchy.
   **/
  private String getObjectName(HiveLockObject key, HiveLockMode mode) {
    return "/" + parent + "/" +
      key.getName().replaceAll("/", ctx.getConf().getVar(HiveConf.ConfVars.DEFAULT_ZOOKEEPER_PARTITION_NAME)) +
      "-" + mode + "-";
  }

  /**
   * @param  key          The object to be locked
   * @param  mode         The mode of the lock
   * @param  keepAlive    Whether the lock is to be persisted after the statement
   * Acuire the lock. Return null if a conflicting lock is present.
   **/
  public ZooKeeperHiveLock lock(HiveLockObject key, HiveLockMode mode, boolean keepAlive)
    throws LockException {
    String name = getObjectName(key, mode);
    String res;

    try {
      if (keepAlive) {
        res = zooKeeper.create(name, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
      }
      else {
        res = zooKeeper.create(name, new byte[0], Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
      }

      int seqNo = getSequenceNumber(res, name);
      if (seqNo == -1) {
        zooKeeper.delete(res, -1);
        return null;
      }

      List<String> children = zooKeeper.getChildren("/" + parent, false);

      String exLock = getObjectName(key, HiveLockMode.EXCLUSIVE);
      String shLock = getObjectName(key, HiveLockMode.SHARED);

      for (String child : children) {
        child = "/" + parent + "/" + child;

        // Is there a conflicting lock on the same object with a lower sequence number
        int childSeq = seqNo;
        if (child.startsWith(exLock)) {
          childSeq = getSequenceNumber(child, exLock);
        }
        if ((mode == HiveLockMode.EXCLUSIVE) && child.startsWith(shLock)) {
          childSeq = getSequenceNumber(child, shLock);
        }

        if ((childSeq >= 0) && (childSeq < seqNo)) {
          zooKeeper.delete(res, -1);
          console.printError("conflicting lock present ");
          return null;
        }
      }

    } catch (Exception e) {
      LOG.error("Failed to get ZooKeeper lock: " + e);
      throw new LockException(e);
    }

    return new ZooKeeperHiveLock(res, key, mode);
  }

  /* Remove the lock specified */
  public void unlock(HiveLock hiveLock) throws LockException {
    unlock(ctx.getConf(), zooKeeper, hiveLock);
  }

  /* Remove the lock specified */
  private static void unlock(HiveConf conf, ZooKeeper zkpClient, HiveLock hiveLock) throws LockException {
    ZooKeeperHiveLock zLock = (ZooKeeperHiveLock)hiveLock;
    try {
      zkpClient.delete(zLock.getPath(), -1);
    } catch (Exception e) {
      LOG.error("Failed to release ZooKeeper lock: " + e);
      throw new LockException(e);
    }
  }

  /* Release all locks - including PERSISTENT locks */
  public static void releaseAllLocks(HiveConf conf) throws Exception {
    try {
      int sessionTimeout = conf.getIntVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_SESSION_TIMEOUT);
      String quorumServers = getQuorumServers(conf);
      ZooKeeper zkpClient = new ZooKeeper(quorumServers, sessionTimeout, new DummyWatcher());
      String parent = conf.getVar(HiveConf.ConfVars.HIVE_ZOOKEEPER_NAMESPACE);
      List<HiveLock> locks = getLocks(conf, zkpClient, null, parent);

      if (locks != null) {
        for (HiveLock lock : locks) {
          unlock(conf, zkpClient, lock);
        }
      }

      zkpClient.close();
      zkpClient = null;
    } catch (Exception e) {
      LOG.error("Failed to release all locks: " + e.getMessage());
      throw new Exception(ErrorMsg.ZOOKEEPER_CLIENT_COULD_NOT_BE_INITIALIZED.getMsg());
    }
  }

  /* Get all locks */
  public List<HiveLock> getLocks() throws LockException {
    return getLocks(ctx.getConf(), zooKeeper, null, parent);
  }

  /* Get all locks for a particular object */
  public List<HiveLock> getLocks(HiveLockObject key) throws LockException {
    return getLocks(ctx.getConf(), zooKeeper, key, parent);
  }

  /**
   * @param conf        Hive configuration
   * @param zkpClient   The ZooKeeper client
   * @param key         The object to be compared against - if key is null, then get all locks
   **/
  private static List<HiveLock> getLocks(HiveConf conf, ZooKeeper zkpClient,
                                         HiveLockObject key, String parent) throws LockException {
    List<HiveLock> locks = new ArrayList<HiveLock>();
    List<String> children;

    try {
      children = zkpClient.getChildren("/" + parent, false);
    } catch (Exception e) {
      LOG.error("Failed to get ZooKeeper children: " + e);
      throw new LockException(e);
    }

    for (String child : children) {
      child = "/" + parent + "/" + child;

      HiveLockMode   mode  = getLockMode(conf, child);
      if (mode == null) {
        continue;
      }

      HiveLockObject obj   = getLockObject(conf, child, mode);
      if ((key == null) ||
          (obj.getName().equals(key.getName()))) {
        HiveLock lck = (HiveLock)(new ZooKeeperHiveLock(child, obj, mode));
        locks.add(lck);
      }
    }

    return locks;
  }

  /* Release all transient locks, by simply closing the client */
  public void close() throws LockException {
    try {
      if (zooKeeper != null) {
        zooKeeper.close();
        zooKeeper = null;
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
   **/
  private static HiveLockObject getLockObject(HiveConf conf, String path, HiveLockMode mode) throws LockException {
    try {
      Hive db = Hive.get(conf);
      int indx = path.lastIndexOf(mode.toString());
      String objName = path.substring(1, indx-1);
      String[] names = objName.split("/")[1].split("@");

      Table tab = db.getTable(MetaStoreUtils.DEFAULT_DATABASE_NAME,
                              names[1], false); // do not throw exception if table does not exist
      assert (tab != null);

      if (names.length == 2) {
        return new HiveLockObject(tab);
      }

      String[] parts = names[2].split(conf.getVar(HiveConf.ConfVars.DEFAULT_ZOOKEEPER_PARTITION_NAME));

      Map<String, String> partSpec = new HashMap<String, String>();
      for (indx = 0; indx < parts.length; indx++) {
        String[] partVals = parts[indx].split("=");
        partSpec.put(partVals[0], partVals[1]);
      }

      Partition partn;
      try {
        partn = db.getPartition(tab, partSpec, false);
      } catch (HiveException e) {
        partn =null;
      }

      if (partn == null) {
        return new HiveLockObject(new DummyPartition(
          objName.split("/")[1].replaceAll(conf.getVar(HiveConf.ConfVars.DEFAULT_ZOOKEEPER_PARTITION_NAME), "/")));
      }

      return new HiveLockObject(partn);
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

    if (shMatcher.matches())
      return HiveLockMode.SHARED;

    if (exMatcher.matches()) {
      return HiveLockMode.EXCLUSIVE;
    }

    return null;
  }

  public static class DummyWatcher implements Watcher {
    public void process(org.apache.zookeeper.WatchedEvent event)  {
    }
  }
}
