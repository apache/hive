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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton.tool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs.Ids;

/**
 * A storage implementation based on storing everything in ZooKeeper.
 * This keeps everything in a central location that is guaranteed
 * to be available and accessible.
 *
 * Data is stored with each key/value pair being a node in ZooKeeper.
 */
public class ZooKeeperStorage implements TempletonStorage {

  public static final String TRACKINGDIR = "/created";

  // Locations for each of the storage types
  public String storage_root = null;
  public String job_path = null;
  public String job_trackingpath = null;
  public String overhead_path = null;

  public static final String ZK_HOSTS = "templeton.zookeeper.hosts";
  public static final String ZK_SESSION_TIMEOUT
    = "templeton.zookeeper.session-timeout";

  public static final String ENCODING = "UTF-8";

  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperStorage.class);

  private CuratorFramework zk;

  /**
   * Open a ZooKeeper connection for the JobState.
   */
  public static CuratorFramework zkOpen(String zkHosts, int zkSessionTimeoutMs)
    throws IOException {
    //do we need to add a connection status listener?  What will that do?
    ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);
    CuratorFramework zk = CuratorFrameworkFactory.newClient(zkHosts, zkSessionTimeoutMs,
      CuratorFrameworkFactory.builder().getConnectionTimeoutMs(), retryPolicy);
    zk.start();
    return zk;
  }

  /**
   * Open a ZooKeeper connection for the JobState.
   */
  public static CuratorFramework zkOpen(Configuration conf) throws IOException {
    /*the silly looking call to Builder below is to get the default value of session timeout
    from Curator which itself exposes it as system property*/
    return zkOpen(conf.get(ZK_HOSTS),
      conf.getInt(ZK_SESSION_TIMEOUT, CuratorFrameworkFactory.builder().getSessionTimeoutMs()));
  }

  public ZooKeeperStorage() {
    // No-op -- this is needed to be able to instantiate the
    // class from the name.
  }

  /**
   * Close this ZK connection.
   */
  public void close() throws IOException {
    if (zk != null) {
      zk.close();
    }
  }

  public void startCleanup(Configuration config) {
    try {
      ZooKeeperCleanup.startInstance(config);
    } catch (Exception e) {
      LOG.warn("Cleanup instance didn't start.");
    }
  }

  /**
   * Create a node in ZooKeeper
   */
  public void create(Type type, String id)
    throws IOException {
    boolean wasCreated = false;
    try {
      zk.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(Ids.OPEN_ACL_UNSAFE).forPath(makeZnode(type, id));
      wasCreated = true;
    }
    catch(KeeperException.NodeExistsException ex) {
      //we just created top level node for this jobId
    }
    catch(Exception ex) {
      throw new IOException("Error creating " + makeZnode(type, id), ex);
    }
    if(wasCreated) {
      try {
        // Really not sure if this should go here.  Will have
        // to see how the storage mechanism evolves.
        if (type.equals(Type.JOB)) {
          JobStateTracker jt = new JobStateTracker(id, zk, false, job_trackingpath);
          jt.create();
        }
      } catch (Exception e) {
        LOG.error("Error tracking (jobId=" + id + "): " + e.getMessage());
        // If we couldn't create the tracker node, don't create the main node.
        try {
          zk.delete().forPath(makeZnode(type, id));//default version is -1
        }
        catch(Exception ex) {
          //EK: it's not obvious that this is the right logic, if we don't record the 'callback'
          //for example and never notify the client of job completion
          throw new IOException("Failed to delete " + makeZnode(type, id) + ":" + ex);
        }
      }
    }
    try {
      if (zk.checkExists().forPath(makeZnode(type, id)) == null) {
        throw new IOException("Unable to create " + makeZnode(type, id));
      }
    }
    catch (Exception ex) {
      throw new IOException(ex);
    }
    if (wasCreated) {
      try {
        saveField(type, id, "created",
          Long.toString(System.currentTimeMillis()));
      } catch (NotFoundException nfe) {
        // Wow, something's really wrong.
        throw new IOException("Couldn't write to node " + id, nfe);
      }
    }
  }

  /**
   * Get the path based on the job type.
   *
   * @param type
   */
  public String getPath(Type type) {
    String typepath = overhead_path;
    switch (type) {
    case JOB:
      typepath = job_path;
      break;
    case JOBTRACKING:
      typepath = job_trackingpath;
      break;
    }
    return typepath;
  }

  public static String[] getPaths(String fullpath) {
    ArrayList<String> paths = new ArrayList<String>();
    if (fullpath.length() < 2) {
      paths.add(fullpath);
    } else {
      int location = 0;
      while ((location = fullpath.indexOf("/", location + 1)) > 0) {
        paths.add(fullpath.substring(0, location));
      }
      paths.add(fullpath);
    }
    String[] strings = new String[paths.size()];
    return paths.toArray(strings);
  }

  /**
   * A helper method that sets a field value.
   * @throws java.lang.Exception
   */
  private void setFieldData(Type type, String id, String name, String val) throws Exception {
    try {
      zk.create().withMode(CreateMode.PERSISTENT).withACL(Ids.OPEN_ACL_UNSAFE)
        .forPath(makeFieldZnode(type, id, name), val.getBytes(ENCODING));
    } catch (KeeperException.NodeExistsException e) {
      zk.setData().forPath(makeFieldZnode(type, id, name), val.getBytes(ENCODING));
    }
  }

  /**
   * Make a ZK path to the named field.
   */
  public String makeFieldZnode(Type type, String id, String name) {
    return makeZnode(type, id) + "/" + name;
  }

  /**
   * Make a ZK path to job
   */
  public String makeZnode(Type type, String id) {
    return getPath(type) + "/" + id;
  }

  @Override
  public void saveField(Type type, String id, String key, String val)
    throws NotFoundException {
    try {
      if (val != null) {
        create(type, id);
        setFieldData(type, id, key, val);
      }
    } catch (Exception e) {
      throw new NotFoundException("Writing " + key + ": " + val + ", "
        + e.getMessage());
    }
  }

  @Override
  public String getField(Type type, String id, String key) {
    try {
      byte[] b = zk.getData().forPath(makeFieldZnode(type, id, key));
      return new String(b, ENCODING);
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public boolean delete(Type type, String id) throws NotFoundException {
    try {
      
      for (String child : zk.getChildren().forPath(makeZnode(type, id))) {
        try {
          zk.delete().forPath(makeFieldZnode(type, id, child));
        } catch (Exception e) {
          // Other nodes may be trying to delete this at the same time,
          // so just log errors and skip them.
          throw new NotFoundException("Couldn't delete " +
            makeFieldZnode(type, id, child));
        }
      }
      try {
        zk.delete().forPath(makeZnode(type, id));
      } catch (Exception e) {
        // Same thing -- might be deleted by other nodes, so just go on.
        throw new NotFoundException("Couldn't delete " +
          makeZnode(type, id));
      }
    } catch (Exception e) {
      // Error getting children of node -- probably node has been deleted
      throw new NotFoundException("Couldn't get children of " +
        makeZnode(type, id));
    }
    return true;
  }

  @Override
  public List<String> getAllForType(Type type) {
    try {
      return zk.getChildren().forPath(getPath(type));
    } catch (Exception e) {
      return new ArrayList<String>();
    }
  }

  @Override
  public void openStorage(Configuration config) throws IOException {
    storage_root = config.get(STORAGE_ROOT);
    job_path = storage_root + "/jobs";
    job_trackingpath = storage_root + TRACKINGDIR;
    overhead_path = storage_root + "/overhead";

    if (zk == null) {
      zk = zkOpen(config);
    }
  }

  @Override
  public void closeStorage() throws IOException {
    close();
  }
}
