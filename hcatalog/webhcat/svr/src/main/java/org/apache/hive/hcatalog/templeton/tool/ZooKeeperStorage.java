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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton.tool;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;

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

  private static final Log LOG = LogFactory.getLog(ZooKeeperStorage.class);

  private ZooKeeper zk;

  /**
   * Open a ZooKeeper connection for the JobState.
   */
  public static ZooKeeper zkOpen(String zkHosts, int zkSessionTimeout)
    throws IOException {
    return new ZooKeeper(zkHosts,
      zkSessionTimeout,
      new Watcher() {
        @Override
        synchronized public void process(WatchedEvent event) {
        }
      });
  }

  /**
   * Open a ZooKeeper connection for the JobState.
   */
  public static ZooKeeper zkOpen(Configuration conf)
    throws IOException {
    return zkOpen(conf.get(ZK_HOSTS),
      conf.getInt(ZK_SESSION_TIMEOUT, 30000));
  }

  public ZooKeeperStorage() {
    // No-op -- this is needed to be able to instantiate the
    // class from the name.
  }

  /**
   * Close this ZK connection.
   */
  public void close()
    throws IOException {
    if (zk != null) {
      try {
        zk.close();
        zk = null;
      } catch (InterruptedException e) {
        throw new IOException("Closing ZooKeeper connection", e);
      }
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
    try {
      String[] paths = getPaths(makeZnode(type, id));
      boolean wasCreated = false;
      for (String znode : paths) {
        try {
          zk.create(znode, new byte[0],
            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
          wasCreated = true;
        } catch (KeeperException.NodeExistsException e) {
        }
      }
      if (wasCreated) {
        try {
          // Really not sure if this should go here.  Will have
          // to see how the storage mechanism evolves.
          if (type.equals(Type.JOB)) {
            JobStateTracker jt = new JobStateTracker(id, zk, false,
              job_trackingpath);
            jt.create();
          }
        } catch (Exception e) {
          LOG.warn("Error tracking: " + e.getMessage());
          // If we couldn't create the tracker node, don't
          // create the main node.
          zk.delete(makeZnode(type, id), -1);
        }
      }
      if (zk.exists(makeZnode(type, id), false) == null)
        throw new IOException("Unable to create " + makeZnode(type, id));
      if (wasCreated) {
        try {
          saveField(type, id, "created",
            Long.toString(System.currentTimeMillis()));
        } catch (NotFoundException nfe) {
          // Wow, something's really wrong.
          throw new IOException("Couldn't write to node " + id, nfe);
        }
      }
    } catch (KeeperException e) {
      throw new IOException("Creating " + id, e);
    } catch (InterruptedException e) {
      throw new IOException("Creating " + id, e);
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
   * @param type
   * @param id
   * @param name
   * @param val
   * @throws KeeperException
   * @throws UnsupportedEncodingException
   * @throws InterruptedException
   */
  private void setFieldData(Type type, String id, String name, String val)
    throws KeeperException, UnsupportedEncodingException, InterruptedException {
    try {
      zk.create(makeFieldZnode(type, id, name),
        val.getBytes(ENCODING),
        Ids.OPEN_ACL_UNSAFE,
        CreateMode.PERSISTENT);
    } catch (KeeperException.NodeExistsException e) {
      zk.setData(makeFieldZnode(type, id, name),
        val.getBytes(ENCODING),
        -1);
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
      byte[] b = zk.getData(makeFieldZnode(type, id, key), false, null);
      return new String(b, ENCODING);
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public Map<String, String> getFields(Type type, String id) {
    HashMap<String, String> map = new HashMap<String, String>();
    try {
      for (String node : zk.getChildren(makeZnode(type, id), false)) {
        byte[] b = zk.getData(makeFieldZnode(type, id, node),
          false, null);
        map.put(node, new String(b, ENCODING));
      }
    } catch (Exception e) {
      return map;
    }
    return map;
  }

  @Override
  public boolean delete(Type type, String id) throws NotFoundException {
    try {
      for (String child : zk.getChildren(makeZnode(type, id), false)) {
        try {
          zk.delete(makeFieldZnode(type, id, child), -1);
        } catch (Exception e) {
          // Other nodes may be trying to delete this at the same time,
          // so just log errors and skip them.
          throw new NotFoundException("Couldn't delete " +
            makeFieldZnode(type, id, child));
        }
      }
      try {
        zk.delete(makeZnode(type, id), -1);
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
  public List<String> getAll() {
    ArrayList<String> allNodes = new ArrayList<String>();
    for (Type type : Type.values()) {
      allNodes.addAll(getAllForType(type));
    }
    return allNodes;
  }

  @Override
  public List<String> getAllForType(Type type) {
    try {
      return zk.getChildren(getPath(type), false);
    } catch (Exception e) {
      return new ArrayList<String>();
    }
  }

  @Override
  public List<String> getAllForKey(String key, String value) {
    ArrayList<String> allNodes = new ArrayList<String>();
    try {
      for (Type type : Type.values()) {
        allNodes.addAll(getAllForTypeAndKey(type, key, value));
      }
    } catch (Exception e) {
      LOG.info("Couldn't find children.");
    }
    return allNodes;
  }

  @Override
  public List<String> getAllForTypeAndKey(Type type, String key, String value) {
    ArrayList<String> allNodes = new ArrayList<String>();
    try {
      for (String id : zk.getChildren(getPath(type), false)) {
        for (String field : zk.getChildren(id, false)) {
          if (field.endsWith("/" + key)) {
            byte[] b = zk.getData(field, false, null);
            if (new String(b, ENCODING).equals(value)) {
              allNodes.add(id);
            }
          }
        }
      }
    } catch (Exception e) {
      // Log and go to the next type -- this one might not exist
      LOG.info("Couldn't find children of " + getPath(type));
    }
    return allNodes;
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
