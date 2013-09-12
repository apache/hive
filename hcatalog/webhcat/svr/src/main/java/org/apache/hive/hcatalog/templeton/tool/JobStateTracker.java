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
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;

public class JobStateTracker {
  // The path to the tracking root
  private String job_trackingroot = null;

  // The zookeeper connection to use
  private ZooKeeper zk;

  // The id of the tracking node -- must be a SEQUENTIAL node
  private String trackingnode;

  // The id of the job this tracking node represents
  private String jobid;

  // The logger
  private static final Log LOG = LogFactory.getLog(JobStateTracker.class);

  /**
   * Constructor for a new node -- takes the jobid of an existing job
   *
   */
  public JobStateTracker(String node, ZooKeeper zk, boolean nodeIsTracker,
               String job_trackingpath) {
    this.zk = zk;
    if (nodeIsTracker) {
      trackingnode = node;
    } else {
      jobid = node;
    }
    job_trackingroot = job_trackingpath;
  }

  /**
   * Create the parent znode for this job state.
   */
  public void create()
    throws IOException {
    String[] paths = ZooKeeperStorage.getPaths(job_trackingroot);
    for (String znode : paths) {
      try {
        zk.create(znode, new byte[0],
          Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
      } catch (KeeperException.NodeExistsException e) {
      } catch (Exception e) {
        throw new IOException("Unable to create parent nodes");
      }
    }
    try {
      trackingnode = zk.create(makeTrackingZnode(), jobid.getBytes(),
        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
    } catch (Exception e) {
      throw new IOException("Unable to create " + makeTrackingZnode());
    }
  }

  public void delete()
    throws IOException {
    try {
      zk.delete(makeTrackingJobZnode(trackingnode), -1);
    } catch (Exception e) {
      // Might have been deleted already
      LOG.info("Couldn't delete " + makeTrackingJobZnode(trackingnode));
    }
  }

  /**
   * Get the jobid for this tracking node
   * @throws IOException
   */
  public String getJobID() throws IOException {
    try {
      return new String(zk.getData(makeTrackingJobZnode(trackingnode),
        false, new Stat()));
    } catch (KeeperException e) {
      // It was deleted during the transaction
      throw new IOException("Node already deleted " + trackingnode);
    } catch (InterruptedException e) {
      throw new IOException("Couldn't read node " + trackingnode);
    }
  }

  /**
   * Make a ZK path to a new tracking node
   */
  public String makeTrackingZnode() {
    return job_trackingroot + "/";
  }

  /**
   * Make a ZK path to an existing tracking node
   */
  public String makeTrackingJobZnode(String nodename) {
    return job_trackingroot + "/" + nodename;
  }

  /*
   * Get the list of tracking jobs.  These can be used to determine which jobs have
   * expired.
   */
  public static List<String> getTrackingJobs(Configuration conf, ZooKeeper zk)
    throws IOException {
    ArrayList<String> jobs = new ArrayList<String>();
    try {
      for (String myid : zk.getChildren(
        conf.get(TempletonStorage.STORAGE_ROOT)
          + ZooKeeperStorage.TRACKINGDIR, false)) {
        jobs.add(myid);
      }
    } catch (Exception e) {
      throw new IOException("Can't get tracking children", e);
    }
    return jobs;
  }
}
