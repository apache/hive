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
import java.util.Collections;
import java.util.List;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.ZooKeeper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This does periodic cleanup
 */
public class ZooKeeperCleanup extends Thread {
  protected Configuration appConf;

  // The interval to wake up and check the queue
  public static final String ZK_CLEANUP_INTERVAL =
    "templeton.zookeeper.cleanup.interval"; // 12 hours

  // The max age of a task allowed
  public static final String ZK_CLEANUP_MAX_AGE =
    "templeton.zookeeper.cleanup.maxage"; // ~ 1 week

  protected static long interval = 1000L * 60L * 60L * 12L;
  protected static long maxage = 1000L * 60L * 60L * 24L * 7L;

  // The logger
  private static final Log LOG = LogFactory.getLog(ZooKeeperCleanup.class);

  // Handle to cancel loop
  private boolean stop = false;

  // The instance
  private static ZooKeeperCleanup thisclass = null;

  // Whether the cycle is running
  private static boolean isRunning = false;

  /**
   * Create a cleanup object.  We use the appConfig to configure JobState.
   * @param appConf
   */
  private ZooKeeperCleanup(Configuration appConf) {
    this.appConf = appConf;
    interval = appConf.getLong(ZK_CLEANUP_INTERVAL, interval);
    maxage = appConf.getLong(ZK_CLEANUP_MAX_AGE, maxage);
  }

  public static ZooKeeperCleanup getInstance(Configuration appConf) {
    if (thisclass != null) {
      return thisclass;
    }
    thisclass = new ZooKeeperCleanup(appConf);
    return thisclass;
  }

  public static void startInstance(Configuration appConf) throws IOException {
    if (!isRunning) {
      getInstance(appConf).start();
    }
  }

  /**
   * Run the cleanup loop.
   *
   * @throws IOException
   */
  public void run() {
    ZooKeeper zk = null;
    List<String> nodes = null;
    isRunning = true;
    while (!stop) {
      try {
        // Put each check in a separate try/catch, so if that particular
        // cycle fails, it'll try again on the next cycle.
        try {
          zk = ZooKeeperStorage.zkOpen(appConf);

          nodes = getChildList(zk);

          for (String node : nodes) {
            boolean deleted = checkAndDelete(node, zk);
            if (!deleted) {
              break;
            }
          }

          zk.close();
        } catch (Exception e) {
          LOG.error("Cleanup cycle failed: " + e.getMessage());
        } finally {
          if (zk != null) {
            try {
              zk.close();
            } catch (InterruptedException e) {
              // We're trying to exit anyway, just ignore.
            }
          }
        }

        long sleepMillis = (long) (Math.random() * interval);
        LOG.info("Next execution: " + new Date(new Date().getTime()
          + sleepMillis));
        Thread.sleep(sleepMillis);

      } catch (Exception e) {
        // If sleep fails, we should exit now before things get worse.
        isRunning = false;
        LOG.error("Cleanup failed: " + e.getMessage(), e);
      }
    }
    isRunning = false;
  }

  /**
   * Get the list of jobs from JobState
   *
   * @throws IOException
   */
  public List<String> getChildList(ZooKeeper zk) {
    try {
      List<String> jobs = JobStateTracker.getTrackingJobs(appConf, zk);
      Collections.sort(jobs);
      return jobs;
    } catch (IOException e) {
      LOG.info("No jobs to check.");
    }
    return new ArrayList<String>();
  }

  /**
   * Check to see if a job is more than maxage old, and delete it if so.
   */
  public boolean checkAndDelete(String node, ZooKeeper zk) {
    JobState state = null;
    try {
      JobStateTracker tracker = new JobStateTracker(node, zk, true,
        appConf.get(TempletonStorage.STORAGE_ROOT +
          ZooKeeperStorage.TRACKINGDIR));
      long now = new Date().getTime();
      state = new JobState(tracker.getJobID(), appConf);

      // Set the default to 0 -- if the created date is null, there was
      // an error in creation, and we want to delete it anyway.
      long then = 0;
      if (state.getCreated() != null) {
        then = state.getCreated();
      }
      if (now - then > maxage) {
        LOG.info("Deleting " + tracker.getJobID());
        state.delete();
        tracker.delete();
        return true;
      }
      return false;
    } catch (Exception e) {
      LOG.info("checkAndDelete failed for " + node);
      // We don't throw a new exception for this -- just keep going with the
      // next one.
      return true;
    } finally {
      if (state != null) {
        try {
          state.close();
        } catch (IOException e) {
          LOG.info("Couldn't close job state.");
        }
      }
    }
  }

  // Handle to stop this process from the outside if needed.
  public void exit() {
    stop = true;
  }
}
