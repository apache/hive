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
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hive.hcatalog.templeton.tool.TempletonStorage.Type;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This does periodic cleanup
 */
public class HDFSCleanup extends Thread {
  protected Configuration appConf;

  // The interval to wake up and check the queue
  public static final String HDFS_CLEANUP_INTERVAL =
    "templeton.hdfs.cleanup.interval"; // 12 hours

  // The max age of a task allowed
  public static final String HDFS_CLEANUP_MAX_AGE =
    "templeton.hdfs.cleanup.maxage"; // ~ 1 week

  protected static long interval = 1000L * 60L * 60L * 12L;
  protected static long maxage = 1000L * 60L * 60L * 24L * 7L;

  // The logger
  private static final Log LOG = LogFactory.getLog(HDFSCleanup.class);

  // Handle to cancel loop
  private boolean stop = false;

  // The instance
  private static HDFSCleanup thisclass = null;

  // Whether the cycle is running
  private static boolean isRunning = false;

  // The storage root
  private String storage_root;

  /**
   * Create a cleanup object. 
   */
  private HDFSCleanup(Configuration appConf) {
    this.appConf = appConf;
    interval = appConf.getLong(HDFS_CLEANUP_INTERVAL, interval);
    maxage = appConf.getLong(HDFS_CLEANUP_MAX_AGE, maxage);
    storage_root = appConf.get(TempletonStorage.STORAGE_ROOT);
  }

  public static HDFSCleanup getInstance(Configuration appConf) {
    if (thisclass != null) {
      return thisclass;
    }
    thisclass = new HDFSCleanup(appConf);
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
   */
  public void run() {
    FileSystem fs = null;
    while (!stop) {
      try {
        // Put each check in a separate try/catch, so if that particular
        // cycle fails, it'll try again on the next cycle.
        try {
          if (fs == null) {
            fs = new Path(storage_root).getFileSystem(appConf);
          }
          checkFiles(fs);
        } catch (Exception e) {
          LOG.error("Cleanup cycle failed: " + e.getMessage());
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
   * Loop through all the files, deleting any that are older than
   * maxage.
   * 
   * @param fs
   * @throws IOException
   */
  private void checkFiles(FileSystem fs) throws IOException {
    long now = new Date().getTime();
    for (Type type : Type.values()) {
      try {
        for (FileStatus status : fs.listStatus(new Path(
            HDFSStorage.getPath(type, storage_root)))) {
          if (now - status.getModificationTime() > maxage) {
            LOG.info("Deleting " + status.getPath().toString());
            fs.delete(status.getPath(), true);
          }
        }
      } catch (Exception e) {
        // Nothing to find for this type.
      }
    }
  }

  // Handle to stop this process from the outside if needed.
  public void exit() {
    stop = true;
  }

}
