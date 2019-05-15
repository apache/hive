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
package org.apache.hadoop.hive.metastore;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.util.SystemClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class HiveProtoEventsCleanerTask implements MetastoreTaskThread {
  public static final Logger LOG = LoggerFactory.getLogger(HiveProtoEventsCleanerTask.class);

  private final String[] eventsSubDirs = new String[] { "query_data", "dag_meta", "dag_data", "app_data" };
  private List<Path> eventsBasePaths = new ArrayList<>();
  private Configuration conf;
  private long ttl;
  private static String expiredDatePtn = null;
  private static final SystemClock clock = SystemClock.getInstance();

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;

    String hiveEventsDir = HiveConf.getVar(conf, ConfVars.HIVE_PROTO_EVENTS_BASE_PATH);
    if (StringUtils.isBlank(hiveEventsDir)) {
      return;
    }
    Path hiveEventsBasePath = new Path(hiveEventsDir);
    Path baseDir = hiveEventsBasePath.getParent();
    for (String subDir : eventsSubDirs) {
      eventsBasePaths.add(new Path(baseDir, subDir));
    }
    assert(eventsBasePaths.get(0).equals(hiveEventsBasePath));
    ttl = HiveConf.getTimeVar(conf, ConfVars.HIVE_PROTO_EVENTS_TTL, TimeUnit.MILLISECONDS);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public long runFrequency(TimeUnit unit) {
    return HiveConf.getTimeVar(conf, ConfVars.HIVE_PROTO_EVENTS_CLEAN_FREQ, unit);
  }

  @Override
  public void run() {
    // If Hive proto logging is not enabled, then nothing to be cleaned-up.
    if (eventsBasePaths.isEmpty()) {
      return;
    }

    // Expired date should be computed each time we run cleaner thread.
    computeExpiredDatePtn(ttl);
    for (Path basePath : eventsBasePaths) {
      cleanupDir(basePath);
    }
  }

  /**
   * Compute the expired date partition, using the underlying clock in UTC time.
   */
  private static void computeExpiredDatePtn(long ttl) {
    // Use UTC date to ensure reader date is same on all timezones.
    LocalDate expiredDate
            = LocalDateTime.ofEpochSecond((clock.getTime() - ttl) / 1000, 0, ZoneOffset.UTC).toLocalDate();
    expiredDatePtn = "date=" + DateTimeFormatter.ISO_LOCAL_DATE.format(expiredDate);
  }

  /**
   * Path filters to include only expired date partitions based on TTL.
   */
  private static final PathFilter expiredDatePartitionsFilter = new PathFilter() {
    @Override
    public boolean accept(Path path) {
      String dirName = path.getName();
      return ((dirName.startsWith("date="))
              && (dirName.compareTo(expiredDatePtn) <= 0));
    }
  };

  /**
   * Finds the expired date partitioned events directory based on TTL and delete them.
   */
  private void cleanupDir(Path eventsBasePath) {
    LOG.debug("Trying to delete expired proto events from " + eventsBasePath);
    try {
      FileSystem fs = FileSystem.get(eventsBasePath.toUri(), conf);
      if (!fs.exists(eventsBasePath)) {
        return;
      }
      FileStatus[] statuses = fs.listStatus(eventsBasePath, expiredDatePartitionsFilter);
      for (FileStatus dir : statuses) {
        try {
          deleteDirByOwner(fs, dir);
          LOG.info("Deleted expired proto events dir: " + dir.getPath());
        } catch (IOException ioe) {
          // Log error and continue to delete other expired dirs.
          LOG.error("Error deleting expired proto events dir " + dir.getPath(), ioe);
        }
      }
    } catch (IOException e) {
      LOG.error("Error while trying to delete expired proto events from " + eventsBasePath, e);
    }
  }

  /**
   * Delete the events dir with it's owner as proxy user.
   */
  private void deleteDirByOwner(FileSystem fs, FileStatus eventsDir) throws IOException {
    String owner = eventsDir.getOwner();
    if (owner.equals(System.getProperty("user.name"))) {
      fs.delete(eventsDir.getPath(), true);
    } else {
      LOG.info("Deleting " + eventsDir.getPath() + " as user " + owner);
      UserGroupInformation ugi = UserGroupInformation.createProxyUser(owner,
              UserGroupInformation.getLoginUser());
      try {
        ugi.doAs(new PrivilegedExceptionAction<Object>() {
          @Override
          public Object run() throws Exception {
            // New FileSystem object to be obtained in user context for doAs flow.
            try (FileSystem doAsFs = FileSystem.newInstance(eventsDir.getPath().toUri(), conf)) {
              doAsFs.delete(eventsDir.getPath(), true);
            }
            return null;
          }
        });
      } catch (InterruptedException ie) {
        LOG.error("Could not delete " + eventsDir.getPath() + " for UGI: " + ugi, ie);
      }
    }
  }
}
