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

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveProtoEventsCleanerTask;
import org.apache.hadoop.hive.metastore.utils.JavaUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.util.SystemClock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHiveProtoEventsCleanerTask {
  protected static final Logger LOG = LoggerFactory.getLogger(TestHiveProtoEventsCleanerTask.class);

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();
  private Path baseDir;
  private HiveConf hiveConf;
  private SystemClock clock = SystemClock.getInstance();
  private HiveProtoEventsCleanerTask cleanerTask;
  private FileSystem fs;

  private final String[] eventsSubDirs = new String[] { "query_data", "dag_meta", "dag_data", "app_data" };

  @Before
  public void setup() throws Exception {
    hiveConf = new HiveConf(TestHiveProtoEventsCleanerTask.class);
    String tmpFolder = folder.newFolder().getAbsolutePath();
    hiveConf.setVar(HiveConf.ConfVars.HIVE_PROTO_EVENTS_BASE_PATH, tmpFolder + "/" + eventsSubDirs[0]);
    HiveConf.setTimeVar(hiveConf, ConfVars.HIVE_PROTO_EVENTS_TTL, 2, TimeUnit.DAYS);

    baseDir = new Path(tmpFolder);
    fs = baseDir.getFileSystem(hiveConf);
    cleanerTask = JavaUtils.newInstance(HiveProtoEventsCleanerTask.class);
    cleanerTask.setConf(hiveConf);
  }

  /**
   * Returns the current date, using the underlying clock in UTC time.
   */
  private LocalDate getNow() {
    // Use UTC date to ensure reader date is same on all timezones.
    return LocalDateTime.ofEpochSecond(clock.getTime() / 1000, 0, ZoneOffset.UTC).toLocalDate();
  }

  /**
   * Returns the directory name for a given date.
   */
  public String getDirForDate(LocalDate date) {
    return "date=" + DateTimeFormatter.ISO_LOCAL_DATE.format(date);
  }

  private void addDatePartition(Path basePath, LocalDate date) throws Exception {
    if (!fs.exists(basePath)) {
      fs.mkdirs(basePath);
      fs.setPermission(basePath, FsPermission.createImmutable((short)01777));
    }

    Path datePtn = new Path(basePath, getDirForDate(date));
    fs.mkdirs(datePtn);
    fs.setPermission(datePtn, FsPermission.createImmutable((short) 01777));
    FsPermission.setUMask(hiveConf, FsPermission.createImmutable((short) 0066));
    Path partFile = new Path(datePtn, "data");
    FSDataOutputStream out = fs.create(partFile);
    out.writeInt(1000);
    out.close();
  }

  @Test
  public void testCleanup() throws Exception {
    int[] inRange = { 3, 5, 2, 1 }; // Must have one entry per eventsSubDirs
    int[] outRange = { 2, 2, 2, 1 }; // Must have one entry per eventsSubDirs
    LocalDate today = getNow();

    // Add partitions for the given range of dates from today to past.
    for (int i = 0; i < inRange.length; i++) {
      Path basePath = new Path(baseDir + "/" + eventsSubDirs[i]);
      for (int j = 0; j < inRange[i]; j++) {
        addDatePartition(basePath, today.minusDays(j));
      }
    }

    // Run the task to cleanup
    cleanerTask.run();

    // Verify if the remaining partitions are not expired ones.
    String expiredPtn = getDirForDate(today.minusDays(2));
    for (int i = 0; i < inRange.length; i++) {
      Path basePath = new Path(baseDir + "/" + eventsSubDirs[i]);
      FileStatus[] statuses = fs.listStatus(basePath);

      // If the test setup created today and if test runs tomorrow, then extra dir will be deleted.
      // So, checking for both cases.
      assertTrue((statuses.length == outRange[i]) || (statuses.length == (outRange[i] - 1)));
      for (FileStatus status : statuses) {
        assertTrue(status.getPath().getName().compareTo(expiredPtn) >= 0);
      }
    }
  }
}
