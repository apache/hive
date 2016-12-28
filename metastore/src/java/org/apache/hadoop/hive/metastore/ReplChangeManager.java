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

package org.apache.hadoop.hive.metastore;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplChangeManager {
  private static final Logger LOG = LoggerFactory.getLogger(ReplChangeManager.class);
  static private ReplChangeManager instance;

  private static boolean inited = false;
  private static boolean enabled = false;
  private static Path cmroot;
  private static HiveConf conf;
  private static Warehouse wh;
  private String user;
  private String group;

  public static ReplChangeManager getInstance(HiveConf conf, Warehouse wh) throws IOException {
    if (instance == null) {
      instance = new ReplChangeManager(conf, wh);
    }
    return instance;
  }

  ReplChangeManager(HiveConf conf, Warehouse wh) throws IOException {
    if (!inited) {
      if (conf.getBoolVar(HiveConf.ConfVars.REPLCMENABLED)) {
        ReplChangeManager.enabled = true;
        ReplChangeManager.cmroot = new Path(conf.get(HiveConf.ConfVars.REPLCMDIR.varname));
        ReplChangeManager.conf = conf;
        ReplChangeManager.wh = wh;

        FileSystem fs = cmroot.getFileSystem(conf);
        // Create cmroot with permission 700 if not exist
        if (!fs.exists(cmroot)) {
          fs.mkdirs(cmroot);
          fs.setPermission(cmroot, new FsPermission("700"));
        }
        UserGroupInformation usergroupInfo = UserGroupInformation.getCurrentUser();
        user = usergroupInfo.getShortUserName();
        group = usergroupInfo.getPrimaryGroupName();
      }
      inited = true;
    }
  }

  /***
   * Recycle a managed table, move table files to cmroot
   * @param db
   * @param table
   * @return
   * @throws IOException
   * @throws MetaException
   */
  public int recycle(Database db, Table table) throws IOException, MetaException {
    if (!enabled) {
      return 0;
    }

    Path tablePath = wh.getTablePath(db, table.getTableName());
    FileSystem fs = tablePath.getFileSystem(conf);
    int failCount = 0;
    for (FileStatus file : fs.listStatus(tablePath)) {
      if (!recycle(file.getPath())) {
        failCount++;
      }
    }
    return failCount;
  }

  /***
   * Recycle a partition of a managed table, move partition files to cmroot
   * @param db
   * @param table
   * @param part
   * @return
   * @throws IOException
   * @throws MetaException
   */
  public int recycle(Database db, Table table, Partition part) throws IOException, MetaException {
    if (!enabled) {
      return 0;
    }

    Map<String, String> pm = Warehouse.makeSpecFromValues(table.getPartitionKeys(), part.getValues());
    Path partPath = wh.getPartitionPath(db, table.getTableName(), pm);
    FileSystem fs = partPath.getFileSystem(conf);
    int failCount = 0;
    for (FileStatus file : fs.listStatus(partPath)) {
      if (!recycle(file.getPath())) {
        failCount++;
      }
    }
    return failCount;
  }

  /***
   * Recycle a single file (of a partition, or table if nonpartitioned),
   *   move files to cmroot. Note the table must be managed table
   * @param path
   * @return
   * @throws IOException
   * @throws MetaException
   */
  public boolean recycle(Path path) throws IOException, MetaException {
    if (!enabled) {
      return true;
    }

    Path cmPath = getCMPath(path, conf, getCksumString(path, conf));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Moving " + path.toString() + " to " + cmPath.toString());
    }

    FileSystem fs = path.getFileSystem(conf);

    boolean succ = fs.rename(path, cmPath);
    // Ignore if a file with same content already exist in cmroot
    // We might want to setXAttr for the new location in the future
    if (!succ) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("A file with the same content of " + path.toString() + " already exists, ignore");
      }
    } else {
      long now = System.currentTimeMillis();
      fs.setTimes(cmPath, now, now);

      // set the file owner to hive (or the id metastore run as)
      fs.setOwner(cmPath, user, group);

      // tag the original file name so we know where the file comes from
      fs.setXAttr(cmPath, "user.original-loc", path.toString().getBytes());
    }
    return succ;
  }

  // Get checksum of a file
  static public String getCksumString(Path path, Configuration conf) throws IOException {
    // TODO: fs checksum only available on hdfs, need to
    //       find a solution for other fs (eg, local fs, s3, etc)
    FileSystem fs = path.getFileSystem(conf);
    FileChecksum checksum = fs.getFileChecksum(path);
    String checksumString = StringUtils.byteToHexString(
        checksum.getBytes(), 0, checksum.getLength());
    return checksumString;
  }

  /***
   * Convert a path of file inside a partition or table (if non-partitioned)
   *   to a deterministic location of cmroot. So user can retrieve the file back
   *   with the original location plus signature.
   * @param path original path inside partition or table
   * @param conf
   * @param signature unique signature of the file, can be retrieved by {@link getSignature}
   * @return
   * @throws IOException
   * @throws MetaException
   */
  static public Path getCMPath(Path path, Configuration conf, String signature)
      throws IOException, MetaException {
    String newFileName = signature + path.getName();
    int maxLength = conf.getInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_DEFAULT);

    if (newFileName.length() > maxLength) {
      newFileName = newFileName.substring(0, maxLength-1);
    }

    Path cmPath = new Path(cmroot, newFileName);

    return cmPath;
  }

  /**
   * Thread to clear old files of cmroot recursively
   */
  static class CMClearer implements Runnable {
    private Path cmroot;
    private long secRetain;
    private Configuration conf;

    CMClearer(String cmrootString, long secRetain, Configuration conf) {
      this.cmroot = new Path(cmrootString);
      this.secRetain = secRetain;
      this.conf = conf;
    }

    @Override
    public void run() {
      try {
        LOG.info("CMClearer started");
        long now = System.currentTimeMillis();
        processDir(cmroot, now);
      } catch (IOException e) {
        LOG.error("Exception when clearing cmroot:" + StringUtils.stringifyException(e));
      }
    }

    private boolean processDir(Path folder, long now) throws IOException {
      FileStatus[] files = folder.getFileSystem(conf).listStatus(folder);
      boolean empty = true;
      for (FileStatus file : files) {
        if (file.isDirectory()) {
          if (processDir(file.getPath(), now)) {
            file.getPath().getFileSystem(conf).delete(file.getPath(), false);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Remove " + file.toString());
            }
          } else {
            empty = false;
          }
        } else {
          long modifiedTime = file.getModificationTime();
          if (now - modifiedTime > secRetain*1000) {
            file.getPath().getFileSystem(conf).delete(file.getPath(), false);
          } else {
            empty = false;
          }
        }
      }
      return empty;
    }
  }

  // Schedule CMClearer thread. Will be invoked by metastore
  public static void scheduleCMClearer(HiveConf hiveConf) {
    if (hiveConf.getBoolVar(HiveConf.ConfVars.REPLCMENABLED)) {
      ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
          new BasicThreadFactory.Builder()
          .namingPattern("cmclearer-%d")
          .daemon(true)
          .build());
      executor.scheduleAtFixedRate(new CMClearer(hiveConf.get(HiveConf.ConfVars.REPLCMDIR.varname),
          HiveConf.getTimeVar(hiveConf, ConfVars.REPLCMRETIAN, TimeUnit.SECONDS), hiveConf),
          0,
          HiveConf.getTimeVar(hiveConf, ConfVars.REPLCMINTERVAL,
          TimeUnit.SECONDS), TimeUnit.SECONDS);
    }
  }
}
