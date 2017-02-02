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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.MetaException;
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
  private static HiveConf hiveConf;
  private String msUser;
  private String msGroup;
  private FileSystem fs;

  public static final String ORIG_LOC_TAG = "user.original-loc";
  public static final String REMAIN_IN_TRASH_TAG = "user.remain-in-trash";
  public static final String URI_FRAGMENT_SEPARATOR = "#";

  public static ReplChangeManager getInstance(HiveConf hiveConf) throws MetaException {
    if (instance == null) {
      instance = new ReplChangeManager(hiveConf);
    }
    return instance;
  }

  ReplChangeManager(HiveConf hiveConf) throws MetaException {
    try {
      if (!inited) {
        if (hiveConf.getBoolVar(HiveConf.ConfVars.REPLCMENABLED)) {
          ReplChangeManager.enabled = true;
          ReplChangeManager.cmroot = new Path(hiveConf.get(HiveConf.ConfVars.REPLCMDIR.varname));
          ReplChangeManager.hiveConf = hiveConf;

          fs = cmroot.getFileSystem(hiveConf);
          // Create cmroot with permission 700 if not exist
          if (!fs.exists(cmroot)) {
            fs.mkdirs(cmroot);
            fs.setPermission(cmroot, new FsPermission("700"));
          }
          UserGroupInformation usergroupInfo = UserGroupInformation.getCurrentUser();
          msUser = usergroupInfo.getShortUserName();
          msGroup = usergroupInfo.getPrimaryGroupName();
        }
        inited = true;
      }
    } catch (IOException e) {
      throw new MetaException(StringUtils.stringifyException(e));
    }
  }

  // Filter files starts with ".". Note Hadoop consider files starts with
  // "." or "_" as hidden file. However, we need to replicate files starts
  // with "_". We find at least 2 use cases:
  // 1. For har files, _index and _masterindex is required files
  // 2. _success file is required for Oozie to indicate availability of data source
  private static final PathFilter hiddenFileFilter = new PathFilter(){
    public boolean accept(Path p){
      return !p.getName().startsWith(".");
    }
  };

  /***
   * Move a path into cmroot. If the path is a directory (of a partition, or table if nonpartitioned),
   *   recursively move files inside directory to cmroot. Note the table must be managed table
   * @param path a single file or directory
   * @param ifPurge if the file should skip Trash when delete
   * @return
   * @throws MetaException
   */
  public int recycle(Path path, boolean ifPurge) throws MetaException {
    if (!enabled) {
      return 0;
    }

    try {
      int count = 0;

      if (fs.isDirectory(path)) {
        FileStatus[] files = fs.listStatus(path, hiddenFileFilter);
        for (FileStatus file : files) {
          count += recycle(file.getPath(), ifPurge);
        }
      } else {
        Path cmPath = getCMPath(path, hiveConf, getChksumString(path, fs));

        if (LOG.isDebugEnabled()) {
          LOG.debug("Moving " + path.toString() + " to " + cmPath.toString());
        }

        // set timestamp before moving to cmroot, so we can
        // avoid race condition CM remove the file before setting
        // timestamp
        long now = System.currentTimeMillis();
        fs.setTimes(path, now, now);

        boolean succ = fs.rename(path, cmPath);
        // Ignore if a file with same content already exist in cmroot
        // We might want to setXAttr for the new location in the future
        if (!succ) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("A file with the same content of " + path.toString() + " already exists, ignore");
          }
          // Need to extend the tenancy if we saw a newer file with the same content
          fs.setTimes(cmPath, now, now);
        } else {

          // set the file owner to hive (or the id metastore run as)
          fs.setOwner(cmPath, msUser, msGroup);

          // tag the original file name so we know where the file comes from
          // Note we currently only track the last known trace as
          // xattr has limited capacity. We shall revisit and store all original
          // locations if orig-loc becomes important
          try {
            fs.setXAttr(cmPath, ORIG_LOC_TAG, path.toString().getBytes());
          } catch (UnsupportedOperationException e) {
            LOG.warn("Error setting xattr for " + path.toString());
          }

          count++;
        }
        // Tag if we want to remain in trash after deletion.
        // If multiple files share the same content, then
        // any file claim remain in trash would be granted
        if (!ifPurge) {
          try {
            fs.setXAttr(cmPath, REMAIN_IN_TRASH_TAG, new byte[]{0});
          } catch (UnsupportedOperationException e) {
            LOG.warn("Error setting xattr for " + cmPath.toString());
          }
        }
      }
      return count;
    } catch (IOException e) {
      throw new MetaException(StringUtils.stringifyException(e));
    }
  }

  // Get checksum of a file
  static public String getChksumString(Path path, FileSystem fs) throws IOException {
    // TODO: fs checksum only available on hdfs, need to
    //       find a solution for other fs (eg, local fs, s3, etc)
    String checksumString = null;
    FileChecksum checksum = fs.getFileChecksum(path);
    if (checksum != null) {
      checksumString = StringUtils.byteToHexString(
          checksum.getBytes(), 0, checksum.getLength());
    }
    return checksumString;
  }

  static public void setCmRoot(Path cmRoot) {
    ReplChangeManager.cmroot = cmRoot;
  }

  /***
   * Convert a path of file inside a partition or table (if non-partitioned)
   *   to a deterministic location of cmroot. So user can retrieve the file back
   *   with the original location plus checksum.
   * @param path original path inside partition or table
   * @param conf
   * @param chksum checksum of the file, can be retrieved by {@link getCksumString}
   * @return
   * @throws IOException
   * @throws MetaException
   */
  static public Path getCMPath(Path path, Configuration conf, String chksum)
      throws IOException, MetaException {
    String newFileName = chksum;
    int maxLength = conf.getInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_DEFAULT);

    if (newFileName.length() > maxLength) {
      newFileName = newFileName.substring(0, maxLength-1);
    }

    Path cmPath = new Path(cmroot, newFileName);

    return cmPath;
  }

  /***
   * Get original file specified by src and chksumString. If the file exists and checksum
   * matches, return the file; otherwise, use chksumString to retrieve it from cmroot
   * @param src Original file location
   * @param chksumString Checksum of the original file
   * @param conf
   * @return Corresponding FileStatus object
   * @throws MetaException
   */
  static public FileStatus getFileStatus(Path src, String chksumString,
      HiveConf conf) throws MetaException {
    try {
      FileSystem srcFs = src.getFileSystem(conf);
      if (chksumString == null) {
        return srcFs.getFileStatus(src);
      }

      if (!srcFs.exists(src)) {
        return srcFs.getFileStatus(getCMPath(src, conf, chksumString));
      }

      String currentChksumString = getChksumString(src, srcFs);
      if (currentChksumString == null || chksumString.equals(currentChksumString)) {
        return srcFs.getFileStatus(src);
      } else {
        return srcFs.getFileStatus(getCMPath(src, conf, chksumString));
      }
    } catch (IOException e) {
      throw new MetaException(StringUtils.stringifyException(e));
    }
  }

  /***
   * Concatenate filename and checksum with "#"
   * @param fileUriStr Filename string
   * @param fileChecksum Checksum string
   * @return Concatenated Uri string
   */
  // TODO: this needs to be enhanced once change management based filesystem is implemented
  // Currently using fileuri#checksum as the format
  static public String encodeFileUri(String fileUriStr, String fileChecksum) {
    if (fileChecksum != null) {
      return fileUriStr + URI_FRAGMENT_SEPARATOR + fileChecksum;
    } else {
      return fileUriStr;
    }
  }

  /***
   * Split uri with fragment into file uri and checksum
   * @param fileURIStr uri with fragment
   * @return array of file name and checksum
   */
  static public String[] getFileWithChksumFromURI(String fileURIStr) {
    String[] uriAndFragment = fileURIStr.split(URI_FRAGMENT_SEPARATOR);
    String[] result = new String[2];
    result[0] = uriAndFragment[0];
    if (uriAndFragment.length>1) {
      result[1] = uriAndFragment[1];
    }
    return result;
  }

  /**
   * Thread to clear old files of cmroot recursively
   */
  static class CMClearer implements Runnable {
    private Path cmroot;
    private long secRetain;
    private HiveConf hiveConf;

    CMClearer(String cmrootString, long secRetain, HiveConf hiveConf) {
      this.cmroot = new Path(cmrootString);
      this.secRetain = secRetain;
      this.hiveConf = hiveConf;
    }

    @Override
    public void run() {
      try {
        LOG.info("CMClearer started");

        long now = System.currentTimeMillis();
        FileSystem fs = cmroot.getFileSystem(hiveConf);
        FileStatus[] files = fs.listStatus(cmroot);

        for (FileStatus file : files) {
          long modifiedTime = file.getModificationTime();
          if (now - modifiedTime > secRetain*1000) {
            try {
              if (fs.getXAttrs(file.getPath()).containsKey(REMAIN_IN_TRASH_TAG)) {
                boolean succ = Trash.moveToAppropriateTrash(fs, file.getPath(), hiveConf);
                if (succ) {
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Move " + file.toString() + " to trash");
                  }
                } else {
                  LOG.warn("Fail to move " + file.toString() + " to trash");
                }
              } else {
                boolean succ = fs.delete(file.getPath(), false);
                if (succ) {
                  if (LOG.isDebugEnabled()) {
                    LOG.debug("Remove " + file.toString());
                  }
                } else {
                  LOG.warn("Fail to remove " + file.toString());
                }
              }
            } catch (UnsupportedOperationException e) {
              LOG.warn("Error getting xattr for " + file.getPath().toString());
            }
          }
        }
      } catch (IOException e) {
        LOG.error("Exception when clearing cmroot:" + StringUtils.stringifyException(e));
      }
    }
  }

  // Schedule CMClearer thread. Will be invoked by metastore
  public static void scheduleCMClearer(HiveConf hiveConf) {
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.REPLCMENABLED)) {
      ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
          new BasicThreadFactory.Builder()
          .namingPattern("cmclearer-%d")
          .daemon(true)
          .build());
      executor.scheduleAtFixedRate(new CMClearer(hiveConf.get(HiveConf.ConfVars.REPLCMDIR.varname),
          hiveConf.getTimeVar(ConfVars.REPLCMRETIAN, TimeUnit.SECONDS), hiveConf),
          0, hiveConf.getTimeVar(ConfVars.REPLCMINTERVAL, TimeUnit.SECONDS), TimeUnit.SECONDS);
    }
  }
}
