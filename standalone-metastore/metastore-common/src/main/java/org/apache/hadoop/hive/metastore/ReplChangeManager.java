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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.Trash;
import org.apache.hadoop.fs.permission.AclEntry;
import static org.apache.hadoop.fs.permission.AclEntryScope.ACCESS;
import static org.apache.hadoop.fs.permission.AclEntryType.GROUP;
import static org.apache.hadoop.fs.permission.AclEntryType.USER;
import static org.apache.hadoop.fs.permission.AclEntryType.OTHER;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hive.common.repl.ReplConst;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.EncryptionZoneUtils;
import org.apache.hadoop.hive.metastore.utils.FileUtils;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.Retry;
import org.apache.hadoop.hive.metastore.utils.StringUtils;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.security.UserGroupInformation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplChangeManager {
  private static final Logger LOG = LoggerFactory.getLogger(ReplChangeManager.class);
  static private ReplChangeManager instance;

  private static boolean inited = false;
  private static boolean enabled = false;
  private static Map<String, String> encryptionZoneToCmrootMapping = new HashMap<>();
  private static Configuration conf;
  private String msUser;
  private String msGroup;

  private static final String ORIG_LOC_TAG = "user.original-loc";
  static final String REMAIN_IN_TRASH_TAG = "user.remain-in-trash";
  private static final String URI_FRAGMENT_SEPARATOR = "#";
  public static final String SOURCE_OF_REPLICATION = ReplConst.SOURCE_OF_REPLICATION;
  private static final String TXN_WRITE_EVENT_FILE_SEPARATOR = "]";
  public static final String CM_THREAD_NAME_PREFIX = "cmclearer-";
  private static final String NO_ENCRYPTION = "noEncryption";
  private static String cmRootDir;
  private static String encryptedCmRootDir;
  private static String fallbackNonEncryptedCmRootDir;

  public enum RecycleType {
    MOVE,
    COPY
  }

  public static class FileInfo {
    private FileSystem srcFs;
    private Path sourcePath;
    private Path cmPath;
    private String checkSum;
    private boolean useSourcePath;
    private String subDir;
    private boolean copyDone;

    public FileInfo(FileSystem srcFs, Path sourcePath, String subDir) {
      this(srcFs, sourcePath, null, null, true, subDir);
    }
    public FileInfo(FileSystem srcFs, Path sourcePath, Path cmPath,
                    String checkSum, boolean useSourcePath, String subDir) {
      this.srcFs = srcFs;
      this.sourcePath = sourcePath;
      this.cmPath = cmPath;
      this.checkSum = checkSum;
      this.useSourcePath = useSourcePath;
      this.subDir = subDir;
      this.copyDone = false;
    }
    public FileSystem getSrcFs() {
      return srcFs;
    }
    public Path getSourcePath() {
      return sourcePath;
    }
    public Path getCmPath() {
      return cmPath;
    }
    public String getCheckSum() {
      return checkSum;
    }
    public boolean isUseSourcePath() {
      return useSourcePath;
    }
    public void setIsUseSourcePath(boolean useSourcePath) {
      this.useSourcePath = useSourcePath;
    }
    public String getSubDir() {
      return subDir;
    }
    public boolean isCopyDone() {
      return copyDone;
    }
    public void setCopyDone(boolean copyDone) {
      this.copyDone = copyDone;
    }
    public Path getEffectivePath() {
      if (useSourcePath) {
        return sourcePath;
      } else {
        return cmPath;
      }
    }
  }

  public static synchronized ReplChangeManager getInstance(Configuration conf)
      throws MetaException {
    if (instance == null) {
      instance = new ReplChangeManager(conf);
    }
    return instance;
  }

  public static synchronized ReplChangeManager getInstance() {
    if (!inited) {
      throw new IllegalStateException("Replication Change Manager is not initialized.");
    }
    return instance;
  }

  private ReplChangeManager(Configuration conf) throws MetaException {
    try {
      if (!inited) {
        if (MetastoreConf.getBoolVar(conf, ConfVars.REPLCMENABLED)) {
          ReplChangeManager.enabled = true;
          ReplChangeManager.conf = conf;
          cmRootDir = MetastoreConf.getVar(conf, ConfVars.REPLCMDIR);
          encryptedCmRootDir = MetastoreConf.getVar(conf, ConfVars.REPLCMENCRYPTEDDIR);
          fallbackNonEncryptedCmRootDir = MetastoreConf.getVar(conf, ConfVars.REPLCMFALLBACKNONENCRYPTEDDIR);
          //validate cmRootEncrypted is absolute
          Path cmRootEncrypted = new Path(encryptedCmRootDir);
          if (cmRootEncrypted.isAbsolute()) {
            throw new MetaException(ConfVars.REPLCMENCRYPTEDDIR.getHiveName() + " should be a relative path");
          }
          //Create default cm root
          Path cmroot = new Path(cmRootDir);
          createCmRoot(cmroot);
          FileSystem cmRootFs = cmroot.getFileSystem(conf);
          if (EncryptionZoneUtils.isPathEncrypted(cmroot, conf)) {
            //If cm root is encrypted we keep using it for the encryption zone
            String encryptionZonePath = cmRootFs.getUri()
                    + EncryptionZoneUtils.getEncryptionZoneForPath(cmroot, conf).getPath();
            encryptionZoneToCmrootMapping.put(encryptionZonePath, cmRootDir);
          } else {
            encryptionZoneToCmrootMapping.put(NO_ENCRYPTION, cmRootDir);
          }
          if (!StringUtils.isEmpty(fallbackNonEncryptedCmRootDir)) {
            Path cmRootFallback = new Path(fallbackNonEncryptedCmRootDir);
            if (!cmRootFallback.isAbsolute()) {
              throw new MetaException(ConfVars.REPLCMENCRYPTEDDIR.getHiveName() + " should be absolute path");
            }
            createCmRoot(cmRootFallback);
            if (EncryptionZoneUtils.isPathEncrypted(cmRootFallback, conf)) {
              throw new MetaException(ConfVars.REPLCMFALLBACKNONENCRYPTEDDIR.getHiveName()
                      + " should not be encrypted");
            }
          }
          UserGroupInformation usergroupInfo = UserGroupInformation.getCurrentUser();
          msUser = usergroupInfo.getShortUserName();
          msGroup = usergroupInfo.getPrimaryGroupName();
        }
        inited = true;
      }
    } catch (IOException e) {
      LOG.error("Failed to created ReplChangeManager", e);
      throw new MetaException(e.getMessage());
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
   * @param type if the files to be copied or moved to cmpath.
   *             Copy is costly but preserve the source file
   * @param ifPurge if the file should skip Trash when move/delete source file.
   *                This is referred only if type is MOVE.
   * @return int
   * @throws IOException
   */
  public int recycle(Path path, RecycleType type, boolean ifPurge) throws IOException {
    if (!enabled) {
      return 0;
    }

    int count = 0;
    FileSystem fs = path.getFileSystem(conf);
    if (fs.isDirectory(path)) {
      FileStatus[] files = fs.listStatus(path, hiddenFileFilter);
      for (FileStatus file : files) {
        count += recycle(file.getPath(), type, ifPurge);
      }
    } else {
      String fileCheckSum = checksumFor(path, fs);
      Path cmRootPath = getCmRoot(path);
      String cmRoot = null;
      if (cmRootPath != null) {
        cmRoot = FileUtils.makeQualified(cmRootPath, conf).toString();
      }
      Path cmPath = getCMPath(conf, path.getName(), fileCheckSum, cmRoot);

      // set timestamp before moving to cmroot, so we can
      // avoid race condition CM remove the file before setting
      // timestamp
      long now = System.currentTimeMillis();
      fs.setTimes(path, now, -1);

      boolean success = false;
      if (fs.exists(cmPath) && fileCheckSum.equalsIgnoreCase(checksumFor(cmPath, fs))) {
        // If already a file with same checksum exists in cmPath, just ignore the copy/move
        // Also, mark the operation is unsuccessful to notify that file with same name already
        // exist which will ensure the timestamp of cmPath is updated to avoid clean-up by
        // CM cleaner.
        success = false;
      } else {
        switch (type) {
        case MOVE: {
          LOG.info("Moving {} to {}", path.toString(), cmPath.toString());
          // Rename fails if the file with same name already exist.
          Retry<Boolean> retriable = new Retry<Boolean>(IOException.class) {
            @Override
            public Boolean execute() throws IOException {
              return fs.rename(path, cmPath);
            }
          };
          try {
            success = retriable.run();
          } catch (Exception e) {
            throw new IOException(org.apache.hadoop.util.StringUtils.stringifyException(e));
          }
          break;
        }
        case COPY: {
          LOG.info("Copying {} to {}", path.toString(), cmPath.toString());

          // It is possible to have a file with same checksum in cmPath but the content is
          // partially copied or corrupted. In this case, just overwrite the existing file with
          // new one.
          success = FileUtils.copy(fs, path, fs, cmPath, false, true, conf);
          break;
        }
        default:
          // Operation fails as invalid input
          break;
        }
      }

      // Ignore if a file with same content already exist in cmroot
      // We might want to setXAttr for the new location in the future
      if (success) {
        // tag the original file name so we know where the file comes from
        // Note we currently only track the last known trace as
        // xattr has limited capacity. We shall revisit and store all original
        // locations if orig-loc becomes important
        try {
          fs.setXAttr(cmPath, ORIG_LOC_TAG, path.toString().getBytes(StandardCharsets.UTF_8));
        } catch (UnsupportedOperationException e) {
          LOG.warn("Error setting xattr for {}", path.toString());
        }

        count++;
      } else {
        LOG.debug("A file with the same content of {} already exists, ignore", path.toString());
        // Need to extend the tenancy if we saw a newer file with the same content
        fs.setTimes(cmPath, now, -1);
      }

      // Tag if we want to remain in trash after deletion.
      // If multiple files share the same content, then
      // any file claim remain in trash would be granted
      if ((type == RecycleType.MOVE) && !ifPurge) {
        try {
          fs.setXAttr(cmPath, REMAIN_IN_TRASH_TAG, new byte[] { 0 });
        } catch (UnsupportedOperationException e) {
          LOG.warn("Error setting xattr for {}", cmPath.toString());
        }
      }
    }
    return count;
  }

  // Get checksum of a file
  static public String checksumFor(Path path, FileSystem fs) throws IOException {
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

  /***
   * Convert a path of file inside a partition or table (if non-partitioned)
   *   to a deterministic location of cmroot. So user can retrieve the file back
   *   with the original location plus checksum.
   * @param conf Hive configuration
   * @param name original filename
   * @param checkSum checksum of the file, can be retrieved by {@link #checksumFor(Path, FileSystem)}
   * @param cmRootUri CM Root URI. (From remote source if REPL LOAD flow. From local config if recycle.)
   * @return Path
   */
  static Path getCMPath(Configuration conf, String name, String checkSum, String cmRootUri) {
    String newFileName = name + "_" + checkSum;
    int maxLength = conf.getInt(DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_KEY,
        DFSConfigKeys.DFS_NAMENODE_MAX_COMPONENT_LENGTH_DEFAULT);

    if (newFileName.length() > maxLength) {
      newFileName = newFileName.substring(0, maxLength-1);
    }

    return new Path(cmRootUri, newFileName);
  }

  /***
   * Get original file specified by src and chksumString. If the file exists and checksum
   * matches, return the file; otherwise, use chksumString to retrieve it from cmroot
   * @param src Original file location
   * @param checksumString Checksum of the original file
   * @param srcCMRootURI CM root URI of the source cluster
   * @param subDir Sub directory to which the source file belongs to
   * @param conf Hive configuration
   * @return Corresponding FileInfo object
   */
  public static FileInfo getFileInfo(Path src, String checksumString, String srcCMRootURI, String subDir,
      Configuration conf) throws IOException {
    FileSystem srcFs = src.getFileSystem(conf);
    if (checksumString == null) {
      return new FileInfo(srcFs, src, subDir);
    }

    Path cmPath = getCMPath(conf, src.getName(), checksumString, srcCMRootURI);
    if (!srcFs.exists(src)) {
      return new FileInfo(srcFs, src, cmPath, checksumString, false, subDir);
    }

    String currentChecksumString;
    try {
      currentChecksumString = checksumFor(src, srcFs);
    } catch (IOException ex) {
      // If the file is missing or getting modified, then refer CM path
      return new FileInfo(srcFs, src, cmPath, checksumString, false, subDir);
    }
    if ((currentChecksumString == null) || checksumString.equals(currentChecksumString)) {
      return new FileInfo(srcFs, src, cmPath, checksumString, true, subDir);
    } else {
      return new FileInfo(srcFs, src, cmPath, checksumString, false, subDir);
    }
  }

  /***
   * Concatenate filename, checksum, source cmroot uri and subdirectory with "#"
   * @param fileUriStr Filename string
   * @param fileChecksum Checksum string
   * @param encodedSubDir sub directory path into which this file belongs to. Here encoded means,
   *                      the multiple levels of subdirectories are concatenated with path separator "/"
   * @return Concatenated Uri string
   */
  // TODO: this needs to be enhanced once change management based filesystem is implemented
  // Currently using fileuri#checksum#cmrooturi#subdirs as the format
  public String encodeFileUri(String fileUriStr, String fileChecksum, String encodedSubDir)
          throws IOException {
    if (instance == null) {
      throw new IllegalStateException("Uninitialized ReplChangeManager instance.");
    }
    Path cmRootPath = getCmRoot(new Path(fileUriStr));
    String cmRoot = null;
    if (cmRootPath != null) {
      cmRoot = FileUtils.makeQualified(cmRootPath, conf).toString();
    }
    return ReplChangeManager.encodeFileUri(fileUriStr, fileChecksum, cmRoot, encodedSubDir);
  }

  public static String encodeFileUri(String fileUriStr, String fileChecksum, String cmRoot, String encodedSubDir) {
    String encodedUri = fileUriStr;
    if ((fileChecksum != null) && (cmRoot != null)) {
      encodedUri = encodedUri + URI_FRAGMENT_SEPARATOR + fileChecksum + URI_FRAGMENT_SEPARATOR + cmRoot;
    } else {
      encodedUri = encodedUri + URI_FRAGMENT_SEPARATOR + URI_FRAGMENT_SEPARATOR;
    }
    encodedUri = encodedUri + URI_FRAGMENT_SEPARATOR + ((encodedSubDir != null) ? encodedSubDir : "");
    LOG.debug("Encoded URI: " + encodedUri);
    return encodedUri;
  }

  /***
   * Split uri with fragment into file uri, subdirs, checksum and source cmroot uri.
   * Currently using fileuri#checksum#cmrooturi#subdirs as the format.
   * @param fileURIStr uri with fragment
   * @return array of file name, subdirs, checksum and source CM root URI
   */
  public static String[] decodeFileUri(String fileURIStr) {
    String[] uriAndFragment = fileURIStr.split(URI_FRAGMENT_SEPARATOR);
    String[] result = new String[4];
    result[0] = uriAndFragment[0];
    if ((uriAndFragment.length > 1) && !StringUtils.isEmpty(uriAndFragment[1])) {
      result[1] = uriAndFragment[1];
    }
    if ((uriAndFragment.length > 2)  && !StringUtils.isEmpty(uriAndFragment[2])) {
      result[2] = uriAndFragment[2];
    }
    if ((uriAndFragment.length > 3)  && !StringUtils.isEmpty(uriAndFragment[3])) {
      result[3] = uriAndFragment[3];
    }
    LOG.debug("Reading Encoded URI: " + result[0] + ":: " + result[1] + ":: " + result[2] + ":: " + result[3]);
    return result;
  }

  public static boolean isCMFileUri(Path fromPath) {
    String[] result = decodeFileUri(fromPath.toString());
    return result[1] != null;
  }

  /**
   * Thread to clear old files of cmroot recursively
   */
  public static class CMClearer implements Runnable {
    private Map<String, String> encryptionZones;
    private long secRetain;
    private Configuration conf;

    public CMClearer(long secRetain, Configuration conf) {
      this.encryptionZones = encryptionZoneToCmrootMapping;
      this.secRetain = secRetain;
      this.conf = conf;
    }

    CMClearer(Map<String, String> encryptionZones, long secRetain, Configuration conf) {
      this.encryptionZones = encryptionZones;
      this.secRetain = secRetain;
      this.conf = conf;
    }

    @Override
    public void run() {
      try {
        LOG.info("CMClearer started");
        for (String cmrootString : encryptionZones.values()) {
          Path cmroot = new Path(cmrootString);
          long now = System.currentTimeMillis();
          FileSystem fs = cmroot.getFileSystem(conf);
          FileStatus[] files = fs.listStatus(cmroot);

          for (FileStatus file : files) {
            long modifiedTime = file.getModificationTime();
            if (now - modifiedTime > secRetain * 1000) {
              try {
                if (fs.getXAttrs(file.getPath()).containsKey(REMAIN_IN_TRASH_TAG)) {
                  boolean succ = Trash.moveToAppropriateTrash(fs, file.getPath(), conf);
                  if (succ) {
                    LOG.debug("Move " + file.toString() + " to trash");
                  } else {
                    LOG.warn("Fail to move " + file.toString() + " to trash");
                  }
                } else {
                  boolean succ = fs.delete(file.getPath(), false);
                  if (succ) {
                    LOG.debug("Remove " + file.toString());
                  } else {
                    LOG.warn("Fail to remove " + file.toString());
                  }
                }
              } catch (UnsupportedOperationException e) {
                LOG.warn("Error getting xattr for " + file.getPath().toString());
              }
            }
          }
        }
      } catch (IOException e) {
        LOG.error("Exception when clearing cmroot", e);
      }
    }
  }

  // Schedule CMClearer thread. Will be invoked by metastore
  static void scheduleCMClearer(Configuration conf) {
    if (MetastoreConf.getBoolVar(conf, ConfVars.REPLCMENABLED)) {
      ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
          new BasicThreadFactory.Builder()
          .namingPattern(CM_THREAD_NAME_PREFIX + "%d")
          .daemon(true)
          .build());
      executor.scheduleAtFixedRate(new CMClearer(MetastoreConf.getTimeVar(conf, ConfVars.REPLCMRETIAN, TimeUnit.SECONDS), conf),
              0, MetastoreConf.getTimeVar(conf, ConfVars.REPLCMINTERVAL, TimeUnit.SECONDS), TimeUnit.SECONDS);
    }
  }

  public static boolean shouldEnableCm(Database db, Table table) {
    assert (table != null);
    return isSourceOfReplication(db) && !MetaStoreUtils.isExternalTable(table);
  }

  public static boolean isSourceOfReplication(Database db) {
    assert (db != null);
    String replPolicyIds = getReplPolicyIdString(db);
    return  !StringUtils.isEmpty(replPolicyIds);
  }

  public static String getReplPolicyIdString(Database db) {
    if (db != null) {
      Map<String, String> m = db.getParameters();
      if ((m != null) && (m.containsKey(ReplConst.SOURCE_OF_REPLICATION))) {
        String replPolicyId = m.get(ReplConst.SOURCE_OF_REPLICATION);
        LOG.debug("repl policy for database {} is {}", db.getName(), replPolicyId);
        return replPolicyId;
      }
      LOG.debug("Repl policy is not set for database: {}", db.getName());
    }
    return null;
  }

  public static String joinWithSeparator(Iterable<?> strings) {
    return org.apache.hadoop.util.StringUtils.join(TXN_WRITE_EVENT_FILE_SEPARATOR, strings);
  }

  public static String[] getListFromSeparatedString(String commaSeparatedString) {
    return commaSeparatedString.split("\\s*" + TXN_WRITE_EVENT_FILE_SEPARATOR + "\\s*");
  }

  @VisibleForTesting
  Path getCmRoot(Path path) throws IOException {
    Path cmroot = null;
    //Default path if hive.repl.cm dir is encrypted
    String cmrootDir = fallbackNonEncryptedCmRootDir;
    String encryptionZonePath = NO_ENCRYPTION;
    if (enabled) {
      if (EncryptionZoneUtils.isPathEncrypted(path, conf)) {
        encryptionZonePath = path.getFileSystem(conf).getUri()
                + EncryptionZoneUtils.getEncryptionZoneForPath(path, conf).getPath();
        //For encryption zone, create cm at the relative path specified by hive.repl.cm.encryptionzone.rootdir
        //at the root of the encryption zone
        cmrootDir = encryptionZonePath + Path.SEPARATOR + encryptedCmRootDir;
      }
      if (encryptionZoneToCmrootMapping.containsKey(encryptionZonePath)) {
        cmroot = new Path(encryptionZoneToCmrootMapping.get(encryptionZonePath));
      } else {
        cmroot = new Path(cmrootDir);
        synchronized (instance) {
          if (!encryptionZoneToCmrootMapping.containsKey(encryptionZonePath)) {
            createCmRoot(cmroot);
            encryptionZoneToCmrootMapping.put(encryptionZonePath, cmrootDir);
          }
        }
      }
    }
    return cmroot;
  }

  private static void createCmRoot(Path cmroot) throws IOException {
    Retry<Void> retriable = new Retry<Void>(IOException.class) {
      @Override
      public Void execute() throws IOException {
        FileSystem cmFs = cmroot.getFileSystem(conf);
        // Create cmroot if not exist
        if (!cmFs.exists(cmroot)) {
          cmFs.mkdirs(cmroot);
          setCmRootPermissions(cmroot);
        }
        return null;
      }
    };
    try {
      retriable.run();
    } catch (Exception e) {
      throw new IOException("Failed to createCmRoot", e);
    }
  }

  /*
   * Provide members of warehouse group access to the cmRoot location.
   * To do this, assign cmRoot to group of warehouse if possible. If not, set acl for wh-group.
   * If warehouse directory cannot be determined then give rwx permissions to default group of cmroot.
   */
  private static void setCmRootPermissions(Path cmroot) throws IOException{
    FileSystem cmFs = cmroot.getFileSystem(conf);
    cmFs.setPermission(cmroot, new FsPermission("770"));
    try {
      FileStatus warehouseStatus = cmFs.getFileStatus(new Path(MetastoreConf.get(conf, ConfVars.WAREHOUSE.getVarname())));
      String warehouseOwner = warehouseStatus.getOwner();
      String warehouseGroup = warehouseStatus.getGroup();
      if (warehouseOwner.equals(cmFs.getFileStatus(cmroot).getOwner())) {
        FsAction whOwnerAction = warehouseStatus.getPermission().getUserAction();
        FsAction whGroupAction = warehouseStatus.getPermission().getGroupAction();
        FsAction whOtherAction = warehouseStatus.getPermission().getOtherAction();
        if(!warehouseGroup.equals(cmFs.getFileStatus(cmroot).getGroup())) {
          //change group to wh-group.
          //since cmRoot owner is already part of wh-group, this can be done.
          cmFs.setOwner(cmroot, null, warehouseGroup);
          cmFs.setPermission(cmroot, new FsPermission(whOwnerAction, whGroupAction, whOtherAction));
        }
      } else {
        LOG.warn("Metastore-user is not same as owner of warehouse.");
        if(!warehouseGroup.equals(cmFs.getFileStatus(cmroot).getGroup())) {
          List<AclEntry> aclList = Lists.newArrayList(
                  new AclEntry.Builder().setScope(ACCESS).setType(USER).setPermission(FsAction.ALL).build(),
                  new AclEntry.Builder().setScope(ACCESS).setType(GROUP).setPermission(FsAction.ALL).build(),
                  new AclEntry.Builder().setScope(ACCESS).setType(OTHER).setPermission(FsAction.NONE).build());
          aclList.add(new AclEntry.Builder().setScope(ACCESS).setType(GROUP).setName(warehouseGroup).
                  setPermission(warehouseStatus.getPermission().getGroupAction()).build());
          cmFs.setAcl(cmroot, aclList);
        }
      }
    } catch (RuntimeException | IOException e) {
      LOG.error("Unable to set permissions corresponding to hive-warehouse on CMRoot: ", e);
    }
  }

  @VisibleForTesting
  static void resetReplChangeManagerInstance() {
    inited = false;
    enabled = false;
    instance = null;
    encryptionZoneToCmrootMapping.clear();
  }

  public static final PathFilter CMROOT_PATH_FILTER = new PathFilter() {
    @Override
    public boolean accept(Path p) {
      if (enabled) {
        String name = p.getName();
        return StringUtils.isEmpty(fallbackNonEncryptedCmRootDir)
                ? (!name.contains(cmRootDir) && !name.contains(encryptedCmRootDir))
                : (!name.contains(cmRootDir) && !name.contains(encryptedCmRootDir)
                && !name.contains(fallbackNonEncryptedCmRootDir));
      }
      return true;
    }
  };
}
