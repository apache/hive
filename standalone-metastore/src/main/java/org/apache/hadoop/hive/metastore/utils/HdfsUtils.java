/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.AclEntry;
import org.apache.hadoop.fs.permission.AclEntryScope;
import org.apache.hadoop.fs.permission.AclEntryType;
import org.apache.hadoop.fs.permission.AclStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.apache.hadoop.tools.DistCpOptions.FileAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HdfsUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsUtils.class);
  private static final String DISTCP_OPTIONS_PREFIX = "distcp.options.";
  // TODO: this relies on HDFS not changing the format; we assume if we could get inode ID, this
  //       is still going to work. Otherwise, file IDs can be turned off. Later, we should use
  //       as public utility method in HDFS to obtain the inode-based path.
  private static final String HDFS_ID_PATH_PREFIX = "/.reserved/.inodes/";

  /**
   * Check the permissions on a file.
   * @param fs Filesystem the file is contained in
   * @param stat Stat info for the file
   * @param action action to be performed
   * @throws IOException If thrown by Hadoop
   * @throws AccessControlException if the file cannot be accessed
   */
  public static void checkFileAccess(FileSystem fs, FileStatus stat, FsAction action)
      throws IOException, LoginException {
    checkFileAccess(fs, stat, action, SecurityUtils.getUGI());
  }

  /**
   * Check the permissions on a file
   * @param fs Filesystem the file is contained in
   * @param stat Stat info for the file
   * @param action action to be performed
   * @param ugi user group info for the current user.  This is passed in so that tests can pass
   *            in mock ones.
   * @throws IOException If thrown by Hadoop
   * @throws AccessControlException if the file cannot be accessed
   */
  @VisibleForTesting
  static void checkFileAccess(FileSystem fs, FileStatus stat, FsAction action,
                              UserGroupInformation ugi) throws IOException {

    String user = ugi.getShortUserName();
    String[] groups = ugi.getGroupNames();

    if (groups != null) {
      String superGroupName = fs.getConf().get("dfs.permissions.supergroup", "");
      if (arrayContains(groups, superGroupName)) {
        LOG.debug("User \"" + user + "\" belongs to super-group \"" + superGroupName + "\". " +
            "Permission granted for action: " + action + ".");
        return;
      }
    }

    FsPermission dirPerms = stat.getPermission();

    if (user.equals(stat.getOwner())) {
      if (dirPerms.getUserAction().implies(action)) {
        return;
      }
    } else if (arrayContains(groups, stat.getGroup())) {
      if (dirPerms.getGroupAction().implies(action)) {
        return;
      }
    } else if (dirPerms.getOtherAction().implies(action)) {
      return;
    }
    throw new AccessControlException("action " + action + " not permitted on path "
        + stat.getPath() + " for user " + user);
  }

  public static boolean isPathEncrypted(Configuration conf, URI fsUri, Path path)
      throws IOException {
    Path fullPath;
    if (path.isAbsolute()) {
      fullPath = path;
    } else {
      fullPath = path.getFileSystem(conf).makeQualified(path);
    }
    if(!"hdfs".equalsIgnoreCase(path.toUri().getScheme())) {
      return false;
    }
    try {
      HdfsAdmin hdfsAdmin = new HdfsAdmin(fsUri, conf);
      return (hdfsAdmin.getEncryptionZoneForPath(fullPath) != null);
    } catch (FileNotFoundException fnfe) {
      LOG.debug("Failed to get EZ for non-existent path: "+ fullPath, fnfe);
      return false;
    }
  }

  private static boolean arrayContains(String[] array, String value) {
    if (array == null) return false;
    for (String element : array) {
      if (element.equals(value)) return true;
    }
    return false;
  }

  public static boolean runDistCpAs(List<Path> srcPaths, Path dst, Configuration conf,
                                    String doAsUser) throws IOException {
    UserGroupInformation proxyUser = UserGroupInformation.createProxyUser(
        doAsUser, UserGroupInformation.getLoginUser());
    try {
      return proxyUser.doAs(new PrivilegedExceptionAction<Boolean>() {
        @Override
        public Boolean run() throws Exception {
          return runDistCp(srcPaths, dst, conf);
        }
      });
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  public static boolean runDistCp(List<Path> srcPaths, Path dst, Configuration conf)
      throws IOException {
    DistCpOptions options = new DistCpOptions.Builder(srcPaths, dst)
        .withSyncFolder(true)
        .withCRC(true)
        .preserve(FileAttribute.BLOCKSIZE)
        .build();

    // Creates the command-line parameters for distcp
    List<String> params = constructDistCpParams(srcPaths, dst, conf);

    try {
      conf.setBoolean("mapred.mapper.new-api", true);
      DistCp distcp = new DistCp(conf, options);

      // HIVE-13704 states that we should use run() instead of execute() due to a hadoop known issue
      // added by HADOOP-10459
      if (distcp.run(params.toArray(new String[params.size()])) == 0) {
        return true;
      } else {
        return false;
      }
    } catch (Exception e) {
      throw new IOException("Cannot execute DistCp process: " + e, e);
    } finally {
      conf.setBoolean("mapred.mapper.new-api", false);
    }
  }

  private static List<String> constructDistCpParams(List<Path> srcPaths, Path dst,
                                                    Configuration conf) {
    List<String> params = new ArrayList<>();
    for (Map.Entry<String,String> entry : conf.getPropsWithPrefix(DISTCP_OPTIONS_PREFIX).entrySet()){
      String distCpOption = entry.getKey();
      String distCpVal = entry.getValue();
      params.add("-" + distCpOption);
      if ((distCpVal != null) && (!distCpVal.isEmpty())){
        params.add(distCpVal);
      }
    }
    if (params.size() == 0){
      // if no entries were added via conf, we initiate our defaults
      params.add("-update");
      params.add("-skipcrccheck");
      params.add("-pb");
    }
    for (Path src : srcPaths) {
      params.add(src.toString());
    }
    params.add(dst.toString());
    return params;
  }

  public static Path getFileIdPath(
      FileSystem fileSystem, Path path, long fileId) {
    return (fileSystem instanceof DistributedFileSystem)
        ? new Path(HDFS_ID_PATH_PREFIX + fileId) : path;
  }

  public static long getFileId(FileSystem fs, String path) throws IOException {
    return ensureDfs(fs).getClient().getFileInfo(path).getFileId();
  }

  private static DistributedFileSystem ensureDfs(FileSystem fs) {
    if (!(fs instanceof DistributedFileSystem)) {
      throw new UnsupportedOperationException("Only supported for DFS; got " + fs.getClass());
    }
    return (DistributedFileSystem)fs;
  }

  public static class HadoopFileStatus {

    private final FileStatus fileStatus;
    private final AclStatus aclStatus;

    public HadoopFileStatus(Configuration conf, FileSystem fs, Path file) throws IOException {

      FileStatus fileStatus = fs.getFileStatus(file);
      AclStatus aclStatus = null;
      if (Objects.equal(conf.get("dfs.namenode.acls.enabled"), "true")) {
        //Attempt extended Acl operations only if its enabled, but don't fail the operation regardless.
        try {
          aclStatus = fs.getAclStatus(file);
        } catch (Exception e) {
          LOG.info("Skipping ACL inheritance: File system for path " + file + " " +
              "does not support ACLs but dfs.namenode.acls.enabled is set to true. ");
          LOG.debug("The details are: " + e, e);
        }
      }this.fileStatus = fileStatus;
      this.aclStatus = aclStatus;
    }

    public FileStatus getFileStatus() {
      return fileStatus;
    }

    List<AclEntry> getAclEntries() {
      return aclStatus == null ? null : Collections.unmodifiableList(aclStatus.getEntries());
    }

    @VisibleForTesting
    AclStatus getAclStatus() {
      return this.aclStatus;
    }
  }

  /**
   * Copy the permissions, group, and ACLs from a source {@link HadoopFileStatus} to a target {@link Path}. This method
   * will only log a warning if permissions cannot be set, no exception will be thrown.
   *
   * @param conf the {@link Configuration} used when setting permissions and ACLs
   * @param sourceStatus the source {@link HadoopFileStatus} to copy permissions and ACLs from
   * @param targetGroup the group of the target {@link Path}, if this is set and it is equal to the source group, an
   *                    extra set group operation is avoided
   * @param fs the {@link FileSystem} that contains the target {@link Path}
   * @param target the {@link Path} to copy permissions, group, and ACLs to
   * @param recursion recursively set permissions and ACLs on the target {@link Path}
   */
  public static void setFullFileStatus(Configuration conf, HdfsUtils.HadoopFileStatus sourceStatus,
                                       String targetGroup, FileSystem fs, Path target, boolean recursion) {
    setFullFileStatus(conf, sourceStatus, targetGroup, fs, target, recursion, recursion ? new FsShell() : null);
  }

  @VisibleForTesting
  static void setFullFileStatus(Configuration conf, HdfsUtils.HadoopFileStatus sourceStatus,
                                String targetGroup, FileSystem fs, Path target, boolean recursion, FsShell fsShell) {
    try {
      FileStatus fStatus = sourceStatus.getFileStatus();
      String group = fStatus.getGroup();
      boolean aclEnabled = Objects.equal(conf.get("dfs.namenode.acls.enabled"), "true");
      FsPermission sourcePerm = fStatus.getPermission();
      List<AclEntry> aclEntries = null;
      if (aclEnabled) {
        if (sourceStatus.getAclEntries() != null) {
          LOG.trace(sourceStatus.getAclStatus().toString());
          aclEntries = new ArrayList<>(sourceStatus.getAclEntries());
          removeBaseAclEntries(aclEntries);

          //the ACL api's also expect the tradition user/group/other permission in the form of ACL
          aclEntries.add(newAclEntry(AclEntryScope.ACCESS, AclEntryType.USER, sourcePerm.getUserAction()));
          aclEntries.add(newAclEntry(AclEntryScope.ACCESS, AclEntryType.GROUP, sourcePerm.getGroupAction()));
          aclEntries.add(newAclEntry(AclEntryScope.ACCESS, AclEntryType.OTHER, sourcePerm.getOtherAction()));
        }
      }

      if (recursion) {
        //use FsShell to change group, permissions, and extended ACL's recursively
        fsShell.setConf(conf);
        //If there is no group of a file, no need to call chgrp
        if (group != null && !group.isEmpty()) {
          run(fsShell, new String[]{"-chgrp", "-R", group, target.toString()});
        }
        if (aclEnabled) {
          if (null != aclEntries) {
            //Attempt extended Acl operations only if its enabled, 8791but don't fail the operation regardless.
            try {
              //construct the -setfacl command
              String aclEntry = Joiner.on(",").join(aclEntries);
              run(fsShell, new String[]{"-setfacl", "-R", "--set", aclEntry, target.toString()});

            } catch (Exception e) {
              LOG.info("Skipping ACL inheritance: File system for path " + target + " " +
                  "does not support ACLs but dfs.namenode.acls.enabled is set to true. ");
              LOG.debug("The details are: " + e, e);
            }
          }
        } else {
          String permission = Integer.toString(sourcePerm.toShort(), 8);
          run(fsShell, new String[]{"-chmod", "-R", permission, target.toString()});
        }
      } else {
        if (group != null && !group.isEmpty()) {
          if (targetGroup == null ||
              !group.equals(targetGroup)) {
            fs.setOwner(target, null, group);
          }
        }
        if (aclEnabled) {
          if (null != aclEntries) {
            fs.setAcl(target, aclEntries);
          }
        } else {
          fs.setPermission(target, sourcePerm);
        }
      }
    } catch (Exception e) {
      LOG.warn(
          "Unable to inherit permissions for file " + target + " from file " + sourceStatus.getFileStatus().getPath(),
          e.getMessage());
      LOG.debug("Exception while inheriting permissions", e);
    }
  }

  /**
   * Removes basic permission acls (unamed acls) from the list of acl entries
   * @param entries acl entries to remove from.
   */
  private static void removeBaseAclEntries(List<AclEntry> entries) {
    Iterables.removeIf(entries, new Predicate<AclEntry>() {
      @Override
      public boolean apply(AclEntry input) {
        if (input.getName() == null) {
          return true;
        }
        return false;
      }
    });
  }

  /**
   * Create a new AclEntry with scope, type and permission (no name).
   *
   * @param scope
   *          AclEntryScope scope of the ACL entry
   * @param type
   *          AclEntryType ACL entry type
   * @param permission
   *          FsAction set of permissions in the ACL entry
   * @return AclEntry new AclEntry
   */
  private static AclEntry newAclEntry(AclEntryScope scope, AclEntryType type,
                                      FsAction permission) {
    return new AclEntry.Builder().setScope(scope).setType(type)
        .setPermission(permission).build();
  }

  private static void run(FsShell shell, String[] command) throws Exception {
    LOG.debug(ArrayUtils.toString(command));
    int retval = shell.run(command);
    LOG.debug("Return value is :" + retval);
  }
}
