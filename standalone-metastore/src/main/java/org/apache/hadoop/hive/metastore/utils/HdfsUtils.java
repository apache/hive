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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.LoginException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HdfsUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsUtils.class);
  private static final String DISTCP_OPTIONS_PREFIX = "distcp.options.";

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
    DistCpOptions options = new DistCpOptions(srcPaths, dst);
    options.setSyncFolder(true);
    options.setSkipCRC(true);
    options.preserve(DistCpOptions.FileAttribute.BLOCKSIZE);

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

}
