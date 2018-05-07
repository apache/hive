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

package org.apache.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

import javax.security.auth.login.LoginException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Implements the default file access logic for HadoopShims.checkFileAccess(), for Hadoop
 * versions which do not implement FileSystem.access().
 *
 */
public class DefaultFileAccess {

  private static Logger LOG = LoggerFactory.getLogger(DefaultFileAccess.class);

  private static List<String> emptyGroups = new ArrayList<String>(0);

  public static void checkFileAccess(FileSystem fs, FileStatus stat, FsAction action)
      throws IOException, AccessControlException, LoginException {
    // Get the user/groups for checking permissions based on the current UGI.
    UserGroupInformation currentUgi = Utils.getUGI();
    DefaultFileAccess.checkFileAccess(fs, stat, action,
        currentUgi.getShortUserName(),
        Arrays.asList(currentUgi.getGroupNames()));
  }

  public static void checkFileAccess(FileSystem fs, FileStatus stat, FsAction action,
      String user, List<String> groups) throws IOException, AccessControlException {

    if (groups == null) {
      groups = emptyGroups;
    }

    String superGroupName = getSuperGroupName(fs.getConf());
    if (userBelongsToSuperGroup(superGroupName, groups)) {
      LOG.info("User \"" + user + "\" belongs to super-group \"" + superGroupName + "\". " +
          "Permission granted for action: " + action + ".");
      return;
    }

    final FsPermission dirPerms = stat.getPermission();
    final String grp = stat.getGroup();

    if (user.equals(stat.getOwner())) {
      if (dirPerms.getUserAction().implies(action)) {
        return;
      }
    } else if (groups.contains(grp)) {
      if (dirPerms.getGroupAction().implies(action)) {
        return;
      }
    } else if (dirPerms.getOtherAction().implies(action)) {
      return;
    }
    throw new AccessControlException("action " + action + " not permitted on path "
        + stat.getPath() + " for user " + user);
  }

  private static String getSuperGroupName(Configuration configuration) {
    return configuration.get("dfs.permissions.supergroup", "");
  }

  private static boolean userBelongsToSuperGroup(String superGroupName, List<String> groups) {
    return groups.contains(superGroupName);
  }
}
