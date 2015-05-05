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

package org.apache.hadoop.fs;

import java.io.IOException;
import java.security.AccessControlException;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

import javax.security.auth.login.LoginException;

import com.google.common.collect.Iterators;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * Implements the default file access logic for HadoopShims.checkFileAccess(), for Hadoop
 * versions which do not implement FileSystem.access().
 *
 */
public class DefaultFileAccess {

  private static Log LOG = LogFactory.getLog(DefaultFileAccess.class);

  private static List<String> emptyGroups = Collections.emptyList();

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
    checkFileAccess(fs, Iterators.singletonIterator(stat), EnumSet.of(action), user, groups);
  }

  public static void checkFileAccess(FileSystem fs, Iterator<FileStatus> statuses, EnumSet<FsAction> actions,
                                     String user, List<String> groups)
    throws IOException, AccessControlException {

    if (groups == null) {
      groups = emptyGroups;
    }

    // Short-circuit for super-users.
    String superGroupName = getSuperGroupName(fs.getConf());
    if (userBelongsToSuperGroup(superGroupName, groups)) {
      LOG.info("User \"" + user + "\" belongs to super-group \"" + superGroupName + "\". " +
          "Permission granted for actions: " + actions + ".");
      return;
    }

    while (statuses.hasNext()) {

      FileStatus stat = statuses.next();
      final FsPermission dirPerms = stat.getPermission();
      final String grp = stat.getGroup();

      FsAction combinedAction = combine(actions);
      if (user.equals(stat.getOwner())) {
        if (dirPerms.getUserAction().implies(combinedAction)) {
          continue;
        }
      } else if (groups.contains(grp)) {
        if (dirPerms.getGroupAction().implies(combinedAction)) {
          continue;
        }
      } else if (dirPerms.getOtherAction().implies(combinedAction)) {
        continue;
      }

      throw new AccessControlException("action " + combinedAction + " not permitted on path "
          + stat.getPath() + " for user " + user);

    } // for_each(fileStatus);
  }

  private static FsAction combine(EnumSet<FsAction> actions) {
    FsAction resultantAction = FsAction.NONE;
    for (FsAction action : actions) {
      resultantAction = resultantAction.or(action);
    }
    return resultantAction;
  }

  public static void checkFileAccess(FileSystem fs, Iterator<FileStatus> statuses, EnumSet<FsAction> actions)
    throws IOException, AccessControlException, LoginException {
    UserGroupInformation ugi = Utils.getUGI();
    checkFileAccess(fs, statuses, actions, ugi.getShortUserName(), Arrays.asList(ugi.getGroupNames()));
  }

  private static String getSuperGroupName(Configuration configuration) {
    return configuration.get("dfs.permissions.supergroup", "");
  }

  private static boolean userBelongsToSuperGroup(String superGroupName, List<String> groups) {
    return groups.contains(superGroupName);
  }
}
