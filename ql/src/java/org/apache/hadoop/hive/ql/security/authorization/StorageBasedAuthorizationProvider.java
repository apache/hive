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

package org.apache.hadoop.hive.ql.security.authorization;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.AccessControlException;
import java.util.EnumSet;
import java.util.List;

import javax.security.auth.login.LoginException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;

/**
 * StorageBasedAuthorizationProvider is an implementation of
 * HiveMetastoreAuthorizationProvider that tries to look at the hdfs
 * permissions of files and directories associated with objects like
 * databases, tables and partitions to determine whether or not an
 * operation is allowed. The rule of thumb for which location to check
 * in hdfs is as follows:
 *
 * CREATE : on location specified, or on location determined from metadata
 * READS : not checked (the preeventlistener does not have an event to fire)
 * UPDATES : on location in metadata
 * DELETES : on location in metadata
 *
 * If the location does not yet exist, as the case is with creates, it steps
 * out to the parent directory recursively to determine its permissions till
 * it finds a parent that does exist.
 */
public class StorageBasedAuthorizationProvider extends HiveAuthorizationProviderBase
    implements HiveMetastoreAuthorizationProvider {

  private Warehouse wh;

  @Override
  public void init(Configuration conf) throws HiveException {
    hive_db = new HiveProxy();
  }

  @Override
  public void authorize(Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {
    // Currently not used in hive code-base, but intended to authorize actions
    // that are directly user-level. As there's no storage based aspect to this,
    // we can follow one of two routes:
    // a) We can allow by default - that way, this call stays out of the way
    // b) We can deny by default - that way, no privileges are authorized that
    // is not understood and explicitly allowed.
    // Both approaches have merit, but given that things like grants and revokes
    // that are user-level do not make sense from the context of storage-permission
    // based auth, denying seems to be more canonical here.

    throw new AuthorizationException(StorageBasedAuthorizationProvider.class.getName() +
        " does not allow user-level authorization");
  }

  @Override
  public void authorize(Database db, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {
    Path path = getDbLocation(db);
    authorize(path, readRequiredPriv, writeRequiredPriv);
  }

  @Override
  public void authorize(Table table, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {

    // Table path can be null in the case of a new create table - in this case,
    // we try to determine what the path would be after the create table is issued.
    Path path = null;
    try {
      String location = table.getTTable().getSd().getLocation();
      if (location == null || location.isEmpty()) {
        path = wh.getTablePath(hive_db.getDatabase(table.getDbName()), table.getTableName());
      } else {
        path = new Path(location);
      }
    } catch (MetaException ex) {
      throw hiveException(ex);
    }

    authorize(path, readRequiredPriv, writeRequiredPriv);
  }

  @Override
  public void authorize(Partition part, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {
    authorize(part.getTable(), part, readRequiredPriv, writeRequiredPriv);
  }

  private void authorize(Table table, Partition part, Privilege[] readRequiredPriv,
      Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {

    // Partition path can be null in the case of a new create partition - in this case,
    // we try to default to checking the permissions of the parent table
    if (part.getLocation() == null) {
      authorize(table, readRequiredPriv, writeRequiredPriv);
    } else {
      authorize(part.getPartitionPath(), readRequiredPriv, writeRequiredPriv);
    }
  }

  @Override
  public void authorize(Table table, Partition part, List<String> columns,
      Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv) throws HiveException,
      AuthorizationException {
    // In a simple storage-based auth, we have no information about columns
    // living in different files, so we do simple partition-auth and ignore
    // the columns parameter.

    authorize(part.getTable(), part, readRequiredPriv, writeRequiredPriv);
  }

  @Override
  public void setMetaStoreHandler(HMSHandler handler) {
    hive_db.setHandler(handler);
    this.wh = handler.getWh();
  }

  /**
   * Given a privilege, return what FsActions are required
   */
  protected FsAction getFsAction(Privilege priv) {

    switch (priv.getPriv()) {
    case ALL:
      return FsAction.READ_WRITE;
    case ALTER_DATA:
      return FsAction.WRITE;
    case ALTER_METADATA:
      return FsAction.WRITE;
    case CREATE:
      return FsAction.WRITE;
    case DROP:
      return FsAction.WRITE;
    case INDEX:
      throw new AuthorizationException(
          "StorageBasedAuthorizationProvider cannot handle INDEX privilege");
    case LOCK:
      throw new AuthorizationException(
          "StorageBasedAuthorizationProvider cannot handle LOCK privilege");
    case SELECT:
      return FsAction.READ;
    case SHOW_DATABASE:
      return FsAction.READ;
    case UNKNOWN:
    default:
      throw new AuthorizationException("Unknown privilege");
    }
  }

  /**
   * Given a Privilege[], find out what all FsActions are required
   */
  protected EnumSet<FsAction> getFsActions(Privilege[] privs) {
    EnumSet<FsAction> actions = EnumSet.noneOf(FsAction.class);

    if (privs == null) {
      return actions;
    }

    for (Privilege priv : privs) {
      actions.add(getFsAction(priv));
    }

    return actions;
  }

  /**
   * Authorization privileges against a path.
   *
   * @param path
   *          a filesystem path
   * @param readRequiredPriv
   *          a list of privileges needed for inputs.
   * @param writeRequiredPriv
   *          a list of privileges needed for outputs.
   */
  public void authorize(Path path, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
      throws HiveException, AuthorizationException {
    try {
      EnumSet<FsAction> actions = getFsActions(readRequiredPriv);
      actions.addAll(getFsActions(writeRequiredPriv));
      if (actions.isEmpty()) {
        return;
      }

      checkPermissions(getConf(), path, actions);

    } catch (AccessControlException ex) {
      throw authorizationException(ex);
    } catch (LoginException ex) {
      throw authorizationException(ex);
    } catch (IOException ex) {
      throw hiveException(ex);
    }
  }


  /**
   * Checks the permissions for the given path and current user on Hadoop FS.
   * If the given path does not exists, it checks for its parent folder.
   */
  protected void checkPermissions(final Configuration conf, final Path path,
      final EnumSet<FsAction> actions) throws IOException, LoginException {

    if (path == null) {
      throw new IllegalArgumentException("path is null");
    }

    final FileSystem fs = path.getFileSystem(conf);

    if (fs.exists(path)) {
      checkPermissions(fs, path, actions,
          authenticator.getUserName(), authenticator.getGroupNames());
    } else if (path.getParent() != null) {
      // find the ancestor which exists to check its permissions
      Path par = path.getParent();
      while (par != null) {
        if (fs.exists(par)) {
          break;
        }
        par = par.getParent();
      }

      checkPermissions(fs, par, actions,
          authenticator.getUserName(), authenticator.getGroupNames());
    }
  }

  /**
   * Checks the permissions for the given path and current user on Hadoop FS. If the given path
   * does not exists, it returns.
   */
  @SuppressWarnings("deprecation")
  protected static void checkPermissions(final FileSystem fs, final Path path,
      final EnumSet<FsAction> actions, String user, List<String> groups) throws IOException,
      AccessControlException {

    final FileStatus stat;

    try {
      stat = fs.getFileStatus(path);
    } catch (FileNotFoundException fnfe) {
      // File named by path doesn't exist; nothing to validate.
      return;
    } catch (org.apache.hadoop.fs.permission.AccessControlException ace) {
      // Older hadoop version will throw this @deprecated Exception.
      throw accessControlException(ace);
    }

    final FsPermission dirPerms = stat.getPermission();
    final String grp = stat.getGroup();

    for (FsAction action : actions) {
      if (user.equals(stat.getOwner())) {
        if (dirPerms.getUserAction().implies(action)) {
          continue;
        }
      }
      if (groups.contains(grp)) {
        if (dirPerms.getGroupAction().implies(action)) {
          continue;
        }
      }
      if (dirPerms.getOtherAction().implies(action)) {
        continue;
      }
      throw new AccessControlException("action " + action + " not permitted on path "
          + path + " for user " + user);
    }
  }

  protected Path getDbLocation(Database db) throws HiveException {
    try {
      String location = db.getLocationUri();
      if (location == null) {
        return wh.getDefaultDatabasePath(db.getName());
      } else {
        return wh.getDnsPath(wh.getDatabasePath(db));
      }
    } catch (MetaException ex) {
      throw hiveException(ex);
    }
  }

  private HiveException hiveException(Exception e) {
    HiveException ex = new HiveException(e);
    ex.initCause(e);
    return ex;
  }

  private AuthorizationException authorizationException(Exception e) {
    AuthorizationException ex = new AuthorizationException(e);
    ex.initCause(e);
    return ex;
  }

  private static AccessControlException accessControlException(
      org.apache.hadoop.fs.permission.AccessControlException e) {
    AccessControlException ace = new AccessControlException(e.getMessage());
    ace.initCause(e);
    return ace;
  }

}
