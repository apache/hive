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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hcatalog.security;

import static org.apache.hadoop.hive.metastore.MetaStoreUtils.DEFAULT_DATABASE_NAME;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;

import javax.security.auth.login.LoginException;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProviderBase;
import org.apache.hadoop.hive.ql.security.authorization.Privilege;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.security.UserGroupInformation;

/**
 * An AuthorizationProvider, which checks against the data access level permissions on HDFS.
 * It makes sense to eventually move this class to Hive, so that all hive users can
 * use this authorization model. 
 * @deprecated use {@link org.apache.hadoop.hive.ql.security.authorization.StorageBasedAuthorizationProvider}
 */
public class HdfsAuthorizationProvider extends HiveAuthorizationProviderBase {

  protected Warehouse wh;

  //Config variables : create an enum to store them if we have more
  private static final String PROXY_USER_NAME = "proxy.user.name";

  public HdfsAuthorizationProvider() {
    super();
  }

  public HdfsAuthorizationProvider(Configuration conf) {
    super();
    setConf(conf);
  }

  @Override
  public void init(Configuration conf) throws HiveException {
    hive_db = new HiveProxy(Hive.get(new HiveConf(conf, HiveAuthorizationProvider.class)));
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    try {
      this.wh = new Warehouse(conf);
    } catch (MetaException ex) {
      throw new RuntimeException(ex);
    }
  }

  protected FsAction getFsAction(Privilege priv, Path path) {

    switch (priv.getPriv()) {
    case ALL:
      throw new AuthorizationException("no matching Action for Privilege.All");
    case ALTER_DATA:
      return FsAction.WRITE;
    case ALTER_METADATA:
      return FsAction.WRITE;
    case CREATE:
      return FsAction.WRITE;
    case DROP:
      return FsAction.WRITE;
    case INDEX:
      return FsAction.WRITE;
    case LOCK:
      return FsAction.WRITE;
    case SELECT:
      return FsAction.READ;
    case SHOW_DATABASE:
      return FsAction.READ;
    case UNKNOWN:
    default:
      throw new AuthorizationException("Unknown privilege");
    }
  }

  protected EnumSet<FsAction> getFsActions(Privilege[] privs, Path path) {
    EnumSet<FsAction> actions = EnumSet.noneOf(FsAction.class);

    if (privs == null) {
      return actions;
    }

    for (Privilege priv : privs) {
      actions.add(getFsAction(priv, path));
    }

    return actions;
  }

  private static final String DATABASE_WAREHOUSE_SUFFIX = ".db";

  private Path getDefaultDatabasePath(String dbName) throws MetaException {
    if (dbName.equalsIgnoreCase(DEFAULT_DATABASE_NAME)) {
      return wh.getWhRoot();
    }
    return new Path(wh.getWhRoot(), dbName.toLowerCase() + DATABASE_WAREHOUSE_SUFFIX);
  }

  protected Path getDbLocation(Database db) throws HiveException {
    try {
      String location = db.getLocationUri();
      if (location == null) {
        return getDefaultDatabasePath(db.getName());
      } else {
        return wh.getDnsPath(wh.getDatabasePath(db));
      }
    } catch (MetaException ex) {
      throw new HiveException(ex.getMessage());
    }
  }

  @Override
  public void authorize(Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
    throws HiveException, AuthorizationException {
    //Authorize for global level permissions at the warehouse dir
    Path root;
    try {
      root = wh.getWhRoot();
      authorize(root, readRequiredPriv, writeRequiredPriv);
    } catch (MetaException ex) {
      throw new HiveException(ex);
    }
  }

  @Override
  public void authorize(Database db, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
    throws HiveException, AuthorizationException {
    if (db == null) {
      return;
    }

    Path path = getDbLocation(db);

    authorize(path, readRequiredPriv, writeRequiredPriv);
  }

  @Override
  public void authorize(Table table, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
    throws HiveException, AuthorizationException {
    if (table == null) {
      return;
    }

    //unlike Hive's model, this can be called at CREATE TABLE as well, since we should authorize
    //against the table's declared location
    Path path = null;
    try {
      if (table.getTTable().getSd().getLocation() == null
        || table.getTTable().getSd().getLocation().isEmpty()) {
        path = wh.getTablePath(hive_db.getDatabase(table.getDbName()), table.getTableName());
      } else {
        path = table.getPath();
      }
    } catch (MetaException ex) {
      throw new HiveException(ex);
    }

    authorize(path, readRequiredPriv, writeRequiredPriv);
  }

  //TODO: HiveAuthorizationProvider should expose this interface instead of #authorize(Partition, Privilege[], Privilege[])
  public void authorize(Table table, Partition part, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
    throws HiveException, AuthorizationException {

    if (part == null || part.getLocation() == null) {
      authorize(table, readRequiredPriv, writeRequiredPriv);
    } else {
      authorize(part.getPartitionPath(), readRequiredPriv, writeRequiredPriv);
    }
  }

  @Override
  public void authorize(Partition part, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
    throws HiveException, AuthorizationException {
    if (part == null) {
      return;
    }
    authorize(part.getTable(), part, readRequiredPriv, writeRequiredPriv);
  }

  @Override
  public void authorize(Table table, Partition part, List<String> columns,
              Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv) throws HiveException,
    AuthorizationException {
    //columns cannot live in different files, just check for partition level permissions
    authorize(table, part, readRequiredPriv, writeRequiredPriv);
  }

  /**
   * Authorization privileges against a path.
   * @param path a filesystem path
   * @param readRequiredPriv a list of privileges needed for inputs.
   * @param writeRequiredPriv a list of privileges needed for outputs.
   */
  public void authorize(Path path, Privilege[] readRequiredPriv, Privilege[] writeRequiredPriv)
    throws HiveException, AuthorizationException {
    try {
      EnumSet<FsAction> actions = getFsActions(readRequiredPriv, path);
      actions.addAll(getFsActions(writeRequiredPriv, path));
      if (actions.isEmpty()) {
        return;
      }

      checkPermissions(getConf(), path, actions);

    } catch (AccessControlException ex) {
      throw new AuthorizationException(ex);
    } catch (LoginException ex) {
      throw new AuthorizationException(ex);
    } catch (IOException ex) {
      throw new HiveException(ex);
    }
  }

  /**
   * Checks the permissions for the given path and current user on Hadoop FS. If the given path
   * does not exists, it checks for it's parent folder.
   */
  protected static void checkPermissions(final Configuration conf, final Path path,
                       final EnumSet<FsAction> actions) throws IOException, LoginException {

    if (path == null) {
      throw new IllegalArgumentException("path is null");
    }

    HadoopShims shims = ShimLoader.getHadoopShims();
    final UserGroupInformation ugi;
    if (conf.get(PROXY_USER_NAME) != null) {
      ugi = UserGroupInformation.createRemoteUser(conf.get(PROXY_USER_NAME));
    } else {
      ugi = shims.getUGIForConf(conf);
    }
    final String user = shims.getShortUserName(ugi);

    final FileSystem fs = path.getFileSystem(conf);

    if (fs.exists(path)) {
      checkPermissions(fs, path, actions, user, ugi.getGroupNames());
    } else if (path.getParent() != null) {
      // find the ancestor which exists to check it's permissions
      Path par = path.getParent();
      while (par != null) {
        if (fs.exists(par)) {
          break;
        }
        par = par.getParent();
      }

      checkPermissions(fs, par, actions, user, ugi.getGroupNames());
    }
  }

  /**
   * Checks the permissions for the given path and current user on Hadoop FS. If the given path
   * does not exists, it returns.
   */
  @SuppressWarnings("deprecation")
  protected static void checkPermissions(final FileSystem fs, final Path path,
                       final EnumSet<FsAction> actions, String user, String[] groups) throws IOException,
    AccessControlException {

    final FileStatus stat;

    try {
      stat = fs.getFileStatus(path);
    } catch (FileNotFoundException fnfe) {
      // File named by path doesn't exist; nothing to validate.
      return;
    } catch (org.apache.hadoop.fs.permission.AccessControlException ace) {
      // Older hadoop version will throw this @deprecated Exception.
      throw new AccessControlException(ace.getMessage());
    }

    final FsPermission dirPerms = stat.getPermission();
    final String grp = stat.getGroup();

    for (FsAction action : actions) {
      if (user.equals(stat.getOwner())) {
        if (dirPerms.getUserAction().implies(action)) {
          continue;
        }
      }
      if (ArrayUtils.contains(groups, grp)) {
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
}
