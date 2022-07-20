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
package org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.shims.Utils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext.CLIENT_TYPE;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.thrift.TException;

public class SQLAuthorizationUtils {

  private static final String[] SUPPORTED_PRIVS = { "INSERT", "UPDATE", "DELETE", "SELECT" };
  private static final Set<String> SUPPORTED_PRIVS_SET = new HashSet<String>(
      Arrays.asList(SUPPORTED_PRIVS));
  public static final Logger LOG = LoggerFactory.getLogger(SQLAuthorizationUtils.class);

  /**
   * Create thrift privileges bag
   *
   * @param hivePrincipals
   * @param hivePrivileges
   * @param hivePrivObject
   * @param grantorPrincipal
   * @param grantOption
   * @return
   * @throws HiveAuthzPluginException
   */
  static PrivilegeBag getThriftPrivilegesBag(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthzPluginException {
    HiveObjectRef privObj = getThriftHiveObjectRef(hivePrivObject);
    PrivilegeBag privBag = new PrivilegeBag();
    for (HivePrivilege privilege : hivePrivileges) {
      if (privilege.getColumns() != null && privilege.getColumns().size() > 0) {
        throw new HiveAuthzPluginException("Privileges on columns not supported currently"
            + " in sql standard authorization mode");
      }
      if (!SUPPORTED_PRIVS_SET.contains(privilege.getName().toUpperCase(Locale.US))) {
        throw new HiveAuthzPluginException("Privilege: " + privilege.getName()
            + " is not supported in sql standard authorization mode");
      }
      PrivilegeGrantInfo grantInfo = getThriftPrivilegeGrantInfo(privilege, grantorPrincipal,
          grantOption, 0 /*real grant time added by metastore*/);
      for (HivePrincipal principal : hivePrincipals) {
        HiveObjectPrivilege objPriv = new HiveObjectPrivilege(privObj, principal.getName(),
            AuthorizationUtils.getThriftPrincipalType(principal.getType()), grantInfo, "SQL");
        privBag.addToPrivileges(objPriv);
      }
    }
    return privBag;
  }

  static PrivilegeGrantInfo getThriftPrivilegeGrantInfo(HivePrivilege privilege,
      HivePrincipal grantorPrincipal, boolean grantOption, int grantTime)
          throws HiveAuthzPluginException {
    try {
      return AuthorizationUtils.getThriftPrivilegeGrantInfo(privilege, grantorPrincipal,
          grantOption, grantTime);
    } catch (HiveException e) {
      throw new HiveAuthzPluginException(e);
    }
  }

  /**
   * Create a thrift privilege object from the plugin interface privilege object
   *
   * @param privObj
   * @return
   * @throws HiveAuthzPluginException
   */
  static HiveObjectRef getThriftHiveObjectRef(HivePrivilegeObject privObj)
      throws HiveAuthzPluginException {
    try {
      return AuthorizationUtils.getThriftHiveObjectRef(privObj);
    } catch (HiveException e) {
      throw new HiveAuthzPluginException(e);
    }
  }

  static HivePrivilegeObjectType getPluginObjType(HiveObjectType objectType)
      throws HiveAuthzPluginException {
    switch (objectType) {
    case DATABASE:
      return HivePrivilegeObjectType.DATABASE;
    case TABLE:
      return HivePrivilegeObjectType.TABLE_OR_VIEW;
    case COLUMN:
    case GLOBAL:
    case PARTITION:
      throw new HiveAuthzPluginException("Unsupported object type " + objectType);
    default:
      throw new AssertionError("Unexpected object type " + objectType);
    }
  }

  /**
   * Check if the privileges are acceptable for SQL Standard authorization implementation
   * @param hivePrivileges
   * @throws HiveAuthzPluginException
   */
  public static void validatePrivileges(List<HivePrivilege> hivePrivileges) throws HiveAuthzPluginException {
    for (HivePrivilege hivePrivilege : hivePrivileges) {
      if (hivePrivilege.getColumns() != null && hivePrivilege.getColumns().size() != 0) {
        throw new HiveAuthzPluginException(
            "Privilege with columns are not currently supported with sql standard authorization:"
                + hivePrivilege);
      }
      //try converting to the enum to verify that this is a valid privilege type
      SQLPrivilegeType.getRequirePrivilege(hivePrivilege.getName());

    }
  }

  /**
   * Get the privileges this user(userName argument) has on the object
   * (hivePrivObject argument) If isAdmin is true, adds an admin privilege as
   * well.
   *
   * @param metastoreClient
   * @param userName
   * @param hivePrivObject
   * @param curRoles
   *          current active roles for user
   * @param isAdmin
   *          if user can run as admin user
   * @param ignoreUnknown
   *          boolean flag to ignore unknown table
   * @return
   * @throws HiveAuthzPluginException
   */
  static RequiredPrivileges getPrivilegesFromMetaStore(IMetaStoreClient metastoreClient,
      String userName, HivePrivilegeObject hivePrivObject, List<String> curRoles, boolean isAdmin, boolean ignoreUnknown)
          throws HiveAuthzPluginException {

    // get privileges for this user and its role on this object
    PrincipalPrivilegeSet thrifPrivs = null;
    try {
      HiveObjectRef objectRef = AuthorizationUtils.getThriftHiveObjectRef(hivePrivObject);
      if (objectRef.getObjectType() == null) {
        objectRef.setObjectType(HiveObjectType.GLOBAL);
      }
      thrifPrivs = metastoreClient.get_privilege_set(
          objectRef, userName, null);
    } catch (MetaException e) {
      throwGetPrivErr(e, hivePrivObject, userName);
    } catch (TException e) {
      throwGetPrivErr(e, hivePrivObject, userName);
    } catch (HiveException e) {
      throwGetPrivErr(e, hivePrivObject, userName);
    }

    filterPrivsByCurrentRoles(thrifPrivs, curRoles);

    // convert to RequiredPrivileges
    RequiredPrivileges privs = getRequiredPrivsFromThrift(thrifPrivs);

    // add owner privilege if user is owner of the object
    try {
      if (isOwner(metastoreClient, userName, curRoles, hivePrivObject)) {
        privs.addPrivilege(SQLPrivTypeGrant.OWNER_PRIV);
      }
    } catch (HiveAuthzPluginException ex) {
      if (ex.getCause() instanceof NoSuchObjectException && ignoreUnknown) {
        privs.addPrivilege(SQLPrivTypeGrant.OWNER_PRIV);
      } else {
        throw ex;
      }
    }
    if (isAdmin) {
      privs.addPrivilege(SQLPrivTypeGrant.ADMIN_PRIV);
    }

    return privs;
  }

  /**
   * Remove any role privileges that don't belong to the roles in curRoles
   * @param thriftPrivs
   * @param curRoles
   * @return
   */
  private static void filterPrivsByCurrentRoles(PrincipalPrivilegeSet thriftPrivs,
      List<String> curRoles) {
    // check if there are privileges to be filtered
    if(thriftPrivs == null || thriftPrivs.getRolePrivileges() == null
        || thriftPrivs.getRolePrivilegesSize() == 0
        ){
      // no privileges to filter
      return;
    }

    // add the privs for roles in curRoles to new role-to-priv map
    Map<String, List<PrivilegeGrantInfo>> filteredRolePrivs = new HashMap<String, List<PrivilegeGrantInfo>>();
    for(String role : curRoles){
      List<PrivilegeGrantInfo> privs = thriftPrivs.getRolePrivileges().get(role);
      if(privs != null){
        filteredRolePrivs.put(role, privs);
      }
    }
    thriftPrivs.setRolePrivileges(filteredRolePrivs);
  }

  /**
   * Check if user is owner of the given object
   *
   * @param metastoreClient
   * @param userName
   *          current user
   * @param curRoles
   *          current roles for userName
   * @param hivePrivObject
   *          given object
   * @return true if user is owner
   * @throws HiveAuthzPluginException
   */
  private static boolean isOwner(IMetaStoreClient metastoreClient, String userName,
      List<String> curRoles, HivePrivilegeObject hivePrivObject) throws HiveAuthzPluginException {
    // for now, check only table & db
    switch (hivePrivObject.getType()) {
    case TABLE_OR_VIEW: {
      Table thriftTableObj = null;
      try {
        thriftTableObj = metastoreClient.getTable(hivePrivObject.getDbname(),
            hivePrivObject.getObjectName());
      } catch (Exception e) {
        throwGetObjErr(e, hivePrivObject);
      }
      return userName.equals(thriftTableObj.getOwner());
    }
    case DATABASE: {
      if (Warehouse.DEFAULT_DATABASE_NAME.equalsIgnoreCase(hivePrivObject.getDbname())) {
        return true;
      }
      Database db = null;
      try {
        db = metastoreClient.getDatabase(hivePrivObject.getDbname());
      } catch (Exception e) {
        throwGetObjErr(e, hivePrivObject);
      }
      // a db owner can be a user or a role
      if(db.getOwnerType() == PrincipalType.USER){
        return userName.equals(db.getOwnerName());
      } else if(db.getOwnerType() == PrincipalType.ROLE){
        // check if any of the roles of this user is an owner
        return curRoles.contains(db.getOwnerName());
      } else {
        // looks like owner is an unsupported type
        LOG.warn("Owner of database " + db.getName() + " is of unsupported type "
            + db.getOwnerType());
        return false;
      }
    }
    case DFS_URI:
    case LOCAL_URI:
    case PARTITION:
    default:
      return false;
    }
  }

  private static void throwGetObjErr(Exception e, HivePrivilegeObject hivePrivObject)
      throws HiveAuthzPluginException {
    String msg = "Error getting object from metastore for " + hivePrivObject;
    throw new HiveAuthzPluginException(msg, e);
  }

  private static void throwGetPrivErr(Exception e, HivePrivilegeObject hivePrivObject,
      String userName) throws HiveAuthzPluginException {
    String msg = "Error getting privileges on " + hivePrivObject + " for " + userName + ": "
      + e.getMessage();
    throw new HiveAuthzPluginException(msg, e);
  }

  private static RequiredPrivileges getRequiredPrivsFromThrift(PrincipalPrivilegeSet thrifPrivs)
      throws HiveAuthzPluginException {

    RequiredPrivileges reqPrivs = new RequiredPrivileges();
    // add user privileges
    Map<String, List<PrivilegeGrantInfo>> userPrivs = thrifPrivs.getUserPrivileges();
    if (userPrivs != null && userPrivs.size() != 1) {
      throw new HiveAuthzPluginException("Invalid number of user privilege objects: "
          + userPrivs.size());
    }
    addRequiredPrivs(reqPrivs, userPrivs);

    // add role privileges
    Map<String, List<PrivilegeGrantInfo>> rolePrivs = thrifPrivs.getRolePrivileges();
    addRequiredPrivs(reqPrivs, rolePrivs);
    return reqPrivs;
  }

  /**
   * Add privileges to RequiredPrivileges object reqPrivs from thrift availPrivs
   * object
   * @param reqPrivs
   * @param availPrivs
   * @throws HiveAuthzPluginException
   */
  private static void addRequiredPrivs(RequiredPrivileges reqPrivs,
      Map<String, List<PrivilegeGrantInfo>> availPrivs) throws HiveAuthzPluginException {
    if(availPrivs == null){
      return;
    }
    for (Map.Entry<String, List<PrivilegeGrantInfo>> userPriv : availPrivs.entrySet()) {
      List<PrivilegeGrantInfo> userPrivGInfos = userPriv.getValue();
      for (PrivilegeGrantInfo userPrivGInfo : userPrivGInfos) {
        reqPrivs.addPrivilege(userPrivGInfo.getPrivilege(), userPrivGInfo.isGrantOption());
      }
    }
  }

  public static void addMissingPrivMsg(Collection<SQLPrivTypeGrant> missingPrivs,
      HivePrivilegeObject hivePrivObject, List<String> deniedMessages) {
    if (missingPrivs.size() != 0) {
      // there are some required privileges missing, create error message
      // sort the privileges so that error message is deterministic (for tests)
      List<SQLPrivTypeGrant> sortedmissingPrivs = new ArrayList<SQLPrivTypeGrant>(missingPrivs);
      Collections.sort(sortedmissingPrivs);
      String errMsg = sortedmissingPrivs + " on " + hivePrivObject;
      deniedMessages.add(errMsg);
    }
  }

  /**
   * Map permissions for this uri to SQL Standard privileges
   * @param filePath
   * @param conf
   * @param userName
   * @return
   * @throws HiveAuthzPluginException
   */
  public static RequiredPrivileges getPrivilegesFromFS(Path filePath, HiveConf conf,
      String userName) throws HiveAuthzPluginException {
    // get the 'available privileges' from file system

    RequiredPrivileges availPrivs = new RequiredPrivileges();
    // check file system permission
    FileSystem fs;
    try {
      fs = FileSystem.get(filePath.toUri(), conf);
      FileStatus[] fileMatches = fs.globStatus(filePath);

      // There are a couple of possibilities to consider here to see if we should recurse or not.
      // a) Path is a regex, and may match multiple entries - if so, this is likely a load and
      //        we should listStatus for all relevant matches, and recurse-check each of those.
      //        Simply passing the filestatus on as recurse=true makes sense for this.
      // b) Path is a singular directory/file and exists
      //        recurse=true to check all its children if applicable
      // c) Path is a singular entity that does not exist
      //        recurse=false to check its parent - this is likely a case of
      //        needing to create a dir that does not exist yet.

      if ((fileMatches != null ) && (fileMatches.length > 1)){
        LOG.debug("Checking fs privileges for multiple files that matched {}",
            filePath.toString());
        addPrivilegesFromFS(userName, availPrivs, fs, fileMatches, true);
      } else {
        FileStatus fileStatus = FileUtils.getFileStatusOrNull(fs, filePath);
        boolean pickParent = (fileStatus == null); // did we find the file/dir itself?
        if (pickParent){
          fileStatus = FileUtils.getPathOrParentThatExists(fs, filePath.getParent());
        }
        Path path = fileStatus.getPath();
        if (pickParent){
          LOG.debug("Checking fs privileges for parent path {} for nonexistent {}",
              path.toString(), filePath.toString());
          addPrivilegesFromFS(userName, availPrivs, fs, fileStatus, false);
        } else {
          LOG.debug("Checking fs privileges for path itself {}, originally specified as {}",
              path.toString(), filePath.toString());
          addPrivilegesFromFS(userName, availPrivs, fs, fileStatus, true);
        }
      }
    } catch (Exception e) {
      String msg = "Error getting permissions for " + filePath + ": " + e.getMessage();
      throw new HiveAuthzPluginException(msg, e);
    }
    return availPrivs;
  }

  private static void addPrivilegesFromFS(
      String userName, RequiredPrivileges availPrivs, FileSystem fs,
      FileStatus[] fileStatuses, boolean recurse) throws Exception {
    // We need to obtain an intersection of all the privileges
    if (fileStatuses.length > 0){
      Set<SQLPrivTypeGrant> privs = getPrivilegesFromFS(userName, fs, fileStatuses[0], recurse);

      for (int i = 1; (i < fileStatuses.length) && (privs.size() > 0); i++){
        privs.retainAll(getPrivilegesFromFS(userName, fs, fileStatuses[i], recurse));
      }
      availPrivs.addAll(privs.toArray(new SQLPrivTypeGrant[privs.size()]));
    }
  }

  private static void addPrivilegesFromFS(
      String userName, RequiredPrivileges availPrivs, FileSystem fs,
      FileStatus fileStatus, boolean recurse) throws Exception {
    Set<SQLPrivTypeGrant> privs = getPrivilegesFromFS(userName, fs, fileStatus, recurse);
    availPrivs.addAll(privs.toArray(new SQLPrivTypeGrant[privs.size()]));
  }

  private static Set<SQLPrivTypeGrant> getPrivilegesFromFS(
      String userName, FileSystem fs,
      FileStatus fileStatus, boolean recurse) throws Exception {
    Set<SQLPrivTypeGrant> privs = new HashSet<SQLPrivTypeGrant>();
    LOG.info("Checking fs privileges of user {} for {} {} ",
        userName, fileStatus.toString(), (recurse? "recursively":"without recursion"));
    if (FileUtils.isOwnerOfFileHierarchy(fs, fileStatus, userName, recurse)) {
      privs.add(SQLPrivTypeGrant.OWNER_PRIV);
    }
    UserGroupInformation proxyUser = null;
    try {
      proxyUser = FileUtils.getProxyUser(userName);
      FileSystem fsAsUser = FileUtils.getFsAsUser(fs, proxyUser);
      if (FileUtils.isActionPermittedForFileHierarchy(fs, fileStatus, userName, FsAction.WRITE, recurse, fsAsUser)) {
        privs.add(SQLPrivTypeGrant.INSERT_NOGRANT);
        privs.add(SQLPrivTypeGrant.DELETE_NOGRANT);
      }
      if (FileUtils.isActionPermittedForFileHierarchy(fs, fileStatus, userName, FsAction.READ, recurse, fsAsUser)) {
        privs.add(SQLPrivTypeGrant.SELECT_NOGRANT);
      }
    }
    finally {
      FileUtils.closeFs(proxyUser);
    }
    LOG.debug("addPrivilegesFromFS:[{}] asked for privileges on [{}] with recurse={} and obtained:[{}]",
        userName, fileStatus, recurse, privs);
    return privs;
  }

  public static void assertNoDeniedPermissions(HivePrincipal hivePrincipal,
      HiveOperationType hiveOpType, List<String> deniedMessages) throws HiveAccessControlException {
    if (deniedMessages.size() != 0) {
      Collections.sort(deniedMessages);
      String errorMessage = "Permission denied: " + hivePrincipal
          + " does not have following privileges for operation " + hiveOpType + " "
          + deniedMessages;
      throw new HiveAccessControlException(errorMessage);
    }
  }

  static HiveAuthzPluginException getPluginException(String prefix, Exception e) {
    return new HiveAuthzPluginException(prefix + ": " + e.getMessage(), e);
  }

  /**
   * Validate the principal type, and convert role name to lower case
   * @param hPrincipal
   * @return validated principal
   * @throws HiveAuthzPluginException
   */
  public static HivePrincipal getValidatedPrincipal(HivePrincipal hPrincipal)
      throws HiveAuthzPluginException {
    if (hPrincipal == null || hPrincipal.getType() == null) {
      // null principal
      return hPrincipal;
    }
    switch (hPrincipal.getType()) {
    case USER:
      return hPrincipal;
    case ROLE:
      // lower case role names, for case insensitive behavior
      return new HivePrincipal(hPrincipal.getName().toLowerCase(), hPrincipal.getType());
    default:
      throw new HiveAuthzPluginException("Invalid principal type in principal " + hPrincipal);
    }
  }

  /**
   * Calls getValidatedPrincipal on each principal in list and updates the list
   * @param hivePrincipals
   * @return
   * @return
   * @throws HiveAuthzPluginException
   */
  public static List<HivePrincipal> getValidatedPrincipals(List<HivePrincipal> hivePrincipals)
      throws HiveAuthzPluginException {
    ListIterator<HivePrincipal> it = hivePrincipals.listIterator();
    while(it.hasNext()){
      it.set(getValidatedPrincipal(it.next()));
    }
    return hivePrincipals;
  }

  /**
   * Change the session context based on configuration to aid in testing of sql
   * std auth
   *
   * @param ctx
   * @param conf
   * @return
   */
  static HiveAuthzSessionContext applyTestSettings(HiveAuthzSessionContext ctx, HiveConf conf) {
    if (conf.getBoolVar(ConfVars.HIVE_TEST_AUTHORIZATION_SQLSTD_HS2_MODE)
        && ctx.getClientType() == CLIENT_TYPE.HIVECLI) {
      // create new session ctx object with HS2 as client type
      HiveAuthzSessionContext.Builder ctxBuilder = new HiveAuthzSessionContext.Builder(ctx);
      ctxBuilder.setClientType(CLIENT_TYPE.HIVESERVER2);
      return ctxBuilder.build();
    }
    return ctx;
  }

}
