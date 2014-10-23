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
package org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalRequest;
import org.apache.hadoop.hive.metastore.api.GetRoleGrantsForPrincipalResponse;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.DisallowTransformHook;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessController;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext.CLIENT_TYPE;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;
import org.apache.hadoop.hive.ql.security.authorization.plugin.SettableConfigUpdater;
import org.apache.thrift.TException;

import com.google.common.collect.ImmutableSet;

/**
 * Implements functionality of access control statements for sql standard based
 * authorization
 */
@Private
public class SQLStdHiveAccessController implements HiveAccessController {

  private static final String ALL = "ALL", DEFAULT = "DEFAULT", NONE = "NONE";
  private static final ImmutableSet<String> RESERVED_ROLE_NAMES = ImmutableSet.of(ALL, DEFAULT, NONE);

  private final HiveMetastoreClientFactory metastoreClientFactory;
  private final HiveAuthenticationProvider authenticator;
  private String currentUserName;
  private List<HiveRoleGrant> currentRoles;
  private HiveRoleGrant adminRole;
  private final String ADMIN_ONLY_MSG = "User has to belong to ADMIN role and "
      + "have it as current role, for this action.";
  private final String HAS_ADMIN_PRIV_MSG = "grantor need to have ADMIN OPTION on role being"
      + " granted and have it as a current role for this action.";
  private final HiveAuthzSessionContext sessionCtx;
  public static final Log LOG = LogFactory.getLog(SQLStdHiveAccessController.class);

  public SQLStdHiveAccessController(HiveMetastoreClientFactory metastoreClientFactory, HiveConf conf,
      HiveAuthenticationProvider authenticator, HiveAuthzSessionContext ctx) throws HiveAuthzPluginException {
    this.metastoreClientFactory = metastoreClientFactory;
    this.authenticator = authenticator;
    this.sessionCtx = SQLAuthorizationUtils.applyTestSettings(ctx, conf);
    LOG.info("Created SQLStdHiveAccessController for session context : " + sessionCtx);
  }

  /**
   * (Re-)initialize currentRoleNames if necessary.
   * @throws HiveAuthzPluginException
   */
  private void initUserRoles() throws HiveAuthzPluginException {
    //to aid in testing through .q files, authenticator is passed as argument to
    // the interface. this helps in being able to switch the user within a session.
    // so we need to check if the user has changed
    String newUserName = authenticator.getUserName();
    if(currentUserName == newUserName){
      //no need to (re-)initialize the currentUserName, currentRoles fields
      return;
    }
    this.currentUserName = newUserName;
    this.currentRoles = getRolesFromMS();
    LOG.info("Current user : " + currentUserName + ", Current Roles : " + currentRoles);
  }

  private List<HiveRoleGrant> getRolesFromMS() throws HiveAuthzPluginException {
    try {
      List<RolePrincipalGrant> roles = getRoleGrants(currentUserName, PrincipalType.USER);
      Map<String, HiveRoleGrant> name2Rolesmap = new HashMap<String, HiveRoleGrant>();
      getAllRoleAncestors(name2Rolesmap, roles);
      List<HiveRoleGrant> currentRoles = new ArrayList<HiveRoleGrant>(roles.size());
      for (HiveRoleGrant role : name2Rolesmap.values()) {
        if (!HiveMetaStore.ADMIN.equalsIgnoreCase(role.getRoleName())) {
          currentRoles.add(role);
        } else {
          this.adminRole = role;
        }
      }
      return currentRoles;
    } catch (Exception e) {
      throw SQLAuthorizationUtils.getPluginException("Failed to retrieve roles for "
          + currentUserName, e);
    }
  }

  private List<RolePrincipalGrant> getRoleGrants(String principalName, PrincipalType principalType)
      throws MetaException, TException, HiveAuthzPluginException {
    GetRoleGrantsForPrincipalRequest req = new GetRoleGrantsForPrincipalRequest(principalName, principalType);
    IMetaStoreClient metastoreClient = metastoreClientFactory.getHiveMetastoreClient();
    GetRoleGrantsForPrincipalResponse resp = metastoreClient.get_role_grants_for_principal(req);
    return resp.getPrincipalGrants();
  }

  /**
   * Add role names of parentRoles and its parents to processedRolesMap
   *
   * @param processedRolesMap
   * @param roleGrants
   * @throws TException
   * @throws HiveAuthzPluginException
   * @throws MetaException
   */
  private void getAllRoleAncestors(Map<String, HiveRoleGrant> processedRolesMap, List<RolePrincipalGrant> roleGrants)
      throws MetaException, HiveAuthzPluginException, TException {
    for (RolePrincipalGrant parentRoleGrant : roleGrants) {
      String parentRoleName = parentRoleGrant.getRoleName();
      if (processedRolesMap.get(parentRoleName) == null) {
        // unprocessed role: get its parents, add it to processed, and call this
        // function recursively

        List<RolePrincipalGrant> nextParentRoles = getRoleGrants(parentRoleName, PrincipalType.ROLE);
        processedRolesMap.put(parentRoleName, new HiveRoleGrant(parentRoleGrant));
        getAllRoleAncestors(processedRolesMap, nextParentRoles);
      }
    }
  }

  @Override
  public void grantPrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption)
          throws HiveAuthzPluginException, HiveAccessControlException {

    hivePrivileges = expandAndValidatePrivileges(hivePrivileges);

    IMetaStoreClient metastoreClient = metastoreClientFactory.getHiveMetastoreClient();
    // authorize the grant
    GrantPrivAuthUtils.authorize(hivePrincipals, hivePrivileges, hivePrivObject, grantOption,
        metastoreClient, authenticator.getUserName(), getCurrentRoleNames(), isUserAdmin());

    // grant
    PrivilegeBag privBag = SQLAuthorizationUtils.getThriftPrivilegesBag(hivePrincipals, hivePrivileges, hivePrivObject,
        grantorPrincipal, grantOption);
    try {
      metastoreClient.grant_privileges(privBag);
    } catch (Exception e) {
      throw SQLAuthorizationUtils.getPluginException("Error granting privileges", e);
    }
  }

  @Override
  public List<String> getCurrentRoleNames() throws HiveAuthzPluginException {
    List<String> roleNames = new ArrayList<String>();
    for(HiveRoleGrant role : getCurrentRoles()){
      roleNames.add(role.getRoleName());
    }
    return roleNames;
  }

  private List<HivePrivilege> expandAndValidatePrivileges(List<HivePrivilege> hivePrivileges)
      throws HiveAuthzPluginException {
    // expand ALL privileges, if any
    hivePrivileges = expandAllPrivileges(hivePrivileges);
    SQLAuthorizationUtils.validatePrivileges(hivePrivileges);
    return hivePrivileges;
  }

  private List<HivePrivilege> expandAllPrivileges(List<HivePrivilege> hivePrivileges) {
    Set<HivePrivilege> hivePrivSet = new HashSet<HivePrivilege>();
    for (HivePrivilege hivePrivilege : hivePrivileges) {
      if (hivePrivilege.getName().equals(ALL)) {
        // expand to all supported privileges
        for (SQLPrivilegeType privType : SQLPrivilegeType.values()) {
          hivePrivSet.add(new HivePrivilege(privType.name(), hivePrivilege.getColumns()));
        }
      } else {
        hivePrivSet.add(hivePrivilege);
      }
    }
    return new ArrayList<HivePrivilege>(hivePrivSet);
  }


  @Override
  public void revokePrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption)
          throws HiveAuthzPluginException, HiveAccessControlException {

    hivePrivileges = expandAndValidatePrivileges(hivePrivileges);

    IMetaStoreClient metastoreClient = metastoreClientFactory.getHiveMetastoreClient();
    // authorize the revoke, and get the set of privileges to be revoked
    List<HiveObjectPrivilege> revokePrivs = RevokePrivAuthUtils
        .authorizeAndGetRevokePrivileges(hivePrincipals, hivePrivileges, hivePrivObject,
            grantOption, metastoreClient, authenticator.getUserName());

    try {
      // unfortunately, the metastore api revokes all privileges that match on
      // principal, privilege object type it does not filter on the grator
      // username.
      // So this will revoke privileges that are granted by other users.This is
      // not SQL compliant behavior. Need to change/add a metastore api
      // that has desired behavior.
      metastoreClient.revoke_privileges(new PrivilegeBag(revokePrivs), grantOption);
    } catch (Exception e) {
      throw SQLAuthorizationUtils.getPluginException("Error revoking privileges", e);
    }
  }

  @Override
  public void createRole(String roleName, HivePrincipal adminGrantor)
      throws HiveAuthzPluginException, HiveAccessControlException {
    // only user belonging to admin role can create new roles.
    if (!isUserAdmin()) {
      throw new HiveAccessControlException("Current user : " + currentUserName+ " is not"
      + " allowed to add roles. " + ADMIN_ONLY_MSG);
    }
    if (RESERVED_ROLE_NAMES.contains(roleName.trim().toUpperCase())) {
      throw new HiveAuthzPluginException("Role name cannot be one of the reserved roles: " +
          RESERVED_ROLE_NAMES);
    }
    try {
      String grantorName = adminGrantor == null ? null : adminGrantor.getName();
      metastoreClientFactory.getHiveMetastoreClient().create_role(
        new Role(roleName, 0, grantorName));
    } catch (TException e) {
      throw SQLAuthorizationUtils.getPluginException("Error create role", e);
    }
  }

  @Override
  public void dropRole(String roleName) throws HiveAuthzPluginException, HiveAccessControlException {
    // only user belonging to admin role can drop existing role
    if (!isUserAdmin()) {
      throw new HiveAccessControlException("Current user : " + currentUserName+ " is not"
      + " allowed to drop role. " + ADMIN_ONLY_MSG);
    }
    try {
      metastoreClientFactory.getHiveMetastoreClient().drop_role(roleName);
    } catch (Exception e) {
      throw SQLAuthorizationUtils.getPluginException("Error dropping role", e);
    }
  }

  @Override
  public void grantRole(List<HivePrincipal> hivePrincipals, List<String> roleNames,
    boolean grantOption, HivePrincipal grantorPrinc) throws HiveAuthzPluginException,
    HiveAccessControlException {
    if (!(isUserAdmin() || doesUserHasAdminOption(roleNames))) {
      throw new HiveAccessControlException("Current user : " + currentUserName+ " is not"
        + " allowed to grant role. " + ADMIN_ONLY_MSG + " Otherwise, " + HAS_ADMIN_PRIV_MSG);
    }
    for (HivePrincipal hivePrincipal : hivePrincipals) {
      for (String roleName : roleNames) {
        try {
          IMetaStoreClient mClient = metastoreClientFactory.getHiveMetastoreClient();
          mClient.grant_role(roleName, hivePrincipal.getName(),
              AuthorizationUtils.getThriftPrincipalType(hivePrincipal.getType()),
              grantorPrinc.getName(),
              AuthorizationUtils.getThriftPrincipalType(grantorPrinc.getType()), grantOption);
        } catch (MetaException e) {
          throw SQLAuthorizationUtils.getPluginException("Error granting role", e);
        } catch (Exception e) {
          String msg = "Error granting roles for " + hivePrincipal.getName() + " to role "
              + roleName;
          throw SQLAuthorizationUtils.getPluginException(msg, e);
        }
      }
    }
  }

  @Override
  public void revokeRole(List<HivePrincipal> hivePrincipals, List<String> roleNames,
    boolean grantOption, HivePrincipal grantorPrinc) throws HiveAuthzPluginException,
    HiveAccessControlException {
    if (!(isUserAdmin() || doesUserHasAdminOption(roleNames))) {
      throw new HiveAccessControlException("Current user : " + currentUserName+ " is not"
          + " allowed to revoke role. " + ADMIN_ONLY_MSG + " Otherwise, " + HAS_ADMIN_PRIV_MSG);
    }
    for (HivePrincipal hivePrincipal : hivePrincipals) {
      for (String roleName : roleNames) {
        try {
          IMetaStoreClient mClient = metastoreClientFactory.getHiveMetastoreClient();
          mClient.revoke_role(roleName, hivePrincipal.getName(),
              AuthorizationUtils.getThriftPrincipalType(hivePrincipal.getType()), grantOption);
        } catch (Exception e) {
          String msg = "Error revoking roles for " + hivePrincipal.getName() + " to role "
              + roleName;
          throw SQLAuthorizationUtils.getPluginException(msg, e);
        }
      }
    }
  }

  @Override
  public List<String> getAllRoles() throws HiveAuthzPluginException, HiveAccessControlException {
    // only user belonging to admin role can list role
    if (!isUserAdmin()) {
      throw new HiveAccessControlException("Current user : " + currentUserName+ " is not"
        + " allowed to list roles. " + ADMIN_ONLY_MSG);
    }
    try {
      return metastoreClientFactory.getHiveMetastoreClient().listRoleNames();
    } catch (Exception e) {
      throw SQLAuthorizationUtils.getPluginException("Error listing all roles", e);
    }
  }


  @Override
  public List<HiveRoleGrant> getPrincipalGrantInfoForRole(String roleName) throws HiveAuthzPluginException, HiveAccessControlException {
    // only user belonging to admin role can list role
    if (!isUserAdmin() &&  !doesUserHasAdminOption(Arrays.asList(roleName))) {
      throw new HiveAccessControlException("Current user : " + currentUserName+ " is not"
        + " allowed get principals in a role. " + ADMIN_ONLY_MSG + " Otherwise, " + HAS_ADMIN_PRIV_MSG);
    }
    try {
      return getHiveRoleGrants(metastoreClientFactory.getHiveMetastoreClient(), roleName);
    } catch (Exception e) {
      throw SQLAuthorizationUtils.getPluginException("Error getting principals for all roles", e);
    }
  }



  public static List<HiveRoleGrant> getHiveRoleGrants(IMetaStoreClient client, String roleName)
      throws Exception {
    GetPrincipalsInRoleRequest request = new GetPrincipalsInRoleRequest(roleName);
    GetPrincipalsInRoleResponse princGrantInfo = client.get_principals_in_role(request);

    List<HiveRoleGrant> hiveRoleGrants = new ArrayList<HiveRoleGrant>();
    for(RolePrincipalGrant thriftRoleGrant :  princGrantInfo.getPrincipalGrants()){
      hiveRoleGrants.add(new HiveRoleGrant(thriftRoleGrant));
    }
    return hiveRoleGrants;
  }

  @Override
  public List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal, HivePrivilegeObject privObj)
      throws HiveAuthzPluginException {
    try {

      // First authorize the call
      if (principal == null) {
        // only the admin is allowed to list privileges for any user
        if (!isUserAdmin()) {
          throw new HiveAccessControlException("User : " + currentUserName + " has to specify"
              + " a user name or role in the show grant. " + ADMIN_ONLY_MSG);
        }
      } else {
        //principal is specified, authorize on it
        if (!isUserAdmin()) {
          ensureShowGrantAllowed(principal);
        }
      }
      IMetaStoreClient mClient = metastoreClientFactory.getHiveMetastoreClient();
      List<HivePrivilegeInfo> resPrivInfos = new ArrayList<HivePrivilegeInfo>();
      String principalName = principal == null ? null : principal.getName();
      PrincipalType principalType = principal == null ? null :
          AuthorizationUtils.getThriftPrincipalType(principal.getType());

      // get metastore/thrift privilege object using metastore api
      List<HiveObjectPrivilege> msObjPrivs = mClient.list_privileges(principalName,
          principalType,
          SQLAuthorizationUtils.getThriftHiveObjectRef(privObj));


      // convert the metastore thrift objects to result objects
      for (HiveObjectPrivilege msObjPriv : msObjPrivs) {
        // result principal
        HivePrincipal resPrincipal = new HivePrincipal(msObjPriv.getPrincipalName(),
            AuthorizationUtils.getHivePrincipalType(msObjPriv.getPrincipalType()));

        // result privilege
        PrivilegeGrantInfo msGrantInfo = msObjPriv.getGrantInfo();
        HivePrivilege resPrivilege = new HivePrivilege(msGrantInfo.getPrivilege(), null);

        // result object
        HiveObjectRef msObjRef = msObjPriv.getHiveObject();

        if (!isSupportedObjectType(msObjRef.getObjectType())) {
          // metastore returns object type such as global GLOBAL
          // when no object is specified.
          // such privileges are not applicable to this authorization mode, so
          // ignore them
          continue;
        }

        HivePrivilegeObject resPrivObj = new HivePrivilegeObject(
            getPluginPrivilegeObjType(msObjRef.getObjectType()), msObjRef.getDbName(),
            msObjRef.getObjectName(), msObjRef.getPartValues(), msObjRef.getColumnName());

        // result grantor principal
        HivePrincipal grantorPrincipal = new HivePrincipal(msGrantInfo.getGrantor(),
            AuthorizationUtils.getHivePrincipalType(msGrantInfo.getGrantorType()));

        HivePrivilegeInfo resPrivInfo = new HivePrivilegeInfo(resPrincipal, resPrivilege,
            resPrivObj, grantorPrincipal, msGrantInfo.isGrantOption(), msGrantInfo.getCreateTime());
        resPrivInfos.add(resPrivInfo);
      }
      return resPrivInfos;

    } catch (Exception e) {
      throw SQLAuthorizationUtils.getPluginException("Error showing privileges", e);
    }

  }

  private void ensureShowGrantAllowed(HivePrincipal principal)
      throws HiveAccessControlException, HiveAuthzPluginException {
    // if user is not an admin user, allow the request only if the user is
    // requesting for privileges for themselves or a role they belong to
    switch (principal.getType()) {
    case USER:
      if (!principal.getName().equals(currentUserName)) {
        throw new HiveAccessControlException("User : " + currentUserName + " is not"
            + " allowed check privileges of another user : " + principal.getName() + ". "
            + ADMIN_ONLY_MSG);
      }
      break;
    case ROLE:
      if (!userBelongsToRole(principal.getName())) {
        throw new HiveAccessControlException("User : " + currentUserName + " is not"
            + " allowed check privileges of a role it does not belong to : "
            + principal.getName() + ". " + ADMIN_ONLY_MSG);
      }
      break;
    default:
      throw new AssertionError("Unexpected principal type " + principal.getType());
    }
  }

  /**
   * @param roleName
   * @return true if roleName is the name of one of the roles (including the role hierarchy)
   * that the user belongs to.
   * @throws HiveAuthzPluginException
   */
  private boolean userBelongsToRole(String roleName) throws HiveAuthzPluginException {
    for (HiveRoleGrant role : getRolesFromMS()) {
      // set to one of the roles user belongs to.
      if (role.getRoleName().equalsIgnoreCase(roleName)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Convert metastore object type to HivePrivilegeObjectType.
   * Also verifies that metastore object type is of a type on which metastore privileges are
   * supported by sql std auth.
   * @param objectType
   * @return corresponding HivePrivilegeObjectType
   */
  private HivePrivilegeObjectType getPluginPrivilegeObjType(HiveObjectType objectType) {
    switch (objectType) {
    case DATABASE:
      return HivePrivilegeObjectType.DATABASE;
    case TABLE:
      return HivePrivilegeObjectType.TABLE_OR_VIEW;
    default:
      throw new AssertionError("Unexpected object type " + objectType);
    }
  }

  private boolean isSupportedObjectType(HiveObjectType objectType) {
    switch (objectType) {
    case DATABASE:
    case TABLE:
      return true;
    default:
      return false;
    }
  }

  @Override
  public void setCurrentRole(String roleName) throws HiveAccessControlException,
    HiveAuthzPluginException {

    initUserRoles();
    if (ALL.equalsIgnoreCase(roleName)) {
      // for set role ALL, reset roles to default roles.
      currentRoles.clear();
      currentRoles.addAll(getRolesFromMS());
      return;
    }
    for (HiveRoleGrant role : getRolesFromMS()) {
      // set to one of the roles user belongs to.
      if (role.getRoleName().equalsIgnoreCase(roleName)) {
        currentRoles.clear();
        currentRoles.add(role);
        return;
      }
    }
    // set to ADMIN role, if user belongs there.
    if (HiveMetaStore.ADMIN.equalsIgnoreCase(roleName) && null != this.adminRole) {
      currentRoles.clear();
      currentRoles.add(adminRole);
      return;
    }
    LOG.info("Current user : " + currentUserName + ", Current Roles : " + currentRoles);
    // If we are here it means, user is requesting a role he doesn't belong to.
    throw new HiveAccessControlException(currentUserName +" doesn't belong to role "
      +roleName);
  }

  private List<HiveRoleGrant> getCurrentRoles() throws HiveAuthzPluginException {
    initUserRoles();
    return currentRoles;
  }

  /**
   * @return true only if current role of user is Admin
   * @throws HiveAuthzPluginException
   */
  boolean isUserAdmin() throws HiveAuthzPluginException {
    List<HiveRoleGrant> roles;
    roles = getCurrentRoles();
    for (HiveRoleGrant role : roles) {
      if (role.getRoleName().equalsIgnoreCase(HiveMetaStore.ADMIN)) {
        return true;
      }
    }
    return false;
  }

  private boolean doesUserHasAdminOption(List<String> roleNames) throws HiveAuthzPluginException {
    List<HiveRoleGrant> currentRoles;
    currentRoles = getCurrentRoles();
    for (String roleName : roleNames) {
      boolean roleFound = false;
      for (HiveRoleGrant currentRole : currentRoles) {
        if (roleName.equalsIgnoreCase(currentRole.getRoleName())) {
          roleFound = true;
          if (!currentRole.isGrantOption()) {
            return false;
          } else {
              break;
          }
        }
      }
      if (!roleFound) {
        return false;
      }
    }
    return true;
  }

  @Override
  public List<HiveRoleGrant> getRoleGrantInfoForPrincipal(HivePrincipal principal)
      throws HiveAuthzPluginException, HiveAccessControlException {
    try {
      // first authorize the call
      if (!isUserAdmin()) {
        ensureShowGrantAllowed(principal);
      }

      List<RolePrincipalGrant> roleGrants = getRoleGrants(principal.getName(),
          AuthorizationUtils.getThriftPrincipalType(principal.getType()));
      List<HiveRoleGrant> hiveRoleGrants = new ArrayList<HiveRoleGrant>(roleGrants.size());
      for (RolePrincipalGrant roleGrant : roleGrants) {
        hiveRoleGrants.add(new HiveRoleGrant(roleGrant));
      }
      return hiveRoleGrants;
    } catch (Exception e) {
      throw SQLAuthorizationUtils.getPluginException("Error getting role grant information for user "
          + principal.getName(), e);
    }
  }

  @Override
  public void applyAuthorizationConfigPolicy(HiveConf hiveConf) throws HiveAuthzPluginException {
    // First apply configuration applicable to both Hive Cli and HiveServer2
    // Not adding any authorization related restrictions to hive cli
    // grant all privileges for table to its owner - set this in cli as well so that owner
    // has permissions via HiveServer2 as well.
    hiveConf.setVar(ConfVars.HIVE_AUTHORIZATION_TABLE_OWNER_GRANTS, "INSERT,SELECT,UPDATE,DELETE");

    // Apply rest of the configuration only to HiveServer2
    if (sessionCtx.getClientType() == CLIENT_TYPE.HIVESERVER2
        && hiveConf.getBoolVar(ConfVars.HIVE_AUTHORIZATION_ENABLED)) {

      // Configure PREEXECHOOKS with DisallowTransformHook to disallow transform queries
      String hooks = hiveConf.getVar(ConfVars.PREEXECHOOKS).trim();
      if (hooks.isEmpty()) {
        hooks = DisallowTransformHook.class.getName();
      } else {
        hooks = hooks + "," + DisallowTransformHook.class.getName();
      }
      LOG.debug("Configuring hooks : " + hooks);
      hiveConf.setVar(ConfVars.PREEXECHOOKS, hooks);

      SettableConfigUpdater.setHiveConfWhiteList(hiveConf);

    }
  }


}
