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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleRequest;
import org.apache.hadoop.hive.metastore.api.GetPrincipalsInRoleResponse;
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
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessController;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRole;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;
import org.apache.thrift.TException;

/**
 * Implements functionality of access control statements for sql standard based
 * authorization
 */
@Private
public class SQLStdHiveAccessController implements HiveAccessController {

  private final HiveMetastoreClientFactory metastoreClientFactory;
  private final HiveAuthenticationProvider authenticator;
  private String currentUserName;
  private List<HiveRole> currentRoles;
  private HiveRole adminRole;
  private final String ADMIN_ONLY_MSG = "User has to belong to ADMIN role and "
      + "have it as current role, for this action.";
  private final String HAS_ADMIN_PRIV_MSG = "grantor need to have ADMIN privileges on role being"
      + " granted and have it as a current role for this action.";

  SQLStdHiveAccessController(HiveMetastoreClientFactory metastoreClientFactory, HiveConf conf,
      HiveAuthenticationProvider authenticator) throws HiveAuthzPluginException {
    this.metastoreClientFactory = metastoreClientFactory;
    this.authenticator = authenticator;
    initUserRoles();
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
  }

  private List<HiveRole> getRolesFromMS() throws HiveAuthzPluginException {
    List<Role> roles;
    try {
      roles = metastoreClientFactory.getHiveMetastoreClient().list_roles(currentUserName,
          PrincipalType.USER);
      Map<String, HiveRole> name2Rolesmap = new HashMap<String, HiveRole>();
      getAllRoleAncestors(name2Rolesmap, roles);
      List<HiveRole> currentRoles = new ArrayList<HiveRole>(roles.size());
      for (HiveRole role : name2Rolesmap.values()) {
        if (!HiveMetaStore.ADMIN.equalsIgnoreCase(role.getRoleName())) {
          currentRoles.add(role);
        } else {
          this.adminRole = role;
        }
      }
      return currentRoles;
    } catch (Exception e) {
      throw new HiveAuthzPluginException("Failed to retrieve roles for " + currentUserName + ": "
          + e.getMessage(), e);
    }
  }

  /**
   * Add role names of parentRoles and its parents to processedRolesMap
   *
   * @param processedRolesMap
   * @param parentRoles
   * @throws TException
   * @throws HiveAuthzPluginException
   * @throws MetaException
   */
  private void getAllRoleAncestors(Map<String, HiveRole> processedRolesMap, List<Role> parentRoles)
      throws MetaException, HiveAuthzPluginException, TException {
    for (Role parentRole : parentRoles) {
      String parentRoleName = parentRole.getRoleName();
      if (processedRolesMap.get(parentRoleName) == null) {
        // unprocessed role: get its parents, add it to processed, and call this
        // function recursively
        List<Role> nextParentRoles = metastoreClientFactory.getHiveMetastoreClient().list_roles(
            parentRoleName, PrincipalType.ROLE);
        processedRolesMap.put(parentRoleName, new HiveRole(parentRole));
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
        metastoreClient, authenticator.getUserName(), getCurrentRoles(), isUserAdmin());

    // grant
    PrivilegeBag privBag = SQLAuthorizationUtils.getThriftPrivilegesBag(hivePrincipals, hivePrivileges, hivePrivObject,
        grantorPrincipal, grantOption);
    try {
      metastoreClient.grant_privileges(privBag);
    } catch (Exception e) {
      throw new HiveAuthzPluginException("Error granting privileges: " + e.getMessage(), e);
    }
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
      if (hivePrivilege.getName().equals("ALL")) {
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
      metastoreClient.revoke_privileges(new PrivilegeBag(revokePrivs));
    } catch (Exception e) {
      throw new HiveAuthzPluginException("Error revoking privileges", e);
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
    try {
      String grantorName = adminGrantor == null ? null : adminGrantor.getName();
      metastoreClientFactory.getHiveMetastoreClient().create_role(
        new Role(roleName, 0, grantorName));
    } catch (Exception e) {
      throw new HiveAuthzPluginException("Error create role", e);
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
      throw new HiveAuthzPluginException("Error dropping role", e);
    }
  }

  @Override
  public List<HiveRole> getRoles(HivePrincipal hivePrincipal) throws HiveAuthzPluginException {
    try {
      List<Role> roles = metastoreClientFactory.getHiveMetastoreClient().list_roles(
          hivePrincipal.getName(), AuthorizationUtils.getThriftPrincipalType(hivePrincipal.getType()));
      List<HiveRole> hiveRoles = new ArrayList<HiveRole>(roles.size());
      for (Role role : roles){
        hiveRoles.add(new HiveRole(role));
      }
      return hiveRoles;
    } catch (Exception e) {
      throw new HiveAuthzPluginException("Error listing roles for user "
          + hivePrincipal.getName() + ": " + e.getMessage(), e);
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
          throw new HiveAuthzPluginException(e.getMessage(), e);
        } catch (Exception e) {
          String msg = "Error granting roles for " + hivePrincipal.getName() + " to role "
              + roleName + ": " + e.getMessage();
          throw new HiveAuthzPluginException(msg, e);
        }
      }
    }
  }

  @Override
  public void revokeRole(List<HivePrincipal> hivePrincipals, List<String> roleNames,
    boolean grantOption, HivePrincipal grantorPrinc) throws HiveAuthzPluginException,
    HiveAccessControlException {
    if (grantOption) {
      // removing grant privileges only is not supported in metastore api
      throw new HiveAuthzPluginException("Revoking only the admin privileges on "
        + "role is not currently supported");
    }
    if (!(isUserAdmin() || doesUserHasAdminOption(roleNames))) {
      throw new HiveAccessControlException("Current user : " + currentUserName+ " is not"
          + " allowed to revoke role. " + ADMIN_ONLY_MSG + " Otherwise, " + HAS_ADMIN_PRIV_MSG);
    }
    for (HivePrincipal hivePrincipal : hivePrincipals) {
      for (String roleName : roleNames) {
        try {
          IMetaStoreClient mClient = metastoreClientFactory.getHiveMetastoreClient();
          mClient.revoke_role(roleName, hivePrincipal.getName(),
              AuthorizationUtils.getThriftPrincipalType(hivePrincipal.getType()));
        } catch (Exception e) {
          String msg = "Error revoking roles for " + hivePrincipal.getName() + " to role "
              + roleName + ": " + e.getMessage();
          throw new HiveAuthzPluginException(msg, e);
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
      throw new HiveAuthzPluginException("Error listing all roles", e);
    }
  }


  @Override
  public List<HiveRoleGrant> getPrincipalsInRoleInfo(String roleName) throws HiveAuthzPluginException, HiveAccessControlException {
    // only user belonging to admin role can list role
    if (!isUserAdmin()) {
      throw new HiveAccessControlException("Current user : " + currentUserName+ " is not"
        + " allowed get principals in a role. " + ADMIN_ONLY_MSG);
    }
    try {
      GetPrincipalsInRoleResponse princGrantInfo =
          metastoreClientFactory.getHiveMetastoreClient().get_principals_in_role(new GetPrincipalsInRoleRequest(roleName));

      List<HiveRoleGrant> hiveRoleGrants = new ArrayList<HiveRoleGrant>();
      for(RolePrincipalGrant thriftRoleGrant :  princGrantInfo.getPrincipalGrants()){
        hiveRoleGrants.add(new HiveRoleGrant(thriftRoleGrant));
      }
      return hiveRoleGrants;
    } catch (Exception e) {
      throw new HiveAuthzPluginException("Error getting principals for all roles", e);
    }
  }

  @Override
  public List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal, HivePrivilegeObject privObj)
      throws HiveAuthzPluginException {
    try {
      IMetaStoreClient mClient = metastoreClientFactory.getHiveMetastoreClient();
      List<HivePrivilegeInfo> resPrivInfos = new ArrayList<HivePrivilegeInfo>();
      // get metastore/thrift privilege object using metastore api
      List<HiveObjectPrivilege> msObjPrivs = mClient.list_privileges(principal.getName(),
          AuthorizationUtils.getThriftPrincipalType(principal.getType()),
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
        HivePrivilegeObject resPrivObj = new HivePrivilegeObject(
            getPluginObjType(msObjRef.getObjectType()), msObjRef.getDbName(),
            msObjRef.getObjectName());

        // result grantor principal
        HivePrincipal grantorPrincipal = new HivePrincipal(msGrantInfo.getGrantor(),
            AuthorizationUtils.getHivePrincipalType(msGrantInfo.getGrantorType()));

        HivePrivilegeInfo resPrivInfo = new HivePrivilegeInfo(resPrincipal, resPrivilege,
            resPrivObj, grantorPrincipal, msGrantInfo.isGrantOption(), msGrantInfo.getCreateTime());
        resPrivInfos.add(resPrivInfo);
      }
      return resPrivInfos;

    } catch (Exception e) {
      throw new HiveAuthzPluginException("Error showing privileges: "+ e.getMessage(), e);
    }

  }

  private HivePrivilegeObjectType getPluginObjType(HiveObjectType objectType)
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

  @Override
  public void setCurrentRole(String roleName) throws HiveAccessControlException,
    HiveAuthzPluginException {

    initUserRoles();
    if ("NONE".equalsIgnoreCase(roleName)) {
      // for set role NONE, reset roles to default roles.
      currentRoles.clear();
      currentRoles.addAll(getRolesFromMS());
      return;
    }
    for (HiveRole role : getRolesFromMS()) {
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
    // If we are here it means, user is requesting a role he doesn't belong to.
    throw new HiveAccessControlException(currentUserName +" doesn't belong to role "
      +roleName);
  }

  @Override
  public List<HiveRole> getCurrentRoles() throws HiveAuthzPluginException {
    initUserRoles();
    return currentRoles;
  }

  /**
   * @return true only if current role of user is Admin
   * @throws HiveAuthzPluginException
   */
  boolean isUserAdmin() throws HiveAuthzPluginException {
    List<HiveRole> roles;
    try {
      roles = getCurrentRoles();
    } catch (Exception e) {
      throw new HiveAuthzPluginException(e);
    }
    for (HiveRole role : roles) {
      if (role.getRoleName().equalsIgnoreCase(HiveMetaStore.ADMIN)) {
        return true;
      }
    }
    return false;
  }

  private boolean doesUserHasAdminOption(List<String> roleNames) throws HiveAuthzPluginException {
    List<HiveRole> currentRoles;
    try {
      currentRoles = getCurrentRoles();
    } catch (Exception e) {
        throw new HiveAuthzPluginException(e);
    }
    for (String roleName : roleNames) {
      boolean roleFound = false;
      for (HiveRole currentRole : currentRoles) {
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

}
