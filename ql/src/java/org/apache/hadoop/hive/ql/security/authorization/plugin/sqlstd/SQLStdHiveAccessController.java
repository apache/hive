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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.ql.metadata.HiveException;
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

/**
 * Implements functionality of access control statements for sql standard based
 * authorization
 */
@Private
public class SQLStdHiveAccessController implements HiveAccessController {

  private final HiveMetastoreClientFactory metastoreClientFactory;
  private final HiveConf conf;
  private final HiveAuthenticationProvider authenticator;
  private String currentUserName;
  private List<HiveRole> currentRoles;
  private HiveRole adminRole;

  SQLStdHiveAccessController(HiveMetastoreClientFactory metastoreClientFactory, HiveConf conf,
      HiveAuthenticationProvider authenticator) throws HiveAuthzPluginException {
    this.metastoreClientFactory = metastoreClientFactory;
    this.conf = conf;
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
      roles = metastoreClientFactory.getHiveMetastoreClient().
        list_roles(currentUserName, PrincipalType.USER);
      List<HiveRole> currentRoles = new ArrayList<HiveRole>(roles.size());
      for (Role role : roles) {
        if (!HiveMetaStore.ADMIN.equalsIgnoreCase(role.getRoleName())) {
          currentRoles.add(new HiveRole(role));
        } else {
          this.adminRole = new HiveRole(role);
        }
      }
      return currentRoles;
    } catch (Exception e) {
        throw new HiveAuthzPluginException("Failed to retrieve roles for "+
            currentUserName, e);
    }
  }

  @Override
  public void grantPrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption)
          throws HiveAuthzPluginException, HiveAccessControlException {

    // expand ALL privileges, if any
    hivePrivileges = expandAllPrivileges(hivePrivileges);

    SQLAuthorizationUtils.validatePrivileges(hivePrivileges);

    IMetaStoreClient metastoreClient = metastoreClientFactory.getHiveMetastoreClient();
    // authorize the grant
    GrantPrivAuthUtils.authorize(hivePrincipals, hivePrivileges, hivePrivObject, grantOption,
        metastoreClient, authenticator.getUserName());

    // grant
    PrivilegeBag privBag = getThriftPrivilegesBag(hivePrincipals, hivePrivileges, hivePrivObject,
        grantorPrincipal, grantOption);
    try {
      metastoreClient.grant_privileges(privBag);
    } catch (Exception e) {
      throw new HiveAuthzPluginException("Error granting privileges", e);
    }
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
  private PrivilegeBag getThriftPrivilegesBag(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthzPluginException {

    HiveObjectRef privObj = SQLAuthorizationUtils.getThriftHiveObjectRef(hivePrivObject);
    PrivilegeBag privBag = new PrivilegeBag();
    for (HivePrivilege privilege : hivePrivileges) {
      if (privilege.getColumns() != null && privilege.getColumns().size() > 0) {
        throw new HiveAuthzPluginException("Privileges on columns not supported currently"
            + " in sql standard authorization mode");
      }

      PrivilegeGrantInfo grantInfo = getThriftPrivilegeGrantInfo(privilege, grantorPrincipal,
          grantOption);
      for (HivePrincipal principal : hivePrincipals) {
        HiveObjectPrivilege objPriv = new HiveObjectPrivilege(privObj, principal.getName(),
            AuthorizationUtils.getThriftPrincipalType(principal.getType()), grantInfo);
        privBag.addToPrivileges(objPriv);
      }
    }
    return privBag;
  }

  private PrivilegeGrantInfo getThriftPrivilegeGrantInfo(HivePrivilege privilege,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthzPluginException {
    try {
      return AuthorizationUtils.getThriftPrivilegeGrantInfo(privilege, grantorPrincipal,
          grantOption);
    } catch (HiveException e) {
      throw new HiveAuthzPluginException(e);
    }
  }

  @Override
  public void revokePrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption)
          throws HiveAuthzPluginException, HiveAccessControlException {
    SQLAuthorizationUtils.validatePrivileges(hivePrivileges);

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
      throws HiveAuthzPluginException {
    try {
      String grantorName = adminGrantor == null ? null : adminGrantor.getName();
      metastoreClientFactory.getHiveMetastoreClient().create_role(
        new Role(roleName, 0, grantorName));
    } catch (Exception e) {
      throw new HiveAuthzPluginException("Error create role", e);
    }
  }

  @Override
  public void dropRole(String roleName) throws HiveAuthzPluginException {
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
          + hivePrincipal.getName(), e);
    }
  }

  @Override
  public void grantRole(List<HivePrincipal> hivePrincipals, List<String> roleNames,
      boolean grantOption, HivePrincipal grantorPrinc) throws HiveAuthzPluginException {
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
      boolean grantOption, HivePrincipal grantorPrinc) throws HiveAuthzPluginException {
    if (grantOption) {
      // removing grant privileges only is not supported in metastore api
      throw new HiveAuthzPluginException("Revoking only the admin privileges on "
          + "role is not currently supported");
    }
    for (HivePrincipal hivePrincipal : hivePrincipals) {
      for (String roleName : roleNames) {
        try {
          IMetaStoreClient mClient = metastoreClientFactory.getHiveMetastoreClient();
          mClient.revoke_role(roleName, hivePrincipal.getName(),
              AuthorizationUtils.getThriftPrincipalType(hivePrincipal.getType()));
        } catch (Exception e) {
          String msg = "Error revoking roles for " + hivePrincipal.getName() + " to role "
              + roleName;
          throw new HiveAuthzPluginException(msg, e);
        }
      }
    }
  }

  @Override
  public List<String> getAllRoles() throws HiveAuthzPluginException {
    try {
      return metastoreClientFactory.getHiveMetastoreClient().listRoleNames();
    } catch (Exception e) {
      throw new HiveAuthzPluginException("Error listing all roles", e);
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
            resPrivObj, grantorPrincipal, msGrantInfo.isGrantOption());
        resPrivInfos.add(resPrivInfo);
      }
      return resPrivInfos;

    } catch (Exception e) {
      throw new HiveAuthzPluginException("Error showing privileges", e);
    }

  }

  private HivePrivilegeObjectType getPluginObjType(HiveObjectType objectType)
      throws HiveAuthzPluginException {
    switch (objectType) {
    case DATABASE:
      return HivePrivilegeObjectType.DATABASE;
    case TABLE:
      return HivePrivilegeObjectType.TABLE;
    case COLUMN:
    case GLOBAL:
    case PARTITION:
      throw new HiveAuthzPluginException("Unsupported object type " + objectType);
    default:
      throw new AssertionError("Unexpected object type " + objectType);
    }
  }

  @Override
  public void setCurrentRole(String roleName) throws HiveAuthzPluginException {

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
    throw new HiveAuthzPluginException(currentUserName +" doesn't belong to role "
      +roleName);
  }

  @Override
  public List<HiveRole> getCurrentRoles() throws HiveAuthzPluginException {
    initUserRoles();
    return currentRoles;
  }
}
