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
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessController;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizationPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject.HivePrivilegeObjectType;


/**
 * Implements functionality of access control statements for sql standard based authorization
 */
@Private
public class SQLStdHiveAccessController implements HiveAccessController {

  private HiveMetastoreClientFactory metastoreClientFactory;


  SQLStdHiveAccessController(HiveMetastoreClientFactory metastoreClientFactory,
      HiveConf conf, String hiveCurrentUser){
    this.metastoreClientFactory = metastoreClientFactory;
  }


  @Override
  public void grantPrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthorizationPluginException {

    PrivilegeBag privBag =
        getThriftPrivilegesBag(hivePrincipals, hivePrivileges, hivePrivObject, grantorPrincipal,
            grantOption);
    try {
      metastoreClientFactory.getHiveMetastoreClient().grant_privileges(privBag);
    } catch (Exception e) {
      throw new HiveAuthorizationPluginException("Error granting privileges", e);
    }
  }

  /**
   * Create thrift privileges bag
   * @param hivePrincipals
   * @param hivePrivileges
   * @param hivePrivObject
   * @param grantorPrincipal
   * @param grantOption
   * @return
   * @throws HiveAuthorizationPluginException
   */
  private PrivilegeBag getThriftPrivilegesBag(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthorizationPluginException {
    HiveObjectRef privObj = getThriftHiveObjectRef(hivePrivObject);
    PrivilegeBag privBag = new PrivilegeBag();
    for(HivePrivilege privilege : hivePrivileges){
      if(privilege.getColumns() != null && privilege.getColumns().size() > 0){
        throw new HiveAuthorizationPluginException("Privileges on columns not supported currently"
            + " in sql standard authorization mode");
      }
      PrivilegeGrantInfo grantInfo = getThriftPrivilegeGrantInfo(privilege, grantorPrincipal, grantOption);
      for(HivePrincipal principal : hivePrincipals){
        HiveObjectPrivilege objPriv = new HiveObjectPrivilege(privObj, principal.getName(),
            AuthorizationUtils.getThriftPrincipalType(principal.getType()), grantInfo);
        privBag.addToPrivileges(objPriv);
      }
    }
    return privBag;
  }

  private PrivilegeGrantInfo getThriftPrivilegeGrantInfo(HivePrivilege privilege,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthorizationPluginException {
    try {
      return  AuthorizationUtils.getThriftPrivilegeGrantInfo(privilege, grantorPrincipal, grantOption);
    } catch (HiveException e) {
      throw new HiveAuthorizationPluginException(e);
    }
  }

  /**
   * Create a thrift privilege object from the plugin interface privilege object
   * @param privObj
   * @return
   * @throws HiveAuthorizationPluginException
   */
  private HiveObjectRef getThriftHiveObjectRef(HivePrivilegeObject privObj)
      throws HiveAuthorizationPluginException {
    try {
      return  AuthorizationUtils.getThriftHiveObjectRef(privObj);
    } catch (HiveException e) {
      throw new HiveAuthorizationPluginException(e);
    }
  }

  @Override
  public void revokePrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthorizationPluginException {

    PrivilegeBag privBag =
        getThriftPrivilegesBag(hivePrincipals, hivePrivileges, hivePrivObject, grantorPrincipal,
            grantOption);
    try {
      metastoreClientFactory.getHiveMetastoreClient().revoke_privileges(privBag);
    } catch (Exception e) {
      throw new HiveAuthorizationPluginException("Error revoking privileges", e);
    }
  }

  @Override
  public void createRole(String roleName, HivePrincipal adminGrantor)
      throws HiveAuthorizationPluginException {
    try {
      String grantorName = adminGrantor == null ? null : adminGrantor.getName();
      metastoreClientFactory.getHiveMetastoreClient()
        .create_role(new Role(roleName, 0, grantorName));
    } catch (Exception e) {
      throw new HiveAuthorizationPluginException("Error create role", e);
    }
  }

  @Override
  public void dropRole(String roleName) throws HiveAuthorizationPluginException {
    try {
      metastoreClientFactory.getHiveMetastoreClient().drop_role(roleName);
    } catch (Exception e) {
      throw new HiveAuthorizationPluginException("Error dropping role", e);
    }
  }

  @Override
  public List<String> getRoles(HivePrincipal hivePrincipal) throws HiveAuthorizationPluginException {
    try {
      List<Role> roles = metastoreClientFactory.getHiveMetastoreClient().list_roles(
          hivePrincipal.getName(), AuthorizationUtils.getThriftPrincipalType(hivePrincipal.getType()));
      List<String> roleNames = new ArrayList<String>(roles.size());
      for(Role role : roles){
        roleNames.add(role.getRoleName());
      }
      return roleNames;
    } catch (Exception e) {
      throw new HiveAuthorizationPluginException(
          "Error listing roles for user" + hivePrincipal.getName(), e);
    }
  }

  @Override
  public void grantRole(List<HivePrincipal> hivePrincipals, List<String> roleNames,
      boolean grantOption, HivePrincipal grantorPrinc) throws HiveAuthorizationPluginException {
    for(HivePrincipal hivePrincipal : hivePrincipals){
      for(String roleName : roleNames){
        try {
          IMetaStoreClient mClient = metastoreClientFactory.getHiveMetastoreClient();
          mClient.grant_role(roleName,
              hivePrincipal.getName(),
              AuthorizationUtils.getThriftPrincipalType(hivePrincipal.getType()),
              grantorPrinc.getName(),
              AuthorizationUtils.getThriftPrincipalType(grantorPrinc.getType()),
              grantOption
              );
        }  catch (Exception e) {
          String msg = "Error granting roles for " + hivePrincipal.getName() +  " to role " + roleName
              + hivePrincipal.getName();
          throw new HiveAuthorizationPluginException(msg, e);
        }
      }
    }
  }

  @Override
  public void revokeRole(List<HivePrincipal> hivePrincipals, List<String> roleNames,
      boolean grantOption, HivePrincipal grantorPrinc) throws HiveAuthorizationPluginException {
    if(grantOption){
      //removing grant privileges only is not supported in metastore api
      throw new HiveAuthorizationPluginException("Revoking only the admin privileges on "
          + "role is not currently supported");
    }
    for(HivePrincipal hivePrincipal : hivePrincipals){
      for(String roleName : roleNames){
        try {
          IMetaStoreClient mClient = metastoreClientFactory.getHiveMetastoreClient();
          mClient.revoke_role(roleName,
              hivePrincipal.getName(),
              AuthorizationUtils.getThriftPrincipalType(hivePrincipal.getType())
              );
        }  catch (Exception e) {
          String msg = "Error revoking roles for " + hivePrincipal.getName() +  " to role " + roleName
              + hivePrincipal.getName();
          throw new HiveAuthorizationPluginException(msg, e);
        }
      }
    }
  }

  @Override
  public List<String> getAllRoles() throws HiveAuthorizationPluginException {
    try {
      return metastoreClientFactory.getHiveMetastoreClient().listRoleNames();
    } catch (Exception e) {
      throw new HiveAuthorizationPluginException("Error listing all roles", e);
    }
  }


  @Override
  public List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal, HivePrivilegeObject privObj)
      throws HiveAuthorizationPluginException {
    try {

      List<HivePrivilegeInfo> resPrivInfos = new ArrayList<HivePrivilegeInfo>();
      IMetaStoreClient mClient = metastoreClientFactory.getHiveMetastoreClient();

      //get metastore/thrift privilege object using metastore api
      List<HiveObjectPrivilege> msObjPrivs
        = mClient.list_privileges(principal.getName(),
            AuthorizationUtils.getThriftPrincipalType(principal.getType()),
            getThriftHiveObjectRef(privObj));

      //convert the metastore thrift objects to result objects
      for(HiveObjectPrivilege msObjPriv : msObjPrivs){
        //result principal
        HivePrincipal resPrincipal =
            new HivePrincipal(msObjPriv.getPrincipalName(),
                AuthorizationUtils.getHivePrincipalType(msObjPriv.getPrincipalType()));

        //result privilege
        PrivilegeGrantInfo msGrantInfo = msObjPriv.getGrantInfo();
        HivePrivilege resPrivilege = new HivePrivilege(msGrantInfo.getPrivilege(), null);

        //result object
        HiveObjectRef msObjRef = msObjPriv.getHiveObject();
        HivePrivilegeObject resPrivObj = new HivePrivilegeObject(
            getPluginObjType(msObjRef.getObjectType()),
            msObjRef.getDbName(),
            msObjRef.getObjectName()
            );

        //result grantor principal
        HivePrincipal grantorPrincipal =
            new HivePrincipal(msGrantInfo.getGrantor(),
                AuthorizationUtils.getHivePrincipalType(msGrantInfo.getGrantorType()));


        HivePrivilegeInfo resPrivInfo = new HivePrivilegeInfo(resPrincipal, resPrivilege,
            resPrivObj, grantorPrincipal, msGrantInfo.isGrantOption());
        resPrivInfos.add(resPrivInfo);
      }
      return resPrivInfos;

    }
    catch (Exception e) {
      throw new HiveAuthorizationPluginException("Error showing privileges", e);
    }

  }


  private HivePrivilegeObjectType getPluginObjType(HiveObjectType objectType)
      throws HiveAuthorizationPluginException {
    switch(objectType){
    case DATABASE:
      return HivePrivilegeObjectType.DATABASE;
    case TABLE:
      return HivePrivilegeObjectType.TABLE;
    case COLUMN:
    case GLOBAL:
    case PARTITION:
      throw new HiveAuthorizationPluginException("Unsupported object type " + objectType);
    default:
      throw new AssertionError("Unexpected object type " + objectType);
    }
  }

}
