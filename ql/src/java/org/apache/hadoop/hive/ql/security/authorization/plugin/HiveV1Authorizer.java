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

package org.apache.hadoop.hive.ql.security.authorization.plugin;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.AuthorizationUtils;
import org.apache.hadoop.hive.ql.security.authorization.PrivilegeScope;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAccessController;
import org.apache.hadoop.hive.ql.session.SessionState;

public class HiveV1Authorizer extends AbstractHiveAuthorizer {

  private final HiveConf conf;

  public HiveV1Authorizer(HiveConf conf) {
    this.conf = conf;
  }


  // Leave this ctor around for backward compat.
  @Deprecated
  public HiveV1Authorizer(HiveConf conf, Hive hive) {
    this(conf);
  }

  @Override
  public VERSION getVersion() {
    return VERSION.V1;
  }

  @Override
  public void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputsHObjs,
      List<HivePrivilegeObject> outputHObjs, HiveAuthzContext context)
      throws HiveAuthzPluginException, HiveAccessControlException {
    throw new UnsupportedOperationException("Should not be called for v1 authorizer");
  }

  @Override
  public void grantPrivileges(
      List<HivePrincipal> principals, List<HivePrivilege> privileges, HivePrivilegeObject privObject,
      HivePrincipal grantor, boolean grantOption)
      throws HiveAuthzPluginException, HiveAccessControlException {
    try {
      PrivilegeBag privBag = toPrivilegeBag(privileges, privObject, grantor, grantOption);
      grantOrRevokePrivs(principals, privBag, true, grantOption);
    } catch (Exception e) {
      throw new HiveAuthzPluginException(e);
    }
  }

  @Override
  public void revokePrivileges(
      List<HivePrincipal> principals, List<HivePrivilege> privileges, HivePrivilegeObject privObject,
      HivePrincipal grantor, boolean grantOption)
      throws HiveAuthzPluginException, HiveAccessControlException {
    try {
      PrivilegeBag privBag = toPrivilegeBag(privileges, privObject, grantor, grantOption);
      grantOrRevokePrivs(principals, privBag, false, grantOption);
    } catch (Exception e) {
      throw new HiveAuthzPluginException(e);
    }
  }

  private void grantOrRevokePrivs(List<HivePrincipal> principals, PrivilegeBag privBag,
      boolean isGrant, boolean grantOption) throws HiveException {
    for (HivePrincipal principal : principals) {
      PrincipalType type = AuthorizationUtils.getThriftPrincipalType(principal.getType());
      for (HiveObjectPrivilege priv : privBag.getPrivileges()) {
        priv.setPrincipalName(principal.getName());
        priv.setPrincipalType(type);
      }
      Hive hive = Hive.getWithFastCheck(this.conf);
      if (isGrant) {
        hive.grantPrivileges(privBag);
      } else {
        hive.revokePrivileges(privBag, grantOption);
      }
    }
  }

  private PrivilegeBag toPrivilegeBag(List<HivePrivilege> privileges,
      HivePrivilegeObject privObject, HivePrincipal grantor, boolean grantOption)
      throws HiveException {

    PrivilegeBag privBag = new PrivilegeBag();
    if (privileges.isEmpty()) {
      return privBag;
    }
    String grantorName = grantor.getName();
    PrincipalType grantorType = AuthorizationUtils.getThriftPrincipalType(grantor.getType());
    if (privObject.getType() == null ||
        privObject.getType() == HivePrivilegeObject.HivePrivilegeObjectType.GLOBAL) {
      for (HivePrivilege priv : privileges) {
        List<String> columns = priv.getColumns();
        if (columns != null && !columns.isEmpty()) {
          throw new HiveException(
              "For user-level privileges, column sets should be null. columns=" +
                  columns.toString());
        }
        privBag.addToPrivileges(new HiveObjectPrivilege(new HiveObjectRef(
            HiveObjectType.GLOBAL, null, null, null, null), null, null,
            new PrivilegeGrantInfo(priv.getName(), 0, grantor.getName(), grantorType,
                grantOption)));
      }
      return privBag;
    }

    if (privObject.getPartKeys() != null && grantOption) {
      throw new HiveException("Grant does not support partition level.");
    }
    Hive hive = Hive.getWithFastCheck(this.conf);
    Database dbObj = hive.getDatabase(privObject.getDbname());
    if (dbObj == null) {
      throw new HiveException("Database " + privObject.getDbname() + " does not exists");
    }
    Table tableObj = null;
    if (privObject.getObjectName() != null) {
      tableObj = hive.getTable(dbObj.getName(), privObject.getObjectName());
    }

    List<String> partValues = null;
    if (tableObj != null) {
      if ((!tableObj.isPartitioned())
          && privObject.getPartKeys() != null) {
        throw new HiveException(
            "Table is not partitioned, but partition name is present: partSpec="
                + privObject.getPartKeys());
      }

      if (privObject.getPartKeys() != null) {
        Map<String, String> partSpec =
            Warehouse.makeSpecFromValues(tableObj.getPartitionKeys(), privObject.getPartKeys());
        Partition partObj = hive.getPartition(tableObj, partSpec, false).getTPartition();
        partValues = partObj.getValues();
      }
    }

    for (HivePrivilege priv : privileges) {
      List<String> columns = priv.getColumns();
      if (columns != null && !columns.isEmpty()) {
        if (!priv.supportsScope(PrivilegeScope.COLUMN_LEVEL_SCOPE)) {
          throw new HiveException(priv.getName() + " does not support column level privilege.");
        }
        if (tableObj == null) {
          throw new HiveException(
              "For user-level/database-level privileges, column sets should be null. columns="
                  + columns);
        }
        for (int i = 0; i < columns.size(); i++) {
          privBag.addToPrivileges(new HiveObjectPrivilege(
              new HiveObjectRef(HiveObjectType.COLUMN, dbObj.getName(), tableObj.getTableName(),
                  partValues, columns.get(i)), null, null,
              new PrivilegeGrantInfo(priv.getName(), 0, grantorName, grantorType, grantOption)));
        }
      } else if (tableObj == null) {
        privBag.addToPrivileges(new HiveObjectPrivilege(
            new HiveObjectRef(HiveObjectType.DATABASE, dbObj.getName(), null,
                null, null), null, null,
            new PrivilegeGrantInfo(priv.getName(), 0, grantorName, grantorType, grantOption)));
      } else if (partValues == null) {
        privBag.addToPrivileges(new HiveObjectPrivilege(
            new HiveObjectRef(HiveObjectType.TABLE, dbObj.getName(), tableObj.getTableName(),
                null, null), null, null,
            new PrivilegeGrantInfo(priv.getName(), 0, grantorName, grantorType, grantOption)));
      } else {
        privBag.addToPrivileges(new HiveObjectPrivilege(
            new HiveObjectRef(HiveObjectType.PARTITION, dbObj.getName(), tableObj.getTableName(),
                partValues, null), null, null,
            new PrivilegeGrantInfo(priv.getName(), 0, grantorName, grantorType, grantOption)));
      }
    }
    return privBag;
  }

  @Override
  public void createRole(String roleName, HivePrincipal adminGrantor) throws HiveAuthzPluginException, HiveAccessControlException {
    try {
      Hive hive = Hive.getWithFastCheck(this.conf);
      hive.createRole(roleName, adminGrantor == null ? null : adminGrantor.getName());
    } catch (HiveException e) {
      throw new HiveAuthzPluginException(e);
    }
  }

  @Override
  public void dropRole(String roleName) throws HiveAuthzPluginException, HiveAccessControlException {
    try {
      Hive hive = Hive.getWithFastCheck(this.conf);
      hive.dropRole(roleName);
    } catch (HiveException e) {
      throw new HiveAuthzPluginException(e);
    }
  }

  @Override
  public List<HiveRoleGrant> getPrincipalGrantInfoForRole(String roleName) throws HiveAuthzPluginException, HiveAccessControlException {
    try {
      Hive hive = Hive.getWithFastCheck(this.conf);
      return SQLStdHiveAccessController.getHiveRoleGrants(hive.getMSC(), roleName);
    } catch (Exception e) {
      throw new HiveAuthzPluginException(e);
    }
  }

  @Override
  public List<HiveRoleGrant> getRoleGrantInfoForPrincipal(HivePrincipal principal) throws HiveAuthzPluginException, HiveAccessControlException {
    PrincipalType type = AuthorizationUtils.getThriftPrincipalType(principal.getType());
    try {
      List<HiveRoleGrant> grants = new ArrayList<HiveRoleGrant>();
      Hive hive = Hive.getWithFastCheck(this.conf);
      for (RolePrincipalGrant grant : hive.getRoleGrantInfoForPrincipal(principal.getName(), type)) {
        grants.add(new HiveRoleGrant(grant));
      }
      return grants;
    } catch (HiveException e) {
      throw new HiveAuthzPluginException(e);
    }
  }

  @Override
  public void grantRole(List<HivePrincipal> principals, List<String> roles, boolean grantOption,
      HivePrincipal grantor) throws HiveAuthzPluginException, HiveAccessControlException {
    try {
      grantOrRevokeRole(principals, roles, grantOption, grantor, true);
    } catch (HiveException e) {
      throw new HiveAuthzPluginException(e);
    }
  }

  @Override
  public void revokeRole(List<HivePrincipal> principals, List<String> roles, boolean grantOption,
      HivePrincipal grantor) throws HiveAuthzPluginException, HiveAccessControlException {
    try {
      grantOrRevokeRole(principals, roles, grantOption, grantor, false);
    } catch (HiveException e) {
      throw new HiveAuthzPluginException(e);
    }
  }

  private void grantOrRevokeRole(List<HivePrincipal> principals, List<String> roles,
      boolean grantOption, HivePrincipal grantor, boolean isGrant) throws HiveException {
    PrincipalType grantorType = AuthorizationUtils.getThriftPrincipalType(grantor.getType());
    Hive hive = Hive.getWithFastCheck(this.conf);
    for (HivePrincipal principal : principals) {
      PrincipalType principalType = AuthorizationUtils.getThriftPrincipalType(principal.getType());
      String userName = principal.getName();
      for (String roleName : roles) {
        if (isGrant) {
          hive.grantRole(roleName, userName, principalType,
              grantor.getName(), grantorType, grantOption);
        } else {
          hive.revokeRole(roleName, userName, principalType, grantOption);
        }
      }
    }
  }

  @Override
  public List<String> getAllRoles() throws HiveAuthzPluginException, HiveAccessControlException {
    try {
      Hive hive = Hive.getWithFastCheck(this.conf);
      return hive.getAllRoleNames();
    } catch (HiveException e) {
      throw new HiveAuthzPluginException(e);
    }
  }

  @Override
  public List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal, HivePrivilegeObject privObj)
      throws HiveAuthzPluginException, HiveAccessControlException {
    String name = principal == null ? null : principal.getName();
    PrincipalType type =
        AuthorizationUtils.getThriftPrincipalType(principal == null ? null : principal.getType());

    List<HiveObjectPrivilege> privs = new ArrayList<HiveObjectPrivilege>();
    try {
      Hive hive = Hive.getWithFastCheck(this.conf);
      if (privObj == null) {
        // show user level privileges
        privs.addAll(hive.showPrivilegeGrant(HiveObjectType.GLOBAL, name, type,
            null, null, null, null));
      } else if (privObj.getDbname() == null) {
        // show all privileges
        privs.addAll(hive.showPrivilegeGrant(null, name, type, null, null, null, null));
      } else {
        Database dbObj = hive.getDatabase(privObj.getDbname());;
        if (dbObj == null) {
          throw new HiveException("Database " + privObj.getDbname() + " does not exists");
        }
        Table tableObj = null;
        if (privObj.getObjectName() != null) {
          tableObj = hive.getTable(dbObj.getName(), privObj.getObjectName());
        }
        List<String> partValues = privObj.getPartKeys();

        if (tableObj == null) {
          // show database level privileges
          privs.addAll(hive.showPrivilegeGrant(HiveObjectType.DATABASE,
              name, type, dbObj.getName(), null, null, null));
        } else {
          List<String> columns = privObj.getColumns();
          if (columns != null && !columns.isEmpty()) {
            // show column level privileges
            for (String columnName : columns) {
              privs.addAll(hive.showPrivilegeGrant(HiveObjectType.COLUMN, name, type,
                  dbObj.getName(), tableObj.getTableName(), partValues, columnName));
            }
          } else if (partValues == null) {
            // show table level privileges
            privs.addAll(hive.showPrivilegeGrant(HiveObjectType.TABLE, name, type,
                dbObj.getName(), tableObj.getTableName(), null, null));
          } else {
            // show partition level privileges
            privs.addAll(hive.showPrivilegeGrant(HiveObjectType.PARTITION, name, type,
                dbObj.getName(), tableObj.getTableName(), partValues, null));
          }
        }
      }
      return AuthorizationUtils.getPrivilegeInfos(privs);
    } catch (Exception ex) {
      throw new HiveAuthzPluginException(ex);
    }
  }

  @Override
  public void setCurrentRole(String roleName) throws HiveAccessControlException, HiveAuthzPluginException {
    throw new HiveAuthzPluginException("Unsupported operation 'setCurrentRole' for V1 auth");
  }

  @Override
  public List<String> getCurrentRoleNames() throws HiveAuthzPluginException {

    String userName = SessionState.get().getUserName();
    if (userName == null) {
      userName = SessionState.getUserFromAuthenticator();
    }
    if (userName == null) {
      throw new HiveAuthzPluginException("Cannot resolve current user name");
    }
    try {
      Hive hive = Hive.getWithFastCheck(this.conf);
      List<String> roleNames = new ArrayList<String>();
      for (Role role : hive.listRoles(userName, PrincipalType.USER)) {
        roleNames.add(role.getRoleName());
      }
      return roleNames;
    } catch (HiveException e) {
      throw new HiveAuthzPluginException(e);
    }
  }

  @Override
  public void applyAuthorizationConfigPolicy(HiveConf hiveConf) {
  }

  @Override
  public List<HivePrivilegeObject> filterListCmdObjects(List<HivePrivilegeObject> listObjs,
      HiveAuthzContext context) throws HiveAuthzPluginException, HiveAccessControlException {
    // do no filtering in old authorizer
    return listObjs;
  }

  @Override
  public boolean needTransform() {
    return false;
  }

  @Override
  public List<HivePrivilegeObject> applyRowFilterAndColumnMasking(HiveAuthzContext context,
      List<HivePrivilegeObject> privObjs) throws SemanticException {
    return null;
  }

}
