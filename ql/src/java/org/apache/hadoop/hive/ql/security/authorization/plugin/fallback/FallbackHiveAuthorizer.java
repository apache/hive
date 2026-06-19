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

package org.apache.hadoop.hive.ql.security.authorization.plugin.fallback;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.AbstractHiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.DisallowTransformHook;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;
import org.apache.hadoop.hive.ql.security.authorization.plugin.SettableConfigUpdater;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.Operation2Privilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLAuthorizationUtils;
import org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLPrivTypeGrant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FallbackHiveAuthorizer extends AbstractHiveAuthorizer {
  private static final Logger LOG = LoggerFactory.getLogger(FallbackHiveAuthorizer.class);

  private final HiveAuthzSessionContext sessionCtx;
  private final HiveAuthenticationProvider authenticator;
  private String[] admins = null;

  public FallbackHiveAuthorizer(HiveConf hiveConf, HiveAuthenticationProvider hiveAuthenticator,
                                HiveAuthzSessionContext ctx) {
    this.authenticator = hiveAuthenticator;
    this.sessionCtx = applyTestSettings(ctx, hiveConf);
    String adminString = hiveConf.getVar(HiveConf.ConfVars.USERS_IN_ADMIN_ROLE);
    if (adminString != null) {
      admins = hiveConf.getVar(HiveConf.ConfVars.USERS_IN_ADMIN_ROLE).split(",");
    }
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
    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_TEST_AUTHORIZATION_SQLSTD_HS2_MODE)
            && ctx.getClientType() == HiveAuthzSessionContext.CLIENT_TYPE.HIVECLI) {
      // create new session ctx object with HS2 as client type
      HiveAuthzSessionContext.Builder ctxBuilder = new HiveAuthzSessionContext.Builder(ctx);
      ctxBuilder.setClientType(HiveAuthzSessionContext.CLIENT_TYPE.HIVESERVER2);
      return ctxBuilder.build();
    }
    return ctx;
  }

  @Override
  public VERSION getVersion() {
    return VERSION.V1;
  }

  @Override
  public void grantPrivileges(List<HivePrincipal> hivePrincipals, List<HivePrivilege> hivePrivileges,
                              HivePrivilegeObject hivePrivObject, HivePrincipal grantorPrincipal, boolean
                                        grantOption) throws HiveAuthzPluginException {
    throw new HiveAuthzPluginException("grantPrivileges not implemented in FallbackHiveAuthorizer");
  }

  @Override
  public void revokePrivileges(List<HivePrincipal> hivePrincipals, List<HivePrivilege> hivePrivileges,
                               HivePrivilegeObject hivePrivObject, HivePrincipal grantorPrincipal, boolean
                                         grantOption) throws HiveAuthzPluginException {
    throw new HiveAuthzPluginException("revokePrivileges not implemented in FallbackHiveAuthorizer");
  }

  @Override
  public void createRole(String roleName, HivePrincipal adminGrantor) throws HiveAuthzPluginException {
    throw new HiveAuthzPluginException("createRole not implemented in FallbackHiveAuthorizer");
  }

  @Override
  public void dropRole(String roleName) throws HiveAuthzPluginException, HiveAccessControlException {
    throw new HiveAuthzPluginException("dropRole not implemented in FallbackHiveAuthorizer");
  }

  @Override
  public List<HiveRoleGrant> getPrincipalGrantInfoForRole(String roleName) throws HiveAuthzPluginException,
          HiveAccessControlException {
    throw new HiveAuthzPluginException("getPrincipalGrantInfoForRole not implemented in FallbackHiveAuthorizer");
  }

  @Override
  public List<HiveRoleGrant> getRoleGrantInfoForPrincipal(HivePrincipal principal) throws HiveAuthzPluginException,
          HiveAccessControlException {
    throw new HiveAuthzPluginException("getRoleGrantInfoForPrincipal not implemented in FallbackHiveAuthorizer");
  }

  @Override
  public void grantRole(List<HivePrincipal> hivePrincipals, List<String> roles, boolean grantOption, HivePrincipal
          grantorPrinc) throws HiveAuthzPluginException, HiveAccessControlException {
    throw new HiveAuthzPluginException("grantRole not implemented in FallbackHiveAuthorizer");
  }

  @Override
  public void revokeRole(List<HivePrincipal> hivePrincipals, List<String> roles, boolean grantOption, HivePrincipal
          grantorPrinc) throws HiveAuthzPluginException, HiveAccessControlException {
    throw new HiveAuthzPluginException("revokeRole not implemented in FallbackHiveAuthorizer");
  }

  @Override
  public void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputHObjs,
                              List<HivePrivilegeObject> outputHObjs, HiveAuthzContext context) throws
          HiveAuthzPluginException, HiveAccessControlException {
    String userName = authenticator.getUserName();
    // check privileges on input and output objects
    List<String> deniedMessages = new ArrayList<>();
    checkPrivileges(hiveOpType, inputHObjs, userName, Operation2Privilege.IOType.INPUT, deniedMessages);
    checkPrivileges(hiveOpType, outputHObjs, userName, Operation2Privilege.IOType.OUTPUT, deniedMessages);

    SQLAuthorizationUtils.assertNoDeniedPermissions(new HivePrincipal(userName,
            HivePrincipal.HivePrincipalType.USER), hiveOpType, deniedMessages);
  }

  // Adapted from SQLStdHiveAuthorizationValidator, only check privileges for LOAD/ADD/DFS/COMPILE and admin privileges
  private void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> hiveObjects,
                               String userName, Operation2Privilege.IOType ioType, List<String> deniedMessages) {

    if (hiveObjects == null) {
      return;
    }
    if (admins != null && Arrays.stream(admins).parallel().anyMatch(n -> n.equals(userName))) {
      return; // Skip rest of checks if user is admin
    }

    // Special-casing for ADMIN-level operations that do not require object checking.
    if (Operation2Privilege.isAdminPrivOperation(hiveOpType)) {
      // Require ADMIN privilege
      deniedMessages.add(SQLPrivTypeGrant.ADMIN_PRIV.toString() + " on " + ioType);
      return; // Ignore object, fail if not admin, succeed if admin.
    }

    boolean needAdmin = false;
    for (HivePrivilegeObject hiveObj : hiveObjects) {
      // If involving local file system
      if (hiveObj.getType() == HivePrivilegeObject.HivePrivilegeObjectType.LOCAL_URI) {
        needAdmin = true;
        break;
      }
    }
    if (!needAdmin) {
      switch (hiveOpType) {
        case ADD:
        case DFS:
        case COMPILE:
          needAdmin = true;
          break;
        default:
          break;
      }
    }
    if (needAdmin) {
      deniedMessages.add("ADMIN");
    }
  }

  @Override
  public List<HivePrivilegeObject> filterListCmdObjects(List<HivePrivilegeObject> listObjs, HiveAuthzContext context) {
    return listObjs;
  }

  @Override
  public List<String> getAllRoles() throws HiveAuthzPluginException {
    throw new HiveAuthzPluginException("getAllRoles not implemented in FallbackHiveAuthorizer");
  }

  @Override
  public List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal, HivePrivilegeObject privObj) throws
          HiveAuthzPluginException {
    throw new HiveAuthzPluginException("showPrivileges not implemented in FallbackHiveAuthorizer");
  }

  @Override
  public void setCurrentRole(String roleName) throws HiveAuthzPluginException {
    throw new HiveAuthzPluginException("setCurrentRole not implemented in FallbackHiveAuthorizer");
  }

  @Override
  public List<String> getCurrentRoleNames() throws HiveAuthzPluginException {
    throw new HiveAuthzPluginException("getCurrentRoleNames not implemented in FallbackHiveAuthorizer");
  }

  @Override
  public void applyAuthorizationConfigPolicy(HiveConf hiveConf) throws HiveAuthzPluginException {
    // from SQLStdHiveAccessController.applyAuthorizationConfigPolicy()
    if (sessionCtx.getClientType() == HiveAuthzSessionContext.CLIENT_TYPE.HIVESERVER2
            && hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_ENABLED)) {

      // Configure PRE_EXEC_HOOKS with DisallowTransformHook to disallow transform queries
      String hooks = hiveConf.getVar(HiveConf.ConfVars.PRE_EXEC_HOOKS).trim();
      if (hooks.isEmpty()) {
        hooks = DisallowTransformHook.class.getName();
      } else {
        hooks = hooks + "," + DisallowTransformHook.class.getName();
      }
      LOG.debug("Configuring hooks : " + hooks);
      hiveConf.setVar(HiveConf.ConfVars.PRE_EXEC_HOOKS, hooks);

      SettableConfigUpdater.setHiveConfWhiteList(hiveConf);
      String curBlackList = hiveConf.getVar(HiveConf.ConfVars.HIVE_SERVER2_BUILTIN_UDF_BLACKLIST);
      if (curBlackList != null && curBlackList.trim().equals("reflect,reflect2,java_method")) {
        hiveConf.setVar(HiveConf.ConfVars.HIVE_SERVER2_BUILTIN_UDF_BLACKLIST, "reflect,reflect2,java_method,in_file");
      }

    }
  }

  @Override
  public List<HivePrivilegeObject> applyRowFilterAndColumnMasking(HiveAuthzContext context, List<HivePrivilegeObject>
          privObjs) throws SemanticException {
    return privObjs;
  }

  @Override
  public boolean needTransform() {
    return false;
  }
}
