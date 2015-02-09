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
package org.apache.hadoop.hive.ql.security.authorization.plugin;

import java.util.List;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;
import org.apache.hadoop.hive.conf.HiveConf;

/**
 * Convenience implementation of HiveAuthorizer.
 * You can customize the behavior by passing different implementations of
 * {@link HiveAccessController} and {@link HiveAuthorizationValidator} to constructor.
 *
 */
@LimitedPrivate(value = { "" })
@Evolving
public class HiveAuthorizerImpl implements HiveAuthorizer {
  HiveAccessController accessController;
  HiveAuthorizationValidator authValidator;

   public HiveAuthorizerImpl(HiveAccessController accessController, HiveAuthorizationValidator authValidator){
     this.accessController = accessController;
     this.authValidator = authValidator;
   }

  @Override
  public void grantPrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthzPluginException, HiveAccessControlException {
    accessController.grantPrivileges(hivePrincipals, hivePrivileges, hivePrivObject,
        grantorPrincipal, grantOption);
  }

  @Override
  public void revokePrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthzPluginException, HiveAccessControlException {
    accessController.revokePrivileges(hivePrincipals, hivePrivileges, hivePrivObject,
        grantorPrincipal, grantOption);
  }

  @Override
  public void createRole(String roleName, HivePrincipal adminGrantor) throws HiveAuthzPluginException, HiveAccessControlException {
    accessController.createRole(roleName, adminGrantor);
  }

  @Override
  public void dropRole(String roleName) throws HiveAuthzPluginException, HiveAccessControlException {
    accessController.dropRole(roleName);
  }

  @Override
  public void grantRole(List<HivePrincipal> hivePrincipals, List<String> roles,
      boolean grantOption, HivePrincipal grantorPrinc) throws HiveAuthzPluginException, HiveAccessControlException {
    accessController.grantRole(hivePrincipals, roles, grantOption, grantorPrinc);
  }

  @Override
  public void revokeRole(List<HivePrincipal> hivePrincipals, List<String> roles,
      boolean grantOption, HivePrincipal grantorPrinc) throws HiveAuthzPluginException, HiveAccessControlException {
    accessController.revokeRole(hivePrincipals, roles, grantOption, grantorPrinc);
  }

  @Override
  public void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputHObjs,
      List<HivePrivilegeObject> outputHObjs, HiveAuthzContext context)
      throws HiveAuthzPluginException, HiveAccessControlException {
    authValidator.checkPrivileges(hiveOpType, inputHObjs, outputHObjs, context);
  }

  @Override
  public List<String> getAllRoles() throws HiveAuthzPluginException, HiveAccessControlException {
    return accessController.getAllRoles();
  }

  @Override
  public List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal,
      HivePrivilegeObject privObj) throws HiveAuthzPluginException, HiveAccessControlException {
    return accessController.showPrivileges(principal, privObj);
  }

  @Override
  public VERSION getVersion() {
    return VERSION.V1;
  }

  @Override
  public void setCurrentRole(String roleName) throws HiveAccessControlException, HiveAuthzPluginException {
    accessController.setCurrentRole(roleName);
  }

  @Override
  public List<String> getCurrentRoleNames() throws HiveAuthzPluginException {
    return accessController.getCurrentRoleNames();
  }

  @Override
  public List<HiveRoleGrant> getPrincipalGrantInfoForRole(String roleName)
      throws HiveAuthzPluginException, HiveAccessControlException {
    return accessController.getPrincipalGrantInfoForRole(roleName);
  }

  @Override
  public List<HiveRoleGrant> getRoleGrantInfoForPrincipal(HivePrincipal principal)
      throws HiveAuthzPluginException, HiveAccessControlException {
    return accessController.getRoleGrantInfoForPrincipal(principal);
  }

  @Override
  public void applyAuthorizationConfigPolicy(HiveConf hiveConf) throws HiveAuthzPluginException {
    accessController.applyAuthorizationConfigPolicy(hiveConf);
  }
}
