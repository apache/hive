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

import java.util.List;
import java.util.ListIterator;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessController;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzPluginException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzSessionContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveMetastoreClientFactory;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;

/**
 * Wrapper for {@link SQLStdHiveAccessController} that does validation of
 * arguments and then calls the real object. Doing the validation in this
 * separate class, so that the chances of missing any validation is small.
 *
 * Validations/Conversions to be done
 * 1. Call SQLAuthorizationUtils.getValidatedPrincipals on HivePrincipal to validate and
 * update
 * 2. Convert roleName to lower case
 *
 */

@Private
public class SQLStdHiveAccessControllerWrapper implements HiveAccessController {

  private final SQLStdHiveAccessController hiveAccessController;

  public SQLStdHiveAccessControllerWrapper(HiveMetastoreClientFactory metastoreClientFactory,
      HiveConf conf, HiveAuthenticationProvider authenticator, HiveAuthzSessionContext ctx)
      throws HiveAuthzPluginException {
    this.hiveAccessController = new SQLStdHiveAccessController(metastoreClientFactory, conf,
        authenticator, ctx);
  }

  @Override
  public void grantPrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthzPluginException,
      HiveAccessControlException {
    // validate principals
    hivePrincipals = SQLAuthorizationUtils.getValidatedPrincipals(hivePrincipals);
    grantorPrincipal = SQLAuthorizationUtils.getValidatedPrincipal(grantorPrincipal);

    hiveAccessController.grantPrivileges(hivePrincipals, hivePrivileges, hivePrivObject,
        grantorPrincipal, grantOption);

  }

  @Override
  public void revokePrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) throws HiveAuthzPluginException,
      HiveAccessControlException {
    // validate principals
    hivePrincipals = SQLAuthorizationUtils.getValidatedPrincipals(hivePrincipals);
    grantorPrincipal = SQLAuthorizationUtils.getValidatedPrincipal(grantorPrincipal);

    hiveAccessController.revokePrivileges(hivePrincipals, hivePrivileges, hivePrivObject,
        grantorPrincipal, grantOption);
  }

  @Override
  public void createRole(String roleName, HivePrincipal adminGrantor)
      throws HiveAuthzPluginException, HiveAccessControlException {
    // validate principals
    roleName = roleName.toLowerCase();
    adminGrantor = SQLAuthorizationUtils.getValidatedPrincipal(adminGrantor);

    hiveAccessController.createRole(roleName, adminGrantor);
  }

  @Override
  public void dropRole(String roleName) throws HiveAuthzPluginException, HiveAccessControlException {
    // lower case roleName
    roleName = roleName.toLowerCase();

    hiveAccessController.dropRole(roleName);
  }

  @Override
  public void grantRole(List<HivePrincipal> hivePrincipals, List<String> roles,
      boolean grantOption, HivePrincipal grantorPrinc) throws HiveAuthzPluginException,
      HiveAccessControlException {
    // validate principals
    hivePrincipals = SQLAuthorizationUtils.getValidatedPrincipals(hivePrincipals);
    roles = getLowerCaseRoleNames(roles);
    grantorPrinc = SQLAuthorizationUtils.getValidatedPrincipal(grantorPrinc);

    hiveAccessController.grantRole(hivePrincipals, roles, grantOption, grantorPrinc);
  }

  @Override
  public void revokeRole(List<HivePrincipal> hivePrincipals, List<String> roles,
      boolean grantOption, HivePrincipal grantorPrinc) throws HiveAuthzPluginException,
      HiveAccessControlException {
    // validate
    hivePrincipals = SQLAuthorizationUtils.getValidatedPrincipals(hivePrincipals);
    roles = getLowerCaseRoleNames(roles);
    grantorPrinc = SQLAuthorizationUtils.getValidatedPrincipal(grantorPrinc);

    hiveAccessController.revokeRole(hivePrincipals, roles, grantOption, grantorPrinc);
  }

  @Override
  public List<String> getAllRoles() throws HiveAuthzPluginException, HiveAccessControlException {
    return hiveAccessController.getAllRoles();
  }

  @Override
  public List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal, HivePrivilegeObject privObj)
      throws HiveAuthzPluginException, HiveAccessControlException {
    // validate
    principal = SQLAuthorizationUtils.getValidatedPrincipal(principal);

    return hiveAccessController.showPrivileges(principal, privObj);
  }

  @Override
  public void setCurrentRole(String roleName) throws HiveAuthzPluginException,
      HiveAccessControlException {
    // validate
    roleName = roleName.toLowerCase();

    hiveAccessController.setCurrentRole(roleName);
  }

  @Override
  public List<String> getCurrentRoleNames() throws HiveAuthzPluginException {
    return hiveAccessController.getCurrentRoleNames();
  }

  @Override
  public List<HiveRoleGrant> getPrincipalGrantInfoForRole(String roleName)
      throws HiveAuthzPluginException, HiveAccessControlException {
    // validate
    roleName = roleName.toLowerCase();

    return hiveAccessController.getPrincipalGrantInfoForRole(roleName);
  }

  @Override
  public List<HiveRoleGrant> getRoleGrantInfoForPrincipal(HivePrincipal principal)
      throws HiveAuthzPluginException, HiveAccessControlException {
    // validate
    principal = SQLAuthorizationUtils.getValidatedPrincipal(principal);

    return hiveAccessController.getRoleGrantInfoForPrincipal(principal);
  }

  @Override
  public void applyAuthorizationConfigPolicy(HiveConf hiveConf) throws HiveAuthzPluginException {
    hiveAccessController.applyAuthorizationConfigPolicy(hiveConf);
  }

  public boolean isUserAdmin() throws HiveAuthzPluginException {
    return hiveAccessController.isUserAdmin();
  }

  private List<String> getLowerCaseRoleNames(List<String> roles) {
    ListIterator<String> roleIter = roles.listIterator();
    while (roleIter.hasNext()) {
      roleIter.set(roleIter.next().toLowerCase());
    }
    return roles;
  }

}
