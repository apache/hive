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


/**
 * Abstract class that extends HiveAuthorizer. This will help to shield
 * Hive authorization implementations from some of the changes to HiveAuthorizer
 * interface by providing default implementation of new methods in HiveAuthorizer
 * when possible.
 */
public abstract class AbstractHiveAuthorizer implements HiveAuthorizer {

  /* (non-Javadoc)
   * @see org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthorizer#getHiveAuthorizationTranslator()
   */
  @Override
  public HiveAuthorizationTranslator getHiveAuthorizationTranslator() throws HiveAuthzPluginException {
    // No customization of this API is done for most Authorization implementations. It is meant 
    // to be used for special cases in Apache Sentry (incubating)
    // null is to be returned when no customization is needed for the translator
    // see javadoc in interface for details.
    return null;
  }


  public void grantPrivileges(List<HivePrincipal> hivePrincipals, List<HivePrivilege> hivePrivileges,
      HivePrivilegeObject hivePrivObject, HivePrincipal grantorPrincipal, boolean grantOption)
          throws HiveAuthzPluginException, HiveAccessControlException{
  }

  @Override
  public void grantPrivileges(List<HivePrincipal> hivePrincipals, List<HivePrivilege> hivePrivileges,
      HivePrivilegeObject hivePrivObject, HivePrincipal grantorPrincipal, boolean grantOption,
      HiveAuthzContext context)
      throws HiveAuthzPluginException, HiveAccessControlException {
    grantPrivileges(hivePrincipals, hivePrivileges, hivePrivObject, grantorPrincipal, grantOption);
  }

  public void revokePrivileges(List<HivePrincipal> hivePrincipals, List<HivePrivilege> hivePrivileges,
      HivePrivilegeObject hivePrivObject, HivePrincipal grantorPrincipal, boolean grantOption)
      throws HiveAuthzPluginException, HiveAccessControlException {
  }

  @Override
  public void revokePrivileges(List<HivePrincipal> hivePrincipals, List<HivePrivilege> hivePrivileges,
      HivePrivilegeObject hivePrivObject, HivePrincipal grantorPrincipal, boolean grantOption,
      HiveAuthzContext context)
      throws HiveAuthzPluginException, HiveAccessControlException {
    revokePrivileges(hivePrincipals, hivePrivileges, hivePrivObject, grantorPrincipal, grantOption);
  }

  public void createRole(String roleName, HivePrincipal adminGrantor)
      throws HiveAuthzPluginException, HiveAccessControlException {
  }

  @Override
  public void createRole(String roleName, HivePrincipal adminGrantor, HiveAuthzContext context)
      throws HiveAuthzPluginException, HiveAccessControlException {
    createRole(roleName, adminGrantor);
  }

  public void dropRole(String roleName)
      throws HiveAuthzPluginException, HiveAccessControlException {
  }

  @Override
  public void dropRole(String roleName, HiveAuthzContext context)
      throws HiveAuthzPluginException, HiveAccessControlException {
    dropRole(roleName);
  }

  public List<HiveRoleGrant> getPrincipalGrantInfoForRole(String roleName)
      throws HiveAuthzPluginException, HiveAccessControlException {
    return null;
  }

  public List<HiveRoleGrant> getPrincipalGrantInfoForRole(String roleName, HiveAuthzContext context)
      throws HiveAuthzPluginException, HiveAccessControlException {
    return getPrincipalGrantInfoForRole(roleName);
  }

  public List<HiveRoleGrant> getRoleGrantInfoForPrincipal(HivePrincipal principal)
      throws HiveAuthzPluginException, HiveAccessControlException {
    return null;
  }


  public List<HiveRoleGrant> getRoleGrantInfoForPrincipal(HivePrincipal principal, HiveAuthzContext context)
      throws HiveAuthzPluginException, HiveAccessControlException {
    return getRoleGrantInfoForPrincipal(principal);
  }

  public void grantRole(List<HivePrincipal> hivePrincipals, List<String> roles, boolean grantOption,
      HivePrincipal grantorPrinc)
      throws HiveAuthzPluginException, HiveAccessControlException {
  }

  public void grantRole(List<HivePrincipal> hivePrincipals, List<String> roles, boolean grantOption,
      HivePrincipal grantorPrinc, HiveAuthzContext context)
      throws HiveAuthzPluginException, HiveAccessControlException {
    grantRole(hivePrincipals, roles, grantOption, grantorPrinc);
  }

  public void revokeRole(List<HivePrincipal> hivePrincipals, List<String> roles, boolean grantOption,
      HivePrincipal grantorPrinc)
      throws HiveAuthzPluginException, HiveAccessControlException {
  }

  public void revokeRole(List<HivePrincipal> hivePrincipals, List<String> roles, boolean grantOption,
      HivePrincipal grantorPrinc, HiveAuthzContext context)
      throws HiveAuthzPluginException, HiveAccessControlException {
    revokeRole(hivePrincipals, roles, grantOption, grantorPrinc);
  }

  public void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputsHObjs,
      List<HivePrivilegeObject> outputHObjs)
      throws HiveAuthzPluginException, HiveAccessControlException {
  }

  public void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputsHObjs,
      List<HivePrivilegeObject> outputHObjs, HiveAuthzContext context)
      throws HiveAuthzPluginException, HiveAccessControlException {
    checkPrivileges(hiveOpType, inputsHObjs, outputHObjs);
  }

  public List<String> getAllRoles()
      throws HiveAuthzPluginException, HiveAccessControlException {
    return null;
  }


  public List<String> getAllRoles(HiveAuthzContext context)
      throws HiveAuthzPluginException, HiveAccessControlException {
    return getAllRoles();
  }

  public List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal, HivePrivilegeObject privObj)
      throws HiveAuthzPluginException, HiveAccessControlException {
    return null;
  }

  public List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal, HivePrivilegeObject privObj,
      HiveAuthzContext context)
      throws HiveAuthzPluginException, HiveAccessControlException {
    return showPrivileges(principal, privObj);
  }

  public void setCurrentRole(String roleName)
      throws HiveAccessControlException, HiveAuthzPluginException {
  }

  public void setCurrentRole(String roleName, HiveAuthzContext hiveAuthzContext)
      throws HiveAccessControlException, HiveAuthzPluginException {
    setCurrentRole(roleName);
  }

  public List<String> getCurrentRoleNames() throws HiveAuthzPluginException {
    return null;
  }

  public List<String> getCurrentRoleNames(HiveAuthzContext hiveAuthzContext)
      throws HiveAuthzPluginException {
    return getCurrentRoleNames();
  }

}
