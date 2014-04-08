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
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;

/**
 * Interface for hive authorization plugins.
 * Used by the DDLTasks for access control statement,
 * and for checking authorization from Driver.doAuthorization()
 *
 * This a more generic version of
 *  {@link HiveAuthorizationProvider} that lets you define the behavior of access control
 *  statements and does not make assumptions about the privileges needed for a hive operation.
 * This is referred to as V2 authorizer in other parts of the code.
 */
@LimitedPrivate(value = { "" })
@Evolving
public interface HiveAuthorizer {

  public enum VERSION { V1 };

  /**
   * @return version of HiveAuthorizer interface that is implemented by this instance
   */
  public VERSION getVersion();

  /**
   * Grant privileges for principals on the object
   * @param hivePrincipals
   * @param hivePrivileges
   * @param hivePrivObject
   * @param grantorPrincipal
   * @param grantOption
   * @throws HiveAuthzPluginException
   * @throws HiveAccessControlException
   */
  void grantPrivileges(List<HivePrincipal> hivePrincipals, List<HivePrivilege> hivePrivileges,
      HivePrivilegeObject hivePrivObject, HivePrincipal grantorPrincipal, boolean grantOption)
      throws HiveAuthzPluginException, HiveAccessControlException;

  /**
   * Revoke privileges for principals on the object
   * @param hivePrincipals
   * @param hivePrivileges
   * @param hivePrivObject
   * @param grantorPrincipal
   * @param grantOption
   * @throws HiveAuthzPluginException
   * @throws HiveAccessControlException
   */
  void revokePrivileges(List<HivePrincipal> hivePrincipals, List<HivePrivilege> hivePrivileges,
      HivePrivilegeObject hivePrivObject, HivePrincipal grantorPrincipal, boolean grantOption)
      throws HiveAuthzPluginException, HiveAccessControlException;


  /**
   * Create role
   * @param roleName
   * @param adminGrantor - The user in "[ WITH ADMIN <user> ]" clause of "create role"
   * @throws HiveAuthzPluginException
   * @throws HiveAccessControlException
   */
  void createRole(String roleName, HivePrincipal adminGrantor)
      throws HiveAuthzPluginException, HiveAccessControlException;

  /**
   * Drop role
   * @param roleName
   * @throws HiveAuthzPluginException
   * @throws HiveAccessControlException
   */
  void dropRole(String roleName)
      throws HiveAuthzPluginException, HiveAccessControlException;

  /**
   * Get the grant information for principals granted the given role
   * @param roleName
   * @return
   * @throws HiveAuthzPluginException
   * @throws HiveAccessControlException
   */
  List<HiveRoleGrant> getPrincipalGrantInfoForRole(String roleName)
      throws HiveAuthzPluginException, HiveAccessControlException;


  /**
   * Get the grant information of roles the given principal belongs to
   * @param principal
   * @return
   * @throws HiveAuthzPluginException
   * @throws HiveAccessControlException
   */
  List<HiveRoleGrant> getRoleGrantInfoForPrincipal(HivePrincipal principal)
      throws HiveAuthzPluginException, HiveAccessControlException;

  /**
   * Grant roles in given roles list to principals in given hivePrincipals list
   * @param hivePrincipals
   * @param roles
   * @param grantOption
   * @param grantorPrinc
   * @throws HiveAuthzPluginException
   * @throws HiveAccessControlException
   */
  void grantRole(List<HivePrincipal> hivePrincipals, List<String> roles, boolean grantOption,
      HivePrincipal grantorPrinc)
      throws HiveAuthzPluginException, HiveAccessControlException;


  /**
   * Revoke roles in given roles list to principals in given hivePrincipals list
   * @param hivePrincipals
   * @param roles
   * @param grantOption
   * @param grantorPrinc
   * @throws HiveAuthzPluginException
   * @throws HiveAccessControlException
   */
  void revokeRole(List<HivePrincipal> hivePrincipals, List<String> roles, boolean grantOption,
      HivePrincipal grantorPrinc)
      throws HiveAuthzPluginException, HiveAccessControlException;

  /**
   * Check if user has privileges to do this action on these objects
   * @param hiveOpType
   * @param inputsHObjs
   * @param outputHObjs
   * @throws HiveAuthzPluginException
   * @throws HiveAccessControlException
   */
  void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputsHObjs,
      List<HivePrivilegeObject> outputHObjs)
      throws HiveAuthzPluginException, HiveAccessControlException;

  /**
   * @return all existing roles
   * @throws HiveAuthzPluginException
   * @throws HiveAccessControlException
   */
  List<String> getAllRoles()
      throws HiveAuthzPluginException, HiveAccessControlException;

  /**
   * Show privileges for given principal on given object
   * @param principal
   * @param privObj
   * @return
   * @throws HiveAuthzPluginException
   * @throws HiveAccessControlException
   */
  List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal, HivePrivilegeObject privObj)
      throws HiveAuthzPluginException, HiveAccessControlException;

  /**
   * Set the current role to roleName argument
   * @param roleName
   * @throws HiveAccessControlException
   * @throws HiveAuthzPluginException
   */
  void setCurrentRole(String roleName) throws HiveAccessControlException, HiveAuthzPluginException;

  /**
   * @return List having names of current roles
   * @throws HiveAuthzPluginException
   */
  List<String> getCurrentRoleNames() throws HiveAuthzPluginException;

  /**
   * Modify the given HiveConf object to configure authorization related parameters
   * or other parameters related to hive security
   * @param hiveConf
   */
  public void applyAuthorizationConfigPolicy(HiveConf hiveConf);

}

