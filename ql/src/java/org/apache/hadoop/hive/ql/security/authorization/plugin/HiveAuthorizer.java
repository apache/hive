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

import org.apache.hadoop.hive.common.classification.InterfaceAudience.Public;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;
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
@Public
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
   * @throws HiveAuthorizationPluginException
   */
  void grantPrivileges(List<HivePrincipal> hivePrincipals, List<HivePrivilege> hivePrivileges,
      HivePrivilegeObject hivePrivObject, HivePrincipal grantorPrincipal, boolean grantOption)
      throws HiveAuthorizationPluginException;

  /**
   * Revoke privileges for principals on the object
   * @param hivePrincipals
   * @param hivePrivileges
   * @param hivePrivObject
   * @param grantorPrincipal
   * @param grantOption
   * @throws HiveAuthorizationPluginException
   */
  void revokePrivileges(List<HivePrincipal> hivePrincipals, List<HivePrivilege> hivePrivileges,
      HivePrivilegeObject hivePrivObject, HivePrincipal grantorPrincipal, boolean grantOption)
      throws HiveAuthorizationPluginException;


  /**
   * Create role
   * @param roleName
   * @param adminGrantor - The user in "[ WITH ADMIN <user> ]" clause of "create role"
   * @throws HiveAuthorizationPluginException
   */
  void createRole(String roleName, HivePrincipal adminGrantor)
      throws HiveAuthorizationPluginException;

  /**
   * Drop role
   * @param roleName
   * @throws HiveAuthorizationPluginException
   */
  void dropRole(String roleName)
      throws HiveAuthorizationPluginException;

  /**
   * Get roles that this user/role belongs to
   * @param hivePrincipal - user or role
   * @return list of roles
   * @throws HiveAuthorizationPluginException
   */
  List<String> getRoles(HivePrincipal hivePrincipal)
      throws HiveAuthorizationPluginException;

  /**
   * Grant roles in given roles list to principals in given hivePrincipals list
   * @param hivePrincipals
   * @param roles
   * @param grantOption
   * @param grantorPrinc
   * @throws HiveAuthorizationPluginException
   */
  void grantRole(List<HivePrincipal> hivePrincipals, List<String> roles, boolean grantOption,
      HivePrincipal grantorPrinc)
      throws HiveAuthorizationPluginException;


  /**
   * Revoke roles in given roles list to principals in given hivePrincipals list
   * @param hivePrincipals
   * @param roles
   * @param grantOption
   * @param grantorPrinc
   * @throws HiveAuthorizationPluginException
   */
  void revokeRole(List<HivePrincipal> hivePrincipals, List<String> roles, boolean grantOption,
      HivePrincipal grantorPrinc)
      throws HiveAuthorizationPluginException;

  /**
   * Check if user has privileges to do this action on these objects
   * @param hiveOpType
   * @param inputsHObjs
   * @param outputHObjs
   * @throws HiveAuthorizationPluginException
   */
  void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputsHObjs,
      List<HivePrivilegeObject> outputHObjs)
      throws HiveAuthorizationPluginException;

  /**
   * @return all existing roles
   * @throws HiveAuthorizationPluginException
   */
  List<String> getAllRoles()
      throws HiveAuthorizationPluginException;

  /**
   * Show privileges for given principal on given object
   * @param principal
   * @param privObj
   * @return
   * @throws HiveAuthorizationPluginException
   */
  List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal, HivePrivilegeObject privObj)
      throws HiveAuthorizationPluginException;


  //other functions to be added -
  //showUsersInRole(rolename)
  //isSuperuser(username)


}

