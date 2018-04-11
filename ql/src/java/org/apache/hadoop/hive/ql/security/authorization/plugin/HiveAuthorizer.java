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

import java.util.List;

import org.apache.hadoop.hive.common.classification.InterfaceAudience.LimitedPrivate;
import org.apache.hadoop.hive.common.classification.InterfaceStability.Evolving;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;

/**
 * Interface for hive authorization plugins. Plugins will be better shielded from changes
 * to this interface by extending AbstractHiveAuthorizer instead of extending this
 * interface directly.
 *
 * Note that this interface is for limited use by specific apache projects, including
 * Apache Ranger (formerly known as Argus), and Apache Sentry, and is subject to
 * change across releases.
 *
 * Used by the DDLTasks for access control statement,
 * and for checking authorization from Driver.doAuthorization()
 *
 * This a more generic version of
 *  {@link HiveAuthorizationProvider} that lets you define the behavior of access control
 *  statements and does not make assumptions about the privileges needed for a hive operation.
 * This is referred to as V2 authorizer in other parts of the code.
 */
@LimitedPrivate(value = { "Apache Argus (incubating)" })
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
   * @param context
   * @throws HiveAuthzPluginException
   * @throws HiveAccessControlException
   */
  void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputsHObjs,
      List<HivePrivilegeObject> outputHObjs, HiveAuthzContext context)
      throws HiveAuthzPluginException, HiveAccessControlException;


  /**
   * Filter out any objects that should not be shown to the user, from the list of
   * tables or databases coming from a 'show tables' or 'show databases' command
   * @param listObjs List of all objects obtained as result of a show command
   * @param context
   * @return filtered list of objects that will be returned to the user invoking the command
   * @throws HiveAuthzPluginException
   * @throws HiveAccessControlException
   */
  List<HivePrivilegeObject> filterListCmdObjects(List<HivePrivilegeObject> listObjs,
      HiveAuthzContext context)
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
   * @throws HiveAuthzPluginException
   */
  void applyAuthorizationConfigPolicy(HiveConf hiveConf) throws HiveAuthzPluginException;

  /**
   * Get a {@link HiveAuthorizationTranslator} implementation. See
   * {@link HiveAuthorizationTranslator} for details. Return null if no
   * customization is needed. Most implementations are expected to return null.
   *
   * The java signature of the method makes it necessary to only return Object
   * type so that older implementations can extend the interface to build
   * against older versions of Hive that don't include this additional method
   * and HiveAuthorizationTranslator class. However, if a non null value is
   * returned, the Object has to be of type HiveAuthorizationTranslator
   *
   * @return
   * @throws HiveException
   */
  Object getHiveAuthorizationTranslator() throws HiveAuthzPluginException;

  /**
   * TableMaskingPolicy defines how users can access base tables. It defines a
   * policy on what columns and rows are hidden, masked or redacted based on
   * user, role or location.
   */
  /**
   * applyRowFilterAndColumnMasking is called once for each table in a query. 
   * (part 1) It expects a valid filter condition to be returned. Null indicates no filtering is
   * required.
   *
   * Example: table foo(c int) -> "c > 0 && c % 2 = 0"
   *
   * (part 2) It expects a valid expression as used in a select clause. Null
   * is NOT a valid option. If no transformation is needed simply return the
   * column name.
   *
   * Example: column a -> "a" (no transform)
   *
   * Example: column a -> "reverse(a)" (call the reverse function on a)
   *
   * Example: column a -> "5" (replace column a with the constant 5)
   *
   * @return List<HivePrivilegeObject>
   * please return the list of HivePrivilegeObjects that need to be rewritten.
   *
   * @throws SemanticException
   */
  List<HivePrivilegeObject> applyRowFilterAndColumnMasking(HiveAuthzContext context,
      List<HivePrivilegeObject> privObjs) throws SemanticException;

  /**
   * needTransform() is called once per user in a query. 
   * Returning false short-circuits the generation of row/column transforms.
   *
   * @return
   * @throws SemanticException
   */
  boolean needTransform();

  /**
   * @return HivePolicyProvider instance (expected to be a singleton)
   * @throws HiveAuthzPluginException
   */
  HivePolicyProvider getHivePolicyProvider() throws HiveAuthzPluginException;
}
