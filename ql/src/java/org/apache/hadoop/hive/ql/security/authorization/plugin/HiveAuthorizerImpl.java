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

/**
 * Convenience implementation of HiveAuthorizer.
 * You can customize the behavior by passing different implementations of
 * {@link HiveAccessController} and {@link HiveAuthorizationValidator} to constructor.
 *
 */
@Public
@Evolving
public class HiveAuthorizerImpl implements HiveAuthorizer {
  HiveAccessController accessController;
  HiveAuthorizationValidator authValidator;

   HiveAuthorizerImpl(HiveAccessController accessController, HiveAuthorizationValidator authValidator){
     this.accessController = accessController;
     this.authValidator = authValidator;
   }

  @Override
  public void grantPrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) {
    accessController.grantPrivileges(hivePrincipals, hivePrivileges, hivePrivObject,
        grantorPrincipal, grantOption);
  }

  @Override
  public void revokePrivileges(List<HivePrincipal> hivePrincipals,
      List<HivePrivilege> hivePrivileges, HivePrivilegeObject hivePrivObject,
      HivePrincipal grantorPrincipal, boolean grantOption) {
    accessController.revokePrivileges(hivePrincipals, hivePrivileges, hivePrivObject,
        grantorPrincipal, grantOption);
  }

  @Override
  public void createRole(String roleName, HivePrincipal adminGrantor) {
    accessController.createRole(roleName, adminGrantor);
  }

  @Override
  public void dropRole(String roleName) {
    accessController.dropRole(roleName);
  }

  @Override
  public List<String> getRoles(HivePrincipal hivePrincipal) {
    return accessController.getRoles(hivePrincipal);
  }

  @Override
  public void grantRole(List<HivePrincipal> hivePrincipals, List<String> roles,
      boolean grantOption, HivePrincipal grantorPrinc) {
    accessController.grantRole(hivePrincipals, roles, grantOption, grantorPrinc);
  }

  @Override
  public void revokeRole(List<HivePrincipal> hivePrincipals, List<String> roles,
      boolean grantOption, HivePrincipal grantorPrinc) {
    accessController.revokeRole(hivePrincipals, roles, grantOption, grantorPrinc);
  }

  @Override
  public void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputHObjs,
      List<HivePrivilegeObject> outputHObjs) {
    authValidator.checkPrivileges(hiveOpType, inputHObjs, outputHObjs);
  }

  @Override
  public List<String> getAllRoles() {
    return accessController.getAllRoles();
  }


 // other access control functions

//   void validateAuthority(HiveAction, inputs, outputs){
//     authValidator.validateAuthority(HiveAction, inputs, outputs);
//   }
}
