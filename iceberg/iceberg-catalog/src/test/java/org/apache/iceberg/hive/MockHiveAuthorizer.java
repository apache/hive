/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.hive;

import java.util.List;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.security.HiveAuthenticationProvider;
import org.apache.hadoop.hive.ql.security.authorization.plugin.AbstractHiveAuthorizer;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAuthzContext;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveOperationType;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrincipal;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilege;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeInfo;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HivePrivilegeObject;
import org.apache.hadoop.hive.ql.security.authorization.plugin.HiveRoleGrant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MockHiveAuthorizer extends AbstractHiveAuthorizer {
  public static final String PERMISSION_TEST_USER = "permission_test_user";
  private static final Logger LOG = LoggerFactory.getLogger(MockHiveAuthorizer.class);

  private final HiveAuthenticationProvider authenticator;

  public MockHiveAuthorizer(HiveAuthenticationProvider authenticator) {
    this.authenticator = authenticator;
  }

  @Override
  public VERSION getVersion() {
    return null;
  }

  @Override
  public void grantPrivileges(List<HivePrincipal> hivePrincipals, List<HivePrivilege> hivePrivileges,
      HivePrivilegeObject hivePrivObject, HivePrincipal grantorPrincipal, boolean grantOption) {
    // NOP
  }

  @Override
  public void revokePrivileges(List<HivePrincipal> hivePrincipals, List<HivePrivilege> hivePrivileges,
      HivePrivilegeObject hivePrivObject, HivePrincipal grantorPrincipal, boolean grantOption) {
    // NOP
  }

  @Override
  public void createRole(String roleName, HivePrincipal adminGrantor) {
    // NOP
  }

  @Override
  public void dropRole(String roleName) {
    // NOP
  }

  @Override
  public List<HiveRoleGrant> getPrincipalGrantInfoForRole(String roleName) {
    return List.of();
  }

  @Override
  public List<HiveRoleGrant> getRoleGrantInfoForPrincipal(HivePrincipal principal) {
    return List.of();
  }

  @Override
  public void grantRole(List<HivePrincipal> hivePrincipals, List<String> roles, boolean grantOption,
      HivePrincipal grantorPrinc) {
    // NOP
  }

  @Override
  public void revokeRole(List<HivePrincipal> hivePrincipals, List<String> roles, boolean grantOption,
      HivePrincipal grantorPrinc) {
    // NOP
  }

  @Override
  public void checkPrivileges(HiveOperationType hiveOpType, List<HivePrivilegeObject> inputsHObjs,
      List<HivePrivilegeObject> outputHObjs, HiveAuthzContext context) throws HiveAccessControlException {
    LOG.info("Checking privileges. User={}, Operation={}, inputs={}, outputs={}", authenticator.getUserName(),
        hiveOpType, inputsHObjs, outputHObjs);
    if (PERMISSION_TEST_USER.equals(authenticator.getUserName())) {
      throw new HiveAccessControlException(String.format("Unauthorized. Operation=%s, inputs=%s, outputs=%s",
          hiveOpType, inputsHObjs, outputHObjs));
    }
  }

  @Override
  public List<HivePrivilegeObject> filterListCmdObjects(List<HivePrivilegeObject> listObjs, HiveAuthzContext context) {
    return List.of();
  }

  @Override
  public List<String> getAllRoles() {
    return List.of();
  }

  @Override
  public List<HivePrivilegeInfo> showPrivileges(HivePrincipal principal, HivePrivilegeObject privObj) {
    return List.of();
  }

  @Override
  public void setCurrentRole(String roleName) {
    // NOP
  }

  @Override
  public List<String> getCurrentRoleNames() {
    return List.of();
  }

  @Override
  public void applyAuthorizationConfigPolicy(HiveConf hiveConf) {
    // NOP
  }

  @Override
  public List<HivePrivilegeObject> applyRowFilterAndColumnMasking(HiveAuthzContext context,
      List<HivePrivilegeObject> privObjs) {
    return List.of();
  }

  @Override
  public boolean needTransform() {
    return false;
  }
}
