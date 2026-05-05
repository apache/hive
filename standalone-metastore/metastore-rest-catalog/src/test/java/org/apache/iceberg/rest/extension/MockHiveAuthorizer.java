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

package org.apache.iceberg.rest.extension;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
  public static final String READ_ONLY_USER = "read_only_user";
  private static final Logger LOG = LoggerFactory.getLogger(MockHiveAuthorizer.class);
  private static final List<PrivilegeCheck> PRIVILEGE_CHECKS = new CopyOnWriteArrayList<>();

  private final HiveAuthenticationProvider authenticator;

  public MockHiveAuthorizer(HiveAuthenticationProvider authenticator) {
    this.authenticator = authenticator;
  }

  public record PrivilegeCheck(HiveOperationType operationType, List<HivePrivilegeObject> inputs,
                               List<HivePrivilegeObject> outputs) {
  }

  public static void clearPrivilegeChecks() {
    PRIVILEGE_CHECKS.clear();
  }

  public static List<PrivilegeCheck> privilegeChecks() {
    return new ArrayList<>(PRIVILEGE_CHECKS);
  }

  private static List<HivePrivilegeObject> copyPrivilegeObjects(List<HivePrivilegeObject> objects) {
    return objects == null ? List.of() : List.copyOf(objects);
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
    PRIVILEGE_CHECKS.add(new PrivilegeCheck(hiveOpType, copyPrivilegeObjects(inputsHObjs),
        copyPrivilegeObjects(outputHObjs)));
    if (PERMISSION_TEST_USER.equals(authenticator.getUserName())) {
      throw new HiveAccessControlException(String.format("Unauthorized. User=%s, Operation=%s, inputs=%s, outputs=%s",
          authenticator.getUserName(), hiveOpType, inputsHObjs, outputHObjs));
    }
    if (READ_ONLY_USER.equals(authenticator.getUserName()) && !outputHObjs.isEmpty()) {
      throw new HiveAccessControlException(String.format("Unauthorized. User=%s, Operation=%s, inputs=%s, outputs=%s",
          authenticator.getUserName(), hiveOpType, inputsHObjs, outputHObjs));
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
