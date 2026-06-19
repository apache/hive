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

package org.apache.hadoop.hive.ql.anon.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.PolicyPriv;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.ANON_POLICY_GRANT_ADMIN_USERS;
import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.PRIV_ERASE;

public class TestPolicyGrantAuthority {

  private static final long POLICY = 5L;

  private static PolicyPriv priv(final String privilege, final long policyId) {
    final PolicyPriv p = new PolicyPriv();
    p.setPrivilege(privilege);
    p.setPolicyId(policyId);
    return p;
  }

  @Test
  public void testNonAdminCannotGrant() {
    Assertions.assertFalse(PolicyPrivilegeUtils.canGrant(false, false, "alice", "bob"),
        "a principal that is not an erasure-admin may not grant");
  }

  @Test
  public void testAdminGrantsToOperator() {
    Assertions.assertTrue(PolicyPrivilegeUtils.canGrant(true, false, "erasure_admin", "bob"),
        "an erasure-admin may grant to a non-admin principal");
  }

  @Test
  public void testAdminCannotSelfGrant() {
    Assertions.assertFalse(
        PolicyPrivilegeUtils.canGrant(true, true, "erasure_admin", "erasure_admin"),
        "an erasure-admin may not grant to itself");
  }

  @Test
  public void testCannotGrantToAnotherAdmin() {
    Assertions.assertFalse(PolicyPrivilegeUtils.canGrant(true, true, "admin_a", "admin_b"),
        "an operational privilege must not be grantable to another erasure-admin");
  }

  @Test
  public void testIsErasureAdmin() {
    final Configuration conf = new Configuration(false);
    conf.set(ANON_POLICY_GRANT_ADMIN_USERS, "erasure_admin, dpo_admin");
    Assertions.assertTrue(PolicyPrivilegeUtils.isErasureAdmin(conf, "erasure_admin"));
    Assertions.assertTrue(PolicyPrivilegeUtils.isErasureAdmin(conf, "dpo_admin"),
        "membership tolerates surrounding whitespace in the list");
    Assertions.assertFalse(PolicyPrivilegeUtils.isErasureAdmin(conf, "bob"));
    Assertions.assertFalse(
        PolicyPrivilegeUtils.isErasureAdmin(new Configuration(false), "erasure_admin"),
        "with no admin list configured, nobody is an erasure-admin");
  }

  @Test
  public void testHoldsSemantics() {
    Assertions.assertFalse(PolicyPrivilegeUtils.holds(null, PRIV_ERASE, POLICY));
    Assertions.assertFalse(PolicyPrivilegeUtils.holds(Collections.emptyList(), PRIV_ERASE, POLICY));
    Assertions.assertFalse(
        PolicyPrivilegeUtils.holds(Collections.singletonList(priv(PRIV_ERASE, 7L)), PRIV_ERASE, POLICY),
        "a per-policy holding on a different policy must not match");
    Assertions.assertTrue(
        PolicyPrivilegeUtils.holds(Collections.singletonList(priv(PRIV_ERASE, POLICY)), PRIV_ERASE, POLICY));
    Assertions.assertTrue(
        PolicyPrivilegeUtils.holds(Collections.singletonList(priv(PRIV_ERASE, 0L)), PRIV_ERASE, POLICY),
        "a wildcard (policyId 0) holding satisfies USE on any policy");
  }
}
