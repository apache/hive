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

package org.apache.hadoop.hive.ql.anon.cmd;

import org.apache.hadoop.hive.metastore.api.PolicyPriv;
import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.apache.hadoop.hive.ql.anon.consts.AnonConst;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestErasureAdmin extends BaseTest {

  private static final String POLICY      = "tea_pii";
  private static final String OTHER_ADMIN = "tea_other_admin";

  private static String me;
  private static Path policyFile;

  private static final String POLICY_DSL =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA   TYPE STRING
      FOR SCHEMA 'user_info_v1'
          ERASE telephone
      """;

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] { POLICY };
  }

  @BeforeAll
  public void makeSessionTheAdmin() throws IOException, CommandProcessorException {
    me = SessionState.get().getAuthenticator().getUserName();
    Assertions.assertNotNull(me, "the test session must carry a principal");

    policyFile = Files.createTempFile("tea_", ".erp");
    Files.write(policyFile, POLICY_DSL.getBytes());

    setEnforce(false);
    execute("LOAD ERASURE POLICY " + POLICY + " FROM '" + policyFile + "'");

    setAdmins(me + "," + OTHER_ADMIN);
    setEnforce(true);
  }

  @AfterAll
  public void clearAdmin() {
    setEnforce(false);
    revokeMyGrants();
    setAdmins("");
  }

  private void setEnforce(final boolean on) {
    conf.setBoolean(AnonConst.ANON_POLICY_ENFORCE_PRIVILEGES, on);
    if (driver != null) {
      driver.getConf().setBoolean(AnonConst.ANON_POLICY_ENFORCE_PRIVILEGES, on);
    }
  }

  private void setAdmins(final String csv) {
    conf.set(AnonConst.ANON_POLICY_GRANT_ADMIN_USERS, csv);
    if (driver != null) {
      driver.getConf().set(AnonConst.ANON_POLICY_GRANT_ADMIN_USERS, csv);
    }
  }

  @Test
  @Order(1)
  public void adminCanGrantAndRevoke() throws Exception {
    execute("GRANT POLICY_VALIDATE ON ERASURE POLICY " + POLICY + " TO USER tea_alice");
    final long pid = hive.getErasurePolicy(POLICY).getPolicyId();
    List<PolicyPriv> rows = hive.listPolicyPrivs(pid, "tea_alice");
    Assertions.assertTrue(
        rows.stream().anyMatch(p -> AnonConst.PRIV_POLICY_VALIDATE.equals(p.getPrivilege())),
        "the erasure-admin's GRANT must persist; got: " + rows);

    execute("REVOKE POLICY_VALIDATE ON ERASURE POLICY " + POLICY + " FROM USER tea_alice");
    rows = hive.listPolicyPrivs(pid, "tea_alice");
    Assertions.assertFalse(
        rows.stream().anyMatch(p -> AnonConst.PRIV_POLICY_VALIDATE.equals(p.getPrivilege())),
        "the erasure-admin's REVOKE must remove the grant; remaining: " + rows);
  }

  @Test
  @Order(2)
  public void adminCannotOperate() {
    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute("VALIDATE ERASURE POLICY " + POLICY),
        "an erasure-admin must be refused operational commands");
    Assertions.assertTrue(messageChain(ex).contains("may not run erasure commands"),
        "the refusal must name the administer/operate separation; got: " + messageChain(ex));
  }

  @Test
  @Order(3)
  public void adminCannotGrantToAnotherAdmin() {
    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute("GRANT ERASE ON ERASURE POLICY " + POLICY + " TO USER " + OTHER_ADMIN),
        "granting an operational privilege to an erasure-admin must be refused");
    Assertions.assertTrue(messageChain(ex).contains("mutually exclusive"),
        "the refusal must cite the mutual exclusion; got: " + messageChain(ex));
  }

  @Test
  @Order(4)
  public void adminCannotSelfGrant() {
    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute("GRANT POLICY_VALIDATE ON ERASURE POLICY " + POLICY + " TO USER " + me),
        "an erasure-admin may not grant to itself");
    Assertions.assertTrue(messageChain(ex).contains("to itself"),
        "the refusal must name the self-grant rule; got: " + messageChain(ex));
  }

  @Test
  @Order(5)
  public void adminCanViewGrants() throws CommandProcessorException {
    final String all = execute("SHOW ERASURE GRANT").stream()
        .map(Object::toString).collect(Collectors.joining("\n"));
    Assertions.assertTrue(all.contains(me) && all.contains("ERASURE_ADMIN"),
        "the oversight view must list the erasure-admin itself; got:\n" + all);
  }

  @Test
  @Order(6)
  public void adminCanGrantCatalogueWide() throws Exception {
    execute("GRANT POLICY_VALIDATE ON ALL ERASURE POLICIES TO USER tea_dpo");
    List<PolicyPriv> rows = hive.listPolicyPrivs(0L, "tea_dpo");
    Assertions.assertTrue(
        rows.stream().anyMatch(p -> p.getPolicyId() == 0L
            && AnonConst.PRIV_POLICY_VALIDATE.equals(p.getPrivilege())),
        "catalogue-wide GRANT must persist a policyId=0 row; got: " + rows);

    execute("REVOKE POLICY_VALIDATE ON ALL ERASURE POLICIES FROM USER tea_dpo");
    rows = hive.listPolicyPrivs(0L, "tea_dpo");
    Assertions.assertFalse(
        rows.stream().anyMatch(p -> p.getPolicyId() == 0L
            && AnonConst.PRIV_POLICY_VALIDATE.equals(p.getPrivilege())),
        "catalogue-wide REVOKE must remove the policyId=0 row; remaining: " + rows);
  }

  @Test
  @Order(7)
  public void adminHoldingGrantsIsConflicted() throws Exception {
    seedValidate(0L);
    try {
      final CommandProcessorException ex = Assertions.assertThrows(
          CommandProcessorException.class,
          () -> execute("GRANT POLICY_VALIDATE ON ERASURE POLICY " + POLICY + " TO USER tea_x"),
          "a conflicted admin must not be able to grant");
      Assertions.assertTrue(messageChain(ex).contains("also holds operational"),
          "refusal must name the held-grant conflict; got: " + messageChain(ex));

      final String all = execute("SHOW ERASURE GRANT").stream()
          .map(Object::toString).collect(Collectors.joining("\n"));
      Assertions.assertTrue(all.contains("config-conflict"),
          "SHOW ERASURE GRANT must flag the conflicted admin (config-conflict); got:\n" + all);
    } finally {
      revokeMyGrants();
    }
  }

  private void seedValidate(final long pid) throws Exception {
    final PolicyPriv pp = new PolicyPriv();
    pp.setPolicyId(pid);
    pp.setPrincipalName(me);
    pp.setPrincipalType("USER");
    pp.setPrivilege(AnonConst.PRIV_POLICY_VALIDATE);
    pp.setCreateTime(System.currentTimeMillis());
    pp.setGrantor("test-setup");
    pp.setGrantorType("USER");
    pp.setGrantOption(false);
    hive.grantPolicyPriv(pp);
  }

  private void revokeMyGrants() {
    try {
      final List<PolicyPriv> held = hive.listPolicyPrivs(0L, me);
      if (held != null) {
        for (final PolicyPriv p : held) {
          try {
            hive.revokePolicyPriv(p.getPolicyPrivId());
          } catch (Exception ignore) {
          }
        }
      }
    } catch (Exception ignore) {
    }
  }

  private static String messageChain(Throwable t) {
    final StringBuilder sb = new StringBuilder();
    while (t != null) {
      if (sb.length() > 0) {
        sb.append(" | ");
      }
      sb.append(t.getMessage());
      t = t.getCause();
    }
    return sb.toString();
  }
}
