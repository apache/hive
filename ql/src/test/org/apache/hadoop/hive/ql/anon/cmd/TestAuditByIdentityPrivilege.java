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

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestAuditByIdentityPrivilege extends BaseTest {

  private static final String POLICY = "tabi_pii";
  private static String me;
  private static long policyId;
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
  public void setUp() throws Exception {
    me = SessionState.get().getAuthenticator().getUserName();
    Assertions.assertNotNull(me, "the test session must carry a principal");
    policyFile = Files.createTempFile("tabi_", ".erp");
    Files.write(policyFile, POLICY_DSL.getBytes());

    setEnforce(false);
    execute("LOAD ERASURE POLICY " + POLICY + " FROM '" + policyFile + "'");
    policyId = hive.getErasurePolicy(POLICY).getPolicyId();
    revokeValidate();
    setEnforce(true);
  }

  @AfterAll
  public void tearDown() throws Exception {
    setEnforce(false);
    revokeValidate();
  }

  @Test
  @Order(1)
  public void perPolicyValidateIsRefused() throws Exception {
    seedValidate(policyId);
    try {
      final CommandProcessorException ex = Assertions.assertThrows(
          CommandProcessorException.class,
          () -> execute("AUDIT BY IDENTITY VALUES (1)"),
          "a per-policy POLICY_VALIDATE holder must not run the org-wide audit");
      Assertions.assertTrue(messageChain(ex).contains("lacks " + AnonConst.PRIV_POLICY_VALIDATE),
          "refusal must report the missing catalogue-wide POLICY_VALIDATE; got: " + messageChain(ex));
    } finally {
      revokeValidate();
    }
  }

  @Test
  @Order(2)
  public void catalogueWideValidateIsAllowed() throws Exception {
    seedValidate(0L);
    try {
      execute("AUDIT BY IDENTITY VALUES (1)");
    } finally {
      revokeValidate();
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

  private void revokeValidate() throws Exception {
    for (final long pid : new long[] { 0L, policyId }) {
      final List<PolicyPriv> rows = hive.listPolicyPrivs(pid, me);
      if (rows == null) {
        continue;
      }
      for (final PolicyPriv p : rows) {
        if (AnonConst.PRIV_POLICY_VALIDATE.equals(p.getPrivilege())) {
          try {
            hive.revokePolicyPriv(p.getPolicyPrivId());
          } catch (Exception ignore) {
          }
        }
      }
    }
  }

  private void setEnforce(final boolean on) {
    conf.setBoolean(AnonConst.ANON_POLICY_ENFORCE_PRIVILEGES, on);
    if (driver != null) {
      driver.getConf().setBoolean(AnonConst.ANON_POLICY_ENFORCE_PRIVILEGES, on);
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
