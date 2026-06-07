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
 */
package org.apache.hadoop.hive.ql.anon.cmd;

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

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestPolicyPrivileges extends BaseTest {

  private static final String POLICY = "tpp_pii";
  private static final String TABLE  = "tpp_t";
  private static final String COLUMN = "b";

  private static Path policyFile;

  private static final String POLICY_DSL =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA   TYPE STRING
      FOR SCHEMA 'user_info_v1'
          ERASE telephone
      """;

  @BeforeAll
  public void enableEnforcement() throws IOException {
    policyFile = Files.createTempFile("tpp_", ".erp");
    Files.write(policyFile, POLICY_DSL.getBytes());
    conf.setBoolean(AnonConst.ANON_POLICY_ENFORCE_PRIVILEGES, true);
    if (driver != null) {
      driver.getConf().setBoolean(AnonConst.ANON_POLICY_ENFORCE_PRIVILEGES, true);
    }
  }

  @AfterAll
  public void disableEnforcement() {
    conf.setBoolean(AnonConst.ANON_POLICY_ENFORCE_PRIVILEGES, false);
    if (driver != null) {
      driver.getConf().setBoolean(AnonConst.ANON_POLICY_ENFORCE_PRIVILEGES, false);
    }
  }


  @Test
  @Order(1)
  public void loadRequiresPolicyValidate() {
    assertLacks(AnonConst.PRIV_POLICY_VALIDATE,
        "LOAD ERASURE POLICY " + POLICY + " FROM '" + policyFile + "'");
  }

  @Test
  @Order(2)
  public void validateRequiresPolicyValidate() {
    assertLacks(AnonConst.PRIV_POLICY_VALIDATE,
        "VALIDATE ERASURE POLICY " + POLICY);
  }

  @Test
  @Order(3)
  public void describeRequiresPolicyValidate() {
    assertLacks(AnonConst.PRIV_POLICY_VALIDATE,
        "DESCRIBE ERASURE POLICY " + POLICY);
  }

  @Test
  @Order(4)
  public void showPoliciesRequiresPolicyValidate() {
    assertLacks(AnonConst.PRIV_POLICY_VALIDATE,
        "SHOW DATA ERASURE POLICIES");
  }

  @Test
  @Order(5)
  public void showGrantsRequiresValidateOrAdmin() {
    assertGrantRefused("SHOW ERASURE GRANT",
        "may not inspect erasure grants", "ERASURE_ADMIN role or POLICY_VALIDATE");
  }

  @Test
  @Order(6)
  public void showLocksRequiresPolicyValidate() {
    assertLacks(AnonConst.PRIV_POLICY_VALIDATE,
        "SHOW ERASURE LOCKS");
  }

  @Test
  @Order(7)
  public void invalidateRequiresPolicyValidate() {
    assertLacks(AnonConst.PRIV_POLICY_VALIDATE,
        "INVALIDATE ERASURE POLICY " + POLICY);
  }

  @Test
  @Order(33)
  public void explainAttachRequiresPolicyManage() {
    assertLacks(AnonConst.PRIV_POLICY_MANAGE,
        "EXPLAIN ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s".formatted(POLICY, TABLE, COLUMN));
  }

  @Test
  @Order(8)
  public void auditPolicyRequiresPolicyValidate() {
    assertLacks(AnonConst.PRIV_POLICY_VALIDATE,
        "AUDIT ERASURE POLICY " + POLICY);
  }

  @Test
  @Order(9)
  public void auditBindingRequiresPolicyValidate() {
    assertLacks(AnonConst.PRIV_POLICY_VALIDATE,
        "AUDIT ERASURE BINDING ON TABLE " + TABLE + " COLUMN " + COLUMN);
  }

  @Test
  @Order(10)
  public void auditExecutionsRequiresPolicyValidate() {
    assertLacks(AnonConst.PRIV_POLICY_VALIDATE,
        "AUDIT ERASURE EXECUTIONS ON TABLE " + TABLE);
  }


  @Test
  @Order(20)
  public void activateRequiresPolicyActivate() {
    assertLacks(AnonConst.PRIV_POLICY_ACTIVATE,
        "ACTIVATE ERASURE POLICY " + POLICY);
  }

  @Test
  @Order(21)
  public void deactivateRequiresPolicyActivate() {
    assertLacks(AnonConst.PRIV_POLICY_ACTIVATE,
        "DEACTIVATE ERASURE POLICY " + POLICY);
  }


  @Test
  @Order(30)
  public void attachRequiresPolicyManage() {
    assertLacks(AnonConst.PRIV_POLICY_MANAGE,
        "ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s".formatted(POLICY, TABLE, COLUMN));
  }

  @Test
  @Order(31)
  public void detachRequiresPolicyManage() {
    assertLacks(AnonConst.PRIV_POLICY_MANAGE,
        "DETACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s".formatted(POLICY, TABLE, COLUMN));
  }

  @Test
  @Order(32)
  public void releaseLockRequiresPolicyManage() {
    assertLacks(AnonConst.PRIV_POLICY_MANAGE,
        "RELEASE ERASURE LOCK ON TABLE %s WITH REASON 'privilege-test'".formatted(TABLE));
  }

  @Test
  @Order(34)
  public void forceReleaseWithoutPrivilegeRefusesOnPrivilegeFirst() {
    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute("RELEASE ERASURE LOCK ON TABLE " + TABLE + " FORCE"));
    final String chain = collectMessages(ex);
    Assertions.assertTrue(chain.contains("lacks " + AnonConst.PRIV_POLICY_MANAGE),
        "refusal must come from the POLICY_MANAGE check, not the WITH REASON validator; got: "
            + chain);
    Assertions.assertFalse(chain.contains("WITH REASON"),
        "the WITH REASON validator must not run before the privilege gate; got: " + chain);
  }


  @Test
  @Order(40)
  public void eraseRequiresErasePrivilege() {
    assertLacks(AnonConst.PRIV_ERASE,
        "ERASE FROM TABLE %s FOR IDENTITY VALUES (1)".formatted(TABLE));
  }


  @Test
  @Order(50)
  public void grantWithoutHoldingPrivilegeRefused() {
    assertGrantRefused(
        "GRANT POLICY_VALIDATE ON ERASURE POLICY " + POLICY + " TO USER alice",
        "may not grant", "only an ERASURE_ADMIN");
  }

  @Test
  @Order(51)
  public void revokeWithoutHoldingPrivilegeRefused() {
    assertGrantRefused(
        "REVOKE POLICY_VALIDATE ON ERASURE POLICY " + POLICY + " FROM USER alice",
        "may not revoke", "only an ERASURE_ADMIN");
  }

  @Test
  @Order(52)
  public void newUserCannotSelfGrant() {
    final String me = SessionState.get().getAuthenticator().getUserName();
    Assertions.assertNotNull(me, "the test session must carry a principal");
    assertGrantRefused(
        "GRANT POLICY_VALIDATE ON ERASURE POLICY " + POLICY + " TO USER " + me,
        "may not grant", "only an ERASURE_ADMIN");
  }

  @Test
  @Order(54)
  public void catalogueWideGrantRequiresAdmin() {
    assertGrantRefused(
        "GRANT POLICY_VALIDATE ON ALL ERASURE POLICIES TO USER alice",
        "may not grant", "only an ERASURE_ADMIN");
  }


  @Test
  @Order(60)
  public void enforcementOnByDefaultWhenFlagUnset() {
    conf.unset(AnonConst.ANON_POLICY_ENFORCE_PRIVILEGES);
    driver.getConf().unset(AnonConst.ANON_POLICY_ENFORCE_PRIVILEGES);
    try {
      assertLacks(AnonConst.PRIV_POLICY_VALIDATE, "VALIDATE ERASURE POLICY " + POLICY);
    } finally {
      conf.setBoolean(AnonConst.ANON_POLICY_ENFORCE_PRIVILEGES, true);
      driver.getConf().setBoolean(AnonConst.ANON_POLICY_ENFORCE_PRIVILEGES, true);
    }
  }


  private void assertLacks(String expectedPrivilege, String sql) {
    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute(sql),
        "command must refuse without a " + expectedPrivilege + " grant");
    final String chain = collectMessages(ex);
    Assertions.assertTrue(chain.contains("lacks " + expectedPrivilege),
        "error must report missing " + expectedPrivilege + "; got: " + chain);
  }

  private void assertGrantRefused(String sql, String... expectedFragments) {
    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute(sql),
        "command must be refused: " + sql);
    final String chain = collectMessages(ex);
    for (final String fragment : expectedFragments) {
      Assertions.assertTrue(chain.contains(fragment),
          "refusal message must contain '" + fragment + "'; got: " + chain);
    }
  }

  private static String collectMessages(Throwable t) {
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
