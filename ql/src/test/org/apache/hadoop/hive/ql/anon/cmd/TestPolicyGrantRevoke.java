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

import org.apache.hadoop.hive.metastore.api.ErasurePolicy;
import org.apache.hadoop.hive.metastore.api.PolicyPriv;
import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.apache.hadoop.hive.ql.anon.consts.AnonConst;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
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

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestPolicyGrantRevoke extends BaseTest {

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] { POLICY };
  }

  private static final String POLICY    = "tpgr_pii";
  private static final String PRINCIPAL = "alice";

  private static final String POLICY_DSL =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA   TYPE STRING
      FOR SCHEMA 'user_info_v1'
          ERASE telephone
      """;

  private static Path policyFile;
  private static long policyId = -1L;

  @BeforeAll
  public void provisionPolicy() throws IOException, CommandProcessorException, HiveException {
    policyFile = Files.createTempFile("tpgr_", ".erp");
    Files.write(policyFile, POLICY_DSL.getBytes());

    execute("LOAD ERASURE POLICY " + POLICY + " FROM '" + policyFile + "'");
    final ErasurePolicy ep = hive.getErasurePolicy(POLICY);
    Assertions.assertNotNull(ep, "LOAD must persist a policy header");
    Assertions.assertTrue(ep.isSetPolicyId() && ep.getPolicyId() > 0L,
        "policy header must carry a policyId");
    policyId = ep.getPolicyId();
  }


  @Test
  @Order(1)
  public void grantPersistsRow() throws CommandProcessorException, HiveException {
    execute("GRANT POLICY_VALIDATE ON ERASURE POLICY %s TO USER %s".formatted(POLICY, PRINCIPAL));

    final List<PolicyPriv> rows = hive.listPolicyPrivs(policyId, PRINCIPAL);
    Assertions.assertNotNull(rows, "listPolicyPrivs must not return null");
    Assertions.assertTrue(rows.stream().anyMatch(p ->
            p.getPolicyId() == policyId
         && PRINCIPAL.equals(p.getPrincipalName())
         && AnonConst.PRIV_POLICY_VALIDATE.equals(p.getPrivilege())),
        "GRANT must persist a row matching (policyId, principal, privilege); got: " + rows);
  }

  @Test
  @Order(2)
  public void revokeRemovesRow() throws CommandProcessorException, HiveException {
    final List<PolicyPriv> before = hive.listPolicyPrivs(policyId, PRINCIPAL);
    Assertions.assertTrue(before.stream().anyMatch(p ->
            AnonConst.PRIV_POLICY_VALIDATE.equals(p.getPrivilege())),
        "precondition: grant from order=1 must still exist; got: " + before);

    execute("REVOKE POLICY_VALIDATE ON ERASURE POLICY %s FROM USER %s".formatted(POLICY, PRINCIPAL));

    final List<PolicyPriv> after = hive.listPolicyPrivs(policyId, PRINCIPAL);
    Assertions.assertFalse(after.stream().anyMatch(p ->
            AnonConst.PRIV_POLICY_VALIDATE.equals(p.getPrivilege())),
        "REVOKE must remove the matching grant row; remaining: " + after);
  }


  @Test
  @Order(10)
  public void grantUnknownPrivilegeRefuses() {
    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute("GRANT POLICY_BOGUS ON ERASURE POLICY %s TO USER %s".formatted(POLICY, PRINCIPAL)));
    Assertions.assertTrue(messageChain(ex).contains("not a recognised"),
        "unknown privilege must be rejected at analyzer time; got: " + messageChain(ex));
  }

  @Test
  @Order(11)
  public void grantUnknownPolicyRefuses() {
    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute("GRANT POLICY_VALIDATE ON ERASURE POLICY tpgr_no_such_policy TO USER %s"
            .formatted(PRINCIPAL)));
    Assertions.assertTrue(messageChain(ex).contains("not found"),
        "missing policy must be reported; got: " + messageChain(ex));
  }

  @Test
  @Order(12)
  public void revokeWithoutMatchingGrantRefuses() {
    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute("REVOKE POLICY_VALIDATE ON ERASURE POLICY %s FROM USER bob".formatted(POLICY)));
    Assertions.assertTrue(messageChain(ex).contains("no POLICY_VALIDATE grant exists"),
        "REVOKE without a matching row must surface a clear error; got: " + messageChain(ex));
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
