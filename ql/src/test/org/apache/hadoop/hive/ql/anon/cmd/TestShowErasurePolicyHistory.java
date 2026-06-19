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
public class TestShowErasurePolicyHistory extends BaseTest {

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] { POLICY };
  }

  private static final String POLICY = "tseph_pii";

  private static final String POLICY_DSL =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA   TYPE STRING
      FOR SCHEMA 'user_info_v1'
          ERASE telephone
      """;

  private static Path policyFile;

  @BeforeAll
  public void writePolicyFile() throws IOException {
    policyFile = Files.createTempFile("tseph_", ".erp");
    Files.write(policyFile, POLICY_DSL.getBytes());
  }

  @Test
  @Order(1)
  public void historyShowsEveryTransition()
      throws CommandProcessorException, HiveException, IOException {
    execute("LOAD     ERASURE POLICY " + POLICY + " FROM '" + policyFile + "'");
    execute("VALIDATE ERASURE POLICY " + POLICY);
    execute("ACTIVATE ERASURE POLICY " + POLICY);
    execute("DEACTIVATE ERASURE POLICY " + POLICY);

    final List<Object> rows = execute("SHOW ERASURE POLICY " + POLICY + " HISTORY");
    Assertions.assertNotNull(rows, "SHOW HISTORY must return a result list");
    Assertions.assertFalse(rows.isEmpty(), "SHOW HISTORY must surface at least one event");

    final StringBuilder blob = new StringBuilder();
    for (final Object row : rows) {
      blob.append(row.toString()).append('\n');
    }
    final String text = blob.toString();
    Assertions.assertTrue(text.contains("LOADED"),
        "history must include LOADED event; got: " + text);
    Assertions.assertTrue(text.contains("VALIDATED"),
        "history must include VALIDATED event; got: " + text);
    Assertions.assertTrue(text.contains("ACTIVATED"),
        "history must include ACTIVATED event; got: " + text);
    Assertions.assertTrue(text.contains("DEACTIVATED"),
        "history must include DEACTIVATED event; got: " + text);
  }

  @Test
  @Order(2)
  public void unknownPolicyRefuses() {
    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute("SHOW ERASURE POLICY tseph_no_such HISTORY"),
        "SHOW HISTORY against a missing policy must refuse");
    final String chain = messageChain(ex);
    Assertions.assertTrue(chain.contains("policy not found")
            || chain.contains("not found"),
        "refusal must name the missing policy; got: " + chain);
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
