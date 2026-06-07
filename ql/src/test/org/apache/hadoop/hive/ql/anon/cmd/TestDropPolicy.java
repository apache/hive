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

import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.apache.hadoop.hive.ql.anon.builders.DropPolicyStatementBuilder;
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
public class TestDropPolicy extends BaseTest {

  private static final String DSL =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA   TYPE STRING
      FOR SCHEMA 'user_info_v1'
          ERASE telephone
      """;

  @BeforeAll
  public void setup() throws CommandProcessorException {
  }

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] {"p", "tdp_draft_only", "tdp_validated", "tdp_active"};
  }

  @Test @Order(1)
  public void testDropPolicy() throws CommandProcessorException {
    final String cmd = new DropPolicyStatementBuilder("p").withIfExists().build();
    final List<Object> lst = execute(cmd);
    Assertions.assertEquals(0, lst.size());
  }

  @Test @Order(2)
  public void dropOnDraftOnlyPolicyPassesRestrictionGate() throws Exception {
    final String policyName = "tdp_draft_only";
    final Path file = Files.createTempFile("tdp_draft_", ".erp");
    Files.write(file, DSL.getBytes());

    execute("LOAD ERASURE POLICY " + policyName + " FROM '" + file + "'");
    execute("DROP DATA ERASURE POLICY IF EXISTS " + policyName);
  }

  @Test @Order(3)
  public void dropOnValidatedPolicyRefuses() throws Exception {
    final String policyName = "tdp_validated";
    final Path file = Files.createTempFile("tdp_validated_", ".erp");
    Files.write(file, DSL.getBytes());

    execute("LOAD     ERASURE POLICY " + policyName + " FROM '" + file + "'");
    execute("VALIDATE ERASURE POLICY " + policyName);

    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute("DROP DATA ERASURE POLICY " + policyName),
        "DROP on a VALIDATED policy must refuse");
    final String chain = messageChain(ex);
    Assertions.assertTrue(chain.contains("VALIDATED"),
        "refusal must name the offending state; got: " + chain);
    Assertions.assertTrue(chain.contains("DEACTIVATE"),
        "refusal must point at DEACTIVATE as the retirement path; got: " + chain);
  }

  @Test @Order(4)
  public void dropOnActivePolicyRefuses() throws Exception {
    final String policyName = "tdp_active";
    final Path file = Files.createTempFile("tdp_active_", ".erp");
    Files.write(file, DSL.getBytes());

    execute("LOAD     ERASURE POLICY " + policyName + " FROM '" + file + "'");
    execute("VALIDATE ERASURE POLICY " + policyName);
    execute("ACTIVATE ERASURE POLICY " + policyName);

    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute("DROP DATA ERASURE POLICY " + policyName),
        "DROP on an ACTIVE policy must refuse");
    Assertions.assertTrue(messageChain(ex).contains("ACTIVE"),
        "refusal must name the ACTIVE state; got: " + messageChain(ex));
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
