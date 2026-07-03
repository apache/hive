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
import org.apache.hadoop.hive.ql.anon.builders.DescErasurePolicyStatementBuilder;
import org.apache.hadoop.hive.ql.anon.builders.ShowErasurePoliciesStatementBuilder;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class TestShowPolicies extends BaseTest {

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] { POLICY };
  }

  private static final String POLICY = "tsp_p";

  private static final String POLICY_DSL =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA   TYPE STRING
      FOR SCHEMA 'user_info_v1'
          ERASE telephone
      """;

  @BeforeAll
  public void setup() throws CommandProcessorException, IOException {
    final Path policyFile = Files.createTempFile("tsp_", ".erp");
    Files.write(policyFile, POLICY_DSL.getBytes());
    execute("LOAD ERASURE POLICY " + POLICY + " FROM '" + policyFile + "'");
  }

  @Test
  public void testShowPolicies() throws CommandProcessorException, IOException {
    final String cmd = new ShowErasurePoliciesStatementBuilder()
      .build();
    List<Object> lst = execute(cmd);
  }

  @Test
  public void testDescPolicy() throws CommandProcessorException, IOException {
    final String cmd = new DescErasurePolicyStatementBuilder(POLICY)
      .build();
    List<Object> lst = execute(cmd);
  }

  @Test
  public void testDescUnknownPolicyReportsClearly() {
    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute(new DescErasurePolicyStatementBuilder("tsp_no_such").build()),
        "DESCRIBE against a missing policy must throw");
    final String chain = messageChain(ex);
    Assertions.assertTrue(chain.contains("policy not found"),
        "error must name the missing-policy condition; got: " + chain);
    Assertions.assertFalse(chain.contains("Dataconnector"),
        "legacy DATACONNECTOR_NOT_EXISTS wording must be gone; got: " + chain);
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
