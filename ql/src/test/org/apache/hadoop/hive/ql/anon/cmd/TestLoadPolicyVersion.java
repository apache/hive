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
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

public class TestLoadPolicyVersion extends BaseTest {

  private static String dsl(String versionHeader) {
    return (versionHeader == null ? "" : "VERSION %s\n".formatted(versionHeader))
        + """
          IDENTITY userId TYPE INT
          SCHEMA   TYPE STRING
          FOR SCHEMA 'user_info_v1'
              ERASE telephone
          """;
  }

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] {"tlpv_dup", "tlpv_distinct", "tlpv_auto"};
  }

  @Test
  public void duplicateVersionHeaderRefused() throws Exception {
    final String policyName = "tlpv_dup";
    final Path file = Files.createTempFile("tlpv_dup_", ".erp");
    Files.write(file, dsl("'2024-11-14'").getBytes());

    execute("LOAD ERASURE POLICY " + policyName + " FROM '" + file + "'");

    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute("LOAD ERASURE POLICY " + policyName + " FROM '" + file + "'"),
        "re-LOAD of an already-used VERSION must be refused");
    final String chain = messageChain(ex);
    Assertions.assertTrue(chain.contains("2024-11-14") && chain.contains("already exists"),
        "refusal must name the duplicate version; got: " + chain);
  }

  @Test
  public void distinctVersionHeadersBothLoad() throws Exception {
    final String policyName = "tlpv_distinct";
    final Path v1 = Files.createTempFile("tlpv_v1_", ".erp");
    final Path v2 = Files.createTempFile("tlpv_v2_", ".erp");
    Files.write(v1, dsl("'v1'").getBytes());
    Files.write(v2, dsl("'v2'").getBytes());

    execute("LOAD ERASURE POLICY " + policyName + " FROM '" + v1 + "'");
    execute("LOAD ERASURE POLICY " + policyName + " FROM '" + v2 + "'");
  }

  @Test
  public void headerlessFileIsRefused() throws Exception {
    final String policyName = "tlpv_auto";
    final Path file = Files.createTempFile("tlpv_auto_", ".erp");
    Files.write(file, dsl(null).getBytes());

    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute("LOAD ERASURE POLICY " + policyName + " FROM '" + file + "'"),
        "a file with no VERSION header must be refused");
    Assertions.assertTrue(messageChain(ex).contains("VERSION"),
        "refusal must mention the missing VERSION header; got: " + messageChain(ex));
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
