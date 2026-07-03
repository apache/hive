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

public class TestPolicyValidateInvalidate extends BaseTest {

  private static String dsl(String version) {
    return """
        VERSION '%s'
        IDENTITY userId TYPE INT
        SCHEMA   TYPE STRING
        FOR SCHEMA 'user_info_v1'
            ERASE telephone
        """.formatted(version);
  }

  private void load(String policy, String version) throws Exception {
    final Path f = Files.createTempFile("tpvi_" + policy + "_" + version + "_", ".erp");
    Files.write(f, dsl(version).getBytes());
    execute("LOAD ERASURE POLICY " + policy + " FROM '" + f + "'");
  }

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] {"tpvi_ambig", "tpvi_single", "tpvi_drop", "tpvi_revalidate", "tpvi_none"};
  }

  @Test
  public void noClauseRefusesWhenMultipleDrafts() throws Exception {
    final String p = "tpvi_ambig";
    load(p, "v1");
    load(p, "v2");
    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute("VALIDATE ERASURE POLICY " + p),
        "VALIDATE with two DRAFTs and no VERSION clause must refuse");
    final String chain = messageChain(ex);
    Assertions.assertTrue(chain.contains("multiple DRAFT"),
        "refusal must flag the ambiguity; got: " + chain);
    execute("VALIDATE ERASURE POLICY " + p + " VERSION 'v1'");
  }

  @Test
  public void versionClauseSelectsDraftAndSecondValidateRefused() throws Exception {
    final String p = "tpvi_single";
    load(p, "v1");
    load(p, "v2");
    execute("VALIDATE ERASURE POLICY " + p + " VERSION 'v1'");
    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute("VALIDATE ERASURE POLICY " + p + " VERSION 'v2'"),
        "a second VALIDATE while one is VALIDATED must refuse");
    final String chain = messageChain(ex);
    Assertions.assertTrue(chain.contains("already has a VALIDATED") && chain.contains("v1"),
        "refusal must name the pending VALIDATED version; got: " + chain);
  }

  @Test
  public void invalidateClearsTheNonDraftDropGate() throws Exception {
    final String p = "tpvi_drop";
    load(p, "v1");
    execute("VALIDATE ERASURE POLICY " + p);

    final CommandProcessorException before = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute("DROP DATA ERASURE POLICY " + p),
        "DROP must refuse at the non-DRAFT gate while a version is VALIDATED");
    Assertions.assertTrue(messageChain(before).contains("VALIDATED"),
        "pre-INVALIDATE refusal must come from the non-DRAFT gate; got: " + messageChain(before));

    execute("INVALIDATE ERASURE POLICY " + p);
    execute("DROP DATA ERASURE POLICY " + p);
  }

  @Test
  public void invalidateThenRevalidate() throws Exception {
    final String p = "tpvi_revalidate";
    load(p, "v1");
    execute("VALIDATE ERASURE POLICY " + p);
    execute("INVALIDATE ERASURE POLICY " + p + " VERSION 'v1'");
    execute("VALIDATE ERASURE POLICY " + p + " VERSION 'v1'");
  }

  @Test
  public void invalidateRefusesWhenNothingValidated() throws Exception {
    final String p = "tpvi_none";
    load(p, "v1");
    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute("INVALIDATE ERASURE POLICY " + p),
        "INVALIDATE with no VALIDATED version must refuse");
    Assertions.assertTrue(messageChain(ex).contains("no VALIDATED"),
        "refusal must say there is nothing to invalidate; got: " + messageChain(ex));
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
