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

import org.apache.hadoop.hive.metastore.api.ErasurePolicyVersion;
import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.apache.hadoop.hive.ql.anon.consts.AnonConst;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class TestActivateConflictRecheck extends BaseTest {

  private static final String SUFFIX = Long.toString(System.nanoTime(), 36);
  private static final String COLUMN = "b";

  private static final String PB    = "tacr_b_"  + SUFFIX;
  private static final String PA    = "tacr_a_"  + SUFFIX;
  private static final String TBL   = "tacr_t_"  + SUFFIX;
  private static final String PB2   = "tacr_b2_" + SUFFIX;
  private static final String PA2   = "tacr_a2_" + SUFFIX;
  private static final String TBL2  = "tacr_t2_" + SUFFIX;

  private static String dsl(final String version, final String body) {
    return "VERSION " + version + "\n"
        + "IDENTITY userId TYPE INT\n"
        + "SCHEMA   TYPE INT\n"
        + "FOR SCHEMA 3\n"
        + "    " + body + "\n";
  }

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] { PA, PB, PA2, PB2 };
  }

  @Test
  public void activateConflictingVersionIsRefusedAndOldVersionStaysActive() throws Exception {
    setEnforce(false);
    execute("CREATE TABLE IF NOT EXISTS %s (m INT, o BIGINT, b STRING) STORED AS ORC".formatted(TBL));

    provision(PB, dsl("v1", "ERASE telephone"));
    provision(PA, dsl("v1", "ERASE ipList"));

    execute(("ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s"
        + " WITH (SCHEMA FIELD (m), ROW LOCATOR (o), COLUMN FORMAT (JSON)) RESOLUTION (EXPLICIT)")
        .formatted(PB, TBL, COLUMN));
    execute("ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s".formatted(PA, TBL, COLUMN));

    loadVersion(PA, dsl("v2", "REPLACE telephone = 'X'"));

    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute("ACTIVATE ERASURE POLICY %s".formatted(PA)),
        "activating a version that conflicts with another bound policy must be refused");
    Assertions.assertTrue(messageChain(ex).contains("conflict"),
        "the refusal must cite the cross-policy conflict; got: " + messageChain(ex));

    final ErasurePolicyVersion active = hive.getActiveErasurePolicyVersion(PA);
    Assertions.assertNotNull(active, "A must still have an ACTIVE version");
    Assertions.assertTrue(active.getSourceText().contains("ipList"),
        "the non-conflicting v1 must remain ACTIVE; got: " + active.getSourceText());
    Assertions.assertFalse(active.getSourceText().contains("telephone"),
        "the conflicting v2 must not have become ACTIVE; got: " + active.getSourceText());
  }

  @Test
  public void activateDisjointNewVersionIsAllowed() throws Exception {
    setEnforce(false);
    execute("CREATE TABLE IF NOT EXISTS %s (m INT, o BIGINT, b STRING) STORED AS ORC".formatted(TBL2));

    provision(PB2, dsl("v1", "ERASE telephone"));
    provision(PA2, dsl("v1", "ERASE ipList"));

    execute(("ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s"
        + " WITH (SCHEMA FIELD (m), ROW LOCATOR (o), COLUMN FORMAT (JSON)) RESOLUTION (EXPLICIT)")
        .formatted(PB2, TBL2, COLUMN));
    execute("ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s".formatted(PA2, TBL2, COLUMN));

    loadVersion(PA2, dsl("v2", "ERASE country"));
    execute("ACTIVATE ERASURE POLICY %s".formatted(PA2));

    final ErasurePolicyVersion active = hive.getActiveErasurePolicyVersion(PA2);
    Assertions.assertTrue(active.getSourceText().contains("country"),
        "the disjoint v2 must become ACTIVE; got: " + active.getSourceText());
  }

  private void provision(final String policy, final String dsl)
      throws CommandProcessorException, IOException {
    loadVersion(policy, dsl);
    execute("ACTIVATE ERASURE POLICY %s".formatted(policy));
  }

  private void loadVersion(final String policy, final String dsl)
      throws CommandProcessorException, IOException {
    final Path file = Files.createTempFile(policy + "_", ".erp");
    Files.write(file, dsl.getBytes());
    execute("LOAD ERASURE POLICY %s FROM '%s'".formatted(policy, file));
    execute("VALIDATE ERASURE POLICY %s".formatted(policy));
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
