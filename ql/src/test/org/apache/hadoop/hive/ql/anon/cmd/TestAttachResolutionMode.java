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

import org.apache.hadoop.hive.metastore.api.ErasurePolicyBinding;
import org.apache.hadoop.hive.metastore.api.PolicyResolutionMode;
import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

public class TestAttachResolutionMode extends BaseTest {

  private static final String TABLE = "tarm_t_" + Long.toString(System.nanoTime(), 36);
  private static final String COLUMN = "b";
  private static final String POLICY = "tarm_pol";

  private static final String DSL =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA TYPE STRING
      FOR SCHEMA 'user_info_v1'
          ERASE telephone
      """;

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] { POLICY };
  }

  @BeforeAll
  public void fixture() throws Exception {
    execute("CREATE TABLE IF NOT EXISTS " + TABLE + " (m INT, o BIGINT, b STRING) STORED AS ORC");
    final Path f = Files.createTempFile(POLICY + "_", ".erp");
    Files.write(f, DSL.getBytes());
    execute("LOAD ERASURE POLICY " + POLICY + " FROM '" + f + "'");
    execute("VALIDATE ERASURE POLICY " + POLICY);
    execute("ACTIVATE ERASURE POLICY " + POLICY);
  }

  private String attachWith(String mode) {
    return ("ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s"
        + " WITH ( SCHEMA FIELD (m), ROW LOCATOR (o), COLUMN FORMAT (JSON) )"
        + " RESOLUTION ( %s )").formatted(POLICY, TABLE, COLUMN, mode);
  }

  private PolicyResolutionMode storedMode() throws Exception {
    final Table t = hive.getTable(TABLE);
    final long tblId = t.getTTable().getId();
    final ErasurePolicyBinding b = hive.getErasurePolicyBinding(tblId, COLUMN);
    return b.getResolutionMode();
  }

  @Test
  public void resolutionModeIsFixedAtFirstAttach() throws Exception {
    execute(attachWith("STRICTEST"));
    Assertions.assertEquals(PolicyResolutionMode.STRICTEST, storedMode(),
        "the first ATTACH should store the STRICTEST mode");

    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute(attachWith("EXPLICIT")),
        "re-ATTACH under a differing RESOLUTION must refuse");
    Assertions.assertTrue(messageChain(ex).contains("fixed at the first ATTACH"),
        "the refusal must say the settings are fixed at the first ATTACH; got: "
            + messageChain(ex));
    Assertions.assertEquals(PolicyResolutionMode.STRICTEST, storedMode(),
        "a refused re-ATTACH must not change the stored mode");

    execute(attachWith("STRICTEST"));
    Assertions.assertEquals(PolicyResolutionMode.STRICTEST, storedMode(),
        "repeating the stored mode must remain accepted");

    execute("ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s".formatted(POLICY, TABLE, COLUMN));
    Assertions.assertEquals(PolicyResolutionMode.STRICTEST, storedMode(),
        "a bare re-ATTACH must inherit the stored mode, not flip it to the default");

    try { execute("DETACH DATA ERASURE POLICY ON TABLE " + TABLE + " COLUMN " + COLUMN); }
    catch (CommandProcessorException ignore) {  }
    try { execute("DROP TABLE IF EXISTS " + TABLE); }
    catch (CommandProcessorException ignore) {  }
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
