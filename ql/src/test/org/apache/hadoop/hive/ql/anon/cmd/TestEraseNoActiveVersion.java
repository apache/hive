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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class TestEraseNoActiveVersion extends BaseTest {

  private static final String TABLE  = "tenav_t1_" + Long.toString(System.nanoTime(), 36);
  private static final String COLUMN = "b";
  private static final String POLICY = "tenav_pol_" + Long.toString(System.nanoTime(), 36);

  private static final String POLICY_DSL =
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

  @Test
  public void eraseRefusesAfterPolicyDeactivated()
      throws CommandProcessorException, IOException {
    try { execute("DROP TABLE IF EXISTS " + TABLE); }
    catch (CommandProcessorException expected) {  }
    execute("CREATE TABLE IF NOT EXISTS %s (m INT, o BIGINT, b STRING) STORED AS ORC".formatted(TABLE));

    final Path policyFile = Files.createTempFile(POLICY + "_", ".erp");
    Files.write(policyFile, POLICY_DSL.getBytes());
    execute("LOAD ERASURE POLICY " + POLICY + " FROM '" + policyFile + "'");
    execute("VALIDATE ERASURE POLICY " + POLICY);
    execute("ACTIVATE ERASURE POLICY " + POLICY);
    execute(("ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s"
        + " WITH ( SCHEMA FIELD (m), ROW LOCATOR (o), COLUMN FORMAT (JSON) )"
        + " RESOLUTION ( EXPLICIT )").formatted(POLICY, TABLE, COLUMN));

    execute("DEACTIVATE ERASURE POLICY " + POLICY);

    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute("ERASE FROM TABLE %s FOR IDENTITY VALUES (1)".formatted(TABLE)),
        "ERASE must refuse when the bound policy has no ACTIVE version");
    final String chain = messageChain(ex);
    Assertions.assertTrue(chain.contains("no ACTIVE version"),
        "refusal must name the missing ACTIVE version; got: " + chain);
    Assertions.assertTrue(chain.contains("ACTIVATE"),
        "refusal must point the operator at ACTIVATE; got: " + chain);

    try { execute("DETACH DATA ERASURE POLICY ON TABLE " + TABLE + " COLUMN " + COLUMN); }
    catch (CommandProcessorException ignored) {  }
    try { execute("DROP TABLE IF EXISTS " + TABLE); }
    catch (CommandProcessorException ignored) {  }
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
