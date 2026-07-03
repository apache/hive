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
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestExtractToPath extends BaseTest {

  private static final String TABLE  = "tetp_t1_" + Long.toString(System.nanoTime(), 36);
  private static final String COLUMN = "b";
  private static final String POLICY = "tetp_pol_" + Long.toString(System.nanoTime(), 36);

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

  @Test @Order(1)
  public void setup() throws CommandProcessorException, IOException {
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
  }

  @Test @Order(2)
  public void extractToPathClauseParsesAndRuns() throws CommandProcessorException {
    final String outPath = Paths.get(
        "target", "dae-extract-" + Long.toString(System.nanoTime(), 36) + ".json")
        .toAbsolutePath().toString();
    execute("EXTRACT FROM TABLE %s COLUMN %s FOR IDENTITY VALUES (42) TO '%s'"
        .formatted(TABLE, COLUMN, outPath));
  }

  @Test @Order(3)
  public void extractWithoutToClauseStillRuns() throws CommandProcessorException {
    execute("EXTRACT FROM TABLE %s COLUMN %s FOR IDENTITY VALUES (42)".formatted(TABLE, COLUMN));
  }

  @Test @Order(4)
  public void cleanup() throws CommandProcessorException {
    try { execute("DETACH DATA ERASURE POLICY ON TABLE " + TABLE + " COLUMN " + COLUMN); }
    catch (CommandProcessorException ignored) {  }
    try { execute("DROP DATA ERASURE POLICY IF EXISTS " + POLICY); }
    catch (CommandProcessorException expected) {  }
    try { execute("DROP TABLE IF EXISTS " + TABLE); }
    catch (CommandProcessorException expected) {  }
  }
}
