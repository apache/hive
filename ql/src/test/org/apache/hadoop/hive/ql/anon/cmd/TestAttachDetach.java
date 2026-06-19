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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestAttachDetach extends BaseTest {

  private static final String SUFFIX = Long.toString(System.nanoTime(), 36);
  private static final String TABLE = "tad_t1_" + SUFFIX;
  private static final String GUARD_TABLE = "tad_guard_" + SUFFIX;
  private static final String COLUMN = "b";
  private static final String POLICY = "tad_pol";

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
  @Order(1)
  public void setup() throws CommandProcessorException, IOException {
    execute("CREATE TABLE IF NOT EXISTS %s (m INT, o BIGINT, b STRING) STORED AS ORC".formatted(TABLE));
    Path policyFile = Files.createTempFile(POLICY + "_", ".erp");
    Files.write(policyFile, POLICY_DSL.getBytes());
    execute("LOAD ERASURE POLICY " + POLICY + " FROM '" + policyFile + "'");
    execute("VALIDATE ERASURE POLICY " + POLICY);
    execute("ACTIVATE ERASURE POLICY " + POLICY);
  }

  @Test
  @Order(2)
  public void explainAttachIsDryRun() throws CommandProcessorException {
    execute("EXPLAIN ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s".formatted(POLICY, TABLE, COLUMN));
  }

  @Test
  @Order(3)
  public void attachPolicy() throws CommandProcessorException {
    execute("ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s".formatted(POLICY, TABLE, COLUMN));
  }

  @Test
  @Order(4)
  public void attachIsIdempotent() throws CommandProcessorException {
    execute("ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s".formatted(POLICY, TABLE, COLUMN));
  }

  @Test
  @Order(5)
  public void validateBinding() throws CommandProcessorException {
    execute("VALIDATE ERASURE BINDING ON TABLE %s COLUMN %s".formatted(TABLE, COLUMN));
  }

  @Test
  @Order(6)
  public void detachPolicy() {
    try {
      execute("DETACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s".formatted(POLICY, TABLE, COLUMN));
    } catch (CommandProcessorException expected) {
    }
  }

  @Test
  @Order(7)
  public void detachWithoutPolicyListDropsBinding() {
    try {
      execute("ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s".formatted(POLICY, TABLE, COLUMN));
      execute("DETACH DATA ERASURE POLICY ON TABLE %s COLUMN %s".formatted(TABLE, COLUMN));
    } catch (CommandProcessorException expected) {
    }
  }

  @Test
  @Order(8)
  public void attachToSecondColumnIsRejected() throws CommandProcessorException {
    try { execute("DETACH DATA ERASURE POLICY ON TABLE " + GUARD_TABLE + " COLUMN b"); }
    catch (CommandProcessorException ignore) {  }
    try { execute("DROP TABLE IF EXISTS " + GUARD_TABLE); }
    catch (CommandProcessorException ignore) {  }
    execute("CREATE TABLE IF NOT EXISTS " + GUARD_TABLE + " (m INT, o BIGINT, b STRING, b2 STRING) STORED AS ORC");
    execute("ATTACH DATA ERASURE POLICY " + POLICY + " ON TABLE " + GUARD_TABLE + " COLUMN b");
    boolean rejected = false;
    try {
      execute("ATTACH DATA ERASURE POLICY " + POLICY + " ON TABLE " + GUARD_TABLE + " COLUMN b2");
    } catch (CommandProcessorException expected) {
      rejected = true;
    }
    Assertions.assertTrue(rejected,
        "ATTACH to a second, different column must be rejected by the single-column guard");
    try { execute("DETACH DATA ERASURE POLICY ON TABLE " + GUARD_TABLE + " COLUMN b"); }
    catch (CommandProcessorException ignore) {  }
    try { execute("DROP TABLE IF EXISTS " + GUARD_TABLE); }
    catch (CommandProcessorException ignore) {  }
  }

  @Test
  @Order(9)
  public void cleanup() throws CommandProcessorException {
    try { execute("DROP DATA ERASURE POLICY IF EXISTS " + POLICY); }
    catch (CommandProcessorException expected) {  }
    try { execute("DETACH DATA ERASURE POLICY ON TABLE " + TABLE + " COLUMN " + COLUMN); }
    catch (CommandProcessorException ignore) {  }
    try { execute("DROP TABLE IF EXISTS " + TABLE); }
    catch (CommandProcessorException expected) {  }
  }
}
