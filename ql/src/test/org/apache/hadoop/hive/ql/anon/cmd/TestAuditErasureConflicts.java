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

import org.apache.hadoop.hive.metastore.api.ErasurePolicyLifecycleEvent;
import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestAuditErasureConflicts extends BaseTest {

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] { POL_A, POL_B };
  }

  private static final String TABLE   = "tac_conf_t1";
  private static final String COLUMN  = "b";
  private static final String POL_A   = "tac_conf_pol_a";
  private static final String POL_B   = "tac_conf_pol_b";

  private static final String DSL_A =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA TYPE STRING
      FOR SCHEMA 'user_info_v1'
          REPLACE telephone = 'REDACTED-A'
      """;
  private static final String DSL_B =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA TYPE STRING
      FOR SCHEMA 'user_info_v1'
          REPLACE telephone = 'REDACTED-B'
      """;

  @Test
  @Order(1)
  public void setupTwoPoliciesAndTable() throws CommandProcessorException, IOException {
    execute("CREATE TABLE IF NOT EXISTS %s (m INT, o BIGINT, b STRING) STORED AS ORC".formatted(TABLE));
    load(POL_A, DSL_A);
    load(POL_B, DSL_B);
  }

  @Test
  @Order(2)
  public void attachUnderExplicitWithConflictPersistsAttachRejected() throws Exception {
    final Hive db = Hive.get();
    final List<ErasurePolicyLifecycleEvent> before =
        db.getAttachRejectedEvents(Long.MIN_VALUE, Long.MAX_VALUE);
    final int baseline = before == null ? 0 : before.size();

    boolean threw = false;
    try {
      execute("ATTACH DATA ERASURE POLICY %s, %s ON TABLE %s COLUMN %s"
          .formatted(POL_A, POL_B, TABLE, COLUMN));
    } catch (CommandProcessorException expected) {
      threw = true;
    }
    Assertions.assertTrue(threw, "ATTACH under EXPLICIT mode with conflicting "
        + "policies must raise CommandProcessorException");

    final List<ErasurePolicyLifecycleEvent> after =
        db.getAttachRejectedEvents(Long.MIN_VALUE, Long.MAX_VALUE);
    Assertions.assertNotNull(after);
    Assertions.assertTrue(after.size() > baseline,
        "ATTACH_REJECTED lifecycle event was not persisted; pre=" + baseline
            + " post=" + after.size());
  }

  @Test
  @Order(3)
  public void auditConflictsReturnsWithoutThrowing() throws CommandProcessorException {
    execute("AUDIT ERASURE CONFLICTS");
  }

  @Test
  @Order(4)
  public void auditConflictsAcceptsTimeRange() throws CommandProcessorException {
    execute("AUDIT ERASURE CONFLICTS FROM '2020-01-01' UNTIL '2099-12-31'");
  }

  @Test
  @Order(99)
  public void cleanup() {
    safe("DROP DATA ERASURE POLICY IF EXISTS " + POL_A);
    safe("DROP DATA ERASURE POLICY IF EXISTS " + POL_B);
    safe("DROP TABLE IF EXISTS " + TABLE);
  }

  private void load(final String name, final String dsl)
      throws CommandProcessorException, IOException {
    final Path file = Files.createTempFile(name + "_", ".erp");
    Files.write(file, dsl.getBytes());
    execute("LOAD ERASURE POLICY " + name + " FROM '" + file + "'");
    execute("VALIDATE ERASURE POLICY " + name);
    execute("ACTIVATE ERASURE POLICY " + name);
  }

  private void safe(final String sql) {
    try {
      execute(sql);
    } catch (CommandProcessorException ignored) {
    }
  }
}
