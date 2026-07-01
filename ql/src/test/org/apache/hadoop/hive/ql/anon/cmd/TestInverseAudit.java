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
import java.util.List;
import org.apache.hadoop.hive.metastore.api.ErasureRunAudit;
import org.apache.hadoop.hive.metastore.api.ErasureRunStatus;
import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestInverseAudit extends BaseTest {

  private static final String TABLE_A  = "tia_t1";
  private static final String TABLE_B  = "tia_t2";
  private static final String COLUMN   = "b";
  private static final String POLICY_A = "tia_pol_a";
  private static final String POLICY_B = "tia_pol_b";

  private static final String POLICY_DSL_A =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA TYPE STRING
      FOR SCHEMA 'user_info_v1'
          ERASE telephone
      """;

  private static final String POLICY_DSL_B =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA TYPE STRING
      FOR SCHEMA 'user_info_v1'
          ERASE address
      """;

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] { POLICY_A, POLICY_B };
  }


  @Test @Order(1)
  public void setup() throws CommandProcessorException, IOException {
    provisionTable(TABLE_A);
    provisionTable(TABLE_B);
    provisionPolicy(POLICY_A, POLICY_DSL_A, TABLE_A);
    provisionPolicy(POLICY_B, POLICY_DSL_B, TABLE_B);
  }


  @Test @Order(2)
  public void seedExtractAndReleaseEventsAcrossBothTables()
      throws CommandProcessorException {
    execute("EXTRACT FROM TABLE %s COLUMN %s FOR IDENTITY VALUES (42)".formatted(TABLE_A, COLUMN));
    execute("EXTRACT FROM TABLE %s COLUMN %s FOR IDENTITY VALUES (42, 99)".formatted(TABLE_A, COLUMN));
    execute("EXTRACT FROM TABLE %s COLUMN %s FOR IDENTITY VALUES (7, 42)".formatted(TABLE_B, COLUMN));
    execute("EXTRACT FROM TABLE %s COLUMN %s FOR IDENTITY VALUES (123)".formatted(TABLE_B, COLUMN));
  }


  @Test @Order(3)
  public void auditByIdentityVerbDispatchesWithoutError()
      throws CommandProcessorException {
    execute("AUDIT BY IDENTITY VALUES (42)");
  }

  @Test @Order(4)
  public void auditByIdentityVerbAcceptsMultipleValues()
      throws CommandProcessorException {
    execute("AUDIT BY IDENTITY VALUES (42, 7, 123)");
  }


  @Test @Order(5)
  public void inverseLookupReturnsEventsAcrossTables() throws HiveException {
    final List<ErasureRunAudit> all = hive.getErasureRunsForTable(
        0L, Long.MIN_VALUE, Long.MAX_VALUE, null, "42");
    assertFalse(all.isEmpty(),
        "inverse lookup must surface every event mentioning identity 42");
    final boolean fromA = all.stream()
        .anyMatch(r -> r.getStatus() == ErasureRunStatus.EXTRACTED
                    && r.getIdentityValues() != null
                    && r.getIdentityValues().contains("42"));
    assertTrue(fromA,
        "inverse lookup must surface EXTRACTED events that mention identity 42");
    long prev = Long.MIN_VALUE;
    for (final ErasureRunAudit r : all) {
      assertTrue(r.getStartedTs() >= prev,
          "inverse lookup must return rows in startedTs-ascending order");
      prev = r.getStartedTs();
    }
  }

  @Test @Order(6)
  public void inverseLookupForUntouchedIdentityIsEmpty() throws HiveException {
    final List<ErasureRunAudit> rows = hive.getErasureRunsForTable(
        0L, Long.MIN_VALUE, Long.MAX_VALUE, null, "no_such_identity_value");
    assertTrue(rows.isEmpty(),
        "an identity that was never targeted must surface zero audit rows");
  }

  @Test @Order(7)
  public void perTableLookupStillFiltersByTable() throws HiveException {
    final Table tblA;
    try {
      tblA = hive.getTable(TABLE_A);
    } catch (HiveException he) {
      throw new HiveException(he);
    }
    final long tblIdA = tblA.getTTable().getId();
    final List<ErasureRunAudit> aOnly = hive.getErasureRunsForTable(
        tblIdA, Long.MIN_VALUE, Long.MAX_VALUE, null, "42");
    assertFalse(aOnly.isEmpty(),
        "per-table inverse lookup must still return rows mentioning identity 42 on table A");
    for (final ErasureRunAudit r : aOnly) {
      assertEquals(tblIdA, r.getTblId(),
          "per-table lookup must restrict results to the named table; got tblId="
              + r.getTblId());
    }
  }


  @Test @Order(8)
  public void auditByIdentityRequiresAtLeastOneValue() {
    assertThrows(CommandProcessorException.class,
        () -> execute("AUDIT BY IDENTITY VALUES ()"),
        "the grammar requires at least one identity value");
  }


  @Test @Order(9)
  public void cleanup() throws CommandProcessorException {
    for (final String[] pair : new String[][] {
        { TABLE_A, POLICY_A },
        { TABLE_B, POLICY_B } }) {
      try {
        execute("DETACH DATA ERASURE POLICY ON TABLE " + pair[0] + " COLUMN " + COLUMN);
      } catch (CommandProcessorException ignored) {  }
      try { execute("DROP DATA ERASURE POLICY IF EXISTS " + pair[1]); }
      catch (CommandProcessorException expected) {  }
      try { execute("DROP TABLE IF EXISTS " + pair[0]); }
      catch (CommandProcessorException expected) {  }
    }
  }


  private void provisionTable(final String table) throws CommandProcessorException {
    execute("CREATE TABLE IF NOT EXISTS %s (m INT, o BIGINT, %s STRING) STORED AS ORC"
        .formatted(table, COLUMN));
  }

  private void provisionPolicy(final String policy, final String dsl, final String table)
      throws CommandProcessorException, IOException {
    Path policyFile = Files.createTempFile(policy + "_", ".erp");
    Files.write(policyFile, dsl.getBytes());
    execute("LOAD ERASURE POLICY " + policy + " FROM '" + policyFile + "'");
    execute("VALIDATE ERASURE POLICY " + policy);
    execute("ACTIVATE ERASURE POLICY " + policy);
    execute(("ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s"
        + " WITH ( SCHEMA FIELD (m), ROW LOCATOR (o), COLUMN FORMAT (JSON) )"
        + " RESOLUTION ( EXPLICIT )").formatted(policy, table, COLUMN));
  }
}
