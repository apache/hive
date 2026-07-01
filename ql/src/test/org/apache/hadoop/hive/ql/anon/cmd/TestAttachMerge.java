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

import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.metastore.api.ErasurePolicyBinding;
import org.apache.hadoop.hive.metastore.api.ErasurePolicyBindingResolved;
import org.apache.hadoop.hive.metastore.api.PolicyActionKind;
import org.apache.hadoop.hive.metastore.api.PolicyResolutionMode;
import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.apache.hadoop.hive.ql.anon.TestUtils;
import org.apache.hadoop.hive.ql.anon.convert.JsonBodyConverter;
import org.apache.hadoop.hive.ql.anon.model.Msg3;
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
public class TestAttachMerge extends BaseTest {

  private static final String SUFFIX = Long.toString(System.nanoTime(), 36);
  private static final String STRICT_TABLE   = "tam_ts_" + SUFFIX;
  private static final String EXPLICIT_TABLE = "tam_te_" + SUFFIX;
  private static final String ERASE_TABLE    = "tam_tx_" + SUFFIX;
  private static final String MIXED_TABLE    = "tam_tn_" + SUFFIX;
  private static final String COLUMN = "b";

  private static final String P1 = "tam_p1_" + SUFFIX;
  private static final String P2 = "tam_p2_" + SUFFIX;
  private static final String P3 = "tam_p3_" + SUFFIX;
  private static final String P4 = "tam_p4_" + SUFFIX;
  private static final String PA = "tam_pa_" + SUFFIX;
  private static final String PB = "tam_pb_" + SUFFIX;
  private static final String PM = "tam_pm_" + SUFFIX;

  private static final String DSL_ERASE_TELEPHONE =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA   TYPE INT
      FOR SCHEMA 3
          ERASE telephone
      """;

  private static final String DSL_REPLACE_TELEPHONE_ERASE_COUNTRY =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA   TYPE INT
      FOR SCHEMA 3
          ERASE country
          REPLACE telephone = 'X'
      """;

  private static final String DSL_REPLACE_TELEPHONE =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA   TYPE INT
      FOR SCHEMA 3
          REPLACE telephone = 'Y'
      """;

  private static final String DSL_ERASE_IPLIST =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA   TYPE INT
      FOR SCHEMA 3
          ERASE ipList
      """;

  private static final String DSL_MIXED_ACTIONS =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA   TYPE INT
      FOR SCHEMA 3
          ERASE telephone
          INSPECT city
          FLAG country
      """;

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] { P1, P2, P3, P4, PA, PB, PM };
  }


  @Test
  @Order(1)
  public void strictestUnionMergesResolvedRulesAcrossAttaches() throws Exception {
    execute("CREATE TABLE %s (m INT, o BIGINT, b STRING) STORED AS ORC".formatted(STRICT_TABLE));
    provision(P1, DSL_ERASE_TELEPHONE);
    provision(P2, DSL_REPLACE_TELEPHONE_ERASE_COUNTRY);

    execute(attachFull(P1, STRICT_TABLE, "STRICTEST"));
    execute("ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s".formatted(P2, STRICT_TABLE, COLUMN));

    final ErasurePolicyBinding b = binding(STRICT_TABLE);
    Assertions.assertEquals(PolicyResolutionMode.STRICTEST, b.getResolutionMode(),
        "a bare re-ATTACH must not flip the stored mode to the EXPLICIT default");

    final List<ErasurePolicyBindingResolved> rules =
        hive.getBindingResolvedRules(b.getBindingId());
    ErasurePolicyBindingResolved telephone = null;
    ErasurePolicyBindingResolved country = null;
    for (final ErasurePolicyBindingResolved r : rules) {
      if ("telephone".equals(r.getFieldPath())) {
        telephone = r;
      } else if ("country".equals(r.getFieldPath())) {
        country = r;
      }
    }
    Assertions.assertNotNull(telephone,
        "the resolved snapshot must keep p1's telephone rule after attaching p2");
    Assertions.assertEquals(PolicyActionKind.ERASE, telephone.getAction(),
        "ERASE vs REPLACE on telephone must escalate to ERASE under STRICTEST");
    Assertions.assertTrue(telephone.getContributingPolicies().contains(P1)
            && telephone.getContributingPolicies().contains(P2),
        "the escalated telephone rule must credit BOTH members; got: "
            + telephone.getContributingPolicies());
    Assertions.assertNotNull(country,
        "p2's country rule must join the snapshot rather than evicting p1's rules");
    Assertions.assertEquals(PolicyActionKind.ERASE, country.getAction());
  }


  @Test
  @Order(2)
  public void explicitConflictWithExistingMemberRefusesAndKeepsSnapshot() throws Exception {
    execute("CREATE TABLE %s (m INT, o BIGINT, b STRING) STORED AS ORC".formatted(EXPLICIT_TABLE));
    provision(P3, DSL_ERASE_TELEPHONE);
    provision(P4, DSL_REPLACE_TELEPHONE);

    execute(attachFull(P3, EXPLICIT_TABLE, "EXPLICIT"));
    final ErasurePolicyBinding b = binding(EXPLICIT_TABLE);
    final List<ErasurePolicyBindingResolved> before =
        hive.getBindingResolvedRules(b.getBindingId());
    Assertions.assertEquals(1, before.size(), "p3 alone resolves to its single rule");

    Assertions.assertThrows(CommandProcessorException.class,
        () -> execute("ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s".formatted(P4, EXPLICIT_TABLE, COLUMN)),
        "a second ATTACH that conflicts with the existing member must refuse under EXPLICIT");

    final List<ErasurePolicyBindingResolved> after =
        hive.getBindingResolvedRules(b.getBindingId());
    Assertions.assertEquals(1, after.size(),
        "the refused ATTACH must not evict or grow the persisted snapshot");
    Assertions.assertEquals("telephone", after.get(0).getFieldPath(),
        "p3's rule must still be the persisted snapshot");
    Assertions.assertEquals(PolicyActionKind.ERASE, after.get(0).getAction());
    Assertions.assertTrue(after.get(0).getContributingPolicies().contains(P3),
        "the surviving rule must still credit p3; got: "
            + after.get(0).getContributingPolicies());
  }


  @Test
  @Order(3)
  public void changingResolutionOnReAttachIsRefused() throws Exception {
    final CommandProcessorException ex = Assertions.assertThrows(CommandProcessorException.class,
        () -> execute(attachFull(P1, STRICT_TABLE, "EXPLICIT")),
        "re-ATTACH with a differing RESOLUTION must refuse");
    Assertions.assertTrue(messageChain(ex).contains("fixed at the first ATTACH"),
        "the refusal must explain that binding settings are fixed at the first ATTACH; got: "
            + messageChain(ex));
    Assertions.assertEquals(PolicyResolutionMode.STRICTEST,
        binding(STRICT_TABLE).getResolutionMode(),
        "a refused re-ATTACH must leave the stored mode untouched");
  }


  @Test
  @Order(4)
  public void eraseAppliesTheUnionOfAllMembers() throws Exception {
    execute("CREATE TABLE %s (m INT, o BIGINT, b STRING) STORED AS ORC".formatted(ERASE_TABLE));
    for (final String cmd : TestUtils.getInsertCommands(ERASE_TABLE, 1, 20, ColumnInternalFormat.JSON)) {
      execute(cmd);
    }
    provision(PA, DSL_ERASE_TELEPHONE);
    provision(PB, DSL_ERASE_IPLIST);
    execute(attachFull(PA, ERASE_TABLE, "EXPLICIT"));
    execute("ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s".formatted(PB, ERASE_TABLE, COLUMN));

    execute("ERASE FROM TABLE %s FOR IDENTITY VALUES (1)".formatted(ERASE_TABLE));

    final List<Object> rows = execute("SELECT m, o, b FROM %s".formatted(ERASE_TABLE));
    Assertions.assertEquals(7, rows.size(), "the erase must preserve the row count");
    int targeted = 0;
    int control = 0;
    for (final Object o : rows) {
      final String[] cols = ((String) o).split("\\t");
      if (cols.length != 3) {
        continue;
      }
      final Msg3 msg = JsonBodyConverter.convert(cols[2], Msg3.class);
      if (msg.getUserId() == 1) {
        targeted++;
        Assertions.assertEquals("", msg.getTelephone(),
            "pA's telephone rule must apply to the targeted identity");
        for (final String ip : msg.getIpList()) {
          Assertions.assertEquals("", ip,
              "pB's ipList rule must apply to the targeted identity");
        }
        Assertions.assertNotEquals("", msg.getCountry(),
            "country is named by neither member and must survive");
        Assertions.assertNotEquals("", msg.getCity(),
            "city is named by neither member and must survive");
      } else {
        control++;
        Assertions.assertEquals(2, msg.getUserId());
        Assertions.assertNotEquals("", msg.getTelephone(),
            "the untargeted identity must keep its telephone");
        Assertions.assertNotEquals("", msg.getCountry());
        Assertions.assertNotEquals("", msg.getCity());
        for (final String ip : msg.getIpList()) {
          Assertions.assertNotEquals("", ip,
              "the untargeted identity must keep its ipList");
        }
      }
    }
    Assertions.assertEquals(6, targeted, "six bodies carry userId=1");
    Assertions.assertEquals(1, control, "one body carries userId=2");
  }



  @Test
  @Order(5)
  public void resolvedSnapshotKeepsInspectAndFlagActionKinds() throws Exception {
    execute("CREATE TABLE %s (m INT, o BIGINT, b STRING) STORED AS ORC".formatted(MIXED_TABLE));
    provision(PM, DSL_MIXED_ACTIONS);
    execute(attachFull(PM, MIXED_TABLE, "EXPLICIT"));

    final ErasurePolicyBinding b = binding(MIXED_TABLE);
    final List<ErasurePolicyBindingResolved> rules =
        hive.getBindingResolvedRules(b.getBindingId());
    Assertions.assertEquals(3, rules.size(), "all three rules must persist");
    PolicyActionKind telephone = null;
    PolicyActionKind city = null;
    PolicyActionKind country = null;
    for (final ErasurePolicyBindingResolved r : rules) {
      switch (r.getFieldPath()) {
        case "telephone": telephone = r.getAction(); break;
        case "city":      city      = r.getAction(); break;
        case "country":   country   = r.getAction(); break;
        default: Assertions.fail("unexpected field path " + r.getFieldPath());
      }
    }
    Assertions.assertEquals(PolicyActionKind.ERASE, telephone);
    Assertions.assertEquals(PolicyActionKind.INSPECT, city,
        "INSPECT must not be misrecorded as ERASE in the resolved snapshot");
    Assertions.assertEquals(PolicyActionKind.FLAG, country,
        "FLAG must not be misrecorded as ERASE in the resolved snapshot");
  }

  @Test
  @Order(99)
  public void cleanup() {
    for (final String t : new String[] { STRICT_TABLE, EXPLICIT_TABLE, ERASE_TABLE }) {
      try { execute("DETACH DATA ERASURE POLICY ON TABLE %s COLUMN %s".formatted(t, COLUMN)); }
      catch (CommandProcessorException ignore) {  }
      try { execute("DROP TABLE IF EXISTS %s".formatted(t)); }
      catch (CommandProcessorException ignore) {  }
    }
    for (final String p : managedErasurePolicies()) {
      try { execute("DROP DATA ERASURE POLICY IF EXISTS %s".formatted(p)); }
      catch (CommandProcessorException ignore) {  }
    }
  }


  private void provision(final String policy, final String dsl)
      throws CommandProcessorException, IOException {
    final Path file = Files.createTempFile(policy + "_", ".erp");
    Files.write(file, dsl.getBytes());
    execute("LOAD ERASURE POLICY %s FROM '%s'".formatted(policy, file));
    execute("VALIDATE ERASURE POLICY %s".formatted(policy));
    execute("ACTIVATE ERASURE POLICY %s".formatted(policy));
  }

  private String attachFull(final String policy, final String table, final String mode) {
    return ("ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s"
        + " WITH ( SCHEMA FIELD (m), ROW LOCATOR (o), COLUMN FORMAT (JSON) )"
        + " RESOLUTION ( %s )").formatted(policy, table, COLUMN, mode);
  }

  private ErasurePolicyBinding binding(final String table) throws Exception {
    final long tblId = hive.getTable(table).getTTable().getId();
    final ErasurePolicyBinding b = hive.getErasurePolicyBinding(tblId, COLUMN);
    Assertions.assertNotNull(b, "expected a binding on " + table + "." + COLUMN);
    return b;
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
