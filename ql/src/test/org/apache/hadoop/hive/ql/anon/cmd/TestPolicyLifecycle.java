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
import org.apache.hadoop.hive.metastore.api.PolicyVersionStatus;
import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TestPolicyLifecycle extends BaseTest {

  private static final String POLICY = "tpl_user_pii";
  private static final String POLICY_REPROMOTE     = "tpl_repromote";
  private static final String POLICY_SINGLE_ACTIVE = "tpl_single_active";
  private static final String POLICY_NO_ACTIVE     = "tpl_no_active";
  private static final String TABLE_NO_ACTIVE      = "tpl_no_active_t_" + Long.toString(System.nanoTime(), 36);
  private static final String POLICY_SUPERSEDED_TERM = "tpl_super_terminal";

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] { POLICY, POLICY_REPROMOTE, POLICY_SINGLE_ACTIVE,
        POLICY_NO_ACTIVE, POLICY_SUPERSEDED_TERM };
  }

  private static String dsl(final String version) {
    return "VERSION " + version + "\n"
        + """
        IDENTITY userId TYPE INT
        SCHEMA   TYPE STRING
        FOR SCHEMA 'user_info_v1'
            ERASE   telephone, ipList
            REPLACE country = 'UNKNOWN', city = 'UNKNOWN'
        """;
  }

  private static Path v1File;
  private static Path v2File;

  @BeforeAll
  public void writePolicyFile() throws IOException {
    v1File = Files.createTempFile("tpl_user_pii_v1_", ".erp");
    v2File = Files.createTempFile("tpl_user_pii_v2_", ".erp");
    Files.write(v1File, dsl("v1").getBytes());
    Files.write(v2File, dsl("v2").getBytes());
  }

  @Test
  @Order(1)
  public void loadCreatesDraftThenValidatePromotes()
      throws CommandProcessorException, HiveException {

    execute("LOAD ERASURE POLICY " + POLICY + " FROM '" + v1File + "'");
    final ErasurePolicyVersion afterFirstLoad = latestVersion(POLICY);
    Assertions.assertNotNull(afterFirstLoad, "LOAD must persist a version row");
    Assertions.assertEquals(PolicyVersionStatus.DRAFT, afterFirstLoad.getStatus(),
        "fresh LOAD must persist version in DRAFT");
    final long firstLoadVersionId = afterFirstLoad.getVersionId();

    execute("VALIDATE ERASURE POLICY " + POLICY);
    final ErasurePolicyVersion afterFirstValidate = latestVersion(POLICY);
    Assertions.assertEquals(firstLoadVersionId, afterFirstValidate.getVersionId(),
        "VALIDATE must promote the same row (preserve versionId)");
    Assertions.assertEquals(PolicyVersionStatus.VALIDATED, afterFirstValidate.getStatus(),
        "VALIDATE must transition the row from DRAFT to VALIDATED");

    execute("ACTIVATE ERASURE POLICY " + POLICY);

    execute("LOAD ERASURE POLICY " + POLICY + " FROM '" + v2File + "'");
    final ErasurePolicyVersion afterSecondLoad = latestVersion(POLICY);
    Assertions.assertNotEquals(firstLoadVersionId, afterSecondLoad.getVersionId(),
        "second LOAD must produce a new versionId");
    Assertions.assertEquals(PolicyVersionStatus.DRAFT, afterSecondLoad.getStatus(),
        "second LOAD's row must materialise in DRAFT");

    execute("VALIDATE ERASURE POLICY " + POLICY);
    final ErasurePolicyVersion afterSecondValidate = latestVersion(POLICY);
    Assertions.assertEquals(afterSecondLoad.getVersionId(),
        afterSecondValidate.getVersionId(),
        "second VALIDATE must promote the latest DRAFT in place");
    Assertions.assertEquals(PolicyVersionStatus.VALIDATED, afterSecondValidate.getStatus(),
        "second VALIDATE must transition the row to VALIDATED");
  }

  private ErasurePolicyVersion latestVersion(String policyName) throws HiveException {
    final List<ErasurePolicyVersion> all = hive.listErasurePolicyVersions(policyName);
    if (all == null || all.isEmpty()) {
      return null;
    }
    ErasurePolicyVersion latest = all.get(0);
    for (final ErasurePolicyVersion v : all) {
      if (v.getVersionId() > latest.getVersionId()) {
        latest = v;
      }
    }
    return latest;
  }

  @Test
  @Order(2)
  public void activatePromotesLatestValidated() throws CommandProcessorException {
    execute("ACTIVATE ERASURE POLICY " + POLICY);
  }

  @Test
  @Order(3)
  public void deactivateTransitionsToInactive()
      throws CommandProcessorException, HiveException {
    final ErasurePolicyVersion beforeDeactivate = hive.getActiveErasurePolicyVersion(POLICY);
    Assertions.assertNotNull(beforeDeactivate,
        "precondition: an ACTIVE version must exist before DEACTIVATE runs");
    final long activeVersionId = beforeDeactivate.getVersionId();

    execute("DEACTIVATE ERASURE POLICY " + POLICY);

    final ErasurePolicyVersion stillActive = hive.getActiveErasurePolicyVersion(POLICY);
    Assertions.assertNull(stillActive,
        "DEACTIVATE must leave the policy with no ACTIVE version");

    final List<ErasurePolicyVersion> all = hive.listErasurePolicyVersions(POLICY);
    ErasurePolicyVersion deactivated = null;
    for (final ErasurePolicyVersion v : all) {
      if (v.getVersionId() == activeVersionId) {
        deactivated = v;
        break;
      }
    }
    Assertions.assertNotNull(deactivated,
        "the previously-active row must still exist (DEACTIVATE does not delete)");
    Assertions.assertEquals(PolicyVersionStatus.INACTIVE, deactivated.getStatus(),
        "DEACTIVATE must transition the ACTIVE row to INACTIVE in place");
  }

  @Test
  @Order(4)
  public void dropAfterLifecycleRefuses() {
    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute("DROP DATA ERASURE POLICY IF EXISTS " + POLICY),
        "DROP on a fully-lifecycled policy must refuse, even with IF EXISTS, "
            + "because the audit invariant is not optional");
    final String chain = collectMessages(ex);
    Assertions.assertTrue(chain.contains("non-DRAFT"),
        "refusal must name the offending non-DRAFT state; got: " + chain);
    Assertions.assertTrue(chain.contains("DEACTIVATE"),
        "refusal must point at DEACTIVATE as the retirement path; got: " + chain);
  }


  @Test
  @Order(5)
  public void inactiveVersionIsRepromotedByActivate()
      throws CommandProcessorException, HiveException {
    execute("LOAD ERASURE POLICY " + POLICY_REPROMOTE + " FROM '" + v1File + "'");
    execute("VALIDATE ERASURE POLICY " + POLICY_REPROMOTE);
    execute("ACTIVATE ERASURE POLICY " + POLICY_REPROMOTE);
    execute("DEACTIVATE ERASURE POLICY " + POLICY_REPROMOTE);

    execute("ACTIVATE ERASURE POLICY " + POLICY_REPROMOTE);

    final ErasurePolicyVersion active = hive.getActiveErasurePolicyVersion(POLICY_REPROMOTE);
    Assertions.assertNotNull(active,
        "ACTIVATE should re-promote the INACTIVE row to ACTIVE");
    Assertions.assertEquals(PolicyVersionStatus.ACTIVE, active.getStatus(),
        "re-promoted row must carry status ACTIVE");

  }

  @Test
  @Order(6)
  public void activatingNewVersionSupersedesPriorActive()
      throws CommandProcessorException, HiveException {
    execute("LOAD ERASURE POLICY " + POLICY_SINGLE_ACTIVE + " FROM '" + v1File + "'");
    execute("VALIDATE ERASURE POLICY " + POLICY_SINGLE_ACTIVE);
    execute("ACTIVATE ERASURE POLICY " + POLICY_SINGLE_ACTIVE);
    final ErasurePolicyVersion v1 = hive.getActiveErasurePolicyVersion(POLICY_SINGLE_ACTIVE);
    Assertions.assertNotNull(v1, "v1 should be ACTIVE after the first ACTIVATE");

    execute("LOAD ERASURE POLICY " + POLICY_SINGLE_ACTIVE + " FROM '" + v2File + "'");
    execute("VALIDATE ERASURE POLICY " + POLICY_SINGLE_ACTIVE);
    execute("ACTIVATE ERASURE POLICY " + POLICY_SINGLE_ACTIVE);
    final ErasurePolicyVersion v2 = hive.getActiveErasurePolicyVersion(POLICY_SINGLE_ACTIVE);
    Assertions.assertNotNull(v2, "v2 should be ACTIVE after the second ACTIVATE");
    Assertions.assertNotEquals(v1.getVersionId(), v2.getVersionId(),
        "v2 must be a different version row than v1");

    final List<ErasurePolicyVersion> all = hive.listErasurePolicyVersions(POLICY_SINGLE_ACTIVE);
    int activeCount = 0;
    ErasurePolicyVersion v1Latest = null;
    for (final ErasurePolicyVersion v : all) {
      if (v.getStatus() == PolicyVersionStatus.ACTIVE) {
        activeCount++;
      }
      if (v.getVersionId() == v1.getVersionId()) {
        v1Latest = v;
      }
    }
    Assertions.assertEquals(1, activeCount,
        "single-ACTIVE invariant: exactly one version may be ACTIVE at any time");
    Assertions.assertNotNull(v1Latest, "v1 must still exist as a row in history");
    Assertions.assertEquals(PolicyVersionStatus.SUPERSEDED, v1Latest.getStatus(),
        "v1 must be automatically demoted to SUPERSEDED when v2 is activated");

  }

  @Test
  @Order(7)
  public void eraseRefusesWhenBoundPolicyHasNoActiveVersion()
      throws CommandProcessorException {
    try { execute("DROP TABLE IF EXISTS " + TABLE_NO_ACTIVE); }
    catch (CommandProcessorException ignored) {  }
    try { execute("DROP DATA ERASURE POLICY IF EXISTS " + POLICY_NO_ACTIVE); }
    catch (CommandProcessorException ignored) {  }

    execute("CREATE TABLE IF NOT EXISTS %s (m INT, o BIGINT, b STRING) STORED AS ORC"
        .formatted(TABLE_NO_ACTIVE));
    execute("LOAD ERASURE POLICY " + POLICY_NO_ACTIVE + " FROM '" + v1File + "'");
    execute("VALIDATE ERASURE POLICY " + POLICY_NO_ACTIVE);

    final CommandProcessorException ex = Assertions.assertThrows(
        CommandProcessorException.class,
        () -> execute("ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN b"
            .formatted(POLICY_NO_ACTIVE, TABLE_NO_ACTIVE)),
        "ATTACH must refuse when the policy has no ACTIVE version");
    final String chain = collectMessages(ex);
    Assertions.assertTrue(chain.contains("no ACTIVE version"),
        "refusal must name the missing ACTIVE version; got: " + chain);

    try { execute("DROP TABLE IF EXISTS " + TABLE_NO_ACTIVE); }
    catch (CommandProcessorException expected) {  }
  }

  @Test
  @Order(8)
  public void supersededIsTerminal()
      throws CommandProcessorException, HiveException {
    execute("LOAD ERASURE POLICY " + POLICY_SUPERSEDED_TERM + " FROM '" + v1File + "'");
    execute("VALIDATE ERASURE POLICY " + POLICY_SUPERSEDED_TERM);
    execute("ACTIVATE ERASURE POLICY " + POLICY_SUPERSEDED_TERM);
    final ErasurePolicyVersion v1 = hive.getActiveErasurePolicyVersion(POLICY_SUPERSEDED_TERM);
    Assertions.assertNotNull(v1, "v1 should be ACTIVE after first ACTIVATE");
    final long v1Id = v1.getVersionId();

    execute("LOAD ERASURE POLICY " + POLICY_SUPERSEDED_TERM + " FROM '" + v2File + "'");
    execute("VALIDATE ERASURE POLICY " + POLICY_SUPERSEDED_TERM);
    execute("ACTIVATE ERASURE POLICY " + POLICY_SUPERSEDED_TERM);
    final ErasurePolicyVersion v2 = hive.getActiveErasurePolicyVersion(POLICY_SUPERSEDED_TERM);
    Assertions.assertNotNull(v2);
    Assertions.assertNotEquals(v1Id, v2.getVersionId(), "v2 must be a different row than v1");
    Assertions.assertEquals(PolicyVersionStatus.SUPERSEDED, findById(v1Id).getStatus(),
        "v1 must transition to SUPERSEDED when v2 is activated");

    execute("DEACTIVATE ERASURE POLICY " + POLICY_SUPERSEDED_TERM);
    execute("ACTIVATE ERASURE POLICY " + POLICY_SUPERSEDED_TERM);
    final ErasurePolicyVersion afterRepromote = hive.getActiveErasurePolicyVersion(POLICY_SUPERSEDED_TERM);
    Assertions.assertNotNull(afterRepromote, "ACTIVATE should re-promote the INACTIVE v2");
    Assertions.assertEquals(v2.getVersionId(), afterRepromote.getVersionId(),
        "ACTIVATE must pick v2 (INACTIVE), not v1 (SUPERSEDED)");

    final ErasurePolicyVersion v1Final = findById(v1Id);
    Assertions.assertEquals(PolicyVersionStatus.SUPERSEDED, v1Final.getStatus(),
        "v1 must remain SUPERSEDED throughout; SUPERSEDED has no outgoing transitions");

  }

  private ErasurePolicyVersion findById(long versionId) throws HiveException {
    for (final ErasurePolicyVersion v : hive.listErasurePolicyVersions(POLICY_SUPERSEDED_TERM)) {
      if (v.getVersionId() == versionId) {
        return v;
      }
    }
    return null;
  }

  private static String collectMessages(Throwable t) {
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
