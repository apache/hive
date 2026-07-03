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

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.model.MErasureRunLock;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.UUID;

/**
 * Tests the per-table ERASE FROM TABLE run-lock semantics described in
 * §4.7 of the paper. Exercises the four ObjectStore methods directly,
 * bypassing the thrift wire path (which is not yet wired; see the TODO
 * in HiveMetaStoreClient.acquireErasureRunLock and the
 * IMetaStoreClient.acquireErasureRunLock declaration).
 */
@Category(MetastoreUnitTest.class)
public class TestErasureRunLock {

  private static final long TBL_ID = 4242L;
  private static final long RUN_A = 100L;
  private static final long RUN_B = 200L;
  private static final String PRINCIPAL_A = "ops_team_a";
  private static final String PRINCIPAL_B = "ops_team_b";
  private static final String DPO = "dpo@operator.example";

  private ObjectStore objectStore;
  private Configuration conf;

  @Before
  public void setUp() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.HIVE_IN_TEST, true);
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    String currentUrl = MetastoreConf.getVar(conf, ConfVars.CONNECT_URL_KEY);
    currentUrl = currentUrl.replace(MetaStoreServerUtils.JUNIT_DATABASE_PREFIX,
        String.format("%s_%s", MetaStoreServerUtils.JUNIT_DATABASE_PREFIX,
            UUID.randomUUID().toString()));
    MetastoreConf.setVar(conf, ConfVars.CONNECT_URL_KEY, currentUrl);
    objectStore = new ObjectStore();
    objectStore.setConf(conf);
  }

  @After
  public void tearDown() throws Exception {
    System.clearProperty(ObjectStore.TRUSTSTORE_PATH_KEY);
    System.clearProperty(ObjectStore.TRUSTSTORE_PASSWORD_KEY);
    System.clearProperty(ObjectStore.TRUSTSTORE_TYPE_KEY);
  }

  /**
   * Happy path: acquire, then complete on clean exit. The lock row
   * transitions RUNNING -> COMPLETED, with completedTs set and
   * releasedBy/releasedTs null.
   */
  @Test
  public void acquireThenCompleteLifecycle() throws MetaException {
    MErasureRunLock acquired = objectStore.acquireErasureRunLock(TBL_ID, RUN_A, PRINCIPAL_A);
    Assert.assertNotNull(acquired);
    Assert.assertEquals(TBL_ID, acquired.getTblId());
    Assert.assertEquals(RUN_A, acquired.getRunId());
    Assert.assertEquals(PRINCIPAL_A, acquired.getPrincipal());
    Assert.assertEquals(MErasureRunLock.STATUS_RUNNING, acquired.getStatus());
    Assert.assertNull(acquired.getCompletedTs());
    Assert.assertNull(acquired.getReleasedBy());

    boolean matched = objectStore.completeErasureRunLock(TBL_ID, RUN_A);
    Assert.assertTrue("completeErasureRunLock should match the holder", matched);

    MErasureRunLock after = objectStore.getErasureRunLock(TBL_ID);
    Assert.assertNotNull(after);
    Assert.assertEquals(MErasureRunLock.STATUS_COMPLETED, after.getStatus());
    Assert.assertNotNull(after.getCompletedTs());
    Assert.assertNull(after.getReleasedBy());
  }

  /**
   * Second ERASE while the first holds the lock: acquire refuses with a
   * MetaException whose message names the holder so the analyzer can
   * surface a clear error to the operator.
   */
  @Test
  public void concurrentAcquireIsRefusedWhileFirstHolds() throws MetaException {
    objectStore.acquireErasureRunLock(TBL_ID, RUN_A, PRINCIPAL_A);
    try {
      objectStore.acquireErasureRunLock(TBL_ID, RUN_B, PRINCIPAL_B);
      Assert.fail("expected MetaException because the lock is already held");
    } catch (MetaException e) {
      Assert.assertTrue("error should name the runId that holds the lock",
          e.getMessage().contains("runId=" + RUN_A));
      Assert.assertTrue("error should name the principal that holds the lock",
          e.getMessage().contains(PRINCIPAL_A));
    }
  }

  /**
   * After the first run completes cleanly, a second ERASE on the same
   * table can acquire the lock. The first run's terminal-status row is
   * superseded by the second run's RUNNING row (one row per tblId).
   */
  @Test
  public void newAcquireAfterCompletionSucceeds() throws MetaException {
    objectStore.acquireErasureRunLock(TBL_ID, RUN_A, PRINCIPAL_A);
    Assert.assertTrue(objectStore.completeErasureRunLock(TBL_ID, RUN_A));

    MErasureRunLock second = objectStore.acquireErasureRunLock(TBL_ID, RUN_B, PRINCIPAL_B);
    Assert.assertEquals(RUN_B, second.getRunId());
    Assert.assertEquals(MErasureRunLock.STATUS_RUNNING, second.getStatus());
  }

  /**
   * Manual release via RELEASE ERASURE LOCK (without FORCE). Status
   * transitions to RELEASED, releasedBy and releasedTs are set, and
   * releaseReason is recorded.
   */
  @Test
  public void manualReleaseRecordsAuditFields() throws MetaException, NoSuchObjectException {
    objectStore.acquireErasureRunLock(TBL_ID, RUN_A, PRINCIPAL_A);
    final String reason = "AM crashed at 14:30, container logs confirmed dead";
    MErasureRunLock released = objectStore.manuallyReleaseErasureRunLock(
        TBL_ID, DPO, reason, false /* not FORCE */);
    Assert.assertEquals(MErasureRunLock.STATUS_RELEASED, released.getStatus());
    Assert.assertEquals(DPO, released.getReleasedBy());
    Assert.assertEquals(reason, released.getReleaseReason());
    Assert.assertNotNull(released.getReleasedTs());
  }

  /**
   * Manual release with FORCE flag. Status transitions to FORCE_RELEASED,
   * which is distinguishable in the audit trail from a clean RELEASED.
   * The operator-supplied reason must reach the row so a compliance
   * auditor can read it later (the analyzer requires WITH REASON when
   * FORCE is specified; this confirms the storage layer round-trips it).
   */
  @Test
  public void forceReleaseRecordsDistinctStatus() throws MetaException, NoSuchObjectException {
    objectStore.acquireErasureRunLock(TBL_ID, RUN_A, PRINCIPAL_A);
    final String reason = "recovering from full-cluster restart at 16:00";
    MErasureRunLock released = objectStore.manuallyReleaseErasureRunLock(
        TBL_ID, DPO, reason, true /* FORCE */);
    Assert.assertEquals(MErasureRunLock.STATUS_FORCE_RELEASED, released.getStatus());
    Assert.assertEquals(DPO, released.getReleasedBy());
    Assert.assertEquals(reason, released.getReleaseReason());
    Assert.assertNotNull(released.getReleasedTs());

    // Round-trip through a fresh fetch to confirm the reason persists
    // beyond the immediate return value (the analyzer reads it back this
    // way for SHOW ERASURE LOCKS).
    MErasureRunLock refetched = objectStore.getErasureRunLock(TBL_ID);
    Assert.assertNotNull(refetched);
    Assert.assertEquals(MErasureRunLock.STATUS_FORCE_RELEASED, refetched.getStatus());
    Assert.assertEquals(reason, refetched.getReleaseReason());
    Assert.assertEquals(DPO, refetched.getReleasedBy());
  }

  /**
   * RELEASE on a table with no held lock raises NoSuchObjectException so
   * the operator's command surfaces a clear error rather than silently
   * succeeding against nothing.
   */
  @Test
  public void releaseWithoutHeldLockRaisesNoSuchObject() throws MetaException {
    try {
      objectStore.manuallyReleaseErasureRunLock(TBL_ID, DPO,
          "release attempt against unheld lock", false);
      Assert.fail("expected NoSuchObjectException because no RUNNING lock exists");
    } catch (NoSuchObjectException e) {
      Assert.assertTrue(e.getMessage().contains(Long.toString(TBL_ID)));
    }
  }

  /**
   * After a manual release, a fresh acquire is allowed. The lock-row
   * lifecycle is: acquire (RUNNING) -> manual release (RELEASED) -> acquire
   * (RUNNING). This is the operator-recovery path when a previous run
   * was crashed and then explicitly cleared.
   */
  @Test
  public void acquireAfterManualReleaseSucceeds() throws MetaException, NoSuchObjectException {
    objectStore.acquireErasureRunLock(TBL_ID, RUN_A, PRINCIPAL_A);
    objectStore.manuallyReleaseErasureRunLock(TBL_ID, DPO,
        "operator-initiated recovery", false);

    MErasureRunLock fresh = objectStore.acquireErasureRunLock(TBL_ID, RUN_B, PRINCIPAL_B);
    Assert.assertEquals(RUN_B, fresh.getRunId());
    Assert.assertEquals(MErasureRunLock.STATUS_RUNNING, fresh.getStatus());
    Assert.assertEquals(PRINCIPAL_B, fresh.getPrincipal());
  }

  /**
   * completeErasureRunLock with a wrong runId is a no-op signalling that
   * the lock was reclaimed by another command. Returns false so the
   * caller can surface a clearer "your run was stolen" error rather than
   * mistakenly believing it completed normally.
   */
  @Test
  public void completeWithStaleRunIdReturnsFalse() throws MetaException, NoSuchObjectException {
    objectStore.acquireErasureRunLock(TBL_ID, RUN_A, PRINCIPAL_A);
    objectStore.manuallyReleaseErasureRunLock(TBL_ID, DPO, "force-recovery", true);
    objectStore.acquireErasureRunLock(TBL_ID, RUN_B, PRINCIPAL_B);

    // Original run RUN_A tries to complete; it should not match.
    boolean matched = objectStore.completeErasureRunLock(TBL_ID, RUN_A);
    Assert.assertFalse(
        "completeErasureRunLock with a stale runId must not match the new holder",
        matched);

    // The new holder's lock is still RUNNING.
    MErasureRunLock current = objectStore.getErasureRunLock(TBL_ID);
    Assert.assertEquals(MErasureRunLock.STATUS_RUNNING, current.getStatus());
    Assert.assertEquals(RUN_B, current.getRunId());
  }

  /**
   * getErasureRunLock on a never-acquired table returns null rather than
   * raising, so SHOW ERASURE LOCK can distinguish "no lock" from "lookup
   * error."
   */
  @Test
  public void getOnUnheldTableReturnsNull() {
    MErasureRunLock m = objectStore.getErasureRunLock(987654321L);
    Assert.assertNull(m);
  }

  /**
   * The operator-supplied release reason must reach the metastore row
   * and survive a re-read through {@code getErasureRunLock}. The analyzer
   * gates {@code RELEASE ERASURE LOCK ON TABLE t FORCE WITH REASON
   * '<text>'} so the audit trail always carries a justification for a
   * forced release; this test verifies the storage layer round-trips
   * the supplied string verbatim (no truncation, escaping, or null
   * coercion) for both the soft and FORCE paths.
   */
  @Test
  public void releaseReasonRoundTripsThroughMetastore()
      throws MetaException, NoSuchObjectException {
    // Soft release: arbitrary reason string, including quotes and a
    // newline, to verify the field is treated as opaque text.
    objectStore.acquireErasureRunLock(TBL_ID, RUN_A, PRINCIPAL_A);
    final String softReason = "ops ticket #4711: 'stuck' run from 2026-05-24\n"
        + "    confirmed dead via YARN UI";
    objectStore.manuallyReleaseErasureRunLock(TBL_ID, DPO, softReason, false);
    MErasureRunLock softReread = objectStore.getErasureRunLock(TBL_ID);
    Assert.assertNotNull(softReread);
    Assert.assertEquals(MErasureRunLock.STATUS_RELEASED, softReread.getStatus());
    Assert.assertEquals(DPO, softReread.getReleasedBy());
    Assert.assertEquals("reason must round-trip verbatim through the metastore",
        softReason, softReread.getReleaseReason());
  }

  /**
   * Compliance-reviewer use case: an ERASE row and a subsequent FORCE
   * release row must appear together in the audit table read by
   * {@code AUDIT ERASURE EXECUTIONS}. This test writes both events
   * directly through the ObjectStore and asserts that
   * {@code getErasureRunsForTable} returns them ordered by startedTs,
   * with the release reason preserved on the release row.
   */
  @Test
  public void releaseEventVisibleInAuditExecutions()
      throws MetaException, NoSuchObjectException {
    final long auditTbl = 8888L;
    final long eraseStart = 1000L;
    final long releaseStart = 2000L;
    final String forceReason = "AM crash, no live containers on 2026-05-24";

    // 1. Simulate the ERASE-run audit row (the analyzer writes this on
    //    ERASE FROM TABLE entry).
    org.apache.hadoop.hive.metastore.api.ErasureRunAudit erase =
        new org.apache.hadoop.hive.metastore.api.ErasureRunAudit();
    erase.setTblId(auditTbl);
    erase.setColumnName("telephone");
    erase.setBindingId(0L);
    erase.setPrincipal(PRINCIPAL_A);
    erase.setStartedTs(eraseStart);
    erase.setCompletedTs(eraseStart + 100L);
    erase.setStatus(org.apache.hadoop.hive.metastore.api.ErasureRunStatus.SUCCEEDED);
    erase.setFilesRewritten(3);
    objectStore.recordErasureRun(erase);

    // 2. The companion FORCE-release audit row written by the analyzer's
    //    analyzeReleaseErasureLock after a RELEASE ERASURE LOCK ... FORCE
    //    WITH REASON 'text' command.
    org.apache.hadoop.hive.metastore.api.ErasureRunAudit release =
        new org.apache.hadoop.hive.metastore.api.ErasureRunAudit();
    release.setTblId(auditTbl);
    release.setColumnName("");
    release.setBindingId(0L);
    release.setPrincipal(DPO);
    release.setStartedTs(releaseStart);
    release.setCompletedTs(releaseStart);
    release.setStatus(org.apache.hadoop.hive.metastore.api.ErasureRunStatus.FORCE_RELEASED);
    release.setReleaseReason(forceReason);
    objectStore.recordErasureRun(release);

    // 3. The compliance reviewer's one-query view: AUDIT ERASURE EXECUTIONS
    //    reads getErasureRunsForTable with no filters, returns both rows
    //    in startedTs order.
    java.util.List<org.apache.hadoop.hive.metastore.api.ErasureRunAudit> all =
        objectStore.getErasureRunsForTable(
            auditTbl, Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    Assert.assertEquals("expected both ERASE and RELEASE rows", 2, all.size());

    final org.apache.hadoop.hive.metastore.api.ErasureRunAudit first = all.get(0);
    final org.apache.hadoop.hive.metastore.api.ErasureRunAudit second = all.get(1);

    Assert.assertEquals(eraseStart, first.getStartedTs());
    Assert.assertEquals(
        org.apache.hadoop.hive.metastore.api.ErasureRunStatus.SUCCEEDED,
        first.getStatus());
    Assert.assertFalse("ERASE row must not carry a release reason",
        first.isSetReleaseReason());

    Assert.assertEquals(releaseStart, second.getStartedTs());
    Assert.assertEquals(
        org.apache.hadoop.hive.metastore.api.ErasureRunStatus.FORCE_RELEASED,
        second.getStatus());
    Assert.assertEquals(DPO, second.getPrincipal());
    Assert.assertTrue("FORCE-release row must carry the reason",
        second.isSetReleaseReason());
    Assert.assertEquals(forceReason, second.getReleaseReason());
  }

  /**
   * Same round-trip check for a FORCE release on a fresh tblId. Kept
   * separate from the soft-release test so the FORCE_RELEASED status is
   * asserted alongside the reason.
   */
  @Test
  public void forceReleaseReasonRoundTripsThroughMetastore()
      throws MetaException, NoSuchObjectException {
    final long FRESH_TBL = 7777L;
    objectStore.acquireErasureRunLock(FRESH_TBL, RUN_A, PRINCIPAL_A);
    final String forceReason = "cluster-wide power event on 2026-05-24, "
        + "verified no live YARN containers";
    objectStore.manuallyReleaseErasureRunLock(FRESH_TBL, DPO, forceReason, true);
    MErasureRunLock forceReread = objectStore.getErasureRunLock(FRESH_TBL);
    Assert.assertNotNull(forceReread);
    Assert.assertEquals(MErasureRunLock.STATUS_FORCE_RELEASED, forceReread.getStatus());
    Assert.assertEquals(DPO, forceReread.getReleasedBy());
    Assert.assertEquals("FORCE-release reason must round-trip verbatim",
        forceReason, forceReread.getReleaseReason());
    Assert.assertNotNull(forceReread.getReleasedTs());
  }
}
