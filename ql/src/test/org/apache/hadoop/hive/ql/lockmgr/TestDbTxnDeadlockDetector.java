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
package org.apache.hadoop.hive.ql.lockmgr;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.LockState;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponse;
import org.apache.hadoop.hive.metastore.api.ShowLocksResponseElement;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.apache.hadoop.hive.metastore.metrics.MetricsConstants;
import org.apache.hadoop.hive.metastore.txn.service.DeadlockDetectorService;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * End-to-end tests for the metastore transaction deadlock detector
 * ({@code DeadlockDetectorService} / {@code DeadlockDetectionFunction}).
 */
public class TestDbTxnDeadlockDetector extends DbTxnManagerEndToEndTestBase {

  /**
   * N-party deadlock cycle: txn_i holds table_i, then requests table_{(i+1) % n}, forming
   * a complete wait-for cycle. The detector must abort the youngest member (txn_{n-1}) and
   * leave the rest live; the predecessor's waiting lock must become acquirable. Tarjan's
   * SCC handles cycles of any size uniformly, so {n=2, 3} cover the canonical case and a
   * non-trivial SCC respectively.
   */
  @Test
  public void testDeadlockDetectionMultiParty() throws Exception {
    for (int n : new int[]{2, 3}) {
      assertCycleAbortsYoungest(n);
    }
  }

  private void assertCycleAbortsYoungest(int n) throws Exception {
    String[] tables = new String[n];
    for (int i = 0; i < n; i++) {
      tables[i] = "DLM" + n + "_T" + i;
    }
    setupDeadlockTestTables(tables);

    HiveTxnManager[] mgrs = new HiveTxnManager[n];
    long[] ids = new long[n];
    for (int i = 0; i < n; i++) {
      mgrs[i] = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
      swapTxnManager(mgrs[i]);
      mgrs[i].openTxn(ctx, "Txn" + i);
      ids[i] = mgrs[i].getCurrentTxnId();
      driver.compileAndRespond("update " + tables[i] + " set a = 1 where b = 1", true);
      mgrs[i].acquireLocks(driver.getPlan(), ctx, "Txn" + i);
    }
    for (int i = 1; i < n; i++) {
      Assert.assertTrue("Txn IDs must be monotonically increasing", ids[i] > ids[i - 1]);
    }

    for (int i = 0; i < n; i++) {
      swapTxnManager(mgrs[i]);
      driver.compileAndRespond("update " + tables[(i + 1) % n] + " set a = 2 where b = 2", true);
      ((DbTxnManager) mgrs[i]).acquireLocks(driver.getPlan(), ctx, "Txn" + i, false);
    }

    runDeadlockDetector();

    int victim = n - 1;
    LockException ex = null;
    try {
      mgrs[victim].heartbeat();
    } catch (LockException e) {
      ex = e;
    }
    Assert.assertNotNull(n + "-party: heartbeat on victim Txn" + victim + " should fail", ex);
    Assert.assertTrue(n + "-party: cause should be TxnAbortedException, was: " + ex,
        ex.getCause() instanceof org.apache.hadoop.hive.metastore.api.TxnAbortedException);
    for (int i = 0; i < victim; i++) {
      mgrs[i].heartbeat();
    }

    // Predecessor's waiting lock (on the victim's table) is now acquirable.
    int predecessor = victim - 1;
    swapTxnManager(mgrs[predecessor]);
    List<ShowLocksResponseElement> locks = getLocks(mgrs[predecessor]);
    long waitingLockId = findWaitingLockId(locks, tables[victim], ids[predecessor]);
    Assert.assertTrue(n + "-party: predecessor's waiting lock should exist", waitingLockId > 0);
    LockState state = ((DbLockManager) mgrs[predecessor].getLockManager()).checkLock(waitingLockId);
    Assert.assertEquals(n + "-party: predecessor's lock should now be acquired",
        LockState.ACQUIRED, state);

    for (int i = 0; i < victim; i++) {
      mgrs[i].rollbackTxn();
    }
    for (HiveTxnManager m : mgrs) {
      m.closeTxnManager();
    }
    swapTxnManager(txnMgr);
  }

  /**
   * Linear wait chain (no cycle): txn1 holds lock, txn2 waits for txn1.
   * The detector must NOT abort either transaction.
   */
  @Test
  public void testNoFalsePositiveLinearChain() throws Exception {
    setupDeadlockTestTables("NF_T1");
    HiveTxnManager txnMgr1 = openTxnWithStatement("Txn1", "update NF_T1 set a = 1 where b = 1");
    HiveTxnManager txnMgr2 = openTxn("Txn2");
    runStatementNonBlocking(txnMgr2, "Txn2", "update NF_T1 set a = 2 where b = 2");

    long waitingLockId = findWaitingLockId(getLocks(txnMgr2), "NF_T1", txnMgr2.getCurrentTxnId());
    Assert.assertTrue("Should have found txn2's waiting lock", waitingLockId > 0);

    long deadlockedBefore = getDeadlockedCounter();
    runDeadlockDetector();
    Assert.assertEquals("Linear chain must not increment deadlock counter",
        deadlockedBefore, getDeadlockedCounter());

    // Neither txn should have been aborted; txn2 remains WAITING.
    txnMgr1.heartbeat();
    txnMgr2.heartbeat();
    LockState state = ((DbLockManager) txnMgr2.getLockManager()).checkLock(waitingLockId);
    Assert.assertEquals("Txn2 should still be WAITING (no deadlock)", LockState.WAITING, state);

    txnMgr1.rollbackTxn();
    txnMgr2.rollbackTxn();
    txnMgr1.closeTxnManager();
    txnMgr2.closeTxnManager();
    swapTxnManager(txnMgr);
  }

  /**
   * Victim selection is confined to the cycle: txn3 — the youngest txn and itself a hard
   * blocker (holds IW_T3) — waits on a cycle member but is not in the cycle, and must not
   * be chosen even though it is the youngest hard blocker overall. The cycle's own
   * youngest member (txn2) is the victim.
   */
  @Test
  public void testInnocentWaiterNotAborted() throws Exception {
    setupDeadlockTestTables("IW_T1", "IW_T2", "IW_T3");
    HiveTxnManager[] cycle = formTwoPartyCycle("IW_T1", "IW_T2");

    // txn3 holds IW_T3 (hard blocker), has the highest txn ID, and waits on IW_T1.
    HiveTxnManager txnMgr3 = openTxnWithStatement("Txn3", "update IW_T3 set a = 1 where b = 1");
    Assert.assertTrue("Txn3 should have the highest txn ID",
        txnMgr3.getCurrentTxnId() > cycle[1].getCurrentTxnId());
    runStatementNonBlocking(txnMgr3, "Txn3", "update IW_T1 set a = 3 where b = 3");

    runDeadlockDetector();

    // txn2 is the youngest cycle member -> aborted. txn1 and txn3 survive.
    Assert.assertTrue("Txn2 (younger of the two cycle members) should be aborted",
        heartbeatThrowsTxnAborted(cycle[1]));
    cycle[0].heartbeat();
    txnMgr3.heartbeat();

    cycle[0].rollbackTxn();
    txnMgr3.rollbackTxn();
    cycle[0].closeTxnManager();
    cycle[1].closeTxnManager();
    txnMgr3.closeTxnManager();
    swapTxnManager(txnMgr);
  }

  /**
   * The detector kill switch ({@code TXN_DEADLOCK_DETECTOR_ENABLED=false}) must
   * short-circuit {@code run()} and prevent any abort, even on a real cycle. This is the
   * production "stop the bleeding" lever; a regression that ignores the flag would let a
   * misbehaving detector keep killing user txns until binary rollback.
   */
  @Test
  public void testDetectorDisabledKillSwitch() throws Exception {
    boolean originalEnabled = MetastoreConf.getBoolVar(conf,
        MetastoreConf.ConfVars.TXN_DEADLOCK_DETECTOR_ENABLED);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.TXN_DEADLOCK_DETECTOR_ENABLED, false);
    HiveTxnManager[] cycle = null;
    try {
      setupDeadlockTestTables("KS_T1", "KS_T2");
      cycle = formTwoPartyCycle("KS_T1", "KS_T2");
      // Positive control: the cycle really formed (txn2 has a waiting lock on KS_T1).
      Assert.assertTrue("Cycle must have formed before testing the kill switch",
          findWaitingLockId(getLocks(cycle[1]), "KS_T1", cycle[1].getCurrentTxnId()) > 0);

      runDeadlockDetector();

      // Both txns must still be alive — kill switch must skip the scan entirely.
      cycle[0].heartbeat();
      cycle[1].heartbeat();
    } finally {
      MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.TXN_DEADLOCK_DETECTOR_ENABLED,
          originalEnabled);
      if (cycle != null) {
        for (HiveTxnManager m : cycle) {
          try { m.rollbackTxn(); } catch (Exception ignored) { }
          m.closeTxnManager();
        }
      }
      swapTxnManager(txnMgr);
    }
  }

  /**
   * Re-running the detector immediately after a successful abort must be a no-op: the
   * cycle is already broken and re-detection must not fire phantom aborts or double-count
   * the metric. Production runs the detector on a periodic timer, so this re-entry path
   * is exercised every interval.
   */
  @Test
  public void testIdempotentReRunReturnsZero() throws Exception {
    setupDeadlockTestTables("IR_T1", "IR_T2");
    HiveTxnManager[] cycle = formTwoPartyCycle("IR_T1", "IR_T2");

    runDeadlockDetector();
    Assert.assertTrue("First run must abort txn2", heartbeatThrowsTxnAborted(cycle[1]));

    long deadlockedAfterFirst = getDeadlockedCounter();
    runDeadlockDetector();
    // Second run: cycle already broken, txn1 still alive, metric must not move.
    cycle[0].heartbeat();
    Assert.assertEquals("Re-running the detector must not increment the deadlock counter",
        deadlockedAfterFirst, getDeadlockedCounter());

    cycle[0].rollbackTxn();
    cycle[0].closeTxnManager();
    cycle[1].closeTxnManager();
    swapTxnManager(txnMgr);
  }

  /**
   * Exact-delta assertion on {@code TOTAL_NUM_DEADLOCKED_TXNS}. A regression that
   * over-counts (e.g. incrementing inside a retry loop) or under-counts (e.g. forgetting
   * the metric on the new abort path) would slip past the existing tests, all of which
   * only assert "victim got aborted" and not "metric moved by exactly one."
   */
  @Test
  public void testDeadlockMetricIncrement() throws Exception {
    boolean originalAcidExt = MetastoreConf.getBoolVar(conf,
        MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_EXT_ON);
    boolean originalMetrics = MetastoreConf.getBoolVar(conf,
        MetastoreConf.ConfVars.METRICS_ENABLED);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_EXT_ON, true);
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED, true);
    // Force a fresh Metrics registry. Without METRICS_ENABLED + a re-init, every
    // getOrCreateCounter call returns the same shared `dummyCounter`, so increments to
    // any counter (e.g. AbortTxnsFunction's TOTAL_NUM_ABORTED_TXNS) leak into our reading
    // of TOTAL_NUM_DEADLOCKED_TXNS — making the delta reflect *all* metric work, not just
    // ours.
    Metrics.shutdown();
    Metrics.initialize(conf);
    HiveTxnManager[] cycle = null;
    try {
      setupDeadlockTestTables("DM_T1", "DM_T2");
      cycle = formTwoPartyCycle("DM_T1", "DM_T2");

      long before = getDeadlockedCounter();
      runDeadlockDetector();
      long after = getDeadlockedCounter();
      Assert.assertEquals("Detector must increment the deadlock counter exactly once",
          before + 1, after);
    } finally {
      MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METASTORE_ACIDMETRICS_EXT_ON,
          originalAcidExt);
      MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.METRICS_ENABLED, originalMetrics);
      Metrics.shutdown();
      if (originalMetrics) {
        Metrics.initialize(conf);
      }
      if (cycle != null) {
        for (HiveTxnManager m : cycle) {
          try { m.rollbackTxn(); } catch (Exception ignored) { }
          m.closeTxnManager();
        }
      }
      swapTxnManager(txnMgr);
    }
  }

  /**
   * Two disjoint 2-party cycles in the same scan, on T1/T2 and T3/T4. The detector must
   * abort one victim per cycle (the youngest in each), increment the metric by 2, and
   * leave the older member of each pair alive. This exercises savepoint iteration across
   * multiple SCCs in a single pass.
   */
  @Test
  public void testTwoIndependentCycles() throws Exception {
    setupDeadlockTestTables("TIC_T1", "TIC_T2", "TIC_T3", "TIC_T4");
    // Pairwise formation keeps txn ids ordered; the victims are the second txn of each pair.
    HiveTxnManager[] cycle1 = formTwoPartyCycle("TIC_T1", "TIC_T2");
    HiveTxnManager[] cycle2 = formTwoPartyCycle("TIC_T3", "TIC_T4");

    long before = getDeadlockedCounter();
    runDeadlockDetector();
    long after = getDeadlockedCounter();

    Assert.assertEquals("Two independent cycles must increment metric by 2", before + 2, after);
    Assert.assertTrue("Youngest in cycle 1 must be aborted", heartbeatThrowsTxnAborted(cycle1[1]));
    Assert.assertTrue("Youngest in cycle 2 must be aborted", heartbeatThrowsTxnAborted(cycle2[1]));
    cycle1[0].heartbeat();
    cycle2[0].heartbeat();

    cycle1[0].rollbackTxn();
    cycle2[0].rollbackTxn();
    for (HiveTxnManager m : new HiveTxnManager[]{cycle1[0], cycle1[1], cycle2[0], cycle2[1]}) {
      m.closeTxnManager();
    }
    swapTxnManager(txnMgr);
  }

  /**
   * A single-statement txn is never the victim, even when it is the load-bearing link of the cycle.
   * txn1 holds SHARED_READ on X and waits for Y; txn2 holds Y and then waits for SHARED_READ on X —
   * compatible with txn1's read, so its only blocker is txn3's queued EXCLUSIVE on X; txn3 (a single
   * statement) waits for EXCLUSIVE on X behind txn1's read. Cycle txn1->txn2->txn3->txn1. Only txn1
   * and txn2 are multi-statement (>1 lock request), so the youngest of THEM (txn2) is the victim —
   * never the single-statement txn3. Aborting txn2 still resolves the deadlock (txn2 releases Y ->
   * txn1 proceeds -> releases X -> txn3 acquires X). Pins that single-statement txns are protected.
   */
  @Test
  public void testSingleStatementSparedWhenLoadBearing() throws Exception {
    setupDeadlockTestTables("LB_X", "LB_Y");

    // txn1 holds SHARED_READ on X (a read); txn2 holds EXCL_WRITE on Y (a write).
    HiveTxnManager txnMgr1 = openTxnWithStatement("Txn1", "select a from LB_X");
    HiveTxnManager txnMgr2 = openTxnWithStatement("Txn2", "update LB_Y set a = 1 where b = 1");

    // txn1 then waits for Y (held by txn2).
    runStatementNonBlocking(txnMgr1, "Txn1", "update LB_Y set a = 2 where b = 2");

    // txn3 (single statement) waits for EXCLUSIVE on X, blocked by txn1's SHARED_READ.
    // Insert overwrite takes a true EXCLUSIVE (hive.txn.xlock.iow): it both waits behind
    // txn1's read and blocks txn2's later read — an UPDATE's EXCL_WRITE would do neither.
    HiveTxnManager txnMgr3 = openTxn("Txn3");
    runStatementNonBlocking(txnMgr3, "Txn3", "insert overwrite table LB_X select 3, 3");

    // txn2 then waits for SHARED_READ on X: compatible with txn1's read, so its only blocker is
    // txn3's queued EXCLUSIVE — deterministically wiring txn2 -> txn3.
    runStatementNonBlocking(txnMgr2, "Txn2", "select a from LB_X");

    long before = getDeadlockedCounter();
    runDeadlockDetector();
    long after = getDeadlockedCounter();

    Assert.assertEquals("One cycle must increment the deadlock counter by one", before + 1, after);
    Assert.assertTrue("Txn2 (youngest multi-statement member) must be the victim",
        heartbeatThrowsTxnAborted(txnMgr2));
    txnMgr1.heartbeat();
    txnMgr3.heartbeat(); // single-statement txn3 must be spared

    txnMgr1.rollbackTxn();
    txnMgr3.rollbackTxn();
    txnMgr1.closeTxnManager();
    txnMgr2.closeTxnManager();
    txnMgr3.closeTxnManager();
    swapTxnManager(txnMgr);
  }

  /**
   * A single-statement txn pulled into a 2-party deadlock as a queue bystander is never the victim,
   * and the victim is deterministic despite the arbitrary HL_BLOCKEDBY pointer. txn1/txn2 form the
   * real cycle on T1/T2 (each holds one, waits the other); txn3 (a single-statement writer, e.g. a
   * compaction) also waits for T1 behind txn1. Whether txn2's recorded blocker is txn1 (SCC
   * {txn1,txn2}) or txn3 (SCC {txn1,txn2,txn3}) is arbitrary, but txn3 is single-statement and can
   * never be chosen — the victim is always the youngest multi-statement member, txn2.
   */
  @Test
  public void testSingleStatementBystanderNeverVictim() throws Exception {
    setupDeadlockTestTables("BY_T1", "BY_T2");

    // txn1 holds T1, txn2 holds T2 — the multi-statement holders that form the real cycle.
    HiveTxnManager txnMgr1 = openTxnWithStatement("Txn1", "update BY_T1 set a = 1 where b = 1");
    HiveTxnManager txnMgr2 = openTxnWithStatement("Txn2", "update BY_T2 set a = 1 where b = 1");

    // txn3 (single statement, compaction-like) queues on T1 behind txn1 — holds nothing.
    HiveTxnManager txnMgr3 = openTxn("Txn3");
    runStatementNonBlocking(txnMgr3, "Txn3", "update BY_T1 set a = 3 where b = 3");

    // Close the cycle: txn1 waits T2, txn2 waits T1.
    runStatementNonBlocking(txnMgr1, "Txn1", "update BY_T2 set a = 2 where b = 2");
    runStatementNonBlocking(txnMgr2, "Txn2", "update BY_T1 set a = 2 where b = 2");

    long before = getDeadlockedCounter();
    runDeadlockDetector();
    long after = getDeadlockedCounter();

    Assert.assertEquals("Exactly one victim for the single cycle", before + 1, after);
    Assert.assertTrue("Txn2 (youngest multi-statement member) must be the victim",
        heartbeatThrowsTxnAborted(txnMgr2));
    txnMgr1.heartbeat();
    txnMgr3.heartbeat(); // single-statement bystander must be spared

    txnMgr1.rollbackTxn();
    txnMgr3.rollbackTxn();
    txnMgr1.closeTxnManager();
    txnMgr2.closeTxnManager();
    txnMgr3.closeTxnManager();
    swapTxnManager(txnMgr);
  }

  /**
   * A 2-party cycle closed by a single-statement txn's atomic multi-resource request: txn1
   * acquires EXCL_WRITE on A; txn2 (single statement) atomically requests EXCLUSIVE on A +
   * SHARED_READ on C (insert overwrite from C) — A conflicts with txn1's write, so both
   * components park WAITING; txn1 then requests EXCLUSIVE on C and queues behind txn2's C
   * component (FIFO). Cycle txn1->txn2->txn1. txn2 is the youngest member but holds nothing —
   * plain max(scc) would kill it for no gain (its atomic request simply re-queues on retry).
   * Only txn1 holds-and-waits, so txn1 is the victim even though it is older; aborting it
   * releases A and txn2's whole request acquires.
   */
  @Test
  public void testSingleStatementAtomicRequestNeverVictim() throws Exception {
    setupDeadlockTestTables("AR_A", "AR_C");

    // txn1 holds EXCL_WRITE on A.
    HiveTxnManager txnMgr1 = openTxnWithStatement("Txn1", "update AR_A set a = 1 where b = 1");

    // txn2 (single statement) atomically requests EXCLUSIVE on A + SHARED_READ on C; the A
    // component is blocked by txn1's write, so the C component parks WAITING with it.
    HiveTxnManager txnMgr2 = openTxn("Txn2");
    runStatementNonBlocking(txnMgr2, "Txn2", "insert overwrite table AR_A select a, b from AR_C");

    // txn1 then requests EXCLUSIVE on C and queues behind txn2's waiting SHARED_READ (FIFO).
    runStatementNonBlocking(txnMgr1, "Txn1", "insert overwrite table AR_C select 2, 2");

    long before = getDeadlockedCounter();
    runDeadlockDetector();
    long after = getDeadlockedCounter();

    Assert.assertEquals("One cycle must increment the deadlock counter by one", before + 1, after);
    Assert.assertTrue("Txn1 (the only hold-and-wait member) must be the victim despite being oldest",
        heartbeatThrowsTxnAborted(txnMgr1));
    txnMgr2.heartbeat(); // single-statement txn2 must be spared

    txnMgr2.rollbackTxn();
    txnMgr1.closeTxnManager();
    txnMgr2.closeTxnManager();
    swapTxnManager(txnMgr);
  }

  private void setupDeadlockTestTables(String... tables) throws Exception {
    dropTable(tables);
    conf.setBoolVar(HiveConf.ConfVars.TXN_WRITE_X_LOCK, true);
    for (String t : tables) {
      driver.run("create table " + t + " (a int, b int) clustered by(b) into 2 buckets "
          + "stored as orc TBLPROPERTIES ('transactional'='true')");
    }
  }

  private HiveTxnManager openTxn(String name) throws Exception {
    HiveTxnManager mgr = TxnManagerFactory.getTxnManagerFactory().getTxnManager(conf);
    swapTxnManager(mgr);
    mgr.openTxn(ctx, name);
    return mgr;
  }

  /** Opens a txn and acquires its first statement's locks, blocking until granted. */
  private HiveTxnManager openTxnWithStatement(String name, String sql) throws Exception {
    HiveTxnManager mgr = openTxn(name);
    driver.compileAndRespond(sql, true);
    mgr.acquireLocks(driver.getPlan(), ctx, name);
    return mgr;
  }

  /** Runs a further statement on an open txn; its locks may park WAITING. */
  private void runStatementNonBlocking(HiveTxnManager mgr, String name, String sql)
      throws Exception {
    swapTxnManager(mgr);
    driver.compileAndRespond(sql, true);
    ((DbTxnManager) mgr).acquireLocks(driver.getPlan(), ctx, name, false);
  }

  /** Returns {txn1, txn2} where txn1 holds t1 and waits for t2, txn2 holds t2 and waits for t1. */
  private HiveTxnManager[] formTwoPartyCycle(String t1, String t2) throws Exception {
    HiveTxnManager txn1 = openTxnWithStatement("Txn1", "update " + t1 + " set a = 1 where b = 1");
    HiveTxnManager txn2 = openTxnWithStatement("Txn2", "update " + t2 + " set a = 1 where b = 1");
    runStatementNonBlocking(txn1, "Txn1", "update " + t2 + " set a = 2 where b = 2");
    runStatementNonBlocking(txn2, "Txn2", "update " + t1 + " set a = 2 where b = 2");
    return new HiveTxnManager[]{txn1, txn2};
  }

  /**
   * Synchronously runs the deadlock detector. In production this runs on a periodic timer
   * inside the metastore leader; tests bypass the schedule and the cluster mutex (via
   * {@code enforceMutex(false)}) to deterministically observe the effect of one scan.
   */
  private void runDeadlockDetector() throws Exception {
    DeadlockDetectorService detector = new DeadlockDetectorService();
    detector.setConf(conf);
    detector.enforceMutex(false);
    detector.run();
  }

  /**
   * Returns true iff a heartbeat() on the given txn manager fails because the txn was
   * aborted. Preferred over commitTxn() because the assertion is non-destructive — the
   * txn stays in its current state if it's still open.
   */
  private static boolean heartbeatThrowsTxnAborted(HiveTxnManager txnMgr) {
    try {
      txnMgr.heartbeat();
      return false;
    } catch (LockException e) {
      return e.getCause() instanceof org.apache.hadoop.hive.metastore.api.TxnAbortedException;
    }
  }

  /**
   * Reads the current value of the deadlock-aborted-txn metric. The counter is
   * process-global; tests must use snapshot-and-delta, not absolute values.
   */
  private static long getDeadlockedCounter() {
    return Metrics.getOrCreateCounter(MetricsConstants.TOTAL_NUM_DEADLOCKED_TXNS).getCount();
  }

  /**
   * Locates the {@code WAITING} lock on the given table for the given txn ID. Returns
   * {@code -1} if none found.
   */
  private static long findWaitingLockId(List<ShowLocksResponseElement> locks, String table,
                                        long txnId) {
    for (ShowLocksResponseElement lock : locks) {
      if (lock.getState() == LockState.WAITING && table.equalsIgnoreCase(lock.getTablename())
          && lock.getTxnid() == txnId) {
        return lock.getLockid();
      }
    }
    return -1;
  }

  /**
   * Simulates concurrent transactions from a single thread by swapping the session's
   * TxnManager instance.
   */
  private static void swapTxnManager(HiveTxnManager txnMgr) {
    SessionState.get().setTxnMgr(txnMgr);
  }

  private List<ShowLocksResponseElement> getLocks(HiveTxnManager txnMgr) throws Exception {
    ShowLocksResponse rsp = ((DbLockManager) txnMgr.getLockManager()).getLocks();
    return rsp.getLocks();
  }

  private void dropTable(String[] tabs) throws Exception {
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    for (String tab : tabs) {
      driver.run("drop table if exists " + tab);
    }
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, true);
  }
}
