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

package org.apache.hadoop.hive.ql.exec.tez;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.SettableFuture;

import java.lang.Thread.State;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.api.WMFullResourcePlan;
import org.apache.hadoop.hive.metastore.api.WMMapping;
import org.apache.hadoop.hive.metastore.api.WMPool;
import org.apache.hadoop.hive.metastore.api.WMResourcePlan;
import org.apache.hadoop.hive.ql.exec.tez.UserPoolMapping.MappingInput;
import org.apache.hadoop.hive.ql.wm.SessionTriggerProvider;
import org.apache.hadoop.hive.ql.wm.WmContext;
import org.apache.hive.common.util.RetryTestRunner;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(RetryTestRunner.class)
public class TestWorkloadManager {
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(TestWorkloadManager.class);

  private final class GetSessionRunnable implements Runnable {
    private final AtomicReference<WmTezSession> session;
    private final WorkloadManager wm;
    private final AtomicReference<Throwable> error;
    private final HiveConf conf;
    private final CountDownLatch cdl;
    private final String userName;

    private GetSessionRunnable(AtomicReference<WmTezSession> session, WorkloadManager wm,
        AtomicReference<Throwable> error, HiveConf conf, CountDownLatch cdl, String userName) {
      this.session = session;
      this.wm = wm;
      this.error = error;
      this.conf = conf;
      this.cdl = cdl;
      this.userName = userName;
    }

    @Override
    public void run() {
      WmTezSession old = session.get();
      session.set(null);
      if (cdl != null) {
        cdl.countDown();
      }
      LOG.info("About to call get with " + old);
      try {
       session.set((WmTezSession) wm.getSession(old, mappingInput(userName), conf));
       LOG.info("Received " + session.get());
      } catch (Throwable e) {
        error.compareAndSet(null, e);
      }
    }
  }

  public static class MockQam implements QueryAllocationManager {
    boolean isCalled = false;

    @Override
    public void start() {
    }

    @Override
    public void stop() {
    }

    @Override
    public int updateSessionsAsync(Double totalMaxAlloc, List<WmTezSession> sessions) {
      isCalled = true;
      return 0;
    }

    @Override
    public void updateSessionAsync(WmTezSession session) {
    }

    void assertWasCalledAndReset() {
      assertTrue(isCalled);
      isCalled = false;
    }

    @Override
    public void setClusterChangedCallback(Runnable clusterChangedCallback) {
    }

    @Override
    public int translateAllocationToCpus(double allocation) {
      return 0;
    }
  }

  public static WMResourcePlan plan() {
    return new WMResourcePlan("rp");
  }

  public static WMPool pool(String path) {
    return pool(path, 4, 0.1f);
  }

  public static WMPool pool(String path, int qp, double alloc) {
    return pool(path, qp, alloc, "fair");
  }

  public static WMPool pool(String path, int qp, double alloc, String policy) {
    WMPool pool = new WMPool("rp", path);
    pool.setAllocFraction(alloc);
    pool.setQueryParallelism(qp);
    pool.setSchedulingPolicy(policy);
    return pool;
  }

  public static WMMapping mapping(String user, String pool) {
    return mapping("USER", user, pool, 0);
  }

  public static WMMapping mapping(String type, String user, String pool, int ordering) {
    WMMapping mapping = new WMMapping("rp", type, user);
    mapping.setPoolPath(pool);
    mapping.setOrdering(ordering);
    return mapping;
  }

  public static MappingInput mappingInput(String userName) {
    return new MappingInput(userName, null, null, null);
  }

  public static MappingInput mappingInput(String userName, List<String> groups) {
    return new MappingInput(userName, groups, null, null);
  }

  public static MappingInput mappingInput(String userName, List<String> groups, String wmPool) {
    return new MappingInput(userName, groups, wmPool, null);
  }

  private List<String> groups(String... groups) {
    return Lists.newArrayList(groups);
  }

  public static class WorkloadManagerForTest extends WorkloadManager {

    private SettableFuture<Boolean> failedWait;

    public WorkloadManagerForTest(String yarnQueue, HiveConf conf, int numSessions,
        QueryAllocationManager qam) throws ExecutionException, InterruptedException {
      super(null, yarnQueue, conf, qam, createDummyPlan(numSessions));
    }

    public WorkloadManagerForTest(String yarnQueue, HiveConf conf,
        QueryAllocationManager qam, WMFullResourcePlan plan) throws ExecutionException, InterruptedException {
      super(null, yarnQueue, conf, qam, plan);
    }

    @Override
    public void notifyOfClusterStateChange() {
      super.notifyOfClusterStateChange();
      try {
        ensureWm();
      } catch (InterruptedException | ExecutionException e) {
        throw new RuntimeException(e);
      }
    }

    private static WMFullResourcePlan createDummyPlan(int numSessions) {
      WMFullResourcePlan plan = new WMFullResourcePlan(new WMResourcePlan("rp"),
          Lists.newArrayList(pool("llap", numSessions, 1.0f)));
      plan.getPlan().setDefaultPoolPath("llap");
      return plan;
    }

    @Override
    protected WmTezSession createSessionObject(String sessionId, HiveConf conf) {
      conf = conf == null ? new HiveConf(getConf()) : conf;
      SampleTezSessionState sess = new SampleTezSessionState(sessionId, this, conf);
      if (failedWait != null) {
        sess.setWaitForAmRegistryFuture(failedWait);
        failedWait = null;
      }
      return sess;
    }

    @Override
    public WmTezSession getSession(
      TezSessionState session, MappingInput input, HiveConf conf,
      final WmContext wmContext) throws Exception {
      // We want to wait for the iteration to finish and set the cluster fraction.
      WmTezSession state = super.getSession(session, input, conf, null);
      ensureWm();
      return state;
    }

    @Override
    public void destroy(TezSessionState session) throws Exception {
      super.destroy(session);
      ensureWm();
    }

    private void ensureWm() throws InterruptedException, ExecutionException {
      addTestEvent().get(); // Wait for the events to be processed.
    }

    @Override
    public void returnAfterUse(TezSessionPoolSession session) throws Exception {
      super.returnAfterUse(session);
      ensureWm();
    }

    @Override
    public TezSessionState reopen(TezSessionState session) throws Exception {
      session = super.reopen(session);
      ensureWm();
      return session;
    }

    public void setNextWaitForAmRegistryFuture(SettableFuture<Boolean> failedWait) {
      this.failedWait = failedWait;
    }
  }

  @Test(timeout = 10000)
  public void testReuse() throws Exception {
    HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WorkloadManager wm = new WorkloadManagerForTest("test", conf, 1, qam);
    wm.start();
    TezSessionState nonPool = mock(TezSessionState.class);
    when(nonPool.getConf()).thenReturn(conf);
    doNothing().when(nonPool).close(anyBoolean());
    TezSessionState session = wm.getSession(nonPool, mappingInput("user"), conf);
    verify(nonPool).close(anyBoolean());
    assertNotSame(nonPool, session);
    session.returnToSessionManager();
    TezSessionPoolSession diffPool = mock(TezSessionPoolSession.class);
    when(diffPool.getConf()).thenReturn(conf);
    doNothing().when(diffPool).returnToSessionManager();
    session = wm.getSession(diffPool, mappingInput("user"), conf);
    verify(diffPool).returnToSessionManager();
    assertNotSame(diffPool, session);
    TezSessionState session2 = wm.getSession(session, mappingInput("user"), conf);
    assertSame(session, session2);
  }

  @Test(timeout = 10000)
  public void testQueueName() throws Exception {
    HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WorkloadManager wm = new WorkloadManagerForTest("test", conf, 1, qam);
    wm.start();
    // The queue should be ignored.
    conf.set(TezConfiguration.TEZ_QUEUE_NAME, "test2");
    TezSessionState session = wm.getSession(null, mappingInput("user"), conf);
    assertEquals("test", session.getQueueName());
    assertEquals("test", conf.get(TezConfiguration.TEZ_QUEUE_NAME));
    session.setQueueName("test2");
    session = wm.getSession(session, mappingInput("user"), conf);
    assertEquals("test", session.getQueueName());
  }

  private final static double EPSILON = 0.001;

  @Test(timeout = 10000)
  public void testReopen() throws Exception {
    // We should always get a different object, and cluster fraction should be propagated.
    HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WorkloadManager wm = new WorkloadManagerForTest("test", conf, 1, qam);
    wm.start();
    WmTezSession session = (WmTezSession) wm.getSession(
        null, mappingInput("user"), conf);
    assertEquals(1.0, session.getClusterFraction(), EPSILON);
    qam.assertWasCalledAndReset();
    WmTezSession session2 = (WmTezSession) session.reopen();
    assertNotSame(session, session2);
    wm.addTestEvent().get();
    assertEquals(session2.toString(), 1.0, session2.getClusterFraction(), EPSILON);
    assertFalse(session.hasClusterFraction());
    qam.assertWasCalledAndReset();
  }

  @Test(timeout = 10000)
  public void testDestroyAndReturn() throws Exception {
    // Session should not be lost; however the fraction should be discarded.
    HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WorkloadManager wm = new WorkloadManagerForTest("test", conf, 2, qam);
    wm.start();
    WmTezSession session = (WmTezSession) wm.getSession(null, mappingInput("user"), conf);
    assertEquals(1.0, session.getClusterFraction(), EPSILON);
    qam.assertWasCalledAndReset();
    WmTezSession session2 = (WmTezSession) wm.getSession(null, mappingInput("user"), conf);
    assertEquals(0.5, session.getClusterFraction(), EPSILON);
    assertEquals(0.5, session2.getClusterFraction(), EPSILON);
    qam.assertWasCalledAndReset();
    assertNotSame(session, session2);
    session.destroy(); // Destroy before returning to the pool.
    assertEquals(1.0, session2.getClusterFraction(), EPSILON);
    assertFalse(session.hasClusterFraction());
    qam.assertWasCalledAndReset();

    // We never lose pool session, so we should still be able to get.
    session = (WmTezSession) wm.getSession(null, mappingInput("user"), conf);
    session.returnToSessionManager();
    assertEquals(1.0, session2.getClusterFraction(), EPSILON);
    assertFalse(session.hasClusterFraction());
    qam.assertWasCalledAndReset();
  }

  @Test(timeout = 10000)
  public void testClusterFractions() throws Exception {
    HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WMFullResourcePlan plan = new WMFullResourcePlan(plan(),
        Lists.newArrayList(pool("r1", 1, 0.6f), pool("r2", 1, 0.4f),
            pool("r1.p1", 1, 0.5f), pool("r1.p2", 2, 0.3f)));
    plan.setMappings(Lists.newArrayList(mapping("p1", "r1.p1"),
        mapping("p2", "r1.p2"), mapping("r1", "r1"), mapping("r2", "r2")));
    WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();
    assertEquals(5, wm.getNumSessions());
    // Get all the 5 sessions; validate cluster fractions.
    WmTezSession session05of06 = (WmTezSession) wm.getSession(
        null, mappingInput("p1"), conf);
    assertEquals(0.3, session05of06.getClusterFraction(), EPSILON);
    WmTezSession session03of06 = (WmTezSession) wm.getSession(
        null, mappingInput("p2"), conf);
    assertEquals(0.18, session03of06.getClusterFraction(), EPSILON);
    WmTezSession session03of06_2 = (WmTezSession) wm.getSession(
        null, mappingInput("p2"), conf);
    assertEquals(0.09, session03of06.getClusterFraction(), EPSILON);
    assertEquals(0.09, session03of06_2.getClusterFraction(), EPSILON);
    WmTezSession session02of06 = (WmTezSession) wm.getSession(
        null,mappingInput("r1"), conf);
    assertEquals(0.12, session02of06.getClusterFraction(), EPSILON);
    WmTezSession session04 = (WmTezSession) wm.getSession(
        null, mappingInput("r2"), conf);
    assertEquals(0.4, session04.getClusterFraction(), EPSILON);
    session05of06.returnToSessionManager();
    session03of06.returnToSessionManager();
    session03of06_2.returnToSessionManager();
    session02of06.returnToSessionManager();
    session04.returnToSessionManager();
  }

  @Test(timeout = 10000)
  public void testMappings() throws Exception {
    HiveConf conf = createConf();
    conf.set(ConfVars.HIVE_SERVER2_WM_ALLOW_ANY_POOL_VIA_JDBC.varname, "false");
    MockQam qam = new MockQam();
    WMFullResourcePlan plan = new WMFullResourcePlan(plan(),
        Lists.newArrayList(pool("u0"), pool("g0"), pool("g1"), pool("u2"), pool("a0")));
    plan.setMappings(Lists.newArrayList(mapping("USER", "u0", "u0", 0),
        mapping("APPLICATION", "a0", "a0", 0), mapping("GROUP", "g0", "g0", 0),
        mapping("GROUP", "g1", "g1", 1), mapping("USER", "u2", "u2", 2)));
    WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();
    // Test various combinations.
    verifyMapping(wm, conf, mappingInput("u0", groups("zzz")), "u0");
    verifyMapping(wm, conf, new MappingInput("u0", null, null, "a0"), "u0");
    verifyMapping(wm, conf, new MappingInput("zzz", groups("g0"), null, "a0"), "a0");
    verifyMapping(wm, conf, mappingInput("zzz", groups("g1")), "g1");
    verifyMapping(wm, conf, mappingInput("u0", groups("g1")), "u0");
    // User takes precendence over groups unless ordered explicitly.
    verifyMapping(wm, conf, mappingInput("u0", groups("g0")), "u0");
    verifyMapping(wm, conf, mappingInput("u2", groups("g1")), "g1");
    verifyMapping(wm, conf, mappingInput("u2", groups("g0", "g1")), "g0");
    // Check explicit pool specifications - valid cases where priority is changed.
    verifyMapping(wm, conf, mappingInput("u0", groups("g1"), "g1"), "g1");
    verifyMapping(wm, conf, mappingInput("u2", groups("g1"), "u2"), "u2");
    verifyMapping(wm, conf, mappingInput("zzz", groups("g0", "g1"), "g1"), "g1");
    // Explicit pool specification - invalid - there's no mapping that matches.
    try {
      TezSessionState r = wm.getSession(
        null, mappingInput("u0", groups("g0", "g1"), "u2"), conf);
      fail("Expected failure, but got " + r);
    } catch (Exception ex) {
      // Expected.
    }
    // Now allow the users to specify any pools.
    conf.set(ConfVars.HIVE_SERVER2_WM_ALLOW_ANY_POOL_VIA_JDBC.varname, "true");
    wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();
    verifyMapping(wm, conf, mappingInput("u0", groups("g0", "g1"), "u2"), "u2");
    // The mapping that doesn't exist still shouldn't work.
    try {
      TezSessionState r = wm.getSession(
        null, mappingInput("u0", groups("g0", "g1"), "zzz"), conf);
      fail("Expected failure, but got " + r);
    } catch (Exception ex) {
      // Expected.
    }
  }

  private static void verifyMapping(
      WorkloadManager wm, HiveConf conf, MappingInput mi, String result) throws Exception {
    WmTezSession session = (WmTezSession) wm.getSession(null, mi, conf, null);
    assertEquals(result, session.getPoolName());
    session.returnToSessionManager();
  }




  @Test(timeout=10000)
  public void testQueueing() throws Exception {
    final HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WMFullResourcePlan plan = new WMFullResourcePlan(plan(), Lists.newArrayList(
        pool("A", 2, 0.5f), pool("B", 2, 0.5f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A"), mapping("B", "B")));
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();
    WmTezSession sessionA1 = (WmTezSession) wm.getSession(null, mappingInput("A"), conf),
        sessionA2 = (WmTezSession) wm.getSession(null, mappingInput("A"), conf),
        sessionB1 = (WmTezSession) wm.getSession(null, mappingInput("B"), conf);
    final AtomicReference<WmTezSession> sessionA3 = new AtomicReference<>(),
        sessionA4 = new AtomicReference<>();
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final CountDownLatch cdl = new CountDownLatch(1);

    Thread t1 = new Thread(new GetSessionRunnable(sessionA3, wm, error, conf, cdl, "A")),
        t2 = new Thread(new GetSessionRunnable(sessionA4, wm, error, conf, null, "A"));
    waitForThreadToBlock(cdl, t1);
    t2.start();
    assertNull(sessionA3.get());
    assertNull(sessionA4.get());
    checkError(error);
    // While threads are blocked on A, we should still be able to get and return a B session.
    WmTezSession sessionB2 = (WmTezSession) wm.getSession(null, mappingInput("B"), conf);
    sessionB1.returnToSessionManager();
    sessionB2.returnToSessionManager();
    assertNull(sessionA3.get());
    assertNull(sessionA4.get());
    checkError(error);
    // Now release a single session from A.
    sessionA1.returnToSessionManager();
    joinThread(t1);
    checkError(error);
    assertNotNull(sessionA3.get());
    assertNull(sessionA4.get());
    sessionA3.get().returnToSessionManager();
    joinThread(t2);
    checkError(error);
    assertNotNull(sessionA4.get());
    sessionA4.get().returnToSessionManager();
    sessionA2.returnToSessionManager();
  }

  @Test(timeout=10000)
  public void testClusterChange() throws Exception {
    final HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WMFullResourcePlan plan = new WMFullResourcePlan(plan(), Lists.newArrayList(pool("A", 2, 1f)));
    plan.getPlan().setDefaultPoolPath("A");
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();
    WmTezSession session1 = (WmTezSession) wm.getSession(null, mappingInput("A"), conf),
        session2 = (WmTezSession) wm.getSession(null, mappingInput("A"), conf);
    assertEquals(0.5, session1.getClusterFraction(), EPSILON);
    assertEquals(0.5, session2.getClusterFraction(), EPSILON);
    qam.assertWasCalledAndReset();

    // If cluster info changes, qam should be called with the same fractions.
    wm.notifyOfClusterStateChange();
    assertEquals(0.5, session1.getClusterFraction(), EPSILON);
    assertEquals(0.5, session2.getClusterFraction(), EPSILON);
    qam.assertWasCalledAndReset();

    session1.returnToSessionManager();
    session2.returnToSessionManager();
  }

  @Test(timeout=10000)
  public void testReuseWithQueueing() throws Exception {
    final HiveConf conf = createConf();
    MockQam qam = new MockQam();
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, 2, qam);
    wm.start();
    WmTezSession session1 = (WmTezSession) wm.getSession(
        null, mappingInput("user"), conf);
    // First, try to reuse from the same pool - should "just work".
    WmTezSession session1a = (WmTezSession) wm.getSession(
        session1, mappingInput("user"), conf);
    assertSame(session1, session1a);
    assertEquals(1.0, session1.getClusterFraction(), EPSILON);
    // Should still be able to get the 2nd session.
    WmTezSession session2 = (WmTezSession) wm.getSession(
        null, mappingInput("user"), conf);

    // Now try to reuse with no other sessions remaining. Should still work.
    WmTezSession session2a = (WmTezSession) wm.getSession(
        session2, mappingInput("user"), conf);
    assertSame(session2, session2a);
    assertEquals(0.5, session1.getClusterFraction(), EPSILON);
    assertEquals(0.5, session2.getClusterFraction(), EPSILON);

    // Finally try to reuse with something in the queue. Due to fairness this won't work.
    final AtomicReference<WmTezSession> session3 = new AtomicReference<>(),
    // We will try to reuse this, but session3 is queued before us.
        session4 = new AtomicReference<>(session2);
    final AtomicReference<Throwable> error = new AtomicReference<>();
    CountDownLatch cdl = new CountDownLatch(1), cdl2 = new CountDownLatch(1);
    Thread t1 = new Thread(new GetSessionRunnable(session3, wm, error, conf, cdl, null), "t1"),
        t2 = new Thread(new GetSessionRunnable(session4, wm, error, conf, cdl2, null), "t2");
    waitForThreadToBlock(cdl, t1);
    assertNull(session3.get());
    checkError(error);
    t2.start();
    cdl2.await();
    assertNull(session4.get());

    // We have released the session by trying to reuse it and going back into queue, s3 can start.
    joinThread(t1);
    checkError(error);
    assertNotNull(session3.get());
    assertEquals(0.5, session3.get().getClusterFraction(), EPSILON);

    // Now release another session; the thread that gave up on reuse can proceed.
    session1.returnToSessionManager();
    joinThread(t2);
    checkError(error);
    assertNotNull(session4.get());
    assertNotSame(session2, session4.get());
    assertEquals(0.5, session4.get().getClusterFraction(), EPSILON);
    session3.get().returnToSessionManager();
    session4.get().returnToSessionManager();
  }

  private static void joinThread(Thread t) throws InterruptedException {
    LOG.debug("Joining " + t.getName());
    t.join();
    LOG.debug("Joined " + t.getName());
  }

  private void waitForThreadToBlock(CountDownLatch cdl, Thread t1) throws InterruptedException {
    t1.start();
    cdl.await();
    // Wait for t1 to block, just be sure. Not ideal...
    State s;
    do {
      s = t1.getState();
    } while (s != State.TIMED_WAITING && s != State.BLOCKED && s != State.WAITING);
  }


  @Test(timeout=10000)
  public void testReuseWithDifferentPool() throws Exception {
    final HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WMFullResourcePlan plan = new WMFullResourcePlan(plan(), Lists.newArrayList(
        pool("A", 2, 0.6f), pool("B", 1, 0.4f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A"), mapping("B", "B")));
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();
    WmTezSession sessionA1 = (WmTezSession) wm.getSession(null, mappingInput("A"), conf),
        sessionA2 = (WmTezSession) wm.getSession(null, mappingInput("A"), conf);
    assertEquals("A", sessionA1.getPoolName());
    assertEquals(0.3f, sessionA1.getClusterFraction(), EPSILON);
    assertEquals("A", sessionA2.getPoolName());
    assertEquals(0.3f, sessionA2.getClusterFraction(), EPSILON);
    WmTezSession sessionB1 = (WmTezSession) wm.getSession(sessionA1, mappingInput("B"), conf);
    assertSame(sessionA1, sessionB1);
    assertEquals("B", sessionB1.getPoolName());
    assertEquals(0.4f, sessionB1.getClusterFraction(), EPSILON);
    assertEquals(0.6f, sessionA2.getClusterFraction(), EPSILON); // A1 removed from A.
    // Make sure that we can still get a session from A.
    WmTezSession sessionA3 = (WmTezSession) wm.getSession(null, mappingInput("A"), conf);
    assertEquals("A", sessionA3.getPoolName());
    assertEquals(0.3f, sessionA3.getClusterFraction(), EPSILON);
    assertEquals(0.3f, sessionA3.getClusterFraction(), EPSILON);
    sessionA3.returnToSessionManager();
    sessionB1.returnToSessionManager();
    sessionA2.returnToSessionManager();
  }

  @Test(timeout=10000)
  public void testApplyPlanUserMapping() throws Exception {
    final HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WMFullResourcePlan plan = new WMFullResourcePlan(plan(), Lists.newArrayList(
        pool("A", 1, 0.5f), pool("B", 1, 0.5f)));
    plan.setMappings(Lists.newArrayList(mapping("U", "A")));
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();

    // One session will be running, the other will be queued in "A"
    WmTezSession sessionA1 = (WmTezSession) wm.getSession(null, mappingInput("U"), conf);
    assertEquals("A", sessionA1.getPoolName());
    assertEquals(0.5f, sessionA1.getClusterFraction(), EPSILON);
    final AtomicReference<WmTezSession> sessionA2 = new AtomicReference<>();
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final CountDownLatch cdl = new CountDownLatch(1);
    Thread t1 = new Thread(new GetSessionRunnable(sessionA2, wm, error, conf, cdl, "U"));
    waitForThreadToBlock(cdl, t1);
    assertNull(sessionA2.get());
    checkError(error);

    // Now change the resource plan - change the mapping for the user.
    plan = new WMFullResourcePlan(plan(), Lists.newArrayList(
        pool("A", 1, 0.6f), pool("B", 1, 0.4f)));
    plan.setMappings(Lists.newArrayList(mapping("U", "B")));
    wm.updateResourcePlanAsync(plan);

    // The session will go to B with the new mapping; check it.
    joinThread(t1);
    checkError(error);
    assertNotNull(sessionA2.get());
    assertEquals("B", sessionA2.get().getPoolName());
    assertEquals(0.4f, sessionA2.get().getClusterFraction(), EPSILON);
    // The new session will also go to B now.
    sessionA2.get().returnToSessionManager();
    WmTezSession sessionB1 = (WmTezSession) wm.getSession(null, mappingInput("U"), conf);
    assertEquals("B", sessionB1.getPoolName());
    assertEquals(0.4f, sessionB1.getClusterFraction(), EPSILON);
    sessionA1.returnToSessionManager();
    sessionB1.returnToSessionManager();
  }


  @Test(timeout=10000)
  public void testApplyPlanQpChanges() throws Exception {
    final HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WMFullResourcePlan plan = new WMFullResourcePlan(plan(), Lists.newArrayList(
        pool("A", 1, 0.35f), pool("B", 2, 0.15f),
        pool("C", 2, 0.3f), pool("D", 1, 0.3f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A"), mapping("B", "B"),
            mapping("C", "C"), mapping("D", "D")));
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();
    TezSessionPool<WmTezSession> tezAmPool = wm.getTezAmPool();
    assertEquals(6, tezAmPool.getCurrentSize());

    // A: 1/1 running, 1 queued; B: 2/2 running, C: 1/2 running, D: 1/1 running, 1 queued.
    // Total: 5/6 running.
    WmTezSession sessionA1 = (WmTezSession) wm.getSession(null, mappingInput("A"), conf),
        sessionB1 = (WmTezSession) wm.getSession(null, mappingInput("B"), conf),
        sessionB2 = (WmTezSession) wm.getSession(null, mappingInput("B"), conf),
        sessionC1 = (WmTezSession) wm.getSession(null, mappingInput("C"), conf),
        sessionD1 = (WmTezSession) wm.getSession(null, mappingInput("D"), conf);
    final AtomicReference<WmTezSession> sessionA2 = new AtomicReference<>(),
        sessionD2 = new AtomicReference<>();
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final CountDownLatch cdl1 = new CountDownLatch(1), cdl2 = new CountDownLatch(1);
    Thread t1 = new Thread(new GetSessionRunnable(sessionA2, wm, error, conf, cdl1, "A")),
        t2 = new Thread(new GetSessionRunnable(sessionD2, wm, error, conf, cdl2, "D"));
    waitForThreadToBlock(cdl1, t1);
    waitForThreadToBlock(cdl2, t2);
    checkError(error);
    assertEquals(0.3f, sessionC1.getClusterFraction(), EPSILON);
    assertEquals(0.3f, sessionD1.getClusterFraction(), EPSILON);
    assertEquals(1, tezAmPool.getCurrentSize());

    // Change the resource plan - resize B and C down, D up, and remove A remapping users to B.
    // Everything will be killed in A and B, C won't change, D will start one more query from
    // the queue, and the query queued in A will be re-queued in B and started.
    // The fractions will also all change.
    // Total: 4/4 running.
    plan = new WMFullResourcePlan(plan(), Lists.newArrayList(pool("B", 1, 0.3f),
        pool("C", 1, 0.2f), pool("D", 2, 0.5f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "B"), mapping("B", "B"),
            mapping("C", "C"), mapping("D", "D")));
    wm.updateResourcePlanAsync(plan);
    wm.addTestEvent().get();

    joinThread(t1);
    joinThread(t2);
    checkError(error);
    assertNotNull(sessionA2.get());
    assertNotNull(sessionD2.get());
    assertEquals("D", sessionD2.get().getPoolName());
    assertEquals("B", sessionA2.get().getPoolName());
    assertEquals("C", sessionC1.getPoolName());
    assertEquals(0.3f, sessionA2.get().getClusterFraction(), EPSILON);
    assertEquals(0.2f, sessionC1.getClusterFraction(), EPSILON);
    assertEquals(0.25f, sessionD1.getClusterFraction(), EPSILON);
    assertKilledByWm(sessionA1);
    assertKilledByWm(sessionB1);
    assertKilledByWm(sessionB2);
    assertEquals(0, tezAmPool.getCurrentSize());

    // Wait for another iteration to make sure event gets processed for D2 to receive allocation.
    sessionA2.get().returnToSessionManager();
    assertEquals(0.25f, sessionD2.get().getClusterFraction(), EPSILON);
    // Return itself should be a no-op - the pool went from 6 to 4 with 1 session in the pool.

    sessionD2.get().returnToSessionManager();
    sessionC1.returnToSessionManager();
    sessionD1.returnToSessionManager();

    // Try to "return" stuff that was killed from "under" us. Should be a no-op.
    sessionA1.returnToSessionManager();
    sessionB1.returnToSessionManager();
    sessionB2.returnToSessionManager();
    assertEquals(4, tezAmPool.getCurrentSize());
  }



  @Test(timeout=10000)
  public void testFifoSchedulingPolicy() throws Exception {
    final HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WMFullResourcePlan plan = new WMFullResourcePlan(
        plan(), Lists.newArrayList(pool("A", 3, 1f, "fair")));
    plan.getPlan().setDefaultPoolPath("A");
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();

    // 2 running.
    WmTezSession sessionA1 = (WmTezSession) wm.getSession(
        null, mappingInput("A", null), conf, null),
        sessionA2 = (WmTezSession) wm.getSession(null, mappingInput("A", null), conf, null);
    assertEquals(0.5f, sessionA1.getClusterFraction(), EPSILON);
    assertEquals(0.5f, sessionA2.getClusterFraction(), EPSILON);

    // Change the resource plan to use fifo policy.
    plan = new WMFullResourcePlan(plan(), Lists.newArrayList(pool("A", 3, 1f, "fifo")));
    plan.getPlan().setDefaultPoolPath("A");
    wm.updateResourcePlanAsync(plan).get();
    assertEquals(1f, sessionA1.getClusterFraction(), EPSILON);
    assertEquals(0f, sessionA2.getClusterFraction(), EPSILON);
    assertEquals("A", sessionA2.getPoolName());

    // Add another session.
    WmTezSession sessionA3 = (WmTezSession) wm.getSession(
        null, mappingInput("A", null), conf, null);
    assertEquals(0f, sessionA3.getClusterFraction(), EPSILON);
    assertEquals("A", sessionA3.getPoolName());

    // Make sure the allocation is transfered correctly on return.
    sessionA1.returnToSessionManager();
    assertEquals(1f, sessionA2.getClusterFraction(), EPSILON);
    assertEquals(0f, sessionA3.getClusterFraction(), EPSILON);
    assertEquals("A", sessionA3.getPoolName());

    // Make sure reuse changes the FIFO order of the session.
    WmTezSession sessionA4 =  (WmTezSession) wm.getSession(
        sessionA2, mappingInput("A", null), conf, null);
    assertSame(sessionA2, sessionA4);
    assertEquals(1f, sessionA3.getClusterFraction(), EPSILON);
    assertEquals(0f, sessionA2.getClusterFraction(), EPSILON);
    assertEquals("A", sessionA2.getPoolName());

    sessionA3.returnToSessionManager();
    assertEquals(1f, sessionA2.getClusterFraction(), EPSILON);
    sessionA2.returnToSessionManager();
  }

  @Test(timeout=10000)
  public void testDisableEnable() throws Exception {
    final HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WMFullResourcePlan plan = new WMFullResourcePlan(plan(), Lists.newArrayList(pool("A", 1, 1f)));
    plan.getPlan().setDefaultPoolPath("A");
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();
    TezSessionPool<WmTezSession> tezAmPool = wm.getTezAmPool();

    // 1 running, 1 queued.
    WmTezSession sessionA1 = (WmTezSession) wm.getSession(
        null, mappingInput("A", null), conf, null);
    final AtomicReference<WmTezSession> sessionA2 = new AtomicReference<>();
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final CountDownLatch cdl1 = new CountDownLatch(1);
    Thread t1 = new Thread(new GetSessionRunnable(sessionA2, wm, error, conf, cdl1, "A"));
    waitForThreadToBlock(cdl1, t1);
    checkError(error);

    // Remove the resource plan - disable WM. All the queries die.
    wm.updateResourcePlanAsync(null).get();

    joinThread(t1);
    assertNotNull(error.get());
    assertNull(sessionA2.get());
    assertKilledByWm(sessionA1);
    assertEquals(0, tezAmPool.getCurrentSize());
    sessionA1.returnToSessionManager(); // No-op for session killed by WM.
    assertEquals(0, tezAmPool.getCurrentSize());

    try {
      TezSessionState r =  wm.getSession(null, mappingInput("A", null), conf, null);
      fail("Expected an error but got " + r);
    } catch (WorkloadManager.NoPoolMappingException ex) {
      // Ignore, this particular error is expected.
    }

    // Apply the plan again - enable WM.
    wm.updateResourcePlanAsync(plan).get();
    sessionA1 = (WmTezSession) wm.getSession(null, mappingInput("A", null), conf, null);
    assertEquals("A", sessionA1.getPoolName());
    sessionA1.returnToSessionManager();
    assertEquals(1, tezAmPool.getCurrentSize());
  }


  @Test(timeout=10000)
  public void testAmPoolInteractions() throws Exception {
    final HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WMFullResourcePlan plan = new WMFullResourcePlan(plan(), Lists.newArrayList(
        pool("A", 1, 1.0f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A")));
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();
    // Take away the only session, as if it was expiring.
    TezSessionPool<WmTezSession> pool = wm.getTezAmPool();
    WmTezSession oob = pool.getSession();

    final AtomicReference<WmTezSession> sessionA1 = new AtomicReference<>();
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final CountDownLatch cdl1 = new CountDownLatch(1);
    Thread t1 = new Thread(new GetSessionRunnable(sessionA1, wm, error, conf, cdl1, "A"));
    waitForThreadToBlock(cdl1, t1);
    checkError(error);
    // Replacing it directly in the pool should unblock get.
    pool.replaceSession(oob);
    joinThread(t1);
    assertNotNull(sessionA1.get());
    assertEquals("A", sessionA1.get().getPoolName());

    // Increase qp, check that the pool grows.
    plan = new WMFullResourcePlan(plan(), Lists.newArrayList(
        pool("A", 4, 1.0f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A")));
    wm.updateResourcePlanAsync(plan);
    WmTezSession oob2 = pool.getSession(),
        oob3 = pool.getSession(),
        oob4 = pool.getSession();
    pool.returnSession(oob2);
    assertEquals(1, pool.getCurrentSize());

    // Decrease qp, check that the pool shrinks incl. killing the unused and returned sessions.
    plan = new WMFullResourcePlan(plan(), Lists.newArrayList(pool("A", 1, 1.0f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A")));
    wm.updateResourcePlanAsync(plan);
    wm.addTestEvent().get();
    assertEquals(0, pool.getCurrentSize());
     sessionA1.get().returnToSessionManager();
    pool.returnSession(oob3);
    assertEquals(0, pool.getCurrentSize());
    pool.returnSession(oob4);
    assertEquals(1, pool.getCurrentSize());

    // Decrease, then increase qp - sessions should not be killed on return.
    plan = new WMFullResourcePlan(plan(), Lists.newArrayList(pool("A", 2, 1.0f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A")));
    wm.updateResourcePlanAsync(plan);
    oob2 = pool.getSession();
    oob3 = pool.getSession();
    assertEquals(0, pool.getCurrentSize());
    plan = new WMFullResourcePlan(plan(), Lists.newArrayList(pool("A", 1, 1.0f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A")));
    wm.updateResourcePlanAsync(plan);
    plan = new WMFullResourcePlan(plan(), Lists.newArrayList(pool("A", 2, 1.0f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A")));
    wm.updateResourcePlanAsync(plan);
    wm.addTestEvent().get();
    assertEquals(0, pool.getCurrentSize());
    pool.returnSession(oob3);
    pool.returnSession(oob4);
    assertEquals(2, pool.getCurrentSize());
  }

  @Test(timeout=10000)
  public void testMoveSessions() throws Exception {
    final HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WMFullResourcePlan plan = new WMFullResourcePlan(plan(), Lists.newArrayList(
      pool("A", 1, 0.6f), pool("B", 2, 0.4f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A"), mapping("B", "B")));
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();

    WmTezSession sessionA1 = (WmTezSession) wm.getSession(null, mappingInput("A"), conf);

    // [A: 1, B: 0]
    Map<String, SessionTriggerProvider> allSessionProviders = wm.getAllSessionTriggerProviders();
    assertEquals(1, allSessionProviders.get("A").getSessions().size());
    assertEquals(0, allSessionProviders.get("B").getSessions().size());
    assertTrue(allSessionProviders.get("A").getSessions().contains(sessionA1));
    assertFalse(allSessionProviders.get("B").getSessions().contains(sessionA1));
    assertEquals(0.6f, sessionA1.getClusterFraction(), EPSILON);
    assertEquals("A", sessionA1.getPoolName());

    // [A: 0, B: 1]
    Future<Boolean> future = wm.applyMoveSessionAsync(sessionA1, "B");
    assertNotNull(future.get());
    assertTrue(future.get());
    wm.addTestEvent().get();
    allSessionProviders = wm.getAllSessionTriggerProviders();
    assertEquals(0, allSessionProviders.get("A").getSessions().size());
    assertEquals(1, allSessionProviders.get("B").getSessions().size());
    assertFalse(allSessionProviders.get("A").getSessions().contains(sessionA1));
    assertTrue(allSessionProviders.get("B").getSessions().contains(sessionA1));
    assertEquals(0.4f, sessionA1.getClusterFraction(), EPSILON);
    assertEquals("B", sessionA1.getPoolName());

    WmTezSession sessionA2 = (WmTezSession) wm.getSession(null, mappingInput("A"), conf);
    // [A: 1, B: 1]
    allSessionProviders = wm.getAllSessionTriggerProviders();
    assertEquals(1, allSessionProviders.get("A").getSessions().size());
    assertEquals(1, allSessionProviders.get("B").getSessions().size());
    assertTrue(allSessionProviders.get("A").getSessions().contains(sessionA2));
    assertTrue(allSessionProviders.get("B").getSessions().contains(sessionA1));
    assertEquals(0.6f, sessionA2.getClusterFraction(), EPSILON);
    assertEquals(0.4f, sessionA1.getClusterFraction(), EPSILON);
    assertEquals("A", sessionA2.getPoolName());
    assertEquals("B", sessionA1.getPoolName());

    // [A: 0, B: 2]
    future = wm.applyMoveSessionAsync(sessionA2, "B");
    assertNotNull(future.get());
    assertTrue(future.get());
    wm.addTestEvent().get();
    allSessionProviders = wm.getAllSessionTriggerProviders();
    assertEquals(0, allSessionProviders.get("A").getSessions().size());
    assertEquals(2, allSessionProviders.get("B").getSessions().size());
    assertTrue(allSessionProviders.get("B").getSessions().contains(sessionA2));
    assertTrue(allSessionProviders.get("B").getSessions().contains(sessionA1));
    assertEquals(0.2f, sessionA2.getClusterFraction(), EPSILON);
    assertEquals(0.2f, sessionA1.getClusterFraction(), EPSILON);
    assertEquals("B", sessionA2.getPoolName());
    assertEquals("B", sessionA1.getPoolName());

    WmTezSession sessionA3 = (WmTezSession) wm.getSession(null, mappingInput("A"), conf);
    // [A: 1, B: 2]
    allSessionProviders = wm.getAllSessionTriggerProviders();
    assertEquals(1, allSessionProviders.get("A").getSessions().size());
    assertEquals(2, allSessionProviders.get("B").getSessions().size());
    assertTrue(allSessionProviders.get("A").getSessions().contains(sessionA3));
    assertTrue(allSessionProviders.get("B").getSessions().contains(sessionA2));
    assertTrue(allSessionProviders.get("B").getSessions().contains(sessionA1));
    assertEquals(0.6f, sessionA3.getClusterFraction(), EPSILON);
    assertEquals(0.2f, sessionA2.getClusterFraction(), EPSILON);
    assertEquals(0.2f, sessionA1.getClusterFraction(), EPSILON);
    assertEquals("A", sessionA3.getPoolName());
    assertEquals("B", sessionA2.getPoolName());
    assertEquals("B", sessionA1.getPoolName());

    // B is maxed out on capacity, so this move should fail the session
    future = wm.applyMoveSessionAsync(sessionA3, "B");
    assertNotNull(future.get());
    assertFalse(future.get());
    wm.addTestEvent().get();
    while(sessionA3.isOpen()) {
      Thread.sleep(100);
    }
    assertNull(sessionA3.getPoolName());
    assertEquals("Destination pool B is full. Killing query.", sessionA3.getReasonForKill());
    assertEquals(0, allSessionProviders.get("A").getSessions().size());
    assertEquals(2, allSessionProviders.get("B").getSessions().size());
  }

  @Test(timeout=10000)
  public void testMoveSessionsMultiPool() throws Exception {
    final HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WMFullResourcePlan plan = new WMFullResourcePlan(plan(), Lists.newArrayList(
      pool("A", 1, 0.4f), pool("B", 1, 0.4f), pool("B.x", 1, 0.2f),
      pool("B.y", 1, 0.8f), pool("C", 1, 0.2f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A"), mapping("B", "B"), mapping("C", "C")));
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();

    WmTezSession sessionA1 = (WmTezSession) wm.getSession(null, mappingInput("A"), conf);

    // [A: 1, B: 0, B.x: 0, B.y: 0, C: 0]
    Map<String, SessionTriggerProvider> allSessionProviders = wm.getAllSessionTriggerProviders();
    assertEquals(1, allSessionProviders.get("A").getSessions().size());
    assertEquals(0, allSessionProviders.get("B").getSessions().size());
    assertEquals(0, allSessionProviders.get("B.x").getSessions().size());
    assertEquals(0, allSessionProviders.get("B.y").getSessions().size());
    assertEquals(0, allSessionProviders.get("C").getSessions().size());
    assertEquals(0.4f, sessionA1.getClusterFraction(), EPSILON);
    assertTrue(allSessionProviders.get("A").getSessions().contains(sessionA1));
    assertEquals("A", sessionA1.getPoolName());

    // [A: 0, B: 1, B.x: 0, B.y: 0, C: 0]
    Future<Boolean> future = wm.applyMoveSessionAsync(sessionA1, "B.y");
    assertNotNull(future.get());
    assertTrue(future.get());
    wm.addTestEvent().get();
    allSessionProviders = wm.getAllSessionTriggerProviders();
    assertEquals(0, allSessionProviders.get("A").getSessions().size());
    assertEquals(0, allSessionProviders.get("B").getSessions().size());
    assertEquals(0, allSessionProviders.get("B.x").getSessions().size());
    assertEquals(1, allSessionProviders.get("B.y").getSessions().size());
    assertEquals(0, allSessionProviders.get("C").getSessions().size());
    assertEquals(0.32f, sessionA1.getClusterFraction(), EPSILON);
    assertTrue(allSessionProviders.get("B.y").getSessions().contains(sessionA1));
    assertEquals("B.y", sessionA1.getPoolName());

    // [A: 0, B: 0, B.x: 0, B.y: 0, C: 1]
    future = wm.applyMoveSessionAsync(sessionA1, "C");
    assertNotNull(future.get());
    assertTrue(future.get());
    wm.addTestEvent().get();
    allSessionProviders = wm.getAllSessionTriggerProviders();
    assertEquals(0, allSessionProviders.get("A").getSessions().size());
    assertEquals(0, allSessionProviders.get("B").getSessions().size());
    assertEquals(0, allSessionProviders.get("B.x").getSessions().size());
    assertEquals(0, allSessionProviders.get("B.y").getSessions().size());
    assertEquals(1, allSessionProviders.get("C").getSessions().size());
    assertEquals(0.2f, sessionA1.getClusterFraction(), EPSILON);
    assertTrue(allSessionProviders.get("C").getSessions().contains(sessionA1));
    assertEquals("C", sessionA1.getPoolName());

    // [A: 0, B: 0, B.x: 1, B.y: 0, C: 0]
    future = wm.applyMoveSessionAsync(sessionA1, "B.x");
    assertNotNull(future.get());
    assertTrue(future.get());
    wm.addTestEvent().get();
    allSessionProviders = wm.getAllSessionTriggerProviders();
    assertEquals(0, allSessionProviders.get("A").getSessions().size());
    assertEquals(0, allSessionProviders.get("B").getSessions().size());
    assertEquals(1, allSessionProviders.get("B.x").getSessions().size());
    assertEquals(0, allSessionProviders.get("B.y").getSessions().size());
    assertEquals(0, allSessionProviders.get("C").getSessions().size());
    assertEquals(0.08f, sessionA1.getClusterFraction(), EPSILON);
    assertTrue(allSessionProviders.get("B.x").getSessions().contains(sessionA1));
    assertEquals("B.x", sessionA1.getPoolName());

    WmTezSession sessionA2 = (WmTezSession) wm.getSession(null, mappingInput("A"), conf);

    // [A: 1, B: 0, B.x: 1, B.y: 0, C: 0]
    allSessionProviders = wm.getAllSessionTriggerProviders();
    assertEquals(1, allSessionProviders.get("A").getSessions().size());
    assertEquals(0, allSessionProviders.get("B").getSessions().size());
    assertEquals(1, allSessionProviders.get("B.x").getSessions().size());
    assertEquals(0, allSessionProviders.get("B.y").getSessions().size());
    assertEquals(0, allSessionProviders.get("C").getSessions().size());
    assertEquals(0.4f, sessionA2.getClusterFraction(), EPSILON);
    assertEquals(0.08f, sessionA1.getClusterFraction(), EPSILON);
    assertTrue(allSessionProviders.get("A").getSessions().contains(sessionA2));
    assertTrue(allSessionProviders.get("B.x").getSessions().contains(sessionA1));
    assertEquals("A", sessionA2.getPoolName());
    assertEquals("B.x", sessionA1.getPoolName());

    // A is maxed out on capacity, so this move should fail the session
    // [A: 1, B: 0, B.x: 0, B.y: 0, C: 0]
    future = wm.applyMoveSessionAsync(sessionA1, "A");
    assertNotNull(future.get());
    assertFalse(future.get());
    wm.addTestEvent().get();
    while(sessionA1.isOpen()) {
      Thread.sleep(100);
    }
    assertNull(sessionA1.getPoolName());
    assertEquals("Destination pool A is full. Killing query.", sessionA1.getReasonForKill());
    assertEquals(1, allSessionProviders.get("A").getSessions().size());
    assertEquals(0, allSessionProviders.get("B.x").getSessions().size());

    // return a loaned session goes back to tez am pool
    // [A: 0, B: 0, B.x: 0, B.y: 0, C: 0]
    wm.returnAfterUse(sessionA2);
    wm.addTestEvent().get();
    allSessionProviders = wm.getAllSessionTriggerProviders();
    assertEquals(0, allSessionProviders.get("A").getSessions().size());
    assertEquals(0, allSessionProviders.get("B").getSessions().size());
    assertEquals(0, allSessionProviders.get("B.x").getSessions().size());
    assertEquals(0, allSessionProviders.get("B.y").getSessions().size());
    assertEquals(0, allSessionProviders.get("C").getSessions().size());
    assertFalse(sessionA1.hasClusterFraction());
    assertFalse(allSessionProviders.get("A").getSessions().contains(sessionA1));
  }

  @Test(timeout=10000)
  public void testDelayedMoveSessions() throws Exception {
    final HiveConf conf = createConfForDelayedMove();
    MockQam qam = new MockQam();
    WMFullResourcePlan plan = new WMFullResourcePlan(plan(), Lists.newArrayList(
        pool("A", 2, 0.6f), pool("B", 1, 0.4f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A"), mapping("B", "B")));
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();

    WmTezSession sessionA1 = (WmTezSession) wm.getSession(null, mappingInput("A"), conf);

    // [A: 1, B: 0]
    Map<String, SessionTriggerProvider> allSessionProviders = wm.getAllSessionTriggerProviders();
    assertEquals(1, allSessionProviders.get("A").getSessions().size());
    assertEquals(0, allSessionProviders.get("B").getSessions().size());
    assertTrue(allSessionProviders.get("A").getSessions().contains(sessionA1));
    assertFalse(allSessionProviders.get("B").getSessions().contains(sessionA1));
    assertEquals(0.6f, sessionA1.getClusterFraction(), EPSILON);
    assertEquals("A", sessionA1.getPoolName());

    // If dest pool has capacity, move immediately
    // [A: 0, B: 1]
    Future<Boolean> future = wm.applyMoveSessionAsync(sessionA1, "B");
    assertNotNull(future.get());
    assertTrue(future.get());
    wm.addTestEvent().get();
    allSessionProviders = wm.getAllSessionTriggerProviders();
    assertEquals(0, allSessionProviders.get("A").getSessions().size());
    assertEquals(1, allSessionProviders.get("B").getSessions().size());
    assertFalse(allSessionProviders.get("A").getSessions().contains(sessionA1));
    assertTrue(allSessionProviders.get("B").getSessions().contains(sessionA1));
    assertEquals(0.4f, sessionA1.getClusterFraction(), EPSILON);
    assertEquals("B", sessionA1.getPoolName());

    WmTezSession sessionA2 = (WmTezSession) wm.getSession(null, mappingInput("A"), conf);
    // [A: 1, B: 1]
    allSessionProviders = wm.getAllSessionTriggerProviders();
    assertEquals(1, allSessionProviders.get("A").getSessions().size());
    assertEquals(1, allSessionProviders.get("B").getSessions().size());
    assertTrue(allSessionProviders.get("A").getSessions().contains(sessionA2));
    assertTrue(allSessionProviders.get("B").getSessions().contains(sessionA1));
    assertEquals(0.6f, sessionA2.getClusterFraction(), EPSILON);
    assertEquals(0.4f, sessionA1.getClusterFraction(), EPSILON);
    assertEquals("A", sessionA2.getPoolName());
    assertEquals("B", sessionA1.getPoolName());

    // Dest pool is maxed out. Keep running in source pool
    // [A: 1, B: 1]
    future = wm.applyMoveSessionAsync(sessionA2, "B");
    assertNotNull(future.get());
    assertFalse(future.get());
    wm.addTestEvent().get();
    allSessionProviders = wm.getAllSessionTriggerProviders();
    assertEquals(1, allSessionProviders.get("A").getSessions().size());
    assertEquals(1, allSessionProviders.get("B").getSessions().size());
    assertTrue(allSessionProviders.get("A").getSessions().contains(sessionA2));
    assertTrue(allSessionProviders.get("B").getSessions().contains(sessionA1));
    assertEquals(0.6f, sessionA2.getClusterFraction(), EPSILON);
    assertEquals(0.4f, sessionA1.getClusterFraction(), EPSILON);
    assertEquals("A", sessionA2.getPoolName());
    assertEquals("B", sessionA1.getPoolName());

    // A has queued requests. The new requests should get accepted. The delayed move should be killed
    WmTezSession sessionA3 = (WmTezSession) wm.getSession(null, mappingInput("A"), conf);
    WmTezSession sessionA4 = (WmTezSession) wm.getSession(null, mappingInput("A"), conf);

    while(sessionA2.isOpen()) {
      Thread.sleep(100);
    }
    assertNull(sessionA2.getPoolName());
    assertEquals("Destination pool B is full. Killing query.", sessionA2.getReasonForKill());

    // [A: 2, B: 1]
    allSessionProviders = wm.getAllSessionTriggerProviders();
    assertEquals(2, allSessionProviders.get("A").getSessions().size());
    assertEquals(1, allSessionProviders.get("B").getSessions().size());
    assertTrue(allSessionProviders.get("A").getSessions().contains(sessionA3));
    assertTrue(allSessionProviders.get("A").getSessions().contains(sessionA4));
    assertTrue(allSessionProviders.get("B").getSessions().contains(sessionA1));
    assertEquals(0.3f, sessionA3.getClusterFraction(), EPSILON);
    assertEquals(0.3f, sessionA4.getClusterFraction(), EPSILON);
    assertEquals(0.4f, sessionA1.getClusterFraction(), EPSILON);
    assertEquals("A", sessionA3.getPoolName());
    assertEquals("A", sessionA4.getPoolName());
    assertEquals("B", sessionA1.getPoolName());


    // Timeout
    // Attempt to move to pool B which is full. Keep running in source pool as a "delayed move".
    future = wm.applyMoveSessionAsync(sessionA3, "B");
    assertNotNull(future.get());
    assertFalse(future.get());
    wm.addTestEvent().get();
    allSessionProviders = wm.getAllSessionTriggerProviders();
    assertEquals(2, allSessionProviders.get("A").getSessions().size());
    assertEquals(1, allSessionProviders.get("B").getSessions().size());
    assertTrue(allSessionProviders.get("A").getSessions().contains(sessionA3));
    assertTrue(allSessionProviders.get("B").getSessions().contains(sessionA1));
    assertEquals(0.3f, sessionA3.getClusterFraction(), EPSILON);
    assertEquals(0.3f, sessionA4.getClusterFraction(), EPSILON);
    assertEquals("A", sessionA3.getPoolName());
    assertEquals("A", sessionA4.getPoolName());
    assertEquals("B", sessionA1.getPoolName());

    // Sleep till the delayed move times out and the move is attempted again.
    // The query should be killed during the move since destination pool B is still full.
    Thread.sleep(2000);
    while (sessionA3.isOpen()) {
      Thread.sleep(1000);
    }
    // [A:1, B:1]
    assertNull(sessionA3.getPoolName());
    assertEquals("Destination pool B is full. Killing query.", sessionA3.getReasonForKill());
    assertEquals(1, allSessionProviders.get("A").getSessions().size());
    assertEquals(1, allSessionProviders.get("B").getSessions().size());
    assertTrue(allSessionProviders.get("A").getSessions().contains(sessionA4));
    assertTrue(allSessionProviders.get("B").getSessions().contains(sessionA1));
    assertEquals(0.6f, sessionA4.getClusterFraction(), EPSILON);
    assertEquals(0.4f, sessionA1.getClusterFraction(), EPSILON);
    assertEquals("A", sessionA4.getPoolName());
    assertEquals("B", sessionA1.getPoolName());

    // Retry
    // Create another delayed move in A
    future = wm.applyMoveSessionAsync(sessionA4, "B");
    assertNotNull(future.get());
    assertFalse(future.get());
    wm.addTestEvent().get();

    assertEquals("A", sessionA4.getPoolName());
    assertTrue(allSessionProviders.get("A").getSessions().contains(sessionA4));
    assertEquals(1, allSessionProviders.get("A").getSessions().size());

    // Free up pool B.
    wm.returnAfterUse(sessionA1);
    wm.addTestEvent().get();
    allSessionProviders = wm.getAllSessionTriggerProviders();
    assertFalse(allSessionProviders.get("B").getSessions().contains(sessionA1));
    assertNull(sessionA1.getPoolName());

    // The delayed move is successfully retried since destination pool has freed up.
    // [A:0 B:1]
    assertEquals(0, allSessionProviders.get("A").getSessions().size());
    assertEquals(1, allSessionProviders.get("B").getSessions().size());
    assertTrue(allSessionProviders.get("B").getSessions().contains(sessionA4));
    assertEquals(0.4f, sessionA4.getClusterFraction(), EPSILON);
    assertEquals("B", sessionA4.getPoolName());
  }

  @org.junit.Ignore("HIVE-26364")
  @Test(timeout=10000)
  public void testAsyncSessionInitFailures() throws Exception {
    final HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WMFullResourcePlan plan = new WMFullResourcePlan(plan(),
        Lists.newArrayList(pool("A", 1, 1.0f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A")));
    final WorkloadManagerForTest wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();

    // Make sure session init gets stuck in init.
    TezSessionPool<WmTezSession> pool = wm.getTezAmPool();
    SampleTezSessionState theOnlySession = (SampleTezSessionState) pool.getSession();
    SettableFuture<Boolean> blockedWait = SettableFuture.create();
    theOnlySession.setWaitForAmRegistryFuture(blockedWait);
    pool.returnSession(theOnlySession);
    assertEquals(1, pool.getCurrentSize());

    final AtomicReference<WmTezSession> sessionA = new AtomicReference<>();
    final AtomicReference<Throwable> error = new AtomicReference<>();
    CountDownLatch cdl = new CountDownLatch(1);
    Thread t1 = new Thread(new GetSessionRunnable(sessionA, wm, error, conf, cdl, "A"));
    waitForThreadToBlock(cdl, t1);
    checkError(error);
    wm.addTestEvent().get();
    // The session is taken out of the pool, but is waiting for registration.
    assertEquals(0, pool.getCurrentSize());

    // Change the resource plan, so that the session gets killed.
    plan = new WMFullResourcePlan(plan(), Lists.newArrayList(pool("B", 1, 1.0f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "B")));
    wm.updateResourcePlanAsync(plan);
    wm.addTestEvent().get();
    blockedWait.set(true); // Meanwhile, the init succeeds!
    joinThread(t1);
    try {
      sessionA.get();
      fail("Expected an error but got " + sessionA.get());
    } catch (Throwable t) {
      // Expected.
    }
    try {
      // The get-session call should also fail.
      checkError(error);
      fail("Expected an error");
    } catch (Exception ex) {
      // Expected.
    }
    error.set(null);
    theOnlySession = validatePoolAfterCleanup(theOnlySession, conf, wm, pool, "B");

    // Initialization fails with retry, no resource plan change.
    SettableFuture<Boolean> failedWait = SettableFuture.create();
    failedWait.setException(new Exception("foo"));
    theOnlySession.setWaitForAmRegistryFuture(failedWait);
    TezSessionState retriedSession = wm.getSession(null, mappingInput("A"), conf);
    assertNotNull(retriedSession);
    assertNotSame(theOnlySession, retriedSession); // Should have been replaced.
    retriedSession.returnToSessionManager();
    theOnlySession = (SampleTezSessionState)retriedSession;

    // Initialization fails and so does the retry, no resource plan change.
    theOnlySession.setWaitForAmRegistryFuture(failedWait);
    wm.setNextWaitForAmRegistryFuture(failedWait); // Fail the retry.
    try {
      TezSessionState r = wm.getSession(null, mappingInput("A"), conf);
      fail("Expected an error but got " + r);
    } catch (Exception ex) {
      // Expected.
    }
    theOnlySession = validatePoolAfterCleanup(theOnlySession, conf, wm, pool, "B");

    // Init fails, but the session is also killed by WM before that.
    failedWait = SettableFuture.create();
    theOnlySession.setWaitForAmRegistryFuture(failedWait);
    wm.setNextWaitForAmRegistryFuture(failedWait); // Fail the retry.
    sessionA.set(null);
    cdl = new CountDownLatch(1);
    t1 = new Thread(new GetSessionRunnable(sessionA, wm, error, conf, cdl, "A"));
    waitForThreadToBlock(cdl, t1);
    wm.addTestEvent().get();
    // The session is taken out of the pool, but is waiting for registration.
    assertEquals(0, pool.getCurrentSize());

    plan = new WMFullResourcePlan(plan(), Lists.newArrayList(pool("A", 1, 1.0f)));
    plan.setMappings(Lists.newArrayList(mapping("A", "A")));
    wm.updateResourcePlanAsync(plan);
    wm.addTestEvent().get();
    failedWait.setException(new Exception("moo")); // Meanwhile, the init fails.
    joinThread(t1);
    try {
      sessionA.get();
      fail("Expected an error but got " + sessionA.get());
    } catch (Throwable t) {
      // Expected.
    }
    try {
      // The get-session call should also fail.
      checkError(error);
      fail("Expected an error");
    } catch (Exception ex) {
      // Expected.
    }
    validatePoolAfterCleanup(theOnlySession, conf, wm, pool, "A");
  }

  private SampleTezSessionState validatePoolAfterCleanup(
      SampleTezSessionState oldSession, HiveConf conf, WorkloadManager wm,
      TezSessionPool<WmTezSession> pool, String sessionPoolName) throws Exception {
    // Make sure the cleanup doesn't leave the pool without a session.
    SampleTezSessionState theOnlySession = (SampleTezSessionState) pool.getSession();
    assertNotNull(theOnlySession);
    theOnlySession.setWaitForAmRegistryFuture(null);
    assertNull(oldSession.getPoolName());
    assertFalse(oldSession.hasClusterFraction());
    pool.returnSession(theOnlySession);
    // Make sure we can actually get a session still - parallelism/etc. should not be affected.
    WmTezSession result = (WmTezSession) wm.getSession(null, mappingInput("A"), conf);
    assertEquals(sessionPoolName, result.getPoolName());
    assertEquals(1f, result.getClusterFraction(), EPSILON);
    result.returnToSessionManager();
    return theOnlySession;
  }

  private void assertKilledByWm(WmTezSession session) {
    assertNull(session.getPoolName());
    assertFalse(session.hasClusterFraction());
    assertTrue(session.isIrrelevantForWm());
  }

  private void checkError(final AtomicReference<Throwable> error) throws Exception {
    Throwable t = error.get();
    if (t == null) return;
    throw new Exception(t);
  }

  private HiveConf createConf() {
    HiveConf conf = new HiveConf();
    conf.set(ConfVars.HIVE_SERVER2_TEZ_SESSION_LIFETIME.varname, "-1");
    conf.set(ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "false");
    conf.set(ConfVars.LLAP_TASK_SCHEDULER_AM_REGISTRY_NAME.varname, "");
    return conf;
  }

  private HiveConf createConfForDelayedMove() {
    HiveConf conf = createConf();
    conf.set(ConfVars.HIVE_SERVER2_WM_DELAYED_MOVE.varname, "true");
    conf.set(ConfVars.HIVE_SERVER2_WM_DELAYED_MOVE_TIMEOUT.varname, "2");
    conf.set(ConfVars.HIVE_SERVER2_WM_DELAYED_MOVE_VALIDATOR_INTERVAL.varname, "1");
    return conf;
  }
}

