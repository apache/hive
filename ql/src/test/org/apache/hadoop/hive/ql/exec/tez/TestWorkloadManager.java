/**
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


import static org.junit.Assert.*;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.*;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.lang.Thread.State;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.tez.WorkloadManager.TmpHivePool;
import org.apache.hadoop.hive.ql.exec.tez.WorkloadManager.TmpResourcePlan;
import org.apache.hadoop.hive.ql.exec.tez.WorkloadManager.TmpUserMapping;
import org.apache.hadoop.hive.ql.exec.tez.WorkloadManager.TmpUserMappingType;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.Test;

public class TestWorkloadManager {
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
      try {
       session.set((WmTezSession) wm.getSession(old, userName, conf));
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
    public void updateSessionsAsync(double totalMaxAlloc, List<WmTezSession> sessions) {
      isCalled = true;
    }

    void assertWasCalled() {
      assertTrue(isCalled);
      isCalled = false;
    }
  }

  public static class WorkloadManagerForTest extends WorkloadManager {

    public WorkloadManagerForTest(String yarnQueue, HiveConf conf, int numSessions,
        QueryAllocationManager qam) {
      super(yarnQueue, conf, qam, null, createDummyPlan(numSessions));
    }

    public WorkloadManagerForTest(String yarnQueue, HiveConf conf,
        QueryAllocationManager qam, TmpResourcePlan plan) {
      super(yarnQueue, conf, qam, null, plan);
    }

    private static TmpResourcePlan createDummyPlan(int numSessions) {
      return new TmpResourcePlan(
          Lists.newArrayList(new TmpHivePool("llap", null, numSessions, 1.0f)),
          Lists.newArrayList(new TmpUserMapping(TmpUserMappingType.DEFAULT, "", "llap", 0)));
    }

    @Override
    protected WmTezSession createSessionObject(String sessionId) {
      return new SampleTezSessionState(sessionId, this, new HiveConf(getConf()));
    }

    @Override
    protected boolean ensureAmIsRegistered(WmTezSession session) throws Exception {
      return true;
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
    TezSessionState session = wm.getSession(nonPool, null, conf);
    verify(nonPool).close(anyBoolean());
    assertNotSame(nonPool, session);
    session.returnToSessionManager();
    TezSessionPoolSession diffPool = mock(TezSessionPoolSession.class);
    when(diffPool.getConf()).thenReturn(conf);
    doNothing().when(diffPool).returnToSessionManager();
    session = wm.getSession(diffPool, null, conf);
    verify(diffPool).returnToSessionManager();
    assertNotSame(diffPool, session);
    TezSessionState session2 = wm.getSession(session, null, conf);
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
    TezSessionState session = wm.getSession(null, null, conf);
    assertEquals("test", session.getQueueName());
    assertEquals("test", conf.get(TezConfiguration.TEZ_QUEUE_NAME));
    session.setQueueName("test2");
    session = wm.getSession(session, null, conf);
    assertEquals("test", session.getQueueName());
  }

  // Note (unrelated to epsilon): all the fraction checks are valid with the current logic in the
  //                              absence of policies. This will change when there are policies.
  private final static double EPSILON = 0.001;

  @Test(timeout = 10000)
  public void testReopen() throws Exception {
    // We should always get a different object, and cluster fraction should be propagated.
    HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WorkloadManager wm = new WorkloadManagerForTest("test", conf, 1, qam);
    wm.start();
    WmTezSession session = (WmTezSession) wm.getSession(null, null, conf);
    assertEquals(1.0, session.getClusterFraction(), EPSILON);
    qam.assertWasCalled();
    WmTezSession session2 = (WmTezSession) session.reopen(conf, null);
    assertNotSame(session, session2);
    assertEquals(1.0, session2.getClusterFraction(), EPSILON);
    assertEquals(0.0, session.getClusterFraction(), EPSILON);
    qam.assertWasCalled();
  }

  @Test(timeout = 10000)
  public void testDestroyAndReturn() throws Exception {
    // Session should not be lost; however the fraction should be discarded.
    HiveConf conf = createConf();
    MockQam qam = new MockQam();
    WorkloadManager wm = new WorkloadManagerForTest("test", conf, 2, qam);
    wm.start();
    WmTezSession session = (WmTezSession) wm.getSession(null, null, conf);
    assertEquals(1.0, session.getClusterFraction(), EPSILON);
    qam.assertWasCalled();
    WmTezSession session2 = (WmTezSession) wm.getSession(null, null, conf);
    assertEquals(0.5, session.getClusterFraction(), EPSILON);
    assertEquals(0.5, session2.getClusterFraction(), EPSILON);
    qam.assertWasCalled();
    assertNotSame(session, session2);
    session.destroy(); // Destroy before returning to the pool.
    assertEquals(1.0, session2.getClusterFraction(), EPSILON);
    assertEquals(0.0, session.getClusterFraction(), EPSILON);
    qam.assertWasCalled();

    // We never lose pool session, so we should still be able to get.
    session = (WmTezSession) wm.getSession(null, null, conf);
    session.returnToSessionManager();
    assertEquals(1.0, session2.getClusterFraction(), EPSILON);
    assertEquals(0.0, session.getClusterFraction(), EPSILON);
    qam.assertWasCalled();
  }

  private static TmpUserMapping create(String user, String pool) {
    return new TmpUserMapping(TmpUserMappingType.USER, user, pool, 0);
  }

  @Test(timeout = 10000)
  public void testClusterFractions() throws Exception {
    HiveConf conf = createConf();
    MockQam qam = new MockQam();
    List<TmpHivePool> l2 = Lists.newArrayList(
        new TmpHivePool("p1", null, 1, 0.5f), new TmpHivePool("p2", null, 2, 0.3f));
    TmpResourcePlan plan = new TmpResourcePlan(Lists.newArrayList(
        new TmpHivePool("r1", l2, 1, 0.6f), new TmpHivePool("r2", null, 1, 0.4f)),
        Lists.newArrayList(create("p1", "r1/p1"), create("p2", "r1/p2"), create("r1", "r1"),
            create("r2", "r2")));
    WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();
    assertEquals(5, wm.getNumSessions());
    // Get all the 5 sessions; validate cluster fractions.
    WmTezSession session05of06 = (WmTezSession) wm.getSession(null, "p1", conf);
    assertEquals(0.3, session05of06.getClusterFraction(), EPSILON);
    WmTezSession session03of06 = (WmTezSession) wm.getSession(null, "p2", conf);
    assertEquals(0.18, session03of06.getClusterFraction(), EPSILON);
    WmTezSession session03of06_2 = (WmTezSession) wm.getSession(null, "p2", conf);
    assertEquals(0.09, session03of06.getClusterFraction(), EPSILON);
    assertEquals(0.09, session03of06_2.getClusterFraction(), EPSILON);
    WmTezSession session02of06 = (WmTezSession) wm.getSession(null, "r1", conf);
    assertEquals(0.12, session02of06.getClusterFraction(), EPSILON);
    WmTezSession session04 = (WmTezSession) wm.getSession(null, "r2", conf);
    assertEquals(0.4, session04.getClusterFraction(), EPSILON);
    session05of06.returnToSessionManager();
    session03of06.returnToSessionManager();
    session03of06_2.returnToSessionManager();
    session02of06.returnToSessionManager();
    session04.returnToSessionManager();
  }

  @Test(timeout=10000)
  public void testQueueing() throws Exception {
    final HiveConf conf = createConf();
    MockQam qam = new MockQam();
    TmpResourcePlan plan = new TmpResourcePlan(Lists.newArrayList(
        new TmpHivePool("A", null, 2, 0.5f), new TmpHivePool("B", null, 2, 0.5f)),
        Lists.newArrayList(create("A", "A"), create("B", "B")));
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();
    WmTezSession sessionA1 = (WmTezSession) wm.getSession(null, "A", conf),
        sessionA2 = (WmTezSession) wm.getSession(null, "A", conf),
        sessionB1 = (WmTezSession) wm.getSession(null, "B", conf);
    final AtomicReference<WmTezSession> sessionA3 = new AtomicReference<>(),
        sessionA4 = new AtomicReference<>();
    final AtomicReference<Throwable> error = new AtomicReference<>();
    final CountDownLatch cdl = new CountDownLatch(1);

    Thread t1 = new Thread(new GetSessionRunnable(sessionA3, wm, error, conf, cdl, "A")),
        t2 = new Thread(new GetSessionRunnable(sessionA4, wm, error, conf, null, "A"));
    waitForThreadToBlockOnQueue(cdl, t1);
    t2.start();
    assertNull(sessionA3.get());
    assertNull(sessionA4.get());
    checkError(error);
    // While threads are blocked on A, we should still be able to get and return a B session.
    WmTezSession sessionB2 = (WmTezSession) wm.getSession(null, "B", conf);
    sessionB1.returnToSessionManager();
    sessionB2.returnToSessionManager();
    assertNull(sessionA3.get());
    assertNull(sessionA4.get());
    checkError(error);
    // Now release a single session from A.
    sessionA1.returnToSessionManager();
    t1.join();
    checkError(error);
    assertNotNull(sessionA3.get());
    assertNull(sessionA4.get());
    sessionA3.get().returnToSessionManager();
    t2.join();
    checkError(error);
    assertNotNull(sessionA4.get());
    sessionA4.get().returnToSessionManager();
    sessionA2.returnToSessionManager();
  }

  @Test(timeout=10000)
  public void testReuseWithQueueing() throws Exception {
    final HiveConf conf = createConf();
    MockQam qam = new MockQam();
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, 2, qam);
    wm.start();
    WmTezSession session1 = (WmTezSession) wm.getSession(null, null, conf);
    // First, try to reuse from the same pool - should "just work".
    WmTezSession session1a = (WmTezSession) wm.getSession(session1, null, conf);
    assertSame(session1, session1a);
    assertEquals(1.0, session1.getClusterFraction(), EPSILON);
    // Should still be able to get the 2nd session.
    WmTezSession session2 = (WmTezSession) wm.getSession(null, null, conf);

    // Now try to reuse with no other sessions remaining. Should still work.
    WmTezSession session2a = (WmTezSession) wm.getSession(session2, null, conf);
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
    waitForThreadToBlockOnQueue(cdl, t1);
    assertNull(session3.get());
    checkError(error);
    t2.start();
    cdl2.await();
    assertNull(session4.get());

    // We have released the session by trying to reuse it and going back into queue, s3 can start.
    t1.join();
    checkError(error);
    assertNotNull(session3.get());
    assertEquals(0.5, session3.get().getClusterFraction(), EPSILON);

    // Now release another session; the thread that gave up on reuse can proceed.
    session1.returnToSessionManager();
    t2.join();
    checkError(error);
    assertNotNull(session4.get());
    assertNotSame(session2, session4.get());
    assertEquals(0.5, session4.get().getClusterFraction(), EPSILON);
    session3.get().returnToSessionManager();
    session4.get().returnToSessionManager();
  }

  private void waitForThreadToBlockOnQueue(CountDownLatch cdl, Thread t1) throws InterruptedException {
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
    TmpResourcePlan plan = new TmpResourcePlan(Lists.newArrayList(
        new TmpHivePool("A", null, 2, 0.6f), new TmpHivePool("B", null, 1, 0.4f)),
        Lists.newArrayList(create("A", "A"), create("B", "B")));
    final WorkloadManager wm = new WorkloadManagerForTest("test", conf, qam, plan);
    wm.start();
    WmTezSession sessionA1 = (WmTezSession) wm.getSession(null, "A", conf),
        sessionA2 = (WmTezSession) wm.getSession(null, "A", conf);
    assertEquals("A", sessionA1.getPoolName());
    assertEquals(0.3f, sessionA1.getClusterFraction(), EPSILON);
    assertEquals("A", sessionA2.getPoolName());
    assertEquals(0.3f, sessionA2.getClusterFraction(), EPSILON);
    WmTezSession sessionB1 = (WmTezSession) wm.getSession(sessionA1, "B", conf);
    assertSame(sessionA1, sessionB1);
    assertEquals("B", sessionB1.getPoolName());
    assertEquals(0.4f, sessionB1.getClusterFraction(), EPSILON);
    assertEquals(0.6f, sessionA2.getClusterFraction(), EPSILON); // A1 removed from A.
    // Make sure that we can still get a session from A.
    WmTezSession sessionA3 = (WmTezSession) wm.getSession(null, "A", conf);
    assertEquals("A", sessionA3.getPoolName());
    assertEquals(0.3f, sessionA3.getClusterFraction(), EPSILON);
    assertEquals(0.3f, sessionA3.getClusterFraction(), EPSILON);
    sessionA3.returnToSessionManager();
    sessionB1.returnToSessionManager();
    sessionA2.returnToSessionManager();
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
}
