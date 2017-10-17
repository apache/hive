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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.Context;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.Test;

public class TestWorkloadManager {
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
      super(yarnQueue, conf, numSessions, qam, null);
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

    // Now destroy the returned session (which is technically not valid) and confirm correctness.
    session.destroy();
    assertEquals(1.0, session2.getClusterFraction(), EPSILON);
    //qam.assertWasNotCalled();
  }

  private HiveConf createConf() {
    HiveConf conf = new HiveConf();
    conf.set(ConfVars.HIVE_SERVER2_TEZ_SESSION_LIFETIME.varname, "-1");
    conf.set(ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "false");
    conf.set(ConfVars.LLAP_TASK_SCHEDULER_AM_REGISTRY_NAME.varname, "");
    return conf;
  }
}
