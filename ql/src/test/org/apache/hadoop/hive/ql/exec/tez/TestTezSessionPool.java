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

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;

public class TestTezSessionPool {

  private static final Logger LOG = LoggerFactory.getLogger(TestTezSessionPoolManager.class);
  HiveConf conf;
  Random random;
  private TezSessionPoolManager poolManager;

  private class TestTezSessionPoolManager extends TezSessionPoolManager {
    public TestTezSessionPoolManager() {
      super();
    }

    @Override
    public TezSessionPoolManager.TezSessionPoolSession createSession(String sessionId) {
      return new SampleTezSessionState(sessionId, this);
    }
  }

  @Before
  public void setUp() {
    conf = new HiveConf();
  }

  @Test
  public void testGetNonDefaultSession() {
    poolManager = new TestTezSessionPoolManager();
    try {
      TezSessionState sessionState = poolManager.getSession(null, conf, true, false);
      TezSessionState sessionState1 = poolManager.getSession(sessionState, conf, true, false);
      if (sessionState1 != sessionState) {
        fail();
      }
      conf.set("tez.queue.name", "nondefault");
      TezSessionState sessionState2 = poolManager.getSession(sessionState, conf, true, false);
      if (sessionState2 == sessionState) {
        fail();
      }
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSessionPoolGetInOrder() {
    try {
      conf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
      conf.setVar(ConfVars.HIVE_SERVER2_TEZ_DEFAULT_QUEUES, "a,b,c");
      conf.setIntVar(ConfVars.HIVE_SERVER2_TEZ_SESSIONS_PER_DEFAULT_QUEUE, 2);
      conf.setIntVar(ConfVars.HIVE_SERVER2_TEZ_SESSION_MAX_INIT_THREADS, 1);

      poolManager = new TestTezSessionPoolManager();
      poolManager.setupPool(conf);
      poolManager.startPool();
      TezSessionState sessionState = poolManager.getSession(null, conf, true, false);
      assertEquals("a", sessionState.getQueueName());
      poolManager.returnSession(sessionState, false);

      sessionState = poolManager.getSession(null, conf, true, false);
      assertEquals("b", sessionState.getQueueName());
      poolManager.returnSession(sessionState, false);

      sessionState = poolManager.getSession(null, conf, true, false);
      assertEquals("c", sessionState.getQueueName());
      poolManager.returnSession(sessionState, false);

      sessionState = poolManager.getSession(null, conf, true, false);
      if (sessionState.getQueueName().compareTo("a") != 0) {
        fail();
      }

      poolManager.returnSession(sessionState, false);

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }


  @Test
  public void testSessionPoolThreads() {
    // Make sure we get a correct number of sessions in each queue and that we don't crash.
    try {
      conf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
      conf.setVar(ConfVars.HIVE_SERVER2_TEZ_DEFAULT_QUEUES, "0,1,2");
      conf.setIntVar(ConfVars.HIVE_SERVER2_TEZ_SESSIONS_PER_DEFAULT_QUEUE, 4);
      conf.setIntVar(ConfVars.HIVE_SERVER2_TEZ_SESSION_MAX_INIT_THREADS, 16);

      poolManager = new TestTezSessionPoolManager();
      poolManager.setupPool(conf);
      poolManager.startPool();
      TezSessionState[] sessions = new TezSessionState[12];
      int[] queueCounts = new int[3];
      for (int i = 0; i < sessions.length; ++i) {
        sessions[i] = poolManager.getSession(null, conf, true, false);
        queueCounts[Integer.parseInt(sessions[i].getQueueName())] += 1;
      }
      for (int i = 0; i < queueCounts.length; ++i) {
        assertEquals(4, queueCounts[i]);
      }
      for (int i = 0; i < sessions.length; ++i) {
        poolManager.returnSession(sessions[i], false);
      }

    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSessionReopen() {
    try {
      conf.setBoolVar(ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
      conf.setVar(ConfVars.HIVE_SERVER2_TEZ_DEFAULT_QUEUES, "default,tezq1");
      conf.setIntVar(ConfVars.HIVE_SERVER2_TEZ_SESSIONS_PER_DEFAULT_QUEUE, 1);

      poolManager = new TestTezSessionPoolManager();
      TezSessionState session = Mockito.mock(TezSessionState.class);
      Mockito.when(session.getQueueName()).thenReturn("default");
      Mockito.when(session.isDefault()).thenReturn(false);
      Mockito.when(session.getConf()).thenReturn(conf);

      poolManager.reopenSession(session, conf, null, false);

      Mockito.verify(session).close(false);
      String[] files = null;
      Mockito.verify(session).open(conf, files);

      // mocked session starts with default queue
      assertEquals("default", session.getQueueName());

      // user explicitly specified queue name
      conf.set("tez.queue.name", "tezq1");
      poolManager.reopenSession(session, conf, null, false);
      assertEquals("tezq1", poolManager.getSession(null, conf, false, false).getQueueName());

      // user unsets queue name, will fallback to default session queue
      conf.unset("tez.queue.name");
      poolManager.reopenSession(session, conf, null, false);
      assertEquals("default", poolManager.getSession(null, conf, false, false).getQueueName());

      // session.open will unset the queue name from conf but Mockito intercepts the open call
      // and does not call the real method, so explicitly unset the queue name here
      conf.unset("tez.queue.name");
      // change session's default queue to tezq1 and rerun test sequence
      Mockito.when(session.getQueueName()).thenReturn("tezq1");
      poolManager.reopenSession(session, conf, null, false);
      assertEquals("tezq1", poolManager.getSession(null, conf, false, false).getQueueName());

      // user sets default queue now
      conf.set("tez.queue.name", "default");
      poolManager.reopenSession(session, conf, null, false);
      assertEquals("default", poolManager.getSession(null, conf, false, false).getQueueName());

      // user does not specify queue so use session default
      conf.unset("tez.queue.name");
      poolManager.reopenSession(session, conf, null, false);
      assertEquals("tezq1", poolManager.getSession(null, conf, false, false).getQueueName());
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testLlapSessionQueuing() {
    try {
      random = new Random(1000);
      conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_LLAP_CONCURRENT_QUERIES, 2);
      poolManager = new TestTezSessionPoolManager();
      poolManager.setupPool(conf);
      poolManager.startPool();
    } catch (Exception e) {
      LOG.error("Initialization error", e);
      fail();
    }

    List<Thread> threadList = new ArrayList<Thread>();
    for (int i = 0; i < 15; i++) {
      Thread t = new Thread(new SessionThread(true));
      threadList.add(t);
      t.start();
    }

    for (Thread t : threadList) {
      try {
        t.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail();
      }
    }
  }

  public class SessionThread implements Runnable {

    private boolean llap = false;

    public SessionThread(boolean llap) {
      this.llap = llap;
    }

    @Override
    public void run() {
      try {
        HiveConf tmpConf = new HiveConf(conf);
        if (random.nextDouble() > 0.5) {
          tmpConf.set("tez.queue.name", "default");
        } else {
          tmpConf.set("tez.queue.name", "");
        }

        TezSessionState session = poolManager.getSession(null, tmpConf, true, llap);
        Thread.sleep((random.nextInt(9) % 10) * 1000);
        poolManager.returnSession(session, llap);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void testReturn() {
    conf.set("tez.queue.name", "");
    random = new Random(1000);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
    conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_DEFAULT_QUEUES, "a,b,c");
    conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_SESSIONS_PER_DEFAULT_QUEUE, 2);
    try {
      poolManager = new TestTezSessionPoolManager();
      poolManager.setupPool(conf);
      poolManager.startPool();
    } catch (Exception e) {
      e.printStackTrace();
      fail();
    }
    List<Thread> threadList = new ArrayList<Thread>();
    for (int i = 0; i < 15; i++) {
      Thread t = new Thread(new SessionThread(false));
      threadList.add(t);
      t.start();
    }

    for (Thread t : threadList) {
      try {
        t.join();
      } catch (InterruptedException e) {
        e.printStackTrace();
        fail();
      }
    }
  }

  @Test
  public void testCloseAndOpenDefault() throws Exception {
    poolManager = new TestTezSessionPoolManager();
    TezSessionState session = Mockito.mock(TezSessionState.class);
    Mockito.when(session.isDefault()).thenReturn(false);

    poolManager.reopenSession(session, conf, null, false);

    Mockito.verify(session).close(false);
    String[] files = null;
    Mockito.verify(session).open(conf, files);
  }

  @Test
  public void testSessionDestroy() throws Exception {
    poolManager = new TestTezSessionPoolManager();
    TezSessionState session = Mockito.mock(TezSessionState.class);
    Mockito.when(session.isDefault()).thenReturn(false);

    poolManager.destroySession(session);
  }

  @Test
  public void testCloseAndOpenWithResources() throws Exception {
    poolManager = new TestTezSessionPoolManager();
    TezSessionState session = Mockito.mock(TezSessionState.class);
    Mockito.when(session.isDefault()).thenReturn(false);
    String[] extraResources = new String[] { "file:///tmp/foo.jar" };

    poolManager.reopenSession(session, conf, extraResources, false);

    Mockito.verify(session).close(false);
    Mockito.verify(session).open(conf, extraResources);
  }
}
