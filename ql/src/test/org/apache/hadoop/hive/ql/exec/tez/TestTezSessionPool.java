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

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;
import org.apache.hadoop.hive.conf.HiveConf;

public class TestTezSessionPool {

  HiveConf conf;
  Random random;
  private TezSessionPoolManager poolManager;

  private class TestTezSessionPoolManager extends TezSessionPoolManager {
    public TestTezSessionPoolManager() {
      super();
    }

    @Override
    public TezSessionState createSession(String sessionId) {
      return new TestTezSessionState(sessionId);
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
        TezSessionState sessionState = poolManager.getSession(null, conf, true);
        TezSessionState sessionState1 = poolManager.getSession(sessionState, conf, true);
        if (sessionState1 != sessionState) {
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
        conf.setBoolVar(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS, false);
        conf.setVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_DEFAULT_QUEUES, "a,b,c");
        conf.setIntVar(HiveConf.ConfVars.HIVE_SERVER2_TEZ_SESSIONS_PER_DEFAULT_QUEUE, 2);

        poolManager = new TestTezSessionPoolManager();
        poolManager.setupPool(conf);
        poolManager.startPool();
        TezSessionState sessionState = poolManager.getSession(null, conf, true);
        if (sessionState.getQueueName().compareTo("a") != 0) {
          fail();
        }
        poolManager.returnSession(sessionState);

        sessionState = poolManager.getSession(null, conf, true);
        if (sessionState.getQueueName().compareTo("b") != 0) {
          fail();
        }
        poolManager.returnSession(sessionState);

        sessionState = poolManager.getSession(null, conf, true);
        if (sessionState.getQueueName().compareTo("c") != 0) {
          fail();
        }
        poolManager.returnSession(sessionState);

        sessionState = poolManager.getSession(null, conf, true);
        if (sessionState.getQueueName().compareTo("a") != 0) {
          fail();
        }

        poolManager.returnSession(sessionState);

      } catch (Exception e) {
        e.printStackTrace();
        fail();
      }
    }

  public class SessionThread implements Runnable {

    @Override
      public void run() {
        try {
          HiveConf tmpConf = new HiveConf(conf);
          if (random.nextDouble() > 0.5) {
            tmpConf.set("tez.queue.name", "default");
          } else {
            tmpConf.set("tez.queue.name", "");
          }

          TezSessionState session = poolManager.getSession(null, tmpConf, true);
          Thread.sleep((random.nextInt(9) % 10) * 1000);
          poolManager.returnSession(session);
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
        Thread t = new Thread(new SessionThread());
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
}
