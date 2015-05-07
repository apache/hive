/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.exec.spark.session;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestSparkSessionManagerImpl {
  private static final Log LOG = LogFactory.getLog(TestSparkSessionManagerImpl.class);

  private SparkSessionManagerImpl sessionManagerHS2 = null;
  private boolean anyFailedSessionThread; // updated only when a thread has failed.


  /** Tests CLI scenario where we get a single session and use it multiple times. */
  @Test
  public void testSingleSessionMultipleUse() throws Exception {
    HiveConf conf = new HiveConf();
    conf.set("spark.master", "local");

    SparkSessionManager sessionManager = SparkSessionManagerImpl.getInstance();
    SparkSession sparkSession1 = sessionManager.getSession(null, conf, true);

    assertTrue(sparkSession1.isOpen());

    SparkSession sparkSession2 = sessionManager.getSession(sparkSession1, conf, true);
    assertTrue(sparkSession1 == sparkSession2); // Same session object is expected.

    assertTrue(sparkSession2.isOpen());
    sessionManager.shutdown();
    sessionManager.closeSession(sparkSession1);
  }

  /**
   * Tests multi-user scenario (like HiveServer2) where each user gets a session
   * and uses it multiple times.
   */
  @Test
  public void testMultiSessionMultipleUse() throws Exception {
    sessionManagerHS2 = SparkSessionManagerImpl.getInstance();

    // Shutdown existing session manager
    sessionManagerHS2.shutdown();

    HiveConf hiveConf = new HiveConf();
    hiveConf.set("spark.master", "local");

    sessionManagerHS2.setup(hiveConf);

    List<Thread> threadList = new ArrayList<Thread>();
    for (int i = 0; i < 10; i++) {
      Thread t = new Thread(new SessionThread(), "Session thread " + i);
      t.start();
      threadList.add(t);
    }

    for (Thread t : threadList) {
      try {
        t.join();
      } catch (InterruptedException e) {
        String msg = "Interrupted while waiting for test session threads.";
        LOG.error(msg, e);
        fail(msg);
      }
    }

    assertFalse("At least one of the session threads failed. See the test output for details.",
        anyFailedSessionThread);

    System.out.println("Ending SessionManagerHS2");
    sessionManagerHS2.shutdown();
  }

  /* Thread simulating a user session in HiveServer2. */
  public class SessionThread implements Runnable {


    @Override
    public void run() {
      try {
        Random random = new Random(Thread.currentThread().getId());
        String threadName = Thread.currentThread().getName();
        System.out.println(threadName + " started.");
        HiveConf conf = new HiveConf();
        conf.set("spark.master", "local");

        SparkSession prevSession = null;
        SparkSession currentSession = null;

        for(int i = 0; i < 5; i++) {
          currentSession = sessionManagerHS2.getSession(prevSession, conf, true);
          assertTrue(prevSession == null || prevSession == currentSession);
          assertTrue(currentSession.isOpen());
          System.out.println(String.format("%s got session (%d): %s",
              threadName, i, currentSession.getSessionId()));
          Thread.sleep((random.nextInt(3)+1) * 1000);

          sessionManagerHS2.returnSession(currentSession);
          prevSession = currentSession;
        }
        sessionManagerHS2.closeSession(currentSession);
        System.out.println(threadName + " ended.");
      } catch (Throwable e) {
        anyFailedSessionThread = true;
        String msg = String.format("Error executing '%s'", Thread.currentThread().getName());
        LOG.error(msg, e);
        fail(msg + " " + StringUtils.stringifyException(e));
      }
    }
  }
}
