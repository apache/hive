/*
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

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeoutException;

import org.apache.hadoop.hive.ql.exec.spark.HiveSparkClient;
import org.apache.hadoop.hive.ql.exec.spark.HiveSparkClientFactory;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.spark.SparkConf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestSparkSessionManagerImpl {
  private static final Logger LOG = LoggerFactory.getLogger(TestSparkSessionManagerImpl.class);

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

  /**
   *  Test HIVE-16395 - by default we force cloning of Configurations for Spark jobs
   */
  @Test
  public void testForceConfCloning() throws Exception {
    HiveConf conf = new HiveConf();
    conf.set("spark.master", "local");
    String sparkCloneConfiguration = HiveSparkClientFactory.SPARK_CLONE_CONFIGURATION;

    // Clear the value of sparkCloneConfiguration
    conf.unset(sparkCloneConfiguration);
    assertNull( "Could not clear " + sparkCloneConfiguration + " in HiveConf",
        conf.get(sparkCloneConfiguration));

    // By default we should set sparkCloneConfiguration to true in the Spark config
    checkSparkConf(conf, sparkCloneConfiguration, "true");

    // User can override value for sparkCloneConfiguration in Hive config to false
    conf.set(sparkCloneConfiguration, "false");
    checkSparkConf(conf, sparkCloneConfiguration, "false");

    // User can override value of sparkCloneConfiguration in Hive config to true
    conf.set(sparkCloneConfiguration, "true");
    checkSparkConf(conf, sparkCloneConfiguration, "true");
  }

  @Test
  public void testGetHiveException() throws Exception {
    HiveConf conf = new HiveConf();
    conf.set("spark.master", "local");
    SparkSessionManager ssm = SparkSessionManagerImpl.getInstance();
    SparkSessionImpl ss = (SparkSessionImpl) ssm.getSession(
        null, conf, true);

    Throwable e;

    e = new TimeoutException();
    checkHiveException(ss, e, ErrorMsg.SPARK_CREATE_CLIENT_TIMEOUT);

    e = new InterruptedException();
    checkHiveException(ss, e, ErrorMsg.SPARK_CREATE_CLIENT_INTERRUPTED);

    e = new RuntimeException("\t diagnostics: Application application_1508358311878_3322732 "
        + "failed 1 times due to ApplicationMaster for attempt "
        + "appattempt_1508358311878_3322732_000001 timed out. Failing the application.");
    checkHiveException(ss, e, ErrorMsg.SPARK_CREATE_CLIENT_TIMEOUT);

    e = new RuntimeException("\t diagnostics: Application application_1508358311878_3330000 "
        + "submitted by user hive to unknown queue: foo");
    checkHiveException(ss, e, ErrorMsg.SPARK_CREATE_CLIENT_INVALID_QUEUE,
        "submitted by user hive to unknown queue: foo");

    e = new RuntimeException("\t diagnostics: org.apache.hadoop.security.AccessControlException: "
        + "Queue root.foo is STOPPED. Cannot accept submission of application: "
        + "application_1508358311878_3369187");
    checkHiveException(ss, e, ErrorMsg.SPARK_CREATE_CLIENT_INVALID_QUEUE,
        "Queue root.foo is STOPPED");

    e = new RuntimeException("\t diagnostics: org.apache.hadoop.security.AccessControlException: "
        + "Queue root.foo already has 10 applications, cannot accept submission of application: "
        + "application_1508358311878_3384544");
    checkHiveException(ss, e, ErrorMsg.SPARK_CREATE_CLIENT_QUEUE_FULL,
        "Queue root.foo already has 10 applications");

    e = new RuntimeException("Exception in thread \"\"main\"\" java.lang.IllegalArgumentException: "
        + "Required executor memory (7168+10240 MB) is above the max threshold (16384 MB) of this "
        + "cluster! Please check the values of 'yarn.scheduler.maximum-allocation-mb' and/or "
        + "'yarn.nodemanager.resource.memory-mb'.");
    checkHiveException(ss, e, ErrorMsg.SPARK_CREATE_CLIENT_INVALID_RESOURCE_REQUEST,
        "Required executor memory (7168+10240 MB) is above the max threshold (16384 MB)");

    e = new RuntimeException("Exception in thread \"\"main\"\" java.lang.IllegalArgumentException: "
        + "requirement failed: initial executor number 5 must between min executor number10 "
        + "and max executor number 50");
    checkHiveException(ss, e, ErrorMsg.SPARK_CREATE_CLIENT_INVALID_RESOURCE_REQUEST,
        "initial executor number 5 must between min executor number10 and max executor number 50");

    // Other exceptions which defaults to SPARK_CREATE_CLIENT_ERROR
    e = new Exception();
    checkHiveException(ss, e, ErrorMsg.SPARK_CREATE_CLIENT_ERROR);
  }

  private void checkHiveException(SparkSessionImpl ss, Throwable e, ErrorMsg expectedErrMsg) {
    checkHiveException(ss, e, expectedErrMsg, null);
  }

  private void checkHiveException(SparkSessionImpl ss, Throwable e,
      ErrorMsg expectedErrMsg, String expectedMatchedStr) {
    HiveException he = ss.getHiveException(e);
    assertEquals(expectedErrMsg, he.getCanonicalErrorMsg());
    if (expectedMatchedStr != null) {
      assertEquals(expectedMatchedStr, ss.getMatchedString());
    }
  }

  /**
   * Force a Spark config to be generated and check that a config value has the expected value
   * @param conf the Hive config to use as a base
   * @param paramName the Spark config name to check
   * @param expectedValue the expected value in the Spark config
   */
  private void checkSparkConf(HiveConf conf, String paramName, String expectedValue) throws HiveException {
    SparkSessionManager sessionManager = SparkSessionManagerImpl.getInstance();
    SparkSessionImpl sparkSessionImpl = (SparkSessionImpl)
        sessionManager.getSession(null, conf, true);
    assertTrue(sparkSessionImpl.isOpen());
    HiveSparkClient hiveSparkClient = sparkSessionImpl.getHiveSparkClient();
    SparkConf sparkConf = hiveSparkClient.getSparkConf();
    String cloneConfig = sparkConf.get(paramName);
    sessionManager.closeSession(sparkSessionImpl);
    assertEquals(expectedValue, cloneConfig);
    sessionManager.shutdown();
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
