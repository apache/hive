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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.hive.metastore.metrics.Metrics;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CycleUpdateThreadTest {

  private static final String GAUGE_NAME = "my-gauge";
  private static final int THREAD_WAIT_TIMEOUT_IN_MILLISECONDS = 5000;

  @Before
  public void setup() {
    Metrics.getOrCreateGauge(GAUGE_NAME)
        .set(0); // pre-warm the gauge
  }

  @Test
  public void testCycleUpdater() throws Exception {
    final long prescribedSleepTime = 90;

    long startTime = System.currentTimeMillis();
    CycleUpdaterThread t = new CycleUpdaterThread(GAUGE_NAME, startTime, 50L);
    t.start();

    waitForTheThreadWithATimeout(t, THREAD_WAIT_TIMEOUT_IN_MILLISECONDS);
    sleepABit(prescribedSleepTime);

    t.interrupt();

    long elapsed = Metrics.getOrCreateGauge(GAUGE_NAME)
        .get();
    assertTrue("Elapsed time must be grater or equal than the sleep duration",
        elapsed >= prescribedSleepTime);
  }

  private static void waitForTheThreadWithATimeout(Thread t, int timeout) throws Exception {
    int waitCount = timeout;
    while (!t.isAlive() && (waitCount > 0)) {
      sleepABit(1L);
      --waitCount;
    }

    if (waitCount <= 0) {
      fail("The thread did not start in the given time " + timeout + "ms");
    }
  }

  private static void sleepABit(long duration) throws Exception {
    Thread.sleep(duration);
  }
}
