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

package org.apache.hadoop.hive.ql.io;

import static org.junit.Assert.*;

import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.junit.Test;

import com.google.common.collect.Sets;

public class TestIOContextMap {

  private void syncThreadStart(final CountDownLatch cdlIn, final CountDownLatch cdlOut) {
    cdlIn.countDown();
    try {
      cdlOut.await();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testMRTezGlobalMap() throws Exception {
    // Tests concurrent modification, and that results are the same per input across threads
    // but different between inputs.
    final int THREAD_COUNT = 2, ITER_COUNT = 1000;
    final AtomicInteger countdown = new AtomicInteger(ITER_COUNT);
    final CountDownLatch phase1End = new CountDownLatch(THREAD_COUNT);
    final IOContext[] results = new IOContext[ITER_COUNT];
    ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
    final CountDownLatch cdlIn = new CountDownLatch(THREAD_COUNT), cdlOut = new CountDownLatch(1);

    @SuppressWarnings("unchecked")
    FutureTask<Void>[] tasks = new FutureTask[THREAD_COUNT];
    for (int i = 0; i < tasks.length; ++i) {
      tasks[i] = new FutureTask<Void>(new Callable<Void>() {
        public Void call() throws Exception {
          Configuration conf = new Configuration();
          syncThreadStart(cdlIn, cdlOut);
          // Phase 1 - create objects.
          while (true) {
            int nextIx = countdown.decrementAndGet();
            if (nextIx < 0) break;
            conf.set(Utilities.INPUT_NAME, "Input " + nextIx);
            results[nextIx] = IOContextMap.get(conf);
            if (nextIx == 0) break;
          }
          phase1End.countDown();
          phase1End.await();
          // Phase 2 - verify we get the expected objects created by all threads.
          for (int i = 0; i < ITER_COUNT; ++i) {
            conf.set(Utilities.INPUT_NAME, "Input " + i);
            IOContext ctx = IOContextMap.get(conf);
            assertSame(results[i], ctx);
          }
          return null;
        }
      });
      executor.execute(tasks[i]);
    }

    cdlIn.await(); // Wait for all threads to be ready.
    cdlOut.countDown(); // Release them at the same time.
    for (int i = 0; i < tasks.length; ++i) {
      tasks[i].get();
    }
    Set<IOContext> resultSet = Sets.newIdentityHashSet();
    for (int i = 0; i < results.length; ++i) {
      assertTrue(resultSet.add(results[i])); // All the objects must be different.
    }
  }

  @Test
  public void testTezLlapAttemptMap() throws Exception {
    // Tests that different threads get the same object per attempt per input, and different
    // between attempts/inputs; that attempt is inherited between threads; and that clearing
    // the attempt produces a different result.
    final int THREAD_COUNT = 2, ITER_COUNT = 1000, ATTEMPT_COUNT = 3;
    final AtomicInteger countdown = new AtomicInteger(ITER_COUNT);
    final IOContext[] results = new IOContext[ITER_COUNT * ATTEMPT_COUNT];
    ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
    final CountDownLatch cdlIn = new CountDownLatch(THREAD_COUNT), cdlOut = new CountDownLatch(1);

    @SuppressWarnings("unchecked")
    FutureTask<Void>[] tasks = new FutureTask[THREAD_COUNT];
    for (int i = 0; i < tasks.length; ++i) {
      tasks[i] = new FutureTask<Void>(new Callable<Void>() {
        public Void call() throws Exception {
          final Configuration conf = new Configuration(), conf2 = new Configuration();
          syncThreadStart(cdlIn, cdlOut);
          while (true) {
            int nextIx = countdown.decrementAndGet();
            if (nextIx < 0) break;
            String input1 = "Input " + nextIx;
            conf.set(Utilities.INPUT_NAME, input1);
            for (int j = 0; j < ATTEMPT_COUNT; ++j) {
              String attemptId = "Attempt " + nextIx + ":" + j;
              IOContextMap.setThreadAttemptId(attemptId);
              final IOContext r1 = results[(nextIx * ATTEMPT_COUNT) + j] = IOContextMap.get(conf);
              // For some attempts, check inheritance.
              if ((nextIx % (ITER_COUNT / 10)) == 0) {
                String input2 = "Input2 " + nextIx;
                conf2.set(Utilities.INPUT_NAME, input2);
                final AtomicReference<IOContext> ref2 = new AtomicReference<>();
                Thread t = new Thread(new Runnable() {
                  public void run() {
                    assertSame(r1, IOContextMap.get(conf));
                    ref2.set(IOContextMap.get(conf2));
                  }
                });
                t.start();
                t.join();
                assertSame(ref2.get(), IOContextMap.get(conf2));
              }
              // Don't clear the attempt ID, or the stuff will be cleared.
            }
            if (nextIx == 0) break;
          }
          return null;
        }
      });
      executor.execute(tasks[i]);
    }

    cdlIn.await(); // Wait for all threads to be ready.
    cdlOut.countDown(); // Release them at the same time.
    for (int i = 0; i < tasks.length; ++i) {
      tasks[i].get();
    }
    Configuration conf = new Configuration();
    Set<IOContext> resultSet = Sets.newIdentityHashSet();
    for (int i = 0; i < ITER_COUNT; ++i) {
      conf.set(Utilities.INPUT_NAME, "Input " + i);
      for (int j = 0; j < ATTEMPT_COUNT; ++j) {
        String attemptId = "Attempt " + i + ":" + j;
        IOContext result = results[(i * ATTEMPT_COUNT) + j];
        assertTrue(resultSet.add(result)); // All the objects must be different.
        IOContextMap.setThreadAttemptId(attemptId);
        assertSame(result, IOContextMap.get(conf)); // Matching result for attemptId + input.
        IOContextMap.clearThreadAttempt(attemptId);
        IOContextMap.setThreadAttemptId(attemptId);
        assertNotSame(result, IOContextMap.get(conf)); // Different result after clearing.
      }
    }
  }

  @Test
    public void testSparkThreadLocal() throws Exception {
    // Test that input name does not change IOContext returned, and that each thread gets its own.
    final Configuration conf1 = new Configuration();
    conf1.set(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE.varname, "spark");
    final Configuration conf2 = new Configuration(conf1);
    conf2.set(Utilities.INPUT_NAME, "Other input");
    final int THREAD_COUNT = 2;
    ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
    final CountDownLatch cdlIn = new CountDownLatch(THREAD_COUNT), cdlOut = new CountDownLatch(1);
    @SuppressWarnings("unchecked")
    FutureTask<IOContext>[] tasks = new FutureTask[THREAD_COUNT];
    for (int i = 0; i < tasks.length; ++i) {
      tasks[i] = new FutureTask<IOContext>(new Callable<IOContext>() {
        public IOContext call() throws Exception {
          syncThreadStart(cdlIn, cdlOut);
          IOContext c1 = IOContextMap.get(conf1), c2 = IOContextMap.get(conf2);
          assertSame(c1, c2);
          return c1;
        }
      });
      executor.execute(tasks[i]);
    }

    cdlIn.await(); // Wait for all threads to be ready.
    cdlOut.countDown(); // Release them at the same time.
    Set<IOContext> results = Sets.newIdentityHashSet();
    for (int i = 0; i < tasks.length; ++i) {
      assertTrue(results.add(tasks[i].get())); // All the objects must be different.
    }
  }

}
