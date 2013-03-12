/*
 * Copyright (c) 2009-2012, toby weston & tempus-fugit committers
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 *
 */
package org.apache.hive.testutils.junit.runners;

import static java.util.concurrent.Executors.newFixedThreadPool;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hive.testutils.junit.runners.model.ConcurrentScheduler;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

/**
 * Originally taken from com.google.code.tempusfugit.concurrency.ConcurrentTestRunner
 */
public class ConcurrentTestRunner extends BlockJUnit4ClassRunner {

  private int numThreads = 1;

  public ConcurrentTestRunner(Class<?> type) throws InitializationError {
    super(type);

    String numThreadsProp = System.getProperty("test.concurrency.num.threads");
    if (numThreadsProp != null) {
      numThreads = Integer.valueOf(numThreadsProp);
    }

    setScheduler(new ConcurrentScheduler(newFixedThreadPool(numThreads, new ConcurrentTestRunnerThreadFactory())));

    System.err.println(">>> ConcurrenTestRunner initialize with " + numThreads + " threads");
    System.err.flush();
  }

  private static class ConcurrentTestRunnerThreadFactory implements ThreadFactory {
    private final AtomicLong count = new AtomicLong();

    public Thread newThread(Runnable runnable) {
      String threadName = ConcurrentTestRunner.class.getSimpleName() + "-Thread-" + count.getAndIncrement();
      System.err.println(">>> ConcurrentTestRunner.newThread " + threadName);
      System.err.flush();
      return new Thread(runnable, threadName);
    }
  }
}