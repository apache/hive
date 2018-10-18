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
package org.apache.hive.beeline;

import org.junit.runners.Parameterized;
import org.junit.runners.model.RunnerScheduler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Class to run Parameterized test in parallel.
 * Source: http://hwellmann.blogspot.hu/2009/12/running-parameterized-junit-tests-in.html
 */
public class Parallelized extends Parameterized {
  private static class ThreadPoolScheduler implements RunnerScheduler {
    private static final String DEFAULT_TIMEOUT = "10";
    private ExecutorService executor;

    public ThreadPoolScheduler() {
      String threads = System.getProperty("junit.parallel.threads");
      //HIVE-17322: remove parallelism to check if the BeeLine test flakyness gets fixed
      //int numThreads = Runtime.getRuntime().availableProcessors();
      int numThreads = 1;
      if (threads != null) {
        numThreads = Integer.parseInt(threads);
      }
      executor = Executors.newFixedThreadPool(numThreads);
    }

    @Override
    public void finished() {
      executor.shutdown();
      try {
        String timeoutProp = System.getProperty("junit.parallel.timeout", DEFAULT_TIMEOUT);
        long timeout = Long.parseLong(timeoutProp);
        executor.awaitTermination(timeout, TimeUnit.MINUTES);
      } catch (InterruptedException exc) {
        throw new RuntimeException(exc);
      }
    }

    @Override
    public void schedule(Runnable childStatement) {
      executor.submit(childStatement);
    }
  }

  public Parallelized(Class klass) throws Throwable {
    super(klass);
    setScheduler(new ThreadPoolScheduler());
  }
}
