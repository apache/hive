/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.ptest.execution;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.hive.ptest.execution.conf.TestBatch;
import org.slf4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class ExecutionPhase extends Phase {

  private final File failedLogDir;
  private final BlockingQueue<TestBatch> parallelWorkQueue;
  private final BlockingQueue<TestBatch> isolatedWorkQueue;
  private final Set<String> failedTests;
  private final Supplier<List<TestBatch>> testBatchSupplier;
  private final List<TestBatch> failedTestResults;

  public ExecutionPhase(ImmutableList<HostExecutor> hostExecutors,
      LocalCommandFactory localCommandFactory,
      ImmutableMap<String, String> templateDefaults,
      File failedLogDir, Supplier<List<TestBatch>> testBatchSupplier,
      Set<String> failedTests, Logger logger) throws IOException {
    super(hostExecutors, localCommandFactory, templateDefaults, logger);
    this.failedLogDir = failedLogDir;
    this.testBatchSupplier = testBatchSupplier;
    this.failedTests = failedTests;
    this.parallelWorkQueue = new LinkedBlockingQueue<TestBatch>();
    this.isolatedWorkQueue = new LinkedBlockingQueue<TestBatch>();
    this.failedTestResults = Collections.
        synchronizedList(new ArrayList<TestBatch>());
  }
  @Override
public void execute() throws Throwable {
    long start = System.currentTimeMillis();
    for(TestBatch batch : testBatchSupplier.get()) {
      if(batch.isParallel()) {
        parallelWorkQueue.add(batch);
      } else {
        isolatedWorkQueue.add(batch);
      }
    }
    try {
      do {
        double numberBadHosts = 0d;
        for(HostExecutor hostExecutor : hostExecutors) {
          if(hostExecutor.remainingDrones() == 0) {
            numberBadHosts++;
          }
        }
        Preconditions.checkState(hostExecutors.size() > 0, "Host executors cannot be empty");
        if((numberBadHosts / (double)hostExecutors.size()) > 0.30d) {
          throw new IllegalStateException("Too many bad hosts: " + (int)numberBadHosts + 
              " bad hosts out of " + hostExecutors.size() + " is greater than threshold of 30%");
        }
        List<ListenableFuture<Void>> results = Lists.newArrayList();
        for(HostExecutor hostExecutor : getHostExecutors()) {
          results.add(hostExecutor.submitTests(parallelWorkQueue, isolatedWorkQueue, failedTestResults));
        }
        Futures.allAsList(results).get();
      } while(!(parallelWorkQueue.isEmpty() && isolatedWorkQueue.isEmpty()));
      Preconditions.checkState(parallelWorkQueue.isEmpty(), "Parallel work queue is not empty. All drones must have aborted.");
      Preconditions.checkState(isolatedWorkQueue.isEmpty(), "Isolated work queue is not empty. All drones must have aborted.");
      if(!failedTestResults.isEmpty()) {
        for(TestBatch failure : failedTestResults) {
          File batchLogDir = new File(failedLogDir, failure.getName());
          JUnitReportParser parser = new JUnitReportParser(logger, batchLogDir);
          for(String failedTest : parser.getFailedTests()) {
            failedTests.add(failedTest);
          }
        }
      }
    } finally {
      long elapsed = System.currentTimeMillis() - start;
      logger.info("PERF: exec phase " +
          TimeUnit.MINUTES.convert(elapsed, TimeUnit.MILLISECONDS) + " minutes");
    }
  }

}
