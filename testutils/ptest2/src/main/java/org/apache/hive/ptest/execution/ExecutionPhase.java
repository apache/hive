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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Joiner;
import org.apache.hive.ptest.execution.conf.Host;
import org.apache.hive.ptest.execution.conf.QFileTestBatch;
import org.apache.hive.ptest.execution.conf.TestBatch;
import org.apache.hive.ptest.execution.context.ExecutionContext;
import org.slf4j.Logger;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class ExecutionPhase extends Phase {
  private static final long FOUR_HOURS = 4L* 60L * 60L * 1000L;
  private final ExecutionContext executionContext;
  private final HostExecutorBuilder hostExecutorBuilder;
  private final File succeededLogDir;
  private final File failedLogDir;
  private final BlockingQueue<TestBatch> parallelWorkQueue;
  private final BlockingQueue<TestBatch> isolatedWorkQueue;
  private final Set<String> executedTests;
  private final Set<String> failedTests;
  private final Supplier<List<TestBatch>> testBatchSupplier;
  private final Set<TestBatch> failedTestResults;

  public ExecutionPhase(List<HostExecutor> hostExecutors, ExecutionContext executionContext,
      HostExecutorBuilder hostExecutorBuilder,
      LocalCommandFactory localCommandFactory,
      ImmutableMap<String, String> templateDefaults,
      File succeededLogDir, File failedLogDir, Supplier<List<TestBatch>> testBatchSupplier,
      Set<String> executedTests, Set<String> failedTests, Logger logger)
          throws IOException {
    super(hostExecutors, localCommandFactory, templateDefaults, logger);
    this.executionContext = executionContext;
    this.hostExecutorBuilder = hostExecutorBuilder;
    this.succeededLogDir = succeededLogDir;
    this.failedLogDir = failedLogDir;
    this.testBatchSupplier = testBatchSupplier;
    this.executedTests = executedTests;
    this.failedTests = failedTests;
    this.parallelWorkQueue = new LinkedBlockingQueue<TestBatch>();
    this.isolatedWorkQueue = new LinkedBlockingQueue<TestBatch>();
    this.failedTestResults = Collections.
        synchronizedSet(new HashSet<TestBatch>());
  }
  @Override
  public void execute() throws Throwable {
    long start = System.currentTimeMillis();
    List<TestBatch> testBatches = Lists.newArrayList();
    for(TestBatch batch : testBatchSupplier.get()) {
      testBatches.add(batch);
      if(batch.isParallel()) {
        parallelWorkQueue.add(batch);
      } else {
        isolatedWorkQueue.add(batch);
      }
    }
    logger.info("ParallelWorkQueueSize={}, IsolatedWorkQueueSize={}", parallelWorkQueue.size(),
        isolatedWorkQueue.size());
    if (logger.isDebugEnabled()) {
      for (TestBatch testBatch : parallelWorkQueue) {
        logger.debug("PBatch: {}", testBatch);
      }
      for (TestBatch testBatch : isolatedWorkQueue) {
        logger.debug("IBatch: {}", testBatch);
      }
    }
    try {
      int expectedNumHosts = hostExecutors.size();
      initalizeHosts();
      do {
        replaceBadHosts(expectedNumHosts);
        List<ListenableFuture<Void>> results = Lists.newArrayList();
        for(HostExecutor hostExecutor : ImmutableList.copyOf(hostExecutors)) {
          results.add(hostExecutor.submitTests(parallelWorkQueue, isolatedWorkQueue, failedTestResults));
        }
        Futures.allAsList(results).get();
      } while(!(parallelWorkQueue.isEmpty() && isolatedWorkQueue.isEmpty()));
      for(TestBatch batch : testBatches) {
        File batchLogDir;
        if(failedTestResults.contains(batch)) {
          batchLogDir = new File(failedLogDir, batch.getName());
        } else {
          batchLogDir = new File(succeededLogDir, batch.getName());
        }
        JUnitReportParser parser = new JUnitReportParser(logger, batchLogDir);
        executedTests.addAll(parser.getAllExecutedTests());
        for (String failedTest : parser.getAllFailedTests()) {
          failedTests.add(failedTest + " (batchId=" + batch.getBatchId() + ")");
        }

        // if the TEST*.xml was not generated or was corrupt, let someone know
        if (parser.getTestClassesWithReportAvailable().size() < batch.getTestClasses().size()) {
          Set<String> expTestClasses = new HashSet<>(batch.getTestClasses());
          expTestClasses.removeAll(parser.getTestClassesWithReportAvailable());
          for (String testClass : expTestClasses) {
            StringBuilder messageBuilder = new StringBuilder();
            messageBuilder.append(testClass).append(" - did not produce a TEST-*.xml file (likely timed out)")
                .append(" (batchId=").append(batch.getBatchId()).append(")");
            if (batch instanceof QFileTestBatch) {
              Collection<String> tests = ((QFileTestBatch)batch).getTests();
              if (tests.size() != 0) {
                messageBuilder.append("\n\t[");
                messageBuilder.append(Joiner.on(",").join(tests));
                messageBuilder.append("]");
              }
            }
            failedTests.add(messageBuilder.toString());
          }
        }
      }
    } finally {
      long elapsed = System.currentTimeMillis() - start;
      logger.info("PERF: exec phase " +
          TimeUnit.MINUTES.convert(elapsed, TimeUnit.MILLISECONDS) + " minutes");
    }
  }
  private void replaceBadHosts(int expectedNumHosts)
      throws Exception {
    Set<Host> goodHosts = Sets.newHashSet();
    for(HostExecutor hostExecutor : ImmutableList.copyOf(hostExecutors)) {
      if(hostExecutor.isBad()) {
        logger.info("Removing host during execution phase: " + hostExecutor.getHost());
        executionContext.addBadHost(hostExecutor.getHost());
        hostExecutors.remove(hostExecutor);
      } else {
        goodHosts.add(hostExecutor.getHost());
      }
    }
    long start = System.currentTimeMillis();
    while(hostExecutors.size() < expectedNumHosts) {
      if(System.currentTimeMillis() - start > FOUR_HOURS) {
        throw new RuntimeException("Waited over fours for hosts, still have only " + 
            hostExecutors.size() + " hosts out of an expected " + expectedNumHosts);
      }
      logger.warn("Only " + hostExecutors.size() + " hosts out of an expected " + expectedNumHosts 
          + ", attempting to replace bad hosts");
      TimeUnit.MINUTES.sleep(1);
      executionContext.replaceBadHosts();
      for(Host host : executionContext.getHosts()) {
        if(!goodHosts.contains(host)) {
          HostExecutor hostExecutor = hostExecutorBuilder.build(host);
          initalizeHost(hostExecutor);
          if(hostExecutor.isBad()) {
            executionContext.addBadHost(hostExecutor.getHost());
          } else {
            logger.info("Adding new host during execution phase: " + host);
            hostExecutors.add(hostExecutor);
          }
        }
      }
    }
  }
}
