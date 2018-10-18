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
package org.apache.hadoop.hive.ql.plan;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

import static org.junit.Assert.assertEquals;


@RunWith(Parameterized.class)
public final class TestExecutionEngineWorkConcurrency {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{{new TezDagIdProvider()}, {new SparkDagIdProvider()}});
  }

  private final ExecutionEngineDagIdGenerator executionEngineDagIdGenerator;

  public TestExecutionEngineWorkConcurrency(ExecutionEngineDagIdGenerator executionEngineDagIdGenerator) {
    this.executionEngineDagIdGenerator = executionEngineDagIdGenerator;
  }

  @Test
  public void ensureDagIdIsUnique() throws Exception {
    final int threadCount = 5;
    final CountDownLatch threadReadyToStartSignal = new CountDownLatch(threadCount);
    final CountDownLatch startThreadSignal = new CountDownLatch(1);
    final int numberOfWorkToCreatePerThread = 100;

    List<FutureTask<Set<String>>> tasks = Lists.newArrayList();
    for (int i = 0; i < threadCount; i++) {
      tasks.add(new FutureTask<>(new Callable<Set<String>>() {
        @Override
        public Set<String> call() throws Exception {
          threadReadyToStartSignal.countDown();
          startThreadSignal.await();
          return generateWorkDagIds(numberOfWorkToCreatePerThread);
        }
      }));
    }
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    for (FutureTask<Set<String>> task : tasks) {
      executor.execute(task);
    }
    threadReadyToStartSignal.await();
    startThreadSignal.countDown();
    Set<String> allWorkDagIds = getAllWorkDagIds(tasks);
    assertEquals(threadCount * numberOfWorkToCreatePerThread, allWorkDagIds.size());
  }

  private Set<String> generateWorkDagIds(int numberOfNames) {
    Set<String> workIds = Sets.newHashSet();
    for (int i = 0; i < numberOfNames; i++) {
      workIds.add(executionEngineDagIdGenerator.getDagId());
    }
    return workIds;
  }

  private static Set<String> getAllWorkDagIds(List<FutureTask<Set<String>>> tasks)
      throws ExecutionException, InterruptedException {
    Set<String> allWorkDagIds = Sets.newHashSet();
    for (FutureTask<Set<String>> task : tasks) {
      allWorkDagIds.addAll(task.get());
    }
    return allWorkDagIds;
  }

  private interface ExecutionEngineDagIdGenerator {
    String getDagId();
  }

  private static final class TezDagIdProvider implements ExecutionEngineDagIdGenerator {

    @Override
    public String getDagId() {
      return new TezWork("query-id").getDagId();
    }
  }

  private static final class SparkDagIdProvider implements ExecutionEngineDagIdGenerator {

    @Override
    public String getDagId() {
      return new SparkWork("query-id").getName();
    }
  }
}
