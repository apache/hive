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

package org.apache.hadoop.hive.ql;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hadoop.hive.ql.session.SessionState;

import com.codahale.metrics.Counter;

import org.hamcrest.Matcher;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.hadoop.hive.common.metrics.common.MetricsConstant.WAITING_COMPILE_OPS;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SERVER2_COMPILE_LOCK_TIMEOUT;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SERVER2_METRICS_ENABLED;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SERVER2_PARALLEL_COMPILATION;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_SERVER2_PARALLEL_COMPILATION_LIMIT;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;

/**
 * Class for testing HS2 compile lock behavior (serializable, parallel unbounded, parallel bounded).
 */
public class TestCompileLock {

  private static final int CONCURRENT_COMPILATION = 15151;

  private Driver driver;
  private HiveConf conf;

  @Before
  public void init() throws Exception {
    conf = new HiveConf();

    conf.setBoolVar(HIVE_SERVER2_METRICS_ENABLED, true);
    conf.setVar(HiveConf.ConfVars.DOWNLOADED_RESOURCES_DIR, System.getProperty("java.io.tmpdir"));
    conf.setTimeVar(HIVE_SERVER2_COMPILE_LOCK_TIMEOUT, 15, TimeUnit.SECONDS);

    MetricsFactory.close();
    MetricsFactory.init(conf);
  }

  private void initDriver(HiveConf conf, int threadCount) throws Exception {
    driver = Mockito.spy(new Driver(conf));
    resetParallelCompilationLimit(conf);

    AtomicInteger count = new AtomicInteger(threadCount);

    Mockito.doAnswer(invocation -> {
      Thread.sleep(500);
      verifyThatWaitingCompileOpsCountIsEqualTo(count.decrementAndGet());
      return null;
    }).when(driver).compile(eq("<QUERY>"), eq(true), eq(false));
  }

  @Test
  public void testSerializableCompilation() throws Exception {
    conf.setBoolVar(HIVE_SERVER2_PARALLEL_COMPILATION, false);

    initDriver(conf, 10);
    List<CommandProcessorResponse> responseList = compileAndRespond(10);

    verifyThatWaitingCompileOpsCountIsEqualTo(0);
    verifyThatTimedOutCompileOpsCountIsZero(responseList);

    verifyThatNoConcurrentCompilationWasIndeed(responseList);
  }

  @Test
  public void testParallelCompilationWithSingleQuota() throws Exception {
    conf.setBoolVar(HIVE_SERVER2_PARALLEL_COMPILATION, true);
    conf.setIntVar(HIVE_SERVER2_PARALLEL_COMPILATION_LIMIT, 1);

    initDriver(conf, 10);
    List<CommandProcessorResponse> responseList = compileAndRespond(10);

    verifyThatWaitingCompileOpsCountIsEqualTo(0);
    verifyThatTimedOutCompileOpsCountIsZero(responseList);

    verifyThatNoConcurrentCompilationWasIndeed(responseList);
  }

  @Test
  public void testParallelCompilationWithUnboundedQuota() throws Exception {
    conf.setBoolVar(HIVE_SERVER2_PARALLEL_COMPILATION, true);
    conf.setIntVar(HIVE_SERVER2_PARALLEL_COMPILATION_LIMIT, -1);

    initDriver(conf, 10);
    List<CommandProcessorResponse> responseList = compileAndRespond(10);

    verifyThatWaitingCompileOpsCountIsEqualTo(0);
    verifyThatTimedOutCompileOpsCountIsZero(responseList);

    verifyThatConcurrentCompilationWasIndeed(responseList);
  }

  @Test
  public void testParallelCompilationWithUnboundedQuotaAndSingleSession() throws Exception {
    conf.setBoolVar(HIVE_SERVER2_PARALLEL_COMPILATION, true);
    conf.setIntVar(HIVE_SERVER2_PARALLEL_COMPILATION_LIMIT, -1);

    initDriver(conf, 10);
    List<CommandProcessorResponse> responseList = compileAndRespond(true, 10);

    verifyThatWaitingCompileOpsCountIsEqualTo(0);
    verifyThatTimedOutCompileOpsCountIsZero(responseList);

    verifyThatNoConcurrentCompilationWasIndeed(responseList);
  }

  @Test
  public void testParallelCompilationTimeoutWithSingleQuota() throws Exception {
    conf.setBoolVar(HIVE_SERVER2_PARALLEL_COMPILATION, true);
    conf.setIntVar(HIVE_SERVER2_PARALLEL_COMPILATION_LIMIT, 1);
    conf.setTimeVar(HIVE_SERVER2_COMPILE_LOCK_TIMEOUT, 1, TimeUnit.SECONDS);

    initDriver(conf, 10);
    List<CommandProcessorResponse> responseList = compileAndRespond(10);

    verifyThatWaitingCompileOpsCountIsEqualTo(0);
    verifyThatTimedOutCompileOpsCountIsNotZero(responseList);
  }

  @Test
  public void testParallelCompilationWithSingleQuotaAndZeroTimeout() throws Exception {
    conf.setBoolVar(HIVE_SERVER2_PARALLEL_COMPILATION, true);
    conf.setIntVar(HIVE_SERVER2_PARALLEL_COMPILATION_LIMIT, 1);
    conf.setTimeVar(HIVE_SERVER2_COMPILE_LOCK_TIMEOUT, 0, TimeUnit.SECONDS);

    initDriver(conf, 10);
    List<CommandProcessorResponse> responseList = compileAndRespond(10);

    verifyThatWaitingCompileOpsCountIsEqualTo(0);
    verifyThatTimedOutCompileOpsCountIsZero(responseList);

    verifyThatNoConcurrentCompilationWasIndeed(responseList);
  }

  @Test
  public void testParallelCompilationWithMultipleQuotas() throws Exception {
    conf.setBoolVar(HIVE_SERVER2_PARALLEL_COMPILATION, true);
    conf.setIntVar(HIVE_SERVER2_PARALLEL_COMPILATION_LIMIT, 2);

    initDriver(conf, 10);
    List<CommandProcessorResponse> responseList = compileAndRespond(10);

    verifyThatWaitingCompileOpsCountIsEqualTo(0);
    verifyThatTimedOutCompileOpsCountIsZero(responseList);

    verifyThatConcurrentCompilationWasIndeed(responseList);
  }

  @Test
  public void testParallelCompilationWithMultipleQuotasAndClientSessionConcurrency() throws Exception {
    conf.setBoolVar(HIVE_SERVER2_PARALLEL_COMPILATION, true);
    conf.setIntVar(HIVE_SERVER2_PARALLEL_COMPILATION_LIMIT, 2);

    initDriver(conf, 10);
    List<CommandProcessorResponse> responseList = new ArrayList<>();

    List<Callable<List<CommandProcessorResponse>>> callables = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      callables.add(() -> compileAndRespond(true, 2));
    }

    ExecutorService pool = Executors.newFixedThreadPool(callables.size());
    try {
      List<Future<List<CommandProcessorResponse>>> futures = pool.invokeAll(callables);
      for (Future<List<CommandProcessorResponse>> future : futures) {
        responseList.addAll(future.get());
      }
    } finally {
      pool.shutdown();
    }

    verifyThatWaitingCompileOpsCountIsEqualTo(0);
    verifyThatTimedOutCompileOpsCountIsZero(responseList);

    verifyThatConcurrentCompilationWasIndeed(responseList);
  }

  private List<CommandProcessorResponse> compileAndRespond(int threadCount) throws Exception {
    return compileAndRespond(false, threadCount);
  }

  private List<CommandProcessorResponse> compileAndRespond(boolean reuseSession, int threadCount) throws Exception {
    List<CommandProcessorResponse> responseList = new ArrayList<>();
    SessionState sessionState = new SessionState(conf);

    List<Callable<CommandProcessorResponse>> callables = new ArrayList<>();
    for (int i = 0; i < threadCount; i++) {
      callables.add(() -> {
        SessionState ss = (reuseSession)? sessionState : new SessionState(conf);
        SessionState.setCurrentSessionState(ss);

        CommandProcessorResponse response;
        try{
          response = driver.compileAndRespond("<QUERY>");

        } finally {
          SessionState.detachSession();
        }
        return response;
      });
    }

    ExecutorService pool = Executors.newFixedThreadPool(callables.size());
    try{
      List<Future<CommandProcessorResponse>> futures = pool.invokeAll(callables);

      for (Future<CommandProcessorResponse> future : futures) {
        try {
          responseList.add(future.get());

        } catch (ExecutionException ex) {
          responseList.add(
              (ex.getCause() instanceof CommandProcessorResponse) ?
                new CommandProcessorResponse(ErrorMsg.COMPILE_LOCK_TIMED_OUT.getErrorCode()) :
                new CommandProcessorResponse(CONCURRENT_COMPILATION));
        }
      }
    } finally {
      pool.shutdown();
    }

    return responseList;
  }

  private void resetParallelCompilationLimit(HiveConf conf) throws Exception {
    Enum<?> compileLock = createEnumInstance("instance", Class.forName("org.apache.hadoop.hive.ql.lock" +
        ".CompileLockFactory$SessionWithQuotaCompileLock"));

    Field field = compileLock.getClass().getDeclaredField("globalCompileQuotas");
    field.setAccessible(true);

    int compileLimit = conf.getIntVar(HIVE_SERVER2_PARALLEL_COMPILATION_LIMIT);
    field.set(compileLock, new Semaphore(compileLimit));
  }

  @SuppressWarnings("unchecked")
  private static <T extends Enum<T>> T createEnumInstance(String name, Type type) {
    return Enum.valueOf((Class<T>) type, name);
  }

  private void verifyThatTimedOutCompileOpsCountIsZero(List<CommandProcessorResponse> responseList) {
    verifyErrorCount(ErrorMsg.COMPILE_LOCK_TIMED_OUT.getErrorCode(),
        is(equalTo(0)), responseList);
  }

  private void verifyThatTimedOutCompileOpsCountIsNotZero(List<CommandProcessorResponse> responseList) {
    verifyErrorCount(ErrorMsg.COMPILE_LOCK_TIMED_OUT.getErrorCode(),
        is(not(equalTo(0))), responseList);
  }

  private void verifyThatConcurrentCompilationWasIndeed(List<CommandProcessorResponse> responseList){
    verifyErrorCount(CONCURRENT_COMPILATION,
        is(not(equalTo(0))), responseList);
  }

  private void verifyThatNoConcurrentCompilationWasIndeed(List<CommandProcessorResponse> responseList){
    verifyErrorCount(CONCURRENT_COMPILATION,
        is(equalTo(0)), responseList);
  }
  private void verifyErrorCount(int code, Matcher<Integer> matcher, List<CommandProcessorResponse> responseList) {
    int count = 0;

    for(CommandProcessorResponse response : responseList){
      if(code == response.getResponseCode()){
        count++;
      }
    }
    assertThat(count, matcher);
  }

  private void verifyThatWaitingCompileOpsCountIsEqualTo(long count) {
    Counter counter = getCounter(WAITING_COMPILE_OPS);
    assertNotNull(counter);
    assertThat(counter.getCount(), is(equalTo(count)));
  }

  private Counter getCounter(String counter) {
    CodahaleMetrics metrics = (CodahaleMetrics) MetricsFactory.getInstance();
    SortedMap<String, Counter> counters = metrics.getMetricRegistry().getCounters();

    assertNotNull(counters);
    return counters.get(counter);
  }

}
