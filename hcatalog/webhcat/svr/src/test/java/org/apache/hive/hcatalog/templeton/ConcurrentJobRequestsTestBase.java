/**
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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hive.hcatalog.templeton;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Future;

import org.apache.hive.hcatalog.templeton.tool.TempletonControllerJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

/*
 * Base class for mocking job operations with concurrent requests.
 */
public class ConcurrentJobRequestsTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(ConcurrentJobRequestsTestBase.class);
  private boolean started = false;
  private Object lock = new Object();

  MockAnswerTestHelper<QueueStatusBean> statusJobHelper = new MockAnswerTestHelper<QueueStatusBean>();
  MockAnswerTestHelper<QueueStatusBean> killJobHelper = new MockAnswerTestHelper<QueueStatusBean>();
  MockAnswerTestHelper<List<JobItemBean>> listJobHelper = new MockAnswerTestHelper<List<JobItemBean>>();
  MockAnswerTestHelper<Integer> submitJobHelper = new MockAnswerTestHelper<Integer>();

  /*
   * Waits for other threads to join and returns with its Id.
   */
  private int waitForAllThreadsToStart(JobRunnable jobRunnable, int poolThreadCount) {
    int currentId = jobRunnable.threadStartCount.incrementAndGet();
    LOG.info("Waiting for other threads with thread id: " + currentId);
    synchronized(lock) {
      /*
       * We need a total of poolThreadCount + 1 threads to start at same. There are
       * poolThreadCount threads in thread pool and another one which has started them.
       * The thread which sees atomic counter as poolThreadCount+1 is the last thread`
       * to join and wake up all threads to start all at once.
       */
      if (currentId > poolThreadCount) {
        LOG.info("Waking up all threads: " + currentId);
        started = true;
        this.lock.notifyAll();
      } else {
        while (!started) {
          try {
            this.lock.wait();
          } catch (InterruptedException ignore) {
          }
        }
      }
    }

    return currentId;
  }

  public JobRunnable ConcurrentJobsStatus(final int threadCount, AppConfig appConfig,
         final boolean killThreads, boolean interruptThreads, final Answer<QueueStatusBean> answer)
         throws IOException, InterruptedException, QueueException, NotAuthorizedException,
         BadParam, BusyException {

    StatusDelegator delegator = new StatusDelegator(appConfig);
    final StatusDelegator mockDelegator = Mockito.spy(delegator);

    Mockito.doAnswer(answer).when(mockDelegator).getJobStatus(Mockito.any(String.class),
                             Mockito.any(String.class));

    JobRunnable statusJobRunnable = new JobRunnable() {
      @Override
      public void run() {
        try {
          int threadId = waitForAllThreadsToStart(this, threadCount);
          LOG.info("Started executing Job Status operation. ThreadId : " + threadId);
          mockDelegator.run("admin", "job_1000" + threadId);
        } catch (Exception ex) {
          exception = ex;
        }
      }
    };

    executeJobOperations(statusJobRunnable, threadCount, killThreads, interruptThreads);
    return statusJobRunnable;
  }

  public JobRunnable ConcurrentListJobs(final int threadCount, AppConfig config,
         final boolean killThreads, boolean interruptThreads, final Answer<List<JobItemBean>> answer)
         throws IOException, InterruptedException, QueueException, NotAuthorizedException,
         BadParam, BusyException {

    ListDelegator delegator = new ListDelegator(config);
    final ListDelegator mockDelegator = Mockito.spy(delegator);

    Mockito.doAnswer(answer).when(mockDelegator).listJobs(Mockito.any(String.class),
                             Mockito.any(boolean.class), Mockito.any(String.class),
                             Mockito.any(int.class), Mockito.any(boolean.class));

    JobRunnable listJobRunnable = new JobRunnable() {
      @Override
      public void run() {
        try {
          int threadId = waitForAllThreadsToStart(this, threadCount);
          LOG.info("Started executing Job List operation. ThreadId : " + threadId);
          mockDelegator.run("admin", true, "", 10, true);
        } catch (Exception ex) {
          exception = ex;
        }
      }
    };

    executeJobOperations(listJobRunnable, threadCount, killThreads, interruptThreads);
    return listJobRunnable;
  }

  public JobRunnable SubmitConcurrentJobs(final int threadCount, AppConfig config,
         final boolean killThreads, boolean interruptThreads, final Answer<Integer> responseAnswer,
         final Answer<QueueStatusBean> timeoutResponseAnswer, final String jobIdResponse)
         throws IOException, InterruptedException, QueueException, NotAuthorizedException,
         BusyException, TimeoutException, Exception {

    LauncherDelegator delegator = new LauncherDelegator(config);
    final LauncherDelegator mockDelegator = Mockito.spy(delegator);
    final List<String> listArgs = new ArrayList<String>();

    TempletonControllerJob mockCtrl = Mockito.mock(TempletonControllerJob.class);

    Mockito.doReturn(jobIdResponse).when(mockCtrl).getSubmittedId();

    Mockito.doReturn(mockCtrl).when(mockDelegator).getTempletonController();

    Mockito.doAnswer(responseAnswer).when(mockDelegator).runTempletonControllerJob(
              Mockito.any(TempletonControllerJob.class), Mockito.any(List.class));

    Mockito.doAnswer(timeoutResponseAnswer).when(mockDelegator).killJob(
              Mockito.any(String.class), Mockito.any(String.class));

    Mockito.doNothing().when(mockDelegator).registerJob(Mockito.any(String.class),
           Mockito.any(String.class), Mockito.any(String.class), Mockito.any(Map.class));

    JobRunnable submitJobRunnable = new JobRunnable() {
      @Override
      public void run() {
        try {
          int threadId = waitForAllThreadsToStart(this, threadCount);
          LOG.info("Started executing Job Submit operation. ThreadId : " + threadId);
          mockDelegator.enqueueController("admin", null, "", listArgs);
        } catch (Throwable ex) {
          exception = ex;
        }
      }
    };

    executeJobOperations(submitJobRunnable, threadCount, killThreads, interruptThreads);
    return submitJobRunnable;
  }

  public void executeJobOperations(JobRunnable jobRunnable, int threadCount, boolean killThreads,
                                   boolean interruptThreads)
    throws IOException, InterruptedException, QueueException, NotAuthorizedException {

    started = false;

    ExecutorService executorService = new ThreadPoolExecutor(threadCount, threadCount, 0L,
        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());;

    ArrayList<Future<?>> futures = new ArrayList<Future<?>>();
    for (int i = 0; i < threadCount; i++) {
      futures.add(executorService.submit(jobRunnable));
    }

    waitForAllThreadsToStart(jobRunnable, threadCount);
    LOG.info("Started all threads ");

    if (killThreads) {
      executorService.shutdownNow();
    } else {
      if (interruptThreads){
        for (Future<?> future : futures) {
          LOG.info("Cancelling the thread");
          future.cancel(true);
        }
      }

      executorService.shutdown();
    }

    /*
     * For both graceful or forceful shutdown, wait for tasks to terminate such that
     * appropriate exceptions are raised and stored in JobRunnable.exception.
     */
    if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
      LOG.info("Force Shutting down the pool\n");
      if (!killThreads) {
        /*
         * killThreads option has already done force shutdown. No need to do again.
         */
        executorService.shutdownNow();
      }
    }
  }

  public abstract class JobRunnable implements Runnable {
    public volatile Throwable exception = null;
    public AtomicInteger threadStartCount = new AtomicInteger(0);
  }
}
