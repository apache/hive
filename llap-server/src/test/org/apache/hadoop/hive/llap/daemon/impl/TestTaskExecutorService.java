/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.daemon.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.daemon.FragmentCompletionHandler;
import org.apache.hadoop.hive.llap.daemon.KilledTaskHandler;
import org.apache.hadoop.hive.llap.daemon.impl.TaskExecutorService.TaskWrapper;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.EntityDescriptorProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.FragmentSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorMetrics;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.task.EndReason;
import org.apache.tez.runtime.task.TaskRunner2Result;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTaskExecutorService {
  private static Configuration conf;
  private static Credentials cred = new Credentials();
  private static final Logger LOG = LoggerFactory.getLogger(TestTaskExecutorService.class);

  @Before
  public void setup() {
    conf = new Configuration();
  }

  @Test(timeout = 5000)
  public void testWaitQueueComparator() throws InterruptedException {
    TaskWrapper r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 2, 100), false, 100000);
    TaskWrapper r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 4, 200), false, 100000);
    TaskWrapper r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 6, 300), false, 1000000);
    TaskWrapper r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 8, 400), false, 1000000);
    TaskWrapper r5 = createTaskWrapper(createSubmitWorkRequestProto(5, 10, 500), false, 1000000);
    EvictingPriorityBlockingQueue<TaskWrapper> queue = new EvictingPriorityBlockingQueue<>(
        new TaskExecutorService.ShortestJobFirstComparator(), 4);
    assertNull(queue.offer(r1));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r3));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r4));
    assertEquals(r1, queue.peek());
    // this offer will be rejected
    assertEquals(r5, queue.offer(r5));
    assertEquals(r1, queue.take());
    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r4, queue.take());

    r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 2, 100), true, 100000);
    r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 4, 200), true, 100000);
    r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 6, 300), true, 1000000);
    r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 8, 400), true, 1000000);
    r5 = createTaskWrapper(createSubmitWorkRequestProto(5, 10, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue(
        new TaskExecutorService.ShortestJobFirstComparator(), 4);
    assertNull(queue.offer(r1));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r3));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r4));
    assertEquals(r1, queue.peek());
    // this offer will be rejected
    assertEquals(r5, queue.offer(r5));
    assertEquals(r1, queue.take());
    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r4, queue.take());

    r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 1, 100), true, 100000);
    r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 1, 200), false, 100000);
    r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 1, 300), true, 1000000);
    r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 1, 400), false, 1000000);
    r5 = createTaskWrapper(createSubmitWorkRequestProto(5, 10, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue(
        new TaskExecutorService.ShortestJobFirstComparator(), 4);
    assertNull(queue.offer(r1));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r3));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r4));
    assertEquals(r1, queue.peek());
    // offer accepted and r4 gets evicted
    assertEquals(r4, queue.offer(r5));
    assertEquals(r1, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r5, queue.take());
    assertEquals(r2, queue.take());

    r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 2, 100), true, 100000);
    r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 4, 200), false, 100000);
    r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 6, 300), true, 1000000);
    r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 8, 400), false, 1000000);
    r5 = createTaskWrapper(createSubmitWorkRequestProto(5, 10, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue(
        new TaskExecutorService.ShortestJobFirstComparator(), 4);
    assertNull(queue.offer(r1));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r3));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r4));
    assertEquals(r1, queue.peek());
    // offer accepted and r4 gets evicted
    assertEquals(r4, queue.offer(r5));
    assertEquals(r1, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r5, queue.take());
    assertEquals(r2, queue.take());

    r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 2, 100), true, 100000);
    r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 4, 200), false, 100000);
    r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 6, 300), false, 1000000);
    r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 8, 400), false, 1000000);
    r5 = createTaskWrapper(createSubmitWorkRequestProto(5, 10, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue(
        new TaskExecutorService.ShortestJobFirstComparator(), 4);
    assertNull(queue.offer(r1));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r3));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r4));
    assertEquals(r1, queue.peek());
    // offer accepted and r4 gets evicted
    assertEquals(r4, queue.offer(r5));
    assertEquals(r1, queue.take());
    assertEquals(r5, queue.take());
    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());

    r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 2, 100), false, 100000);
    r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 4, 200), true, 100000);
    r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 6, 300), true, 1000000);
    r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 8, 400), true, 1000000);
    r5 = createTaskWrapper(createSubmitWorkRequestProto(5, 10, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue(
        new TaskExecutorService.ShortestJobFirstComparator(), 4);
    assertNull(queue.offer(r1));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2));
    assertEquals(r2, queue.peek());
    assertNull(queue.offer(r3));
    assertEquals(r2, queue.peek());
    assertNull(queue.offer(r4));
    assertEquals(r2, queue.peek());
    // offer accepted, r1 evicted
    assertEquals(r1, queue.offer(r5));
    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r4, queue.take());
    assertEquals(r5, queue.take());
  }

  @Test(timeout = 5000)
  public void testPreemptionQueueComparator() throws InterruptedException {
    TaskWrapper r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 2, 100), false, 100000);
    TaskWrapper r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 4, 200), false, 100000);
    TaskWrapper r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 6, 300), false, 1000000);
    TaskWrapper r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 8, 400), false, 1000000);
    BlockingQueue<TaskWrapper> queue = new PriorityBlockingQueue<>(4,
        new TaskExecutorService.PreemptionQueueComparator());

    queue.offer(r1);
    assertEquals(r1, queue.peek());
    queue.offer(r2);
    assertEquals(r1, queue.peek());
    queue.offer(r3);
    assertEquals(r1, queue.peek());
    queue.offer(r4);
    assertEquals(r1, queue.take());
    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r4, queue.take());
  }

  @Test(timeout = 10000)
  public void testFinishablePreeptsNonFinishable() throws InterruptedException {
    MockRequest r1 = createMockRequest(1, 1, 100, false, 5000l);
    MockRequest r2 = createMockRequest(2, 1, 100, true, 1000l);
    TaskExecutorServiceForTest taskExecutorService = new TaskExecutorServiceForTest(1, 2, false, true);
    taskExecutorService.init(conf);
    taskExecutorService.start();

    try {
      taskExecutorService.schedule(r1);
      r1.awaitStart();
      taskExecutorService.schedule(r2);
      r2.awaitStart();
      // Verify r1 was preempted. Also verify that it finished (single executor), otherwise
      // r2 could have run anyway.
      assertTrue(r1.wasPreempted());
      assertTrue(r1.hasFinished());

      r2.complete();
      r2.awaitEnd();

      TaskExecutorServiceForTest.InternalCompletionListenerForTest icl1 =
          taskExecutorService.getInternalCompletionListenerForTest(r1.getRequestId());
      TaskExecutorServiceForTest.InternalCompletionListenerForTest icl2 =
          taskExecutorService.getInternalCompletionListenerForTest(r2.getRequestId());

      // Ensure Data structures are updated in the main TaskScheduler
      icl1.awaitCompletion();
      icl2.awaitCompletion();

      assertEquals(0, taskExecutorService.knownTasks.size());
    } finally {
      taskExecutorService.shutDown(false);
    }
  }


  // ----------- Helper classes and methods go after this point. Tests above this -----------

  private SubmitWorkRequestProto createSubmitWorkRequestProto(int fragmentNumber, int parallelism,
                                                              long attemptStartTime) {
    ApplicationId appId = ApplicationId.newInstance(9999, 72);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vId = TezVertexID.getInstance(dagId, 35);
    TezTaskID tId = TezTaskID.getInstance(vId, 389);
    TezTaskAttemptID taId = TezTaskAttemptID.getInstance(tId, fragmentNumber);
    return SubmitWorkRequestProto
        .newBuilder()
        .setFragmentSpec(
            FragmentSpecProto
                .newBuilder()
                .setAttemptNumber(0)
                .setDagName("MockDag")
                .setFragmentNumber(fragmentNumber)
                .setVertexName("MockVertex")
                .setVertexParallelism(parallelism)
                .setProcessorDescriptor(
                    EntityDescriptorProto.newBuilder().setClassName("MockProcessor").build())
                .setFragmentIdentifierString(taId.toString()).build()).setAmHost("localhost")
        .setAmPort(12345).setAppAttemptNumber(0).setApplicationIdString("MockApp_1")
        .setContainerIdString("MockContainer_1").setUser("MockUser")
        .setTokenIdentifier("MockToken_1")
        .setFragmentRuntimeInfo(LlapDaemonProtocolProtos
            .FragmentRuntimeInfo
            .newBuilder()
            .setFirstAttemptStartTime(attemptStartTime)
            .build())
        .build();
  }

  private MockRequest createMockRequest(int fragmentNum, int parallelism, long startTime,
                                        boolean canFinish, long workTime) {
    SubmitWorkRequestProto requestProto = createSubmitWorkRequestProto(fragmentNum, parallelism,
        startTime);
    MockRequest mockRequest = new MockRequest(requestProto, canFinish, workTime);
    return mockRequest;
  }

  private TaskWrapper createTaskWrapper(SubmitWorkRequestProto request, boolean canFinish, int workTime) {
    MockRequest mockRequest = new MockRequest(request, canFinish, workTime);
    TaskWrapper taskWrapper = new TaskWrapper(mockRequest, null);
    return taskWrapper;
  }

  private static void logInfo(String message, Throwable t) {
    LOG.info(message, t);
  }

  private static void logInfo(String message) {
    logInfo(message, null);
  }

  private static class MockRequest extends TaskRunnerCallable {
    private final long workTime;
    private final boolean canFinish;

    private final AtomicBoolean isStarted = new AtomicBoolean(false);
    private final AtomicBoolean isFinished = new AtomicBoolean(false);
    private final AtomicBoolean wasKilled = new AtomicBoolean(false);
    private final AtomicBoolean wasInterrupted = new AtomicBoolean(false);

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition startedCondition = lock.newCondition();
    private final Condition sleepCondition = lock.newCondition();
    private final Condition finishedCondition = lock.newCondition();

    public MockRequest(LlapDaemonProtocolProtos.SubmitWorkRequestProto requestProto,
                       boolean canFinish, long workTime) {
      super(requestProto, mock(QueryFragmentInfo.class), conf,
          new ExecutionContextImpl("localhost"), null, cred, 0, null, null, mock(
              LlapDaemonExecutorMetrics.class),
          mock(KilledTaskHandler.class), mock(
              FragmentCompletionHandler.class));
      this.workTime = workTime;
      this.canFinish = canFinish;
    }

    @Override
    protected TaskRunner2Result callInternal() {
      try {
        logInfo(super.getRequestId() + " is executing..", null);
        lock.lock();
        try {
          isStarted.set(true);
          startedCondition.signal();
        } finally {
          lock.unlock();
        }

        lock.lock();
        try {
          sleepCondition.await(workTime, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          wasInterrupted.set(true);
          return new TaskRunner2Result(EndReason.KILL_REQUESTED, null, false);
        } finally {
          lock.unlock();
        }
        if (wasKilled.get()) {
          return new TaskRunner2Result(EndReason.KILL_REQUESTED, null, false);
        } else {
          return new TaskRunner2Result(EndReason.SUCCESS, null, false);
        }
      } finally {
        lock.lock();
        try {
          isFinished.set(true);
          finishedCondition.signal();
        } finally {
          lock.unlock();
        }
      }
    }

    @Override
    public void killTask() {
      lock.lock();
      try {
        wasKilled.set(true);
        sleepCondition.signal();
      } finally {
        lock.unlock();
      }
    }

    boolean hasStarted() {
      return isStarted.get();
    }

    boolean hasFinished() {
      return isFinished.get();
    }

    boolean wasPreempted() {
      return wasKilled.get();
    }

    void complete() {
      lock.lock();
      try {
        sleepCondition.signal();
      } finally {
        lock.unlock();
      }
    }

    void awaitStart() throws InterruptedException {
      lock.lock();
      try {
        while (!isStarted.get()) {
          startedCondition.await();
        }
      } finally {
        lock.unlock();
      }
    }

    void awaitEnd() throws InterruptedException {
      lock.lock();
      try {
        while (!isFinished.get()) {
          finishedCondition.await();
        }
      } finally {
        lock.unlock();
      }
    }


    @Override
    public boolean canFinish() {
      return canFinish;
    }
  }

  private static class TaskExecutorServiceForTest extends TaskExecutorService {
    public TaskExecutorServiceForTest(int numExecutors, int waitQueueSize, boolean useFairOrdering,
                                      boolean enablePreemption) {
      super(numExecutors, waitQueueSize, useFairOrdering, enablePreemption);
    }

    private ConcurrentMap<String, InternalCompletionListenerForTest> completionListeners = new ConcurrentHashMap<>();

    InternalCompletionListener createInternalCompletionListener(TaskWrapper taskWrapper) {
      InternalCompletionListenerForTest icl = new InternalCompletionListenerForTest(taskWrapper);
      completionListeners.put(taskWrapper.getRequestId(), icl);
      return icl;
    }

    InternalCompletionListenerForTest getInternalCompletionListenerForTest(String requestId) {
      return completionListeners.get(requestId);
    }


    private class InternalCompletionListenerForTest extends TaskExecutorService.InternalCompletionListener {

      private final Lock lock = new ReentrantLock();
      private final Condition completionCondition = lock.newCondition();
      private final AtomicBoolean isComplete = new AtomicBoolean(false);

      public InternalCompletionListenerForTest(TaskWrapper taskWrapper) {
        super(taskWrapper);
      }

      @Override
      public void onSuccess(TaskRunner2Result result) {
        super.onSuccess(result);
        markComplete();
      }

      @Override
      public void onFailure(Throwable t) {
        super.onFailure(t);
        markComplete();
      }

      private void markComplete() {
        lock.lock();
        try {
          isComplete.set(true);
          completionCondition.signal();
        } finally {
          lock.unlock();
        }
      }

      private void awaitCompletion() throws InterruptedException {
        lock.lock();
        try {
          while (!isComplete.get()) {
            completionCondition.await();
          }
        } finally {
          lock.unlock();
        }
      }
    }
  }
}
