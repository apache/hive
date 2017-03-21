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

import static org.apache.hadoop.hive.llap.daemon.impl.TaskExecutorTestHelpers.createMockRequest;
import static org.apache.hadoop.hive.llap.daemon.impl.TaskExecutorTestHelpers.createSubmitWorkRequestProto;
import static org.apache.hadoop.hive.llap.daemon.impl.TaskExecutorTestHelpers.createTaskWrapper;
import static org.junit.Assert.*;

import org.apache.hadoop.yarn.util.SystemClock;

import org.apache.hadoop.hive.llap.testhelpers.ControlledClock;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.yarn.util.Clock;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.daemon.SchedulerFragmentCompletingListener;
import org.apache.hadoop.hive.llap.daemon.impl.TaskExecutorService.TaskWrapper;
import org.apache.hadoop.hive.llap.daemon.impl.TaskExecutorTestHelpers.MockRequest;
import org.apache.hadoop.hive.llap.daemon.impl.comparator.ShortestJobFirstComparator;
import org.apache.tez.runtime.task.TaskRunner2Result;
import org.junit.Test;

public class TestTaskExecutorService {

  @Test(timeout = 5000)
  public void testPreemptionQueueComparator() throws InterruptedException {
    TaskWrapper r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 2, 100, 200), false, 100000);
    TaskWrapper r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 4, 200, 300), false, 100000);
    TaskWrapper r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 6, 300, 400), false, 1000000);
    TaskWrapper r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 8, 400, 500), false, 1000000);
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
    MockRequest r1 = createMockRequest(1, 1, 100, 200, false, 5000l);
    MockRequest r2 = createMockRequest(2, 1, 100, 200, true, 1000l);
    TaskExecutorServiceForTest taskExecutorService = new TaskExecutorServiceForTest(1, 2,
        ShortestJobFirstComparator.class.getName(), true);
    taskExecutorService.init(new Configuration());
    taskExecutorService.start();

    try {
      taskExecutorService.schedule(r1);
      awaitStartAndSchedulerRun(r1, taskExecutorService);
      taskExecutorService.schedule(r2);
      awaitStartAndSchedulerRun(r2, taskExecutorService);
      // Verify r1 was preempted. Also verify that it finished (single executor), otherwise
      // r2 could have run anyway.
      r1.awaitEnd();
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

  @Test(timeout = 10000)
  public void testPreemptionStateOnTaskMoveToFinishableState() throws InterruptedException {

    MockRequest r1 = createMockRequest(1, 1, 100, 200, false, 20000l);

    TaskExecutorServiceForTest taskExecutorService =
        new TaskExecutorServiceForTest(1, 2, ShortestJobFirstComparator.class.getName(), true);
    taskExecutorService.init(new Configuration());
    taskExecutorService.start();

    try {
      Scheduler.SubmissionState submissionState = taskExecutorService.schedule(r1);
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState);
      awaitStartAndSchedulerRun(r1, taskExecutorService);

      TaskWrapper taskWrapper = taskExecutorService.preemptionQueue.peek();
      assertNotNull(taskWrapper);
      assertTrue(taskWrapper.isInPreemptionQueue());

      // Now notify the executorService that the task has moved to finishable state.
      taskWrapper.finishableStateUpdated(true);
      TaskWrapper taskWrapper2 = taskExecutorService.preemptionQueue.peek();
      assertNull(taskWrapper2);
      assertFalse(taskWrapper.isInPreemptionQueue());

      r1.complete();
      r1.awaitEnd();
    } finally {
      taskExecutorService.shutDown(false);
    }
  }

  @Test(timeout = 10000)
  public void testPreemptionStateOnTaskMoveToNonFinishableState() throws InterruptedException {

    MockRequest r1 = createMockRequest(1, 1, 100, 200, true, 20000l);

    TaskExecutorServiceForTest taskExecutorService =
        new TaskExecutorServiceForTest(1, 2, ShortestJobFirstComparator.class.getName(), true);
    taskExecutorService.init(new Configuration());
    taskExecutorService.start();

    try {
      Scheduler.SubmissionState submissionState = taskExecutorService.schedule(r1);
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState);
      awaitStartAndSchedulerRun(r1, taskExecutorService);

      TaskWrapper taskWrapper = taskExecutorService.preemptionQueue.peek();
      assertNull(taskWrapper);
      assertEquals(1, taskExecutorService.knownTasks.size());
      taskWrapper = taskExecutorService.knownTasks.entrySet().iterator().next().getValue();
      assertFalse(taskWrapper.isInPreemptionQueue());

      // Now notify the executorService that the task has moved to finishable state.
      taskWrapper.finishableStateUpdated(false);
      TaskWrapper taskWrapper2 = taskExecutorService.preemptionQueue.peek();
      assertNotNull(taskWrapper2);
      assertTrue(taskWrapper2.isInPreemptionQueue());
      assertEquals(taskWrapper, taskWrapper2);

      r1.complete();
      r1.awaitEnd();
    } finally {
      taskExecutorService.shutDown(false);
    }
  }

  // Tests wait queue behaviour for fragments which have reported to the AM, but have not given up their executor slot.
  @Test (timeout = 10000)
  public void testWaitQueueAcceptAfterAMTaskReport() throws
      InterruptedException {

    TaskExecutorServiceForTest taskExecutorService =
        new TaskExecutorServiceForTest(1, 2, ShortestJobFirstComparator.class.getName(), true);

    // Fourth is lower priority as a result of canFinish being set to false.
    MockRequest r1 = createMockRequest(1, 1, 100, 200, true, 20000l);
    MockRequest r2 = createMockRequest(2, 1, 1, 200, 2000, true, 20000l);
    MockRequest r3 = createMockRequest(3, 1, 2, 300, 420, true, 20000l);
    MockRequest r4 = createMockRequest(4, 1, 3, 400, 510, false, 20000l);

    taskExecutorService.init(new Configuration());
    taskExecutorService.start();
    try {
      Scheduler.SubmissionState submissionState;
      submissionState = taskExecutorService.schedule(r1);
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState);
      r1.awaitStart();

      submissionState = taskExecutorService.schedule(r2);
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState);

      submissionState = taskExecutorService.schedule(r3);
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState);

      submissionState = taskExecutorService.schedule(r4);
      assertEquals(Scheduler.SubmissionState.REJECTED, submissionState);

      // Mark a fragment as completing, but don't actually complete it yet.
      // The wait queue should now have capacity to accept one more fragment.
      taskExecutorService.fragmentCompleting(r1.getRequestId(),
          SchedulerFragmentCompletingListener.State.SUCCESS);

      submissionState = taskExecutorService.schedule(r4);
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState);

      assertEquals(3, taskExecutorService.waitQueue.size());
      assertEquals(1, taskExecutorService.completingFragmentMap.size());

      r1.complete();
      r1.awaitEnd();
      // r2 can only start once 1 fragment has completed. the map should be clear at this point.
      awaitStartAndSchedulerRun(r2, taskExecutorService);
      assertEquals(0, taskExecutorService.completingFragmentMap.size());

    } finally {
      taskExecutorService.shutDown(false);
    }
  }

  @Test(timeout = 10000)
  public void testWaitQueuePreemption() throws InterruptedException {
    MockRequest r1 = createMockRequest(1, 1, 100, 200, true, 20000l);
    MockRequest r2 = createMockRequest(2, 1, 1,200, 330, false, 20000l);
    MockRequest r3 = createMockRequest(3, 2, 2,300, 420, false, 20000l);
    MockRequest r4 = createMockRequest(4, 1, 3,400, 510, false, 20000l);
    MockRequest r5 = createMockRequest(5, 1, 500, 610, true, 20000l);

    TaskExecutorServiceForTest taskExecutorService =
        new TaskExecutorServiceForTest(1, 2, ShortestJobFirstComparator.class.getName(), true);
    taskExecutorService.init(new Configuration());
    taskExecutorService.start();

    try {
      taskExecutorService.schedule(r1);

      // 1 scheduling run will happen, which may or may not pick up this task in the test..
      awaitStartAndSchedulerRun(r1, taskExecutorService);
      Scheduler.SubmissionState submissionState = taskExecutorService.schedule(r2);
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState);

      submissionState = taskExecutorService.schedule(r3);
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState);

      submissionState = taskExecutorService.schedule(r4);
      assertEquals(Scheduler.SubmissionState.REJECTED, submissionState);

      submissionState = taskExecutorService.schedule(r5);
        assertEquals(Scheduler.SubmissionState.EVICTED_OTHER, submissionState);

      // Ensure the correct task was preempted.
      assertEquals(true, r3.wasPreempted());

      TaskExecutorServiceForTest.InternalCompletionListenerForTest icl1 =
          taskExecutorService.getInternalCompletionListenerForTest(r1.getRequestId());

      // Currently 3 known tasks. 1, 2, 5
      assertEquals(3, taskExecutorService.knownTasks.size());
      assertTrue(taskExecutorService.knownTasks.containsKey(r1.getRequestId()));
      assertTrue(taskExecutorService.knownTasks.containsKey(r2.getRequestId()));
      assertTrue(taskExecutorService.knownTasks.containsKey(r5.getRequestId()));

      r1.complete();
      r1.awaitEnd();
      icl1.awaitCompletion();

      // Two known tasks left. r2 and r5. (r1 complete, r3 evicted, r4 rejected)
      assertEquals(2, taskExecutorService.knownTasks.size());
      assertTrue(taskExecutorService.knownTasks.containsKey(r2.getRequestId()));
      assertTrue(taskExecutorService.knownTasks.containsKey(r5.getRequestId()));

      awaitStartAndSchedulerRun(r5, taskExecutorService);
      TaskExecutorServiceForTest.InternalCompletionListenerForTest icl5 =
          taskExecutorService.getInternalCompletionListenerForTest(r5.getRequestId());
      r5.complete();
      r5.awaitEnd();
      icl5.awaitCompletion();

      // 1 Pending task which is not finishable
      assertEquals(1, taskExecutorService.knownTasks.size());
      assertTrue(taskExecutorService.knownTasks.containsKey(r2.getRequestId()));

      awaitStartAndSchedulerRun(r2, taskExecutorService);
      TaskExecutorServiceForTest.InternalCompletionListenerForTest icl2 =
          taskExecutorService.getInternalCompletionListenerForTest(r2.getRequestId());
      r2.complete();
      r2.awaitEnd();
      icl2.awaitCompletion();
      // 0 Pending task which is not finishable
      assertEquals(0, taskExecutorService.knownTasks.size());
    } finally {
      taskExecutorService.shutDown(false);
    }
  }

  @Test(timeout = 10000)
  public void testDontKillMultiple() throws InterruptedException {
    MockRequest victim1 = createMockRequest(1, 1, 100, 100, false, 20000l);
    MockRequest victim2 = createMockRequest(2, 1, 100, 100, false, 20000l);
    runPreemptionGraceTest(victim1, victim2, 200);
    assertNotEquals(victim1.wasPreempted(), victim2.wasPreempted()); // One and only one.
  }

  @Test(timeout = 10000)
  public void testDoKillMultiple() throws InterruptedException {
    MockRequest victim1 = createMockRequest(1, 1, 100, 100, false, 20000l);
    MockRequest victim2 = createMockRequest(2, 1, 100, 100, false, 20000l);
    runPreemptionGraceTest(victim1, victim2, 1000);
    assertTrue(victim1.wasPreempted());
    assertTrue(victim2.wasPreempted());
  }

  private void runPreemptionGraceTest(
      MockRequest victim1, MockRequest victim2, int time) throws InterruptedException {
    MockRequest preemptor = createMockRequest(3, 1, 100, 100, true, 20000l);
    victim1.setSleepAfterKill();
    victim2.setSleepAfterKill();

    ControlledClock clock = new ControlledClock(new SystemClock());
    clock.setTime(0);
    TaskExecutorServiceForTest taskExecutorService = new TaskExecutorServiceForTest(
        2, 3, ShortestJobFirstComparator.class.getName(), true, clock);
    taskExecutorService.init(new Configuration());
    taskExecutorService.start();

    try {
      taskExecutorService.schedule(victim1);
      awaitStartAndSchedulerRun(victim1, taskExecutorService);
      taskExecutorService.schedule(victim2);
      awaitStartAndSchedulerRun(victim2, taskExecutorService);
      taskExecutorService.schedule(preemptor);
      taskExecutorService.waitForScheduleRuns(5); // Wait for scheduling to run a few times.
      clock.setTime(time);
      taskExecutorService.waitForScheduleRuns(5); // Wait for scheduling to run a few times.
      victim1.unblockKill();
      victim2.unblockKill();
      preemptor.complete();
      preemptor.awaitEnd();
      TaskExecutorServiceForTest.InternalCompletionListenerForTest icl3 =
          taskExecutorService.getInternalCompletionListenerForTest(preemptor.getRequestId());
      icl3.awaitCompletion();
    } finally {
      taskExecutorService.shutDown(false);
    }
  }



  private void awaitStartAndSchedulerRun(MockRequest mockRequest,
                                         TaskExecutorServiceForTest taskExecutorServiceForTest) throws
      InterruptedException {
    mockRequest.awaitStart();
    taskExecutorServiceForTest.awaitTryScheduleIfInProgress();
  }

  private static class TaskExecutorServiceForTest extends TaskExecutorService {

    private final Lock iclCreationLock = new ReentrantLock();
    private final Map<String, Condition> iclCreationConditions = new HashMap<>();

    private final Lock tryScheduleLock = new ReentrantLock();
    private final Condition tryScheduleCondition = tryScheduleLock.newCondition();
    private boolean isInTrySchedule = false;
    private int scheduleAttempts = 0;

    public TaskExecutorServiceForTest(int numExecutors, int waitQueueSize,
        String waitQueueComparatorClassName, boolean enablePreemption) {
      this(numExecutors, waitQueueSize, waitQueueComparatorClassName, enablePreemption, null);
    }

    public TaskExecutorServiceForTest(int numExecutors, int waitQueueSize,
        String waitQueueComparatorClassName, boolean enablePreemption, Clock clock) {
      super(numExecutors, waitQueueSize, waitQueueComparatorClassName, enablePreemption,
          Thread.currentThread().getContextClassLoader(), null, clock);
    }

    private ConcurrentMap<String, InternalCompletionListenerForTest> completionListeners =
        new ConcurrentHashMap<>();

    @Override
    void tryScheduleUnderLock(final TaskWrapper taskWrapper) throws RejectedExecutionException {
      tryScheduleLock.lock();
      try {
        isInTrySchedule = true;
        super.tryScheduleUnderLock(taskWrapper);
      } finally {
        isInTrySchedule = false;
        ++scheduleAttempts;
        tryScheduleCondition.signal();
        tryScheduleLock.unlock();
      }
    }

    public void waitForScheduleRuns(int n) throws InterruptedException {
      tryScheduleLock.lock();
      try {
        int targetRuns = scheduleAttempts + n;
        while (scheduleAttempts < targetRuns) {
          tryScheduleCondition.await(100, TimeUnit.MILLISECONDS);
        }
      } finally {
        tryScheduleLock.unlock();
      }
    }

    private void awaitTryScheduleIfInProgress() throws InterruptedException {
      tryScheduleLock.lock();
      try {
        while (isInTrySchedule) {
          tryScheduleCondition.await();
        }
      } finally {
        tryScheduleLock.unlock();
      }
    }

    @Override
    InternalCompletionListener createInternalCompletionListener(TaskWrapper taskWrapper) {
      iclCreationLock.lock();
      try {
        InternalCompletionListenerForTest icl = new InternalCompletionListenerForTest(taskWrapper);
        completionListeners.put(taskWrapper.getRequestId(), icl);
        Condition condition = iclCreationConditions.get(taskWrapper.getRequestId());
        if (condition == null) {
          condition = iclCreationLock.newCondition();
          iclCreationConditions.put(taskWrapper.getRequestId(), condition);
        }
        condition.signalAll();
        return icl;
      } finally {
        iclCreationLock.unlock();
      }
    }

    InternalCompletionListenerForTest getInternalCompletionListenerForTest(String requestId) throws
        InterruptedException {
      iclCreationLock.lock();
      try {
        Condition condition = iclCreationConditions.get(requestId);
        if (condition == null) {
          condition = iclCreationLock.newCondition();
          iclCreationConditions.put(requestId, condition);
        }
        while (completionListeners.get(requestId) == null) {
          condition.await();
        }
        return completionListeners.get(requestId);
      } finally {
        iclCreationLock.unlock();
      }
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
