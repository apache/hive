/*
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
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.MockitoAnnotations.initMocks;

import org.apache.hadoop.hive.llap.metrics.LlapDaemonExecutorMetrics;
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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class TestTaskExecutorService {

  @Mock
  private LlapDaemonExecutorMetrics mockMetrics;

  @Before
  public void setUp() {
    initMocks(this);
  }

  @Test(timeout = 10000)
  public void testPreemptionQueueComparator() throws InterruptedException {
    TaskWrapper r1 = createTaskWrapper(
        createSubmitWorkRequestProto(1, 2, 100, 200, false), false, 100000);
    TaskWrapper r2 = createTaskWrapper(
        createSubmitWorkRequestProto(2, 4, 200, 300, false), false, 100000);
    TaskWrapper r3 = createTaskWrapper(
        createSubmitWorkRequestProto(3, 6, 300, 400, false), false, 1000000);
    TaskWrapper r4 = createTaskWrapper(
        createSubmitWorkRequestProto(4, 8, 400, 500, false), false, 1000000);
    TaskWrapper r5 = createTaskWrapper(
        createSubmitWorkRequestProto(5, 2, 100, 200, false), true, 1000000);
    TaskWrapper r6 = createTaskWrapper(
        createSubmitWorkRequestProto(6, 8, 400, 500, true), false, 1000000);
    BlockingQueue<TaskWrapper> queue = new PriorityBlockingQueue<>(6,
        new TaskExecutorService.PreemptionQueueComparator());

    queue.offer(r6);
    queue.offer(r5);
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
    assertEquals(r5, queue.take());
    assertEquals(r6, queue.take());
  }

  org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(TestTaskExecutorService.class);
  @Test(timeout = 20000)
  public void testFinishablePreemptsNonFinishable() throws InterruptedException {
    MockRequest r1 = createMockRequest(1, 1, 100, 200, false, 50000000l, false, 10);
    MockRequest r2 = createMockRequest(2, 1, 100, 200, true, 10000000l, false, 11);
    testPreemptionHelper(r1, r2, true);
    r1 = createMockRequest(1, 1, 100, 200, false, 5000l, true, 12);
    r2 = createMockRequest(2, 1, 100, 200, true, 1000l, true, 13);
    testPreemptionHelper(r1, r2, true);
    // No preemption with ducks reversed.
    r1 = createMockRequest(1, 1, 100, 200, false, 500l, true, 14);
    r2 = createMockRequest(2, 1, 100, 200, true, 1000l, false, 15);
    testPreemptionHelper(r1, r2, false);
    // Same DAG/Vertex tasks should NOT preempt each-other!
    r1 = createMockRequest(1, 1, 100, 200, false, 500l, false, 15);
    r2 = createMockRequest(2, 1, 100, 200, true, 1000l, false, 15);
    testPreemptionHelper(r1, r2, false);
  }

  @Test//(timeout = 10000)
  public void testDuckPreemptsNonDuck() throws InterruptedException {
    MockRequest r1 = createMockRequest(1, 1, 100, 200, true, 5000l, false, 1);
    MockRequest r2 = createMockRequest(2, 1, 100, 200, false, 1000l, true, 2);
    testPreemptionHelper(r1, r2, true);
    r1 = createMockRequest(1, 1, 100, 200, false, 5000l, false, 2);
    r2 = createMockRequest(2, 1, 100, 200, false, 1000l, true, 3);
    testPreemptionHelper(r1, r2, true);
  }

  private void testPreemptionHelper(
      MockRequest r1, MockRequest r2, boolean isPreemted) throws InterruptedException {
    TaskExecutorServiceForTest taskExecutorService = new TaskExecutorServiceForTest(1, 2,
        ShortestJobFirstComparator.class.getName(), true, mockMetrics);
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
      assertEquals(isPreemted, r1.wasPreempted());
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
  public void testPreemptionStateOnTaskFlagChanges() throws InterruptedException {

    MockRequest r1 = createMockRequest(1, 1, 100, 200, false, 20000l, false);
    MockRequest r2 = createMockRequest(2, 1, 100, 200, true, 2000000l, true);

    TaskExecutorServiceForTest taskExecutorService =
        new TaskExecutorServiceForTest(1, 2, ShortestJobFirstComparator.class.getName(), true, mockMetrics);
    taskExecutorService.init(new Configuration());
    taskExecutorService.start();

    try {
      String fragmentId = r1.getRequestId();
      Scheduler.SubmissionState submissionState = taskExecutorService.schedule(r1);
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState);
      awaitStartAndSchedulerRun(r1, taskExecutorService);

      TaskWrapper taskWrapper = taskExecutorService.preemptionQueue.peek();
      assertNotNull(taskWrapper);
      assertTrue(taskWrapper.isInPreemptionQueue());

      // Now notify the executorService that the task has moved to finishable state.
      r1.setCanUpdateFinishable();
      taskWrapper.finishableStateUpdated(true);
      TaskWrapper taskWrapper2 = taskExecutorService.preemptionQueue.peek();
      assertNotNull(taskWrapper2);
      assertTrue(taskWrapper.isInPreemptionQueue());

      // And got a duck.
      boolean result = taskExecutorService.updateFragment(fragmentId, true);
      assertTrue(result);
      taskWrapper2 = taskExecutorService.preemptionQueue.peek();
      assertNull(taskWrapper2);
      assertFalse(taskWrapper.isInPreemptionQueue());

      r1.complete();
      r1.awaitEnd();

      // Now start with everything and test losing stuff.
      fragmentId = r2.getRequestId();
      submissionState = taskExecutorService.schedule(r2);
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState);
      awaitStartAndSchedulerRun(r2, taskExecutorService);

      taskWrapper = taskExecutorService.preemptionQueue.peek();
      assertNull(taskWrapper);

      // Lost the duck.
      result = taskExecutorService.updateFragment(fragmentId, false);
      assertTrue(result);
      taskWrapper = taskExecutorService.preemptionQueue.peek();
      assertNotNull(taskWrapper);
      assertTrue(taskWrapper.isInPreemptionQueue());

      // Gained it again.
      result = taskExecutorService.updateFragment(fragmentId, true);
      assertTrue(result);
      taskWrapper2 = taskExecutorService.preemptionQueue.peek();
      assertNull(taskWrapper2);
      assertFalse(taskWrapper.isInPreemptionQueue());

      // Now lost a finishable state.
      r2.setCanUpdateFinishable();
      taskWrapper.finishableStateUpdated(false);
      taskWrapper2 = taskExecutorService.preemptionQueue.peek();
      assertNotNull(taskWrapper2);
      assertTrue(taskWrapper.isInPreemptionQueue());

      r2.complete();
      r2.awaitEnd();
    } finally {
      taskExecutorService.shutDown(false);
    }
  }

  @Test(timeout = 10000)
  public void testPreemptionQueueOnFinishableStateUpdates() throws InterruptedException {

    long r1WorkTime = 1000L;
    long r2WorkTime = 2000L;
    long r3WorkTime = 2000L;
    // all tasks start with non-finishable state
    MockRequest r1 = createMockRequest(1, 2, 100, 200, false, r1WorkTime, false);
    MockRequest r2 = createMockRequest(2, 1, 100, 200, false, r2WorkTime, false);
    MockRequest r3 = createMockRequest(3, 3, 50, 200, false, r3WorkTime, false);


    TaskExecutorServiceForTest taskExecutorService =
      new TaskExecutorServiceForTest(4, 2, ShortestJobFirstComparator.class.getName(), true, mockMetrics);
    taskExecutorService.init(new Configuration());
    taskExecutorService.start();

    try {
      String fragmentId1 = r1.getRequestId();
      Scheduler.SubmissionState submissionState1 = taskExecutorService.schedule(r1);
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState1);
      awaitStartAndSchedulerRun(r1, taskExecutorService);

      String fragmentId2 = r2.getRequestId();
      Scheduler.SubmissionState submissionState2 = taskExecutorService.schedule(r2);
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState2);
      awaitStartAndSchedulerRun(r2, taskExecutorService);

      String fragmentId3 = r3.getRequestId();
      Scheduler.SubmissionState submissionState3 = taskExecutorService.schedule(r3);
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState3);
      awaitStartAndSchedulerRun(r3, taskExecutorService);

      TaskWrapper taskWrapper = taskExecutorService.preemptionQueue.peek();
      assertNotNull(taskWrapper);
      assertTrue(taskWrapper.isInPreemptionQueue());
      // all tasks are non-finishables, r2 has min tasks
      assertEquals(fragmentId2, taskWrapper.getRequestId());
      assertEquals(3, taskExecutorService.preemptionQueue.size());

      // to let us set the finishable state for tests
      r1.setCanUpdateFinishable();
      r2.setCanUpdateFinishable();
      r3.setCanUpdateFinishable();

      TaskWrapper taskWrapper1 = taskExecutorService.knownTasks.get(fragmentId1);
      TaskWrapper taskWrapper2 = taskExecutorService.knownTasks.get(fragmentId2);
      TaskWrapper taskWrapper3 = taskExecutorService.knownTasks.get(fragmentId3);

      // r2 is finishable now, so it should go to back of pre-emption queue.
      taskExecutorService.finishableStateUpdated(taskWrapper2, true);
      taskWrapper = taskExecutorService.preemptionQueue.peek();
      assertNotNull(taskWrapper);
      assertTrue(taskWrapper.isInPreemptionQueue());
      // r1 is smallest among non-finishables, so should be first in queue
      assertEquals(fragmentId1, taskWrapper.getRequestId());
      assertFalse(taskWrapper.canFinishForPriority());
      assertEquals(3, taskExecutorService.preemptionQueue.size());

      // r1 is finishable now, so it should go to back of pre-emption queue.
      taskExecutorService.finishableStateUpdated(taskWrapper1, true);
      taskWrapper = taskExecutorService.preemptionQueue.peek();
      assertNotNull(taskWrapper);
      assertTrue(taskWrapper.isInPreemptionQueue());
      // r3 is the only non-finishable
      assertEquals(fragmentId3, taskWrapper.getRequestId());
      assertFalse(taskWrapper.canFinishForPriority());
      assertEquals(3, taskExecutorService.preemptionQueue.size());

      // r3 is finishable now, so it should go to back of pre-emption queue.
      taskExecutorService.finishableStateUpdated(taskWrapper3, true);
      taskWrapper = taskExecutorService.preemptionQueue.peek();
      assertNotNull(taskWrapper);
      assertTrue(taskWrapper.isInPreemptionQueue());
      // no more non-finishables left, r2 is smallest among the finishables
      assertEquals(fragmentId2, taskWrapper.getRequestId());
      assertTrue(taskWrapper.canFinishForPriority());
      assertEquals(3, taskExecutorService.preemptionQueue.size());

      // double notification test (nothing should change from the above sequence)
      taskExecutorService.finishableStateUpdated(taskWrapper3, true);taskWrapper = taskExecutorService.preemptionQueue.peek();
      assertNotNull(taskWrapper);
      assertTrue(taskWrapper.isInPreemptionQueue());
      // no more non-finishables left, r2 is smallest among the finishables
      assertEquals(fragmentId2, taskWrapper.getRequestId());
      assertTrue(taskWrapper.canFinishForPriority());
      assertEquals(3, taskExecutorService.preemptionQueue.size());

      // remove r2 from scheduler
      taskExecutorService.killFragment(fragmentId2);

      taskWrapper = taskExecutorService.preemptionQueue.peek();
      assertNotNull(taskWrapper);
      assertTrue(taskWrapper.isInPreemptionQueue());
      // no more non-finishables left, r1 is the smallest among the finishables
      assertEquals(fragmentId1, taskWrapper.getRequestId());
      assertTrue(taskWrapper.canFinishForPriority());
      assertEquals(2, taskExecutorService.preemptionQueue.size());

      // make r3 as non-finishable and make sure its at top of queue
      taskExecutorService.finishableStateUpdated(taskWrapper3, false);
      taskWrapper = taskExecutorService.preemptionQueue.peek();
      assertNotNull(taskWrapper);
      assertTrue(taskWrapper.isInPreemptionQueue());
      // r3 is non-finishable and should be at top
      assertEquals(fragmentId3, taskWrapper.getRequestId());
      assertFalse(taskWrapper.canFinishForPriority());
      assertEquals(2, taskExecutorService.preemptionQueue.size());
      // make sure the task is not added twice to pre-emption queue
      taskExecutorService.tryScheduleUnderLock(taskWrapper);
      assertEquals(2, taskExecutorService.preemptionQueue.size());

      // remove r3 from scheduler
      taskExecutorService.killFragment(fragmentId3);

      taskWrapper = taskExecutorService.preemptionQueue.peek();
      assertNotNull(taskWrapper);
      assertTrue(taskWrapper.isInPreemptionQueue());
      // r1 is the only one left in queue and is finishable
      assertEquals(fragmentId1, taskWrapper.getRequestId());
      assertTrue(taskWrapper.canFinishForPriority());
      assertEquals(1, taskExecutorService.preemptionQueue.size());

      // remove r1 from scheduler
      taskExecutorService.killFragment(fragmentId1);

      // no more left in queue
      taskWrapper = taskExecutorService.preemptionQueue.peek();
      assertNull(taskWrapper);
    } finally {
      taskExecutorService.shutDown(false);
    }
  }

  // Tests wait queue behaviour for fragments which have reported to the AM, but have not given up their executor slot.
  @Test (timeout = 10000)
  public void testWaitQueueAcceptAfterAMTaskReport() throws
      InterruptedException {

    TaskExecutorServiceForTest taskExecutorService =
        new TaskExecutorServiceForTest(1, 2, ShortestJobFirstComparator.class.getName(), true, mockMetrics);

    // Fourth is lower priority as a result of canFinish being set to false.
    MockRequest r1 = createMockRequest(1, 1, 100, 200, true, 20000l, false);
    MockRequest r2 = createMockRequest(2, 1, 1, 200, 2000, true, 20000l, false);
    MockRequest r3 = createMockRequest(3, 1, 2, 300, 420, true, 20000l, false);
    MockRequest r4 = createMockRequest(4, 1, 3, 400, 510, false, 20000l, false);

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
    MockRequest r1 = createMockRequest(1, 1, 100, 200, true, 20000l, false);
    MockRequest r2 = createMockRequest(2, 1, 1,200, 330, false, 20000l, false);
    MockRequest r3 = createMockRequest(3, 2, 2,300, 420, false, 20000l, false);
    MockRequest r4 = createMockRequest(4, 1, 3,400, 510, false, 20000l, false);
    MockRequest r5 = createMockRequest(5, 1, 500, 610, true, 20000l, false);

    testWaitQueuePreemptionHelper(r1, r2, r3, r4, r5);
  }

  @Test(timeout = 10000)
  public void testWaitQueuePreemptionDucks() throws InterruptedException {
    // Throw in some canFinish variations just for fun.
    MockRequest r1 = createMockRequest(1, 1, 100, 200, false, 20000l, true);
    MockRequest r2 = createMockRequest(2, 1, 1,200, 330, true, 20000l, false);
    MockRequest r3 = createMockRequest(3, 2, 2,300, 420, true, 20000l, false);
    MockRequest r4 = createMockRequest(4, 1, 3,400, 510, false, 20000l, false);
    MockRequest r5 = createMockRequest(5, 1, 500, 610, false, 20000l, true);

    testWaitQueuePreemptionHelper(r1, r2, r3, r4, r5);
  }

  private void testWaitQueuePreemptionHelper(MockRequest r1, MockRequest r2,
      MockRequest r3, MockRequest r4, MockRequest r5)
      throws InterruptedException {
    TaskExecutorServiceForTest taskExecutorService =
        new TaskExecutorServiceForTest(1, 2, ShortestJobFirstComparator.class.getName(), true, mockMetrics);
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
    MockRequest victim1 = createMockRequest(1, 1, 100, 100, false, 20000l, false, 1);
    MockRequest victim2 = createMockRequest(2, 1, 100, 100, false, 20000l, false, 2);
    runPreemptionGraceTest(victim1, victim2, 200);
    assertNotEquals(victim1.wasPreempted(), victim2.wasPreempted()); // One and only one.
  }

  @Test(timeout = 10000)
  public void testDoKillMultiple() throws InterruptedException {
    MockRequest victim1 = createMockRequest(1, 1, 100, 100, false, 20000l, false, 1);
    MockRequest victim2 = createMockRequest(2, 1, 100, 100, false, 20000l, false, 2);
    runPreemptionGraceTest(victim1, victim2, 1000);
    assertTrue(victim1.wasPreempted());
    assertTrue(victim2.wasPreempted());
  }

  /**
   * Tests if we can decrease and increase the TaskExecutorService capacity on an active service.
   * Already submitted tasks will not be cancelled or rejected because of this change only if a new
   * task is submitted with higher priority
   * @throws InterruptedException
   */
  @Test(timeout = 10000)
  public void testSetCapacity() throws InterruptedException {
    TaskExecutorServiceForTest taskExecutorService =
        new TaskExecutorServiceForTest(2, 3, ShortestJobFirstComparator.class.getName(), true, mockMetrics);

    // Fourth is lower priority as a result of canFinish being set to false.
    MockRequest r1 = createMockRequest(1, 1, 1, 100, 200, true, 20000L, true);
    MockRequest r2 = createMockRequest(2, 1, 2, 100, 200, true, 20000L, true);
    MockRequest r3 = createMockRequest(3, 1, 3, 100, 200, true, 20000L, true);
    MockRequest r4 = createMockRequest(4, 1, 4, 100, 200, true, 20000L, false);
    MockRequest r5 = createMockRequest(5, 1, 5, 100, 200, true, 20000L, false);
    MockRequest r6 = createMockRequest(6, 1, 6, 100, 200, true, 20000L, false);
    MockRequest r7 = createMockRequest(7, 1, 7, 100, 200, true, 20000L, false);
    MockRequest r8 = createMockRequest(8, 1, 8, 100, 200, true, 20000L, false);
    MockRequest r9 = createMockRequest(9, 1, 9, 100, 200, true, 20000L, false);

    taskExecutorService.init(new Configuration());
    taskExecutorService.start();
    try {
      Scheduler.SubmissionState submissionState;
      TaskExecutorServiceForTest.InternalCompletionListenerForTest icl;

      // Schedule the first 4 tasks (2 to execute, 2 to the queue)
      submissionState = taskExecutorService.schedule(r1);
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState);

      submissionState = taskExecutorService.schedule(r2);
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState);

      submissionState = taskExecutorService.schedule(r3);
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState);

      submissionState = taskExecutorService.schedule(r4);
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState);
      // TaskExecutorService: Executing: r1, r2 + Queued: r3, r4

      awaitStartAndSchedulerRun(r1, taskExecutorService);
      awaitStartAndSchedulerRun(r2, taskExecutorService);

      // Check if the queue and the executing tasks are as expected
      assertEquals(2, taskExecutorService.waitQueue.size());
      assertEquals(0, taskExecutorService.numSlotsAvailable.get());

      // Change the capacity
      taskExecutorService.setCapacity(1, 1);

      // Check that the actual queue size is not changed, but the available executor size is changed
      assertEquals(1, taskExecutorService.waitQueue.waitQueueSize);
      assertEquals(1, taskExecutorService.maxParallelExecutors);
      assertEquals(2, taskExecutorService.waitQueue.size());
      assertEquals(-1, taskExecutorService.numSlotsAvailable.get());

      // Try to schedule one more task, it should be rejected now
      submissionState = taskExecutorService.schedule(r5);
      assertEquals(Scheduler.SubmissionState.REJECTED, submissionState);
      // TaskExecutorService: Executing: r1, r2 + Queued: r3, r4

      // Complete r1
      r1.awaitStart();
      r1.complete();
      r1.awaitEnd();
      icl = taskExecutorService.getInternalCompletionListenerForTest(r1.getRequestId());
      icl.awaitCompletion();
      // TaskExecutorService: Executing: r2 + Queued: r3, r4

      // Check if it is really finished
      assertEquals(2, taskExecutorService.waitQueue.size());
      assertEquals(0, taskExecutorService.numSlotsAvailable.get());

      // Complete r2
      r2.awaitStart();
      r2.complete();
      r2.awaitEnd();
      icl = taskExecutorService.getInternalCompletionListenerForTest(r2.getRequestId());
      icl.awaitCompletion();
      // TaskExecutorService: Executing: r3 + Queued: r4

      // Wait for a scheduling attempt, after that wait queue should be reduced
      awaitStartAndSchedulerRun(r3, taskExecutorService);
      assertEquals(1, taskExecutorService.waitQueue.size());
      assertEquals(0, taskExecutorService.numSlotsAvailable.get());

      // Try to schedule one more task, it still should be rejected
      submissionState = taskExecutorService.schedule(r6);
      assertEquals(Scheduler.SubmissionState.REJECTED, submissionState);
      // TaskExecutorService: Executing: r3 + Queued: r4

      // Complete r3
      r3.complete();
      r3.awaitEnd();
      icl = taskExecutorService.getInternalCompletionListenerForTest(r3.getRequestId());
      icl.awaitCompletion();
      // TaskExecutorService: Executing: r4 + Queued: -

      // Try to schedule one more task, it still should accepted finally
      submissionState = taskExecutorService.schedule(r7);
      // TaskExecutorService: Executing: r4 + Queued: r7
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState);

      // Change back the capacity
      taskExecutorService.setCapacity(2, 3);
      assertEquals(3, taskExecutorService.waitQueue.waitQueueSize);
      assertEquals(2, taskExecutorService.maxParallelExecutors);
      // TaskExecutorService Executing: r4, r7 + Queued: -

      // Wait for a scheduling attempt, the new task should be started
      awaitStartAndSchedulerRun(r7, taskExecutorService);
      assertEquals(0, taskExecutorService.waitQueue.size());
      assertEquals(0, taskExecutorService.numSlotsAvailable.get());

      submissionState = taskExecutorService.schedule(r8);
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState);
      // TaskExecutorService: Executing: r4, r7 + Queued: r8

      submissionState = taskExecutorService.schedule(r9);
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState);
      // TaskExecutorService: Executing: r4, r7 + Queued: r8, r9

      assertEquals(2, taskExecutorService.waitQueue.size());
      assertEquals(0, taskExecutorService.numSlotsAvailable.get());

    } finally {
      taskExecutorService.shutDown(false);
    }
  }

  @Test(timeout = 10000)
  public void testZeroCapacity() throws InterruptedException {
    TaskExecutorServiceForTest taskExecutorService =
        new TaskExecutorServiceForTest(1, 1, ShortestJobFirstComparator.class.getName(), true, mockMetrics);

    // Fourth is lower priority as a result of canFinish being set to false.
    MockRequest r1 = createMockRequest(1, 1, 1, 100, 200, true, 20000L, true);
    MockRequest r2 = createMockRequest(2, 1, 2, 100, 200, true, 20000L, true);

    taskExecutorService.init(new Configuration());
    taskExecutorService.start();

    try {
      Scheduler.SubmissionState submissionState;
      // Schedule the first 2 tasks (1 to execute, 1 to the queue)
      submissionState = taskExecutorService.schedule(r1);
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState);

      submissionState = taskExecutorService.schedule(r2);
      assertEquals(Scheduler.SubmissionState.ACCEPTED, submissionState);

      awaitStartAndSchedulerRun(r1, taskExecutorService);

      taskExecutorService.setCapacity(0, 0);

      // The queued task should be killed
      assertTrue(r2.wasPreempted());

      // The already running should be able to finish
      assertFalse(r1.wasPreempted());
      r1.complete();
      r1.awaitEnd();
      TaskExecutorServiceForTest.InternalCompletionListenerForTest icl =
          taskExecutorService.getInternalCompletionListenerForTest(r1.getRequestId());
      icl.awaitCompletion();
    } finally {
      taskExecutorService.shutDown(false);
    }
  }

  @Test(timeout = 10000, expected = IllegalArgumentException.class)
  public void testSetCapacityHighExecutors() {
    TaskExecutorServiceForTest taskExecutorService =
        new TaskExecutorServiceForTest(2, 3, ShortestJobFirstComparator.class.getName(), true, mockMetrics);
    taskExecutorService.setCapacity(3, 3);
  }

  @Test(timeout = 10000, expected = IllegalArgumentException.class)
  public void testSetCapacityHighQueueSize() {
    TaskExecutorServiceForTest taskExecutorService =
        new TaskExecutorServiceForTest(2, 3, ShortestJobFirstComparator.class.getName(), true, mockMetrics);
    taskExecutorService.setCapacity(2, 5);
  }

  @Test(timeout = 10000, expected = IllegalArgumentException.class)
  public void testSetCapacityNegativeExecutors() {
    TaskExecutorServiceForTest taskExecutorService =
        new TaskExecutorServiceForTest(2, 3, ShortestJobFirstComparator.class.getName(), true, mockMetrics);
    taskExecutorService.setCapacity(-3, 3);
  }

  @Test(timeout = 10000, expected = IllegalArgumentException.class)
  public void testSetCapacityNegativeQueueSize() {
    TaskExecutorServiceForTest taskExecutorService =
        new TaskExecutorServiceForTest(2, 3, ShortestJobFirstComparator.class.getName(), true, mockMetrics);
    taskExecutorService.setCapacity(2, -5);
  }

  @Test(timeout = 10000)
  public void testCapacityMetricsInitial() {
    TaskExecutorServiceForTest taskExecutorService = new TaskExecutorServiceForTest(2, 10,
        ShortestJobFirstComparator.class.getName(), true, mockMetrics);

    verify(mockMetrics).setNumExecutors(2);
    verify(mockMetrics).setWaitQueueSize(10);
  }

  @Test(timeout = 10000)
  public void testCapacityMetricsModification() {
    TaskExecutorServiceForTest taskExecutorService = new TaskExecutorServiceForTest(2, 10,
        ShortestJobFirstComparator.class.getName(), true, mockMetrics);
    reset(mockMetrics);
    taskExecutorService.setCapacity(1, 5);

    verify(mockMetrics).setNumExecutors(1);
    verify(mockMetrics).setWaitQueueSize(5);
  }

  private void runPreemptionGraceTest(
      MockRequest victim1, MockRequest victim2, int time) throws InterruptedException {
    MockRequest preemptor = createMockRequest(3, 1, 100, 100, true, 20000l, false);
    victim1.setSleepAfterKill();
    victim2.setSleepAfterKill();

    ControlledClock clock = new ControlledClock(new SystemClock());
    clock.setTime(0);
    TaskExecutorServiceForTest taskExecutorService = new TaskExecutorServiceForTest(
        2, 3, ShortestJobFirstComparator.class.getName(), true, mockMetrics, clock);
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
        String waitQueueComparatorClassName, boolean enablePreemption, LlapDaemonExecutorMetrics metrics) {
      this(numExecutors, waitQueueSize, waitQueueComparatorClassName, enablePreemption, metrics, null);
    }

    public TaskExecutorServiceForTest(int numExecutors, int waitQueueSize,
        String waitQueueComparatorClassName, boolean enablePreemption, LlapDaemonExecutorMetrics metrics, Clock clock) {
      super(numExecutors, waitQueueSize, waitQueueComparatorClassName, enablePreemption,
              Thread.currentThread().getContextClassLoader(), metrics, clock);
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
