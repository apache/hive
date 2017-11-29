/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.llap.tezplugins;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import org.apache.tez.dag.records.TezDAGID;

import org.apache.tez.dag.records.TezVertexID;

import org.apache.tez.dag.records.TezTaskID;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstance;
import org.apache.hadoop.hive.llap.registry.LlapServiceInstanceSet;
import org.apache.hadoop.hive.llap.registry.impl.InactiveServiceInstance;
import org.apache.hadoop.hive.llap.registry.impl.LlapFixedRegistryImpl;
import org.apache.hadoop.hive.llap.testhelpers.ControlledClock;
import org.apache.hadoop.hive.llap.tezplugins.LlapTaskSchedulerService.TaskInfo;
import org.apache.hadoop.hive.llap.tezplugins.LlapTaskSchedulerService.TaskInfo.State;
import org.apache.hadoop.hive.llap.tezplugins.helpers.MonotonicClock;
import org.apache.hadoop.util.Time;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

public class TestLlapTaskSchedulerService {

  private static final Logger LOG = LoggerFactory.getLogger(TestLlapTaskSchedulerService.class);

  private static final String HOST1 = "host1";
  private static final String HOST2 = "host2";
  private static final String HOST3 = "host3";
  private static final String HOST4 = "host4";

  @Test(timeout = 10000)
  public void testSimpleLocalAllocation() throws IOException, InterruptedException {

    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper();

    try {
      Priority priority1 = Priority.newInstance(1);
      String[] hosts1 = new String[]{HOST1};

      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie1 = new Object();

      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, hosts1, priority1, clientCookie1);

      tsWrapper.awaitLocalTaskAllocations(1);

      verify(tsWrapper.mockAppCallback).taskAllocated(eq(task1), eq(clientCookie1), any(Container.class));
      assertEquals(1, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(1, tsWrapper.ts.dagStats.numAllocationsPerHost.get(HOST1).get());
    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testGuaranteedScheduling() throws IOException, InterruptedException {
    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper();
    // Schedule a task - it should get the only duck; the 2nd one at the same pri doesn't get one.
    // When the first one finishes, the duck goes to the 2nd, and then becomes unused.
    try {
      Priority priority = Priority.newInstance(1);
      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId(), task2 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie1 = new Object(), clientCookie2 = new Object();
      tsWrapper.ts.updateGuaranteedCount(1);
      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, null, priority, clientCookie1);
      tsWrapper.awaitTotalTaskAllocations(1);
      TaskInfo ti = tsWrapper.ts.getTaskInfo(task1);
      assertTrue(ti.isGuaranteed());
      assertEquals(State.ASSIGNED, ti.getState());
      assertEquals(0, tsWrapper.ts.getUnusedGuaranteedCount());

      tsWrapper.allocateTask(task2, null, priority, clientCookie2);
      tsWrapper.awaitTotalTaskAllocations(2);
      TaskInfo ti2 = tsWrapper.ts.getTaskInfo(task2);
      assertFalse(ti2.isGuaranteed());
      assertEquals(0, tsWrapper.ts.getUnusedGuaranteedCount());

      tsWrapper.deallocateTask(task1, true, TaskAttemptEndReason.CONTAINER_EXITED);
      assertTrue(ti2.isGuaranteed());
      assertEquals(0, tsWrapper.ts.getUnusedGuaranteedCount());


      tsWrapper.deallocateTask(task2, true, TaskAttemptEndReason.CONTAINER_EXITED);
      assertEquals(1, tsWrapper.ts.getUnusedGuaranteedCount());
    } finally {
      tsWrapper.shutdown();
    }
  }


  @Test(timeout = 10000)
  public void testGuaranteedTransfer() throws IOException, InterruptedException {
    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper();
    // Schedule low pri first. When high pri is scheduled, it takes away the duck from the
    // low pri task. When the high pri finishes, low pri gets the duck back.
    try {
      Priority highPri = Priority.newInstance(1), lowPri = Priority.newInstance(2);
      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId(), task2 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      tsWrapper.ts.updateGuaranteedCount(1);
      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, null, lowPri, new Object());
      tsWrapper.awaitTotalTaskAllocations(1);
      TaskInfo ti1 = tsWrapper.ts.getTaskInfo(task1);
      assertTrue(ti1.isGuaranteed());
      assertEquals(State.ASSIGNED, ti1.getState());
      assertEquals(0, tsWrapper.ts.getUnusedGuaranteedCount());

      tsWrapper.allocateTask(task2, null, highPri, new Object());
      tsWrapper.awaitTotalTaskAllocations(2);
      TaskInfo ti2 = tsWrapper.ts.getTaskInfo(task2);
      assertTrue(ti2.isGuaranteed());
      assertFalse(ti1.isGuaranteed());
      assertEquals(0, tsWrapper.ts.getUnusedGuaranteedCount());

      tsWrapper.deallocateTask(task2, true, TaskAttemptEndReason.CONTAINER_EXITED);
      assertTrue(ti1.isGuaranteed());
      assertEquals(0, tsWrapper.ts.getUnusedGuaranteedCount());
      tsWrapper.deallocateTask(task1, true, TaskAttemptEndReason.CONTAINER_EXITED);
      assertEquals(1, tsWrapper.ts.getUnusedGuaranteedCount());
    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testChangeGuaranteedTotal() throws IOException, InterruptedException {
    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper();
    // Schedule 3 tasks. Give out two ducks - two higher pri tasks get them. Give out 2 more
    // - the last task gets it and one duck is unused. Give out 2 more - goes to unused.
    // Then revoke similarly in steps (1, 4, 1), with the opposite effect.
    try {
      Priority highPri = Priority.newInstance(1), lowPri = Priority.newInstance(2);
      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId(), task2 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId(), task3 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      tsWrapper.ts.updateGuaranteedCount(0);
      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, null, highPri, new Object());
      tsWrapper.allocateTask(task2, null, lowPri, new Object());
      tsWrapper.allocateTask(task3, null, lowPri, new Object());
      tsWrapper.awaitTotalTaskAllocations(3);
      TaskInfo ti1 = tsWrapper.ts.getTaskInfo(task1), ti2 = tsWrapper.ts.getTaskInfo(task2),
          ti3 = tsWrapper.ts.getTaskInfo(task3);
      assertFalse(ti1.isGuaranteed() || ti2.isGuaranteed() || ti3.isGuaranteed());
      assertEquals(0, tsWrapper.ts.getUnusedGuaranteedCount());

      tsWrapper.ts.updateGuaranteedCount(2);
      assertTrue(ti1.isGuaranteed());
      // This particular test doesn't care which of the lower pri tasks gets the duck.
      TaskInfo ti23High = ti2.isGuaranteed() ? ti2 : ti3, ti23Low = (ti2 == ti23High) ? ti3 : ti2;
      assertTrue(ti23High.isGuaranteed());
      assertFalse(ti23Low.isGuaranteed());
      assertEquals(0, tsWrapper.ts.getUnusedGuaranteedCount());

      tsWrapper.ts.updateGuaranteedCount(4);
      assertTrue(ti1.isGuaranteed());
      assertTrue(ti23High.isGuaranteed());
      assertTrue(ti23Low.isGuaranteed());
      assertEquals(1, tsWrapper.ts.getUnusedGuaranteedCount());

      tsWrapper.ts.updateGuaranteedCount(6);
      assertTrue(ti1.isGuaranteed());
      assertTrue(ti23High.isGuaranteed());
      assertTrue(ti23Low.isGuaranteed());
      assertEquals(3, tsWrapper.ts.getUnusedGuaranteedCount());

      tsWrapper.ts.updateGuaranteedCount(5);
      assertTrue(ti1.isGuaranteed());
      assertTrue(ti23High.isGuaranteed());
      assertTrue(ti23Low.isGuaranteed());
      assertEquals(2, tsWrapper.ts.getUnusedGuaranteedCount());

      tsWrapper.ts.updateGuaranteedCount(1);
      assertTrue(ti1.isGuaranteed());
      assertFalse(ti23High.isGuaranteed());
      assertFalse(ti23Low.isGuaranteed());
      assertEquals(0, tsWrapper.ts.getUnusedGuaranteedCount());

      tsWrapper.ts.updateGuaranteedCount(0);
      assertFalse(ti1.isGuaranteed());
      assertFalse(ti23High.isGuaranteed());
      assertFalse(ti23Low.isGuaranteed());
      assertEquals(0, tsWrapper.ts.getUnusedGuaranteedCount());

      tsWrapper.deallocateTask(task1, true, TaskAttemptEndReason.CONTAINER_EXITED);
      tsWrapper.deallocateTask(task2, true, TaskAttemptEndReason.CONTAINER_EXITED);
      tsWrapper.deallocateTask(task3, true, TaskAttemptEndReason.CONTAINER_EXITED);
      assertEquals(0, tsWrapper.ts.getUnusedGuaranteedCount());
    } finally {
      tsWrapper.shutdown();
    }
  }


  @Test(timeout = 20000)
  public void testConcurrentUpdates() throws IOException, InterruptedException {
    final TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper();
    // Test 4 variations of callbacks. 2 increases/2 revokes - do not update the same task again;
    // Then, increase + decrease and decrease + increase, the 2nd call coming after the message is sent;
    // the message callback should undo the change.
    try {
      Priority highPri = Priority.newInstance(1), lowPri = Priority.newInstance(2);
      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId(), task2 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      tsWrapper.ts.updateGuaranteedCount(0);
      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, null, highPri, new Object());
      tsWrapper.allocateTask(task2, null, lowPri, new Object());
      tsWrapper.awaitTotalTaskAllocations(2);
      TaskInfo ti1 = tsWrapper.ts.getTaskInfo(task1), ti2 = tsWrapper.ts.getTaskInfo(task2);
      assertFalse(ti1.isGuaranteed() || ti2.isGuaranteed());

      // Boring scenario #1 - two concurrent increases.
      tsWrapper.ts.updateGuaranteedCount(1);
      tsWrapper.ts.waitForMessagesSent(1);
      assertTrue(ti1.isGuaranteed());
      assertFalse(ti1.getLastSetGuaranteed());  // Not updated yet.
      assertFalse(ti2.isGuaranteed());
      // We are now "sending" a message... update again, "return" both callbacks.
      tsWrapper.ts.updateGuaranteedCount(2);
      tsWrapper.ts.waitForMessagesSent(1);
      assertTrue(ti1.isGuaranteed());
      assertTrue(ti2.isGuaranteed());

      tsWrapper.ts.handleUpdateResult(ti1, true);
      tsWrapper.ts.handleUpdateResult(ti2, true);
      assertTrue(ti1.isGuaranteed());
      assertTrue(ti2.isGuaranteed());
      assertTrue(ti1.getLastSetGuaranteed());
      assertTrue(ti2.getLastSetGuaranteed());

      // Boring scenario #2 - two concurrent revokes. Same as above.
      tsWrapper.ts.updateGuaranteedCount(1);
      tsWrapper.ts.waitForMessagesSent(1);
      assertTrue(ti1.isGuaranteed());
      assertFalse(ti2.isGuaranteed());
      assertTrue(ti2.getLastSetGuaranteed()); // Not updated yet.
      tsWrapper.ts.updateGuaranteedCount(0);
      tsWrapper.ts.waitForMessagesSent(1);
      assertFalse(ti1.isGuaranteed());
      assertFalse(ti2.isGuaranteed());

      tsWrapper.ts.handleUpdateResult(ti1, true);
      tsWrapper.ts.handleUpdateResult(ti2, true);
      assertFalse(ti1.isGuaranteed());
      assertFalse(ti2.isGuaranteed());
      assertFalse(ti1.getLastSetGuaranteed());
      assertFalse(ti2.getLastSetGuaranteed());

      // Concurrent increase and revocation, then another increase - after the message is sent.
      tsWrapper.ts.updateGuaranteedCount(1);
      tsWrapper.ts.waitForMessagesSent(1);
      assertTrue(ti1.isGuaranteed());
      assertFalse(ti1.getLastSetGuaranteed()); // Not updated yet.
      assertTrue(ti1.isUpdateInProgress());
      tsWrapper.ts.updateGuaranteedCount(0);
      tsWrapper.ts.assertNoMessagesSent(); // We are revoking from an updating task.
      assertFalse(ti1.isGuaranteed());

      tsWrapper.ts.handleUpdateResult(ti1, true);
      tsWrapper.ts.waitForMessagesSent(1); // We should send a message to undo what we just did.
      assertFalse(ti1.isGuaranteed());
      assertTrue(ti1.getLastSetGuaranteed());
      assertTrue(ti1.isUpdateInProgress());
      tsWrapper.ts.updateGuaranteedCount(1);
      tsWrapper.ts.assertNoMessagesSent();
      assertTrue(ti1.isGuaranteed());
      assertTrue(ti1.getLastSetGuaranteed());

      tsWrapper.ts.handleUpdateResult(ti1, true);
      tsWrapper.ts.waitForMessagesSent(1);
      assertTrue(ti1.isGuaranteed());
      assertFalse(ti1.getLastSetGuaranteed());
      assertTrue(ti1.isUpdateInProgress());
      tsWrapper.ts.handleUpdateResult(ti1, true);
      tsWrapper.ts.assertNoMessagesSent();
      assertTrue(ti1.isGuaranteed());
      assertTrue(ti1.getLastSetGuaranteed());
      assertFalse(ti1.isUpdateInProgress());

      tsWrapper.deallocateTask(task1, true, TaskAttemptEndReason.CONTAINER_EXITED);
      tsWrapper.deallocateTask(task2, true, TaskAttemptEndReason.CONTAINER_EXITED);
      assertEquals(1, tsWrapper.ts.getUnusedGuaranteedCount());
    } finally {
      tsWrapper.shutdown();
    }
  }


  @Test(timeout = 10000)
  public void testUpdateOnFinishingTask() throws IOException, InterruptedException {
    final TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper();
    // The update fails because the task has terminated on the node.
    try {
      Priority highPri = Priority.newInstance(1), lowPri = Priority.newInstance(2);
      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId(), task2 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      tsWrapper.ts.updateGuaranteedCount(0);
      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, null, highPri, new Object());
      tsWrapper.allocateTask(task2, null, lowPri, new Object());
      tsWrapper.awaitTotalTaskAllocations(2);
      TaskInfo ti1 = tsWrapper.ts.getTaskInfo(task1), ti2 = tsWrapper.ts.getTaskInfo(task2);

      // Concurrent increase and termination, increase fails.
      tsWrapper.ts.updateGuaranteedCount(1);
      tsWrapper.ts.waitForMessagesSent(1);
      assertTrue(ti1.isGuaranteed());
      assertFalse(ti1.getLastSetGuaranteed()); // Not updated yet.
      assertTrue(ti1.isUpdateInProgress());
      tsWrapper.deallocateTask(task1, true, TaskAttemptEndReason.CONTAINER_EXITED);
      tsWrapper.ts.handleUpdateResult(ti1, false);

      // We must have the duck still; it should just go to the other task.
      assertTrue(ti2.isGuaranteed());
      assertTrue(ti2.isUpdateInProgress());
      tsWrapper.ts.handleUpdateResult(ti2, false);
      tsWrapper.deallocateTask(task2, true, TaskAttemptEndReason.CONTAINER_EXITED);

      // Same; with the termination after the failed update, we should maintain the correct count.
      assertEquals(1, tsWrapper.ts.getUnusedGuaranteedCount());
    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testConcurrentUpdateWithError() throws IOException, InterruptedException {
    final TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper();
    // The update has failed but the state has changed since then - no retry needed.
    try {
      Priority highPri = Priority.newInstance(1);
      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      tsWrapper.ts.updateGuaranteedCount(0);
      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, null, highPri, new Object());
      tsWrapper.awaitTotalTaskAllocations(1);
      TaskInfo ti1 = tsWrapper.ts.getTaskInfo(task1);
      assertFalse(ti1.isGuaranteed());

      // Concurrent increase and revocation, increase fails - no revocation is needed.
      tsWrapper.ts.updateGuaranteedCount(1);
      tsWrapper.ts.waitForMessagesSent(1);
      assertTrue(ti1.isGuaranteed());
      assertFalse(ti1.getLastSetGuaranteed()); // Not updated yet.
      assertTrue(ti1.isUpdateInProgress());
      tsWrapper.ts.updateGuaranteedCount(0);
      tsWrapper.ts.assertNoMessagesSent(); // We are revoking from an updating task.
      assertFalse(ti1.isGuaranteed());

      tsWrapper.ts.handleUpdateResult(ti1, false);
      assertFalse(ti1.isGuaranteed());
      assertFalse(ti1.getLastSetGuaranteed());
      assertFalse(ti1.isUpdateInProgress());
      tsWrapper.ts.assertNoMessagesSent();

      tsWrapper.deallocateTask(task1, true, TaskAttemptEndReason.CONTAINER_EXITED);
      assertEquals(0, tsWrapper.ts.getUnusedGuaranteedCount());
    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testUpdateWithError() throws IOException, InterruptedException {
    final TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper();
    // The update has failed; we'd try with another candidate first, but only at the same priority.
    try {
      Priority highPri = Priority.newInstance(1), lowPri = Priority.newInstance(2);
      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId(), task2 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId(), task3 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      tsWrapper.ts.updateGuaranteedCount(0);
      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, null, highPri, new Object());
      tsWrapper.allocateTask(task2, null, highPri, new Object());
      tsWrapper.awaitTotalTaskAllocations(2);
      TaskInfo ti1 = tsWrapper.ts.getTaskInfo(task1), ti2 = tsWrapper.ts.getTaskInfo(task2);
      assertFalse(ti1.isGuaranteed() || ti2.isGuaranteed());

      tsWrapper.ts.updateGuaranteedCount(1);
      tsWrapper.ts.waitForMessagesSent(1);
      TaskInfo tiHigher = ti1.isGuaranteed() ? ti1 : ti2, tiLower = (tiHigher == ti1) ? ti2 : ti1;
      assertTrue(tiHigher.isGuaranteed());
      assertFalse(tiHigher.getLastSetGuaranteed()); // Not updated yet.
      assertTrue(tiHigher.isUpdateInProgress());

      tsWrapper.ts.handleUpdateResult(tiHigher, false); // Update has failed. We should try task2.
      tsWrapper.ts.waitForMessagesSent(1);
      assertFalse(tiHigher.isGuaranteed());
      assertFalse(tiHigher.getLastSetGuaranteed());
      assertFalse(tiHigher.isUpdateInProgress());
      assertTrue(tiLower.isGuaranteed());
      assertFalse(tiLower.getLastSetGuaranteed());
      assertTrue(tiLower.isUpdateInProgress());

      // Fail the 2nd update too to get rid of the duck for the next test.
      tsWrapper.ts.updateGuaranteedCount(0);
      tsWrapper.ts.handleUpdateResult(tiLower, false);
      tsWrapper.ts.assertNoMessagesSent();

      // Now run a lower priority task.
      tsWrapper.deallocateTask(task2, true, TaskAttemptEndReason.CONTAINER_EXITED);
      tsWrapper.allocateTask(task3, null, lowPri, new Object());
      tsWrapper.awaitTotalTaskAllocations(3);
      TaskInfo ti3 = tsWrapper.ts.getTaskInfo(task3);

      tsWrapper.ts.updateGuaranteedCount(1);
      tsWrapper.ts.waitForMessagesSent(1);
      assertTrue(ti1.isGuaranteed());
      assertTrue(ti1.isUpdateInProgress());

      tsWrapper.ts.handleUpdateResult(ti1, false); // Update has failed. We won't try a low pri task.
      assertTrue(ti1.isGuaranteed());
      assertFalse(ti1.getLastSetGuaranteed());
      assertTrue(ti1.isUpdateInProgress());
      assertFalse(ti3.isGuaranteed());
      assertFalse(ti3.isUpdateInProgress());

      assertEquals(0, tsWrapper.ts.getUnusedGuaranteedCount());
    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testConcurrentUpdatesBeforeMessage() throws IOException, InterruptedException {
    final TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper();
    // 2 more variations of callbacks; increase + decrease and decrease + increase, the 2nd call coming
    // before the message is sent; no message should ever be sent.
    try {
      Priority highPri = Priority.newInstance(1), lowPri = Priority.newInstance(2);
      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId(), task2 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      tsWrapper.ts.updateGuaranteedCount(0);
      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, null, highPri, new Object());
      tsWrapper.allocateTask(task2, null, lowPri, new Object());
      tsWrapper.awaitTotalTaskAllocations(2);
      TaskInfo ti1 = tsWrapper.ts.getTaskInfo(task1), ti2 = tsWrapper.ts.getTaskInfo(task2);
      assertFalse(ti1.isGuaranteed() || ti2.isGuaranteed());

      // Concurrent increase and revocation - before the message is sent.
      tsWrapper.ts.clearTestCounts();
      tsWrapper.ts.setDelayCheckAndSend(true);
      Thread updateThread = new Thread(new Runnable() {
        @Override
        public void run() {
          tsWrapper.ts.updateGuaranteedCount(1);
        }
      }, "test-update-thread");
      updateThread.start(); // This should eventually hang in the delay code.
      tsWrapper.ts.waitForCheckAndSendCall(1); // From the background thread.
      assertTrue(ti1.isGuaranteed());
      assertTrue(ti1.isUpdateInProgress());
      assertFalse(ti1.getLastSetGuaranteed());
      tsWrapper.ts.updateGuaranteedCount(0); // This won't go into checkAndSend.
      tsWrapper.ts.assertNoMessagesSent();
      // Release the background thread.
      tsWrapper.ts.setDelayCheckAndSend(false);
      updateThread.join();
      tsWrapper.ts.assertNoMessagesSent(); // No message is needed.
      assertFalse(ti1.isGuaranteed());
      assertFalse(ti1.getLastSetGuaranteed());
      assertFalse(ti1.isUpdateInProgress());

      // Concurrent revocation and increase - before the message is sent.
      // First, actually give it a duck.
      tsWrapper.ts.updateGuaranteedCount(1);
      tsWrapper.ts.handleUpdateResult(ti1, true);
      tsWrapper.ts.clearTestCounts();
      assertTrue(ti1.isGuaranteed() && ti1.getLastSetGuaranteed());

      tsWrapper.ts.setDelayCheckAndSend(true);
      updateThread = new Thread(new Runnable() {
        @Override
        public void run() {
          tsWrapper.ts.updateGuaranteedCount(0);
        }
      }, "test-update-thread");
      updateThread.start(); // This should eventually hang in the delay code.
      tsWrapper.ts.waitForCheckAndSendCall(1);
      assertFalse(ti1.isGuaranteed());
      assertTrue(ti1.isUpdateInProgress());
      assertTrue(ti1.getLastSetGuaranteed());
      tsWrapper.ts.updateGuaranteedCount(1); // This won't go into checkAndSend.
      tsWrapper.ts.assertNoMessagesSent();
      // Release the background thread.
      tsWrapper.ts.setDelayCheckAndSend(false);
      updateThread.join();
      tsWrapper.ts.assertNoMessagesSent(); // No message is needed.
      assertTrue(ti1.isGuaranteed());
      assertTrue(ti1.getLastSetGuaranteed());
      assertFalse(ti1.isUpdateInProgress());

      tsWrapper.deallocateTask(task1, true, TaskAttemptEndReason.CONTAINER_EXITED);
      tsWrapper.deallocateTask(task2, true, TaskAttemptEndReason.CONTAINER_EXITED);
      assertEquals(1, tsWrapper.ts.getUnusedGuaranteedCount());
    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testSimpleNoLocalityAllocation() throws IOException, InterruptedException {
    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper();

    try {
      Priority priority1 = Priority.newInstance(1);

      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie1 = new Object();
      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, null, priority1, clientCookie1);
      tsWrapper.awaitTotalTaskAllocations(1);
      verify(tsWrapper.mockAppCallback).taskAllocated(eq(task1), eq(clientCookie1), any(Container.class));
      assertEquals(1, tsWrapper.ts.dagStats.numAllocationsNoLocalityRequest);
    } finally {
      tsWrapper.shutdown();
    }
  }


  @Test(timeout = 10000)
  public void testPreemption() throws InterruptedException, IOException {

    Priority priority1 = Priority.newInstance(1);
    Priority priority2 = Priority.newInstance(2);
    String [] hosts = new String[] {HOST1};
    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(2000, hosts, 1, 1);
    try {

      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie1 = "cookie1";
      TezTaskAttemptID task2 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie2 = "cookie2";
      TezTaskAttemptID task3 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie3 = "cookie3";
      TezTaskAttemptID task4 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie4 = "cookie4";

      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, hosts, priority2, clientCookie1);
      tsWrapper.allocateTask(task2, hosts, priority2, clientCookie2);
      tsWrapper.allocateTask(task3, hosts, priority2, clientCookie3);
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numLocalAllocations == 2) {
          break;
        }
      }
      verify(tsWrapper.mockAppCallback, times(2)).taskAllocated(any(Object.class),
          any(Object.class), any(Container.class));
      assertEquals(2, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(0, tsWrapper.ts.dagStats.numAllocationsNoLocalityRequest);

      reset(tsWrapper.mockAppCallback);

      tsWrapper.allocateTask(task4, hosts, priority1, clientCookie4);
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numPreemptedTasks == 1) {
          break;
        }
      }
      verify(tsWrapper.mockAppCallback).preemptContainer(any(ContainerId.class));


      tsWrapper.deallocateTask(task2, false, TaskAttemptEndReason.INTERNAL_PREEMPTION);

      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numTotalAllocations == 3) {
          break;
        }
      }
      verify(tsWrapper.mockAppCallback, times(1)).taskAllocated(eq(task4),
          eq(clientCookie4), any(Container.class));

    } finally {
      tsWrapper.shutdown();
    }

  }

  @Test(timeout = 10000)
  public void testNodeDisabled() throws IOException, InterruptedException {
    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(10000l);
    try {
      Priority priority1 = Priority.newInstance(1);
      String[] hosts1 = new String[]{HOST1};
      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie1 = new Object();
      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, hosts1, priority1, clientCookie1);
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numTotalAllocations == 1) {
          break;
        }
      }
      verify(tsWrapper.mockAppCallback).taskAllocated(eq(task1), eq(clientCookie1),
          any(Container.class));
      assertEquals(1, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(0, tsWrapper.ts.dagStats.numAllocationsNoLocalityRequest);
      assertEquals(0, tsWrapper.ts.dagStats.numNonLocalAllocations);
      assertEquals(1, tsWrapper.ts.dagStats.numTotalAllocations);

      tsWrapper.resetAppCallback();

      tsWrapper.clock.setTime(10000l);
      tsWrapper.rejectExecution(task1);

      // Verify that the node is blacklisted
      assertEquals(1, tsWrapper.ts.dagStats.numRejectedTasks);
      assertEquals(3, tsWrapper.ts.instanceToNodeMap.size());
      LlapTaskSchedulerService.NodeInfo disabledNodeInfo = tsWrapper.ts.disabledNodesQueue.peek();
      assertNotNull(disabledNodeInfo);
      assertEquals(HOST1, disabledNodeInfo.getHost());
      assertEquals((10000l), disabledNodeInfo.getDelay(TimeUnit.MILLISECONDS));
      assertEquals((10000l + 10000l), disabledNodeInfo.expireTimeMillis);

      TezTaskAttemptID task2 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie2 = new Object();
      tsWrapper.allocateTask(task2, hosts1, priority1, clientCookie2);
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numTotalAllocations == 2) {
          break;
        }
      }
      verify(tsWrapper.mockAppCallback).taskAllocated(eq(task2), eq(clientCookie2), any(Container.class));
      assertEquals(1, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(0, tsWrapper.ts.dagStats.numAllocationsNoLocalityRequest);
      assertEquals(1, tsWrapper.ts.dagStats.numNonLocalAllocations);
      assertEquals(2, tsWrapper.ts.dagStats.numTotalAllocations);

    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testNodeReEnabled() throws InterruptedException, IOException {
    // Based on actual timing.
    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(1000l);
    try {
      Priority priority1 = Priority.newInstance(1);
      String[] hosts1 = new String[]{HOST1};
      String[] hosts2 = new String[]{HOST2};
      String[] hosts3 = new String[]{HOST3};

      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie1 = new Object();
      TezTaskAttemptID task2 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie2 = new Object();
      TezTaskAttemptID task3 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie3 = new Object();

      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, hosts1, priority1, clientCookie1);
      tsWrapper.allocateTask(task2, hosts2, priority1, clientCookie2);
      tsWrapper.allocateTask(task3, hosts3, priority1, clientCookie3);
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numTotalAllocations == 3) {
          break;
        }
      }
      verify(tsWrapper.mockAppCallback, times(3)).taskAllocated(any(Object.class), any(Object.class), any(Container.class));
      assertEquals(3, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(0, tsWrapper.ts.dagStats.numAllocationsNoLocalityRequest);
      assertEquals(3, tsWrapper.ts.dagStats.numTotalAllocations);

      tsWrapper.resetAppCallback();

      tsWrapper.rejectExecution(task1);
      tsWrapper.rejectExecution(task2);
      tsWrapper.rejectExecution(task3);

      // Verify that the node is blacklisted
      assertEquals(3, tsWrapper.ts.dagStats.numRejectedTasks);
      assertEquals(3, tsWrapper.ts.instanceToNodeMap.size());
      assertEquals(3, tsWrapper.ts.disabledNodesQueue.size());


      TezTaskAttemptID task4 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie4 = new Object();
      TezTaskAttemptID task5 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie5 = new Object();
      TezTaskAttemptID task6 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie6 = new Object();
      tsWrapper.allocateTask(task4, hosts1, priority1, clientCookie4);
      tsWrapper.allocateTask(task5, hosts2, priority1, clientCookie5);
      tsWrapper.allocateTask(task6, hosts3, priority1, clientCookie6);
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numTotalAllocations == 6) {
          break;
        }
      }

      ArgumentCaptor<Container> argumentCaptor = ArgumentCaptor.forClass(Container.class);
      verify(tsWrapper.mockAppCallback, times(3)).taskAllocated(any(Object.class), any(Object.class), argumentCaptor.capture());

      // which affects the locality matching
      assertEquals(0, tsWrapper.ts.dagStats.numAllocationsNoLocalityRequest);
      assertEquals(6, tsWrapper.ts.dagStats.numTotalAllocations);

    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testForceLocalityTest1() throws IOException, InterruptedException {
    // 2 hosts. 2 per host. 5 requests at the same priority.
    // First 3 on host1, Next at host2, Last with no host.
    // Third request on host1 should not be allocated immediately.
    forceLocalityTest1(true);

  }

  @Test(timeout = 10000)
  public void testNoForceLocalityCounterTest1() throws IOException, InterruptedException {
    // 2 hosts. 2 per host. 5 requests at the same priority.
    // First 3 on host1, Next at host2, Last with no host.
    // Third should allocate on host2, 4th on host2, 5th will wait.

    forceLocalityTest1(false);
  }

  private void forceLocalityTest1(boolean forceLocality) throws IOException, InterruptedException {
    Priority priority1 = Priority.newInstance(1);

    String[] hosts = new String[] {HOST1, HOST2};

    String[] hostsH1 = new String[] {HOST1};
    String[] hostsH2 = new String[] {HOST2};

    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(2000, hosts, 1, 1, (forceLocality ? -1l : 0l));

    try {
      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie1 = "cookie1";
      TezTaskAttemptID task2 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie2 = "cookie2";
      TezTaskAttemptID task3 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie3 = "cookie3";
      TezTaskAttemptID task4 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie4 = "cookie4";
      TezTaskAttemptID task5 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie5 = "cookie5";

      tsWrapper.controlScheduler(true);
      //H1 - should allocate
      tsWrapper.allocateTask(task1, hostsH1, priority1, clientCookie1);
      //H1 - should allocate
      tsWrapper.allocateTask(task2, hostsH1, priority1, clientCookie2);
      //H1 - no capacity if force, should allocate otherwise
      tsWrapper.allocateTask(task3, hostsH1, priority1, clientCookie3);
      //H2 - should allocate
      tsWrapper.allocateTask(task4, hostsH2, priority1, clientCookie4);
      //No location - should allocate if force, no capacity otherwise
      tsWrapper.allocateTask(task5, null, priority1, clientCookie5);

      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numTotalAllocations == 4) {
          break;
        }
      }

      // Verify no preemption requests - since everything is at the same priority
      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      verify(tsWrapper.mockAppCallback, times(4)).taskAllocated(argumentCaptor.capture(), any(Object.class), any(Container.class));
      assertEquals(4, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));
      if (forceLocality) {
        // task3 not allocated
        assertEquals(task4, argumentCaptor.getAllValues().get(2));
        assertEquals(task5, argumentCaptor.getAllValues().get(3));
      } else {
        assertEquals(task3, argumentCaptor.getAllValues().get(2));
        assertEquals(task4, argumentCaptor.getAllValues().get(3));
      }

      //Complete one task on host1.
      tsWrapper.deallocateTask(task1, true, null);

      reset(tsWrapper.mockAppCallback);

      // Try scheduling again.
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numTotalAllocations == 5) {
          break;
        }
      }
      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      argumentCaptor = ArgumentCaptor.forClass(Object.class);
      verify(tsWrapper.mockAppCallback, times(1)).taskAllocated(argumentCaptor.capture(), any(Object.class), any(Container.class));
      assertEquals(1, argumentCaptor.getAllValues().size());
      if (forceLocality) {
        assertEquals(task3, argumentCaptor.getAllValues().get(0));
      } else {
        assertEquals(task5, argumentCaptor.getAllValues().get(0));
      }

    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testForcedLocalityUnknownHost() throws IOException, InterruptedException {
    Priority priority1 = Priority.newInstance(1);

    String[] hostsKnown = new String[]{HOST1};
    String[] hostsUnknown = new String[]{HOST2};

    TestTaskSchedulerServiceWrapper tsWrapper =
        new TestTaskSchedulerServiceWrapper(2000, hostsKnown, 1, 1, -1l);
    try {
      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie1 = "cookie1";

      TezTaskAttemptID task2 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie2 = "cookie2";

      tsWrapper.controlScheduler(true);
      // Should allocate since H2 is not known.
      tsWrapper.allocateTask(task1, hostsUnknown, priority1, clientCookie1);
      tsWrapper.allocateTask(task2, hostsKnown, priority1, clientCookie2);


      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numTotalAllocations == 2) {
          break;
        }
      }

      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      verify(tsWrapper.mockAppCallback, times(2))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), any(Container.class));
      assertEquals(2, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));


    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testHostPreferenceUnknownAndNotSpecified() throws IOException, InterruptedException {
    Priority priority1 = Priority.newInstance(1);

    String[] hostsKnown = new String[]{HOST1, HOST2};
    String[] hostsUnknown = new String[]{HOST3};
    String[] noHosts = new String[]{};

    TestTaskSchedulerServiceWrapper tsWrapper =
      new TestTaskSchedulerServiceWrapper(2000, hostsKnown, 1, 1, -1l);
    try {
      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie1 = "cookie1";

      TezTaskAttemptID task2 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie2 = "cookie2";

      TezTaskAttemptID task3 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie3 = "cookie3";

      TezTaskAttemptID task4 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie4 = "cookie4";

      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, hostsKnown, priority1, clientCookie1);
      tsWrapper.allocateTask(task2, hostsKnown, priority1, clientCookie2);
      tsWrapper.allocateTask(task3, hostsUnknown, priority1, clientCookie3);
      tsWrapper.allocateTask(task4, noHosts, priority1, clientCookie4);

      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numTotalAllocations == 4) {
          break;
        }
      }

      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      ArgumentCaptor<Container> argumentCaptor2 = ArgumentCaptor.forClass(Container.class);
      verify(tsWrapper.mockAppCallback, times(4))
        .taskAllocated(argumentCaptor.capture(), any(Object.class), argumentCaptor2.capture());
      assertEquals(4, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));
      assertEquals(task3, argumentCaptor.getAllValues().get(2));
      assertEquals(task4, argumentCaptor.getAllValues().get(3));
      // 1st task requested host1, got host1
      assertEquals(HOST1, argumentCaptor2.getAllValues().get(0).getNodeId().getHost());
      // 2nd task requested host1, got host1
      assertEquals(HOST1, argumentCaptor2.getAllValues().get(1).getNodeId().getHost());
      // 3rd task requested unknown host, got host2 since host1 is full and only host2 is left in random pool
      assertEquals(HOST2, argumentCaptor2.getAllValues().get(2).getNodeId().getHost());
      // 4rd task provided no location preference, got host2 since host1 is full and only host2 is left in random pool
      assertEquals(HOST2, argumentCaptor2.getAllValues().get(3).getNodeId().getHost());

      assertEquals(1, tsWrapper.ts.dagStats.numAllocationsNoLocalityRequest);
      assertEquals(2, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(1, tsWrapper.ts.dagStats.numNonLocalAllocations);
    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testHostPreferenceMissesConsistentRollover() throws IOException, InterruptedException {
    Priority priority1 = Priority.newInstance(1);

    String[] hostsKnown = new String[]{HOST1, HOST2, HOST3};
    String[] hostsLive = new String[]{HOST1, HOST2, HOST3};
    String[] hostsH2 = new String[]{HOST2};
    TestTaskSchedulerServiceWrapper tsWrapper =
      new TestTaskSchedulerServiceWrapper(2000, hostsKnown, 1, 0, 0l, false, hostsLive, true);
    try {
      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie1 = "cookie1";

      TezTaskAttemptID task2 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie2 = "cookie2";

      TezTaskAttemptID task3 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie3 = "cookie3";

      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, hostsH2, priority1, clientCookie1);
      tsWrapper.allocateTask(task2, hostsH2, priority1, clientCookie2);
      tsWrapper.allocateTask(task3, hostsH2, priority1, clientCookie3);

      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numTotalAllocations == 3) {
          break;
        }
      }

      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      ArgumentCaptor<Container> argumentCaptor2 = ArgumentCaptor.forClass(Container.class);
      verify(tsWrapper.mockAppCallback, times(3))
        .taskAllocated(argumentCaptor.capture(), any(Object.class), argumentCaptor2.capture());
      assertEquals(3, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));
      assertEquals(task3, argumentCaptor.getAllValues().get(2));
      // 1st task requested host2, got host2
      assertEquals(HOST2, argumentCaptor2.getAllValues().get(0).getNodeId().getHost());
      // 2nd task requested host2, got host3 as host2 is full
      assertEquals(HOST3, argumentCaptor2.getAllValues().get(1).getNodeId().getHost());
      // 3rd task requested host2, got host1 as host2 and host3 are full
      assertEquals(HOST1, argumentCaptor2.getAllValues().get(2).getNodeId().getHost());

      verify(tsWrapper.mockServiceInstanceSet, times(2)).getAllInstancesOrdered(true);

      assertEquals(0, tsWrapper.ts.dagStats.numAllocationsNoLocalityRequest);
      assertEquals(1, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(2, tsWrapper.ts.dagStats.numNonLocalAllocations);
    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testHostPreferenceMissesConsistentPartialAlive() throws IOException, InterruptedException {
    Priority priority1 = Priority.newInstance(1);

    String[] hostsKnown = new String[]{HOST1, HOST2, HOST3, HOST4};
    String[] hostsLive = new String[]{HOST1, HOST2, null, HOST4}; // host3 dead before scheduling
    String[] hostsH2 = new String[]{HOST2};
    String[] hostsH3 = new String[]{HOST3};
    TestTaskSchedulerServiceWrapper tsWrapper =
      new TestTaskSchedulerServiceWrapper(2000, hostsKnown, 1, 0, 0l, false, hostsLive, true);
    try {
      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie1 = "cookie1";

      TezTaskAttemptID task2 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie2 = "cookie2";

      TezTaskAttemptID task3 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie3 = "cookie3";

      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, hostsH2, priority1, clientCookie1);
      tsWrapper.allocateTask(task2, hostsH2, priority1, clientCookie2);
      tsWrapper.allocateTask(task3, hostsH3, priority1, clientCookie3);

      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numTotalAllocations == 3) {
          break;
        }
      }

      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      ArgumentCaptor<Container> argumentCaptor2 = ArgumentCaptor.forClass(Container.class);
      verify(tsWrapper.mockAppCallback, times(3))
        .taskAllocated(argumentCaptor.capture(), any(Object.class), argumentCaptor2.capture());
      assertEquals(3, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));
      assertEquals(task3, argumentCaptor.getAllValues().get(2));

      // 1st task requested host2, got host2
      assertEquals(HOST2, argumentCaptor2.getAllValues().get(0).getNodeId().getHost());
      // 2nd task requested host2, got host4 since host3 is dead and host2 is full
      assertEquals(HOST4, argumentCaptor2.getAllValues().get(1).getNodeId().getHost());
      // 3rd task requested host3, got host1 since host3 is dead and host4 is full
      assertEquals(HOST1, argumentCaptor2.getAllValues().get(2).getNodeId().getHost());

      verify(tsWrapper.mockServiceInstanceSet, times(2)).getAllInstancesOrdered(true);

      assertEquals(0, tsWrapper.ts.dagStats.numAllocationsNoLocalityRequest);
      assertEquals(1, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(2, tsWrapper.ts.dagStats.numNonLocalAllocations);
    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testForcedLocalityPreemption() throws IOException, InterruptedException {
    Priority priority1 = Priority.newInstance(1);
    Priority priority2 = Priority.newInstance(2);
    String [] hosts = new String[] {HOST1, HOST2};

    String [] hostsH1 = new String[] {HOST1};

    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(2000, hosts, 2, 0, -1l);

    // Fill up host1 with p2 tasks.
    // Leave host2 empty
    // Try running p1 task on host1 - should preempt

    try {
      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie1 = "cookie1";
      TezTaskAttemptID task2 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie2 = "cookie2";
      TezTaskAttemptID task3 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie3 = "cookie3";
      TezTaskAttemptID task4 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie4 = "cookie4";

      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, hostsH1, priority2, clientCookie1);
      tsWrapper.allocateTask(task2, hostsH1, priority2, clientCookie2);
      // This request at a lower priority should not affect anything.
      tsWrapper.allocateTask(task3, hostsH1, priority2, clientCookie3);
      tsWrapper.awaitLocalTaskAllocations(2);

      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      verify(tsWrapper.mockAppCallback, times(2))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), any(Container.class));
      assertEquals(2, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));

      reset(tsWrapper.mockAppCallback);
      // Allocate t4 at higher priority. t3 should not be allocated,
      // and a preemption should be attempted on host1, despite host2 having available capacity
      tsWrapper.allocateTask(task4, hostsH1, priority1, clientCookie4);

      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numPreemptedTasks == 1) {
          break;
        }
      }
      verify(tsWrapper.mockAppCallback).preemptContainer(any(ContainerId.class));

      tsWrapper.deallocateTask(task1, false, TaskAttemptEndReason.INTERNAL_PREEMPTION);

      tsWrapper.awaitLocalTaskAllocations(3);

      verify(tsWrapper.mockAppCallback, times(1)).taskAllocated(eq(task4),
          eq(clientCookie4), any(Container.class));

    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testPreemptionChoiceTimeOrdering() throws IOException, InterruptedException {

    Priority priority1 = Priority.newInstance(1);
    Priority priority2 = Priority.newInstance(2);

    String [] hosts = new String[] {HOST1, HOST2};

    String [] hostsH1 = new String[] {HOST1};

    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(2000, hosts, 1, 1, -1l);

    try {
      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie1 = "cookie1";
      TezTaskAttemptID task2 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie2 = "cookie2";
      TezTaskAttemptID task3 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie3 = "cookie3";

      tsWrapper.controlScheduler(true);

      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      ArgumentCaptor<Container> cArgCaptor = ArgumentCaptor.forClass(Container.class);

      // Request task1
      tsWrapper.getClock().setTime(10000l);
      tsWrapper.allocateTask(task1, hostsH1, priority2, clientCookie1);
      tsWrapper.awaitLocalTaskAllocations(1);
      verify(tsWrapper.mockAppCallback, times(1))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), cArgCaptor.capture());
      ContainerId t1Cid = cArgCaptor.getValue().getId();

      reset(tsWrapper.mockAppCallback);
      // Move clock backwards (so that t1 allocation is after t2 allocation)
      // Request task2 (task1 already started at previously set time)
      tsWrapper.getClock().setTime(tsWrapper.getClock().getTime() - 1000);
      tsWrapper.allocateTask(task2, hostsH1, priority2, clientCookie2);
      tsWrapper.awaitLocalTaskAllocations(2);
      verify(tsWrapper.mockAppCallback, times(1))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), cArgCaptor.capture());


      reset(tsWrapper.mockAppCallback);
      // Move clock forward, and request a task at p=1
      tsWrapper.getClock().setTime(tsWrapper.getClock().getTime() + 2000);

      tsWrapper.allocateTask(task3, hostsH1, priority1, clientCookie3);
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numPreemptedTasks == 1) {
          break;
        }
      }
      // Ensure task1 is preempted based on time (match it's allocated containerId)
      ArgumentCaptor<ContainerId> cIdArgCaptor = ArgumentCaptor.forClass(ContainerId.class);
      verify(tsWrapper.mockAppCallback).preemptContainer(cIdArgCaptor.capture());
      assertEquals(t1Cid, cIdArgCaptor.getValue());

    } finally {
      tsWrapper.shutdown();
    }

  }

  @Test(timeout = 10000)
  public void testForcedLocalityMultiplePreemptionsSameHost1() throws IOException,
      InterruptedException {
    Priority priority1 = Priority.newInstance(1);
    Priority priority2 = Priority.newInstance(2);

    String [] hosts = new String[] {HOST1, HOST2};

    String [] hostsH1 = new String[] {HOST1};

    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(2000, hosts, 1, 1, -1l);

    // Fill up host1 with p2 tasks.
    // Leave host2 empty
    // Try running p1 task on host1 - should preempt
    // Await preemption request.
    // Try running another p1 task on host1 - should preempt
    // Await preemption request.

    try {

      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie1 = "cookie1";
      TezTaskAttemptID task2 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie2 = "cookie2";
      TezTaskAttemptID task3 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie3 = "cookie3";
      TezTaskAttemptID task4 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie4 = "cookie4";

      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, hostsH1, priority2, clientCookie1);
      tsWrapper.allocateTask(task2, hostsH1, priority2, clientCookie2);

      tsWrapper.awaitLocalTaskAllocations(2);
      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      ArgumentCaptor<Container> cArgCaptor = ArgumentCaptor.forClass(Container.class);
      verify(tsWrapper.mockAppCallback, times(2))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), cArgCaptor.capture());
      assertEquals(2, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));
      assertEquals(2, cArgCaptor.getAllValues().size());
      ContainerId t1CId = cArgCaptor.getAllValues().get(0).getId();

      reset(tsWrapper.mockAppCallback);
      // At this point. 2 tasks running - both at priority 2.
      // Try running a priority 1 task
      tsWrapper.allocateTask(task3, hostsH1, priority1, clientCookie3);
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numPreemptedTasks == 1) {
          break;
        }
      }
      ArgumentCaptor<ContainerId> cIdArgCaptor = ArgumentCaptor.forClass(ContainerId.class);
      verify(tsWrapper.mockAppCallback).preemptContainer(cIdArgCaptor.capture());

      // Determin which task has been preempted. Normally task2 would be preempted based on it starting
      // later. However - both may have the same start time, so either could be picked.
      Object deallocatedTask1; // De-allocated now
      Object deallocatedTask2; // Will be de-allocated later.
      if (cIdArgCaptor.getValue().equals(t1CId)) {
        deallocatedTask1 = task1;
        deallocatedTask2 = task2;
      } else {
        deallocatedTask1 = task2;
        deallocatedTask2 = task1;
      }

      tsWrapper.deallocateTask(deallocatedTask1, false, TaskAttemptEndReason.INTERNAL_PREEMPTION);

      tsWrapper.awaitLocalTaskAllocations(3);

      verify(tsWrapper.mockAppCallback, times(1)).taskAllocated(eq(task3),
          eq(clientCookie3), any(Container.class));

      reset(tsWrapper.mockAppCallback);
      // At this point. one p=2 task and task3(p=1) running. Ask for another p1 task.
      tsWrapper.allocateTask(task4, hostsH1, priority1, clientCookie4);
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numPreemptedTasks == 2) {
          break;
        }
      }
      verify(tsWrapper.mockAppCallback).preemptContainer(any(ContainerId.class));

      tsWrapper.deallocateTask(deallocatedTask2, false, TaskAttemptEndReason.INTERNAL_PREEMPTION);

      tsWrapper.awaitLocalTaskAllocations(4);

      verify(tsWrapper.mockAppCallback, times(1)).taskAllocated(eq(task4),
          eq(clientCookie4), any(Container.class));


    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testForcedLocalityMultiplePreemptionsSameHost2() throws IOException,
      InterruptedException {
    Priority priority1 = Priority.newInstance(1);
    Priority priority2 = Priority.newInstance(2);

    String [] hosts = new String[] {HOST1, HOST2};

    String [] hostsH1 = new String[] {HOST1};

    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(2000, hosts, 1, 1, -1l);

    // Fill up host1 with p2 tasks.
    // Leave host2 empty
    // Try running both p1 tasks on host1.
    // R: Single preemption triggered, followed by allocation, followed by another preemption.
    //

    try {

      TezTaskAttemptID task1 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie1 = "cookie1";
      TezTaskAttemptID task2 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie2 = "cookie2";
      TezTaskAttemptID task3 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie3 = "cookie3";
      TezTaskAttemptID task4 = TestTaskSchedulerServiceWrapper.generateTaskAttemptId();
      Object clientCookie4 = "cookie4";

      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, hostsH1, priority2, clientCookie1);
      tsWrapper.allocateTask(task2, hostsH1, priority2, clientCookie2);

      tsWrapper.awaitLocalTaskAllocations(2);
      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      ArgumentCaptor<Container> cArgCaptor = ArgumentCaptor.forClass(Container.class);
      verify(tsWrapper.mockAppCallback, times(2))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), cArgCaptor.capture());
      assertEquals(2, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));
      assertEquals(2, cArgCaptor.getAllValues().size());
      ContainerId t1CId = cArgCaptor.getAllValues().get(0).getId();

      reset(tsWrapper.mockAppCallback);
      // At this point. 2 tasks running - both at priority 2.
      // Try running a priority 1 task
      tsWrapper.allocateTask(task3, hostsH1, priority1, clientCookie3);
      tsWrapper.allocateTask(task4, hostsH1, priority1, clientCookie4);
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun();
        if (tsWrapper.ts.dagStats.numPreemptedTasks == 1) {
          break;
        }
      }
      ArgumentCaptor<ContainerId> cIdArgCaptor = ArgumentCaptor.forClass(ContainerId.class);
      verify(tsWrapper.mockAppCallback).preemptContainer(cIdArgCaptor.capture());

      // Determin which task has been preempted. Normally task2 would be preempted based on it starting
      // later. However - both may have the same start time, so either could be picked.
      Object deallocatedTask1; // De-allocated now
      Object deallocatedTask2; // Will be de-allocated later.
      if (cIdArgCaptor.getValue().equals(t1CId)) {
        deallocatedTask1 = task1;
        deallocatedTask2 = task2;
      } else {
        deallocatedTask1 = task2;
        deallocatedTask2 = task1;
      }

      tsWrapper.deallocateTask(deallocatedTask1, false, TaskAttemptEndReason.INTERNAL_PREEMPTION);

      tsWrapper.awaitLocalTaskAllocations(3);

      verify(tsWrapper.mockAppCallback, times(1)).taskAllocated(eq(task3),
          eq(clientCookie3), any(Container.class));

      // At this point. one p=2 task and task3(p=1) running. Ask for another p1 task.
      while (true) {
        tsWrapper.signalSchedulerRun();
        tsWrapper.awaitSchedulerRun(1000l);
        if (tsWrapper.ts.dagStats.numPreemptedTasks == 2) {
          break;
        }
      }
      verify(tsWrapper.mockAppCallback, times(2)).preemptContainer(any(ContainerId.class));

      tsWrapper.deallocateTask(deallocatedTask2, false, TaskAttemptEndReason.INTERNAL_PREEMPTION);

      tsWrapper.awaitLocalTaskAllocations(4);

      verify(tsWrapper.mockAppCallback, times(1)).taskAllocated(eq(task4),
          eq(clientCookie4), any(Container.class));


    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testForcedLocalityNotInDelayedQueue() throws IOException, InterruptedException {
    String[] hosts = new String[]{HOST1, HOST2};

    String[] hostsH1 = new String[]{HOST1};

    TestTaskSchedulerServiceWrapper tsWrapper =
        new TestTaskSchedulerServiceWrapper(2000, hosts, 1, 1, -1l);
    testNotInQueue(tsWrapper, hostsH1);
  }

  @Test(timeout = 10000)
  public void testNoLocalityNotInDelayedQueue() throws IOException, InterruptedException {
    String[] hosts = new String[]{HOST1};

    String[] hostsH1 = new String[]{HOST1};

    TestTaskSchedulerServiceWrapper tsWrapper =
        new TestTaskSchedulerServiceWrapper(2000, hosts, 1, 1, 0l);
    testNotInQueue(tsWrapper, hostsH1);
  }

  private void testNotInQueue(TestTaskSchedulerServiceWrapper tsWrapper, String[] hosts) throws
      InterruptedException {
    Priority priority1 = Priority.newInstance(1);
    try {
      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(hosts, priority1);
      tsWrapper.allocateTask(hosts, priority1);
      tsWrapper.allocateTask(hosts, priority1); // 1 more than capacity.

      tsWrapper.awaitLocalTaskAllocations(2);

      assertEquals(0, tsWrapper.ts.delayedTaskQueue.size());

    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testDelayedLocalityFallbackToNonLocal() throws IOException, InterruptedException {
    Priority priority1 = Priority.newInstance(1);
    String [] hosts = new String[] {HOST1, HOST2};

    String [] hostsH1 = new String[] {HOST1};

    TestTaskSchedulerServiceWrapper tsWrapper =
        new TestTaskSchedulerServiceWrapper(2000, hosts, 1, 1, 10000l, true);
    LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled
        delayedTaskSchedulerCallableControlled =
        (LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled) tsWrapper.ts.delayedTaskSchedulerCallable;
    ControlledClock clock = tsWrapper.getClock();
    clock.setTime(clock.getTime());

    // Fill up host1 with tasks. Leave host2 empty.
    try {
      tsWrapper.controlScheduler(true);
      TezTaskAttemptID task1 = tsWrapper.allocateTask(hostsH1, priority1);
      TezTaskAttemptID task2 = tsWrapper.allocateTask(hostsH1, priority1);
      TezTaskAttemptID task3 = tsWrapper.allocateTask(hostsH1, priority1); // 1 more than capacity.

      tsWrapper.awaitLocalTaskAllocations(2);

      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      verify(tsWrapper.mockAppCallback, times(2))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), any(Container.class));
      assertEquals(2, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));

      reset(tsWrapper.mockAppCallback);

      // No capacity left on node1. The next task should be allocated to node2 after it times out.
      clock.setTime(clock.getTime() + 10000l); // Past the timeout.

      assertEquals(
          LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled.STATE_NOT_RUN,
          delayedTaskSchedulerCallableControlled.lastState);

      delayedTaskSchedulerCallableControlled.triggerGetNextTask();
      delayedTaskSchedulerCallableControlled.awaitGetNextTaskProcessing();

      // Verify that an attempt was made to schedule the task, but the decision was to skip scheduling
      assertEquals(
          LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled.STATE_RETURNED_TASK,
          delayedTaskSchedulerCallableControlled.lastState);
      assertTrue(delayedTaskSchedulerCallableControlled.shouldScheduleTaskTriggered &&
          delayedTaskSchedulerCallableControlled.lastShouldScheduleTaskResult);

      tsWrapper.awaitChangeInTotalAllocations(2);

      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      argumentCaptor = ArgumentCaptor.forClass(Object.class);
      ArgumentCaptor<Container> containerCaptor = ArgumentCaptor.forClass(Container.class);
      verify(tsWrapper.mockAppCallback, times(1))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), containerCaptor.capture());
      assertEquals(1, argumentCaptor.getAllValues().size());
      assertEquals(task3, argumentCaptor.getAllValues().get(0));
      Container assignedContainer = containerCaptor.getValue();
      assertEquals(HOST2, assignedContainer.getNodeId().getHost());


      assertEquals(2, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(1, tsWrapper.ts.dagStats.numNonLocalAllocations);
      assertEquals(1, tsWrapper.ts.dagStats.numDelayedAllocations);
      assertEquals(2, tsWrapper.ts.dagStats.numAllocationsPerHost.get(HOST1).get());
      assertEquals(1, tsWrapper.ts.dagStats.numAllocationsPerHost.get(HOST2).get());

    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testDelayedLocalityDelayedAllocation() throws InterruptedException, IOException {
    Priority priority1 = Priority.newInstance(1);
    String [] hosts = new String[] {HOST1, HOST2};

    String [] hostsH1 = new String[] {HOST1};

    TestTaskSchedulerServiceWrapper tsWrapper =
        new TestTaskSchedulerServiceWrapper(2000, hosts, 1, 1, 10000l, true);
    LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled
        delayedTaskSchedulerCallableControlled =
        (LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled) tsWrapper.ts.delayedTaskSchedulerCallable;
    ControlledClock clock = tsWrapper.getClock();
    clock.setTime(clock.getTime());

    // Fill up host1 with tasks. Leave host2 empty.
    try {
      tsWrapper.controlScheduler(true);
      TezTaskAttemptID task1 = tsWrapper.allocateTask(hostsH1, priority1);
      TezTaskAttemptID task2 = tsWrapper.allocateTask(hostsH1, priority1);
      TezTaskAttemptID task3 = tsWrapper.allocateTask(hostsH1, priority1); // 1 more than capacity.

      tsWrapper.awaitLocalTaskAllocations(2);

      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      verify(tsWrapper.mockAppCallback, times(2))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), any(Container.class));
      assertEquals(2, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));

      reset(tsWrapper.mockAppCallback);

      // Move the clock forward 2000ms, and check the delayed queue
      clock.setTime(clock.getTime() + 2000l); // Past the timeout.

      assertEquals(
          LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled.STATE_NOT_RUN,
          delayedTaskSchedulerCallableControlled.lastState);

      delayedTaskSchedulerCallableControlled.triggerGetNextTask();
      delayedTaskSchedulerCallableControlled.awaitGetNextTaskProcessing();

      // Verify that an attempt was made to schedule the task, but the decision was to skip scheduling
      assertEquals(
          LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled.STATE_TIMEOUT_NOT_EXPIRED,
          delayedTaskSchedulerCallableControlled.lastState);
      assertFalse(delayedTaskSchedulerCallableControlled.shouldScheduleTaskTriggered);

      tsWrapper.deallocateTask(task1, true, null);

      // Node1 now has free capacity. task1 should be allocated to it.
      tsWrapper.awaitChangeInTotalAllocations(2);

      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      argumentCaptor = ArgumentCaptor.forClass(Object.class);
      ArgumentCaptor<Container> containerCaptor = ArgumentCaptor.forClass(Container.class);
      verify(tsWrapper.mockAppCallback, times(1))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), containerCaptor.capture());
      assertEquals(1, argumentCaptor.getAllValues().size());
      assertEquals(task3, argumentCaptor.getAllValues().get(0));
      Container assignedContainer = containerCaptor.getValue();
      assertEquals(HOST1, assignedContainer.getNodeId().getHost());


      assertEquals(3, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(0, tsWrapper.ts.dagStats.numNonLocalAllocations);
      assertEquals(1, tsWrapper.ts.dagStats.numDelayedAllocations);
      assertEquals(3, tsWrapper.ts.dagStats.numAllocationsPerHost.get(HOST1).get());

    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testDelayedQueeTaskSelectionAfterScheduled() throws IOException,
      InterruptedException {
    Priority priority1 = Priority.newInstance(1);
    String [] hosts = new String[] {HOST1, HOST2};

    String [] hostsH1 = new String[] {HOST1};

    TestTaskSchedulerServiceWrapper tsWrapper =
        new TestTaskSchedulerServiceWrapper(2000, hosts, 1, 1, 10000l, true);
    LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled
        delayedTaskSchedulerCallableControlled =
        (LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled) tsWrapper.ts.delayedTaskSchedulerCallable;
    ControlledClock clock = tsWrapper.getClock();
    clock.setTime(clock.getTime());

    // Fill up host1 with tasks. Leave host2 empty.
    try {
      tsWrapper.controlScheduler(true);
      TezTaskAttemptID task1 = tsWrapper.allocateTask(hostsH1, priority1);
      TezTaskAttemptID task2 = tsWrapper.allocateTask(hostsH1, priority1);
      TezTaskAttemptID task3 = tsWrapper.allocateTask(hostsH1, priority1); // 1 more than capacity.

      tsWrapper.awaitLocalTaskAllocations(2);

      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      verify(tsWrapper.mockAppCallback, times(2))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), any(Container.class));
      assertEquals(2, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));

      // Simulate a 2s delay before finishing the task.
      clock.setTime(clock.getTime() + 2000);

      assertEquals(
          LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled.STATE_NOT_RUN,
          delayedTaskSchedulerCallableControlled.lastState);

      delayedTaskSchedulerCallableControlled.triggerGetNextTask();
      delayedTaskSchedulerCallableControlled.awaitGetNextTaskProcessing();
      assertEquals(
          LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled.STATE_TIMEOUT_NOT_EXPIRED,
          delayedTaskSchedulerCallableControlled.lastState);
      assertFalse(delayedTaskSchedulerCallableControlled.shouldScheduleTaskTriggered);

      reset(tsWrapper.mockAppCallback);

      // Now finish task1, which will make capacity for task3 to run. Nothing is coming out of the delayed queue yet.
      tsWrapper.deallocateTask(task1, true, null);
      tsWrapper.awaitLocalTaskAllocations(3);

      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      argumentCaptor = ArgumentCaptor.forClass(Object.class);
      ArgumentCaptor<Container> containerCaptor = ArgumentCaptor.forClass(Container.class);
      verify(tsWrapper.mockAppCallback, times(1))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), containerCaptor.capture());
      assertEquals(1, argumentCaptor.getAllValues().size());
      assertEquals(task3, argumentCaptor.getAllValues().get(0));
      Container assignedContainer = containerCaptor.getValue();
      assertEquals(HOST1, assignedContainer.getNodeId().getHost());

      reset(tsWrapper.mockAppCallback);

      // Move the clock forward and trigger a run.
      clock.setTime(clock.getTime() + 8000); // Set to start + 10000 which is the timeout
      delayedTaskSchedulerCallableControlled.triggerGetNextTask();
      delayedTaskSchedulerCallableControlled.awaitGetNextTaskProcessing();
      assertEquals(
          LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled.STATE_RETURNED_TASK,
          delayedTaskSchedulerCallableControlled.lastState);
      // Verify that an attempt was made to schedule the task, but the decision was to skip scheduling
      assertTrue(delayedTaskSchedulerCallableControlled.shouldScheduleTaskTriggered &&
          !delayedTaskSchedulerCallableControlled.lastShouldScheduleTaskResult);

      // Ensure there's no more invocations.
      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      verify(tsWrapper.mockAppCallback, never()).taskAllocated(any(Object.class), any(Object.class), any(Container.class));

    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout = 10000)
  public void testTaskInfoDelay() {

    LlapTaskSchedulerService.LocalityDelayConf localityDelayConf1 =
        new LlapTaskSchedulerService.LocalityDelayConf(3000);

    ControlledClock clock = new ControlledClock(new MonotonicClock());
    clock.setTime(clock.getTime());


    // With a timeout of 3000.
    LlapTaskSchedulerService.TaskInfo taskInfo =
        new LlapTaskSchedulerService.TaskInfo(localityDelayConf1, clock, new Object(), new Object(),
            mock(Priority.class), mock(Resource.class), null, null, clock.getTime(), null);

    assertFalse(taskInfo.shouldForceLocality());

    assertEquals(3000, taskInfo.getDelay(TimeUnit.MILLISECONDS));
    assertTrue(taskInfo.shouldDelayForLocality(clock.getTime()));

    clock.setTime(clock.getTime() + 500);
    assertEquals(2500, taskInfo.getDelay(TimeUnit.MILLISECONDS));
    assertTrue(taskInfo.shouldDelayForLocality(clock.getTime()));

    clock.setTime(clock.getTime() + 2500);
    assertEquals(0, taskInfo.getDelay(TimeUnit.MILLISECONDS));
    assertFalse(taskInfo.shouldDelayForLocality(clock.getTime()));


    // No locality delay
    LlapTaskSchedulerService.LocalityDelayConf localityDelayConf2 =
        new LlapTaskSchedulerService.LocalityDelayConf(0);
    taskInfo =
        new LlapTaskSchedulerService.TaskInfo(localityDelayConf2, clock, new Object(), new Object(),
            mock(Priority.class), mock(Resource.class), null, null, clock.getTime(), null);
    assertFalse(taskInfo.shouldDelayForLocality(clock.getTime()));
    assertFalse(taskInfo.shouldForceLocality());
    assertTrue(taskInfo.getDelay(TimeUnit.MILLISECONDS) < 0);

    // Force locality
    LlapTaskSchedulerService.LocalityDelayConf localityDelayConf3 =
        new LlapTaskSchedulerService.LocalityDelayConf(-1);
    taskInfo =
        new LlapTaskSchedulerService.TaskInfo(localityDelayConf3, clock, new Object(), new Object(),
            mock(Priority.class), mock(Resource.class), null, null, clock.getTime(), null);
    assertTrue(taskInfo.shouldDelayForLocality(clock.getTime()));
    assertTrue(taskInfo.shouldForceLocality());
    assertFalse(taskInfo.getDelay(TimeUnit.MILLISECONDS) < 0);
  }

  @Test(timeout = 10000)
  public void testLocalityDelayTaskOrdering() throws InterruptedException, IOException {

    LlapTaskSchedulerService.LocalityDelayConf localityDelayConf =
        new LlapTaskSchedulerService.LocalityDelayConf(3000);

    ControlledClock clock = new ControlledClock(new MonotonicClock());
    clock.setTime(clock.getTime());

    DelayQueue<LlapTaskSchedulerService.TaskInfo> delayedQueue = new DelayQueue<>();

    LlapTaskSchedulerService.TaskInfo taskInfo1 =
        new LlapTaskSchedulerService.TaskInfo(localityDelayConf, clock, new Object(), new Object(),
            mock(Priority.class), mock(Resource.class), null, null, clock.getTime(), null);

    clock.setTime(clock.getTime() + 1000);
    LlapTaskSchedulerService.TaskInfo taskInfo2 =
        new LlapTaskSchedulerService.TaskInfo(localityDelayConf, clock, new Object(), new Object(),
            mock(Priority.class), mock(Resource.class), null, null, clock.getTime(), null);

    delayedQueue.add(taskInfo1);
    delayedQueue.add(taskInfo2);

    assertEquals(taskInfo1, delayedQueue.peek());
  }

  @Test (timeout = 15000)
  public void testDelayedLocalityNodeCommErrorImmediateAllocation() throws IOException, InterruptedException {
    Priority priority1 = Priority.newInstance(1);
    String [] hosts = new String[] {HOST1, HOST2};

    String [] hostsH1 = new String[] {HOST1};

    // Node disable timeout higher than locality delay.
    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(20000, hosts, 1, 1, 10000l);

    // Fill up host1 with tasks. Leave host2 empty.
    try {
      long startTime = tsWrapper.getClock().getTime();
      tsWrapper.controlScheduler(true);
      TezTaskAttemptID task1 = tsWrapper.allocateTask(hostsH1, priority1);
      TezTaskAttemptID task2 = tsWrapper.allocateTask(hostsH1, priority1);
      TezTaskAttemptID task3 = tsWrapper.allocateTask(hostsH1, priority1); // 1 more than capacity.

      tsWrapper.awaitLocalTaskAllocations(2);

      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      verify(tsWrapper.mockAppCallback, times(2))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), any(Container.class));
      assertEquals(2, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));

      reset(tsWrapper.mockAppCallback);
      // Flush any pending scheduler runs which may be blocked. Wait 2 seconds for the run to complete.
      tsWrapper.signalSchedulerRun();
      tsWrapper.awaitSchedulerRun(2000l);

      // Mark a task as failed due to a comm failure.
      tsWrapper.deallocateTask(task1, false, TaskAttemptEndReason.COMMUNICATION_ERROR);

      // Node1 marked as failed, node2 has capacity.
      // Timeout for nodes is larger than delay - immediate allocation
      tsWrapper.awaitChangeInTotalAllocations(2);

      long thirdAllocateTime = tsWrapper.getClock().getTime();
      long diff = thirdAllocateTime - startTime;
      // diffAfterSleep < total sleepTime
      assertTrue("Task not allocated in expected time window: duration=" + diff, diff < 10000l);

      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      argumentCaptor = ArgumentCaptor.forClass(Object.class);
      ArgumentCaptor<Container> containerCaptor = ArgumentCaptor.forClass(Container.class);
      verify(tsWrapper.mockAppCallback, times(1))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), containerCaptor.capture());
      assertEquals(1, argumentCaptor.getAllValues().size());
      assertEquals(task3, argumentCaptor.getAllValues().get(0));
      Container assignedContainer = containerCaptor.getValue();
      assertEquals(HOST2, assignedContainer.getNodeId().getHost());


      assertEquals(2, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(1, tsWrapper.ts.dagStats.numNonLocalAllocations);
      assertEquals(1, tsWrapper.ts.dagStats.numDelayedAllocations);
      assertEquals(2, tsWrapper.ts.dagStats.numAllocationsPerHost.get(HOST1).get());
      assertEquals(1, tsWrapper.ts.dagStats.numAllocationsPerHost.get(HOST2).get());

    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test (timeout = 15000)
  public void testDelayedLocalityNodeCommErrorDelayedAllocation() throws IOException, InterruptedException {
    Priority priority1 = Priority.newInstance(1);
    String [] hosts = new String[] {HOST1, HOST2};

    String [] hostsH1 = new String[] {HOST1};

    TestTaskSchedulerServiceWrapper tsWrapper =
        new TestTaskSchedulerServiceWrapper(5000, hosts, 1, 1, 10000l, true);
    LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled
        delayedTaskSchedulerCallableControlled =
        (LlapTaskSchedulerServiceForTestControlled.DelayedTaskSchedulerCallableControlled) tsWrapper.ts.delayedTaskSchedulerCallable;
    ControlledClock clock = tsWrapper.getClock();
    clock.setTime(clock.getTime());

    // Fill up host1 with tasks. Leave host2 empty.
    try {
      tsWrapper.controlScheduler(true);
      TezTaskAttemptID task1 = tsWrapper.allocateTask(hostsH1, priority1);
      TezTaskAttemptID task2 = tsWrapper.allocateTask(hostsH1, priority1);
      TezTaskAttemptID task3 = tsWrapper.allocateTask(hostsH1, priority1); // 1 more than capacity.

      tsWrapper.awaitLocalTaskAllocations(2);

      verify(tsWrapper.mockAppCallback, never()).preemptContainer(any(ContainerId.class));
      ArgumentCaptor<Object> argumentCaptor = ArgumentCaptor.forClass(Object.class);
      verify(tsWrapper.mockAppCallback, times(2))
          .taskAllocated(argumentCaptor.capture(), any(Object.class), any(Container.class));
      assertEquals(2, argumentCaptor.getAllValues().size());
      assertEquals(task1, argumentCaptor.getAllValues().get(0));
      assertEquals(task2, argumentCaptor.getAllValues().get(1));

      reset(tsWrapper.mockAppCallback);

      // Mark a task as failed due to a comm failure.
      tsWrapper.deallocateTask(task1, false, TaskAttemptEndReason.COMMUNICATION_ERROR);

      // Node1 has free capacity but is disabled. Node 2 has capcaity. Delay > re-enable tiemout
      tsWrapper.ensureNoChangeInTotalAllocations(2, 2000l);
    } finally {
      tsWrapper.shutdown();
    }
  }

  private static class TestTaskSchedulerServiceWrapper {
    static final Resource resource = Resource.newInstance(1024, 1);
    Configuration conf;
    TaskSchedulerContext mockAppCallback = mock(TaskSchedulerContext.class);
    LlapServiceInstanceSet mockServiceInstanceSet = mock(LlapServiceInstanceSet.class);
    ControlledClock clock = new ControlledClock(new MonotonicClock());
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(1000, 1), 1);
    LlapTaskSchedulerServiceForTest ts;

    TestTaskSchedulerServiceWrapper() throws IOException, InterruptedException {
      this(2000l);
    }

    TestTaskSchedulerServiceWrapper(long disableTimeoutMillis) throws IOException,
        InterruptedException {
      this(disableTimeoutMillis, new String[]{HOST1, HOST2, HOST3}, 4,
          ConfVars.LLAP_DAEMON_TASK_SCHEDULER_WAIT_QUEUE_SIZE.defaultIntVal);
    }

    TestTaskSchedulerServiceWrapper(long disableTimeoutMillis, String[] hosts, int numExecutors, int waitQueueSize) throws
        IOException, InterruptedException {
      this(disableTimeoutMillis, hosts, numExecutors, waitQueueSize, 0l);
    }

    TestTaskSchedulerServiceWrapper(long nodeDisableTimeoutMillis, String[] hosts, int numExecutors,
                                    int waitQueueSize, long localityDelayMs) throws
        IOException, InterruptedException {
      this(nodeDisableTimeoutMillis, hosts, numExecutors, waitQueueSize, localityDelayMs, false);
    }

    TestTaskSchedulerServiceWrapper(long nodeDisableTimeoutMillis, String[] hosts, int numExecutors,
                                    int waitQueueSize, long localityDelayMs, boolean controlledDelayedTaskQueue) throws
        IOException, InterruptedException {
      this(nodeDisableTimeoutMillis, hosts, numExecutors, waitQueueSize, localityDelayMs, controlledDelayedTaskQueue,
        hosts, false);
    }

    TestTaskSchedulerServiceWrapper(long nodeDisableTimeoutMillis, String[] hosts, int numExecutors,
      int waitQueueSize, long localityDelayMs, boolean controlledDelayedTaskQueue, String[] liveHosts,
      boolean useMockRegistry) throws
      IOException, InterruptedException {
      conf = new Configuration();
      conf.setStrings(ConfVars.LLAP_DAEMON_SERVICE_HOSTS.varname, hosts);
      conf.setInt(ConfVars.LLAP_DAEMON_NUM_EXECUTORS.varname, numExecutors);
      conf.setInt(ConfVars.LLAP_DAEMON_TASK_SCHEDULER_WAIT_QUEUE_SIZE.varname, waitQueueSize);
      conf.set(ConfVars.LLAP_TASK_SCHEDULER_NODE_REENABLE_MIN_TIMEOUT_MS.varname,
        nodeDisableTimeoutMillis + "ms");
      conf.setBoolean(LlapFixedRegistryImpl.FIXED_REGISTRY_RESOLVE_HOST_NAMES, false);
      conf.setLong(ConfVars.LLAP_TASK_SCHEDULER_LOCALITY_DELAY.varname, localityDelayMs);
      conf.set(ConfVars.LLAP_TASK_SCHEDULER_AM_REGISTRY_NAME.varname, "");

      doReturn(appAttemptId).when(mockAppCallback).getApplicationAttemptId();
      doReturn(11111l).when(mockAppCallback).getCustomClusterIdentifier();
      UserPayload userPayload = TezUtils.createUserPayloadFromConf(conf);
      doReturn(userPayload).when(mockAppCallback).getInitialUserPayload();

      if (useMockRegistry) {
        List<LlapServiceInstance> liveInstances = new ArrayList<>();
        for (String host : liveHosts) {
          if (host == null) {
            LlapServiceInstance mockInactive = mock(InactiveServiceInstance.class);
            doReturn(host).when(mockInactive).getHost();
            doReturn("inactive-host-" + host).when(mockInactive).getWorkerIdentity();
            doReturn(ImmutableSet.builder().add(mockInactive).build()).when(mockServiceInstanceSet).getByHost(host);
            liveInstances.add(mockInactive);
          } else {
            LlapServiceInstance mockActive = mock(LlapServiceInstance.class);
            doReturn(host).when(mockActive).getHost();
            doReturn("host-" + host).when(mockActive).getWorkerIdentity();
            doReturn(ImmutableSet.builder().add(mockActive).build()).when(mockServiceInstanceSet).getByHost(host);
            liveInstances.add(mockActive);
          }
        }
        doReturn(liveInstances).when(mockServiceInstanceSet).getAllInstancesOrdered(true);

        List<LlapServiceInstance> allInstances = new ArrayList<>();
        for (String host : hosts) {
          LlapServiceInstance mockActive = mock(LlapServiceInstance.class);
          doReturn(host).when(mockActive).getHost();
          doReturn(Resource.newInstance(100, 1)).when(mockActive).getResource();
          doReturn("host-" + host).when(mockActive).getWorkerIdentity();
          allInstances.add(mockActive);
        }
        doReturn(allInstances).when(mockServiceInstanceSet).getAll();
      }
      if (controlledDelayedTaskQueue) {
        ts = new LlapTaskSchedulerServiceForTestControlled(mockAppCallback, clock);
      } else {
        ts = new LlapTaskSchedulerServiceForTest(mockAppCallback, clock);
      }
      controlScheduler(true);
      ts.initialize();
      ts.start();
      if (useMockRegistry) {
        ts.setServiceInstanceSet(mockServiceInstanceSet);
      }
      // One scheduler pass from the nodes that are added at startup
      signalSchedulerRun();
      controlScheduler(false);
      awaitSchedulerRun();
    }

    ControlledClock getClock() {
      return clock;
    }

    void controlScheduler(boolean val) {
      ts.forTestsetControlScheduling(val);
    }

    void signalSchedulerRun() throws InterruptedException {
      ts.forTestSignalSchedulingRun();
    }

    void awaitSchedulerRun() throws InterruptedException {
      ts.forTestAwaitSchedulingRun(-1);
    }

    /**
     *
     * @param timeoutMs
     * @return false if the time elapsed
     * @throws InterruptedException
     */
    boolean awaitSchedulerRun(long timeoutMs) throws InterruptedException {
      return ts.forTestAwaitSchedulingRun(timeoutMs);
    }

    void resetAppCallback() {
      reset(mockAppCallback);
    }

    void shutdown() {
      ts.shutdown();
    }

    void allocateTask(TezTaskAttemptID task, String[] hosts, Priority priority, Object clientCookie) {
      ts.allocateTask(task, resource, hosts, null, priority, null, clientCookie);
    }

    private static final AtomicInteger TASK_COUNTER = new AtomicInteger(0);
    private static final TezVertexID VERTEX_ID = TezVertexID.getInstance(
        TezDAGID.getInstance(ApplicationId.newInstance(1, 1), 0), 0);
    public static TezTaskAttemptID generateTaskAttemptId() {
      int taskId = TASK_COUNTER.getAndIncrement();
      return TezTaskAttemptID.getInstance(TezTaskID.getInstance(VERTEX_ID, taskId), 0);
    }


    void deallocateTask(Object task, boolean succeeded, TaskAttemptEndReason endReason) {
      ts.deallocateTask(task, succeeded, endReason, null);
    }

    void rejectExecution(Object task) {
      ts.deallocateTask(task, false, TaskAttemptEndReason.EXECUTOR_BUSY, null);
    }


    // More complex methods which may wrap multiple operations
    TezTaskAttemptID allocateTask(String[] hosts, Priority priority) {
      TezTaskAttemptID task = generateTaskAttemptId();
      Object clientCookie = new Object();
      allocateTask(task, hosts, priority, clientCookie);
      return task;
    }

    public void awaitTotalTaskAllocations(int numTasks) throws InterruptedException {
      while (true) {
        signalSchedulerRun();
        awaitSchedulerRun();
        if (ts.dagStats.numTotalAllocations == numTasks) {
          break;
        }
      }
    }

    public void awaitLocalTaskAllocations(int numTasks) throws InterruptedException {
      while (true) {
        signalSchedulerRun();
        awaitSchedulerRun();
        if (ts.dagStats.numLocalAllocations == numTasks) {
          break;
        }
      }
    }

    public void awaitChangeInTotalAllocations(int previousAllocations) throws InterruptedException {
      while (true) {
        signalSchedulerRun();
        awaitSchedulerRun();
        if (ts.dagStats.numTotalAllocations > previousAllocations) {
          break;
        }
        Thread.sleep(200l);
      }
    }

    public void ensureNoChangeInTotalAllocations(int previousAllocations, long timeout) throws
        InterruptedException {
      long startTime = Time.monotonicNow();
      long timeLeft = timeout;
      while (timeLeft > 0) {
        signalSchedulerRun();
        awaitSchedulerRun(Math.min(200, timeLeft));
        if (ts.dagStats.numTotalAllocations != previousAllocations) {
          throw new IllegalStateException("NumTotalAllocations expected to stay at " + previousAllocations + ". Actual=" + ts.dagStats.numTotalAllocations);
        }
        timeLeft = (startTime + timeout) - Time.monotonicNow();
      }
    }
  }

  private static class LlapTaskSchedulerServiceForTest extends LlapTaskSchedulerService {

    private AtomicBoolean controlScheduling = new AtomicBoolean(false);
    private final Lock testLock = new ReentrantLock();
    private final Condition schedulingCompleteCondition = testLock.newCondition();
    private boolean schedulingComplete = false;
    private final Condition triggerSchedulingCondition = testLock.newCondition();
    private boolean schedulingTriggered = false;
    private final AtomicInteger numSchedulerRuns = new AtomicInteger(0);
    private final Object messageLock = new Object();
    private int sentCount = 0, checkAndSendCount = 0;
    private final Object checkDelay = new Object();
    private boolean doDelayCheckAndSend = false;

    public LlapTaskSchedulerServiceForTest(
        TaskSchedulerContext appClient, Clock clock) {
      super(appClient, clock, false);
    }

    @Override
    protected void registerRunningTask(TaskInfo taskInfo) {
      super.registerRunningTask(taskInfo);
      notifyStarted(taskInfo.getAttemptId()); // Do this here; normally communicator does this.
    }

    @Override
    protected void checkAndSendGuaranteedStateUpdate(TaskInfo ti) {
      // A test-specific delay just before the check happens.
      synchronized (checkDelay) {
        boolean isFirst = true;
        while (doDelayCheckAndSend) {
          if (isFirst) {
            synchronized (messageLock) {
              ++checkAndSendCount;
              messageLock.notifyAll();
            }
            isFirst = false;
          }
          try {
            checkDelay.wait(100);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      }
      super.checkAndSendGuaranteedStateUpdate(ti);
    }

    public void setDelayCheckAndSend(boolean value) {
      synchronized (checkDelay) {
        doDelayCheckAndSend = value;
        if (!value) {
          checkDelay.notifyAll();
        }
      }
    }

    @Override
    public void handleUpdateResult(TaskInfo ti, boolean isOk) {
      super.handleUpdateResult(ti, isOk);
    }

    public void clearTestCounts() {
      synchronized (messageLock) {
        sentCount = checkAndSendCount = 0;
      }
    }

    public void waitForMessagesSent(int count) throws InterruptedException {
      while (true) {
        synchronized (messageLock) {
          assert sentCount <= count;
          if (sentCount == count) {
            sentCount = 0;
            return;
          }
          messageLock.wait(200);
        }
      }
    }

    /** Note: this only works for testing the lack of invocations from the main thread. */
    public void assertNoMessagesSent() {
      synchronized (messageLock) {
        assert sentCount == 0;
      }
    }

    public void waitForCheckAndSendCall(int count) throws InterruptedException {
      while (true) {
        synchronized (messageLock) {
          assert checkAndSendCount <= count;
          if (checkAndSendCount == count) {
            checkAndSendCount = 0;
            return;
          }
          messageLock.wait(200);
        }
      }
    }
    @Override
    protected void sendUpdateMessageAsync(TaskInfo ti, boolean newState) {
      synchronized (messageLock) {
        ++sentCount;
        messageLock.notifyAll();
      }
    }

    @Override
    protected TezTaskAttemptID getTaskAttemptId(Object task) {
      if (task instanceof TezTaskAttemptID) {
        return (TezTaskAttemptID)task;
      }
      return null;
    }

    @Override
    protected void schedulePendingTasks() throws InterruptedException {
      LOG.info("Attempted schedulPendingTasks");
      testLock.lock();
      try {
        if (controlScheduling.get()) {
          while (!schedulingTriggered) {
            try {
              triggerSchedulingCondition.await();
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        }
        numSchedulerRuns.incrementAndGet();
        super.schedulePendingTasks();
        schedulingTriggered = false;
        schedulingComplete = true;
        schedulingCompleteCondition.signal();
      } finally {
        testLock.unlock();
      }
    }

    // Enable or disable test scheduling control.
    void forTestsetControlScheduling(boolean control) {
      this.controlScheduling.set(control);
    }

    void forTestSignalSchedulingRun() throws InterruptedException {
      testLock.lock();
      try {
        schedulingTriggered = true;
        triggerSchedulingCondition.signal();
      } finally {
        testLock.unlock();
      }
    }

    boolean forTestAwaitSchedulingRun(long timeout) throws InterruptedException {
      testLock.lock();
      try {
        boolean success = true;
        while (!schedulingComplete) {
          if (timeout == -1) {
            schedulingCompleteCondition.await();
          } else {
            success = schedulingCompleteCondition.await(timeout, TimeUnit.MILLISECONDS);
            break;
          }
        }
        schedulingComplete = false;
        return success;
      } finally {
        testLock.unlock();
      }
    }

  }

  private static class LlapTaskSchedulerServiceForTestControlled extends LlapTaskSchedulerServiceForTest {

    private DelayedTaskSchedulerCallableControlled controlledTSCallable;

    public LlapTaskSchedulerServiceForTestControlled(
        TaskSchedulerContext appClient, Clock clock) {
      super(appClient, clock);
    }

    @Override
    LlapTaskSchedulerService.DelayedTaskSchedulerCallable createDelayedTaskSchedulerCallable() {
      controlledTSCallable = new DelayedTaskSchedulerCallableControlled();
      return controlledTSCallable;
    }

    class DelayedTaskSchedulerCallableControlled extends DelayedTaskSchedulerCallable {
      private final ReentrantLock lock = new ReentrantLock();
      private final Condition triggerRunCondition = lock.newCondition();
      private boolean shouldRun = false;
      private final Condition runCompleteCondition = lock.newCondition();
      private boolean runComplete = false;

      static final int STATE_NOT_RUN = 0;
      static final int STATE_NULL_FOUND = 1;
      static final int STATE_TIMEOUT_NOT_EXPIRED = 2;
      static final int STATE_RETURNED_TASK = 3;

      volatile int lastState = STATE_NOT_RUN;

      volatile boolean lastShouldScheduleTaskResult = false;
      volatile boolean shouldScheduleTaskTriggered = false;

      @Override
      public void processEvictedTask(TaskInfo taskInfo) {
        super.processEvictedTask(taskInfo);
        signalRunComplete();
      }

      @Override
      public TaskInfo getNextTask() throws InterruptedException {

        while (true) {
          lock.lock();
          try {
            while (!shouldRun) {
              triggerRunCondition.await();
            }
            // Preven subsequent runs until a new trigger is set.
            shouldRun = false;
          } finally {
            lock.unlock();
          }
          TaskInfo taskInfo = delayedTaskQueue.peek();
          if (taskInfo == null) {
            LOG.info("Triggered getTask but the queue is empty");
            lastState = STATE_NULL_FOUND;
            signalRunComplete();
            continue;
          }
          if (taskInfo.shouldDelayForLocality(
              LlapTaskSchedulerServiceForTestControlled.this.clock.getTime())) {
            LOG.info("Triggered getTask but the first element is not ready to execute");
            lastState = STATE_TIMEOUT_NOT_EXPIRED;
            signalRunComplete();
            continue;
          } else {
            delayedTaskQueue.poll(); // Remove the previously peeked element.
            lastState = STATE_RETURNED_TASK;
            return taskInfo;
          }
        }
      }

      @Override
      public boolean shouldScheduleTask(TaskInfo taskInfo) {
        shouldScheduleTaskTriggered = true;
        lastShouldScheduleTaskResult = super.shouldScheduleTask(taskInfo);
        return lastShouldScheduleTaskResult;
      }

      void resetShouldScheduleInformation() {
        shouldScheduleTaskTriggered = false;
        lastShouldScheduleTaskResult = false;
      }

      private void signalRunComplete() {
        lock.lock();
        try {
          runComplete = true;
          runCompleteCondition.signal();
        } finally {
          lock.unlock();
        }
      }

      void triggerGetNextTask() {
        lock.lock();
        try {
          shouldRun = true;
          triggerRunCondition.signal();
        } finally {
          lock.unlock();
        }
      }

      void awaitGetNextTaskProcessing() throws InterruptedException {
        lock.lock();
        try {
          while (!runComplete) {
            runCompleteCondition.await();
          }
          runComplete = false;
        } finally {
          lock.unlock();
        }
      }
    }
  }
}
