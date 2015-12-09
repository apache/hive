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

package org.apache.tez.dag.app.rm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.configuration.LlapConfiguration;
import org.apache.hadoop.hive.llap.registry.impl.LlapFixedRegistryImpl;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.app.ControlledClock;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.apache.tez.serviceplugins.api.TaskSchedulerContext;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestLlapTaskSchedulerService {

  private static final Logger LOG = LoggerFactory.getLogger(TestLlapTaskSchedulerService.class);

  private static final String HOST1 = "host1";
  private static final String HOST2 = "host2";
  private static final String HOST3 = "host3";

  @Test (timeout = 5000)
  public void testSimpleLocalAllocation() throws IOException, InterruptedException {

    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper();

    try {
      Priority priority1 = Priority.newInstance(1);
      String[] hosts1 = new String[]{HOST1};

      Object task1 = new Object();
      Object clientCookie1 = new Object();

      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, hosts1, priority1, clientCookie1);

      tsWrapper.signalSchedulerRun();
      tsWrapper.awaitSchedulerRun();

      verify(tsWrapper.mockAppCallback).taskAllocated(eq(task1), eq(clientCookie1), any(Container.class));
      // TODO Verify this is on host1.
      assertEquals(1, tsWrapper.ts.dagStats.numLocalAllocations);
    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test (timeout = 5000)
  public void testSimpleNoLocalityAllocation() throws IOException, InterruptedException {
    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper();

    try {
      Priority priority1 = Priority.newInstance(1);

      Object task1 = new Object();
      Object clientCookie1 = new Object();
      tsWrapper.controlScheduler(true);
      tsWrapper.allocateTask(task1, null, priority1, clientCookie1);
      tsWrapper.signalSchedulerRun();
      tsWrapper.awaitSchedulerRun();
      verify(tsWrapper.mockAppCallback).taskAllocated(eq(task1), eq(clientCookie1), any(Container.class));
      assertEquals(1, tsWrapper.ts.dagStats.numAllocationsNoLocalityRequest);
    } finally {
      tsWrapper.shutdown();
    }
  }

  // TODO Add a test to ensure the correct task is being preempted, and the completion for the specific
  // task triggers the next task to be scheduled.

  @Test(timeout=5000)
  public void testPreemption() throws InterruptedException, IOException {

    Priority priority1 = Priority.newInstance(1);
    Priority priority2 = Priority.newInstance(2);
    String [] hosts = new String[] {HOST1};
    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(2000, hosts, 1, 1);
    try {

      Object task1 = "task1";
      Object clientCookie1 = "cookie1";
      Object task2 = "task2";
      Object clientCookie2 = "cookie1";
      Object task3 = "task3";
      Object clientCookie3 = "cookie1";
      Object task4 = "task4";
      Object clientCookie4 = "cookie1";

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

  @Test(timeout=5000)
  public void testNodeDisabled() throws IOException, InterruptedException {
    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(10000l);
    try {
      Priority priority1 = Priority.newInstance(1);
      String[] hosts1 = new String[]{HOST1};
      Object task1 = new Object();
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
      assertEquals(HOST1, disabledNodeInfo.serviceInstance.getHost());
      assertEquals((10000l), disabledNodeInfo.getDelay(TimeUnit.MILLISECONDS));
      assertEquals((10000l + 10000l), disabledNodeInfo.expireTimeMillis);

      Object task2 = new Object();
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

  @Test(timeout=5000)
  public void testNodeReEnabled() throws InterruptedException, IOException {
    // Based on actual timing.
    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(1000l);
    try {
      Priority priority1 = Priority.newInstance(1);
      String[] hosts1 = new String[]{HOST1};
      String[] hosts2 = new String[]{HOST2};
      String[] hosts3 = new String[]{HOST3};

      Object task1 = new Object();
      Object clientCookie1 = new Object();
      Object task2 = new Object();
      Object clientCookie2 = new Object();
      Object task3 = new Object();
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


      Object task4 = new Object();
      Object clientCookie4 = new Object();
      Object task5 = new Object();
      Object clientCookie5 = new Object();
      Object task6 = new Object();
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

  private static class TestTaskSchedulerServiceWrapper {
    static final Resource resource = Resource.newInstance(1024, 1);
    Configuration conf;
    TaskSchedulerContext mockAppCallback = mock(TaskSchedulerContext.class);
    ControlledClock clock = new ControlledClock(new SystemClock());
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
      conf = new Configuration();
      conf.setStrings(ConfVars.LLAP_DAEMON_SERVICE_HOSTS.varname, hosts);
      conf.setInt(ConfVars.LLAP_DAEMON_NUM_EXECUTORS.varname, numExecutors);
      conf.setInt(ConfVars.LLAP_DAEMON_TASK_SCHEDULER_WAIT_QUEUE_SIZE.varname, waitQueueSize);
      conf.set(ConfVars.LLAP_TASK_SCHEDULER_NODE_REENABLE_MIN_TIMEOUT_MS.varname,
          disableTimeoutMillis + "ms");
      conf.setBoolean(LlapFixedRegistryImpl.FIXED_REGISTRY_RESOLVE_HOST_NAMES, false);

      doReturn(appAttemptId).when(mockAppCallback).getApplicationAttemptId();
      doReturn(11111l).when(mockAppCallback).getCustomClusterIdentifier();
      UserPayload userPayload = TezUtils.createUserPayloadFromConf(conf);
      doReturn(userPayload).when(mockAppCallback).getInitialUserPayload();

      ts = new LlapTaskSchedulerServiceForTest(mockAppCallback, clock);

      controlScheduler(true);
      ts.initialize();
      ts.start();
      // One scheduler pass from the nodes that are added at startup
      signalSchedulerRun();
      controlScheduler(false);
      awaitSchedulerRun();
    }

    void controlScheduler(boolean val) {
      ts.forTestsetControlScheduling(val);
    }

    void signalSchedulerRun() throws InterruptedException {
      ts.forTestSignalSchedulingRun();
    }

    void awaitSchedulerRun() throws InterruptedException {
      ts.forTestAwaitSchedulingRun();
    }
    void resetAppCallback() {
      reset(mockAppCallback);
    }

    void shutdown() {
      ts.shutdown();
    }

    void allocateTask(Object task, String[] hosts, Priority priority, Object clientCookie) {
      ts.allocateTask(task, resource, hosts, null, priority, null, clientCookie);
    }

    void deallocateTask(Object task, boolean succeeded, TaskAttemptEndReason endReason) {
      ts.deallocateTask(task, succeeded, endReason, null);
    }

    void rejectExecution(Object task) {
      ts.deallocateTask(task, false, TaskAttemptEndReason.EXECUTOR_BUSY, null);
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


    public LlapTaskSchedulerServiceForTest(
        TaskSchedulerContext appClient, Clock clock) {
      super(appClient, clock);
    }

    @Override
    protected void schedulePendingTasks() {
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

    void forTestAwaitSchedulingRun() throws InterruptedException {
      testLock.lock();
      try {
        while (!schedulingComplete) {
          schedulingCompleteCondition.await();
        }
        schedulingComplete = false;
      } finally {
        testLock.unlock();
      }
    }

  }
}
