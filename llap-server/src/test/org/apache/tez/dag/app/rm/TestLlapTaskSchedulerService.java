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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.daemon.LlapDaemonConfiguration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.tez.dag.api.TaskAttemptEndReason;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ControlledClock;
import org.apache.tez.dag.app.rm.TaskSchedulerService.TaskSchedulerAppCallback;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class TestLlapTaskSchedulerService {

  private static final String HOST1 = "host1";
  private static final String HOST2 = "host2";
  private static final String HOST3 = "host3";

  @Test (timeout = 5000)
  public void testSimpleLocalAllocation() {

    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper();

    try {
      Priority priority1 = Priority.newInstance(1);
      String[] hosts1 = new String[]{HOST1};

      Object task1 = new Object();
      Object clientCookie1 = new Object();
      tsWrapper.allocateTask(task1, hosts1, priority1, clientCookie1);
      verify(tsWrapper.mockAppCallback).taskAllocated(eq(task1), eq(clientCookie1), any(Container.class));
      // TODO Verify this is on host1.
      assertEquals(1, tsWrapper.ts.dagStats.numLocalAllocations);
    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test (timeout = 5000)
  public void testSimpleNoLocalityAllocation() {
    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper();

    try {
      Priority priority1 = Priority.newInstance(1);

      Object task1 = new Object();
      Object clientCookie1 = new Object();
      tsWrapper.allocateTask(task1, null, priority1, clientCookie1);
      verify(tsWrapper.mockAppCallback).taskAllocated(eq(task1), eq(clientCookie1), any(Container.class));
      assertEquals(1, tsWrapper.ts.dagStats.numAllocationsNoLocalityRequest);
    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout=5000)
  public void testNodeDisabled() {
    TestTaskSchedulerServiceWrapper tsWrapper = new TestTaskSchedulerServiceWrapper(10000l);
    try {
      Priority priority1 = Priority.newInstance(1);
      String[] hosts1 = new String[]{HOST1};

      Object task1 = new Object();
      Object clientCookie1 = new Object();
      tsWrapper.allocateTask(task1, hosts1, priority1, clientCookie1);
      verify(tsWrapper.mockAppCallback).taskAllocated(eq(task1), eq(clientCookie1), any(Container.class));
      assertEquals(1, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(0, tsWrapper.ts.dagStats.numAllocationsNoLocalityRequest);

      tsWrapper.resetAppCallback();

      tsWrapper.clock.setTime(10000l);
      tsWrapper.rejectExecution(task1);

      // Verify that the node is blacklisted
      assertEquals(1, tsWrapper.ts.dagStats.numRejectedTasks);
      assertEquals(2, tsWrapper.ts.activeHosts.size());
      assertEquals(2, tsWrapper.ts.activeHostList.length);
      LlapTaskSchedulerService.NodeInfo disabledNodeInfo = tsWrapper.ts.disabledNodes.peek();
      assertNotNull(disabledNodeInfo);
      assertEquals(HOST1, disabledNodeInfo.hostname);
      assertEquals((10000l), disabledNodeInfo.getDelay(TimeUnit.NANOSECONDS));
      assertEquals((10000l + 10000l), disabledNodeInfo.expireTimeMillis);

      Object task2 = new Object();
      Object clientCookie2 = new Object();
      tsWrapper.allocateTask(task2, hosts1, priority1, clientCookie2);
      verify(tsWrapper.mockAppCallback).taskAllocated(eq(task2), eq(clientCookie2), any(Container.class));
      assertEquals(1, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(0, tsWrapper.ts.dagStats.numAllocationsNoLocalityRequest);
      assertEquals(1, tsWrapper.ts.dagStats.numNonLocalAllocations);

      // TODO Enhance this to verify unblacklisting of the node.
    } finally {
      tsWrapper.shutdown();
    }
  }

  @Test(timeout=5000)
  public void testNodeReEnabled() throws InterruptedException {
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
      tsWrapper.allocateTask(task1, hosts1, priority1, clientCookie1);
      tsWrapper.allocateTask(task2, hosts2, priority1, clientCookie2);
      tsWrapper.allocateTask(task3, hosts3, priority1, clientCookie3);
      verify(tsWrapper.mockAppCallback, times(3)).taskAllocated(any(Object.class), any(Object.class), any(Container.class));
      assertEquals(3, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(0, tsWrapper.ts.dagStats.numAllocationsNoLocalityRequest);

      tsWrapper.resetAppCallback();

      tsWrapper.rejectExecution(task1);
      tsWrapper.rejectExecution(task2);
      tsWrapper.rejectExecution(task3);

      // Verify that the node is blacklisted
      assertEquals(3, tsWrapper.ts.dagStats.numRejectedTasks);
      assertEquals(0, tsWrapper.ts.activeHosts.size());
      assertEquals(0, tsWrapper.ts.activeHostList.length);
      assertEquals(3, tsWrapper.ts.disabledNodes.size());


      Object task4 = new Object();
      Object clientCookie4 = new Object();
      Object task5 = new Object();
      Object clientCookie5 = new Object();
      Object task6 = new Object();
      Object clientCookie6 = new Object();
      tsWrapper.allocateTask(task4, hosts1, priority1, clientCookie4);
      tsWrapper.allocateTask(task5, hosts2, priority1, clientCookie5);
      tsWrapper.allocateTask(task6, hosts3, priority1, clientCookie6);

      // Sleep longer than the re-enable timeout.
      Thread.sleep(3000l);

      ArgumentCaptor<Container> argumentCaptor = ArgumentCaptor.forClass(Container.class);
      verify(tsWrapper.mockAppCallback, times(3)).taskAllocated(any(Object.class), any(Object.class), argumentCaptor.capture());

      // Everything should go to host1 since it gets of the list first, and there are no locality delays
      assertEquals(4, tsWrapper.ts.dagStats.numLocalAllocations);
      assertEquals(0, tsWrapper.ts.dagStats.numAllocationsNoLocalityRequest);
      assertEquals(2, tsWrapper.ts.dagStats.numNonLocalAllocations);

      // TODO Enhance this to verify unblacklisting of the node.
    } finally {
      tsWrapper.shutdown();
    }
  }

  private static class TestTaskSchedulerServiceWrapper {
    static final Resource resource = Resource.newInstance(1024, 1);
    Configuration conf;
    TaskSchedulerAppCallback mockAppCallback = mock(TaskSchedulerAppCallback.class);
    AppContext mockAppContext = mock(AppContext.class);
    ControlledClock clock = new ControlledClock(new SystemClock());
    ApplicationAttemptId appAttemptId = ApplicationAttemptId.newInstance(ApplicationId.newInstance(1000, 1), 1);
    LlapTaskSchedulerService ts;

    TestTaskSchedulerServiceWrapper() {
      this(2000l);
    }

    TestTaskSchedulerServiceWrapper(long disableTimeoutMillis) {
      conf = new Configuration();
      conf.setStrings(LlapDaemonConfiguration.LLAP_DAEMON_SERVICE_HOSTS, HOST1, HOST2, HOST3);
      conf.setInt(LlapDaemonConfiguration.LLAP_DAEMON_NUM_EXECUTORS, 4);
      conf.setLong(LlapDaemonConfiguration.LLAP_DAEMON_TASK_SCHEDULER_NODE_REENABLE_TIMEOUT_MILLIS, disableTimeoutMillis);

      doReturn(clock).when(mockAppContext).getClock();
      doReturn(appAttemptId).when(mockAppContext).getApplicationAttemptId();

      ts = new LlapTaskSchedulerServiceForTest(mockAppCallback, mockAppContext, null, 0, null, 11111, conf);

      ts.init(conf);
      ts.start();
    }

    void resetAppCallback() {
      reset(mockAppCallback);
    }

    void shutdown() {
      ts.stop();
    }

    void allocateTask(Object task, String[] hosts, Priority priority, Object clientCookie) {
      ts.allocateTask(task, resource, hosts, null, priority, null, clientCookie);
    }

    void rejectExecution(Object task) {
      ts.deallocateTask(task, false, TaskAttemptEndReason.SERVICE_BUSY);
    }
  }

  private static class LlapTaskSchedulerServiceForTest extends LlapTaskSchedulerService {


    public LlapTaskSchedulerServiceForTest(
        TaskSchedulerAppCallback appClient, AppContext appContext, String clientHostname,
        int clientPort, String trackingUrl, long customAppIdIdentifier,
        Configuration conf) {
      super(appClient, appContext, clientHostname, clientPort, trackingUrl, customAppIdIdentifier,
          conf);
    }

    @Override
    TaskSchedulerAppCallback createAppCallbackDelegate(
        TaskSchedulerAppCallback realAppClient) {
      return realAppClient;
    }
  }
}
