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
package org.apache.hadoop.hive.llap.daemon.impl.comparator;

import static org.apache.hadoop.hive.llap.daemon.impl.TaskExecutorTestHelpers.createTaskWrapper;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.daemon.FragmentCompletionHandler;
import org.apache.hadoop.hive.llap.daemon.KilledTaskHandler;
import org.apache.hadoop.hive.llap.daemon.impl.EvictingPriorityBlockingQueue;
import org.apache.hadoop.hive.llap.daemon.impl.QueryFragmentInfo;
import org.apache.hadoop.hive.llap.daemon.impl.TaskExecutorService.TaskWrapper;
import org.apache.hadoop.hive.llap.daemon.impl.TaskRunnerCallable;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.EntityDescriptorProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.FragmentSpecProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.hadoop.shim.DefaultHadoopShim;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.task.EndReason;
import org.apache.tez.runtime.task.TaskRunner2Result;
import org.junit.Before;
import org.junit.Test;

public class TestFirstInFirstOutComparator {
  private static Configuration conf;
  private static Credentials cred = new Credentials();

  private static class MockRequest extends TaskRunnerCallable {
    private int workTime;
    private boolean canFinish;

    public MockRequest(SubmitWorkRequestProto requestProto,
        boolean canFinish, int workTime) {
      super(requestProto, mock(QueryFragmentInfo.class), conf,
          new ExecutionContextImpl("localhost"), null, cred, 0, null, null, null,
          mock(KilledTaskHandler.class), mock(
          FragmentCompletionHandler.class), new DefaultHadoopShim());
      this.workTime = workTime;
      this.canFinish = canFinish;
    }

    @Override
    protected TaskRunner2Result callInternal() {
      System.out.println(super.getRequestId() + " is executing..");
      try {
        Thread.sleep(workTime);
      } catch (InterruptedException e) {
        return new TaskRunner2Result(EndReason.KILL_REQUESTED, null, false);
      }
      return new TaskRunner2Result(EndReason.SUCCESS, null, false);
    }

    @Override
    public boolean canFinish() {
      return canFinish;
    }
  }

  @Before
  public void setup() {
    conf = new Configuration();
  }

  private SubmitWorkRequestProto createRequest(int fragmentNumber, int numSelfAndUpstreamTasks, int dagStartTime,
                                               int attemptStartTime) {
    // Same priority for all tasks.
    return createRequest(fragmentNumber, numSelfAndUpstreamTasks, 0, dagStartTime, attemptStartTime, 1);
  }

  private SubmitWorkRequestProto createRequest(int fragmentNumber, int numSelfAndUpstreamTasks,
                                               int numSelfAndUpstreamComplete, int dagStartTime,
                                               int attemptStartTime, int withinDagPriority) {
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
                .setProcessorDescriptor(
                    EntityDescriptorProto.newBuilder().setClassName("MockProcessor").build())
                .setFragmentIdentifierString(taId.toString()).build()).setAmHost("localhost")
        .setAmPort(12345).setAppAttemptNumber(0).setApplicationIdString("MockApp_1")
        .setContainerIdString("MockContainer_1").setUser("MockUser")
        .setTokenIdentifier("MockToken_1")
        .setFragmentRuntimeInfo(LlapDaemonProtocolProtos
            .FragmentRuntimeInfo
            .newBuilder()
            .setDagStartTime(dagStartTime)
            .setFirstAttemptStartTime(attemptStartTime)
            .setNumSelfAndUpstreamTasks(numSelfAndUpstreamTasks)
            .setNumSelfAndUpstreamCompletedTasks(numSelfAndUpstreamComplete)
            .setWithinDagPriority(withinDagPriority)
            .build())
        .build();
  }

  @Test
  public void testWaitQueueComparator() throws InterruptedException {
    TaskWrapper r1 = createTaskWrapper(createRequest(1, 2, 5, 100), false, 100000);
    TaskWrapper r2 = createTaskWrapper(createRequest(2, 4, 4, 200), false, 100000);
    TaskWrapper r3 = createTaskWrapper(createRequest(3, 6, 3, 300), false, 1000000);
    TaskWrapper r4 = createTaskWrapper(createRequest(4, 8, 2, 400), false, 1000000);
    TaskWrapper r5 = createTaskWrapper(createRequest(5, 10, 1, 500), false, 1000000);
    EvictingPriorityBlockingQueue<TaskWrapper> queue = new EvictingPriorityBlockingQueue<>(
        new FirstInFirstOutComparator(), 4);
    assertNull(queue.offer(r1));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2));
    assertEquals(r2, queue.peek());
    assertNull(queue.offer(r3));
    assertEquals(r3, queue.peek());
    assertNull(queue.offer(r4));
    assertEquals(r4, queue.peek());
    // this offer will be accepted and r1 evicted
    assertEquals(r1, queue.offer(r5));
    assertEquals(r5, queue.take());
    assertEquals(r4, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r2, queue.take());

    r1 = createTaskWrapper(createRequest(1, 2, 5, 100), true, 100000);
    r2 = createTaskWrapper(createRequest(2, 4, 4, 200), true, 100000);
    r3 = createTaskWrapper(createRequest(3, 6, 3, 300), true, 1000000);
    r4 = createTaskWrapper(createRequest(4, 8, 2, 400), true, 1000000);
    r5 = createTaskWrapper(createRequest(5, 10, 1, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue(
        new FirstInFirstOutComparator(), 4);
    assertNull(queue.offer(r1));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2));
    assertEquals(r2, queue.peek());
    assertNull(queue.offer(r3));
    assertEquals(r3, queue.peek());
    assertNull(queue.offer(r4));
    assertEquals(r4, queue.peek());
    // this offer will be accpeted and r1 evicted
    assertEquals(r1, queue.offer(r5));
    assertEquals(r5, queue.take());
    assertEquals(r4, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r2, queue.take());

    r1 = createTaskWrapper(createRequest(1, 1, 5, 100), true, 100000);
    r2 = createTaskWrapper(createRequest(2, 1, 4, 200), false, 100000);
    r3 = createTaskWrapper(createRequest(3, 1, 3, 300), true, 1000000);
    r4 = createTaskWrapper(createRequest(4, 1, 2, 400), false, 1000000);
    r5 = createTaskWrapper(createRequest(5, 10, 1, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue(
        new FirstInFirstOutComparator(), 4);
    assertNull(queue.offer(r1));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r3));
    assertEquals(r3, queue.peek());
    assertNull(queue.offer(r4));
    assertEquals(r3, queue.peek());
    // offer accepted and r2 gets evicted
    assertEquals(r2, queue.offer(r5));
    assertEquals(r5, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r1, queue.take());
    assertEquals(r4, queue.take());

    r1 = createTaskWrapper(createRequest(1, 2, 5, 100), true, 100000);
    r2 = createTaskWrapper(createRequest(2, 4, 4, 200), false, 100000);
    r3 = createTaskWrapper(createRequest(3, 6, 3, 300), true, 1000000);
    r4 = createTaskWrapper(createRequest(4, 8, 2, 400), false, 1000000);
    r5 = createTaskWrapper(createRequest(5, 10, 1, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue(
        new FirstInFirstOutComparator(), 4);
    assertNull(queue.offer(r1));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r3));
    assertEquals(r3, queue.peek());
    assertNull(queue.offer(r4));
    assertEquals(r3, queue.peek());
    // offer accepted and r2 gets evicted
    assertEquals(r2, queue.offer(r5));
    assertEquals(r5, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r1, queue.take());
    assertEquals(r4, queue.take());

    r1 = createTaskWrapper(createRequest(1, 2, 5, 100), true, 100000);
    r2 = createTaskWrapper(createRequest(2, 4, 4, 200), false, 100000);
    r3 = createTaskWrapper(createRequest(3, 6, 3, 300), false, 1000000);
    r4 = createTaskWrapper(createRequest(4, 8, 2, 400), false, 1000000);
    r5 = createTaskWrapper(createRequest(5, 10, 1, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue(
        new FirstInFirstOutComparator(), 4);
    assertNull(queue.offer(r1));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r3));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r4));
    assertEquals(r1, queue.peek());
    // offer accepted and r2 gets evicted
    assertEquals(r2, queue.offer(r5));
    assertEquals(r5, queue.take());
    assertEquals(r1, queue.take());
    assertEquals(r4, queue.take());
    assertEquals(r3, queue.take());

    r1 = createTaskWrapper(createRequest(1, 2, 5, 100), false, 100000);
    r2 = createTaskWrapper(createRequest(2, 4, 4, 200), true, 100000);
    r3 = createTaskWrapper(createRequest(3, 6, 3, 300), true, 1000000);
    r4 = createTaskWrapper(createRequest(4, 8, 2, 400), true, 1000000);
    r5 = createTaskWrapper(createRequest(5, 10, 1, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue(
        new FirstInFirstOutComparator(), 4);
    assertNull(queue.offer(r1));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2));
    assertEquals(r2, queue.peek());
    assertNull(queue.offer(r3));
    assertEquals(r3, queue.peek());
    assertNull(queue.offer(r4));
    assertEquals(r4, queue.peek());
    // offer accepted, r1 evicted
    assertEquals(r1, queue.offer(r5));
    assertEquals(r5, queue.take());
    assertEquals(r4, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r2, queue.take());

    r1 = createTaskWrapper(createRequest(1, 2, 5, 100), false, 100000);
    r2 = createTaskWrapper(createRequest(2, 4, 4, 200), true, 100000);
    r3 = createTaskWrapper(createRequest(3, 6, 3, 300), true, 1000000);
    r4 = createTaskWrapper(createRequest(4, 8, 2, 400), true, 1000000);
    r5 = createTaskWrapper(createRequest(5, 10, 2, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue(
        new FirstInFirstOutComparator(), 4);
    assertNull(queue.offer(r1));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2));
    assertEquals(r2, queue.peek());
    assertNull(queue.offer(r3));
    assertEquals(r3, queue.peek());
    assertNull(queue.offer(r4));
    assertEquals(r4, queue.peek());
    // offer accepted, r1 evicted
    assertEquals(r1, queue.offer(r5));
    assertEquals(r4, queue.take());
    assertEquals(r5, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r2, queue.take());
  }

  @Test(timeout = 5000)
  public void testWaitQueueComparatorWithinDagPriority() throws InterruptedException {
    TaskWrapper r1 = createTaskWrapper(createRequest(1, 1, 0, 100, 100, 10), false, 100000);
    TaskWrapper r2 = createTaskWrapper(createRequest(2, 1, 0, 100, 100, 1), false, 100000);
    TaskWrapper r3 = createTaskWrapper(createRequest(3, 1, 0, 100, 100, 5), false, 100000);

    EvictingPriorityBlockingQueue<TaskWrapper> queue = new EvictingPriorityBlockingQueue<>(
        new FirstInFirstOutComparator(), 4);

    assertNull(queue.offer(r1));
    assertNull(queue.offer(r2));
    assertNull(queue.offer(r3));

    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r1, queue.take());
  }

  @Test(timeout = 5000)
  public void testWaitQueueComparatorParallelism() throws InterruptedException {
    TaskWrapper r1 = createTaskWrapper(createRequest(1, 10, 3, 100, 100, 1), false, 100000);
    TaskWrapper r2 = createTaskWrapper(createRequest(2, 10, 7, 100, 100, 1), false, 100000);
    TaskWrapper r3 = createTaskWrapper(createRequest(3, 10, 5, 100, 100, 1), false, 100000);

    EvictingPriorityBlockingQueue<TaskWrapper> queue = new EvictingPriorityBlockingQueue<>(
        new FirstInFirstOutComparator(), 4);

    assertNull(queue.offer(r1));
    assertNull(queue.offer(r2));
    assertNull(queue.offer(r3));

    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r1, queue.take());
  }
}
