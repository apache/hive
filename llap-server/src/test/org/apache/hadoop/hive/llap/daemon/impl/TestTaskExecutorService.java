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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;

import org.apache.hadoop.conf.Configuration;
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
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.task.EndReason;
import org.apache.tez.runtime.task.TaskRunner2Result;
import org.apache.tez.runtime.task.TezChild;
import org.apache.tez.runtime.task.TezChild.ContainerExecutionResult;
import org.apache.tez.runtime.task.TezChild.ContainerExecutionResult.ExitStatus;
import org.junit.Before;
import org.junit.Test;

public class TestTaskExecutorService {
  private static Configuration conf;
  private static Credentials cred = new Credentials();

  private static class MockRequest extends TaskRunnerCallable {
    private int workTime;
    private boolean canFinish;

    public MockRequest(LlapDaemonProtocolProtos.SubmitWorkRequestProto requestProto,
        boolean canFinish, int workTime) {
      super(requestProto, conf, new ExecutionContextImpl("localhost"), null, null, cred, 0, null,
          null, null, null);
      this.workTime = workTime;
      this.canFinish = canFinish;
    }

    @Override
    protected TaskRunner2Result callInternal() throws Exception {
      System.out.println(requestId + " is executing..");
      Thread.sleep(workTime);
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

  private SubmitWorkRequestProto createRequest(int fragmentNumber, int parallelism) {
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
                .setTaskAttemptIdString(taId.toString()).build()).setAmHost("localhost")
        .setAmPort(12345).setAppAttemptNumber(0).setApplicationIdString("MockApp_1")
        .setContainerIdString("MockContainer_1").setUser("MockUser")
        .setTokenIdentifier("MockToken_1").build();
  }


  @Test(expected = RejectedExecutionException.class)
  public void testThreadPoolRejection() throws InterruptedException {
    TaskExecutorService scheduler = new TaskExecutorService(2, 2, false);
    scheduler.schedule(new MockRequest(createRequest(1, 4), true, 1000));
    Thread.sleep(100);
    scheduler.schedule(new MockRequest(createRequest(2, 4), true, 1000));
    Thread.sleep(100);
    assertEquals(0, scheduler.getPreemptionListSize());
    scheduler.schedule(new MockRequest(createRequest(3, 4), true, 1000));
    Thread.sleep(100);
    scheduler.schedule(new MockRequest(createRequest(4, 4), true, 1000));
    Thread.sleep(100);
    assertEquals(0, scheduler.getPreemptionListSize());
    // this request should be rejected
    scheduler.schedule(new MockRequest(createRequest(5, 8), true, 1000));
  }

  @Test
  public void testPreemption() throws InterruptedException {
    TaskExecutorService scheduler = new TaskExecutorService(2, 2, true);
    scheduler.schedule(new MockRequest(createRequest(1, 4), false, 100000));
    Thread.sleep(100);
    scheduler.schedule(new MockRequest(createRequest(2, 4), false, 100000));
    Thread.sleep(100);
    assertEquals(2, scheduler.getPreemptionListSize());
    // these should invoke preemption
    scheduler.schedule(new MockRequest(createRequest(3, 8), true, 1000));
    Thread.sleep(100);
    scheduler.schedule(new MockRequest(createRequest(4, 8), true, 1000));
    Thread.sleep(100);
    assertEquals(0, scheduler.getPreemptionListSize());
  }

  @Test
  public void testPreemptionOrder() throws InterruptedException {
    TaskExecutorService scheduler = new TaskExecutorService(2, 2, true);
    MockRequest r1 = new MockRequest(createRequest(1, 4), false, 100000);
    scheduler.schedule(r1);
    Thread.sleep(100);
    MockRequest r2 = new MockRequest(createRequest(2, 4), false, 100000);
    scheduler.schedule(r2);
    Thread.sleep(100);
    assertEquals(r1, scheduler.getPreemptionTask());
    // these should invoke preemption
    scheduler.schedule(new MockRequest(createRequest(3, 8), true, 1000));
    // wait till pre-emption to kick-in and complete
    Thread.sleep(100);
    assertEquals(r2, scheduler.getPreemptionTask());
    scheduler.schedule(new MockRequest(createRequest(4, 8), true, 1000));
    // wait till pre-emption to kick-in and complete
    Thread.sleep(100);
    assertEquals(0, scheduler.getPreemptionListSize());
  }

  @Test
  public void testWaitQueueComparator() throws InterruptedException {
    MockRequest r1 = new MockRequest(createRequest(1, 2), false, 100000);
    MockRequest r2 = new MockRequest(createRequest(2, 4), false, 100000);
    MockRequest r3 = new MockRequest(createRequest(3, 6), false, 1000000);
    MockRequest r4 = new MockRequest(createRequest(4, 8), false, 1000000);
    MockRequest r5 = new MockRequest(createRequest(5, 10), false, 1000000);
    BlockingQueue queue = new BoundedPriorityBlockingQueue(
        new TaskExecutorService.WaitQueueComparator(), 4);
    queue.offer(r1);
    assertEquals(r1, queue.peek());
    queue.offer(r2);
    assertEquals(r1, queue.peek());
    queue.offer(r3);
    assertEquals(r1, queue.peek());
    queue.offer(r4);
    assertEquals(r1, queue.peek());
    assertEquals(false, queue.offer(r5));
    assertEquals(r1, queue.take());
    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r4, queue.take());

    r1 = new MockRequest(createRequest(1, 2), true, 100000);
    r2 = new MockRequest(createRequest(2, 4), true, 100000);
    r3 = new MockRequest(createRequest(3, 6), true, 1000000);
    r4 = new MockRequest(createRequest(4, 8), true, 1000000);
    r5 = new MockRequest(createRequest(5, 10), true, 1000000);
    queue = new BoundedPriorityBlockingQueue(
        new TaskExecutorService.WaitQueueComparator(), 4);
    queue.offer(r1);
    assertEquals(r1, queue.peek());
    queue.offer(r2);
    assertEquals(r1, queue.peek());
    queue.offer(r3);
    assertEquals(r1, queue.peek());
    queue.offer(r4);
    assertEquals(r1, queue.peek());
    assertEquals(false, queue.offer(r5));
    assertEquals(r1, queue.take());
    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r4, queue.take());

    r1 = new MockRequest(createRequest(1, 1), true, 100000);
    r2 = new MockRequest(createRequest(2, 1), false, 100000);
    r3 = new MockRequest(createRequest(3, 1), true, 1000000);
    r4 = new MockRequest(createRequest(4, 1), false, 1000000);
    r5 = new MockRequest(createRequest(5, 10), true, 1000000);
    queue = new BoundedPriorityBlockingQueue(
        new TaskExecutorService.WaitQueueComparator(), 4);
    queue.offer(r1);
    assertEquals(r1, queue.peek());
    queue.offer(r2);
    assertEquals(r1, queue.peek());
    queue.offer(r3);
    assertEquals(r1, queue.peek());
    queue.offer(r4);
    assertEquals(r1, queue.peek());
    assertEquals(false, queue.offer(r5));
    assertEquals(r1, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r4, queue.take());
    assertEquals(r2, queue.take());

    r1 = new MockRequest(createRequest(1, 2), true, 100000);
    r2 = new MockRequest(createRequest(2, 4), false, 100000);
    r3 = new MockRequest(createRequest(3, 6), true, 1000000);
    r4 = new MockRequest(createRequest(4, 8), false, 1000000);
    r5 = new MockRequest(createRequest(5, 10), true, 1000000);
    queue = new BoundedPriorityBlockingQueue(
        new TaskExecutorService.WaitQueueComparator(), 4);
    queue.offer(r1);
    assertEquals(r1, queue.peek());
    queue.offer(r2);
    assertEquals(r1, queue.peek());
    queue.offer(r3);
    assertEquals(r1, queue.peek());
    queue.offer(r4);
    assertEquals(r1, queue.peek());
    assertEquals(false, queue.offer(r5));
    assertEquals(r1, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r2, queue.take());
    assertEquals(r4, queue.take());

    r1 = new MockRequest(createRequest(1, 2), true, 100000);
    r2 = new MockRequest(createRequest(2, 4), false, 100000);
    r3 = new MockRequest(createRequest(3, 6), false, 1000000);
    r4 = new MockRequest(createRequest(4, 8), false, 1000000);
    r5 = new MockRequest(createRequest(5, 10), true, 1000000);
    queue = new BoundedPriorityBlockingQueue(
        new TaskExecutorService.WaitQueueComparator(), 4);
    queue.offer(r1);
    assertEquals(r1, queue.peek());
    queue.offer(r2);
    assertEquals(r1, queue.peek());
    queue.offer(r3);
    assertEquals(r1, queue.peek());
    queue.offer(r4);
    assertEquals(r1, queue.peek());
    assertEquals(false, queue.offer(r5));
    assertEquals(r1, queue.take());
    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r4, queue.take());

    r1 = new MockRequest(createRequest(1, 2), false, 100000);
    r2 = new MockRequest(createRequest(2, 4), true, 100000);
    r3 = new MockRequest(createRequest(3, 6), true, 1000000);
    r4 = new MockRequest(createRequest(4, 8), true, 1000000);
    r5 = new MockRequest(createRequest(5, 10), true, 1000000);
    queue = new BoundedPriorityBlockingQueue(
        new TaskExecutorService.WaitQueueComparator(), 4);
    queue.offer(r1);
    assertEquals(r1, queue.peek());
    queue.offer(r2);
    assertEquals(r2, queue.peek());
    queue.offer(r3);
    assertEquals(r2, queue.peek());
    queue.offer(r4);
    assertEquals(r2, queue.peek());
    assertEquals(false, queue.offer(r5));
    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r4, queue.take());
    assertEquals(r1, queue.take());
  }

  @Test
  public void testPreemptionQueueComparator() throws InterruptedException {
    MockRequest r1 = new MockRequest(createRequest(1, 2), false, 100000);
    MockRequest r2 = new MockRequest(createRequest(2, 4), false, 100000);
    MockRequest r3 = new MockRequest(createRequest(3, 6), false, 1000000);
    MockRequest r4 = new MockRequest(createRequest(4, 8), false, 1000000);
    BlockingQueue queue = new PriorityBlockingQueue(4,
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
}
