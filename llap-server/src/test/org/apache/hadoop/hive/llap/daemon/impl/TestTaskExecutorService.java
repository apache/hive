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
import static org.mockito.Mockito.mock;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.llap.daemon.KilledTaskHandler;
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
          null, null, null, mock(KilledTaskHandler.class));
      this.workTime = workTime;
      this.canFinish = canFinish;
    }

    @Override
    protected TaskRunner2Result callInternal() {
      System.out.println(requestId + " is executing..");
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
  
  @Test
  public void testWaitQueueComparator() throws InterruptedException {
    MockRequest r1 = new MockRequest(createRequest(1, 2), false, 100000);
    MockRequest r2 = new MockRequest(createRequest(2, 4), false, 100000);
    MockRequest r3 = new MockRequest(createRequest(3, 6), false, 1000000);
    MockRequest r4 = new MockRequest(createRequest(4, 8), false, 1000000);
    MockRequest r5 = new MockRequest(createRequest(5, 10), false, 1000000);
    EvictingPriorityBlockingQueue queue = new EvictingPriorityBlockingQueue(
        new TaskExecutorService.WaitQueueComparator(), 4);
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

    r1 = new MockRequest(createRequest(1, 2), true, 100000);
    r2 = new MockRequest(createRequest(2, 4), true, 100000);
    r3 = new MockRequest(createRequest(3, 6), true, 1000000);
    r4 = new MockRequest(createRequest(4, 8), true, 1000000);
    r5 = new MockRequest(createRequest(5, 10), true, 1000000);
    queue = new EvictingPriorityBlockingQueue(
        new TaskExecutorService.WaitQueueComparator(), 4);
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

    r1 = new MockRequest(createRequest(1, 1), true, 100000);
    r2 = new MockRequest(createRequest(2, 1), false, 100000);
    r3 = new MockRequest(createRequest(3, 1), true, 1000000);
    r4 = new MockRequest(createRequest(4, 1), false, 1000000);
    r5 = new MockRequest(createRequest(5, 10), true, 1000000);
    queue = new EvictingPriorityBlockingQueue(
        new TaskExecutorService.WaitQueueComparator(), 4);
    assertNull(queue.offer(r1));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r3));
    // same priority with r1
    assertEquals(r3, queue.peek());
    // same priority with r2
    assertNull(queue.offer(r4));
    assertEquals(r3, queue.peek());
    // offer accepted and r2 gets evicted
    assertEquals(r2, queue.offer(r5));
    assertEquals(r3, queue.take());
    assertEquals(r1, queue.take());
    assertEquals(r5, queue.take());
    assertEquals(r4, queue.take());

    r1 = new MockRequest(createRequest(1, 2), true, 100000);
    r2 = new MockRequest(createRequest(2, 4), false, 100000);
    r3 = new MockRequest(createRequest(3, 6), true, 1000000);
    r4 = new MockRequest(createRequest(4, 8), false, 1000000);
    r5 = new MockRequest(createRequest(5, 10), true, 1000000);
    queue = new EvictingPriorityBlockingQueue(
        new TaskExecutorService.WaitQueueComparator(), 4);
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

    r1 = new MockRequest(createRequest(1, 2), true, 100000);
    r2 = new MockRequest(createRequest(2, 4), false, 100000);
    r3 = new MockRequest(createRequest(3, 6), false, 1000000);
    r4 = new MockRequest(createRequest(4, 8), false, 1000000);
    r5 = new MockRequest(createRequest(5, 10), true, 1000000);
    queue = new EvictingPriorityBlockingQueue(
        new TaskExecutorService.WaitQueueComparator(), 4);
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

    r1 = new MockRequest(createRequest(1, 2), false, 100000);
    r2 = new MockRequest(createRequest(2, 4), true, 100000);
    r3 = new MockRequest(createRequest(3, 6), true, 1000000);
    r4 = new MockRequest(createRequest(4, 8), true, 1000000);
    r5 = new MockRequest(createRequest(5, 10), true, 1000000);
    queue = new EvictingPriorityBlockingQueue(
        new TaskExecutorService.WaitQueueComparator(), 4);
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
