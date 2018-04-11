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
package org.apache.hadoop.hive.llap.daemon.impl.comparator;

import static org.apache.hadoop.hive.llap.daemon.impl.TaskExecutorTestHelpers.createSubmitWorkRequestProto;
import static org.apache.hadoop.hive.llap.daemon.impl.TaskExecutorTestHelpers.createTaskWrapper;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.apache.hadoop.hive.llap.daemon.impl.EvictingPriorityBlockingQueue;
import org.apache.hadoop.hive.llap.daemon.impl.TaskExecutorService.TaskWrapper;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.EntityDescriptorProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.QueryIdentifierProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SignableVertexSpec;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.SubmitWorkRequestProto;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos.VertexOrBinary;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezVertexID;
import org.junit.Test;

public class TestFirstInFirstOutComparator {

  private SubmitWorkRequestProto createRequest(int fragmentNumber, int numSelfAndUpstreamTasks, int dagStartTime,
      int attemptStartTime) {
    // Same priority for all tasks.
    return createRequest(fragmentNumber, numSelfAndUpstreamTasks, 0, dagStartTime, attemptStartTime, 1);
  }

  private SubmitWorkRequestProto createRequest(int fragmentNumber, int numSelfAndUpstreamTasks,
      int numSelfAndUpstreamComplete, int dagStartTime,
      int attemptStartTime, int withinDagPriority) {
    return createRequest(fragmentNumber, numSelfAndUpstreamTasks, numSelfAndUpstreamComplete,
        dagStartTime, attemptStartTime, withinDagPriority, "MockDag");
  }


  private SubmitWorkRequestProto createRequest(int fragmentNumber, int numSelfAndUpstreamTasks,
      int numSelfAndUpstreamComplete, int dagStartTime,
      int attemptStartTime, int withinDagPriority,
      String dagName) {
    ApplicationId appId = ApplicationId.newInstance(9999, 72);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID vId = TezVertexID.getInstance(dagId, 35);
    return SubmitWorkRequestProto
        .newBuilder()
        .setAttemptNumber(0)
        .setFragmentNumber(fragmentNumber)
        .setWorkSpec(
            VertexOrBinary.newBuilder().setVertex(
                SignableVertexSpec
                    .newBuilder()
                    .setQueryIdentifier(
                        QueryIdentifierProto.newBuilder()
                            .setApplicationIdString(appId.toString())
                            .setAppAttemptNumber(0)
                            .setDagIndex(dagId.getId())
                            .build())
                    .setVertexIndex(vId.getId())
                    .setDagName(dagName)
                    .setHiveQueryId(dagName)
                    .setVertexName("MockVertex")
                    .setUser("MockUser")
                    .setTokenIdentifier("MockToken_1")
                    .setProcessorDescriptor(
                        EntityDescriptorProto.newBuilder().setClassName("MockProcessor").build())
                    .build()).build())
        .setAmHost("localhost")
        .setAmPort(12345)
        .setContainerIdString("MockContainer_1")
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

  @Test (timeout = 60000)
  public void testWaitQueueComparator() throws InterruptedException {
    TaskWrapper r1 = createTaskWrapper(createRequest(1, 2, 5, 100), false, 100000);
    TaskWrapper r2 = createTaskWrapper(createRequest(2, 4, 4, 200), false, 100000);
    TaskWrapper r3 = createTaskWrapper(createRequest(3, 6, 3, 300), false, 1000000);
    TaskWrapper r4 = createTaskWrapper(createRequest(4, 8, 2, 400), false, 1000000);
    TaskWrapper r5 = createTaskWrapper(createRequest(5, 10, 1, 500), false, 1000000);
    EvictingPriorityBlockingQueue<TaskWrapper> queue = new EvictingPriorityBlockingQueue<>(
        new FirstInFirstOutComparator(), 4);
    assertNull(queue.offer(r1, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2, 0));
    assertEquals(r2, queue.peek());
    assertNull(queue.offer(r3, 0));
    assertEquals(r3, queue.peek());
    assertNull(queue.offer(r4, 0));
    assertEquals(r4, queue.peek());
    // this offer will be accepted and r1 evicted
    assertEquals(r1, queue.offer(r5, 0));
    assertEquals(r5, queue.take());
    assertEquals(r4, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r2, queue.take());

    r1 = createTaskWrapper(createRequest(1, 2, 5, 100), true, 100000);
    r2 = createTaskWrapper(createRequest(2, 4, 4, 200), true, 100000);
    r3 = createTaskWrapper(createRequest(3, 6, 3, 300), true, 1000000);
    r4 = createTaskWrapper(createRequest(4, 8, 2, 400), true, 1000000);
    r5 = createTaskWrapper(createRequest(5, 10, 1, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue<TaskWrapper>(
        new FirstInFirstOutComparator(), 4);
    assertNull(queue.offer(r1, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2, 0));
    assertEquals(r2, queue.peek());
    assertNull(queue.offer(r3, 0));
    assertEquals(r3, queue.peek());
    assertNull(queue.offer(r4, 0));
    assertEquals(r4, queue.peek());
    // this offer will be accpeted and r1 evicted
    assertEquals(r1, queue.offer(r5, 0));
    assertEquals(r5, queue.take());
    assertEquals(r4, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r2, queue.take());

    r1 = createTaskWrapper(createRequest(1, 1, 5, 100), true, 100000);
    r2 = createTaskWrapper(createRequest(2, 1, 4, 200), false, 100000);
    r3 = createTaskWrapper(createRequest(3, 1, 3, 300), true, 1000000);
    r4 = createTaskWrapper(createRequest(4, 1, 2, 400), false, 1000000);
    r5 = createTaskWrapper(createRequest(5, 10, 1, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue<TaskWrapper>(
        new FirstInFirstOutComparator(), 4);
    assertNull(queue.offer(r1, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r3, 0));
    assertEquals(r3, queue.peek());
    assertNull(queue.offer(r4, 0));
    assertEquals(r3, queue.peek());
    // offer accepted and r2 gets evicted
    assertEquals(r2, queue.offer(r5, 0));
    assertEquals(r5, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r1, queue.take());
    assertEquals(r4, queue.take());

    r1 = createTaskWrapper(createRequest(1, 2, 5, 100), true, 100000);
    r2 = createTaskWrapper(createRequest(2, 4, 4, 200), false, 100000);
    r3 = createTaskWrapper(createRequest(3, 6, 3, 300), true, 1000000);
    r4 = createTaskWrapper(createRequest(4, 8, 2, 400), false, 1000000);
    r5 = createTaskWrapper(createRequest(5, 10, 1, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue<TaskWrapper>(
        new FirstInFirstOutComparator(), 4);
    assertNull(queue.offer(r1, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r3, 0));
    assertEquals(r3, queue.peek());
    assertNull(queue.offer(r4, 0));
    assertEquals(r3, queue.peek());
    // offer accepted and r2 gets evicted
    assertEquals(r2, queue.offer(r5, 0));
    assertEquals(r5, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r1, queue.take());
    assertEquals(r4, queue.take());

    r1 = createTaskWrapper(createRequest(1, 2, 5, 100), true, 100000);
    r2 = createTaskWrapper(createRequest(2, 4, 4, 200), false, 100000);
    r3 = createTaskWrapper(createRequest(3, 6, 3, 300), false, 1000000);
    r4 = createTaskWrapper(createRequest(4, 8, 2, 400), false, 1000000);
    r5 = createTaskWrapper(createRequest(5, 10, 1, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue<TaskWrapper>(
        new FirstInFirstOutComparator(), 4);
    assertNull(queue.offer(r1, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r3, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r4, 0));
    assertEquals(r1, queue.peek());
    // offer accepted and r2 gets evicted
    assertEquals(r2, queue.offer(r5, 0));
    assertEquals(r5, queue.take());
    assertEquals(r1, queue.take());
    assertEquals(r4, queue.take());
    assertEquals(r3, queue.take());

    r1 = createTaskWrapper(createRequest(1, 2, 5, 100), false, 100000);
    r2 = createTaskWrapper(createRequest(2, 4, 4, 200), true, 100000);
    r3 = createTaskWrapper(createRequest(3, 6, 3, 300), true, 1000000);
    r4 = createTaskWrapper(createRequest(4, 8, 2, 400), true, 1000000);
    r5 = createTaskWrapper(createRequest(5, 10, 1, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue<TaskWrapper>(
        new FirstInFirstOutComparator(), 4);
    assertNull(queue.offer(r1, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2, 0));
    assertEquals(r2, queue.peek());
    assertNull(queue.offer(r3, 0));
    assertEquals(r3, queue.peek());
    assertNull(queue.offer(r4, 0));
    assertEquals(r4, queue.peek());
    // offer accepted, r1 evicted
    assertEquals(r1, queue.offer(r5, 0));
    assertEquals(r5, queue.take());
    assertEquals(r4, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r2, queue.take());

    r1 = createTaskWrapper(createRequest(1, 2, 5, 100), false, 100000);
    r2 = createTaskWrapper(createRequest(2, 4, 4, 200), true, 100000);
    r3 = createTaskWrapper(createRequest(3, 6, 3, 300), true, 1000000);
    r4 = createTaskWrapper(createRequest(4, 8, 2, 400), true, 1000000);
    r5 = createTaskWrapper(createRequest(5, 10, 2, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue<TaskWrapper>(
        new FirstInFirstOutComparator(), 4);
    assertNull(queue.offer(r1, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2, 0));
    assertEquals(r2, queue.peek());
    assertNull(queue.offer(r3, 0));
    assertEquals(r3, queue.peek());
    assertNull(queue.offer(r4, 0));
    assertEquals(r4, queue.peek());
    // offer accepted, r1 evicted
    assertEquals(r1, queue.offer(r5, 0));
    assertEquals(r5, queue.take());
    assertEquals(r4, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r2, queue.take());
  }

  @Test(timeout = 60000)
  public void testWaitQueueComparatorCanFinish() throws InterruptedException {
    // Test that only the fixed property (...ForQueue) is used in order determination, not the dynamic call.
    TaskWrapper r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 1, 0, 10, 100, 2), true, false, 100000);
    TaskWrapper r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 1, 0, 10, 100, 1), false, true, 100000);
    TaskWrapper r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 1, 0, 10, 100, 5), true, true, 100000);

    EvictingPriorityBlockingQueue<TaskWrapper> queue = new EvictingPriorityBlockingQueue<TaskWrapper>(
        new FirstInFirstOutComparator(), 4);

    assertNull(queue.offer(r1, 0));
    assertNull(queue.offer(r2, 0));
    assertNull(queue.offer(r3, 0));

    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r1, queue.take());
  }
  
  @Test(timeout = 60000)
  public void testWaitQueueComparatorWithinDagPriority() throws InterruptedException {
    TaskWrapper r1 = createTaskWrapper(createRequest(1, 1, 0, 100, 100, 10), false, 100000);
    TaskWrapper r2 = createTaskWrapper(createRequest(2, 1, 0, 100, 100, 1), false, 100000);
    TaskWrapper r3 = createTaskWrapper(createRequest(3, 1, 0, 100, 100, 5), false, 100000);

    EvictingPriorityBlockingQueue<TaskWrapper> queue = new EvictingPriorityBlockingQueue<>(
        new FirstInFirstOutComparator(), 4);

    assertNull(queue.offer(r1, 0));
    assertNull(queue.offer(r2, 0));
    assertNull(queue.offer(r3, 0));

    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r1, queue.take());
  }

  @Test(timeout = 60000)
  public void testWaitQueueComparatorWithinSameDagPriority() throws InterruptedException {
    TaskWrapper r1 = createTaskWrapper(createRequest(1, 1, 0, 10, 100, 10), true, 100000);
    TaskWrapper r2 = createTaskWrapper(createRequest(2, 1, 0, 10, 100, 10), true, 100000);
    TaskWrapper r3 = createTaskWrapper(createRequest(3, 1, 0, 10, 100, 10), true, 100000);

    EvictingPriorityBlockingQueue<TaskWrapper> queue = new EvictingPriorityBlockingQueue<>(
        new ShortestJobFirstComparator(), 3);

    assertNull(queue.offer(r1, 0));
    assertNull(queue.offer(r2, 0));
    assertNull(queue.offer(r3, 0));

    // can not queue more requests as queue is full
    TaskWrapper r4 = createTaskWrapper(createRequest(4, 1, 0, 10, 100, 10), true, 100000);
    assertEquals(r4, queue.offer(r4, 0));
  }

  @Test(timeout = 60000)
  public void testWaitQueueComparatorParallelism() throws InterruptedException {
    TaskWrapper r1 = createTaskWrapper(createRequest(1, 10, 3, 100, 100, 1, "q1"), false, 100000);
    TaskWrapper r2 = createTaskWrapper(createRequest(2, 10, 7, 100, 100, 1, "q2"), false, 100000);
    TaskWrapper r3 = createTaskWrapper(createRequest(3, 10, 5, 100, 100, 1, "q3"), false, 100000);

    EvictingPriorityBlockingQueue<TaskWrapper> queue = new EvictingPriorityBlockingQueue<>(
        new FirstInFirstOutComparator(), 4);

    assertNull(queue.offer(r1, 0));
    assertNull(queue.offer(r2, 0));
    assertNull(queue.offer(r3, 0));

    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r1, queue.take());
  }
}
