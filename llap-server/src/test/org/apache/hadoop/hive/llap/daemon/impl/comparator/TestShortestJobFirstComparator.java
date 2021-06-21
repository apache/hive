/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
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
import static org.junit.Assert.assertTrue;

import org.apache.hadoop.hive.llap.daemon.impl.EvictingPriorityBlockingQueue;
import org.apache.hadoop.hive.llap.daemon.impl.TaskExecutorService.TaskWrapper;
import org.apache.hadoop.hive.llap.daemon.rpc.LlapDaemonProtocolProtos;
import org.junit.Test;

public class TestShortestJobFirstComparator {


  @Test(timeout = 60000)
  public void testWaitQueueComparator() throws InterruptedException {
    TaskWrapper r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 2, 100, 200, "q1"), false, 100000);
    TaskWrapper r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 4, 200, 300, "q2"), false, 100000);
    TaskWrapper r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 6, 300, 400, "q3"), false, 1000000);
    TaskWrapper r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 8, 400, 500, "q4"), false, 1000000);
    TaskWrapper r5 = createTaskWrapper(createSubmitWorkRequestProto(5, 10, 500, 600, "q5"), false, 1000000);
    EvictingPriorityBlockingQueue<TaskWrapper> queue = new EvictingPriorityBlockingQueue<>(
        new ShortestJobFirstComparator(), 4);
    assertNull(queue.offer(r1, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r3, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r4, 0));
    assertEquals(r1, queue.peek());
    // this offer will be rejected
    assertEquals(r5, queue.offer(r5, 0));
    assertEquals(r1, queue.take());
    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r4, queue.take());

    r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 2, 100, 200, "q1"), true, 100000);
    r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 4, 200, 300, "q2"), true, 100000);
    r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 6, 300, 400, "q3"), true, 1000000);
    r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 8, 400, 500, "q4"), true, 1000000);
    r5 = createTaskWrapper(createSubmitWorkRequestProto(5, 10, 500, 600, "q5"), true, 1000000);
    queue = new EvictingPriorityBlockingQueue<TaskWrapper>(
        new ShortestJobFirstComparator(), 4);
    assertNull(queue.offer(r1, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r3, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r4, 0));
    assertEquals(r1, queue.peek());
    // this offer will be rejected
    assertEquals(r5, queue.offer(r5, 0));
    assertEquals(r1, queue.take());
    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r4, queue.take());

    r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 1, 100, 1000, "q1"), true, 100000);
    r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 1, 200, 900, "q2"), false, 100000);
    r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 1, 300, 800, "q3"), true, 1000000);
    r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 1, 400, 700, "q4"), false, 1000000);
    r5 = createTaskWrapper(createSubmitWorkRequestProto(5, 10, 500, 600, "q5"), true, 1000000);
    queue = new EvictingPriorityBlockingQueue<TaskWrapper>(
        new ShortestJobFirstComparator(), 4);
    assertNull(queue.offer(r1, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2, 0));
    // q2 can not finish thus q1 remains in top
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r3, 0));
    // q1 is waiting longer than q3
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r4, 0));
    // q4 can not finish thus q1 remains in top
    assertEquals(r1, queue.peek());
    // offer accepted and r4 gets evicted (later start-time than q4)
    assertEquals(r4, queue.offer(r5, 0));
    assertEquals(r1, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r5, queue.take());
    assertEquals(r2, queue.take());

    r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 2, 100, 200, "q1"), true, 100000);
    r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 4, 200, 300, "q2"), false, 100000);
    r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 6, 300, 400, "q3"), true, 1000000);
    r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 8, 400, 500, "q4"), false, 1000000);
    r5 = createTaskWrapper(createSubmitWorkRequestProto(5, 10, 500, 600, "q5"), true, 1000000);
    queue = new EvictingPriorityBlockingQueue<TaskWrapper>(
        new ShortestJobFirstComparator(), 4);
    assertNull(queue.offer(r1, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r3, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r4, 0));
    assertEquals(r1, queue.peek());
    // offer accepted and r4 gets evicted
    assertEquals(r4, queue.offer(r5, 0));
    assertEquals(r1, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r5, queue.take());
    assertEquals(r2, queue.take());

    r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 2, 100, 200, "q1"), true, 100000);
    r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 4, 200, 300, "q2"), false, 100000);
    r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 6, 300, 400, "q3"), false, 1000000);
    r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 8, 400, 500, "q4"), false, 1000000);
    r5 = createTaskWrapper(createSubmitWorkRequestProto(5, 10, 500, 600, "q5"), true, 1000000);
    queue = new EvictingPriorityBlockingQueue<TaskWrapper>(
        new ShortestJobFirstComparator(), 4);
    assertNull(queue.offer(r1, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r3, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r4, 0));
    assertEquals(r1, queue.peek());
    // offer accepted and r4 gets evicted
    assertEquals(r4, queue.offer(r5, 0));
    assertEquals(r1, queue.take());
    assertEquals(r5, queue.take());
    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());

    r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 2, 100, 200, "q1"), false, 100000);
    r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 4, 200, 300, "q2"), true, 100000);
    r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 6, 300, 400, "q3"), true, 1000000);
    r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 8, 400, 500, "q4"), true, 1000000);
    r5 = createTaskWrapper(createSubmitWorkRequestProto(5, 10, 500, 600, "q5"), true, 1000000);
    queue = new EvictingPriorityBlockingQueue<TaskWrapper>(
        new ShortestJobFirstComparator(), 4);
    assertNull(queue.offer(r1, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2, 0));
    assertEquals(r2, queue.peek());
    assertNull(queue.offer(r3, 0));
    assertEquals(r2, queue.peek());
    assertNull(queue.offer(r4, 0));
    assertEquals(r2, queue.peek());
    // offer accepted, r1 evicted
    assertEquals(r1, queue.offer(r5, 0));
    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r4, queue.take());
    assertEquals(r5, queue.take());
  }

  @Test(timeout = 60000)
  public void testWaitQueueComparatorWithinDagPriority() throws InterruptedException {
    TaskWrapper r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 1, 0, 10, 100, 10), false, 100000);
    TaskWrapper r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 1, 0, 10, 100, 1), false, 100000);
    TaskWrapper r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 1, 0, 10, 100, 5), false, 100000);

    EvictingPriorityBlockingQueue<TaskWrapper> queue = new EvictingPriorityBlockingQueue<>(
        new ShortestJobFirstComparator(), 4);

    assertNull(queue.offer(r1, 0));
    assertNull(queue.offer(r2, 0));
    assertNull(queue.offer(r3, 0));

    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r1, queue.take());
  }

  @Test(timeout = 60000)
  public void testWaitQueueComparatorCanFinish() throws InterruptedException {
    // Test that only the fixed property (...ForQueue) is used in order determination, not the dynamic call.
    TaskWrapper r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 1, 0, 10, 100, 2), true, false, 100000);
    TaskWrapper r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 1, 0, 10, 100, 1), false, true, 100000);
    TaskWrapper r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 1, 0, 10, 100, 5), true, true, 100000);

    EvictingPriorityBlockingQueue<TaskWrapper> queue = new EvictingPriorityBlockingQueue<>(
        new ShortestJobFirstComparator(), 4);

    assertNull(queue.offer(r1, 0));
    assertNull(queue.offer(r2, 0));
    assertNull(queue.offer(r3, 0));

    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r1, queue.take());
  }

  @Test(timeout = 60000)
  public void testWaitQueueComparatorWithinSameDagPriority() throws InterruptedException {
    TaskWrapper r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 1, 0, 10, 1, 10), true, 100000);
    TaskWrapper r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 1, 0, 10, 2, 10), true, 100000);
    TaskWrapper r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 1, 0, 10, 3, 10), true, 100000);

    EvictingPriorityBlockingQueue<TaskWrapper> queue = new EvictingPriorityBlockingQueue<>(
        new ShortestJobFirstComparator(), 3);

    assertNull(queue.offer(r1, 0));
    assertNull(queue.offer(r2, 0));
    assertNull(queue.offer(r3, 0));

    // can not queue more requests as queue is full
    TaskWrapper r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 1, 0, 10, 100, 10), true, 100000);
    assertEquals(r4, queue.offer(r4, 0));

    // add a task with currentAttemptStartTime lesser than existing tasks
    TaskWrapper r0 = createTaskWrapper(createSubmitWorkRequestProto(0, 1, 0, 10, 0, 10), true, 100000);
    assertEquals(r3, queue.offer(r0, 0));
    // ensure that R0 is picked up as it started much earlier.
    assertEquals(r0, queue.take());

    // other tasks
    assertEquals(r1, queue.take());
    assertEquals(r2, queue.take());
  }

  @Test(timeout = 60000)
  public void testWaitQueueComparatorParallelism() throws InterruptedException {
    TaskWrapper r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 10, 3, 10, 100, 1, "q1", 35, false), false, 100000); // 7 pending
    TaskWrapper r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 10, 7, 10, 100, 1, "q2", 35, false), false, 100000); // 3 pending
    TaskWrapper r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 10, 5, 10, 100, 1, "q3", 35, false), false, 100000); // 5 pending

    EvictingPriorityBlockingQueue<TaskWrapper> queue = new EvictingPriorityBlockingQueue<>(
        new ShortestJobFirstComparator(), 4);

    assertNull(queue.offer(r1, 0));
    assertNull(queue.offer(r2, 0));
    assertNull(queue.offer(r3, 0));

    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r1, queue.take());
  }

  @Test(timeout = 60000)
  public void testWaitQueueComparatorAging() throws InterruptedException {
    TaskWrapper r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 10, 100, 200, "q1"), true, 100000);
    TaskWrapper r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 20, 100, 200, "q2"), true, 100000);
    TaskWrapper r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 30, 100, 200, "q3"), true, 100000);

    EvictingPriorityBlockingQueue<TaskWrapper> queue = new EvictingPriorityBlockingQueue<>(
        new ShortestJobFirstComparator(), 4);

    assertNull(queue.offer(r1, 0));
    assertNull(queue.offer(r2, 0));
    assertNull(queue.offer(r3, 0));

    assertEquals(r1, queue.take());
    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());

    // priority = 10 / (200 - 100) = 0.01
    r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 10, 100, 200, "q1"), true, 100000);
    // priority = 20 / (3000 - 100) = 0.0069
    r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 20, 100, 3000, "q2"), true, 100000);
    // priority = 30 / (4000 - 100) = 0.0076
    r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 30, 100, 4000, "q3"), true, 100000);

    queue = new EvictingPriorityBlockingQueue<>(new ShortestJobFirstComparator(), 4);

    assertNull(queue.offer(r1, 0));
    assertNull(queue.offer(r2, 0));
    assertNull(queue.offer(r3, 0));

    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r1, queue.take());
  }

  @Test(timeout = 60000)
  public void testWaitQueueAging() throws InterruptedException {
    // Different Queries (DAGs) where all (different) fragments have
    // upstream parallelism of 1. They also have 1 task, which means first
    // & current attempt time would be the same.
    TaskWrapper[] r = new TaskWrapper[50];

    for (int i = 0; i < 50; i++) {
      LlapDaemonProtocolProtos.SubmitWorkRequestProto proto =
              createSubmitWorkRequestProto(i, 1, 100 + i, 100 + i, "q" + i, true);
      r[i] = createTaskWrapper(proto, true, 100000);
    }

    // Make sure we dont have evictions triggered (maxSize = taskSize)
    EvictingPriorityBlockingQueue<TaskWrapper> queue =
            new EvictingPriorityBlockingQueue<>(new ShortestJobFirstComparator(), 50);

    for (int i = 0; i < 50; i++) {
      assertNull(queue.offer(r[i], 0));
    }

    TaskWrapper prev = queue.take();
    for (int i = 1; i < 50; i++) {
      TaskWrapper curr = queue.take();
      // Make sure order is respected (earlier requestStartTime first)
      assertTrue(curr.getRequestId().compareTo(prev.getRequestId()) > 0);
      prev = curr;
    }
  }

  @Test(timeout = 60000)
  public void testWaitQueueEdgeCases() {
    // Make sure we dont have evictions triggered (maxSize = taskSize)
    EvictingPriorityBlockingQueue<TaskWrapper> queue =
            new EvictingPriorityBlockingQueue<>(new ShortestJobFirstComparator(), 10);

    // same number of pending tasks (longer waitTime has priority)
    // Single task DAG with same start and attempt time (wait-time zero)
    TaskWrapper r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 1, 1000, 1000, "q11", true), true, 1000);
    // Multi task DAG with 11 out of 12 task completed
    TaskWrapper r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 12, 11, 1000, 1500, 1, "q12", 35, true), true, 1000);
    assertNull(queue.offer(r1, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2, 0));
    assertEquals(r2, queue.peek());

    queue.remove(r1);
    queue.remove(r2);
    assertTrue(queue.isEmpty());

    // Single task DAG with different start and attempt time
    r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 1, 800, 1000, "q11", true), true, 1000);
    assertNull(queue.offer(r1, 0));
    assertEquals(r1, queue.peek()); // ratio = 1/200 = 0.005
    assertNull(queue.offer(r2, 0));
    assertEquals(r2, queue.peek()); // ratio = 1/500 = 0.002

    queue.remove(r1);
    queue.remove(r2);
    assertTrue(queue.isEmpty());

    // same waitTime -> lower number of pending has priority
    // Single task DAG with different start and attempt time
    r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 1, 1000, 1000, "q11", true), true, 1000);
    // Multi-task DAG with 5 out of 12
    r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 12, 5, 1000, 1000, 1, "q12", 35, true), true, 1000);

    // pending/wait-time -> r2 has lower priority because it has more pending tasks
    assertNull(queue.offer(r1, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2, 0));
    assertEquals(r1, queue.peek());

    queue.remove(r1);
    queue.remove(r2);
    assertTrue(queue.isEmpty());

    // waitTime1==waitTime2 AND pending1==pending2 -> earlier startTime gets priority
    // Single task DAG with different start and attempt time
    r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 5, 800, 1000, "q11", true), true, 1000);
    // Multi-task DAG with 5 out of 12
    r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 12, 7, 700, 900, 1, "q12", 35, true), true, 1000);

    // r2 started earlier it should thus receive priority
    assertNull(queue.offer(r1, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2, 0));
    assertEquals(r2, queue.peek());
  }

  @Test(timeout = 60000)
  public void testWaitQueueAgingComplex() throws InterruptedException {
    // Make sure we dont have evictions triggered (maxSize = taskSize)
    EvictingPriorityBlockingQueue<TaskWrapper> queue =
            new EvictingPriorityBlockingQueue<>(new ShortestJobFirstComparator(), 10);

    // Single-Task DAGs
    TaskWrapper r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 1, 200, 200, "q1", true), true, 1000);
    TaskWrapper r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 1, 199, 199, "q2", true), true, 1000);
    TaskWrapper r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 1, 300, 310, "q3", true), true, 1000);
    TaskWrapper r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 1, 400, 420, "q4", true), true, 1000);
    TaskWrapper r5 = createTaskWrapper(createSubmitWorkRequestProto(5, 1, 500, 521, "q5", true), true, 1000);

    assertNull(queue.offer(r1, 0));
    assertEquals(r1, queue.peek());
    assertNull(queue.offer(r2, 0));
    assertEquals(r2, queue.peek());
    assertNull(queue.offer(r3, 0));
    assertEquals(r3, queue.peek());
    assertNull(queue.offer(r4, 0));
    assertEquals(r4, queue.peek());
    assertNull(queue.offer(r5, 0));
    assertEquals(r5, queue.peek());

    // Multi-Task DAGs
    TaskWrapper r6 = createTaskWrapper(createSubmitWorkRequestProto(6, 10, 100, 200, "q6", true), true, 1000);
    TaskWrapper r7 = createTaskWrapper(createSubmitWorkRequestProto(7, 10, 200, 400, "q7", true), true, 1000);
    TaskWrapper r8 = createTaskWrapper(createSubmitWorkRequestProto(8, 10, 300, 600, "q8", true), true, 1000);
    TaskWrapper r9 = createTaskWrapper(createSubmitWorkRequestProto(9, 10, 400, 800, "q9", true), true, 1000);
    TaskWrapper r10 = createTaskWrapper(createSubmitWorkRequestProto(10, 10, 500, 1000, "q10", true), true, 1000);

    assertNull(queue.offer(r6, 0));
    assertEquals(r5, queue.peek());
    assertNull(queue.offer(r7, 0));
    assertEquals(r5, queue.peek());
    assertNull(queue.offer(r8, 0));
    assertEquals(r8, queue.peek()); // r5: 1/21 (0.047) -> r8: 10/300 (0.033)
    assertNull(queue.offer(r9, 0));
    assertEquals(r9, queue.peek()); // r9: 10/400 (0.025)
    assertNull(queue.offer(r10, 0));
    assertEquals(r10, queue.peek()); // r10: 10/500 (0.02)
  }
}
