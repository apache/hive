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
    // q3 is a single-task vertex tha started before q1 so it will take its place
    assertEquals(r3, queue.peek());
    assertNull(queue.offer(r4, 0));
    // q4 can not finish thus q3 remains in top
    assertEquals(r3, queue.peek());
    // offer accepted and r2 gets evicted (latest start-time than q4)
    assertEquals(r2, queue.offer(r5, 0));
    assertEquals(r3, queue.take());
    assertEquals(r1, queue.take());
    assertEquals(r5, queue.take());
    assertEquals(r4, queue.take());

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
    TaskWrapper r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 10, 3, 10, 100, 1, "q1", false), false, 100000); // 7 pending
    TaskWrapper r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 10, 7, 10, 100, 1, "q2", false), false, 100000); // 3 pending
    TaskWrapper r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 10, 5, 10, 100, 1, "q3", false), false, 100000); // 5 pending

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
              createSubmitWorkRequestProto(i, 1, 100, 100 + i, "q" + i);
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
      // Make sure that earlier requests were scheduled first
      assertTrue(curr.getRequestId().compareTo(prev.getRequestId()) > 0);
      prev = curr;
    }
  }

  @Test(timeout = 60000)
  public void testWaitQueueAgingComplex() throws InterruptedException {
    // Make sure we dont have evictions triggered (maxSize = taskSize)
    EvictingPriorityBlockingQueue<TaskWrapper> queue =
            new EvictingPriorityBlockingQueue<>(new ShortestJobFirstComparator(), 10);

    // Single-Task DAGs
    TaskWrapper r1 = createTaskWrapper(createSubmitWorkRequestProto(5, 1, 100, 1000, "q5"), true, 1000);
    TaskWrapper r2 = createTaskWrapper(createSubmitWorkRequestProto(4, 1, 200, 900, "q4"), true, 1000);
    TaskWrapper r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 1, 300, 800, "q3"), true, 1000);
    TaskWrapper r4 = createTaskWrapper(createSubmitWorkRequestProto(2, 1, 400, 700, "q2"), true, 1000);
    TaskWrapper r5 = createTaskWrapper(createSubmitWorkRequestProto(1, 1, 500, 600, "q1"), true, 1000);

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
    TaskWrapper r6 = createTaskWrapper(createSubmitWorkRequestProto(10, 10, 100, 1000, "q10"), true, 1000);
    TaskWrapper r7 = createTaskWrapper(createSubmitWorkRequestProto(9, 10, 200, 900, "q9"), true, 1000);
    TaskWrapper r8 = createTaskWrapper(createSubmitWorkRequestProto(8, 10, 300, 800, "q8"), true, 1000);
    TaskWrapper r9 = createTaskWrapper(createSubmitWorkRequestProto(7, 10, 400, 700, "q7"), true, 1000);
    TaskWrapper r10 = createTaskWrapper(createSubmitWorkRequestProto(6, 10, 500, 600, "q6"), true, 1000);

    assertNull(queue.offer(r6, 0));
    assertEquals(r5, queue.peek());
    assertNull(queue.offer(r7, 0));
    assertEquals(r5, queue.peek());
    assertNull(queue.offer(r8, 0));
    assertEquals(r5, queue.peek());
    assertNull(queue.offer(r9, 0));
    assertEquals(r5, queue.peek());
    assertNull(queue.offer(r10, 0));
    assertEquals(r5, queue.peek());


    TaskWrapper prev = queue.take();
    for (int i = 1; i < 10; i++) {
      TaskWrapper curr = queue.take();
      // Single Task vertices have lower ratio (negative waitime) so they are always scheduled first
      if (i <= 5) {
        assertTrue(curr.getRequestId().compareTo(prev.getRequestId()) > 0);
      }
      // Multi-task vertices are scheduled based on wait time ( so 10->5 in descending order)
      else {
        assertTrue(curr.getRequestId().compareTo(prev.getRequestId()) < 0);
      }
      prev = curr;
    }
  }
}
