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

import org.apache.hadoop.hive.llap.daemon.impl.EvictingPriorityBlockingQueue;
import org.apache.hadoop.hive.llap.daemon.impl.TaskExecutorService.TaskWrapper;
import org.junit.Test;

public class TestShortestJobFirstComparator {


  @Test(timeout = 5000)
  public void testWaitQueueComparator() throws InterruptedException {
    TaskWrapper r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 2, 100), false, 100000);
    TaskWrapper r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 4, 200), false, 100000);
    TaskWrapper r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 6, 300), false, 1000000);
    TaskWrapper r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 8, 400), false, 1000000);
    TaskWrapper r5 = createTaskWrapper(createSubmitWorkRequestProto(5, 10, 500), false, 1000000);
    EvictingPriorityBlockingQueue<TaskWrapper> queue = new EvictingPriorityBlockingQueue<>(
        new ShortestJobFirstComparator(), 4);
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

    r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 2, 100), true, 100000);
    r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 4, 200), true, 100000);
    r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 6, 300), true, 1000000);
    r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 8, 400), true, 1000000);
    r5 = createTaskWrapper(createSubmitWorkRequestProto(5, 10, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue(
        new ShortestJobFirstComparator(), 4);
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

    r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 1, 100), true, 100000);
    r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 1, 200), false, 100000);
    r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 1, 300), true, 1000000);
    r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 1, 400), false, 1000000);
    r5 = createTaskWrapper(createSubmitWorkRequestProto(5, 10, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue(
        new ShortestJobFirstComparator(), 4);
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

    r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 2, 100), true, 100000);
    r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 4, 200), false, 100000);
    r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 6, 300), true, 1000000);
    r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 8, 400), false, 1000000);
    r5 = createTaskWrapper(createSubmitWorkRequestProto(5, 10, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue(
        new ShortestJobFirstComparator(), 4);
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

    r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 2, 100), true, 100000);
    r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 4, 200), false, 100000);
    r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 6, 300), false, 1000000);
    r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 8, 400), false, 1000000);
    r5 = createTaskWrapper(createSubmitWorkRequestProto(5, 10, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue(
        new ShortestJobFirstComparator(), 4);
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

    r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 2, 100), false, 100000);
    r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 4, 200), true, 100000);
    r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 6, 300), true, 1000000);
    r4 = createTaskWrapper(createSubmitWorkRequestProto(4, 8, 400), true, 1000000);
    r5 = createTaskWrapper(createSubmitWorkRequestProto(5, 10, 500), true, 1000000);
    queue = new EvictingPriorityBlockingQueue(
        new ShortestJobFirstComparator(), 4);
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

  @Test(timeout = 5000)
  public void testWaitQueueComparatorWithinDagPriority() throws InterruptedException {
    TaskWrapper r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 1, 0, 100, 10), false, 100000);
    TaskWrapper r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 1, 0, 100, 1), false, 100000);
    TaskWrapper r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 1, 0, 100, 5), false, 100000);

    EvictingPriorityBlockingQueue<TaskWrapper> queue = new EvictingPriorityBlockingQueue<>(
        new ShortestJobFirstComparator(), 4);

    assertNull(queue.offer(r1));
    assertNull(queue.offer(r2));
    assertNull(queue.offer(r3));

    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r1, queue.take());
  }

  @Test(timeout = 5000)
  public void testWaitQueueComparatorParallelism() throws InterruptedException {
    TaskWrapper r1 = createTaskWrapper(createSubmitWorkRequestProto(1, 10, 3, 100, 1), false, 100000); // 7 pending
    TaskWrapper r2 = createTaskWrapper(createSubmitWorkRequestProto(2, 10, 7, 100, 1), false, 100000); // 3 pending
    TaskWrapper r3 = createTaskWrapper(createSubmitWorkRequestProto(3, 10, 5, 100, 1), false, 100000); // 5 pending

    EvictingPriorityBlockingQueue<TaskWrapper> queue = new EvictingPriorityBlockingQueue<>(
        new ShortestJobFirstComparator(), 4);

    assertNull(queue.offer(r1));
    assertNull(queue.offer(r2));
    assertNull(queue.offer(r3));

    assertEquals(r2, queue.take());
    assertEquals(r3, queue.take());
    assertEquals(r1, queue.take());
  }
}
