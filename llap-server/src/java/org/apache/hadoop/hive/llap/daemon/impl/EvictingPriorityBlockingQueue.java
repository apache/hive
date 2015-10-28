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

import java.util.Comparator;

/**
 * Bounded priority queue that evicts the last element based on priority order specified
 * through comparator. Elements that are added to the queue are sorted based on the specified
 * comparator. If the queue is full and if a new element is added to it, the new element is compared
 * with the last element so as to claim a spot. The evicted element (or the added item) is then
 * returned back. If the queue is not full, new element will be added to queue and null is returned.
 */
public class EvictingPriorityBlockingQueue<E> {
  private final PriorityBlockingDeque<E> deque;
  private final Comparator<E> comparator;

  public EvictingPriorityBlockingQueue(Comparator<E> comparator, int maxSize) {
    this.deque = new PriorityBlockingDeque<>(comparator, maxSize);
    this.comparator = comparator;
  }

  public synchronized E offer(E e) {
    if (deque.offer(e)) {
      return null;
    } else {
      E last = deque.peekLast();
      if (comparator.compare(e, last) < 0) {
        deque.removeLast();
        deque.offer(e);
        return last;
      }
      return e;
    }
  }

  public synchronized boolean isEmpty() {
    return deque.isEmpty();
  }

  public synchronized E peek() {
    return deque.peek();
  }

  public synchronized E take() throws InterruptedException {
    return deque.take();
  }

  public synchronized boolean remove(E e) {
    return deque.remove(e);
  }

  public synchronized int size() {
    return deque.size();
  }

  @Override
  public synchronized String toString() {
    return deque.toString();
  }
}
