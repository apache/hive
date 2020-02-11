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
package org.apache.hadoop.hive.llap.daemon.impl;

import java.util.Comparator;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;

/**
 * Bounded priority queue that evicts the last element based on priority order specified
 * through comparator. Elements that are added to the queue are sorted based on the specified
 * comparator. If the queue is full and if a new element is added to it, the new element is compared
 * with the last element so as to claim a spot. The evicted element (or the added item) is then
 * returned back. If the queue is not full, new element will be added to queue and null is returned.
 */
public class EvictingPriorityBlockingQueue<E> {

  private static final Logger LOG =
      LoggerFactory.getLogger(EvictingPriorityBlockingQueue.class);

  private final PriorityBlockingDeque<E> deque;
  private final Comparator<E> comparator;
  @VisibleForTesting
  int waitQueueSize;

  private int currentSize = 0;

  public EvictingPriorityBlockingQueue(Comparator<E> comparator, int maxSize) {
    this.deque = new PriorityBlockingDeque<>(comparator);
    this.waitQueueSize = maxSize;
    this.comparator = comparator;
  }

  public synchronized void apply(Function<E, Boolean> fn) {
    for (E item : deque) {
      boolean isOk = fn.apply(item);
      if (!isOk) return;
    }
  }

  public synchronized void forceOffer(E e) {
    offerToDequeueInternal(e);
    currentSize++;
  }

  public synchronized E offer(E e, int additionalElementsAllowed) {
    if (currentSize < waitQueueSize + additionalElementsAllowed) {
      // Capacity exists.
      offerToDequeueInternal(e);
      currentSize++;
      return null;
    } else {
      if (isEmpty()) {
        // Empty queue. But no capacity available, due to waitQueueSize and additionalElementsAllowed
        // Return the element.
        return e;
      }
      // No capacity. Check if an element needs to be evicted.
      E last = deque.peekLast();
      if (comparator.compare(e, last) < 0) {
        deque.removeLast();
        offerToDequeueInternal(e);
        return last;
      }
      return e;
    }
  }

  public synchronized boolean isEmpty() {
    return currentSize == 0;
  }

  public synchronized E peek() {
    return deque.peek();
  }

  public synchronized E take() throws InterruptedException {
    E e = deque.take();
    currentSize--; // Decrement only if an element was removed.
    return e;
  }

  public synchronized boolean remove(E e) {
    boolean removed = deque.remove(e);
    if (removed) {
      currentSize--;
    }
    return removed;
  }

  private void offerToDequeueInternal(E e) {
    boolean result = deque.offer(e);
    if (!result) {
      LOG.error(
          "Failed to insert element into queue with capacity available. size={}, element={}",
          size(), e);
      throw new RuntimeException(
          "Failed to insert element into queue with capacity available. size=" +
              size());
    }
  }

  public synchronized int size() {
    return currentSize;
  }

  public synchronized void setWaitQueueSize(int waitQueueSize) {
    this.waitQueueSize = waitQueueSize;
  }

  @Override
  public synchronized String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("currentSize=").append(size()).append(", queue=")
        .append(deque.toString());
    return sb.toString();
  }
}
