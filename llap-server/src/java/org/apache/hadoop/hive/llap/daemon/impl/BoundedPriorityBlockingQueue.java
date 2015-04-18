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
import java.util.concurrent.PriorityBlockingQueue;

/**
 * Priority blocking queue of bounded size. The entries that are added already added will be
 * ordered based on the specified comparator. If the queue is full, offer() will return false and
 * add() will throw IllegalStateException.
 */
public class BoundedPriorityBlockingQueue<E> extends PriorityBlockingQueue<E> {
  private int maxSize;

  public BoundedPriorityBlockingQueue(int maxSize) {
    this.maxSize = maxSize;
  }

  public BoundedPriorityBlockingQueue(Comparator<E> comparator, int maxSize) {
    super(maxSize, comparator);
    this.maxSize = maxSize;
  }

  @Override
  public boolean add(E e) {
    if (size() >= maxSize) {
      throw new IllegalStateException("BoundedPriorityBlockingQueue is full");
    } else {
      return super.add(e);
    }
  }

  @Override
  public boolean offer(E e) {
    if (size() >= maxSize) {
      return false;
    } else {
      return super.offer(e);
    }
  }
}
