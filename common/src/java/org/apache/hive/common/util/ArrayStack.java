/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.common.util;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

/**
 * ArrayStack is a non-synchronized version of a Stack. It's faster than
 * a Stack, since it doesn't have synchronization lock overhead.
 * Deque is a more preferred interface than Stack, but it lacks indexed
 * accesses, such as get(int). ArrayStack provides get(int) for compatibility.
 * @see Stack
 */
public class ArrayStack<E> extends AbstractCollection<E>
    implements Collection<E> {
  /**
   * An array list is used for indexed accesses.
   */
  protected final List<E> list = new ArrayList<>();

  @Override
  public Iterator<E> iterator() {
    return list.iterator();
  }

  @Override
  public int size() {
    return list.size();
  }

  /**
   * @see Stack#get(int)
   */
  public E get(int index) {
    return list.get(index);
  }

  /**
   * @see Stack#push(Object)
   */
  public void push(E element) {
    list.add(element);
  }

  /**
   * @see Stack#pop()
   */
  public E pop() {
    if (isEmpty()) {
      return null;
    } else {
      return list.remove(list.size() - 1);
    }
  }

  /**
   * @see Stack#peek()
   */
  public E peek() {
    if (isEmpty()) {
      return null;
    } else {
      return list.get(list.size() - 1);
    }
  }
}
