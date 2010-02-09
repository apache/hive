/**
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

package org.apache.hadoop.hive.ql.exec.persistence;

/**
 * An MRU (Most Recently Used) cache implementation. This implementation
 * maintains a doubly circular linked list and it can be used with an auxiliary
 * data structure such as a HashMap to locate the item quickly.
 */
public class MRU<T extends DCLLItem> {

  T head; // head of the linked list -- MRU; tail (head.prev) will be the LRU

  public MRU() {
    head = null;
  }

  /**
   * Insert a value into the MRU. It will appear as the head.
   */
  public T put(T item) {
    addToHead(item);
    return item;
  }

  /**
   * Remove a item from the MRU list.
   * 
   * @param v
   *          linked list item.
   */
  public void remove(T v) {
    if (v == null) {
      return;
    }
    if (v == head) {
      if (head != head.getNext()) {
        head = (T) head.getNext();
      } else {
        head = null;
      }
    }
    v.remove();
  }

  /**
   * Get the most recently used.
   * 
   * @return the most recently used item.
   */
  public T head() {
    return head;
  }

  /**
   * Get the least recently used.
   * 
   * @return the least recently used item.
   */
  public T tail() {
    return (T) head.getPrev();
  }

  /**
   * Insert a new item as the head.
   * 
   * @param v
   *          the new linked list item to be added to the head.
   */
  private void addToHead(T v) {
    if (head == null) {
      head = v;
    } else {
      head.insertBefore(v);
      head = v;
    }
  }

  /**
   * Move an existing item to the head.
   * 
   * @param v
   *          the linked list item to be moved to the head.
   */
  public void moveToHead(T v) {
    assert (head != null);
    if (head != v) {
      v.remove();
      head.insertBefore(v);
      head = v;
    }
  }

  /**
   * Clear all elements in the MRU list. This is not very efficient (linear)
   * since it will call remove() to every item in the list.
   */
  public void clear() {
    while (head.getNext() != head) {
      head.getNext().remove();
    }
    head.remove();
    head = null;
  }
}
