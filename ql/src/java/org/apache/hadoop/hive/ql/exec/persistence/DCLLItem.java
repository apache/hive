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
 * Doubly circular linked list item.
 */
public class DCLLItem {

  DCLLItem prev;
  DCLLItem next;

  DCLLItem() {
    prev = next = this;
  }

  /**
   * Get the next item.
   * 
   * @return the next item.
   */
  public DCLLItem getNext() {
    return next;
  }

  /**
   * Get the previous item.
   * 
   * @return the previous item.
   */
  public DCLLItem getPrev() {
    return prev;
  }

  /**
   * Set the next item as itm.
   * 
   * @param itm
   *          the item to be set as next.
   */
  public void setNext(DCLLItem itm) {
    next = itm;
  }

  /**
   * Set the previous item as itm.
   * 
   * @param itm
   *          the item to be set as previous.
   */
  public void setPrev(DCLLItem itm) {
    prev = itm;
  }

  /**
   * Remove the current item from the doubly circular linked list.
   */
  public void remove() {
    next.prev = prev;
    prev.next = next;
    prev = next = null;
  }

  /**
   * Add v as the previous of the current list item.
   * 
   * @param v
   *          inserted item.
   */
  public void insertBefore(DCLLItem v) {
    prev.next = v;
    v.prev = prev;
    v.next = this;
    prev = v;
  }

  /**
   * Add v as the previous of the current list item.
   * 
   * @param v
   *          inserted item.
   */
  public void insertAfter(DCLLItem v) {
    next.prev = v;
    v.next = next;
    v.prev = this;
    next = v;
  }
}
