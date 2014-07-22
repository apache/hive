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

package org.apache.hadoop.hive.ql.exec.vector;

import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * 
 */
public class VectorUtilBatchObjectPool<T extends Object> {
  private final T[] buffer;
  
  /**
   * Head of the pool. This is where where we should insert the next
   * object returned to the pool  
   */
  private int head = 0;
  
  /**
   * Count of available elements. They are behind the head, with wrap-around
   * The head itself is not free, is null
   */
  private int count = 0;
  
  private IAllocator<T> allocator; 
  
  public static interface IAllocator<T> {
    public T alloc() throws HiveException;
    public void free(T t);
  }
  
  @SuppressWarnings("unchecked")
  public VectorUtilBatchObjectPool(int size, IAllocator<T> allocator) {
    buffer = (T[]) new Object[size];
    this.allocator = allocator;
  }
  
  public T getFromPool() throws HiveException {
    T ret = null;
    if (count == 0) {
      // Pool is exhausted, return a new object
      ret = allocator.alloc();
    }
    else {
      int tail = (head + buffer.length - count) % buffer.length;
      ret = buffer[tail];
      buffer[tail] = null;
      --count;
    }
    
    return ret;
  }
  
  public void putInPool(T object) {
    if (count < buffer.length) {
      buffer[head] = object;
      ++count;
      ++head;
      if (head == buffer.length) {
        head = 0;
      }
    }
    else {
      allocator.free(object);
    }
  }
}
