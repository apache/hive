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

package org.apache.hadoop.hive.ql.exec.tez;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * LlapObjectSubCache. A subcache which lives inside the LlapObjectCache.
 * The subcache maintains two lists
 * 1. List of softreference to the objects
 * 2. List of locks to access the objects.
 */
public class LlapObjectSubCache<T> {
  // List of softreferences
  private Object[] softReferenceList;
  // List of locks to protect the above list
  private List<ReentrantLock> locks;

  // Function to create subCache
  private Object[] createSubCache(int numEntries) {
    return new Object[numEntries];
  }

  // Function to setup locks
  private List<ReentrantLock> createSubCacheLocks(int numEntries) {
    List<ReentrantLock> lockList = new ArrayList<>();
    for (int i = 0; i < numEntries; i++) {
      lockList.add(i, new ReentrantLock());
    }
    return lockList;
  }

  public LlapObjectSubCache(org.apache.hadoop.hive.ql.exec.ObjectCache cache,
                             String subCacheKey,
                             final int numEntries) throws HiveException {
    softReferenceList = cache.retrieve(subCacheKey + "_main", () -> createSubCache(numEntries));
    locks = cache.retrieve(subCacheKey + "_locks", () -> createSubCacheLocks(numEntries));
  }

  public void lock(final int index) {
    locks.get(index).lock();
  }

  public void unlock(final int index) {
    locks.get(index).unlock();
  }


  @SuppressWarnings("unchecked")
  public T get(final int index) {
    // Must be held by same thread
    Preconditions.checkState(locks.get(index).isHeldByCurrentThread());
    if (softReferenceList[index] != null) {
      return ((SoftReference<T>)(softReferenceList[index])).get();
    }
    return null;
  }

  public void set(T value, final int index) {
    // Must be held by same thread
    Preconditions.checkState(locks.get(index).isHeldByCurrentThread());
    softReferenceList[index] = new SoftReference<>(value);
  }
}
