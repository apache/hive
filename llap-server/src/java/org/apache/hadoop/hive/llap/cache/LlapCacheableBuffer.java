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
package org.apache.hadoop.hive.llap.cache;

/**
 * Buffer that can be managed by LowLevelEvictionPolicy.
 * We want to have cacheable and non-allocator buffers, as well as allocator buffers with no
 * cache dependency, and also ones that are both. Alas, we could only achieve this if we were
 * using a real programming language.
 */
public abstract class LlapCacheableBuffer {
  protected static final int IN_LIST = -2, NOT_IN_CACHE = -1;

  /** Priority for cache policy (should be pretty universal). */
  public double priority;
  /** Last priority update time for cache policy (should be pretty universal). */
  public long lastUpdate = -1;

  // TODO: remove some of these fields as needed?
  /** Linked list pointers for LRFU/LRU cache policies. Given that each block is in cache
   * that might be better than external linked list. Or not, since this is not concurrent. */
  public LlapCacheableBuffer prev = null;
  /** Linked list pointers for LRFU/LRU cache policies. Given that each block is in cache
   * that might be better than external linked list. Or not, since this is not concurrent. */
  public LlapCacheableBuffer next = null;
  /** Index in heap for LRFU/LFU cache policies. */
  public int indexInHeap = NOT_IN_CACHE;

  public static final int INVALIDATE_OK = 0, INVALIDATE_FAILED = 1, INVALIDATE_ALREADY_INVALID = 2;
  protected abstract int invalidate();
  public abstract long getMemoryUsage();
  public abstract void notifyEvicted(EvictionDispatcher evictionDispatcher);

  @Override
  public String toString() {
    return "0x" + Integer.toHexString(System.identityHashCode(this));
  }

  public String toStringForCache() {
    return "[" + Integer.toHexString(hashCode()) + " " + String.format("%1$.2f", priority) + " "
        + lastUpdate + " " + (isLocked() ? "!" : ".") + "]";
  }

  protected abstract boolean isLocked();
}
