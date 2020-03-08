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

import org.apache.hadoop.hive.common.io.CacheTag;

/**
 * Buffer that can be managed by LowLevelEvictionPolicy.
 * We want to have cacheable and non-allocator buffers, as well as allocator buffers with no
 * cache dependency, and also ones that are both. Alas, we could only achieve this if we were
 * using a real programming language.
 */
public abstract class LlapCacheableBuffer {
  public static final int INVALIDATE_OK = 0, INVALIDATE_FAILED = 1, INVALIDATE_ALREADY_INVALID = 2;

  public CacheAttribute cacheAttribute;
  /** Linked list pointers for LRFU/LRU cache policies. Given that each block is in cache
   * that might be better than external linked list. Or not, since this is not concurrent. */
  public LlapCacheableBuffer prev = null;
  /** Linked list pointers for LRFU/LRU cache policies. Given that each block is in cache
   * that might be better than external linked list. Or not, since this is not concurrent. */
  public LlapCacheableBuffer next = null;

  /**
   * @return result of invalidation.
   */
  protected abstract int invalidate();

  /**
   * @return size of the buffer in bytes.
   */
  public abstract long getMemoryUsage();

  /**
   * @param evictionDispatcher dispatcher object to be notified.
   */
  public abstract void notifyEvicted(EvictionDispatcher evictionDispatcher);

  /**
   * Set the clock bit to true, should be thread safe
   */
  public abstract void setClockBit();

  /**
   * Set the clock bit to false, should be thread safe.
   */
  public abstract void unSetClockBit();

  /**
   * @return value of the clock bit.
   */
  public abstract boolean isClockBitSet();

  @Override
  public String toString() {
    return "0x" + Integer.toHexString(System.identityHashCode(this));
  }

  public String toStringForCache() {
    return cacheAttribute == null ?
        "NONE" :
        String.format("[ObjectId %s, Entry Attributes %s, Locked %s]",
            Integer.toHexString(hashCode()),
            cacheAttribute.toString(),
            isLocked());

  }

  /**
   * @return human readable tag used by the cache content tracker.
   */
  public abstract CacheTag getTag();

  /**
   * @return true if the buffer is locked as part of query execution.
   */
  protected abstract boolean isLocked();

  public interface CacheAttribute {
    double getPriority();
    void setPriority(double priority);
    long getLastUpdate();
    void setTouchTime(long time);
    int getIndex();
    void setIndex(int index);
  }
}
