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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import javax.annotation.concurrent.GuardedBy;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Clock eviction policy. Uses a simple circular list to keep a ring of current used buffers.
 * New entries are added to tail of the clock hand AKA (clockHand.prev)
 * Eviction start at the current clock hand following the next pointer.
 *
 */
public class ClockCachePolicy implements LowLevelCachePolicy {

  private static final int DEFAULT_MAX_CIRCLES = 5;

  /**
   * Lock to protect the state of the policy, used as mutex when modifying the circular linked list.
   */
  private final Lock listLock = new ReentrantLock();

  /**
   * The clock hand shared between threads thus made volatile, to ensure state when read outside of lock.
   */
  @GuardedBy("listLock")
  private volatile LlapCacheableBuffer clockHand;

  private EvictionListener evictionListener;
  /**
   * Max number of clock rotation before giving up on clock operation like eviction.
   */
  private final int maxCircles;

  public ClockCachePolicy() {
    maxCircles = DEFAULT_MAX_CIRCLES;
  }

  public ClockCachePolicy(int maxCircles) {
    Preconditions.checkState(maxCircles > 0, "Maximum number of clock rotation must be positive and got " + maxCircles);
    this.maxCircles = maxCircles;
  }

  /**
   * Signals to the policy the addition of a new entry to the cache. An entry come with a priority that can be used as
   * a hint to replacement policy.
   *
   * @param buffer   buffer to be cached
   * @param priority the priority of cached element
   */
  @Override public void cache(LlapCacheableBuffer buffer, LowLevelCache.Priority priority) {
    listLock.lock();
    try {
      //noinspection NonAtomicOperationOnVolatileField
      clockHand = appendToCircularList(clockHand, buffer);
    } finally {
      listLock.unlock();
    }
  }

  /**
   * Appends new entry to the tail of circular list.
   *
   * @param head   circular list head.
   * @param buffer new entry to be added.
   * @return the ring head.
   */
  private static LlapCacheableBuffer appendToCircularList(LlapCacheableBuffer head, LlapCacheableBuffer buffer) {
    if (head == null) {
      return linkToItSelf(buffer);
    }
    buffer.next = head;
    buffer.prev = head.prev;
    head.prev.next = buffer;
    head.prev = buffer;
    return head;
  }

  /**
   * Links the entry to it self to form a ring.
   *
   * @param buffer input
   * @return buffer
   */
  private static LlapCacheableBuffer linkToItSelf(LlapCacheableBuffer buffer) {
    buffer.prev = buffer;
    buffer.next = buffer;
    return buffer;
  }

  @Override public void notifyLock(LlapCacheableBuffer buffer) {
    buffer.setClockBit();
  }

  /**
   * Notifies the policy that a buffer is unlocked after been used. This notification signals to the policy that an
   * access to this page occurred.
   *
   * @param buffer buffer that just got unlocked after a read.
   */
  @Override public void notifyUnlock(LlapCacheableBuffer buffer) {

  }

  /**
   * Signals to the policy that it has to evict some entries from the cache.
   * Policy has to at least evict the amount memory requested.
   * Not that is method will block until at least {@code memoryToReserve} bytes are evicted.
   *
   * @param memoryToReserve amount of bytes to be evicted
   * @return actual amount of evicted bytes.
   */
  @Override public long evictSomeBlocks(long memoryToReserve) {
    long evicted = 0;
    if (clockHand == null) {
      return evicted;
    }
    int fullClockRotation = 0;
    listLock.lock();
    try {
      // ring tail is used to mark a clock circle
      LlapCacheableBuffer ringTail = clockHand.prev;
      // ring head is the current clock position that is under lock. Using local var under lock and updating actual
      // clock position as soon we are done with looping
      LlapCacheableBuffer currentClockHead = clockHand;

      while (evicted < memoryToReserve && currentClockHead != null && fullClockRotation < maxCircles) {
        if (ringTail == currentClockHead) {
          fullClockRotation++;
        }
        if (currentClockHead.isClockBitSet()) {
          // case the buffer getting second chance.
          currentClockHead.unSetClockBit();
          currentClockHead = currentClockHead.next;
        } else {
          // try to evict this victim
          int invalidateFlag = currentClockHead.invalidate();
          if (invalidateFlag == LlapCacheableBuffer.INVALIDATE_OK
              || invalidateFlag == LlapCacheableBuffer.INVALIDATE_ALREADY_INVALID) {
            if (invalidateFlag == LlapCacheableBuffer.INVALIDATE_OK) {
              // case we are able to evict the buffer notify and account for it.
              evictionListener.notifyEvicted(currentClockHead);
              evicted += currentClockHead.getMemoryUsage();
            }
            LlapCacheableBuffer newHand = currentClockHead.next;
            if (newHand == currentClockHead) {
              // end of the ring we have looped, nothing else can be done...
              currentClockHead = null;
              break;
            } else {
              //remove it from the ring.
              if (currentClockHead == ringTail) {
                // we are about to remove the current ring tail thus we need to compute new tail
                ringTail = ringTail.prev;
              }
              currentClockHead.prev.next = newHand;
              newHand.prev = currentClockHead.prev;
              currentClockHead = newHand;
            }
          } else if (invalidateFlag == LlapCacheableBuffer.INVALIDATE_FAILED) {
            // can not be evicted case locked
            currentClockHead = currentClockHead.next;
          } else {
            throw new IllegalStateException("Unknown invalidation flag " + invalidateFlag);
          }
        }
      }
      // done with clock rotations, update the current clock hand under lock.
      clockHand = currentClockHead;
      return evicted;
    } finally {
      listLock.unlock();
    }
  }

  @Override public void setEvictionListener(EvictionListener listener) {
    evictionListener = listener;
  }

  @Override public long purge() {
    return evictSomeBlocks(Long.MAX_VALUE);
  }

  @Override public void debugDumpShort(StringBuilder sb) {
    if (clockHand == null) {
      sb.append("Clock is empty");
      return;
    }
    listLock.lock();
    try {
      sb.append("Clock Status\n");
      LlapCacheableBuffer currentClockHand = clockHand;
      LlapCacheableBuffer lastElement = clockHand.prev;
      while (currentClockHand != lastElement) {
        sb.append(currentClockHand.toStringForCache());
        currentClockHand = currentClockHand.next;
      }
      sb.append(lastElement.toStringForCache());
    } finally {
      listLock.unlock();
    }
  }

  @VisibleForTesting protected Iterator<LlapCacheableBuffer> getIterator() {
    final LlapCacheableBuffer currentHead = clockHand;
    if (currentHead == null) {
      return new Iterator<LlapCacheableBuffer>() {
        @Override public boolean hasNext() {
          return false;
        }

        @Override public LlapCacheableBuffer next() {
          throw new NoSuchElementException("empty iterator");
        }
      };
    }
    final LlapCacheableBuffer tail = clockHand.prev;
    return new Iterator<LlapCacheableBuffer>() {
      LlapCacheableBuffer current = currentHead;
      private boolean isLast = false;

      @Override public boolean hasNext() {
        return !isLast;
      }

      @Override public LlapCacheableBuffer next() {
        if (isLast) {
          throw new NoSuchElementException("Iterator done");
        }
        if (current == tail) {
          isLast = true;
        }
        LlapCacheableBuffer r = current;
        current = current.next;
        return r;
      }
    };
  }

  @VisibleForTesting public LlapCacheableBuffer getClockHand() {
    return clockHand;
  }
}
