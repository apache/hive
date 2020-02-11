/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;

import com.google.common.annotations.VisibleForTesting;

/**
 * We want to have cacheable and non-allocator buffers, as well as allocator buffers with no
 * cache dependency, and also ones that are both. Alas, we could only achieve this if we were
 * using a real programming language.
 */
public abstract class LlapAllocatorBuffer extends LlapCacheableBuffer implements MemoryBuffer {
  private final AtomicLong state = new AtomicLong(0);

  public ByteBuffer byteBuffer;
  /** Allocator uses this to remember the allocation size. Somewhat redundant with header. */
  public int allocSize;

  public void initialize(ByteBuffer byteBuffer, int offset, int length) {
    this.byteBuffer = byteBuffer.slice();
    this.byteBuffer.position(offset);
    this.byteBuffer.limit(offset + length);
    this.allocSize = length;
  }

  public void initializeWithExistingSlice(ByteBuffer byteBuffer, int allocSize) {
    this.byteBuffer = byteBuffer;
    this.allocSize = allocSize;
  }

  public void setNewAllocLocation(int arenaIx, int headerIx) {
    assert state.get() == 0 : "New buffer state is not 0 " + this;
    long newState = State.setFlag(State.setLocation(0, arenaIx, headerIx), State.FLAG_NEW_ALLOC);
    if (!state.compareAndSet(0, newState)) {
      throw new AssertionError("Contention on the new buffer " + this);
    }
  }

  @Override
  public ByteBuffer getByteBufferDup() {
    return byteBuffer.duplicate();
  }

  @Override
  public ByteBuffer getByteBufferRaw() {
    return byteBuffer;
  }

  @Override
  public long getMemoryUsage() {
    return allocSize;
  }

  public int incRef() {
    return incRefInternal(true);
  }

  int tryIncRef() {
    return incRefInternal(false);
  }

  static final int INCREF_EVICTED = -1, INCREF_FAILED = -2;
  private int incRefInternal(boolean doWait) {
    long newValue = -1;
    while (true) {
      long oldValue = state.get();
      if (State.hasFlags(oldValue, State.FLAG_EVICTED)) return INCREF_EVICTED;
      if (State.hasFlags(oldValue, State.FLAG_MOVING)) {
        if (!doWait || !waitForState()) return INCREF_FAILED; // Thread is being interrupted.
        continue;
      }
      int oldRefCount = State.getRefCount(oldValue);
      assert oldRefCount >= 0 : "oldValue is " + oldValue + " " + this;
      if (oldRefCount == State.MAX_REFCOUNT) throw new AssertionError(this);
      newValue = State.incRefCount(oldValue);
      if (State.hasFlags(oldValue, State.FLAG_NEW_ALLOC)) {
        // Remove new-alloc flag on first use. Full unlock after that would imply force-discarding
        // this buffer is acceptable. This is kind of an ugly compact between the cache and us.
        newValue = State.switchFlag(newValue, State.FLAG_NEW_ALLOC);
      }
      if (state.compareAndSet(oldValue, newValue)) break;
    }
    int newRefCount = State.getRefCount(newValue);
    if (LlapIoImpl.LOCKING_LOGGER.isTraceEnabled()) {
      LlapIoImpl.LOCKING_LOGGER.trace("Locked {}; new ref count {}", this, newRefCount);
    }
    return newRefCount;
  }

  @VisibleForTesting
  int getRefCount() {
    return State.getRefCount(state.get());
  }

  @VisibleForTesting
  @Override
  public boolean isLocked() {
    // Best-effort check. We cannot do a good check against caller thread, since
    // refCount could still be > 0 if someone else locked. This is used for asserts and logs.
    return State.getRefCount(state.get()) > 0;
  }

  @VisibleForTesting
  public boolean isInvalid() {
    return State.hasFlags(state.get(), State.FLAG_EVICTED);
  }


  public int decRef() {
    long newState, oldState;
    do {
      oldState = state.get();
      // We have to check it here since invalid decref will overflow.
      int oldRefCount = State.getRefCount(oldState);
      if (oldRefCount == 0) {
        throw new AssertionError("Invalid decRef when refCount is 0: " + this);
      }
      newState = State.decRefCount(oldState);
    } while (!state.compareAndSet(oldState, newState));
    int newRefCount = State.getRefCount(newState);
    if (LlapIoImpl.LOCKING_LOGGER.isTraceEnabled()) {
      LlapIoImpl.LOCKING_LOGGER.trace("Unlocked {}; refcount {}", this, newRefCount);
    }
    return newRefCount;
  }


  /**
   * Invalidates the cached buffer. The memory allocation in memory manager is managed by the
   * caller, and therefore we mark the buffer as having released the memory too.
   * @return Whether the we can invalidate; false if locked or already evicted.
   */
  @Override
  public int invalidate() {
    while (true) {
      long oldValue = state.get();
      if (State.getRefCount(oldValue) != 0) return INVALIDATE_FAILED;
      if (State.hasFlags(oldValue, State.FLAG_EVICTED)) return INVALIDATE_ALREADY_INVALID;
      long newValue = State.setFlag(oldValue, State.FLAG_EVICTED | State.FLAG_MEM_RELEASED);
      if (state.compareAndSet(oldValue, newValue)) break;
    }
    if (LlapIoImpl.LOCKING_LOGGER.isTraceEnabled()) {
      LlapIoImpl.LOCKING_LOGGER.trace("Invalidated {} due to eviction", this);
    }
    return INVALIDATE_OK;
  }

  /**
   * Invalidates the uncached buffer. The memory allocation in memory manager is managed by the
   * allocator; we will only mark it as released if there are no concurrent moves. In that case,
   * the caller of this will release the memory; otherwise, the end of move will release.
   * @return Arena index, if the buffer memory can be released; -1 otherwise.
   */
  public int invalidateAndRelease() {
    boolean result;
    long oldValue, newValue;
    do {
      result = false;
      oldValue = state.get();
      if (State.getRefCount(oldValue) != 0) {
        throw new AssertionError("Refcount is " + State.getRefCount(oldValue));
      }
      if (State.hasFlags(oldValue, State.FLAG_EVICTED)) {
        return -1; // Concurrent force-eviction - ignore.
      }
      newValue = State.setFlag(oldValue, State.FLAG_EVICTED);
      if (!State.hasFlags(oldValue, State.FLAG_MOVING)) {
        // No move pending, the allocator can release.
        newValue = State.setFlag(newValue, State.FLAG_REMOVED | State.FLAG_MEM_RELEASED);
        result = true;
      }
    } while (!state.compareAndSet(oldValue, newValue));
    if (LlapIoImpl.LOCKING_LOGGER.isTraceEnabled()) {
      LlapIoImpl.LOCKING_LOGGER.trace("Invalidated {} due to direct deallocation", this);
    }
    // Arena cannot change after we have marked it as released.
    return result ? State.getArena(oldValue) : -1;
  }

  /**
   * Marks previously invalidated buffer as released.
   * @return Whether the buffer memory can be released.
   */
  public int releaseInvalidated() {
    long oldValue, newValue;
    do {
      oldValue = state.get();
      if (!State.hasFlags(oldValue, State.FLAG_EVICTED)) {
        throw new AssertionError("Not invalidated");
      }
      if (State.hasFlags(oldValue, State.FLAG_MOVING | State.FLAG_REMOVED)) return -1;
      // No move pending and no intervening discard, the allocator can release.
      newValue = State.setFlag(oldValue, State.FLAG_REMOVED);
    } while (!state.compareAndSet(oldValue, newValue));
    if (LlapIoImpl.LOCKING_LOGGER.isTraceEnabled()) {
      LlapIoImpl.LOCKING_LOGGER.trace("Removed {}", this);
    }
    // Arena cannot change after we have marked it as released.
    return State.getArena(oldValue);
  }

  private boolean waitForState() {
    synchronized (state) {
      try {
        state.wait(10);
        return true;
      } catch (InterruptedException e) {
        LlapIoImpl.LOG.debug("Buffer incRef is deffering an interrupt");
        Thread.currentThread().interrupt();
        return false;
      }
    }
  }

  @Override
  public String toString() {
    long state = this.state.get();
    int flags = State.getAllFlags(state);
    return "0x" + Integer.toHexString(System.identityHashCode(this)) + "("
        + State.getRefCount(state) + (flags == 0 ? "" : (", "
            + State.toFlagString(flags))) + ")";
  }


  public String toDebugString() {
    return toDebugString(state.get());
  }

  private String toDebugString(long state) {
    return "0x" + Integer.toHexString(System.identityHashCode(this)) + "("
        + State.getArena(state) + ":" + State.getHeader(state) + "; " + allocSize + "; "
        + State.toFlagString(State.getAllFlags(state)) + ")";
  }

  public boolean startMoveOrDiscard(int arenaIx, int headerIx, boolean isForceDiscard) {
    long oldValue, newValue;
    do {
      oldValue = state.get();
      if (State.getRefCount(oldValue) != 0) {
        return false;
      }
      int flags = State.getAllFlags(oldValue);
      // The only allowed flag is new-alloc, and that only if we are not discarding.
      if (flags != 0 && (isForceDiscard || flags != State.FLAG_NEW_ALLOC)) {
        return false; // We could start a move if it's being evicted, but let's not do it for now.
      }
      if (State.getArena(oldValue) != arenaIx || State.getHeader(oldValue) != headerIx) {
        return false; // The caller could re-check the location, but would probably find it locked.
      }
      newValue = State.setFlag(oldValue, State.FLAG_MOVING);
    } while (!state.compareAndSet(oldValue, newValue));
    if (LlapIoImpl.LOCKING_LOGGER.isTraceEnabled()) {
      LlapIoImpl.LOCKING_LOGGER.trace("Locked {} in preparation for a move", this);
    }
    return true;
  }

  /**
   * @return null if no action is required; otherwise, the buffer should be deallocated and,
   *         if the value is true, its memory should be released to the memory manager.
   */
  public Boolean cancelDiscard() {
    long oldValue, newValue;
    Boolean result;
    do {
      oldValue = state.get();
      assert State.hasFlags(oldValue, State.FLAG_MOVING) : this.toDebugString();
      newValue = State.switchFlag(oldValue, State.FLAG_MOVING);
      result = null;
      if (State.hasFlags(oldValue, State.FLAG_EVICTED)) {
        if (State.hasFlags(oldValue, State.FLAG_REMOVED)) {
          throw new AssertionError("Removed during the move " + this);
        }
        result = !State.hasFlags(oldValue, State.FLAG_MEM_RELEASED);
        // Not necessary here cause noone will be looking at these after us; set them for clarity.
        newValue = State.setFlag(newValue, State.FLAG_MEM_RELEASED | State.FLAG_REMOVED);
      }
    } while (!state.compareAndSet(oldValue, newValue));
    if (LlapIoImpl.LOCKING_LOGGER.isTraceEnabled()) {
      LlapIoImpl.LOCKING_LOGGER.trace("Move ended for {}", this);
    }
    synchronized (state) {
      state.notifyAll();
    }
    return result;
  }

  /**
   * @return null if no action is required; otherwise, the buffer should be deallocated and,
   *         if the value is true, its memory should be released to the memory manager.
   */
  public Boolean endDiscard() {
    long oldValue, newValue;
    Boolean result;
    do {
      oldValue = state.get();
      assert State.hasFlags(oldValue, State.FLAG_MOVING);
      newValue = State.switchFlag(oldValue, State.FLAG_MOVING);
      newValue = State.setFlag(newValue,
          State.FLAG_EVICTED | State.FLAG_MEM_RELEASED | State.FLAG_REMOVED);
      result = null;
      // See if someone else evicted this in parallel.
      if (State.hasFlags(oldValue, State.FLAG_EVICTED)) {
        if (State.hasFlags(oldValue, State.FLAG_REMOVED)) {
          throw new AssertionError("Removed during the move " + this);
        }
        result = !State.hasFlags(oldValue, State.FLAG_MEM_RELEASED);
      }
    } while (!state.compareAndSet(oldValue, newValue));
    if (LlapIoImpl.LOCKING_LOGGER.isTraceEnabled()) {
      LlapIoImpl.LOCKING_LOGGER.trace("Discared {}", this);
    }
    synchronized (state) {
      state.notifyAll();
    }
    return result;
  }

  private static final class State {
    public static final int
        FLAG_MOVING =       0b00001, // Locked by someone to move or force-evict.
        FLAG_EVICTED =      0b00010, // Evicted. This is cache-specific.
        FLAG_REMOVED =      0b00100, // Removed from allocator structures. The final state.
        FLAG_MEM_RELEASED = 0b01000, // The memory was released to memory manager.
        FLAG_NEW_ALLOC =    0b10000; // New allocation before the first use; cannot force-evict.
    private static final int FLAGS_WIDTH = 5,
        REFCOUNT_WIDTH = 19, ARENA_WIDTH = 16, HEADER_WIDTH = 24;

    public static final long MAX_REFCOUNT = (1 << REFCOUNT_WIDTH) - 1;

    private static final int REFCOUNT_SHIFT = FLAGS_WIDTH,
        ARENA_SHIFT = REFCOUNT_SHIFT + REFCOUNT_WIDTH, HEADER_SHIFT = ARENA_SHIFT + ARENA_WIDTH;

    private static final long FLAGS_MASK = (1L << FLAGS_WIDTH) - 1,
      REFCOUNT_MASK = ((1L << REFCOUNT_WIDTH) - 1) << REFCOUNT_SHIFT,
      ARENA_MASK = ((1L << ARENA_WIDTH) - 1) << ARENA_SHIFT,
      HEADER_MASK = ((1L << HEADER_WIDTH) - 1) << HEADER_SHIFT;

    public static boolean hasFlags(long value, int flags) {
      return (value & flags) != 0;
    }

    public static int getAllFlags(long state) {
      return (int) (state & FLAGS_MASK);
    }
    public static final int getRefCount(long state) {
      return (int)((state & REFCOUNT_MASK) >>> REFCOUNT_SHIFT);
    }
    public static final int getArena(long state) {
      return (int)((state & ARENA_MASK) >>> ARENA_SHIFT);
    }
    public static final int getHeader(long state) {
      return (int)((state & HEADER_MASK) >>> HEADER_SHIFT);
    }

    public static final long incRefCount(long state) {
      // Note: doesn't check for overflow. Could AND with max refcount mask but the caller checks.
      return state + (1 << REFCOUNT_SHIFT);
    }

    public static final long decRefCount(long state) {
      // Note: doesn't check for overflow. Could AND with max refcount mask but the caller checks.
      return state - (1 << REFCOUNT_SHIFT);
    }

    public static final long setLocation(long state, int arenaIx, int headerIx) {
      long arenaVal = ((long)arenaIx) << ARENA_SHIFT, arenaWMask = arenaVal & ARENA_MASK;
      long headerVal = ((long)headerIx) << HEADER_SHIFT, headerWMask = headerVal & HEADER_MASK;
      assert arenaVal == arenaWMask : "Arena " + arenaIx + " is wider than " + ARENA_WIDTH;
      assert headerVal == headerWMask : "Header " + headerIx + " is wider than " + HEADER_WIDTH;
      return (state & ~(ARENA_MASK | HEADER_MASK)) | arenaWMask | headerWMask;
    }

    public static final long setFlag(long state, int flags) {
      assert flags <= FLAGS_MASK;
      return state | flags;
    }

    public static final long switchFlag(long state, int flags) {
      assert flags <= FLAGS_MASK;
      return state ^ flags;
    }

    public static String toFlagString(int state) {
      return StringUtils.leftPad(Integer.toBinaryString(state), REFCOUNT_SHIFT, '0');
    }
  }

  @VisibleForTesting
  int getArenaIndex() {
    return State.getArena(state.get());
  }
}
