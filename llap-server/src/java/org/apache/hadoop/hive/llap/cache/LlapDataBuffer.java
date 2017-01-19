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

package org.apache.hadoop.hive.llap.cache;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;

import com.google.common.annotations.VisibleForTesting;

public final class LlapDataBuffer extends LlapCacheableBuffer implements MemoryBuffer {

  // For now, we don't track refcount for metadata blocks, don't clear them, don't reuse them and
  // basically rely on GC to remove them. So, refcount only applies to data blocks. If that
  // changes, refcount management should be move to LlapCacheableBuffer to be shared.
  private static final int EVICTED_REFCOUNT = -1;
  public static final int UNKNOWN_CACHED_LENGTH = -1;
  protected final AtomicInteger refCount = new AtomicInteger(0);

  public ByteBuffer byteBuffer;
  /** Allocator uses this to remember which arena to alloc from. */
  public int arenaIndex = -1;
  /** Allocator uses this to remember the allocation size. */
  public int allocSize;
  /** ORC cache uses this to store compressed length; buffer is cached uncompressed, but
   * the lookup is on compressed ranges, so we need to know this. */
  public int declaredCachedLength = UNKNOWN_CACHED_LENGTH;

  public void initialize(
      int arenaIndex, ByteBuffer byteBuffer, int offset, int length) {
    this.byteBuffer = byteBuffer.slice();
    this.byteBuffer.position(offset);
    this.byteBuffer.limit(offset + length);
    this.arenaIndex = arenaIndex;
    this.allocSize = length;
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

  @Override
  public void notifyEvicted(EvictionDispatcher evictionDispatcher) {
    evictionDispatcher.notifyEvicted(this);
  }

  int incRef() {
    int newRefCount = -1;
    while (true) {
      int oldRefCount = refCount.get();
      if (oldRefCount == EVICTED_REFCOUNT) return -1;
      assert oldRefCount >= 0 : "oldRefCount is " + oldRefCount + " " + this;
      newRefCount = oldRefCount + 1;
      if (refCount.compareAndSet(oldRefCount, newRefCount)) break;
    }
    if (LlapIoImpl.LOCKING_LOGGER.isTraceEnabled()) {
      LlapIoImpl.LOCKING_LOGGER.trace("Locked {}; new ref count {}", this, newRefCount);
    }
    return newRefCount;
  }

  @VisibleForTesting
  int getRefCount() {
    return refCount.get();
  }

  @VisibleForTesting
  @Override
  public boolean isLocked() {
    // Best-effort check. We cannot do a good check against caller thread, since
    // refCount could still be > 0 if someone else locked. This is used for asserts and logs.
    return refCount.get() > 0;
  }

  @VisibleForTesting
  public boolean isInvalid() {
    return refCount.get() == EVICTED_REFCOUNT;
  }

  int decRef() {
    int newRefCount = refCount.decrementAndGet();
    if (LlapIoImpl.LOCKING_LOGGER.isTraceEnabled()) {
      LlapIoImpl.LOCKING_LOGGER.trace("Unlocked {}; refcount {}", this, newRefCount);
    }
    if (newRefCount < 0) {
      throw new AssertionError("Unexpected refCount " + newRefCount + ": " + this);
    }
    return newRefCount;
  }

  /**
   * @return Whether the we can invalidate; false if locked or already evicted.
   */
  @Override
  public boolean invalidate() {
    while (true) {
      int value = refCount.get();
      if (value != 0) return false;
      if (refCount.compareAndSet(value, EVICTED_REFCOUNT)) break;
    }
    if (LlapIoImpl.LOCKING_LOGGER.isTraceEnabled()) {
      LlapIoImpl.LOCKING_LOGGER.trace("Invalidated {} due to eviction", this);
    }
    return true;
  }

  @Override
  public String toString() {
    int refCount = this.refCount.get();
    return "0x" + Integer.toHexString(System.identityHashCode(this)) + "(" + refCount + ")";
  }

  public static String toDataString(MemoryBuffer s) {
    if (s == null || s.getByteBufferRaw().remaining() == 0) return "" + s;
    byte b = s.getByteBufferRaw().get(s.getByteBufferRaw().position());
    int i = (b < 0) ? -b : b;
    return s + " (0x" + Integer.toHexString(i) + ")";
  }
}
