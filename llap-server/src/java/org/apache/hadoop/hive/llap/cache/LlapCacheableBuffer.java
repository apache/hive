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

import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.io.api.cache.LlapMemoryBuffer;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;

public final class LlapCacheableBuffer extends LlapMemoryBuffer {
  public LlapCacheableBuffer(ByteBuffer byteBuffer, int offset, int length) {
    super(byteBuffer, offset, length);
  }

  public String toStringForCache() {
    return "[" + Integer.toHexString(hashCode()) + " " + String.format("%1$.2f", priority) + " "
        + lastUpdate + " " + (isLocked() ? "!" : ".") + "]";
  }

  private static final int EVICTED_REFCOUNT = -1;
  private final AtomicInteger refCount = new AtomicInteger(0);

  // TODO: Fields pertaining to cache policy. Perhaps they should live in separate object.
  public double priority;
  public long lastUpdate = -1;
  public int indexInHeap = -1;
  public boolean isLockedInHeap; // TODO#: this flag is invalid and not thread safe

  @Override
  public int hashCode() {
    if (this.byteBuffer == null) return 0;
    return (System.identityHashCode(this.byteBuffer) * 37 + offset) * 37 + length;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof LlapCacheableBuffer)) return false;
    LlapCacheableBuffer other = (LlapCacheableBuffer)obj;
    // We only compare objects, and not contents of the ByteBuffer.
    return byteBuffer == other.byteBuffer
        && this.offset == other.offset && this.length == other.length;
  }

  int lock() {
    int oldRefCount = -1;
    while (true) {
      oldRefCount = refCount.get();
      if (oldRefCount == EVICTED_REFCOUNT) return -1;
      assert oldRefCount >= 0;
      if (refCount.compareAndSet(oldRefCount, oldRefCount + 1)) break;
    }
    return oldRefCount;
  }

  public boolean isLocked() {
    // Best-effort check. We cannot do a good check against caller thread, since
    // refCount could still be > 0 if someone else locked. This is used for asserts.
    return refCount.get() > 0;
  }

  public boolean isInvalid() {
    return refCount.get() == EVICTED_REFCOUNT;
  }

  int unlock() {
    int newRefCount = refCount.decrementAndGet();
    if (newRefCount < 0) {
      throw new AssertionError("Unexpected refCount " + newRefCount);
    }
    return newRefCount;
  }

  @Override
  public String toString() {
    return "0x" + Integer.toHexString(hashCode());
  }

  /**
   * @return Whether the we can invalidate; false if locked or already evicted.
   */
  boolean invalidate() {
    while (true) {
      int value = refCount.get();
      if (value != 0) return false;
      if (refCount.compareAndSet(value, EVICTED_REFCOUNT)) break;
    }
    if (DebugUtils.isTraceLockingEnabled()) {
      LlapIoImpl.LOG.info("Invalidated " + this + " due to eviction");
    }
    return true;
  }
}
