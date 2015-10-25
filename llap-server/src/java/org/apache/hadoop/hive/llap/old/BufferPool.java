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


package org.apache.hadoop.hive.llap.old;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;

import com.google.common.annotations.VisibleForTesting;

public class BufferPool {
  // TODO: we should keep evicted buffers for reuse. Perhaps that too should be factored out.
  private final CachePolicy cachePolicy;
  private final Object evictionNotifyObj = new Object();
  private int evictionIsWaiting; // best effort flag
  private final long maxCacheSize;
  private final int bufferSize;


  public BufferPool(Configuration conf) {
    this.maxCacheSize = 0;// HiveConf.getLongVar(conf, HiveConf.ConfVars.LLAP_CACHE_SIZE);
    this.bufferSize = 0; // HiveConf.getIntVar(conf, HiveConf.ConfVars.LLAP_BUFFER_SIZE);
    this.cachePolicy = null;
  }

  /**
   * Allocates a new buffer. Buffer starts out locked (assumption is that caller is going to
   * write to it immediately and then unlock it; future writers/readers will lock and unlock).
   * @return Buffer.
   */
  public WeakBuffer allocateBuffer() throws InterruptedException {
    // TODO: for now, dumb byte arrays. Should be off-heap.
    ByteBuffer newBuffer = ByteBuffer.allocate(bufferSize);
    WeakBuffer wb = new WeakBuffer(this, newBuffer);
    // Don't touch the buffer - it's not in cache yet. cache() will set the initial priority.
    if (!wb.lock(false)) {
      throw new AssertionError("Cannot lock a new buffer");
    }
    if (DebugUtils.isTraceLockingEnabled()) {
      LlapIoImpl.LOG.info("Locked " + wb + " after creation");
    }
    boolean hasWaited = false;
    WeakBuffer evicted = null;
    while (true) {
      evicted = cachePolicy.cache(wb);
      if (evicted != CachePolicy.CANNOT_EVICT) break;
      if (DebugUtils.isTraceCachingEnabled() && !hasWaited) {
        LlapIoImpl.LOG.info("Failed to add a new block to cache; waiting for blocks to be unlocked");
        hasWaited = true;
      }
      synchronized (evictionNotifyObj) {
        ++evictionIsWaiting;
        evictionNotifyObj.wait(1000);
        --evictionIsWaiting;
      }
    }
    if (DebugUtils.isTraceCachingEnabled() && hasWaited) {
      LlapIoImpl.LOG.info("Eviction is done waiting");
    }
    if (evicted != null) {
      //if (evictionListener != null) {
      //  evictionListener.evictionNotice(evicted);
      //}
      // After eviction notice, the contents can be reset.
      evicted.clear();
    }
    return wb;
  }

  private final void unblockEviction() {
    if (evictionIsWaiting <= 0) return;
    synchronized (evictionNotifyObj) {
      if (evictionIsWaiting <= 0) return;
      if (DebugUtils.isTraceCachingEnabled()) {
        LlapIoImpl.LOG.info("Notifying eviction that some block has been unlocked");
      }
      evictionNotifyObj.notifyAll();
    }
  }

  @VisibleForTesting
  public static WeakBuffer allocateFake() {
    return new WeakBuffer(null, ByteBuffer.wrap(new byte[1]));
  }

  /**
   * This class serves 3 purposes:
   * 1) it implements BufferPool-specific hashCode and equals (ByteBuffer ones are content-based);
   * 2) it contains the refCount;
   * 3) by extension from (2), it can be held while it is evicted; when locking before the usage,
   *    the fact that the data has been evicted will be discovered (similar to weak_ptr).
   * Note: not static because when we wait for something to become evict-able,
   * we need to receive notifications from unlock (see unlock). Otherwise could be static.
   */
  public static final class WeakBuffer {
    private static final int EVICTED_REFCOUNT = -1;
    private final BufferPool parent;
    private ByteBuffer contents;
    private final AtomicInteger refCount = new AtomicInteger(0);

    // TODO: Fields pertaining to cache policy. Perhaps they should live in separate object.
    public double priority;
    public long lastUpdate = -1;
    public int indexInHeap = -1;
    public boolean isLockedInHeap = false;

    private WeakBuffer(BufferPool parent, ByteBuffer contents) {
      this.parent = parent;
      this.contents = contents;
    }

    public ByteBuffer getContents() {
      assert isLocked() : "Cannot get contents with refCount " + refCount.get();
      return contents;
    }

    @Override
    public int hashCode() {
      if (contents == null) return 0;
      return System.identityHashCode(contents);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) return true;
      if (!(obj instanceof WeakBuffer)) return false;
      // We only compare objects, and not contents of the ByteBuffer.
      // One ByteBuffer is never put in multiple WeakBuffer-s (that is the invariant).
      return contents == ((WeakBuffer)obj).contents;
    }

    public boolean lock(boolean doTouch) {
      int oldRefCount = -1;
      while (true) {
        oldRefCount = refCount.get();
        if (oldRefCount == EVICTED_REFCOUNT) return false;
        assert oldRefCount >= 0;
        if (refCount.compareAndSet(oldRefCount, oldRefCount + 1)) break;
      }
      if (doTouch && oldRefCount == 0 && parent != null) {
        parent.cachePolicy.notifyLock(this);
      }
      return true;
    }

    public boolean isLocked() {
      // Best-effort check. We cannot do a good check against caller thread, since
      // refCount could still be > 0 if someone else locked. This is used for asserts.
      return refCount.get() > 0;
    }

    public boolean isInvalid() {
      return refCount.get() == EVICTED_REFCOUNT;
    }

    public boolean isCleared() {
      return contents == null;
    }

    public void unlock() {
      int newRefCount = refCount.decrementAndGet();
      if (newRefCount < 0) {
        throw new AssertionError("Unexpected refCount " + newRefCount);
      }
      // If this block became eligible, see if we need to unblock the eviction.
      if (newRefCount == 0 && parent != null) {
        parent.cachePolicy.notifyUnlock(this);
        parent.unblockEviction();
      }
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

    ByteBuffer clear() {
      assert refCount.get() == EVICTED_REFCOUNT;
      ByteBuffer result = contents;
      contents = null;
      return result;
    }

    public String toStringForCache() {
      return "[" + Integer.toHexString(hashCode()) + " " + String.format("%1$.2f", priority) + " "
    + lastUpdate + " " + (isLocked() ? "!" : ".") + "]";
    }
  }
}
