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

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.llap.DebugUtils;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.old.BufferPool.WeakBuffer;
import org.apache.hadoop.hive.llap.old.ChunkPool.Chunk;

/**
 * This class contains the mapping of file chunks to buffers inside BufferPool.
 */
public class ChunkPool<K> /*implements EvictionListener*/ {
  private final ConcurrentHashMap<K, Chunk> chunkCache = new ConcurrentHashMap<K, Chunk>();

  /** Number of unprocessed evictions, for the background thread. */
  private final AtomicInteger newEvictions = new AtomicInteger(0);
  private final Thread cleanupThread;

  public ChunkPool() {
    cleanupThread = new CleanupThread();
    cleanupThread.start();
  }

  /**
   * Gets a chunk from cache
   * TODO:  We expect that in most cases, some related chunks (e.g. columns for a stripe)
   *        will be stored in the same buffer. We could use this to get keys more efficiently.
   *        On the other hand, real stripes are pretty big.
   * @param key key to search for.
   * @return Chunk corresponding to k.
   */
  public Chunk getChunk(K key, HashSet<WeakBuffer> lockedBuffers) {
    while (true) {
      Chunk result = chunkCache.get(key);
      if (result == null) return null;
      if (lockChunk(result, lockedBuffers)) return result;
      if (chunkCache.remove(key, result)) return null;
    }
  }

  private boolean lockChunk(Chunk result, HashSet<WeakBuffer> lockedBuffers) {
    // We expect the chain to have 1 or 2 buffers (2 if we are on buffer boundary). Keep track of
    // what we lock in the bitmask; may need fixing (extremely unlikely - 64+ buffer, giant chunks)
    boolean failedToLock = false;
    long blocksToUnlock = 0;
    long bit = 1 << 63; // The bit indicating that current chunk was locked.

    Chunk chunk = result;
    while (chunk != null) {
      if (lockedBuffers.contains(chunk.buffer)) {
        assert chunk.buffer.isLocked() : chunk.buffer + " is in lockedBuffers but is not locked";
      } else if (chunk.buffer.lock(true)) {
        if (DebugUtils.isTraceLockingEnabled()) {
          LlapIoImpl.LOG.info("Locked " + chunk.buffer + " for " + result);
        }
        lockedBuffers.add(chunk.buffer);
        blocksToUnlock += bit;
      } else {
        failedToLock = true;
        break;
      }
      bit >>>= 1;
      chunk = chunk.nextChunk;
      if (bit == 1 && chunk != null) {
        throw new AssertionError("Chunk chain was too long");
      }
    }
    if (!failedToLock) return true;

    bit = 1 << 63;
    Chunk chunk2 = result;
    while (chunk2 != chunk) {
      if ((blocksToUnlock & bit) == bit) {
        if (DebugUtils.isTraceLockingEnabled()) {
          LlapIoImpl.LOG.info("Unlocking " + chunk2.buffer + " due to failed chunk lock");
        }
        lockedBuffers.remove(chunk2.buffer);
        chunk2.buffer.unlock();
      }
      bit >>>= 1;
      chunk2 = chunk2.nextChunk;
    }
    return false;
  }

  private boolean verifyChunk(Chunk entry) {
    Chunk chunk = entry;
    while (chunk != null) {
      if (!chunk.buffer.lock(false)) break;
      chunk = chunk.nextChunk;
    }
    Chunk chunk2 = entry;
    while (chunk2 != chunk) {
      chunk2.buffer.unlock();
      chunk2 = chunk2.nextChunk;
    }
    return chunk == null;
  }

  public Chunk addOrGetChunk(K key, Chunk val, HashSet<WeakBuffer> lockedBuffers) {
    assert val.buffer.isLocked();
    while (true) {
      Chunk oldVal = chunkCache.putIfAbsent(key, val);
      if (oldVal == null) return val;
      if (DebugUtils.isTraceCachingEnabled()) {
        LlapIoImpl.LOG.info("Trying to cache when the chunk is already cached for "
            + key + "; old " + oldVal + ", new " + val);
      }
      if (lockChunk(oldVal, lockedBuffers)) return oldVal;
      // We found some old value but couldn't lock it; remove it.
      chunkCache.remove(key, oldVal);
    }
  }

  //@Override
  public void evictionNotice(WeakBuffer evicted) {
    int oldValue = newEvictions.getAndIncrement();
    if (oldValue == 0) {
      synchronized (newEvictions) {
        newEvictions.notifyAll();
      }
    }
  }

  public static class Chunk {
    public WeakBuffer buffer;
    public int offset, length;
    public Chunk nextChunk;

    public Chunk(WeakBuffer buffer, int offset, int length) {
      this.buffer = buffer;
      this.offset = offset;
      this.length = length;
    }

    public Chunk addChunk(Chunk another) {
      // Traversing list is bad; however, we expect that this will very rarely happen; and in
      // nearly all the cases when it does (buffer boundary) the list will have 1 element.
      Chunk chunk = this;
      while (chunk.nextChunk != null) {
        chunk = chunk.nextChunk;
      }
      chunk.nextChunk = another;
      return this;
    }

    @Override
    public String toString() {
      return "{" + buffer + ", " + offset + ", " + length + "}";
    }

    public String toFullString() {
      String result = "";
      Chunk chunk = this;
      while (chunk != null) {
        result += chunk.toString() + ", ";
        chunk = chunk.nextChunk;
      }
      return result;
    }
  }

  private final class CleanupThread extends Thread {
    private int APPROX_CLEANUP_INTERVAL_SEC = 600;

    public CleanupThread() {
      super("Llap ChunkPool cleanup thread");
      setDaemon(true);
      setPriority(1);
    }

    @Override
    public void run() {
      while (true) {
        try {
          doOneCleanupRound();
        } catch (InterruptedException ex) {
          LlapIoImpl.LOG.warn("Cleanup thread has been interrupted");
          Thread.currentThread().interrupt();
          break;
        } catch (Throwable t) {
          LlapIoImpl.LOG.error("Cleanup has failed; the thread will now exit", t);
          break;
        }
      }
    }

    private void doOneCleanupRound() throws InterruptedException {
      while (true) {
        int evictionsSinceLast = newEvictions.getAndSet(0);
        if (evictionsSinceLast > 0) break;
        synchronized (newEvictions) {
          newEvictions.wait(10000);
        }
      }
      // Duration is an estimate; if the size of the map changes rapidly, it can be very different.
      long endTime = System.nanoTime() + APPROX_CLEANUP_INTERVAL_SEC * 1000000000L;
      int processed = 0;
      // TODO: if this iterator affects the map in some bad way,
      //       we'd need to sleep once per round instead.
      Iterator<Map.Entry<K, Chunk>> iter = chunkCache.entrySet().iterator();
      while (iter.hasNext()) {
        if (!verifyChunk(iter.next().getValue())) {
          iter.remove();
        }
        ++processed;
        int approxElementsLeft = chunkCache.size() - processed;
        Thread.sleep((approxElementsLeft <= 0)
            ? 1 : (endTime - System.nanoTime()) / (1000000L * approxElementsLeft));
      }
    }
  }
}
