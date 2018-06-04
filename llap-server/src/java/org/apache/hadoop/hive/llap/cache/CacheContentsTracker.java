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

import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.llap.cache.LowLevelCache.Priority;

/**
 * A wrapper around cache eviction policy that tracks cache contents via tags.
 */
public class CacheContentsTracker implements LowLevelCachePolicy, EvictionListener {
  private static final long CLEANUP_TIME_MS = 3600 * 1000L, MIN_TIME_MS = 300 * 1000L;

  private final ConcurrentSkipListMap<String, TagState> tagInfo = new ConcurrentSkipListMap<>();
  private EvictionListener evictionListener;
  private LowLevelCachePolicy realPolicy;
  private final Thread cleanupThread;

  public CacheContentsTracker(LowLevelCachePolicy realPolicy) {
    this.realPolicy = realPolicy;
    realPolicy.setEvictionListener(this);
    this.cleanupThread = new Thread(new CleanupRunnable());
    this.cleanupThread.start();
  }

  private final class CleanupRunnable implements Runnable {
    @Override
    public void run() {
      final long cleanupTimeNs = CLEANUP_TIME_MS * 1000000L;
      long sleepTimeMs = CLEANUP_TIME_MS;
      try {
        while (true) {
          Thread.sleep(sleepTimeMs);
          long timeNs = System.nanoTime();
          long nextCleanupInNs = cleanupTimeNs;
          Iterator<TagState> iter = tagInfo.values().iterator();
          while (iter.hasNext()) {
            TagState v = iter.next();
            synchronized (v) {
              if (v.bufferCount > 0) continue; // The file is still cached.
              long deltaNs = timeNs - v.emptyTimeNs;
              if (deltaNs < cleanupTimeNs) {
                nextCleanupInNs = Math.min(nextCleanupInNs, deltaNs);
                continue;
              } else {
                iter.remove();
              }
            }
          }
          sleepTimeMs = Math.max(MIN_TIME_MS, nextCleanupInNs / 1000000L);
        }
      } catch (InterruptedException ex) {
        return; // Interrupted.
      }
    }
  }

  private static class TagState {
    public TagState(String name) {
      this.name = name;
    }
    public final String name;
    public long emptyTimeNs;
    public long bufferCount, totalSize, maxCount, maxSize;
    public boolean isRemoved = false;
  }


  private void reportCached(LlapCacheableBuffer buffer) {
    long size = buffer.getMemoryUsage();
    TagState state;
    do {
       state = getTagState(buffer);
    } while (!reportCached(state, size));
    state = null;
    do {
      state = getParentTagState(buffer);
      if (state == null) break;
    } while (!reportCached(state, size));
  }

  private boolean reportCached(TagState state, long size) {
    synchronized (state) {
      if (state.isRemoved) return false;
      ++state.bufferCount;
      state.totalSize += size;
      state.maxSize = Math.max(state.maxSize, state.totalSize);
      state.maxCount = Math.max(state.maxCount, state.bufferCount);
    }
    return true;
  }

  private void reportRemoved(LlapCacheableBuffer buffer) {
    long size = buffer.getMemoryUsage();
    TagState state;
    do {
       state = getTagState(buffer);
    } while (!reportRemoved(state, size));
    state = null;
    do {
      state = getParentTagState(buffer);
      if (state == null) break;
    } while (!reportRemoved(state, size));
  }

  private boolean reportRemoved(TagState state, long size) {
    synchronized (state) {
      if (state.isRemoved) return false;
      --state.bufferCount;
      assert state.bufferCount >= 0;
      state.totalSize -= size;
      if (state.bufferCount == 0) {
        state.emptyTimeNs = System.nanoTime();
      }
    }
    return true;
  }

  private TagState getTagState(LlapCacheableBuffer buffer) {
    return getTagState(buffer.getTag());
  }

  private TagState getParentTagState(LlapCacheableBuffer buffer) {
    String tag = buffer.getTag();
    int ix = tag.indexOf(LlapUtil.DERIVED_ENTITY_PARTITION_SEPARATOR);
    if (ix <= 0) return null;
    return getTagState(tag.substring(0, ix));
  }

  private TagState getTagState(String tag) {
    TagState state = tagInfo.get(tag);
    if (state == null) {
      state = new TagState(tag);
      TagState old = tagInfo.putIfAbsent(tag, state);
      state = (old == null) ? state : old;
    }
    return state;
  }


  @Override
  public void cache(LlapCacheableBuffer buffer, Priority priority) {
    realPolicy.cache(buffer, priority);
    reportCached(buffer);
  }

  @Override
  public void notifyLock(LlapCacheableBuffer buffer) {
    realPolicy.notifyLock(buffer);
  }

  @Override
  public void notifyUnlock(LlapCacheableBuffer buffer) {
    realPolicy.notifyUnlock(buffer);
  }

  @Override
  public void setEvictionListener(EvictionListener listener) {
    evictionListener = listener;
  }

  @Override
  public long purge() {
    return realPolicy.purge();
  }


  @Override
  public long evictSomeBlocks(long memoryToReserve) {
    return realPolicy.evictSomeBlocks(memoryToReserve);
  }

  @Override
  public void debugDumpShort(StringBuilder sb) {
    sb.append("\nCache state: ");
    for (TagState state : tagInfo.values()) {
      synchronized (state) {
        sb.append("\n").append(state.name).append(": ").append(state.bufferCount).append("/")
          .append(state.maxCount).append(", ").append(state.totalSize).append("/")
          .append(state.maxSize);
      }
    }
  }

  @Override
  public void notifyEvicted(LlapCacheableBuffer buffer) {
    evictionListener.notifyEvicted(buffer);
    reportRemoved(buffer);
  }
}
