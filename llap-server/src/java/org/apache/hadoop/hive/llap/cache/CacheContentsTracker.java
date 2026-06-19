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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;

import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.llap.cache.LowLevelCache.Priority;

import static java.util.stream.Collectors.joining;

/**
 * A wrapper around cache eviction policy that tracks cache contents via tags.
 */
public class CacheContentsTracker implements LowLevelCachePolicy, ProactiveEvictingCachePolicy, EvictionListener {
  private static final long CLEANUP_TIME_MS = 3600 * 1000L, MIN_TIME_MS = 300 * 1000L;

  private final ConcurrentSkipListMap<CacheTag, TagState> tagInfo = new ConcurrentSkipListMap<>();
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
    TagState(CacheTag cacheTag) {
      this.cacheTag = cacheTag;
    }
    public final CacheTag cacheTag;
    public long emptyTimeNs;
    public long bufferCount, totalSize, maxCount, maxSize;
  }

  private void reportCached(LlapCacheableBuffer buffer) {
    long size = buffer.getMemoryUsage();
    TagState state = getTagState(buffer);
    reportCached(state, size);
  }

  private void reportCached(TagState state, long size) {
    synchronized (state) {
      ++state.bufferCount;
      state.totalSize += size;
      state.maxSize = Math.max(state.maxSize, state.totalSize);
      state.maxCount = Math.max(state.maxCount, state.bufferCount);
    }
  }

  private void reportRemoved(LlapCacheableBuffer buffer) {
    long size = buffer.getMemoryUsage();
    TagState state = getTagState(buffer);
    reportRemoved(state, size);
  }

  private void reportRemoved(TagState state, long size) {
    synchronized (state) {
      --state.bufferCount;
      assert state.bufferCount >= 0;
      state.totalSize -= size;
      if (state.bufferCount == 0) {
        state.emptyTimeNs = System.nanoTime();
      }
    }
  }

  private TagState getTagState(LlapCacheableBuffer buffer) {
    return getTagState(buffer.getTag());
  }

  private TagState getTagState(CacheTag tag) {
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
    ArrayList<String> endResult = new ArrayList<>();
    Map<CacheTag, TagState> summaries = new TreeMap<>();

    for (TagState state : tagInfo.values()) {
      synchronized (state) {
        endResult.add(unsafePrintTagState(state));

        // Handle summary calculation
        CacheTag parentTag = CacheTag.createParentCacheTag(state.cacheTag);
        while (parentTag != null) {
          if (!summaries.containsKey(parentTag)) {
            summaries.put(parentTag, new TagState(parentTag));
          }
          TagState parentState = summaries.get(parentTag);
          parentState.bufferCount += state.bufferCount;
          parentState.maxCount += state.maxCount;
          parentState.totalSize += state.totalSize;
          parentState.maxSize += state.maxSize;
          parentTag = CacheTag.createParentCacheTag(parentTag);
        }
      }
    }
    for (TagState state : summaries.values()) {
      endResult.add(unsafePrintTagState(state));
    }
    sb.append("\nCache state: \n");
    sb.append(endResult.stream().sorted().collect(joining("\n")));
  }

  /**
   * Constructs a String by pretty printing a TagState instance - for Web UI consumption.
   * Note: does not lock on TagState instance.
   * @param state
   * @return
   */
  private String unsafePrintTagState(TagState state) {
    StringBuilder sb = new StringBuilder();
    sb.append(state.cacheTag.getTableName());
    if (state.cacheTag instanceof CacheTag.PartitionCacheTag) {
      sb.append("/").append(String.join("/",
          ((CacheTag.PartitionCacheTag) state.cacheTag).partitionDescToString()));
    }
    sb.append(" : ").append(state.bufferCount).append("/").append(state.maxCount).append(", ")
            .append(state.totalSize).append("/").append(state.maxSize);
    return sb.toString();
  }

  @Override
  public void notifyEvicted(LlapCacheableBuffer buffer) {
    evictionListener.notifyEvicted(buffer);
    reportRemoved(buffer);
  }

  @Override
  public void notifyProactivelyEvicted(LlapCacheableBuffer buffer) {
    evictionListener.notifyProactivelyEvicted(buffer);
    reportRemoved(buffer);
  }

  @Override
  public void notifyProactiveEvictionMark() {
    if (realPolicy instanceof ProactiveEvictingCachePolicy) {
      ((ProactiveEvictingCachePolicy) realPolicy).notifyProactiveEvictionMark();
    }
  }
}
