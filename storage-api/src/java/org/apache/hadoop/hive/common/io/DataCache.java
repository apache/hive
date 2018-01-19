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

package org.apache.hadoop.hive.common.io;

import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;

/** An abstract data cache that IO formats can use to retrieve and cache data. */
public interface DataCache {
  public static final class BooleanRef {
    public boolean value;
  }

  /** Disk range factory used during cache retrieval. */
  public interface DiskRangeListFactory {
    DiskRangeList createCacheChunk(MemoryBuffer buffer, long startOffset, long endOffset);
  }

  /**
   * Gets file data for particular offsets. The range list is modified in place; it is then
   * returned (since the list head could have changed). Ranges are replaced with cached ranges.
   *
   * Any such buffer is locked in cache to prevent eviction, and must therefore be released
   * back to cache via a corresponding call (releaseBuffer) when the caller is done with it.
   *
   * In case of partial overlap with cached data, full cache blocks are always returned;
   * there's no capacity for partial matches in return type. The rules are as follows:
   * 1) If the requested range starts in the middle of a cached range, that cached range will not
   *    be returned by default (e.g. if [100,200) and [200,300) are cached, the request for
   *    [150,300) will only return [200,300) from cache). This may be configurable in impls.
   *    This is because we assume well-known range start offsets are used (rg/stripe offsets), so
   *    a request from the middle of the start doesn't make sense.
   * 2) If the requested range ends in the middle of a cached range, that entire cached range will
   *    be returned (e.g. if [100,200) and [200,300) are cached, the request for [100,250) will
   *    return both ranges). It should really be same as #1, however currently ORC uses estimated
   *    end offsets; if we don't return the end block, the caller may read it from disk needlessly.
   *
   * @param fileKey Unique ID of the target file on the file system.
   * @param range A set of DiskRange-s (linked list) that is to be retrieved. May be modified.
   * @param baseOffset base offset for the ranges (stripe/stream offset in case of ORC).
   * @param factory A factory to produce DiskRangeList-s out of cached MemoryBuffer-s.
   * @param gotAllData An out param - whether all the requested data was found in cache.
   * @return The new or modified list of DiskRange-s, where some ranges may contain cached data.
   */
  DiskRangeList getFileData(Object fileKey, DiskRangeList range, long baseOffset,
      DiskRangeListFactory factory, BooleanRef gotAllData);

  /**
   * Puts file data into cache, or gets older data in case of collisions.
   *
   * The memory buffers provided MUST be allocated via an allocator returned by getAllocator
   * method, to allow cache implementations that evict and then de-allocate the buffer.
   *
   * It is assumed that the caller will use the data immediately, therefore any buffers provided
   * to putFileData (or returned due to cache collision) are locked in cache to prevent eviction,
   * and must therefore be released back to cache via a corresponding call (releaseBuffer) when the
   * caller is done with it. Buffers rejected due to conflict will neither be locked, nor
   * automatically deallocated. The caller must take care to discard these buffers.
   *
   * @param fileKey Unique ID of the target file on the file system.
   * @param ranges The ranges for which the data is being cached. These objects will not be stored.
   * @param data The data for the corresponding ranges.
   * @param baseOffset base offset for the ranges (stripe/stream offset in case of ORC).
   * @return null if all data was put; bitmask indicating which chunks were not put otherwise;
   *         the replacement chunks from cache are updated directly in the array.
   */
  long[] putFileData(Object fileKey, DiskRange[] ranges, MemoryBuffer[] data, long baseOffset);

  /**
   * Releases the buffer returned by getFileData/provided to putFileData back to cache.
   * See respective javadocs for details.
   * @param buffer the buffer to release
   */
  void releaseBuffer(MemoryBuffer buffer);

  /**
   * Notifies the cache that the buffer returned from getFileData/provided to putFileData will
   * be used by another consumer and therefore released multiple times (one more time per call).
   * @param buffer the buffer to reuse
   */
  void reuseBuffer(MemoryBuffer buffer);

  /**
   * Gets the allocator associated with this DataCache.
   * @return the allocator
   */
  Allocator getAllocator();

  /**
   * Gets the buffer object factory associated with this DataCache, to use with allocator.
   * @return the factory
   */
  Allocator.BufferObjectFactory getDataBufferFactory();
}
