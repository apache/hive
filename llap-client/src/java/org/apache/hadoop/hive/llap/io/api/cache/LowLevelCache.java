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

package org.apache.hadoop.hive.llap.io.api.cache;

import java.util.List;

import org.apache.hadoop.hive.common.DiskRange;
import org.apache.hadoop.hive.common.DiskRangeList;

public interface LowLevelCache {
  public enum Priority {
    NORMAL,
    HIGH
    // TODO: we could add more priorities, e.g. tiered-high, where we always evict it last.
  }

  /**
   * Gets file data for particular offsets. The range list is modified in place; it is then
   * returned (since the list head could have changed). Ranges are replaced with cached ranges.
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
   *    end offsets; we do in fact know in such cases that partially-matched cached block (rg)
   *    can be thrown away, the reader will never touch it; but we need code in the reader to
   *    handle such cases to avoid disk reads for these "tails" vs real unmatched ranges.
   *    Some sort of InvalidCacheChunk could be placed to avoid them. TODO
   * @param base base offset for the ranges (stripe/stream offset in case of ORC).
   */
  DiskRangeList getFileData(
      long fileId, DiskRangeList range, long baseOffset, CacheChunkFactory factory);

  /**
   * Puts file data into cache.
   * @return null if all data was put; bitmask indicating which chunks were not put otherwise;
   *         the replacement chunks from cache are updated directly in the array.
   */
  long[] putFileData(
      long fileId, DiskRange[] ranges, LlapMemoryBuffer[] chunks, long base, Priority priority);

  /**
   * Releases the buffer returned by getFileData or allocateMultiple.
   */
  void releaseBuffer(LlapMemoryBuffer buffer);

  /**
   * Allocate dest.length new blocks of size into dest.
   */
  void allocateMultiple(LlapMemoryBuffer[] dest, int size);

  void releaseBuffers(List<LlapMemoryBuffer> cacheBuffers);

  LlapMemoryBuffer createUnallocated();

  boolean notifyReused(LlapMemoryBuffer buffer);

  boolean isDirectAlloc();

  public interface CacheChunkFactory {
    DiskRangeList createCacheChunk(LlapMemoryBuffer buffer, long startOffset, long endOffset);
  }
}
