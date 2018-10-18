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

import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.DataCache.BooleanRef;
import org.apache.hadoop.hive.common.io.DataCache.DiskRangeListFactory;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;

public interface LowLevelCache {
  public enum Priority {
    NORMAL,
    HIGH
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
  DiskRangeList getFileData(Object fileKey, DiskRangeList range, long baseOffset,
      DiskRangeListFactory factory, LowLevelCacheCounters qfCounters, BooleanRef gotAllData);

  /**
   * Puts file data into cache.
   * @return null if all data was put; bitmask indicating which chunks were not put otherwise;
   *         the replacement chunks from cache are updated directly in the array.
   */
  long[] putFileData(Object fileKey, DiskRange[] ranges, MemoryBuffer[] chunks,
      long baseOffset, Priority priority, LowLevelCacheCounters qfCounters, String tag);

  /** Notifies the cache that a particular buffer should be removed due to eviction. */
  void notifyEvicted(MemoryBuffer buffer);
}
