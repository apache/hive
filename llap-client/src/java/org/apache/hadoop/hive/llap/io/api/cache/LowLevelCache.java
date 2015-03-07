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
   * Gets file data for particular offsets. Null entries mean no data.
   * @param base base offset for the ranges (stripe offset in case of ORC).
   * @return 
   */
  DiskRangeList getFileData(long fileId, DiskRangeList range, long baseOffset);

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

  void notifyReused(LlapMemoryBuffer buffer);

  boolean isDirectAlloc();
}
