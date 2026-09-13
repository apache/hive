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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.llap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.DataCache.DiskRangeListFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.orc.encoded.CacheChunk;

/**
 * How Parquet column chunks are laid out in the LLAP cache and fetched from the file. Everything
 * that caches a chunk goes through this, so a reader finding only part of one cached re-caches
 * the gap on the boundaries the first reader used. A reader builds one from the daemon's allocator
 * and configuration and uses it for every chunk, which is what keeps its layout consistent.
 */
public final class ParquetCacheLayout {

  /**
   * An object store fetches separate ranges concurrently but never splits one, so cutting buys
   * parallelism while a large range amortises the request. Measured against S3 the knee is here:
   * below 4 Mb throughput drops sharply, above 8 Mb it is flat. Trino also splits at 8 Mb.
   */
  private static final int MIN_RANGE_CAP_BYTES = 8 << 20;

  public static final DiskRangeListFactory CACHE_CHUNK_FACTORY = CacheChunk::new;

  private final int maxBuffer;
  private final int minBuffer;
  private final int maxRange;

  /** The layout for a daemon: its allocator's maximum and the grain it caches non-ORC data at. */
  public ParquetCacheLayout(Allocator allocator, Configuration conf) {
    this(allocator.getMaxAllocation(), HiveConf.getSizeVar(conf, HiveConf.ConfVars.LLAP_IO_ENCODE_ALLOC_SIZE));
  }

  /**
   * @param maxAlloc   largest buffer the allocator hands out; rounded down to a power of two
   * @param splitFloor size below which a remainder is left as one buffer rather than split further
   */
  public ParquetCacheLayout(int maxAlloc, long splitFloor) {
    this.maxBuffer = Integer.highestOneBit(maxAlloc);
    // Clamped, not cast: the key is validated as a size but not against the allocator's maximum.
    this.minBuffer = (int) Math.min(splitFloor, maxBuffer);
    this.maxRange = Math.max(MIN_RANGE_CAP_BYTES, maxAlloc);
  }

  /**
   * Largest range of a vectored read. A cache buffer is always fetched whole, so a cap below the
   * allocator's maximum would not be one; it does not simply follow the allocator either, since
   * shrinking the cache should not shrink reads.
   */
  public int maxRangeBytes() {
    return maxRange;
  }

  /**
   * Cuts a range into power-of-two buffers, largest first, so each fills its buddy allocation
   * exactly; caching a chunk whole leaves the allocator to round it up, and a 5 Mb chunk then
   * occupies 8 Mb. Below the floor the remainder stays one buffer, since splitting a small tail
   * costs a key, a refcount and an eviction to save very little. ORC caches in fixed parts of its
   * compression buffer size instead, which needs no decomposition but many more buffers.
   *
   * Any contiguous run of the result decomposes into itself, so a gap left by evicting some of a
   * chunk's buffers is re-cached under the same keys.
   */
  public int[] bufferSizes(long length) {
    int count = 0;
    for (long left = length; left > 0; ++count) {
      left -= left < minBuffer ? left : Math.min(maxBuffer, Long.highestOneBit(left));
    }
    int[] sizes = new int[count];
    long left = length;
    for (int i = 0; i < count; ++i) {
      sizes[i] = left < minBuffer ? (int) left : (int) Math.min(maxBuffer, Long.highestOneBit(left));
      left -= sizes[i];
    }
    return sizes;
  }
}
