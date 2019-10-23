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

package org.apache.hadoop.hive.llap.io.api.impl;

import org.apache.hadoop.hive.common.io.Allocator;
import org.apache.hadoop.hive.common.io.CacheTag;
import org.apache.hadoop.hive.common.io.DataCache;
import org.apache.hadoop.hive.common.io.DiskRange;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.encoded.MemoryBuffer;
import org.apache.hadoop.hive.llap.cache.BufferUsageManager;
import org.apache.hadoop.hive.llap.cache.LlapDataBuffer;
import org.apache.hadoop.hive.llap.cache.LowLevelCache;

/**
 * Cache implementation for generic data format like parquet where we can't use full llap io elevator.
 */
class GenericDataCache implements DataCache, Allocator.BufferObjectFactory {
  private final LowLevelCache lowLevelCache;
  private final BufferUsageManager bufferManager;

  public GenericDataCache(LowLevelCache lowLevelCache, BufferUsageManager bufferManager) {
    this.lowLevelCache = lowLevelCache;
    this.bufferManager = bufferManager;
  }

  @Override
  public DiskRangeList getFileData(Object fileKey, DiskRangeList range,
      long baseOffset, DiskRangeListFactory factory, BooleanRef gotAllData) {
    // TODO: we currently pass null counters because this doesn't use LlapRecordReader.
    //       Create counters for non-elevator-using fragments also?
    return lowLevelCache.getFileData(fileKey, range, baseOffset, factory, null, gotAllData);
  }

  @Override
  public long[] putFileData(Object fileKey, DiskRange[] ranges,
      MemoryBuffer[] data, long baseOffset) {
    return putFileData(fileKey, ranges, data, baseOffset, null);
  }

  @Override
  public long[] putFileData(Object fileKey, DiskRange[] ranges,
      MemoryBuffer[] data, long baseOffset, CacheTag tag) {
    return lowLevelCache.putFileData(
        fileKey, ranges, data, baseOffset, LowLevelCache.Priority.NORMAL, null, tag);
  }

  @Override
  public void releaseBuffer(MemoryBuffer buffer) {
    bufferManager.decRefBuffer(buffer);
  }

  @Override
  public void reuseBuffer(MemoryBuffer buffer) {
    boolean isReused = bufferManager.incRefBuffer(buffer);
    assert isReused;
  }

  @Override
  public Allocator getAllocator() {
    return bufferManager.getAllocator();
  }

  @Override
  public Allocator.BufferObjectFactory getDataBufferFactory() {
    return this;
  }

  @Override
  public MemoryBuffer create() {
    return new LlapDataBuffer();
  }
}
