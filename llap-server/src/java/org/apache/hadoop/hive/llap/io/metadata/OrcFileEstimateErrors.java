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
package org.apache.hadoop.hive.llap.io.metadata;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.io.DiskRangeList;
import org.apache.hadoop.hive.common.io.DataCache.BooleanRef;
import org.apache.hadoop.hive.common.io.DiskRangeList.MutateHelper;
import org.apache.hadoop.hive.llap.IncrementalObjectSizeEstimator;
import org.apache.hadoop.hive.llap.IncrementalObjectSizeEstimator.ObjectEstimator;
import org.apache.hadoop.hive.llap.cache.EvictionDispatcher;
import org.apache.hadoop.hive.llap.cache.LlapCacheableBuffer;
import org.apache.hadoop.hive.ql.io.SyntheticFileId;
import org.apache.hadoop.hive.ql.io.orc.encoded.IncompleteCb;

public class OrcFileEstimateErrors extends LlapCacheableBuffer {
  private final Object fileKey;
  private int estimatedMemUsage;
  private final ConcurrentHashMap<Long, Integer> cache = new ConcurrentHashMap<>();

  private final static HashMap<Class<?>, ObjectEstimator> SIZE_ESTIMATORS;
  private final static ObjectEstimator SIZE_ESTIMATOR;
  static {
    SIZE_ESTIMATORS = IncrementalObjectSizeEstimator.createEstimators(createDummy(
        new SyntheticFileId(new Path("/"), 0, 0)));
    SIZE_ESTIMATOR = SIZE_ESTIMATORS.get(OrcFileEstimateErrors.class);
  }

  public OrcFileEstimateErrors(Object fileKey) {
    this.fileKey = fileKey;
  }

  public void addError(long offset, int length, long baseOffset) {
    Long key = Long.valueOf(offset + baseOffset);
    Integer existingLength = cache.get(key);
    if (existingLength != null && existingLength >= length) return;
    Integer value = Integer.valueOf(length);
    while (true) {
      existingLength = cache.putIfAbsent(key, value);
      if (existingLength == null || existingLength >= length) return;
      cache.remove(key, existingLength);
    }
  }

  public DiskRangeList getIncompleteCbs(
      DiskRangeList ranges, long baseOffset, BooleanRef gotAllData) {
    DiskRangeList prev = ranges.prev;
    if (prev == null) {
      prev = new MutateHelper(ranges);
    }
    DiskRangeList current = ranges;
    gotAllData.value = true; // Assume by default that we would find everything.
    while (current != null) {
      // We assume ranges in "ranges" are non-overlapping; thus, we will save next in advance.
      DiskRangeList check = current;
      current = current.next;
      if (check.hasData()) continue;
      Integer badLength = cache.get(Long.valueOf(check.getOffset() + baseOffset));
      if (badLength == null || badLength < check.getLength()) {
        gotAllData.value = false;
        continue;
      }
      // We could just remove here and handle the missing tail during read, but that can be
      // dangerous; let's explicitly add an incomplete CB.
      check.replaceSelfWith(new IncompleteCb(check.getOffset(), check.getEnd()));
    }
    return prev.next;
  }

  public Object getFileKey() {
    return fileKey;
  }

  public long estimateMemoryUsage() {
    // Since we won't be able to update this as we add, for now, estimate 10x usage.
    // This shouldn't be much and this cache should be remove later anyway.
    estimatedMemUsage = 10 * SIZE_ESTIMATOR.estimate(this, SIZE_ESTIMATORS);
    return estimatedMemUsage;
  }

  private static OrcFileEstimateErrors createDummy(Object fileKey) {
    OrcFileEstimateErrors dummy = new OrcFileEstimateErrors(fileKey);
    dummy.addError(0L, 0, 0L);
    return dummy;
  }

  @Override
  protected int invalidate() {
    return INVALIDATE_OK;
  }

  @Override
  public long getMemoryUsage() {
    return estimatedMemUsage;
  }

  @Override
  public void notifyEvicted(EvictionDispatcher evictionDispatcher) {
    evictionDispatcher.notifyEvicted(this);
  }

  @Override
  protected boolean isLocked() {
    return false;
  }
}