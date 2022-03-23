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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast;

import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.mapjoin.MapJoinMemoryExhaustionError;
import org.apache.hadoop.hive.ql.exec.persistence.MatchTracker;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTable;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinNonMatchedIterator;

public abstract class VectorMapJoinFastHashTable implements VectorMapJoinHashTable {
  public static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastHashTable.class);

  // when rehashing, jump directly to 1M items
  public static final int FIRST_SIZE_UP = 1048576;

  protected final boolean isFullOuter;

  protected int logicalHashBucketCount;
  protected int logicalHashBucketMask;

  protected final float loadFactor;
  protected final int writeBuffersSize;

  protected long estimatedKeyCount;

  protected int metricPutConflict;
  protected int largestNumberOfSteps;
  protected int keysAssigned;
  protected int resizeThreshold;
  protected int metricExpands;

  // 2^30 (we cannot use Integer.MAX_VALUE which is 2^31-1).
  public static final int HIGHEST_INT_POWER_OF_2 = 1073741824;

  public static final int ONE_QUARTER_LIMIT = HIGHEST_INT_POWER_OF_2 / 4;
  public static final int ONE_SIXTH_LIMIT = HIGHEST_INT_POWER_OF_2 / 6;

  public void throwExpandError(int limit, String dataTypeName) {
    throw new MapJoinMemoryExhaustionError(
        "Vector MapJoin " + dataTypeName + " Hash Table cannot grow any more -- use a smaller container size. " +
        "Current logical size is " + logicalHashBucketCount + " and " +
        "the limit is " + limit + ". " +
        "Estimated key count was " + (estimatedKeyCount == -1 ? "not available" : estimatedKeyCount) + ".");
  }

  private static void validateCapacity(long capacity) {
    if (capacity <= 0) {
      throw new AssertionError("Invalid capacity " + capacity);
    }
    if (Long.bitCount(capacity) != 1) {
      throw new AssertionError("Capacity must be a power of two " + capacity);
    }
  }

  private static int nextHighestPowerOfTwo(int v) {
    int value = Integer.highestOneBit(v);
    if (Integer.highestOneBit(v) == HIGHEST_INT_POWER_OF_2) {
      LOG.warn("Reached highest 2 power: {}", HIGHEST_INT_POWER_OF_2);
      return value;
    }
    return value << 1;
  }

  public VectorMapJoinFastHashTable(
      boolean isFullOuter,
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount) {

    this.isFullOuter = isFullOuter;

    this.logicalHashBucketCount = (Long.bitCount(initialCapacity) == 1)
        ? initialCapacity : nextHighestPowerOfTwo(initialCapacity);
    LOG.info("Initial Capacity {} Recomputed Capacity {}", initialCapacity, logicalHashBucketCount);

    validateCapacity(logicalHashBucketCount);

    this.estimatedKeyCount = estimatedKeyCount;
    logicalHashBucketMask = logicalHashBucketCount - 1;
    resizeThreshold = (int)(logicalHashBucketCount * loadFactor);

    this.loadFactor = loadFactor;
    this.writeBuffersSize = writeBuffersSize;
  }

  @Override
  public int size() {
    return keysAssigned;
  }

  protected final boolean checkResize() {
    // resize small hashtables up to a higher width (4096 items), but when there are collisions
    return (resizeThreshold <= keysAssigned)
        || (logicalHashBucketCount <= FIRST_SIZE_UP && largestNumberOfSteps > 1);
  }

  @Override
  public long getEstimatedMemorySize() {
    int size = 0;
    JavaDataModel jdm = JavaDataModel.get();
    size += JavaDataModel.alignUp(10L * jdm.primitive1() + jdm.primitive2(), jdm.memoryAlign());
    if (isFullOuter) {
      size += MatchTracker.calculateEstimatedMemorySize(logicalHashBucketCount);
    }
    return size;
  }

  @Override
  public MatchTracker createMatchTracker() {
    return MatchTracker.create(logicalHashBucketCount);
  }

  @Override
  public VectorMapJoinNonMatchedIterator createNonMatchedIterator(MatchTracker matchTracker) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public int spillPartitionId() {
    throw new RuntimeException("Not implemented");
  }
}
