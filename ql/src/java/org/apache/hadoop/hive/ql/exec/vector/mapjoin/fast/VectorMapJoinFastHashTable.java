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
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTable;

public abstract class VectorMapJoinFastHashTable implements VectorMapJoinHashTable {
  public static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastHashTable.class);

  protected int logicalHashBucketCount;
  protected int logicalHashBucketMask;

  protected float loadFactor;
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
    if (Long.bitCount(capacity) != 1) {
      throw new AssertionError("Capacity must be a power of two");
    }
    if (capacity <= 0) {
      throw new AssertionError("Invalid capacity " + capacity);
    }
  }

  private static int nextHighestPowerOfTwo(int v) {
    return Integer.highestOneBit(v) << 1;
  }

  public VectorMapJoinFastHashTable(
        int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount) {

    initialCapacity = (Long.bitCount(initialCapacity) == 1)
        ? initialCapacity : nextHighestPowerOfTwo(initialCapacity);

    validateCapacity(initialCapacity);

    this.estimatedKeyCount = estimatedKeyCount;

    logicalHashBucketCount = initialCapacity;
    logicalHashBucketMask = logicalHashBucketCount - 1;
    resizeThreshold = (int)(logicalHashBucketCount * loadFactor);

    this.loadFactor = loadFactor;
    this.writeBuffersSize = writeBuffersSize;
  }

  @Override
  public int size() {
    return keysAssigned;
  }

  @Override
  public long getEstimatedMemorySize() {
    JavaDataModel jdm = JavaDataModel.get();
    return JavaDataModel.alignUp(10L * jdm.primitive1() + jdm.primitive2(), jdm.memoryAlign());
  }
}
