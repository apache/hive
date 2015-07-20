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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTable;

public abstract class VectorMapJoinFastHashTable implements VectorMapJoinHashTable {
  public static final Log LOG = LogFactory.getLog(VectorMapJoinFastHashTable.class);

  protected int logicalHashBucketCount;
  protected int logicalHashBucketMask;

  protected float loadFactor;
  protected int writeBuffersSize;

  protected int metricPutConflict;
  protected int largestNumberOfSteps;
  protected int keysAssigned;
  protected int resizeThreshold;
  protected int metricExpands;

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
        int initialCapacity, float loadFactor, int writeBuffersSize) {

    initialCapacity = (Long.bitCount(initialCapacity) == 1)
        ? initialCapacity : nextHighestPowerOfTwo(initialCapacity);

    validateCapacity(initialCapacity);

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
}