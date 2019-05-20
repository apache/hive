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
package org.apache.hadoop.hive.ql.exec.persistence;

import org.apache.hadoop.hive.ql.util.JavaDataModel;

import com.google.common.base.Preconditions;

/**
 * Record which hash table slot entries had key matches for FULL OUTER MapJoin.
 * Supports partitioned match trackers for HybridHashTableContainer.
 */
public final class MatchTracker {

  /*
   * Regular case:
   *    isPartitioned = false
   *    The longMatchFlags array: one bit per hash table slot entry.
   *    If this tracker is underneath a partitioned tracker, then partitionParent is set.
   *
   * Partitioned case:
   *    isPartitioned = true
   *    The partitions array: a tracker for the currently active partitions.
   */
  private final boolean isPartitioned;
  private final MatchTracker partitionParent;
  private final long[] longMatchFlags;
  private final MatchTracker[] partitions;

  private MatchTracker(boolean isPartitioned, MatchTracker partitionParent, int count) {
    this.isPartitioned = isPartitioned;
    this.partitionParent = partitionParent;
    if (!isPartitioned) {
      final int longMatchFlagsSize = (count + Long.SIZE - 1) / Long.SIZE;
      longMatchFlags = new long[longMatchFlagsSize];
      partitions = null;
    } else {
      longMatchFlags = null;
      partitions = new MatchTracker[count];
    }
  }

  /*
   * Create a regular tracker.
   */
  public static MatchTracker create(int logicalHashBucketCount) {
    return new MatchTracker(false, null, logicalHashBucketCount);
  }

  /*
   * Create a partitioned tracker.  Use addPartition and clearPartition to maintain the currently
   * active partition trackers.
   */
  public static MatchTracker createPartitioned(int partitionCount) {
    return new MatchTracker(true, null, partitionCount);
  }

  public boolean getIsPartitioned() {
    return isPartitioned;
  }

  public void addPartition(int partitionId, int logicalHashBucketCount) {
    partitions[partitionId] = new MatchTracker(false, this, logicalHashBucketCount);
  }

  public void clearPartition(int partitionId) {
    partitions[partitionId] = null;
  }

  public MatchTracker getPartition(int partitionId) {
    return partitions[partitionId];
  }

  private boolean isFirstMatch;

  public boolean getIsFirstMatch() {
    return isFirstMatch;
  }

  /*
   * Track a regular hash table slot match.
   * If this tracker is underneath a partitioned tracker, the partitioned tracker's first-match
   * flag will be updated.
   */
  public void trackMatch(int logicalSlotNum) {

    Preconditions.checkState(!isPartitioned);

    final int longWordIndex = logicalSlotNum / Long.SIZE;
    final long longBitMask = 1L << (logicalSlotNum % Long.SIZE);
    if ((longMatchFlags[longWordIndex] & longBitMask) != 0) {

      // Flag is already on.
      isFirstMatch = false;
    } else {
      longMatchFlags[longWordIndex] |= longBitMask;
      isFirstMatch = true;
    }
    if (partitionParent != null) {

      // Push match flag up.
      partitionParent.isFirstMatch = isFirstMatch;
    }
  }

  /*
   * Track a partitioned hash table slot match.
   */
  public void trackPartitionMatch(int partitionId, int logicalSlotNum) {
    partitions[partitionId].trackMatch(logicalSlotNum);
  }

  /*
   * Was a regular hash table slot matched?
   */
  public boolean wasMatched(int logicalSlotNum) {
    final int longWordIndex = logicalSlotNum / Long.SIZE;
    final long longBitMask = 1L << (logicalSlotNum % Long.SIZE);
    return (longMatchFlags[longWordIndex] & longBitMask) != 0;
  }

  /*
   * Was a partitioned hash table slot matched?
   */
  public boolean wasPartitionMatched(int partitionId, int logicalSlotNum) {
    return partitions[partitionId].wasMatched(logicalSlotNum);
  }

  public static int calculateEstimatedMemorySize(int count) {
    // FUTURE: Partitioning not included yet.
    final int longMatchFlagsSize = (count + Long.SIZE - 1) / Long.SIZE;
    int size = 0;
    JavaDataModel jdm = JavaDataModel.get();
    size += jdm.lengthForLongArrayOfSize(longMatchFlagsSize);
    size += jdm.primitive1();
    size += (2 * jdm.object());
    return size;
  }
}
