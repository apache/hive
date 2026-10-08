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

package org.apache.hadoop.hive.ql.exec.vector.ptf;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ConstantVectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;

/**
 * Evaluates {@code ntile(n)} as a <b>group-aggregated streaming</b> evaluator (see
 * {@link VectorPTFEvaluatorBase}).
 *
 * <p>NTILE divides the rows of a partition into {@code n} buckets and returns the 1-based bucket
 * number of the current row. When the partition size is not divisible by {@code n}, the first
 * {@code partitionSize % n} buckets each get one extra row, matching {@code GenericUDAFNTile}.
 * The result depends only on row position and partition size, never on peer groups.
 *
 * <p>The partition is buffered so {@link #setPartitionSize(int)} is known before output is
 * written. Bucket boundaries are then derived on the fly while the batches are replayed, so the
 * evaluator keeps O(1) state regardless of partition size, and a batch that falls entirely within
 * one bucket is emitted as a repeating vector.
 *
 * <p>The number of buckets must be a positive integer constant (e.g. {@code ntile(4)}); the
 * Vectorizer leaves any other form to row mode.
 */
public class VectorPTFEvaluatorNTile extends VectorPTFEvaluatorBase {

  private final int numBuckets;

  // Per-partition bucket layout, set up by addStreamingGroupResults.
  private int bucketSize;
  private int largerBucketCount;

  // Replay position within the partition.
  private int currentBucket;
  private int rowsLeftInBucket;
  private int rowsLeftInPartition;

  public VectorPTFEvaluatorNTile(WindowFrameDef windowFrameDef,
      VectorExpression numBucketsExpression, int outputColumnNum) {
    super(windowFrameDef, outputColumnNum);
    if (!(numBucketsExpression instanceof ConstantVectorExpression)) {
      throw new RuntimeException(
          "NTILE expects a constant integer expression for the number of buckets, got: "
              + (numBucketsExpression == null ? "null"
                  : numBucketsExpression.getClass().getName()));
    }
    long value = ((ConstantVectorExpression) numBucketsExpression).getLongValue();
    if (value <= 0 || value > Integer.MAX_VALUE) {
      throw new RuntimeException(
          "NTILE requires a positive integer number of buckets, got: " + value);
    }
    this.numBuckets = (int) value;
    resetEvaluator();
  }

  @Override
  public boolean isGroupAggregatedStreamingEvaluator() {
    return true;
  }

  @Override
  public void addStreamingGroupResults(List<Integer> groupRowCounts) throws HiveException {
    if (partitionSize < 0) {
      throw new HiveException("Partition size must be set before computing ntile");
    }
    // Peer groups are irrelevant: buckets depend only on row position and partition size.
    bucketSize = partitionSize / numBuckets;
    largerBucketCount = partitionSize % numBuckets;
    rowsLeftInPartition = partitionSize;
    currentBucket = 1;
    rowsLeftInBucket = rowsInBucket(currentBucket);
  }

  @Override
  public void evaluateGroupBatch(VectorizedRowBatch batch) throws HiveException {
    final int size = batch.size;
    if (size > rowsLeftInPartition) {
      throw new HiveException("ntile received " + size + " rows but only " + rowsLeftInPartition
          + " remain in the partition (partitionSize=" + partitionSize + ")");
    }
    if (size == 0) {
      return;
    }
    rowsLeftInPartition -= size;

    LongColumnVector outputColVector = (LongColumnVector) batch.cols[outputColumnNum];
    outputColVector.noNulls = true;

    if (size <= rowsLeftInBucket) {
      // The whole batch falls inside the current bucket.
      outputColVector.isRepeating = true;
      outputColVector.isNull[0] = false;
      outputColVector.vector[0] = currentBucket;
      consumeRows(size);
      return;
    }

    outputColVector.isRepeating = false;
    long[] vector = outputColVector.vector;
    int i = 0;
    while (i < size) {
      // rowsLeftInBucket > 0 here: rows remain, so a non-empty bucket remains.
      int run = Math.min(rowsLeftInBucket, size - i);
      Arrays.fill(vector, i, i + run, currentBucket);
      i += run;
      consumeRows(run);
    }
  }

  private void consumeRows(int rows) {
    rowsLeftInBucket -= rows;
    if (rowsLeftInBucket == 0) {
      currentBucket++;
      rowsLeftInBucket = rowsInBucket(currentBucket);
    }
  }

  private int rowsInBucket(int bucket) {
    return bucket <= largerBucketCount ? bucketSize + 1 : bucketSize;
  }

  @Override
  public boolean streamsResult() {
    return true;
  }

  @Override
  public Type getResultColumnVectorType() {
    return Type.LONG;
  }

  @Override
  public void resetEvaluator() {
    partitionSize = -1;
    rowsLeftInPartition = 0;
  }

  // Visible for tests: exposes the number of buckets captured at construction.
  int getNumBuckets() {
    return numBuckets;
  }
}
