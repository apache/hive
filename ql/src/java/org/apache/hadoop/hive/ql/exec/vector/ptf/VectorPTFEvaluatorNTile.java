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

import com.google.common.base.Preconditions;

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
 * <p>The number of buckets is either a positive integer constant (e.g. {@code ntile(4)}) or any
 * other int expression (e.g. {@code ntile(col)}), which, as in {@code GenericUDAFNTile}, is read
 * from the first row of each partition. The Vectorizer leaves a non-positive constant to row mode.
 */
public class VectorPTFEvaluatorNTile extends VectorPTFEvaluatorBase {

  // Number of buckets of a constant argument, or null when it is read from the partition's first
  // row.
  private final Long constantNumBuckets;

  // Per-partition bucket layout, set up by setUpBuckets once the first batch is replayed.
  private int bucketSize;
  private int largerBucketCount;

  // Replay position within the partition; 0 until the bucket layout is set up.
  private int currentBucket;
  private int rowsLeftInBucket;

  public VectorPTFEvaluatorNTile(WindowFrameDef windowFrameDef,
      VectorExpression numBucketsExpression, int outputColumnNum) {
    super(windowFrameDef, getInputExpression(numBucketsExpression), outputColumnNum);
    constantNumBuckets = numBucketsExpression instanceof ConstantVectorExpression constantExpr
        ? constantExpr.getLongValue() : null;
    resetEvaluator();
  }

  /**
   * Returns the input expression to hand to the base class, failing before it is constructed if
   * there is no number of buckets. A constant needs no input column; any other expression is
   * evaluated before batches are buffered, and its result column is read back when the partition's
   * first batch is replayed.
   */
  private static VectorExpression getInputExpression(VectorExpression numBucketsExpression) {
    Preconditions.checkArgument(numBucketsExpression != null,
        "NTILE expects an expression for the number of buckets");
    return numBucketsExpression instanceof ConstantVectorExpression ? null : numBucketsExpression;
  }

  @Override
  public boolean isGroupAggregatedStreamingEvaluator() {
    return true;
  }

  @Override
  public void addStreamingGroupResults(List<Integer> groupRowCounts) throws HiveException {
    if (partitionSize <= 0) {
      throw new HiveException("Partition size must be set before computing ntile");
    }
    // Peer groups are irrelevant: buckets depend only on row position and partition size. The
    // layout is set up on the first replayed batch, which holds the row a non-constant number of
    // buckets is read from.
    currentBucket = 0;
  }

  /**
   * Validates the number of buckets (null for NULL), once per partition, and sets up the bucket
   * layout. This is the only place it is validated, as a non-constant one is known only once the
   * partition's first row is replayed. Mirrors {@code GenericUDAFNTile}, which does not validate
   * it: wherever row mode produces a result this produces the same one, and wherever row mode
   * fails this fails fast.
   */
  private void setUpBuckets(Long numBuckets) throws HiveException {
    final int buckets;
    if (numBuckets != null && numBuckets > 0) {
      // With more buckets than rows every row gets its own bucket, which is ntile(partitionSize);
      // capping the bucket count keeps every bucket non-empty (and fits it in an int).
      buckets = (int) Math.min(numBuckets, partitionSize);
    } else if (numBuckets != null && numBuckets < -partitionSize) {
      // Row mode's truncating division yields an empty base bucket size, so every row gets its own
      // bucket, as for a positive count larger than the partition.
      buckets = partitionSize;
    } else {
      // Row mode throws on NULL, divides by zero on 0, and for any other negative count loops until
      // it runs out of memory.
      throw new HiveException("NTILE requires a positive number of buckets, got: "
          + (numBuckets == null ? "NULL" : numBuckets));
    }
    bucketSize = partitionSize / buckets;
    largerBucketCount = partitionSize % buckets;
    currentBucket = 1;
    rowsLeftInBucket = rowsInBucket(currentBucket);
  }

  /**
   * Reads the number of buckets, or null for NULL, from the first row of the partition, held by the
   * first replayed batch. Replayed batches never use a selection vector, so that row is at index 0,
   * which is also where a repeating vector (e.g. a partition key column) keeps its value.
   */
  private Long readNumBuckets(VectorizedRowBatch batch) {
    LongColumnVector numBucketsColVector = (LongColumnVector) batch.cols[inputColumnNum];
    return !numBucketsColVector.noNulls && numBucketsColVector.isNull[0]
        ? null : numBucketsColVector.vector[0];
  }

  @Override
  public void evaluateGroupBatch(VectorizedRowBatch batch) throws HiveException {
    if (currentBucket == 0) {
      setUpBuckets(constantNumBuckets != null ? constantNumBuckets : readNumBuckets(batch));
    }
    final int size = batch.size;
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
      // Buckets are never empty, so every run covers at least one row.
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
  }
}
