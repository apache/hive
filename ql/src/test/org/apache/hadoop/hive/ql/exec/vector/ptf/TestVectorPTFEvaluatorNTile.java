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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.expressions.ConstantVectorExpression;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowType;
import org.apache.hadoop.hive.ql.plan.ptf.BoundaryDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for {@link VectorPTFEvaluatorNTile}. The tests validate that the vectorized ntile
 * evaluator produces the same bucket numbers as the row-mode {@code GenericUDAFNTile}
 * implementation for various partition sizes, batch sizes, and bucket counts (including cases
 * where the partition size is not evenly divisible by the number of buckets).
 */
public class TestVectorPTFEvaluatorNTile {

  private static final int OUTPUT_COL_NUM = 0;

  /**
   * A default window frame — ntile is a ranking function and does not depend on the frame, but the
   * base evaluator requires one.
   */
  private static WindowFrameDef defaultFrame() {
    return new WindowFrameDef(WindowType.ROWS,
        new BoundaryDef(Direction.PRECEDING, Integer.MAX_VALUE),
        new BoundaryDef(Direction.CURRENT, 0));
  }

  private static VectorExpression numBucketsExpr(long value) throws HiveException {
    // The output column of the constant is irrelevant — the evaluator only reads its long value.
    return new ConstantVectorExpression(/* outputColumnNum */ 0, value, TypeInfoFactory.intTypeInfo);
  }

  private static VectorPTFEvaluatorNTile newEvaluator(int numBuckets) throws HiveException {
    return new VectorPTFEvaluatorNTile(defaultFrame(), numBucketsExpr(numBuckets), OUTPUT_COL_NUM);
  }

  /**
   * Mirrors {@code GenericUDAFNTile.terminate} — the row-mode reference implementation. Any
   * discrepancy against this method means the vectorized evaluator would produce different results
   * than the non-vectorized code path, which is exactly what we want to guard against.
   */
  private static long[] expectedBuckets(int partitionSize, int numBuckets) {
    long[] out = new long[partitionSize];
    if (partitionSize == 0) {
      return out;
    }
    int bucketSize = partitionSize / numBuckets;
    int rem = partitionSize % numBuckets;
    int start = 0;
    int bucket = 1;
    while (start < partitionSize) {
      int end = start + bucketSize;
      if (rem > 0) {
        end++;
        rem--;
      }
      end = Math.min(partitionSize, end);
      for (int i = start; i < end; i++) {
        out[i] = bucket;
      }
      start = end;
      bucket++;
    }
    return out;
  }

  private static VectorizedRowBatch newBatch(int capacity) {
    VectorizedRowBatch batch = new VectorizedRowBatch(1, capacity);
    LongColumnVector out = new LongColumnVector(capacity);
    out.init();
    batch.cols[OUTPUT_COL_NUM] = out;
    return batch;
  }

  private static void startPartition(VectorPTFEvaluatorNTile evaluator, int partitionSize)
      throws HiveException {
    evaluator.setPartitionSize(partitionSize);
    // NTile does not use peer group info; passing empty list is enough.
    evaluator.addStreamingGroupResults(Collections.emptyList());
  }

  /**
   * Feed the evaluator through the same three-step protocol that {@code VectorPTFGroupBatches}
   * uses: (1) {@link VectorPTFEvaluatorBase#setPartitionSize(int)}, (2)
   * {@link VectorPTFEvaluatorBase#addStreamingGroupResults(List)}, (3) repeated
   * {@link VectorPTFEvaluatorBase#evaluateGroupBatch(VectorizedRowBatch)} calls to fill the output
   * column vector — and return the emitted bucket numbers as one flat array.
   */
  private static long[] runEvaluator(int partitionSize, int numBuckets, int batchSize)
      throws HiveException {
    VectorPTFEvaluatorNTile evaluator = newEvaluator(numBuckets);
    startPartition(evaluator, partitionSize);
    return drain(evaluator, partitionSize, batchSize);
  }

  private static long[] drain(VectorPTFEvaluatorNTile evaluator, int partitionSize, int batchSize)
      throws HiveException {
    // Reuse one batch like VectorPTFGroupBatches does with its overflow batch, so stale
    // isRepeating state left by a previous batch would be caught.
    VectorizedRowBatch batch = newBatch(batchSize);
    LongColumnVector out = (LongColumnVector) batch.cols[OUTPUT_COL_NUM];
    long[] result = new long[partitionSize];
    for (int emitted = 0; emitted < partitionSize; emitted += batch.size) {
      batch.size = Math.min(batchSize, partitionSize - emitted);
      evaluator.evaluateGroupBatch(batch);

      Assert.assertTrue("output must be marked noNulls", out.noNulls);
      for (int i = 0; i < batch.size; i++) {
        result[emitted + i] = out.vector[out.isRepeating ? 0 : i];
      }
    }
    return result;
  }

  @Test
  public void testEvenSplitSingleBatch() throws HiveException {
    // 10 rows into 5 buckets => 2 rows per bucket, no remainder
    long[] actual = runEvaluator(10, 5, 10);
    Assert.assertArrayEquals(new long[] { 1, 1, 2, 2, 3, 3, 4, 4, 5, 5 }, actual);
    Assert.assertArrayEquals(expectedBuckets(10, 5), actual);
  }

  @Test
  public void testUnevenSplitRemainderInEarlyBuckets() throws HiveException {
    // 10 rows into 3 buckets => bucket sizes 4, 3, 3 (rem=1 goes to bucket 1 twice — see logic)
    // Per GenericUDAFNTile: 10/3 = 3, rem = 1 -> bucket1 gets 4 rows, bucket2 gets 3, bucket3 gets 3
    long[] actual = runEvaluator(10, 3, 10);
    Assert.assertArrayEquals(new long[] { 1, 1, 1, 1, 2, 2, 2, 3, 3, 3 }, actual);
    Assert.assertArrayEquals(expectedBuckets(10, 3), actual);
  }

  @Test
  public void testMoreBucketsThanRows() throws HiveException {
    // 3 rows into 5 buckets => first 3 buckets get 1 row each, buckets 4 and 5 are empty
    long[] actual = runEvaluator(3, 5, 3);
    Assert.assertArrayEquals(new long[] { 1, 2, 3 }, actual);
    Assert.assertArrayEquals(expectedBuckets(3, 5), actual);
  }

  @Test
  public void testSingleBucket() throws HiveException {
    // Everything falls into bucket 1
    long[] actual = runEvaluator(7, 1, 3);
    Assert.assertArrayEquals(new long[] { 1, 1, 1, 1, 1, 1, 1 }, actual);
  }

  @Test
  public void testAcrossMultipleBatches() throws HiveException {
    // 11 rows into 4 buckets, split into small batches of 3
    // 11/4=2 rem=3 -> bucket sizes: 3,3,3,2
    long[] actual = runEvaluator(11, 4, 3);
    long[] expected = expectedBuckets(11, 4);
    Assert.assertArrayEquals(expected, actual);
    // Sanity check the expected shape
    Assert.assertArrayEquals(new long[] { 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4 }, actual);
  }

  @Test
  public void testLargePartitionRandomBatchSizes() throws HiveException {
    // Sweep a range of (partitionSize, numBuckets) pairs, comparing against the row-mode reference
    for (int partitionSize = 0; partitionSize <= 25; partitionSize++) {
      for (int numBuckets = 1; numBuckets <= 10; numBuckets++) {
        for (int batchSize : new int[] { 1, 2, 3, 5, 8, 25 }) {
          long[] actual = runEvaluator(partitionSize, numBuckets, batchSize);
          long[] expected = expectedBuckets(partitionSize, numBuckets);
          Assert.assertArrayEquals(String.format(
              "mismatch for partitionSize=%d numBuckets=%d batchSize=%d", partitionSize,
              numBuckets, batchSize), expected, actual);
        }
      }
    }
  }

  @Test
  public void testResetEvaluatorAllowsReuse() throws HiveException {
    // Same evaluator instance should be usable across two partitions after resetEvaluator.
    VectorPTFEvaluatorNTile evaluator = newEvaluator(4);

    // First partition: 8 rows / 4 buckets = 2 per bucket
    startPartition(evaluator, 8);
    long[] first = drain(evaluator, 8, 8);
    Assert.assertArrayEquals(new long[] { 1, 1, 2, 2, 3, 3, 4, 4 }, first);

    evaluator.resetEvaluator();

    // Second partition: 6 rows / 4 buckets = bucket sizes 2, 2, 1, 1
    startPartition(evaluator, 6);
    long[] second = drain(evaluator, 6, 3);
    Assert.assertArrayEquals(new long[] { 1, 1, 2, 2, 3, 4 }, second);
  }

  @Test
  public void testRepeatingOnlyWhenBatchWithinOneBucket() throws HiveException {
    // 6 rows into 2 buckets => 1, 1, 1, 2, 2, 2
    VectorPTFEvaluatorNTile evaluator = newEvaluator(2);
    startPartition(evaluator, 6);
    VectorizedRowBatch batch = newBatch(4);
    LongColumnVector out = (LongColumnVector) batch.cols[OUTPUT_COL_NUM];

    // First batch straddles the bucket boundary.
    batch.size = 4;
    evaluator.evaluateGroupBatch(batch);
    Assert.assertFalse(out.isRepeating);
    Assert.assertArrayEquals(new long[] { 1, 1, 1, 2 }, Arrays.copyOf(out.vector, 4));

    // Second batch lies within bucket 2.
    batch.size = 2;
    evaluator.evaluateGroupBatch(batch);
    Assert.assertTrue(out.isRepeating);
    Assert.assertEquals(2, out.vector[0]);
  }

  @Test
  public void testLargePartitionStreamsWithoutMaterializing() throws HiveException {
    // Materializing per-row bucket numbers for ~100M rows would take ~800MB of heap.
    final int partitionSize = 100_000_007;
    final int numBuckets = 7;
    VectorPTFEvaluatorNTile evaluator = newEvaluator(numBuckets);
    startPartition(evaluator, partitionSize);

    VectorizedRowBatch batch = newBatch(VectorizedRowBatch.DEFAULT_SIZE);
    LongColumnVector out = (LongColumnVector) batch.cols[OUTPUT_COL_NUM];
    long[] rowsPerBucket = new long[numBuckets + 1];
    long previousBucket = 1;
    for (int emitted = 0; emitted < partitionSize; emitted += batch.size) {
      batch.size = Math.min(VectorizedRowBatch.DEFAULT_SIZE, partitionSize - emitted);
      evaluator.evaluateGroupBatch(batch);
      int distinctValues = out.isRepeating ? 1 : batch.size;
      for (int i = 0; i < distinctValues; i++) {
        long bucket = out.vector[i];
        Assert.assertTrue("buckets must be contiguous",
            bucket == previousBucket || bucket == previousBucket + 1);
        rowsPerBucket[(int) bucket] += out.isRepeating ? batch.size : 1;
        previousBucket = bucket;
      }
    }
    // 100_000_007 = 7 * 14_285_715 + 2, so the first two buckets get one extra row.
    Assert.assertArrayEquals(new long[] { 0, 14_285_716, 14_285_716, 14_285_715, 14_285_715,
        14_285_715, 14_285_715, 14_285_715 }, rowsPerBucket);
  }

  @Test
  public void testMoreRowsThanPartitionSizeThrows() throws HiveException {
    VectorPTFEvaluatorNTile evaluator = newEvaluator(2);
    startPartition(evaluator, 3);
    VectorizedRowBatch batch = newBatch(4);
    batch.size = 4;
    try {
      evaluator.evaluateGroupBatch(batch);
      Assert.fail("expected HiveException when a batch exceeds the remaining partition rows");
    } catch (HiveException expected) {
      // ok
    }
  }

  @Test
  public void testEvaluateBeforePrecomputeThrows() throws HiveException {
    VectorPTFEvaluatorNTile evaluator = newEvaluator(4);
    VectorizedRowBatch batch = newBatch(1);
    batch.size = 1;

    try {
      evaluator.evaluateGroupBatch(batch);
      Assert.fail("expected HiveException when evaluating before addStreamingGroupResults");
    } catch (HiveException expected) {
      // ok
    }
  }

  @Test
  public void testPrecomputeBeforeSetPartitionSizeThrows() throws HiveException {
    VectorPTFEvaluatorNTile evaluator = newEvaluator(4);
    try {
      evaluator.addStreamingGroupResults(Collections.emptyList());
      Assert.fail("expected HiveException when precomputing without partition size");
    } catch (HiveException expected) {
      // ok
    }
  }

  @Test
  public void testConstructorRejectsNonPositiveBuckets() throws HiveException {
    try {
      new VectorPTFEvaluatorNTile(defaultFrame(), numBucketsExpr(0), OUTPUT_COL_NUM);
      Assert.fail("expected RuntimeException for numBuckets == 0");
    } catch (RuntimeException expected) {
      // ok
    }
    try {
      new VectorPTFEvaluatorNTile(defaultFrame(), numBucketsExpr(-3), OUTPUT_COL_NUM);
      Assert.fail("expected RuntimeException for negative numBuckets");
    } catch (RuntimeException expected) {
      // ok
    }
  }

  @Test
  public void testConstructorRejectsNonConstantArgument() {
    // A generic VectorExpression that is not a ConstantVectorExpression should be rejected —
    // ntile requires a compile-time constant number of buckets.
    VectorExpression notAConstant = new VectorExpression() {
      @Override
      public void evaluate(VectorizedRowBatch batch) {
      }

      @Override
      public String vectorExpressionParameters() {
        return "";
      }

      @Override
      public VectorExpressionDescriptor.Descriptor getDescriptor() {
        return null;
      }
    };
    try {
      new VectorPTFEvaluatorNTile(defaultFrame(), notAConstant, OUTPUT_COL_NUM);
      Assert.fail("expected RuntimeException for non-constant number of buckets");
    } catch (RuntimeException expected) {
      // ok
    }
  }

  @Test
  public void testResultColumnVectorTypeIsLong() throws HiveException {
    Assert.assertEquals(ColumnVector.Type.LONG, newEvaluator(4).getResultColumnVectorType());
  }

  @Test
  public void testStreamingFlags() throws HiveException {
    VectorPTFEvaluatorNTile evaluator = newEvaluator(4);
    Assert.assertTrue("ntile should be a streaming evaluator", evaluator.streamsResult());
    Assert.assertTrue("ntile should be a group-aggregated streaming evaluator",
        evaluator.isGroupAggregatedStreamingEvaluator());
  }

  /**
   * Regression guard — several unrelated helpers rely on {@code Arrays.asList(...)} not being
   * mutated by the evaluator when passing peer group counts. We use an unmodifiable list here to
   * catch any accidental mutation.
   */
  @Test
  public void testPeerGroupCountsAreNotMutated() throws HiveException {
    VectorPTFEvaluatorNTile evaluator = newEvaluator(4);
    evaluator.setPartitionSize(8);
    List<Integer> peerGroups =
        Collections.unmodifiableList(new ArrayList<>(Arrays.asList(2, 3, 3)));
    // Should not throw — evaluator must not attempt to modify the list.
    evaluator.addStreamingGroupResults(peerGroups);
    Assert.assertEquals(Arrays.asList(2, 3, 3), peerGroups);
  }
}
