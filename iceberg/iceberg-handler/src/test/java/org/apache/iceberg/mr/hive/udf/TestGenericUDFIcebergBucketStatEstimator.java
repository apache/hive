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

package org.apache.iceberg.mr.hive.udf;

import java.util.Arrays;
import java.util.Optional;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.stats.estimator.StatEstimator;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the BucketStatEstimator in GenericUDFIcebergBucket.
 * Verifies that the StatEstimator correctly narrows NDV based on bucket count.
 */
public class TestGenericUDFIcebergBucketStatEstimator {

  @Test
  public void testNdvNarrowedByBucketCount() {
    // source NDV (100) > numBuckets (8) -> output NDV should be 8
    Optional<ColStatistics> result = estimateBucket(100, 8);
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(8, result.get().getCountDistint());
  }

  @Test
  public void testNdvBelowBucketCount() {
    // source NDV (3) < numBuckets (8) -> output NDV should be 3
    Optional<ColStatistics> result = estimateBucket(3, 8);
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(3, result.get().getCountDistint());
  }

  @Test
  public void testNdvEqualsBucketCount() {
    // source NDV (8) == numBuckets (8) -> output NDV should be 8
    Optional<ColStatistics> result = estimateBucket(8, 8);
    Assert.assertTrue(result.isPresent());
    Assert.assertEquals(8, result.get().getCountDistint());
  }

  @Test
  public void testZeroBucketsReturnsEmpty() {
    Optional<ColStatistics> result = estimateBucket(100, 0);
    Assert.assertFalse(result.isPresent());
  }

  private Optional<ColStatistics> estimateBucket(long sourceNdv, long numBuckets) {
    ColStatistics sourceStats = new ColStatistics("col", "int");
    sourceStats.setCountDistint(sourceNdv);
    ColStatistics numBucketsStats = new ColStatistics("numBuckets", "int");
    numBucketsStats.setRange(numBuckets, numBuckets);

    StatEstimator estimator = new GenericUDFIcebergBucket().getStatEstimator();
    return estimator.estimate(Arrays.asList(sourceStats, numBucketsStats));
  }
}
