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

package org.apache.hadoop.hive.metastore.columnstats.merge;

import com.google.common.primitives.Longs;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.hadoop.hive.common.histogram.KllHistogramEstimator;
import org.apache.hadoop.hive.common.histogram.KllHistogramEstimatorFactory;
import org.apache.hadoop.hive.metastore.StatisticsTestUtils;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreUnitTest.class)
public class ColumnStatsMergerTest {

  // the implementation we use does not matter, as we only tests methods from the parent plan here
  private final static ColumnStatsMerger MERGER = new DateColumnStatsMerger();

  private final static long[] VALUES_1 = { 1, 2 };
  private final static long[] VALUES_2 = { 1, 3 };

  private final static KllFloatsSketch KLL_1 = StatisticsTestUtils.createKll(VALUES_1);
  private final static KllFloatsSketch KLL_2 = StatisticsTestUtils.createKll(VALUES_2);

  @Test
  public void testMergeNonNullHistogramEstimators() {
    KllHistogramEstimator estimator1 =
        KllHistogramEstimatorFactory.getKllHistogramEstimator(KLL_1.toByteArray());
    KllHistogramEstimator estimator2 =
        KllHistogramEstimatorFactory.getKllHistogramEstimator(KLL_2.toByteArray());

    KllHistogramEstimator computedEstimator = MERGER.mergeHistogramEstimator("", estimator1, estimator2);

    KllFloatsSketch expectedKll = StatisticsTestUtils.createKll(Longs.concat(VALUES_1, VALUES_2));
    KllHistogramEstimator expectedEstimator =
        KllHistogramEstimatorFactory.getKllHistogramEstimator(expectedKll.toByteArray());

    Assert.assertEquals(expectedEstimator.getSketch().toString(), computedEstimator.getSketch().toString());
  }

  @Test
  public void testMergeHistogramEstimatorsFirstNull() {
    KllHistogramEstimator estimator2 =
        KllHistogramEstimatorFactory.getKllHistogramEstimator(KLL_2.toByteArray());

    KllHistogramEstimator computedEstimator = MERGER.mergeHistogramEstimator("", null, estimator2);

    Assert.assertEquals(estimator2.getSketch().toString(), computedEstimator.getSketch().toString());
  }

  @Test
  public void testMergeHistogramEstimatorsSecondNull() {
    KllHistogramEstimator estimator1 =
        KllHistogramEstimatorFactory.getKllHistogramEstimator(KLL_1.toByteArray());

    KllHistogramEstimator computedEstimator = MERGER.mergeHistogramEstimator("", estimator1, null);

    Assert.assertEquals(estimator1.getSketch().toString(), computedEstimator.getSketch().toString());
  }

  @Test
  public void testMergeNullHistogramEstimators() {
    Assert.assertNull(MERGER.mergeHistogramEstimator("", null, null));
  }
}
