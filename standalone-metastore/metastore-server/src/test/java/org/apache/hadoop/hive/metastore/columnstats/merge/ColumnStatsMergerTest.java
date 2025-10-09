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
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimator;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimatorFactory;
import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog;
import org.apache.hadoop.hive.metastore.StatisticsTestUtils;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Category(MetastoreUnitTest.class)
public class ColumnStatsMergerTest {

  // the implementation we use does not matter, as we only tests methods from the parent plan here
  private final static ColumnStatsMerger<?> MERGER = new DateColumnStatsMerger();

  private final static List<ColumnStatsMerger<?>> MERGERS = Arrays.asList(
      new BinaryColumnStatsMerger(),
      new BooleanColumnStatsMerger(),
      new DateColumnStatsMerger(),
      new DecimalColumnStatsMerger(),
      new DoubleColumnStatsMerger(),
      new LongColumnStatsMerger(),
      new StringColumnStatsMerger(),
      new TimestampColumnStatsMerger()
  );

  private final static long[] VALUES_1 = { 1, 2 };
  private final static long[] VALUES_2 = { 1, 3 };

  private final static HyperLogLog HLL_1 = StatisticsTestUtils.createHll(VALUES_1);
  private final static HyperLogLog HLL_2 = StatisticsTestUtils.createHll(VALUES_2);

  private final static KllFloatsSketch KLL_1 = StatisticsTestUtils.createKll(VALUES_1);
  private final static KllFloatsSketch KLL_2 = StatisticsTestUtils.createKll(VALUES_2);

  @Test
  public void testMergeNumDVs() {
    assertEquals(3, MERGER.mergeNumDVs(1, 3));
    assertEquals(3, MERGER.mergeNumDVs(3, 1));
  }

  @Test
  public void testMergeNumNulls() {
    assertEquals(4, MERGER.mergeNumNulls(1, 3));
    assertEquals(4, MERGER.mergeNumNulls(3, 1));
  }

  @Test
  public void testMergeMaxColLen() {
    assertEquals(3, MERGER.mergeMaxColLen(1, 3));
    assertEquals(3, MERGER.mergeMaxColLen(3, 1));
  }

  @Test
  public void testMergeAvgColLen() {
    assertEquals(3, MERGER.mergeAvgColLen(1, 3), Double.MIN_VALUE);
    assertEquals(3, MERGER.mergeAvgColLen(3, 1), Double.MIN_VALUE);
  }

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

    assertEquals(expectedEstimator.getSketch().toString(), computedEstimator.getSketch().toString());
  }

  @Test
  public void testMergeHistogramEstimatorsFirstNull() {
    KllHistogramEstimator estimator2 =
        KllHistogramEstimatorFactory.getKllHistogramEstimator(KLL_2.toByteArray());

    KllHistogramEstimator computedEstimator = MERGER.mergeHistogramEstimator("", null, estimator2);

    assertEquals(estimator2.getSketch().toString(), computedEstimator.getSketch().toString());
  }

  @Test
  public void testMergeHistogramEstimatorsSecondNull() {
    KllHistogramEstimator estimator1 =
        KllHistogramEstimatorFactory.getKllHistogramEstimator(KLL_1.toByteArray());

    KllHistogramEstimator computedEstimator = MERGER.mergeHistogramEstimator("", estimator1, null);

    assertEquals(estimator1.getSketch().toString(), computedEstimator.getSketch().toString());
  }

  @Test
  public void testMergeNullHistogramEstimators() {
    assertNull(MERGER.mergeHistogramEstimator("", null, null));
  }

  @Test
  public void testMergeNonNullNDVEstimators() {
    NumDistinctValueEstimator estimator1 =
        NumDistinctValueEstimatorFactory.getNumDistinctValueEstimator(HLL_1.serialize());
    NumDistinctValueEstimator estimator2 =
        NumDistinctValueEstimatorFactory.getNumDistinctValueEstimator(HLL_2.serialize());

    for (ColumnStatsMerger<?> MERGER : MERGERS) {
      long computedNDV = MERGER.mergeNumDistinctValueEstimator(
          "", Arrays.asList(estimator1, estimator2), 2, 2);
      assertEquals(3, computedNDV);
    }
  }

  @Test
  public void testMergeNDVEstimatorsFirstNull() {
    NumDistinctValueEstimator estimator2 =
        NumDistinctValueEstimatorFactory.getNumDistinctValueEstimator(HLL_2.serialize());

    for (ColumnStatsMerger<?> MERGER : MERGERS) {
      List<NumDistinctValueEstimator> estimatorList = Arrays.asList(null, estimator2);
      long computedNDV = MERGER.mergeNumDistinctValueEstimator("", estimatorList, 1, 2);

      assertEquals(estimator2, estimatorList.get(0));
      assertEquals(2, computedNDV);
    }
  }

  @Test
  public void testMergeNDVEstimatorsSecondNull() {
    NumDistinctValueEstimator estimator1 =
        NumDistinctValueEstimatorFactory.getNumDistinctValueEstimator(HLL_1.serialize());

    for (ColumnStatsMerger<?> MERGER : MERGERS) {
      List<NumDistinctValueEstimator> estimatorList = Arrays.asList(estimator1, null);
      long computedNDV = MERGER.mergeNumDistinctValueEstimator("", estimatorList, 2, 1);

      assertEquals(Arrays.asList(estimator1, null), estimatorList);
      assertEquals(2, computedNDV);
    }
  }

  @Test
  public void testMergeNullNDVEstimators() {
    List<NumDistinctValueEstimator> estimatorList = Arrays.asList(null, null);

    for (ColumnStatsMerger<?> MERGER : MERGERS) {
      long computedNDV = MERGER.mergeNumDistinctValueEstimator("", estimatorList, 1, 2);
      assertEquals(2, computedNDV);
      assertEquals(Arrays.asList(null, null), estimatorList);
    }
  }

  protected static ColumnStatisticsObj createColumnStatisticsObj(ColumnStatisticsData columnStatisticsData) {
    ColumnStatisticsObj columnStatisticsObj = new ColumnStatisticsObj();
    columnStatisticsObj.setStatsData(columnStatisticsData);
    return columnStatisticsObj;
  }
}
