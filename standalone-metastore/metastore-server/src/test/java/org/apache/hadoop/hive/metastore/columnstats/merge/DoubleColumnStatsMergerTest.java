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

import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.columnstats.ColStatsBuilder;
import org.apache.hadoop.hive.metastore.columnstats.cache.DoubleColumnStatsDataInspector;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.hadoop.hive.metastore.columnstats.merge.ColumnStatsMergerTest.createColumnStatisticsObj;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Category(MetastoreUnitTest.class)
public class DoubleColumnStatsMergerTest {
  private final DoubleColumnStatsMerger merger = new DoubleColumnStatsMerger();

  private static final DoubleColumnStatsDataInspector DATA_1 = new DoubleColumnStatsDataInspector();
  private static final DoubleColumnStatsDataInspector DATA_2 = new DoubleColumnStatsDataInspector();
  private static final DoubleColumnStatsDataInspector DATA_3 = new DoubleColumnStatsDataInspector();

  static {
    DATA_1.setLowValue(1d);
    DATA_1.setHighValue(1d);
    DATA_2.setLowValue(2d);
    DATA_2.setHighValue(2d);
    DATA_3.setLowValue(3d);
    DATA_3.setHighValue(3d);
  }

  @Test
  public void testMergeNullValues() {
    ColumnStatisticsObj aggrObj = createColumnStatisticsObj(new ColStatsBuilder<>(double.class)
        .low(null)
        .high(null)
        .numNulls(1)
        .numDVs(0)
        .build());
    merger.merge(aggrObj, aggrObj);

    ColumnStatisticsData expectedColumnStatisticsData = new ColStatsBuilder<>(double.class)
        .low(null)
        .high(null)
        .numNulls(2)
        .numDVs(0)
        .build();

    assertEquals(expectedColumnStatisticsData, aggrObj.getStatsData());
  }

  @Test
  public void testMergeNullWithNonNullValues() {
    ColumnStatisticsObj aggrObj = createColumnStatisticsObj(new ColStatsBuilder<>(double.class)
        .low(null)
        .high(null)
        .numNulls(0)
        .numDVs(0)
        .build());
    ColumnStatisticsObj newObj = createColumnStatisticsObj(new ColStatsBuilder<>(double.class)
        .low(1d)
        .high(3d)
        .numNulls(4)
        .numDVs(2)
        .hll(1d, 3d, 3d)
        .kll(1d, 3d, 3d)
        .build());
    merger.merge(aggrObj, newObj);

    ColumnStatisticsData expectedColumnStatisticsData = new ColStatsBuilder<>(double.class)
        .low(1d)
        .high(3d)
        .numNulls(4)
        .numDVs(2)
        .hll(1d, 3d, 3d)
        .kll(1d, 3d, 3d)
        .build();
    assertEquals(expectedColumnStatisticsData, aggrObj.getStatsData());
  }

  @Test
  public void testMergeNonNullWithNullValues() {
    ColumnStatisticsObj aggrObj = createColumnStatisticsObj(new ColStatsBuilder<>(double.class)
        .low(1d)
        .high(3d)
        .numNulls(4)
        .numDVs(2)
        .hll(1d, 3d, 3d)
        .kll(1d, 3d, 3d)
        .build());

    ColumnStatisticsObj newObj = createColumnStatisticsObj(new ColStatsBuilder<>(double.class)
        .low(null)
        .high(null)
        .numNulls(2)
        .numDVs(0)
        .build());
    merger.merge(aggrObj, newObj);

    ColumnStatisticsData expectedColumnStatisticsData = new ColStatsBuilder<>(double.class)
        .low(1d)
        .high(3d)
        .numNulls(6)
        .numDVs(2)
        .hll(1d, 3d, 3d)
        .kll(1d, 3d, 3d)
        .build();
    assertEquals(expectedColumnStatisticsData, aggrObj.getStatsData());
  }

  @Test
  public void testMergeNonNullValues() {
    ColumnStatisticsObj aggrObj = createColumnStatisticsObj(new ColStatsBuilder<>(double.class)
        .low(2d)
        .high(2d)
        .numNulls(2)
        .numDVs(1)
        .hll(2d)
        .kll(2d)
        .build());
    ColumnStatisticsObj newObj = createColumnStatisticsObj(new ColStatsBuilder<>(double.class)
        .low(3d)
        .high(3d)
        .numNulls(3)
        .numDVs(1)
        .hll(3d)
        .kll(3d)
        .build());
    merger.merge(aggrObj, newObj);

    newObj = createColumnStatisticsObj(new ColStatsBuilder<>(double.class)
        .low(1d)
        .high(1d)
        .numNulls(1)
        .numDVs(1)
        .hll(1d, 1d)
        .kll(1d, 1d)
        .build());
    merger.merge(aggrObj, newObj);

    ColumnStatisticsData expectedColumnStatisticsData = new ColStatsBuilder<>(double.class)
        .low(1d)
        .high(3d)
        .numNulls(6)
        .numDVs(3)
        .hll(2d, 3d, 1d, 1d)
        .kll(2d, 3d, 1d, 1d)
        .build();
    assertEquals(expectedColumnStatisticsData, aggrObj.getStatsData());
  }

  @Test
  public void testCompareSimple() {
    DoubleColumnStatsDataInspector data1 = new DoubleColumnStatsDataInspector(DATA_1);
    DoubleColumnStatsDataInspector data2 = new DoubleColumnStatsDataInspector(DATA_2);
    assertEquals(2, merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)), Double.MIN_VALUE);
  }

  @Test
  public void testCompareSimpleFlipped() {
    DoubleColumnStatsDataInspector data1 = new DoubleColumnStatsDataInspector(DATA_2);
    DoubleColumnStatsDataInspector data2 = new DoubleColumnStatsDataInspector(DATA_1);
    assertEquals(2, merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)), Double.MIN_VALUE);
  }

  @Test
  public void testCompareSimpleReversed() {
    DoubleColumnStatsDataInspector data1 = new DoubleColumnStatsDataInspector(DATA_1);
    DoubleColumnStatsDataInspector data2 = new DoubleColumnStatsDataInspector(DATA_2);
    assertEquals(1, merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)), Double.MIN_VALUE);
  }

  @Test
  public void testCompareSimpleFlippedReversed() {
    DoubleColumnStatsDataInspector data1 = new DoubleColumnStatsDataInspector(DATA_2);
    DoubleColumnStatsDataInspector data2 = new DoubleColumnStatsDataInspector(DATA_1);
    assertEquals(1, merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)), Double.MIN_VALUE);
  }

  @Test
  public void testCompareNullsMin() {
    DoubleColumnStatsDataInspector data1 = new DoubleColumnStatsDataInspector();
    DoubleColumnStatsDataInspector data2 = new DoubleColumnStatsDataInspector();
    assertNull(merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)));
  }

  @Test
  public void testCompareNullsMax() {
    DoubleColumnStatsDataInspector data1 = new DoubleColumnStatsDataInspector();
    DoubleColumnStatsDataInspector data2 = new DoubleColumnStatsDataInspector();
    assertNull(merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }

  @Test
  public void testCompareFirstNullMin() {
    DoubleColumnStatsDataInspector data1 = new DoubleColumnStatsDataInspector();
    DoubleColumnStatsDataInspector data2 = new DoubleColumnStatsDataInspector(DATA_1);
    assertEquals(1, merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)), Double.MIN_VALUE);
  }

  @Test
  public void testCompareSecondNullMin() {
    DoubleColumnStatsDataInspector data1 = new DoubleColumnStatsDataInspector(DATA_1);
    DoubleColumnStatsDataInspector data2 = new DoubleColumnStatsDataInspector();
    assertEquals(1, merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)), Double.MIN_VALUE);
  }

  @Test
  public void testCompareFirstNullMax() {
    DoubleColumnStatsDataInspector data1 = new DoubleColumnStatsDataInspector(DATA_1);
    DoubleColumnStatsDataInspector data2 = new DoubleColumnStatsDataInspector();
    assertEquals(1, merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)), Double.MIN_VALUE);
  }

  @Test
  public void testCompareSecondNullMax() {
    DoubleColumnStatsDataInspector data1 = new DoubleColumnStatsDataInspector();
    DoubleColumnStatsDataInspector data2 = new DoubleColumnStatsDataInspector(DATA_1);
    assertEquals(1, merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)), Double.MIN_VALUE);
  }
}
