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
import org.apache.hadoop.hive.metastore.columnstats.cache.LongColumnStatsDataInspector;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.hadoop.hive.metastore.columnstats.merge.ColumnStatsMergerTest.createColumnStatisticsObj;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Category(MetastoreUnitTest.class)
public class LongColumnStatsMergerTest {
  private final LongColumnStatsMerger merger = new LongColumnStatsMerger();
  
  private static final LongColumnStatsDataInspector DATA_1 = new LongColumnStatsDataInspector();
  private static final LongColumnStatsDataInspector DATA_2 = new LongColumnStatsDataInspector();
  private static final LongColumnStatsDataInspector DATA_3 = new LongColumnStatsDataInspector();

  static {
    DATA_1.setLowValue(1);
    DATA_1.setHighValue(1);
    DATA_2.setLowValue(2);
    DATA_2.setHighValue(2);
    DATA_3.setLowValue(3);
    DATA_3.setHighValue(3);
  }

  @Test
  public void testMergeNullValues() {
    ColumnStatisticsObj aggrObj = createColumnStatisticsObj(new ColStatsBuilder<>(long.class)
        .low(null)
        .high(null)
        .numNulls(1)
        .numDVs(0)
        .build());
    merger.merge(aggrObj, aggrObj);

    ColumnStatisticsData expectedColumnStatisticsData = new ColStatsBuilder<>(long.class)
        .low(null)
        .high(null)
        .numNulls(2)
        .numDVs(0)
        .build();

    assertEquals(expectedColumnStatisticsData, aggrObj.getStatsData());
  }

  @Test
  public void testMergeNullWithNonNullValues() {
    ColumnStatisticsObj aggrObj = createColumnStatisticsObj(new ColStatsBuilder<>(long.class)
        .low(null)
        .high(null)
        .numNulls(0)
        .numDVs(0)
        .build());
    ColumnStatisticsObj newObj = createColumnStatisticsObj(new ColStatsBuilder<>(long.class)
        .low(1L)
        .high(3L)
        .numNulls(4)
        .numDVs(2)
        .hll(1, 3, 3)
        .kll(1, 3, 3)
        .build());
    merger.merge(aggrObj, newObj);

    ColumnStatisticsData expectedColumnStatisticsData = new ColStatsBuilder<>(long.class)
        .low(1L)
        .high(3L)
        .numNulls(4)
        .numDVs(2)
        .hll(1, 3, 3)
        .kll(1, 3, 3)
        .build();
    assertEquals(expectedColumnStatisticsData, aggrObj.getStatsData());
  }

  @Test
  public void testMergeNonNullWithNullValues() {
    ColumnStatisticsObj aggrObj = createColumnStatisticsObj(new ColStatsBuilder<>(long.class)
        .low(1L)
        .high(3L)
        .numNulls(4)
        .numDVs(2)
        .hll(1, 3, 3)
        .kll(1, 3, 3)
        .build());

    ColumnStatisticsObj newObj = createColumnStatisticsObj(new ColStatsBuilder<>(long.class)
        .low(null)
        .high(null)
        .numNulls(2)
        .numDVs(0)
        .build());
    merger.merge(aggrObj, newObj);

    ColumnStatisticsData expectedColumnStatisticsData = new ColStatsBuilder<>(long.class)
        .low(1L)
        .high(3L)
        .numNulls(6)
        .numDVs(2)
        .hll(1, 3, 3)
        .kll(1, 3, 3)
        .build();
    assertEquals(expectedColumnStatisticsData, aggrObj.getStatsData());
  }

  @Test
  public void testMergeNonNullValues() {
    ColumnStatisticsObj aggrObj = createColumnStatisticsObj(new ColStatsBuilder<>(long.class)
        .low(2L)
        .high(2L)
        .numNulls(2)
        .numDVs(1)
        .hll(2L)
        .kll(2L)
        .build());
    ColumnStatisticsObj newObj = createColumnStatisticsObj(new ColStatsBuilder<>(long.class)
        .low(3L)
        .high(3L)
        .numNulls(3)
        .numDVs(1)
        .hll(3L)
        .kll(3L)
        .build());
    merger.merge(aggrObj, newObj);

    newObj = createColumnStatisticsObj(new ColStatsBuilder<>(long.class)
        .low(1L)
        .high(1L)
        .numNulls(1)
        .numDVs(1)
        .hll(1L, 1L)
        .kll(1L, 1L)
        .build());
    merger.merge(aggrObj, newObj);

    ColumnStatisticsData expectedColumnStatisticsData = new ColStatsBuilder<>(long.class)
        .low(1L)
        .high(3L)
        .numNulls(6)
        .numDVs(3)
        .hll(2L, 3L, 1L, 1L)
        .kll(2L, 3L, 1L, 1L)
        .build();
    assertEquals(expectedColumnStatisticsData, aggrObj.getStatsData());
  }

  @Test
  public void testCompareSimple() {
    LongColumnStatsDataInspector data1 = new LongColumnStatsDataInspector(DATA_1);
    LongColumnStatsDataInspector data2 = new LongColumnStatsDataInspector(DATA_2);
    assertEquals(2, (long)  merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }

  @Test
  public void testCompareSimpleFlipped() {
    LongColumnStatsDataInspector data1 = new LongColumnStatsDataInspector(DATA_2);
    LongColumnStatsDataInspector data2 = new LongColumnStatsDataInspector(DATA_1);
    assertEquals(2, (long) merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }

  @Test
  public void testCompareSimpleReversed() {
    LongColumnStatsDataInspector data1 = new LongColumnStatsDataInspector(DATA_1);
    LongColumnStatsDataInspector data2 = new LongColumnStatsDataInspector(DATA_2);
    assertEquals(1, (long) merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)));
  }

  @Test
  public void testCompareSimpleFlippedReversed() {
    LongColumnStatsDataInspector data1 = new LongColumnStatsDataInspector(DATA_2);
    LongColumnStatsDataInspector data2 = new LongColumnStatsDataInspector(DATA_1);
    assertEquals(1, (long)  merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)));
  }

  @Test
  public void testCompareNullsMin() {
    LongColumnStatsDataInspector data1 = new LongColumnStatsDataInspector();
    LongColumnStatsDataInspector data2 = new LongColumnStatsDataInspector();
    assertNull(merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)));
  }

  @Test
  public void testCompareNullsMax() {
    LongColumnStatsDataInspector data1 = new LongColumnStatsDataInspector();
    LongColumnStatsDataInspector data2 = new LongColumnStatsDataInspector();
    assertNull(merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }

  @Test
  public void testCompareFirstNullMin() {
    LongColumnStatsDataInspector data1 = new LongColumnStatsDataInspector();
    LongColumnStatsDataInspector data2 = new LongColumnStatsDataInspector(DATA_1);
    assertEquals(1, (long)  merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)));
  }

  @Test
  public void testCompareSecondNullMin() {
    LongColumnStatsDataInspector data1 = new LongColumnStatsDataInspector(DATA_1);
    LongColumnStatsDataInspector data2 = new LongColumnStatsDataInspector();
    assertEquals(1, (long)  merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)));
  }

  @Test
  public void testCompareFirstNullMax() {
    LongColumnStatsDataInspector data1 = new LongColumnStatsDataInspector(DATA_1);
    LongColumnStatsDataInspector data2 = new LongColumnStatsDataInspector();
    assertEquals(1, (long)  merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }

  @Test
  public void testCompareSecondNullMax() {
    LongColumnStatsDataInspector data1 = new LongColumnStatsDataInspector();
    LongColumnStatsDataInspector data2 = new LongColumnStatsDataInspector(DATA_1);
    assertEquals(1, (long)  merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }
}
