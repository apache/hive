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
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.columnstats.ColStatsBuilder;
import org.apache.hadoop.hive.metastore.columnstats.cache.DateColumnStatsDataInspector;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.hadoop.hive.metastore.columnstats.merge.ColumnStatsMergerTest.createColumnStatisticsObj;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Category(MetastoreUnitTest.class)
public class DateColumnStatsMergerTest {

  private static final Date DATE_1 = new Date(1);
  private static final Date DATE_2 = new Date(2);
  private static final Date DATE_3 = new Date(3);

  private static final DateColumnStatsDataInspector DATA_1 = new DateColumnStatsDataInspector();
  private static final DateColumnStatsDataInspector DATA_2 = new DateColumnStatsDataInspector();
  private static final DateColumnStatsDataInspector DATA_3 = new DateColumnStatsDataInspector();

  static {
    DATA_1.setLowValue(DATE_1);
    DATA_1.setHighValue(DATE_1);
    DATA_2.setLowValue(DATE_2);
    DATA_2.setHighValue(DATE_2);
    DATA_3.setLowValue(DATE_3);
    DATA_3.setHighValue(DATE_3);
  }

  private final DateColumnStatsMerger merger = new DateColumnStatsMerger();

  @Test
  public void testMergeNullValues() {
    ColumnStatisticsObj aggrObj = createColumnStatisticsObj(new ColStatsBuilder<>(Date.class)
        .low(null)
        .high(null)
        .numNulls(1)
        .numDVs(0)
        .build());
    merger.merge(aggrObj, aggrObj);

    ColumnStatisticsData expectedColumnStatisticsData = new ColStatsBuilder<>(Date.class)
        .low(null)
        .high(null)
        .numNulls(2)
        .numDVs(0)
        .build();

    assertEquals(expectedColumnStatisticsData, aggrObj.getStatsData());
  }

  @Test
  public void testMergeNullWithNonNullValues() {
    ColumnStatisticsObj aggrObj = createColumnStatisticsObj(new ColStatsBuilder<>(Date.class)
        .low(null)
        .high(null)
        .numNulls(0)
        .numDVs(0)
        .build());
    ColumnStatisticsObj newObj = createColumnStatisticsObj(new ColStatsBuilder<>(Date.class)
        .low(DATE_1)
        .high(DATE_3)
        .numNulls(4)
        .numDVs(2)
        .hll(DATE_1.getDaysSinceEpoch(), DATE_3.getDaysSinceEpoch(), DATE_3.getDaysSinceEpoch())
        .kll(DATE_1.getDaysSinceEpoch(), DATE_3.getDaysSinceEpoch(), DATE_3.getDaysSinceEpoch())
        .build());
    merger.merge(aggrObj, newObj);

    ColumnStatisticsData expectedColumnStatisticsData = new ColStatsBuilder<>(Date.class)
        .low(DATE_1)
        .high(DATE_3)
        .numNulls(4)
        .numDVs(2)
        .hll(DATE_1.getDaysSinceEpoch(), DATE_3.getDaysSinceEpoch(), DATE_3.getDaysSinceEpoch())
        .kll(DATE_1.getDaysSinceEpoch(), DATE_3.getDaysSinceEpoch(), DATE_3.getDaysSinceEpoch())
        .build();
    assertEquals(expectedColumnStatisticsData, aggrObj.getStatsData());
  }

  @Test
  public void testMergeNonNullWithNullValues() {
    ColumnStatisticsObj aggrObj = createColumnStatisticsObj(new ColStatsBuilder<>(Date.class)
        .low(DATE_1)
        .high(DATE_3)
        .numNulls(4)
        .numDVs(2)
        .hll(DATE_1.getDaysSinceEpoch(), DATE_3.getDaysSinceEpoch(), DATE_3.getDaysSinceEpoch())
        .kll(DATE_1.getDaysSinceEpoch(), DATE_3.getDaysSinceEpoch(), DATE_3.getDaysSinceEpoch())
        .build());

    ColumnStatisticsObj newObj = createColumnStatisticsObj(new ColStatsBuilder<>(Date.class)
        .low(null)
        .high(null)
        .numNulls(2)
        .numDVs(0)
        .build());
    merger.merge(aggrObj, newObj);

    ColumnStatisticsData expectedColumnStatisticsData = new ColStatsBuilder<>(Date.class)
        .low(DATE_1)
        .high(DATE_3)
        .numNulls(6)
        .numDVs(2)
        .hll(DATE_1.getDaysSinceEpoch(), DATE_3.getDaysSinceEpoch(), DATE_3.getDaysSinceEpoch())
        .kll(DATE_1.getDaysSinceEpoch(), DATE_3.getDaysSinceEpoch(), DATE_3.getDaysSinceEpoch())
        .build();
    assertEquals(expectedColumnStatisticsData, aggrObj.getStatsData());
  }
  
  @Test
  public void testMergeNonNullValues() {
    ColumnStatisticsObj aggrObj = createColumnStatisticsObj(new ColStatsBuilder<>(Date.class)
        .low(DATE_2)
        .high(DATE_2)
        .numNulls(2)
        .numDVs(1)
        .hll(DATE_2.getDaysSinceEpoch())
        .kll(DATE_2.getDaysSinceEpoch())
        .build());
    ColumnStatisticsObj newObj = createColumnStatisticsObj(new ColStatsBuilder<>(Date.class)
        .low(DATE_3)
        .high(DATE_3)
        .numNulls(3)
        .numDVs(1)
        .hll(DATE_3.getDaysSinceEpoch())
        .kll(DATE_3.getDaysSinceEpoch())
        .build());
    merger.merge(aggrObj, newObj);

    newObj = createColumnStatisticsObj(new ColStatsBuilder<>(Date.class)
        .low(DATE_1)
        .high(DATE_1)
        .numNulls(1)
        .numDVs(1)
        .hll(DATE_1.getDaysSinceEpoch(), DATE_1.getDaysSinceEpoch())
        .kll(DATE_1.getDaysSinceEpoch(), DATE_1.getDaysSinceEpoch())
        .build());
    merger.merge(aggrObj, newObj);

    ColumnStatisticsData expectedColumnStatisticsData = new ColStatsBuilder<>(Date.class)
        .low(DATE_1)
        .high(DATE_3)
        .numNulls(6)
        .numDVs(3)
        .hll(DATE_2.getDaysSinceEpoch(), DATE_3.getDaysSinceEpoch(),
            DATE_1.getDaysSinceEpoch(), DATE_1.getDaysSinceEpoch())
        .kll(DATE_2.getDaysSinceEpoch(), DATE_3.getDaysSinceEpoch(),
            DATE_1.getDaysSinceEpoch(), DATE_1.getDaysSinceEpoch())
        .build();
    assertEquals(expectedColumnStatisticsData, aggrObj.getStatsData());
  }

  @Test
  public void testCompareSimple() {
    DateColumnStatsDataInspector data1 = new DateColumnStatsDataInspector(DATA_1);
    DateColumnStatsDataInspector data2 = new DateColumnStatsDataInspector(DATA_2);
    assertEquals(DATE_2, merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }

  @Test
  public void testCompareSimpleFlipped() {
    DateColumnStatsDataInspector data1 = new DateColumnStatsDataInspector(DATA_2);
    DateColumnStatsDataInspector data2 = new DateColumnStatsDataInspector(DATA_1);
    assertEquals(DATE_2, merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }

  @Test
  public void testCompareSimpleReversed() {
    DateColumnStatsDataInspector data1 = new DateColumnStatsDataInspector(DATA_1);
    DateColumnStatsDataInspector data2 = new DateColumnStatsDataInspector(DATA_2);
    assertEquals(DATE_1, merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)));
  }

  @Test
  public void testCompareSimpleFlippedReversed() {
    DateColumnStatsDataInspector data1 = new DateColumnStatsDataInspector(DATA_2);
    DateColumnStatsDataInspector data2 = new DateColumnStatsDataInspector(DATA_1);
    assertEquals(DATE_1, merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)));
  }

  @Test
  public void testCompareNullsMin() {
    DateColumnStatsDataInspector data1 = new DateColumnStatsDataInspector();
    DateColumnStatsDataInspector data2 = new DateColumnStatsDataInspector();
    assertNull(merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)));
  }

  @Test
  public void testCompareNullsMax() {
    DateColumnStatsDataInspector data1 = new DateColumnStatsDataInspector();
    DateColumnStatsDataInspector data2 = new DateColumnStatsDataInspector();
    assertNull(merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }

  @Test
  public void testCompareFirstNullMin() {
    DateColumnStatsDataInspector data1 = new DateColumnStatsDataInspector();
    DateColumnStatsDataInspector data2 = new DateColumnStatsDataInspector(DATA_1);
    assertEquals(DATE_1, merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)));
  }

  @Test
  public void testCompareSecondNullMin() {
    DateColumnStatsDataInspector data1 = new DateColumnStatsDataInspector(DATA_1);
    DateColumnStatsDataInspector data2 = new DateColumnStatsDataInspector();
    assertEquals(DATE_1, merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)));
  }

  @Test
  public void testCompareFirstNullMax() {
    DateColumnStatsDataInspector data1 = new DateColumnStatsDataInspector(DATA_1);
    DateColumnStatsDataInspector data2 = new DateColumnStatsDataInspector();
    assertEquals(DATE_1, merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }

  @Test
  public void testCompareSecondNullMax() {
    DateColumnStatsDataInspector data1 = new DateColumnStatsDataInspector();
    DateColumnStatsDataInspector data2 = new DateColumnStatsDataInspector(DATA_1);
    assertEquals(DATE_1, merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }
}
