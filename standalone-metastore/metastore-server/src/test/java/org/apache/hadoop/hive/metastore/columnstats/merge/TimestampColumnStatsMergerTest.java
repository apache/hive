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
import org.apache.hadoop.hive.metastore.api.Timestamp;
import org.apache.hadoop.hive.metastore.columnstats.ColStatsBuilder;
import org.apache.hadoop.hive.metastore.columnstats.cache.TimestampColumnStatsDataInspector;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.hadoop.hive.metastore.columnstats.merge.ColumnStatsMergerTest.createColumnStatisticsObj;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@Category(MetastoreUnitTest.class)
public class TimestampColumnStatsMergerTest {

  private static final Timestamp TS_1 = new Timestamp(1);
  private static final Timestamp TS_2 = new Timestamp(2);
  private static final Timestamp TS_3 = new Timestamp(3);

  private static final TimestampColumnStatsDataInspector DATA_1 = new TimestampColumnStatsDataInspector();
  private static final TimestampColumnStatsDataInspector DATA_2 = new TimestampColumnStatsDataInspector();
  private static final TimestampColumnStatsDataInspector DATA_3 = new TimestampColumnStatsDataInspector();

  static {
    DATA_1.setLowValue(TS_1);
    DATA_1.setHighValue(TS_1);
    DATA_2.setLowValue(TS_2);
    DATA_2.setHighValue(TS_2);
    DATA_3.setLowValue(TS_3);
    DATA_3.setHighValue(TS_3);
  }

  private final TimestampColumnStatsMerger merger = new TimestampColumnStatsMerger();

  @Test
  public void testMergeNullValues() {
    ColumnStatisticsObj aggrObj = createColumnStatisticsObj(new ColStatsBuilder<>(Timestamp.class)
        .low(null)
        .high(null)
        .numNulls(1)
        .numDVs(0)
        .build());
    merger.merge(aggrObj, aggrObj);

    ColumnStatisticsData expectedColumnStatisticsData = new ColStatsBuilder<>(Timestamp.class)
        .low(null)
        .high(null)
        .numNulls(2)
        .numDVs(0)
        .build();

    assertEquals(expectedColumnStatisticsData, aggrObj.getStatsData());
  }

  @Test
  public void testMergeNullWithNonNullValues() {
    ColumnStatisticsObj aggrObj = createColumnStatisticsObj(new ColStatsBuilder<>(Timestamp.class)
        .low(null)
        .high(null)
        .numNulls(0)
        .numDVs(0)
        .build());
    ColumnStatisticsObj newObj = createColumnStatisticsObj(new ColStatsBuilder<>(Timestamp.class)
        .low(TS_1)
        .high(TS_3)
        .numNulls(4)
        .numDVs(2)
        .hll(TS_1.getSecondsSinceEpoch(), TS_3.getSecondsSinceEpoch(), TS_3.getSecondsSinceEpoch())
        .kll(TS_1.getSecondsSinceEpoch(), TS_3.getSecondsSinceEpoch(), TS_3.getSecondsSinceEpoch())
        .build());

    merger.merge(aggrObj, newObj);

    assertEquals(newObj.getStatsData(), aggrObj.getStatsData());
  }

  @Test
  public void testMergeNonNullWithNullValues() {
    ColumnStatisticsObj aggrObj = createColumnStatisticsObj(new ColStatsBuilder<>(Timestamp.class)
        .low(TS_1)
        .high(TS_3)
        .numNulls(4)
        .numDVs(2)
        .hll(TS_1.getSecondsSinceEpoch(), TS_3.getSecondsSinceEpoch(), TS_3.getSecondsSinceEpoch())
        .kll(TS_1.getSecondsSinceEpoch(), TS_3.getSecondsSinceEpoch(), TS_3.getSecondsSinceEpoch())
        .build());

    ColumnStatisticsObj newObj = createColumnStatisticsObj(new ColStatsBuilder<>(Timestamp.class)
        .low(null)
        .high(null)
        .numNulls(2)
        .numDVs(0)
        .build());
    merger.merge(aggrObj, newObj);

    ColumnStatisticsData expectedColumnStatisticsData = new ColStatsBuilder<>(Timestamp.class)
        .low(TS_1)
        .high(TS_3)
        .numNulls(6)
        .numDVs(2)
        .hll(TS_1.getSecondsSinceEpoch(), TS_3.getSecondsSinceEpoch(), TS_3.getSecondsSinceEpoch())
        .kll(TS_1.getSecondsSinceEpoch(), TS_3.getSecondsSinceEpoch(), TS_3.getSecondsSinceEpoch())
        .build();
    assertEquals(expectedColumnStatisticsData, aggrObj.getStatsData());
  }

  @Test
  public void testMergeNonNullValues() {
    ColumnStatisticsObj aggrObj = createColumnStatisticsObj(new ColStatsBuilder<>(Timestamp.class)
        .low(TS_2)
        .high(TS_2)
        .numNulls(2)
        .numDVs(1)
        .hll(TS_2.getSecondsSinceEpoch())
        .kll(TS_2.getSecondsSinceEpoch())
        .build());
    ColumnStatisticsObj newObj = createColumnStatisticsObj(new ColStatsBuilder<>(Timestamp.class)
        .low(TS_3)
        .high(TS_3)
        .numNulls(3)
        .numDVs(1)
        .hll(TS_3.getSecondsSinceEpoch())
        .kll(TS_3.getSecondsSinceEpoch())
        .build());
    merger.merge(aggrObj, newObj);

    newObj = createColumnStatisticsObj(new ColStatsBuilder<>(Timestamp.class)
        .low(TS_1)
        .high(TS_1)
        .numNulls(1)
        .numDVs(1)
        .hll(TS_1.getSecondsSinceEpoch(), TS_1.getSecondsSinceEpoch())
        .kll(TS_1.getSecondsSinceEpoch(), TS_1.getSecondsSinceEpoch())
        .build());
    merger.merge(aggrObj, newObj);

    ColumnStatisticsData expectedColumnStatisticsData = new ColStatsBuilder<>(Timestamp.class)
        .low(TS_1)
        .high(TS_3)
        .numNulls(6)
        .numDVs(3)
        .hll(TS_2.getSecondsSinceEpoch(), TS_3.getSecondsSinceEpoch(),
            TS_1.getSecondsSinceEpoch(), TS_1.getSecondsSinceEpoch())
        .kll(TS_2.getSecondsSinceEpoch(), TS_3.getSecondsSinceEpoch(),
            TS_1.getSecondsSinceEpoch(), TS_1.getSecondsSinceEpoch())
        .build();
    assertEquals(expectedColumnStatisticsData, aggrObj.getStatsData());
  }

  @Test
  public void testCompareSimple() {
    TimestampColumnStatsDataInspector data1 = new TimestampColumnStatsDataInspector(DATA_1);
    TimestampColumnStatsDataInspector data2 = new TimestampColumnStatsDataInspector(DATA_2);
    assertEquals(TS_2, merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }

  @Test
  public void testCompareSimpleFlipped() {
    TimestampColumnStatsDataInspector data1 = new TimestampColumnStatsDataInspector(DATA_2);
    TimestampColumnStatsDataInspector data2 = new TimestampColumnStatsDataInspector(DATA_1);
    assertEquals(TS_2, merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }

  @Test
  public void testCompareSimpleReversed() {
    TimestampColumnStatsDataInspector data1 = new TimestampColumnStatsDataInspector(DATA_1);
    TimestampColumnStatsDataInspector data2 = new TimestampColumnStatsDataInspector(DATA_2);
    assertEquals(TS_1, merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)));
  }

  @Test
  public void testCompareSimpleFlippedReversed() {
    TimestampColumnStatsDataInspector data1 = new TimestampColumnStatsDataInspector(DATA_2);
    TimestampColumnStatsDataInspector data2 = new TimestampColumnStatsDataInspector(DATA_1);
    assertEquals(TS_1, merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)));
  }

  @Test
  public void testCompareNullsMin() {
    TimestampColumnStatsDataInspector data1 = new TimestampColumnStatsDataInspector();
    TimestampColumnStatsDataInspector data2 = new TimestampColumnStatsDataInspector();
    assertNull(merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)));
  }

  @Test
  public void testCompareNullsMax() {
    TimestampColumnStatsDataInspector data1 = new TimestampColumnStatsDataInspector();
    TimestampColumnStatsDataInspector data2 = new TimestampColumnStatsDataInspector();
    assertNull(merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }

  @Test
  public void testCompareFirstNullMin() {
    TimestampColumnStatsDataInspector data1 = new TimestampColumnStatsDataInspector();
    TimestampColumnStatsDataInspector data2 = new TimestampColumnStatsDataInspector(DATA_1);
    assertEquals(TS_1, merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)));
  }

  @Test
  public void testCompareSecondNullMin() {
    TimestampColumnStatsDataInspector data1 = new TimestampColumnStatsDataInspector(DATA_1);
    TimestampColumnStatsDataInspector data2 = new TimestampColumnStatsDataInspector();
    assertEquals(TS_1, merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)));
  }

  @Test
  public void testCompareFirstNullMax() {
    TimestampColumnStatsDataInspector data1 = new TimestampColumnStatsDataInspector(DATA_1);
    TimestampColumnStatsDataInspector data2 = new TimestampColumnStatsDataInspector();
    assertEquals(TS_1, merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }

  @Test
  public void testCompareSecondNullMax() {
    TimestampColumnStatsDataInspector data1 = new TimestampColumnStatsDataInspector();
    TimestampColumnStatsDataInspector data2 = new TimestampColumnStatsDataInspector(DATA_1);
    assertEquals(TS_1, merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }
}
