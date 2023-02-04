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
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.utils.DecimalUtils;
import org.apache.hadoop.hive.metastore.columnstats.ColStatsBuilder;
import org.apache.hadoop.hive.metastore.columnstats.cache.DecimalColumnStatsDataInspector;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.apache.hadoop.hive.metastore.columnstats.merge.ColumnStatsMergerTest.createColumnStatisticsObj;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class DecimalColumnStatsMergerTest {

  private static final Decimal DECIMAL_1 = DecimalUtils.getDecimal(1, 0);
  private static final Decimal DECIMAL_3 = DecimalUtils.getDecimal(3, 0);
  private static final Decimal DECIMAL_5 = DecimalUtils.getDecimal(5, 0);
  private static final Decimal DECIMAL_20 = DecimalUtils.getDecimal(2, 1);

  private static final DecimalColumnStatsDataInspector DATA_3 = new DecimalColumnStatsDataInspector();
  private static final DecimalColumnStatsDataInspector DATA_5 = new DecimalColumnStatsDataInspector();
  private static final DecimalColumnStatsDataInspector DATA_20 = new DecimalColumnStatsDataInspector();

  static {
    DATA_3.setLowValue(DECIMAL_3);
    DATA_3.setHighValue(DECIMAL_3);
    DATA_5.setLowValue(DECIMAL_5);
    DATA_5.setHighValue(DECIMAL_5);
    DATA_20.setLowValue(DECIMAL_20);
    DATA_20.setHighValue(DECIMAL_20);
  }

  private final DecimalColumnStatsMerger merger = new DecimalColumnStatsMerger();

  @Test
  public void testMergeNullValues() {
    ColumnStatisticsObj aggrObj = createColumnStatisticsObj(new ColStatsBuilder<>(Decimal.class)
        .low(null)
        .high(null)
        .numNulls(1)
        .numDVs(0)
        .build());
    merger.merge(aggrObj, aggrObj);

    ColumnStatisticsData expectedColumnStatisticsData = new ColStatsBuilder<>(Decimal.class)
        .low(null)
        .high(null)
        .numNulls(2)
        .numDVs(0)
        .build();

    assertEquals(expectedColumnStatisticsData, aggrObj.getStatsData());
  }

  @Test
  public void testMergeNullWithNonNullValues() {
    ColumnStatisticsObj aggrObj = createColumnStatisticsObj(new ColStatsBuilder<>(Decimal.class)
        .low(null)
        .high(null)
        .numNulls(0)
        .numDVs(0)
        .build());
    ColumnStatisticsObj newObj = createColumnStatisticsObj(new ColStatsBuilder<>(Decimal.class)
        .low(DECIMAL_1)
        .high(DECIMAL_3)
        .numNulls(4)
        .numDVs(2)
        .hll(1, 3, 3)
        .kll(1, 3, 3)
        .build());
    merger.merge(aggrObj, newObj);

    ColumnStatisticsData expectedColumnStatisticsData = new ColStatsBuilder<>(Decimal.class)
        .low(DECIMAL_1)
        .high(DECIMAL_3)
        .numNulls(4)
        .numDVs(2)
        .hll(1, 3, 3)
        .kll(1, 3, 3)
        .build();
    assertEquals(expectedColumnStatisticsData, aggrObj.getStatsData());
  }

  @Test
  public void testMergeNonNullWithNullValues() {
    ColumnStatisticsObj aggrObj = createColumnStatisticsObj(new ColStatsBuilder<>(Decimal.class)
        .low(DECIMAL_1)
        .high(DECIMAL_3)
        .numNulls(4)
        .numDVs(2)
        .hll(1, 3, 3)
        .kll(1, 3, 3)
        .build());

    ColumnStatisticsObj newObj = createColumnStatisticsObj(new ColStatsBuilder<>(Decimal.class)
        .low(null)
        .high(null)
        .numNulls(2)
        .numDVs(0)
        .build());
    merger.merge(aggrObj, newObj);

    ColumnStatisticsData expectedColumnStatisticsData = new ColStatsBuilder<>(Decimal.class)
        .low(DECIMAL_1)
        .high(DECIMAL_3)
        .numNulls(6)
        .numDVs(2)
        .hll(1, 3, 3)
        .kll(1, 3, 3)
        .build();
    assertEquals(expectedColumnStatisticsData, aggrObj.getStatsData());
  }

  @Test
  public void testMergeNonNullValues() {
    ColumnStatisticsObj aggrObj = createColumnStatisticsObj(new ColStatsBuilder<>(Decimal.class)
        .low(DECIMAL_1)
        .high(DECIMAL_1)
        .numNulls(2)
        .numDVs(1)
        .hll(2)
        .kll(2)
        .build());
    ColumnStatisticsObj newObj = createColumnStatisticsObj(new ColStatsBuilder<>(Decimal.class)
        .low(DECIMAL_3)
        .high(DECIMAL_3)
        .numNulls(3)
        .numDVs(1)
        .hll(3)
        .kll(3)
        .build());
    merger.merge(aggrObj, newObj);

    newObj = createColumnStatisticsObj(new ColStatsBuilder<>(Decimal.class)
        .low(DECIMAL_1)
        .high(DECIMAL_1)
        .numNulls(1)
        .numDVs(1)
        .hll(1, 1)
        .kll(1, 1)
        .build());
    merger.merge(aggrObj, newObj);

    ColumnStatisticsData expectedColumnStatisticsData = new ColStatsBuilder<>(Decimal.class)
        .low(DECIMAL_1)
        .high(DECIMAL_3)
        .numNulls(6)
        .numDVs(3)
        .hll(2, 3, 1, 1)
        .kll(2, 3, 1, 1)
        .build();
    assertEquals(expectedColumnStatisticsData, aggrObj.getStatsData());
  }

  @Test
  public void testDecimalCompareEqual() {
    assertTrue(DECIMAL_3.equals(DECIMAL_3));
  }

  @Test
  public void testDecimalCompareDoesntEqual() {
    assertFalse(DECIMAL_3.equals(DECIMAL_5));
  }

  @Test
  public void testCompareSimple() {
    DecimalColumnStatsDataInspector data1 = new DecimalColumnStatsDataInspector(DATA_3);
    DecimalColumnStatsDataInspector data2 = new DecimalColumnStatsDataInspector(DATA_5);
    assertEquals(DECIMAL_5, merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }

  @Test
  public void testCompareSimpleFlipped() {
    DecimalColumnStatsDataInspector data1 = new DecimalColumnStatsDataInspector(DATA_5);
    DecimalColumnStatsDataInspector data2 = new DecimalColumnStatsDataInspector(DATA_3);
    assertEquals(DECIMAL_5, merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }

  @Test
  public void testCompareSimpleReversed() {
    DecimalColumnStatsDataInspector data1 = new DecimalColumnStatsDataInspector(DATA_3);
    DecimalColumnStatsDataInspector data2 = new DecimalColumnStatsDataInspector(DATA_5);
    assertEquals(DECIMAL_3, merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)));
  }

  @Test
  public void testCompareSimpleFlippedReversed() {
    DecimalColumnStatsDataInspector data1 = new DecimalColumnStatsDataInspector(DATA_5);
    DecimalColumnStatsDataInspector data2 = new DecimalColumnStatsDataInspector(DATA_3);
    assertEquals(DECIMAL_3, merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)));
  }

  @Test
  public void testCompareUnscaledValue() {
    DecimalColumnStatsDataInspector data1 = new DecimalColumnStatsDataInspector(DATA_3);
    DecimalColumnStatsDataInspector data2 = new DecimalColumnStatsDataInspector(DATA_20);
    assertEquals(DECIMAL_20, merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }

  @Test
  public void testCompareNullsMin() {
    DecimalColumnStatsDataInspector data1 = new DecimalColumnStatsDataInspector();
    DecimalColumnStatsDataInspector data2 = new DecimalColumnStatsDataInspector();
    assertNull(merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)));
  }

  @Test
  public void testCompareNullsMax() {
    DecimalColumnStatsDataInspector data1 = new DecimalColumnStatsDataInspector();
    DecimalColumnStatsDataInspector data2 = new DecimalColumnStatsDataInspector();
    assertNull(merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }

  @Test
  public void testCompareFirstNullMin() {
    DecimalColumnStatsDataInspector data1 = new DecimalColumnStatsDataInspector();
    DecimalColumnStatsDataInspector data2 = new DecimalColumnStatsDataInspector(DATA_3);
    assertEquals(DECIMAL_3, merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)));
  }

  @Test
  public void testCompareSecondNullMin() {
    DecimalColumnStatsDataInspector data1 = new DecimalColumnStatsDataInspector(DATA_3);
    DecimalColumnStatsDataInspector data2 = new DecimalColumnStatsDataInspector();
    assertEquals(DECIMAL_3, merger.mergeLowValue(merger.getLowValue(data1), merger.getLowValue(data2)));
  }

  @Test
  public void testCompareFirstNullMax() {
    DecimalColumnStatsDataInspector data1 = new DecimalColumnStatsDataInspector(DATA_3);
    DecimalColumnStatsDataInspector data2 = new DecimalColumnStatsDataInspector();
    assertEquals(DECIMAL_3, merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }

  @Test
  public void testCompareSecondNullMax() {
    DecimalColumnStatsDataInspector data1 = new DecimalColumnStatsDataInspector();
    DecimalColumnStatsDataInspector data2 = new DecimalColumnStatsDataInspector(DATA_3);
    assertEquals(DECIMAL_3, merger.mergeHighValue(merger.getHighValue(data1), merger.getHighValue(data2)));
  }
}
