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
import org.apache.hadoop.hive.metastore.columnstats.DecimalComparator;
import org.apache.hadoop.hive.metastore.columnstats.cache.DecimalColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Objects;

import static org.apache.hadoop.hive.metastore.columnstats.merge.ColumnStatsMergerTest.createColumnStatisticsObj;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@Category(MetastoreUnitTest.class)
public class DecimalColumnStatsMergerTest {

  /**
   * Creates a decimal and checks its string representation.
   */
  private static Decimal getDecimal(String expected, int number, int scale) {
    Decimal d = DecimalUtils.getDecimal(number, scale);
    assertEquals(expected, MetaStoreServerUtils.decimalToString(d));
    return d;
  }

  private static final Decimal DECIMAL_1 = getDecimal("1", 1, 0);
  private static final Decimal DECIMAL_3 = getDecimal("3", 3, 0);
  private static final Decimal DECIMAL_5 = getDecimal("5", 5, 0);
  private static final Decimal DECIMAL_20 = getDecimal("20", 2, -1);

  private static final DecimalColumnStatsDataInspector DATA_3 = new DecimalColumnStatsDataInspector();
  private static final DecimalColumnStatsDataInspector DATA_5 = new DecimalColumnStatsDataInspector();

  static {
    DATA_3.setLowValue(DECIMAL_3);
    DATA_3.setHighValue(DECIMAL_3);
    DATA_5.setLowValue(DECIMAL_5);
    DATA_5.setHighValue(DECIMAL_5);
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
    assertTrue(DECIMAL_3.equals(getDecimal("3", 3, 0)));
    // the equals method does not check for numerical equality,
    // e.g., DECIMAL_3 is not equal to getDecimal("3", 30, 1)
  }

  @Test
  public void testDecimalCompareDoesntEqual() {
    assertFalse(DECIMAL_3.equals(DECIMAL_5));
    assertFalse(DECIMAL_3.equals(getDecimal("30", 3, -1)));
  }

  private void checkMergedValue(Decimal low, Decimal high) {
    Objects.requireNonNull(low);
    Objects.requireNonNull(high);
    assertTrue(new DecimalComparator().compare(low, high) < 0);
    var data1 = new DecimalColumnStatsDataInspector();
    data1.setLowValue(low);
    data1.setHighValue(low);
    var data2 = new DecimalColumnStatsDataInspector();
    data2.setLowValue(high);
    data2.setHighValue(high);

    assertEquals(low, merger.mergeLowValue(data1.getLowValue(), data2.getLowValue()));
    assertEquals(low, merger.mergeLowValue(data2.getLowValue(), data1.getLowValue()));
    assertEquals(high, merger.mergeHighValue(data1.getHighValue(), data2.getHighValue()));
    assertEquals(high, merger.mergeHighValue(data2.getHighValue(), data1.getHighValue()));
  }

  @Test
  public void testCompareSimple() {
    checkMergedValue(DECIMAL_3, DECIMAL_5);
  }

  @Test
  public void testCompareUnscaledValue() {
    checkMergedValue(DECIMAL_3, DECIMAL_20);
  }

  @Test
  public void testCompareScaledValue() {
    checkMergedValue(
        getDecimal("-123.2", -1232, 1),
        getDecimal("-10.2", -102, 1));

    checkMergedValue(
        getDecimal("1.02", 102, 2),
        getDecimal("123.2", 1232, 1)
    );

    checkMergedValue(
        getDecimal("1.02", 102, 2),
        getDecimal("1232000", 1232, -3)
    );
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
