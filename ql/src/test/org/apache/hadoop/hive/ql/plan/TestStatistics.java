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
package org.apache.hadoop.hive.ql.plan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.Arrays;

import org.junit.Test;

public class TestStatistics {

  @Test
  public void testScaleToRowCountPreventsNegativeNonNullCount() {
    Statistics stats = new Statistics(10, 1000, 0, 0);
    ColStatistics colStats = new ColStatistics("str_col", "string");
    colStats.setNumNulls(9);
    colStats.setCountDistint(2);
    colStats.setAvgColLen(10.0);
    stats.setColumnStats(Arrays.asList(colStats));

    Statistics scaled = stats.scaleToRowCount(1, false);

    assertEquals(1, scaled.getNumRows());
    ColStatistics scaledCol = scaled.getColumnStatisticsFromColName("str_col");
    assertEquals(-1, scaledCol.getNumNulls());
  }

  @Test
  public void testScaleToRowCountCapsCountDistinct() {
    Statistics stats = new Statistics(100, 1000, 0, 0);
    ColStatistics colStats = new ColStatistics("col1", "int");
    colStats.setCountDistint(100);
    colStats.setNumNulls(0);
    stats.setColumnStats(Arrays.asList(colStats));

    Statistics scaled = stats.scaleToRowCount(10, false);

    ColStatistics scaledCol = scaled.getColumnStatisticsFromColName("col1");
    assertEquals(10, scaledCol.getCountDistint());
  }

  @Test
  public void testScaleToRowCountSetsNumNullsToUnknown() {
    Statistics stats = new Statistics(100, 1000, 0, 0);
    ColStatistics colStats = new ColStatistics("col1", "string");
    colStats.setNumNulls(50);
    stats.setColumnStats(Arrays.asList(colStats));

    Statistics scaled = stats.scaleToRowCount(10, false);

    ColStatistics scaledCol = scaled.getColumnStatisticsFromColName("col1");
    assertEquals(-1, scaledCol.getNumNulls());
  }

  @Test
  public void testScaleToRowCountSetsBooleanStatsToUnknown() {
    Statistics stats = new Statistics(100, 1000, 0, 0);
    ColStatistics colStats = new ColStatistics("bool_col", "boolean");
    colStats.setNumTrues(30);
    colStats.setNumFalses(70);
    colStats.setNumNulls(0);
    stats.setColumnStats(Arrays.asList(colStats));

    Statistics scaled = stats.scaleToRowCount(10, false);

    ColStatistics scaledCol = scaled.getColumnStatisticsFromColName("bool_col");
    assertEquals(-1, scaledCol.getNumTrues());
    assertEquals(-1, scaledCol.getNumFalses());
  }

  @Test
  public void testScaleToRowCountPreservesZeroBooleanStats() {
    Statistics stats = new Statistics(100, 1000, 0, 0);
    ColStatistics colStats = new ColStatistics("bool_col", "boolean");
    colStats.setNumTrues(0);
    colStats.setNumFalses(100);
    colStats.setNumNulls(0);
    stats.setColumnStats(Arrays.asList(colStats));

    Statistics scaled = stats.scaleToRowCount(10, false);

    ColStatistics scaledCol = scaled.getColumnStatisticsFromColName("bool_col");
    assertEquals(0, scaledCol.getNumTrues());
    assertEquals(-1, scaledCol.getNumFalses());
  }

  @Test
  public void testScaleToRowCountClearsDistributionData() {
    Statistics stats = new Statistics(100, 1000, 0, 0);
    ColStatistics colStats = new ColStatistics("col1", "int");
    colStats.setNumNulls(0);
    colStats.setBitVectors(new byte[]{1, 2, 3});
    colStats.setHistogram(new byte[]{4, 5, 6});
    stats.setColumnStats(Arrays.asList(colStats));

    Statistics scaled = stats.scaleToRowCount(10, false);

    ColStatistics scaledCol = scaled.getColumnStatisticsFromColName("col1");
    assertNull(scaledCol.getBitVectors());
    assertNull(scaledCol.getHistogram());
  }

  @Test
  public void testScaleToRowCountMultipleColumns() {
    Statistics stats = new Statistics(100, 1000, 0, 0);

    ColStatistics col1 = new ColStatistics("int_col", "int");
    col1.setNumNulls(20);
    col1.setCountDistint(80);

    ColStatistics col2 = new ColStatistics("str_col", "string");
    col2.setNumNulls(0);
    col2.setCountDistint(50);

    ColStatistics col3 = new ColStatistics("bool_col", "boolean");
    col3.setNumNulls(10);
    col3.setNumTrues(40);
    col3.setNumFalses(50);

    stats.setColumnStats(Arrays.asList(col1, col2, col3));

    Statistics scaled = stats.scaleToRowCount(5, false);

    assertEquals(5, scaled.getNumRows());

    ColStatistics scaledCol1 = scaled.getColumnStatisticsFromColName("int_col");
    assertEquals(-1, scaledCol1.getNumNulls());
    assertEquals(5, scaledCol1.getCountDistint());

    ColStatistics scaledCol2 = scaled.getColumnStatisticsFromColName("str_col");
    assertEquals(0, scaledCol2.getNumNulls());
    assertEquals(5, scaledCol2.getCountDistint());

    ColStatistics scaledCol3 = scaled.getColumnStatisticsFromColName("bool_col");
    assertEquals(-1, scaledCol3.getNumNulls());
    assertEquals(-1, scaledCol3.getNumTrues());
    assertEquals(-1, scaledCol3.getNumFalses());
  }
}
