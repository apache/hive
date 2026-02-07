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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Collections;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TestStatistics {

  @Test
  void testAddToColumnStatsSingleColumn() {
    Statistics stats = new Statistics(100, 1000, 0, 0);
    ColStatistics colStats = createColStats("col1", 50, 10, 8.0);

    stats.addToColumnStats(Collections.singletonList(colStats));

    ColStatistics result = stats.getColumnStatisticsFromColName("col1");
    assertEquals(50, result.getCountDistint());
    assertEquals(10, result.getNumNulls());
    assertEquals(8.0, result.getAvgColLen());
  }

  @Test
  void testAddToColumnStatsMergesTakesMaxAvgColLen() {
    Statistics stats = new Statistics(100, 1000, 0, 0);
    ColStatistics first = createColStats("col1", 50, 10, 8.0);
    ColStatistics second = createColStats("col1", 60, 20, 12.0);

    stats.addToColumnStats(Collections.singletonList(first));
    stats.addToColumnStats(Collections.singletonList(second));

    ColStatistics result = stats.getColumnStatisticsFromColName("col1");
    assertEquals(12.0, result.getAvgColLen());
  }

  @Test
  void testAddToColumnStatsMergesSumsNumNulls() {
    Statistics stats = new Statistics(100, 1000, 0, 0);
    ColStatistics first = createColStats("col1", 50, 10, 8.0);
    ColStatistics second = createColStats("col1", 60, 20, 8.0);

    stats.addToColumnStats(Collections.singletonList(first));
    stats.addToColumnStats(Collections.singletonList(second));

    ColStatistics result = stats.getColumnStatisticsFromColName("col1");
    assertEquals(30, result.getNumNulls());
  }

  @ParameterizedTest(name = "ndv1={0}, ndv2={1} -> expected={2}")
  @CsvSource({
      "50, 60, 60",   // both known, takes max
      "50, 0, 0",     // second unknown, result unknown
      "0, 60, 0"      // first unknown, result unknown
  })
  void testAddToColumnStatsMergesNdv(long ndv1, long ndv2, long expectedNdv) {
    Statistics stats = new Statistics(100, 1000, 0, 0);
    ColStatistics first = createColStats("col1", ndv1, 10, 8.0);
    ColStatistics second = createColStats("col1", ndv2, 20, 8.0);

    stats.addToColumnStats(Collections.singletonList(first));
    stats.addToColumnStats(Collections.singletonList(second));

    ColStatistics result = stats.getColumnStatisticsFromColName("col1");
    assertEquals(expectedNdv, result.getCountDistint());
  }

  @Test
  void testAddToColumnStatsNullListIsNoOp() {
    Statistics stats = new Statistics(100, 1000, 0, 0);
    ColStatistics colStats = createColStats("col1", 50, 10, 8.0);
    stats.addToColumnStats(Collections.singletonList(colStats));

    stats.addToColumnStats(null);

    assertEquals(1, stats.getColumnStats().size());
  }

  @Test
  void testAddToColumnStatsNullElementIsSkipped() {
    Statistics stats = new Statistics(100, 1000, 0, 0);
    ColStatistics colStats = createColStats("col1", 50, 10, 8.0);

    stats.addToColumnStats(Arrays.asList(null, colStats, null));

    assertEquals(1, stats.getColumnStats().size());
    assertEquals(50, stats.getColumnStatisticsFromColName("col1").getCountDistint());
  }

  private ColStatistics createColStats(String name, long ndv, long numNulls, double avgColLen) {
    ColStatistics cs = new ColStatistics(name, "string");
    cs.setCountDistint(ndv);
    cs.setNumNulls(numNulls);
    cs.setAvgColLen(avgColLen);
    return cs;
  }
}
