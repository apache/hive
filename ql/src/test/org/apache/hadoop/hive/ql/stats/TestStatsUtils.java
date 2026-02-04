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

package org.apache.hadoop.hive.ql.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ColStatistics.Range;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.google.common.collect.Sets;

class TestStatsUtils {

  @Test
  void testCombinedRange1() {
    Range r1 = new Range(0, 1);
    Range r2 = new Range(1, 11);
    Range r3 = StatsUtils.combineRange(r1, r2);
    assertNotNull(r3);
    rangeContains(r3, 0);
    rangeContains(r3, 1);
    rangeContains(r3, 11);
  }

  @Test
  void testCombinedRange2() {
    checkCombinedRange(false, new Range(-2, -1), new Range(0, 10));
    checkCombinedRange(true, new Range(-2, 1), new Range(0, 10));
    checkCombinedRange(true, new Range(-2, 11), new Range(0, 10));
    checkCombinedRange(true, new Range(1, 2), new Range(0, 10));
    checkCombinedRange(true, new Range(1, 11), new Range(0, 10));
    checkCombinedRange(false, new Range(11, 12), new Range(0, 10));
  }

  private void checkCombinedRange(boolean valid, Range r1, Range r2) {
    Range r3a = StatsUtils.combineRange(r1, r2);
    Range r3b = StatsUtils.combineRange(r2, r1);
    if (valid) {
      assertNotNull(r3a);
      assertNotNull(r3b);
    } else {
      assertNull(r3a);
      assertNull(r3b);
    }
  }

  private boolean rangeContains(Range range, Number f) {
    double m = range.minValue.doubleValue();
    double M = range.maxValue.doubleValue();
    double v = f.doubleValue();
    return m <= v && v <= M;
  }

  @Test
  void testPrimitiveSizeEstimations() throws Exception {
    HiveConf conf = new HiveConf();
    Set<String> exclusions = Sets.newHashSet();
    exclusions.add(serdeConstants.VOID_TYPE_NAME);
    exclusions.add(serdeConstants.LIST_TYPE_NAME);
    exclusions.add(serdeConstants.MAP_TYPE_NAME);
    exclusions.add(serdeConstants.STRUCT_TYPE_NAME);
    exclusions.add(serdeConstants.UNION_TYPE_NAME);
    exclusions.add(serdeConstants.VARIANT_TYPE_NAME);
    Field[] serdeFields = serdeConstants.class.getFields();
    for (Field field : serdeFields) {
      if (!Modifier.isStatic(field.getModifiers())) {
        continue;
      }
      if (!field.getName().endsWith("_TYPE_NAME")) {
        continue;
      }
      String typeName = (String) FieldUtils.readStaticField(field);
      if (exclusions.contains(typeName)) {
        continue;
      }
      int maxVarLen = HiveConf.getIntVar(conf, HiveConf.ConfVars.HIVE_STATS_MAX_VARIABLE_LENGTH);
      long siz = StatsUtils.getSizeOfPrimitiveTypeArraysFromType(typeName, 3, maxVarLen);
      assertNotEquals(0, siz, field.toString());
    }
  }

  @ParameterizedTest(name = "{0} - {1}")
  @MethodSource("integerStatisticsTestData")
  void testGetColStatisticsIntegerTypes(String typeName, String scenarioName,
      Long lowValue, Long highValue, Long expectedMin, Long expectedMax) {
    ColumnStatisticsObj cso = new ColumnStatisticsObj();
    cso.setColName("test_col");
    cso.setColType(typeName);

    LongColumnStatsData longStats = new LongColumnStatsData();
    longStats.setNumDVs(100);
    longStats.setNumNulls(10);
    if (lowValue != null) {
      longStats.setLowValue(lowValue);
    }
    if (highValue != null) {
      longStats.setHighValue(highValue);
    }

    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setLongStats(longStats);
    cso.setStatsData(data);

    ColStatistics cs = StatsUtils.getColStatistics(cso, "test_col");

    assertNotNull(cs, "ColStatistics should not be null");
    assertEquals(100, cs.getCountDistint(), "NumDVs mismatch");
    assertEquals(10, cs.getNumNulls(), "NumNulls mismatch");

    Range range = cs.getRange();
    assertNotNull(range, "Range should be created");

    if (expectedMin == null) {
      assertNull(range.minValue, "minValue should be null when lowValue is not set");
    } else {
      assertEquals(expectedMin.longValue(), range.minValue.longValue(), "minValue mismatch");
    }

    if (expectedMax == null) {
      assertNull(range.maxValue, "maxValue should be null when highValue is not set");
    } else {
      assertEquals(expectedMax.longValue(), range.maxValue.longValue(), "maxValue mismatch");
    }
  }

  @ParameterizedTest(name = "{0} - {1}")
  @MethodSource("floatingPointStatisticsTestData")
  void testGetColStatisticsFloatingPointTypes(String typeName, String scenarioName,
      Double lowValue, Double highValue, Double expectedMin, Double expectedMax) {
    ColumnStatisticsObj cso = new ColumnStatisticsObj();
    cso.setColName("test_col");
    cso.setColType(typeName);

    DoubleColumnStatsData doubleStats = new DoubleColumnStatsData();
    doubleStats.setNumDVs(100);
    doubleStats.setNumNulls(10);
    if (lowValue != null) {
      doubleStats.setLowValue(lowValue);
    }
    if (highValue != null) {
      doubleStats.setHighValue(highValue);
    }

    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setDoubleStats(doubleStats);
    cso.setStatsData(data);

    ColStatistics cs = StatsUtils.getColStatistics(cso, "test_col");

    assertNotNull(cs, "ColStatistics should not be null");
    assertEquals(100, cs.getCountDistint(), "NumDVs mismatch");
    assertEquals(10, cs.getNumNulls(), "NumNulls mismatch");

    Range range = cs.getRange();
    assertNotNull(range, "Range should be created");

    if (expectedMin == null) {
      assertNull(range.minValue, "minValue should be null when lowValue is not set");
    } else {
      assertEquals(expectedMin, range.minValue.doubleValue(), 0.0001, "minValue mismatch");
    }

    if (expectedMax == null) {
      assertNull(range.maxValue, "maxValue should be null when highValue is not set");
    } else {
      assertEquals(expectedMax, range.maxValue.doubleValue(), 0.0001, "maxValue mismatch");
    }
  }

  static Stream<Arguments> integerStatisticsTestData() {
    return Stream.of(
      // Test arguments: typeName, scenarioName, lowValue, highValue, expectedMin, expectedMax
      Arguments.of(serdeConstants.TINYINT_TYPE_NAME, "BothValuesSet", 1L, 1000L, 1L, 1000L),
      Arguments.of(serdeConstants.TINYINT_TYPE_NAME, "NoValuesSet", null, null, null, null),
      Arguments.of(serdeConstants.TINYINT_TYPE_NAME, "OnlyLowValueSet", 100L, null, 100L, null),
      Arguments.of(serdeConstants.TINYINT_TYPE_NAME, "OnlyHighValueSet", null, 1000L, null, 1000L),
      Arguments.of(serdeConstants.TINYINT_TYPE_NAME, "NegativeHighValueOnly", null, -5L, null, -5L),
      Arguments.of(serdeConstants.SMALLINT_TYPE_NAME, "BothValuesSet", 1L, 1000L, 1L, 1000L),
      Arguments.of(serdeConstants.SMALLINT_TYPE_NAME, "NoValuesSet", null, null, null, null),
      Arguments.of(serdeConstants.SMALLINT_TYPE_NAME, "OnlyLowValueSet", 100L, null, 100L, null),
      Arguments.of(serdeConstants.SMALLINT_TYPE_NAME, "OnlyHighValueSet", null, 1000L, null, 1000L),
      Arguments.of(serdeConstants.SMALLINT_TYPE_NAME, "NegativeHighValueOnly", null, -5L, null, -5L),
      Arguments.of(serdeConstants.INT_TYPE_NAME, "BothValuesSet", 1L, 1000L, 1L, 1000L),
      Arguments.of(serdeConstants.INT_TYPE_NAME, "NoValuesSet", null, null, null, null),
      Arguments.of(serdeConstants.INT_TYPE_NAME, "OnlyLowValueSet", 100L, null, 100L, null),
      Arguments.of(serdeConstants.INT_TYPE_NAME, "OnlyHighValueSet", null, 1000L, null, 1000L),
      Arguments.of(serdeConstants.INT_TYPE_NAME, "NegativeHighValueOnly", null, -5L, null, -5L),
      Arguments.of(serdeConstants.BIGINT_TYPE_NAME, "BothValuesSet", 1L, 1000L, 1L, 1000L),
      Arguments.of(serdeConstants.BIGINT_TYPE_NAME, "NoValuesSet", null, null, null, null),
      Arguments.of(serdeConstants.BIGINT_TYPE_NAME, "OnlyLowValueSet", 100L, null, 100L, null),
      Arguments.of(serdeConstants.BIGINT_TYPE_NAME, "OnlyHighValueSet", null, 1000L, null, 1000L),
      Arguments.of(serdeConstants.BIGINT_TYPE_NAME, "NegativeHighValueOnly", null, -5L, null, -5L)
    );
  }

  static Stream<Arguments> floatingPointStatisticsTestData() {
    return Stream.of(
      // Test arguments: typeName, scenarioName, lowValue, highValue, expectedMin, expectedMax
      Arguments.of(serdeConstants.FLOAT_TYPE_NAME, "BothValuesSet", 1.5, 1000.5, 1.5, 1000.5),
      Arguments.of(serdeConstants.FLOAT_TYPE_NAME, "NoValuesSet", null, null, null, null),
      Arguments.of(serdeConstants.FLOAT_TYPE_NAME, "OnlyLowValueSet", 100.5, null, 100.5, null),
      Arguments.of(serdeConstants.FLOAT_TYPE_NAME, "OnlyHighValueSet", null, 1000.5, null, 1000.5),
      Arguments.of(serdeConstants.FLOAT_TYPE_NAME, "NegativeHighValueOnly", null, -5.5, null, -5.5),
      Arguments.of(serdeConstants.DOUBLE_TYPE_NAME, "BothValuesSet", 1.5, 1000.5, 1.5, 1000.5),
      Arguments.of(serdeConstants.DOUBLE_TYPE_NAME, "NoValuesSet", null, null, null, null),
      Arguments.of(serdeConstants.DOUBLE_TYPE_NAME, "OnlyLowValueSet", 100.5, null, 100.5, null),
      Arguments.of(serdeConstants.DOUBLE_TYPE_NAME, "OnlyHighValueSet", null, 1000.5, null, 1000.5),
      Arguments.of(serdeConstants.DOUBLE_TYPE_NAME, "NegativeHighValueOnly", null, -5.5, null, -5.5)
    );
  }

  private ColStatistics createColStats(String name, long ndv, long numNulls) {
    ColStatistics cs = new ColStatistics(name, "string");
    cs.setCountDistint(ndv);
    cs.setNumNulls(numNulls);
    return cs;
  }

  @Test
  void testStatisticsAddToColumnStatsPropagatesUnknownNumNulls() {
    Statistics stats = new Statistics(1000, 8000, 0, 0);
    ColStatistics cs1 = createColStats("col1", 100, 50);
    stats.setColumnStats(Collections.singletonList(cs1));

    ColStatistics cs2 = createColStats("col1", 150, -1); // unknown numNulls
    stats.addToColumnStats(Collections.singletonList(cs2));

    ColStatistics merged = stats.getColumnStatisticsFromColName("col1");
    assertEquals(-1, merged.getNumNulls(), "Unknown numNulls (-1) should be propagated when merging");
  }

  @Test
  void testStatisticsAddToColumnStatsPropagatesUnknownFromExisting() {
    Statistics stats = new Statistics(1000, 8000, 0, 0);
    ColStatistics cs1 = createColStats("col1", 100, -1); // unknown numNulls
    stats.setColumnStats(Collections.singletonList(cs1));

    ColStatistics cs2 = createColStats("col1", 150, 50);
    stats.addToColumnStats(Collections.singletonList(cs2));

    ColStatistics merged = stats.getColumnStatisticsFromColName("col1");
    assertEquals(-1, merged.getNumNulls(), "Unknown numNulls (-1) should be propagated when existing is unknown");
  }

  @Test
  void testGetColStatisticsBooleanWithUnknownNumTrues() {
    ColumnStatisticsObj cso = new ColumnStatisticsObj();
    cso.setColName("bool_col");
    cso.setColType(serdeConstants.BOOLEAN_TYPE_NAME);

    BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
    boolStats.setNumTrues(-1);  // unknown
    boolStats.setNumFalses(100);
    boolStats.setNumNulls(10);

    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setBooleanStats(boolStats);
    cso.setStatsData(data);

    ColStatistics cs = StatsUtils.getColStatistics(cso, "bool_col");

    assertNotNull(cs);
    assertEquals(2, cs.getCountDistint(), "Boolean NDV should be 2 when numTrues is unknown (-1)");
  }

  @Test
  void testGetColStatisticsBooleanWithUnknownNumFalses() {
    ColumnStatisticsObj cso = new ColumnStatisticsObj();
    cso.setColName("bool_col");
    cso.setColType(serdeConstants.BOOLEAN_TYPE_NAME);

    BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
    boolStats.setNumTrues(100);
    boolStats.setNumFalses(-1);  // unknown
    boolStats.setNumNulls(10);

    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setBooleanStats(boolStats);
    cso.setStatsData(data);

    ColStatistics cs = StatsUtils.getColStatistics(cso, "bool_col");

    assertNotNull(cs);
    assertEquals(2, cs.getCountDistint(), "Boolean NDV should be 2 when numFalses is unknown (-1)");
  }

  @Test
  void testGetColStatisticsBooleanWithBothUnknown() {
    ColumnStatisticsObj cso = new ColumnStatisticsObj();
    cso.setColName("bool_col");
    cso.setColType(serdeConstants.BOOLEAN_TYPE_NAME);

    BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
    boolStats.setNumTrues(-1);  // unknown
    boolStats.setNumFalses(-1);  // unknown
    boolStats.setNumNulls(10);

    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setBooleanStats(boolStats);
    cso.setStatsData(data);

    ColStatistics cs = StatsUtils.getColStatistics(cso, "bool_col");

    assertNotNull(cs);
    assertEquals(2, cs.getCountDistint(), "Boolean NDV should be 2 when both numTrues and numFalses are unknown");
  }

  @Test
  void testUpdateStatsPreservesUnknownNumNulls() {
    Statistics stats = new Statistics(1000, 8000, 0, 0);
    ColStatistics cs = createColStats("col1", 100, -1); // unknown numNulls
    stats.setColumnStats(Collections.singletonList(cs));

    StatsUtils.updateStats(stats, 500, true, null); // scale down to 500 rows

    ColStatistics updated = stats.getColumnStats().get(0);
    assertEquals(-1, updated.getNumNulls(), "Unknown numNulls (-1) should be preserved after scaling");
  }

  @Test
  void testScaleColStatisticsPreservesUnknownNumNulls() {
    ColStatistics cs = createColStats("col1", 100, -1); // unknown numNulls
    List<ColStatistics> colStats = Collections.singletonList(cs);

    StatsUtils.scaleColStatistics(colStats, 2.0);

    assertEquals(-1, colStats.get(0).getNumNulls(), "Unknown numNulls (-1) should be preserved after scaling");
  }

  @Test
  void testScaleColStatisticsPreservesUnknownNumTrues() {
    ColStatistics cs = new ColStatistics("bool_col", "boolean");
    cs.setNumTrues(-1);  // unknown
    cs.setNumFalses(100);
    cs.setNumNulls(10);
    List<ColStatistics> colStats = Collections.singletonList(cs);

    StatsUtils.scaleColStatistics(colStats, 2.0);

    assertEquals(-1, colStats.get(0).getNumTrues(), "Unknown numTrues (-1) should be preserved after scaling");
    assertEquals(200, colStats.get(0).getNumFalses(), "Known numFalses should be scaled");
  }

  @Test
  void testScaleColStatisticsPreservesUnknownNumFalses() {
    ColStatistics cs = new ColStatistics("bool_col", "boolean");
    cs.setNumTrues(100);
    cs.setNumFalses(-1);  // unknown
    cs.setNumNulls(10);
    List<ColStatistics> colStats = Collections.singletonList(cs);

    StatsUtils.scaleColStatistics(colStats, 2.0);

    assertEquals(200, colStats.get(0).getNumTrues(), "Known numTrues should be scaled");
    assertEquals(-1, colStats.get(0).getNumFalses(), "Unknown numFalses (-1) should be preserved after scaling");
  }

}
