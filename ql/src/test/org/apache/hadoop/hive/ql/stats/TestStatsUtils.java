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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
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
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIf;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
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
  void testGetColStatisticsBooleanAllNull() {
    ColumnStatisticsObj cso = new ColumnStatisticsObj();
    cso.setColName("bool_col");
    cso.setColType(serdeConstants.BOOLEAN_TYPE_NAME);

    BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
    boolStats.setNumTrues(0);
    boolStats.setNumFalses(0);
    boolStats.setNumNulls(100);  // all NULL

    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setBooleanStats(boolStats);
    cso.setStatsData(data);

    ColStatistics cs = StatsUtils.getColStatistics(cso, "bool_col");

    assertNotNull(cs);
    assertEquals(0, cs.getCountDistint(), "Boolean NDV should be 0 for all-NULL column");
  }

  @Test
  void testGetColStatisticsBooleanOnlyTrueValues() {
    ColumnStatisticsObj cso = new ColumnStatisticsObj();
    cso.setColName("bool_col");
    cso.setColType(serdeConstants.BOOLEAN_TYPE_NAME);

    BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
    boolStats.setNumTrues(100);
    boolStats.setNumFalses(0);  // no FALSE values
    boolStats.setNumNulls(10);

    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setBooleanStats(boolStats);
    cso.setStatsData(data);

    ColStatistics cs = StatsUtils.getColStatistics(cso, "bool_col");

    assertNotNull(cs);
    assertEquals(1, cs.getCountDistint(), "Boolean NDV should be 1 when only TRUE values present");
  }

  @Test
  void testGetColStatisticsBooleanOnlyFalseValues() {
    ColumnStatisticsObj cso = new ColumnStatisticsObj();
    cso.setColName("bool_col");
    cso.setColType(serdeConstants.BOOLEAN_TYPE_NAME);

    BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
    boolStats.setNumTrues(0);  // no TRUE values
    boolStats.setNumFalses(100);
    boolStats.setNumNulls(10);

    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setBooleanStats(boolStats);
    cso.setStatsData(data);

    ColStatistics cs = StatsUtils.getColStatistics(cso, "bool_col");

    assertNotNull(cs);
    assertEquals(1, cs.getCountDistint(), "Boolean NDV should be 1 when only FALSE values present");
  }

  @Test
  void testGetColStatisticsBooleanNoTrueUnknownFalse() {
    ColumnStatisticsObj cso = new ColumnStatisticsObj();
    cso.setColName("bool_col");
    cso.setColType(serdeConstants.BOOLEAN_TYPE_NAME);

    BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
    boolStats.setNumTrues(0);   // confirmed no TRUE values
    boolStats.setNumFalses(-1); // unknown FALSE count
    boolStats.setNumNulls(10);

    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setBooleanStats(boolStats);
    cso.setStatsData(data);

    ColStatistics cs = StatsUtils.getColStatistics(cso, "bool_col");

    assertNotNull(cs);
    assertEquals(1, cs.getCountDistint(), "Boolean NDV should be 1 when TRUE is 0 and FALSE is unknown");
  }

  @Test
  void testGetColStatisticsBooleanUnknownTrueNoFalse() {
    ColumnStatisticsObj cso = new ColumnStatisticsObj();
    cso.setColName("bool_col");
    cso.setColType(serdeConstants.BOOLEAN_TYPE_NAME);

    BooleanColumnStatsData boolStats = new BooleanColumnStatsData();
    boolStats.setNumTrues(-1);  // unknown TRUE count
    boolStats.setNumFalses(0);  // confirmed no FALSE values
    boolStats.setNumNulls(10);

    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setBooleanStats(boolStats);
    cso.setStatsData(data);

    ColStatistics cs = StatsUtils.getColStatistics(cso, "bool_col");

    assertNotNull(cs);
    assertEquals(1, cs.getCountDistint(), "Boolean NDV should be 1 when TRUE is unknown and FALSE is 0");
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

  // Tests for buildColStatForConstant (via getColStatisticsFromExpression)

  @Test
  void testGetColStatisticsFromExpressionNullConstant() {
    HiveConf conf = new HiveConf();
    Statistics parentStats = new Statistics(1000, 8000, 0, 0);

    ExprNodeConstantDesc nullConst = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, null);
    ColStatistics cs = StatsUtils.getColStatisticsFromExpression(conf, parentStats, nullConst);

    assertNotNull(cs);
    assertEquals(1, cs.getCountDistint(), "NULL constant should have NDV=1");
    assertEquals(1000, cs.getNumNulls(), "NULL constant should have numNulls=numRows");
    assertFalse(cs.isEstimated(), "Constant stats should not be marked as estimated");
  }

  @Test
  void testGetColStatisticsFromExpressionNonNullConstant() {
    HiveConf conf = new HiveConf();
    Statistics parentStats = new Statistics(1000, 8000, 0, 0);

    ExprNodeConstantDesc strConst = new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, "hello");
    ColStatistics cs = StatsUtils.getColStatisticsFromExpression(conf, parentStats, strConst);

    assertNotNull(cs);
    assertEquals(1, cs.getCountDistint(), "Non-NULL constant should have NDV=1");
    assertEquals(0, cs.getNumNulls(), "Non-NULL constant should have numNulls=0");
  }

  @Test
  void testGetColStatisticsFromExpressionIntConstant() {
    HiveConf conf = new HiveConf();
    Statistics parentStats = new Statistics(500, 4000, 0, 0);

    ExprNodeConstantDesc intConst = new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, 42);
    ColStatistics cs = StatsUtils.getColStatisticsFromExpression(conf, parentStats, intConst);

    assertNotNull(cs);
    assertEquals(1, cs.getCountDistint(), "Integer constant should have NDV=1");
    assertEquals(0, cs.getNumNulls(), "Integer constant should have numNulls=0");
    assertNotNull(cs.getRange(), "Integer constant should have a range");
    assertEquals(42, cs.getRange().minValue.intValue());
    assertEquals(42, cs.getRange().maxValue.intValue());
  }

  // Tests for computeNDVGroupingColumns / extractNDVGroupingColumns

  @Test
  void testComputeNDVGroupingColumnsSourceColumnWithNulls() {
    Statistics parentStats = new Statistics(1000, 8000, 0, 0);
    parentStats.setColumnStatsState(Statistics.State.COMPLETE);

    ColStatistics cs = new ColStatistics("col1", "string");
    cs.setCountDistint(100);
    cs.setNumNulls(50);
    cs.setIsEstimated(false);  // source column

    long ndv = StatsUtils.computeNDVGroupingColumns(Arrays.asList(cs), parentStats, false);
    assertEquals(101, ndv, "Source column with nulls should get +1 for NULL: 100 + 1 = 101");
  }

  @Test
  void testComputeNDVGroupingColumnsSourceColumnNoNulls() {
    Statistics parentStats = new Statistics(1000, 8000, 0, 0);
    parentStats.setColumnStatsState(Statistics.State.COMPLETE);

    ColStatistics cs = new ColStatistics("col1", "string");
    cs.setCountDistint(100);
    cs.setNumNulls(0);
    cs.setIsEstimated(false);

    long ndv = StatsUtils.computeNDVGroupingColumns(Arrays.asList(cs), parentStats, false);
    assertEquals(100, ndv, "Source column without nulls should not get +1");
  }

  @Test
  void testComputeNDVGroupingColumnsEstimatedExpression() {
    Statistics parentStats = new Statistics(1000, 8000, 0, 0);
    parentStats.setColumnStatsState(Statistics.State.COMPLETE);

    ColStatistics cs = new ColStatistics("case_expr", "string");
    cs.setCountDistint(3);
    cs.setNumNulls(500);
    cs.setIsEstimated(true);  // computed expression (e.g., CASE)

    long ndv = StatsUtils.computeNDVGroupingColumns(Arrays.asList(cs), parentStats, false);
    assertEquals(3, ndv, "Estimated expression should NOT get +1 (already accounts for NULL)");
  }

  @Test
  void testComputeNDVGroupingColumnsAllNullColumn() {
    Statistics parentStats = new Statistics(1000, 8000, 0, 0);
    parentStats.setColumnStatsState(Statistics.State.COMPLETE);

    ColStatistics cs = new ColStatistics("col1", "string");
    cs.setCountDistint(1);
    cs.setNumNulls(1000);  // all rows are NULL
    cs.setIsEstimated(false);

    long ndv = StatsUtils.computeNDVGroupingColumns(Arrays.asList(cs), parentStats, false);
    assertEquals(1, ndv, "All-NULL column should NOT get +1 (numNulls == numRows)");
  }

  @Test
  void testComputeNDVGroupingColumnsUnknownNdv() {
    Statistics parentStats = new Statistics(1000, 8000, 0, 0);
    parentStats.setColumnStatsState(Statistics.State.COMPLETE);

    ColStatistics cs = new ColStatistics("col1", "string");
    cs.setCountDistint(0);  // unknown NDV
    cs.setNumNulls(50);
    cs.setIsEstimated(false);

    long ndv = StatsUtils.computeNDVGroupingColumns(Arrays.asList(cs), parentStats, false);
    assertEquals(0, ndv, "Unknown NDV (0) should NOT get +1 to avoid false precision");
  }

  @Test
  void testComputeNDVGroupingColumnsMultipleColumns() {
    Statistics parentStats = new Statistics(1000, 8000, 0, 0);
    parentStats.setColumnStatsState(Statistics.State.COMPLETE);

    ColStatistics cs1 = new ColStatistics("col1", "string");
    cs1.setCountDistint(10);
    cs1.setNumNulls(50);
    cs1.setIsEstimated(false);

    ColStatistics cs2 = new ColStatistics("col2", "int");
    cs2.setCountDistint(5);
    cs2.setNumNulls(0);
    cs2.setIsEstimated(false);

    long ndv = StatsUtils.computeNDVGroupingColumns(Arrays.asList(cs1, cs2), parentStats, false);
    // col1: 10 + 1 = 11 (has nulls), col2: 5 (no nulls)
    // Product: 11 * 5 = 55
    assertEquals(55, ndv, "Product of NDVs: (10+1) * 5 = 55");
  }

  @Test
  void testComputeNDVGroupingColumnsMixedEstimatedAndSource() {
    Statistics parentStats = new Statistics(1000, 8000, 0, 0);
    parentStats.setColumnStatsState(Statistics.State.COMPLETE);

    ColStatistics sourceCol = new ColStatistics("col1", "string");
    sourceCol.setCountDistint(10);
    sourceCol.setNumNulls(50);
    sourceCol.setIsEstimated(false);  // source: gets +1

    ColStatistics caseExpr = new ColStatistics("case_expr", "string");
    caseExpr.setCountDistint(3);
    caseExpr.setNumNulls(200);
    caseExpr.setIsEstimated(true);  // estimated: no +1

    long ndv = StatsUtils.computeNDVGroupingColumns(Arrays.asList(sourceCol, caseExpr), parentStats, false);
    // sourceCol: 10 + 1 = 11, caseExpr: 3 (no +1)
    // Product: 11 * 3 = 33
    assertEquals(33, ndv, "Mixed columns: source (10+1) * estimated (3) = 33");
  }

  // Test for NDV cap after StatEstimator (NDV cannot exceed numRows)

  @Test
  void testGetColStatisticsFromExpressionNdvCappedAtNumRows() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_STATS_ESTIMATORS_ENABLE, true);

    // Create parent stats with only 100 rows
    Statistics parentStats = new Statistics(100, 800, 0, 0);

    // Create column stats for col1 and col2 with high NDV (each 80)
    ColStatistics col1Stats = new ColStatistics("col1", "string");
    col1Stats.setCountDistint(80);
    col1Stats.setNumNulls(0);
    col1Stats.setAvgColLen(10);

    ColStatistics col2Stats = new ColStatistics("col2", "string");
    col2Stats.setCountDistint(80);
    col2Stats.setNumNulls(0);
    col2Stats.setAvgColLen(10);

    parentStats.setColumnStats(Arrays.asList(col1Stats, col2Stats));

    // Create IF(true, col1, col2) expression
    // IF uses PessimisticStatCombiner which sums NDVs: 80 + 80 = 160
    // But numRows is only 100, so NDV should be capped at 100
    GenericUDFIf udfIf = new GenericUDFIf();
    ExprNodeConstantDesc condExpr = new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, true);
    ExprNodeColumnDesc col1Expr = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "col1", "t", false);
    ExprNodeColumnDesc col2Expr = new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "col2", "t", false);

    ExprNodeGenericFuncDesc ifExpr = new ExprNodeGenericFuncDesc(
        TypeInfoFactory.stringTypeInfo, udfIf, "if",
        Arrays.asList(condExpr, col1Expr, col2Expr));

    ColStatistics result = StatsUtils.getColStatisticsFromExpression(conf, parentStats, ifExpr);

    assertNotNull(result);
    // PessimisticStatCombiner would produce 80 + 80 = 160, but cap ensures NDV <= numRows (100)
    assertEquals(100, result.getCountDistint(), "NDV should be capped at numRows (100), not 160");
  }

}
