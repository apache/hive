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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Timestamp;
import org.apache.hadoop.hive.metastore.api.TimestampColumnStatsData;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ColStatistics.Range;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
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
    exclusions.add(serdeConstants.UNKNOWN_TYPE_NAME);
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

  @ParameterizedTest(name = "{0}")
  @MethodSource("addToColumnStatsCountDistinctCases")
  void testStatisticsAddToColumnStatsCountDistinctMerge(
      String scenarioName, long existingNdv, long incomingNdv, long expectedMergedNdv) {
    Statistics stats = new Statistics(1000, 8000, 0, 0);
    ColStatistics existing = createColStats("col1", existingNdv, 0);
    stats.setColumnStats(Collections.singletonList(existing));

    ColStatistics incoming = createColStats("col1", incomingNdv, 0);
    stats.addToColumnStats(Collections.singletonList(incoming));

    ColStatistics merged = stats.getColumnStatisticsFromColName("col1");
    assertEquals(expectedMergedNdv, merged.getCountDistint(),
        "countDistinct after merge");
  }

  private static Stream<Arguments> addToColumnStatsCountDistinctCases() {
    return Stream.of(
        Arguments.of("incomingUnknownPropagates",   5L, -1L, -1L),
        Arguments.of("existingUnknownPropagates", -1L,  5L, -1L),
        Arguments.of("bothUnknownStaysUnknown",   -1L, -1L, -1L),
        Arguments.of("maxPicksIncomingWhenHigher", 3L,  7L,  7L),
        Arguments.of("maxPicksExistingWhenHigher", 7L,  3L,  7L)
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("containsUnknownNDVCases")
  void testContainsUnknownNDV(String scenarioName, List<Long> input, boolean expected) {
    assertEquals(expected, StatsUtils.containsUnknownNDV(input),
        "containsUnknownNDV(" + input + ")");
  }

  private static Stream<Arguments> containsUnknownNDVCases() {
    return Stream.of(
        Arguments.of("allPositive",                Arrays.asList(1L, 2L, 3L),   false),
        Arguments.of("containsZero_NotUnknown",    Arrays.asList(1L, 0L, 3L),   false),
        Arguments.of("singleUnknown",              Arrays.asList(1L, -1L, 3L),  true),
        Arguments.of("allUnknown",                 Arrays.asList(-1L, -1L, -1L), true),
        Arguments.of("firstIsUnknown_ShortCircuit", Arrays.asList(-1L, 2L, 3L),  true),
        Arguments.of("emptyList",                  Collections.emptyList(),     false)
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("maxOrUnknownCases")
  void testMaxPropagatingUnknown(String name, long a, long b, long expected) {
    assertEquals(expected, StatsUtils.maxOrUnknown(a, b));
  }

  private static Stream<Arguments> maxOrUnknownCases() {
    return Stream.of(
        Arguments.of("bothKnownPicksMax",   3L,  7L,  7L),
        Arguments.of("firstUnknown",       -1L,  7L, -1L),
        Arguments.of("secondUnknown",       7L, -1L, -1L),
        Arguments.of("bothUnknown",        -1L, -1L, -1L),
        Arguments.of("verifiedZeroIsKnown", 0L,  0L,  0L)
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("scaleDownNDVCases")
  void testScaleDownNDV(String name, long ndv, double ratio, long expected) {
    assertEquals(expected, StatsUtils.scaleDownNDV(ndv, ratio));
  }

  private static Stream<Arguments> scaleDownNDVCases() {
    return Stream.of(
        Arguments.of("shrinksByRatio",           100L, 0.5, 50L),
        Arguments.of("ratioOneKeepsNdv",         100L, 1.0, 100L),
        Arguments.of("ratioAboveOneKeepsNdv",    100L, 2.0, 100L),
        Arguments.of("hugeNdvExactAtRatioOne",   (1L << 53) + 1, 1.0, (1L << 53) + 1),
        Arguments.of("zeroRatioZeroesNdv",       100L, 0.0, 0L)
    );
  }

  @Test
  void testAddWithExpDecayReturnsUnknownWhenAnyInputIsUnknown() {
    Long result = StatsUtils.addWithExpDecay(Arrays.asList(10L, -1L, 5L));
    assertEquals(-1L, result, "addWithExpDecay should propagate unknown NDV (-1) when present");
  }

  @Test
  void testAddWithExpDecayComputesWhenAllInputsKnown() {
    Long result = StatsUtils.addWithExpDecay(Arrays.asList(100L, 25L));
    // Exponential decay: 100 * 25^(1/2) = 100 * 5 = 500.
    assertEquals(500L, result, "addWithExpDecay should return the exponential-decay denominator for known inputs");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("computeNDVGroupingColumnsCases")
  void testComputeNDVGroupingColumns(String scenarioName, List<ColStatistics> colStats,
      Statistics.State parentColStatsState, boolean expDecay, long expected) {
    Statistics parentStats = new Statistics(1000, 8000, 0, 0);
    parentStats.setColumnStatsState(parentColStatsState);

    long result = StatsUtils.computeNDVGroupingColumns(colStats, parentStats, expDecay);

    assertEquals(expected, result, scenarioName);
  }

  private static Stream<Arguments> computeNDVGroupingColumnsCases() {
    return Stream.of(
        Arguments.of("allKnownReturnsProduct",
            Arrays.asList(makeColStat("c1", 10), makeColStat("c2", 20)),
            Statistics.State.COMPLETE, false, 200L),
        Arguments.of("unknownColumnReturnsMinusOne",
            Arrays.asList(makeColStat("c1", 10), makeColStat("c2", -1)),
            Statistics.State.COMPLETE, false, -1L),
        Arguments.of("emptyColumnsReturnsOne",
            Collections.<ColStatistics>emptyList(),
            Statistics.State.COMPLETE, false, 1L),
        Arguments.of("nullColStatWithCompleteParentSkipped",
            Arrays.asList(null, makeColStat("c2", 10)),
            Statistics.State.COMPLETE, false, 10L),
        Arguments.of("nullColStatWithPartialParentReturnsMinusOne",
            Arrays.asList((ColStatistics) null),
            Statistics.State.PARTIAL, false, -1L),
        Arguments.of("expDecayWithKnownInputs",
            Arrays.asList(makeColStat("c1", 100), makeColStat("c2", 25)),
            Statistics.State.COMPLETE, true, 500L),
        Arguments.of("expDecayWithUnknownPropagates",
            Arrays.asList(makeColStat("c1", 100), makeColStat("c2", -1)),
            Statistics.State.COMPLETE, true, -1L)
    );
  }

  private static ColStatistics makeColStat(String name, long ndv) {
    ColStatistics cs = new ColStatistics(name, "string");
    cs.setCountDistint(ndv);
    cs.setNumNulls(0);
    return cs;
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("getColStatisticsUnsetNumDVsCases")
  void testGetColStatisticsReturnsUnknownNDVWhenNumDVsNotSet(
      String typeName, ColumnStatisticsData data) {
    ColumnStatisticsObj cso = new ColumnStatisticsObj();
    cso.setColName("test_col");
    cso.setColType(typeName);
    cso.setStatsData(data);

    ColStatistics cs = StatsUtils.getColStatistics(cso, "test_col");

    assertNotNull(cs, "ColStatistics should not be null for " + typeName);
    assertEquals(-1, cs.getCountDistint(),
        "When numDVs is unset for " + typeName + ", NDV should be -1");
  }

  private static Stream<Arguments> getColStatisticsUnsetNumDVsCases() {
    LongColumnStatsData longStats = new LongColumnStatsData();
    longStats.setNumNulls(10);
    // numDVs NOT set

    DoubleColumnStatsData doubleStats = new DoubleColumnStatsData();
    doubleStats.setNumNulls(10);

    StringColumnStatsData stringStats = new StringColumnStatsData();
    stringStats.setNumNulls(10);
    stringStats.setAvgColLen(5.0);
    stringStats.setMaxColLen(20);

    BinaryColumnStatsData binaryStats = new BinaryColumnStatsData();
    binaryStats.setNumNulls(10);
    binaryStats.setAvgColLen(5.0);
    binaryStats.setMaxColLen(20);

    TimestampColumnStatsData timestampStats = new TimestampColumnStatsData();
    timestampStats.setNumNulls(10);

    DecimalColumnStatsData decimalStats = new DecimalColumnStatsData();
    decimalStats.setNumNulls(10);

    DateColumnStatsData dateStats = new DateColumnStatsData();
    dateStats.setNumNulls(10);

    return Stream.of(
        Arguments.of(serdeConstants.BIGINT_TYPE_NAME,    wrapLong(longStats)),
        Arguments.of(serdeConstants.DOUBLE_TYPE_NAME,    wrapDouble(doubleStats)),
        Arguments.of(serdeConstants.STRING_TYPE_NAME,    wrapString(stringStats)),
        Arguments.of(serdeConstants.BINARY_TYPE_NAME,    wrapBinary(binaryStats)),
        Arguments.of(serdeConstants.TIMESTAMP_TYPE_NAME, wrapTimestamp(timestampStats)),
        Arguments.of(serdeConstants.DECIMAL_TYPE_NAME,   wrapDecimal(decimalStats)),
        Arguments.of(serdeConstants.DATE_TYPE_NAME,      wrapDate(dateStats))
    );
  }

  @Test
  void testGetColStatisticsTimestampLocalTzHasUnknownNDVAndNumNulls() {
    // ANALYZE maps TIMESTAMPLOCALTZ to LONG column stats (ColStatsProcessor), but the
    // read path ignores the stats data for this type, so NDV and numNulls must surface
    // as unknown (-1) rather than the field defaults of 0 ("verified zero" / "no nulls").
    LongColumnStatsData longStats = new LongColumnStatsData();
    longStats.setNumNulls(2);
    longStats.setNumDVs(42);
    ColumnStatisticsObj cso = new ColumnStatisticsObj();
    cso.setColName("test_col");
    cso.setColType(serdeConstants.TIMESTAMPLOCALTZ_TYPE_NAME);
    cso.setStatsData(wrapLong(longStats));

    ColStatistics cs = StatsUtils.getColStatistics(cso, "test_col");

    assertNotNull(cs, "ColStatistics should not be null for timestamplocaltz");
    assertEquals(-1, cs.getCountDistint(),
        "NDV is not read for timestamplocaltz, so it should be -1 (unknown)");
    assertEquals(-1, cs.getNumNulls(),
        "numNulls is not read for timestamplocaltz, so it should be -1 (unknown)");
  }

  private static ColumnStatisticsData wrapLong(LongColumnStatsData s) {
    ColumnStatisticsData d = new ColumnStatisticsData();
    d.setLongStats(s);
    return d;
  }

  private static ColumnStatisticsData wrapDouble(DoubleColumnStatsData s) {
    ColumnStatisticsData d = new ColumnStatisticsData();
    d.setDoubleStats(s);
    return d;
  }

  private static ColumnStatisticsData wrapString(StringColumnStatsData s) {
    ColumnStatisticsData d = new ColumnStatisticsData();
    d.setStringStats(s);
    return d;
  }

  private static ColumnStatisticsData wrapBinary(BinaryColumnStatsData s) {
    ColumnStatisticsData d = new ColumnStatisticsData();
    d.setBinaryStats(s);
    return d;
  }

  private static ColumnStatisticsData wrapTimestamp(TimestampColumnStatsData s) {
    ColumnStatisticsData d = new ColumnStatisticsData();
    d.setTimestampStats(s);
    return d;
  }

  private static ColumnStatisticsData wrapDecimal(DecimalColumnStatsData s) {
    ColumnStatisticsData d = new ColumnStatisticsData();
    d.setDecimalStats(s);
    return d;
  }

  private static ColumnStatisticsData wrapDate(DateColumnStatsData s) {
    ColumnStatisticsData d = new ColumnStatisticsData();
    d.setDateStats(s);
    return d;
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
  void testUpdateStatsMarksFilteredColumnEvenWhenNDVUnknown() {
    // HIVE-29625: setFilterColumn() is now called unconditionally for affected columns,
    // even when NDV is unknown (-1). The NDV math is skipped but the filter mark applies.
    Statistics stats = new Statistics(1000, 8000, 0, 0);
    ColStatistics cs = createColStats("col1", -1, 0); // unknown NDV
    stats.setColumnStats(Collections.singletonList(cs));

    StatsUtils.updateStats(stats, 500, true, null, Collections.singleton("col1"));

    ColStatistics updated = stats.getColumnStats().get(0);
    assertEquals(true, updated.isFilteredColumn(),
        "Filter-column flag should be set even when NDV is unknown");
    assertEquals(-1, updated.getCountDistint(),
        "Unknown NDV (-1) should be preserved when affected column has no NDV");
  }

  @Test
  void testUpdateStatsRecomputesNDVWhenAffectedAndKnown() {
    // when NDV is known and ratio < 1.0, the NDV math still runs inside the oldDV >= 0 guard
    Statistics stats = new Statistics(1000, 8000, 0, 0);
    ColStatistics cs = createColStats("col1", 100, 0); // known NDV
    stats.setColumnStats(Collections.singletonList(cs));

    StatsUtils.updateStats(stats, 500, true, null, Collections.singleton("col1"));

    ColStatistics updated = stats.getColumnStats().get(0);
    assertEquals(true, updated.isFilteredColumn(),
        "Filter-column flag should be set for affected column with known NDV");
    // ratio = 500/1000 = 0.5 -> newDV = ceil(0.5 * 100) = 50
    assertEquals(50, updated.getCountDistint(),
        "Known NDV should be scaled by the row-count ratio");
  }

  @Test
  void testUpdateStatsRatioOneKeepsNDV() {
    // at ratio == 1.0 the row count did not shrink, so the NDV must not change
    Statistics stats = new Statistics(1000, 8000, 0, 0);
    ColStatistics cs = createColStats("col1", 100, 0);
    stats.setColumnStats(Collections.singletonList(cs));

    StatsUtils.updateStats(stats, 1000, true, null, Collections.singleton("col1"));

    assertEquals(100, stats.getColumnStats().get(0).getCountDistint(),
        "NDV should be unchanged when the row count is unchanged");
  }

  @Test
  void testUpdateStatsRatioOnePreservesHugeNDVExactly() {
    // at ratio == 1.0 the scaling must be skipped, not computed: ceil(1.0 * oldDV)
    // round-trips through double and corrupts NDVs above 2^53
    long hugeNdv = (1L << 53) + 1;
    Statistics stats = new Statistics(hugeNdv, 8000, 0, 0);
    ColStatistics cs = createColStats("col1", hugeNdv, 0);
    stats.setColumnStats(Collections.singletonList(cs));

    StatsUtils.updateStats(stats, hugeNdv, true, null, Collections.singleton("col1"));

    assertEquals(hugeNdv, stats.getColumnStats().get(0).getCountDistint(),
        "NDV above 2^53 must survive a ratio == 1.0 update bit-exactly");
  }

  @Test
  void testScaleColStatisticsScalesDownKnownNDV() {
    ColStatistics cs = createColStats("col1", 100, 0);
    StatsUtils.scaleColStatistics(Collections.singletonList(cs), 0.5);
    assertEquals(50, cs.getCountDistint(), "Known NDV should scale with the factor");
  }

  @Test
  void testScaleColStatisticsKeepsNDVWhenFactorAboveOne() {
    ColStatistics cs = createColStats("col1", 100, 0);
    StatsUtils.scaleColStatistics(Collections.singletonList(cs), 2.0);
    assertEquals(100, cs.getCountDistint(), "Row growth cannot add distinct values");
  }

  @Test
  void testScaleColStatisticsPreservesUnknownCountDistint() {
    // HIVE-29625: when factor < 1.0 and NDV is unknown (-1), the sentinel is preserved.
    ColStatistics cs = createColStats("col1", -1, 0); // unknown NDV
    List<ColStatistics> colStats = Collections.singletonList(cs);

    StatsUtils.scaleColStatistics(colStats, 0.5);

    assertEquals(-1, colStats.get(0).getCountDistint(),
        "Unknown NDV (-1) should be preserved when factor < 1.0");
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

  @Test
  void testGetColStatisticsDateType() {
    ColumnStatisticsObj cso = new ColumnStatisticsObj();
    cso.setColName("date_col");
    cso.setColType(serdeConstants.DATE_TYPE_NAME);

    byte[] bitVectors = new byte[] {1, 2, 3, 4};
    DateColumnStatsData dateStats = new DateColumnStatsData();
    dateStats.setNumDVs(100);
    dateStats.setNumNulls(10);
    dateStats.setLowValue(new Date(18000)); // days since epoch
    dateStats.setHighValue(new Date(19000));
    dateStats.setBitVectors(bitVectors);

    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setDateStats(dateStats);
    cso.setStatsData(data);

    ColStatistics cs = StatsUtils.getColStatistics(cso, "date_col");

    assertNotNull(cs, "ColStatistics should not be null");
    assertEquals(100, cs.getCountDistint(), "NumDVs mismatch for DATE");
    assertEquals(10, cs.getNumNulls(), "NumNulls mismatch for DATE");
    assertNotNull(cs.getBitVectors(), "BitVectors should not be null for DATE");

    ColStatistics.Range range = cs.getRange();
    assertNotNull(range, "Range should be created for DATE");
    assertEquals(18000L, range.minValue.longValue(), "minValue mismatch for DATE");
    assertEquals(19000L, range.maxValue.longValue(), "maxValue mismatch for DATE");
  }

  @Test
  void testGetColStatisticsTimestampType() {
    ColumnStatisticsObj cso = new ColumnStatisticsObj();
    cso.setColName("ts_col");
    cso.setColType(serdeConstants.TIMESTAMP_TYPE_NAME);

    byte[] bitVectors = new byte[] {5, 6, 7, 8};
    TimestampColumnStatsData tsStats = new TimestampColumnStatsData();
    tsStats.setNumDVs(200);
    tsStats.setNumNulls(20);
    tsStats.setLowValue(new Timestamp(1600000000L));
    tsStats.setHighValue(new Timestamp(1700000000L));
    tsStats.setBitVectors(bitVectors);

    ColumnStatisticsData data = new ColumnStatisticsData();
    data.setTimestampStats(tsStats);
    cso.setStatsData(data);

    ColStatistics cs = StatsUtils.getColStatistics(cso, "ts_col");

    assertNotNull(cs, "ColStatistics should not be null");
    assertEquals(200, cs.getCountDistint(), "NumDVs mismatch for TIMESTAMP");
    assertEquals(20, cs.getNumNulls(), "NumNulls mismatch for TIMESTAMP");
    assertNotNull(cs.getBitVectors(), "BitVectors should not be null for TIMESTAMP");

    ColStatistics.Range range = cs.getRange();
    assertNotNull(range, "Range should be created for TIMESTAMP");
    assertEquals(1600000000L, range.minValue.longValue(), "minValue mismatch for TIMESTAMP");
    assertEquals(1700000000L, range.maxValue.longValue(), "maxValue mismatch for TIMESTAMP");
  }

  @Test
  void testEstimateStatsForMissingColsHandlesEmptyList() {
    HiveConf conf = new HiveConf();

    ColumnInfo columnInfoA = new ColumnInfo("a", TypeInfoFactory.intTypeInfo, "t", false);

    List<ColStatistics> allColumnStats = StatsUtils.estimateStatsForMissingCols(
        List.of("a"), Collections.emptyList(), conf, 0, List.of(columnInfoA));

    assertEquals(1, allColumnStats.size());
  }

  @Test
  void testEstimateStatsForMissingColsCombinesExistingStatsAndEstimations() {
    HiveConf conf = new HiveConf();

    ColumnInfo colNeededButNotExists = new ColumnInfo("neededButNotExists", TypeInfoFactory.intTypeInfo, "t", false);
    ColumnInfo colNeededAndExists = new ColumnInfo("neededAndExists", TypeInfoFactory.intTypeInfo, "t", false);
    ColumnInfo colNotNeededButExists = new ColumnInfo("notNeededButExists", TypeInfoFactory.intTypeInfo, "t", false);
    ColumnInfo colNotNeededNotExists = new ColumnInfo("notNeededNotExists", TypeInfoFactory.intTypeInfo, "t", false);

    ColStatistics colStatNeededAndExists = new ColStatistics();
    colStatNeededAndExists.setColumnName(colNeededAndExists.getInternalName());
    ColStatistics colStatNotNeededButExists = new ColStatistics();
    colStatNotNeededButExists.setColumnName(colNotNeededButExists.getInternalName());

    List<ColStatistics> allColumnStats = StatsUtils.estimateStatsForMissingCols(
        List.of(colNeededAndExists.getInternalName(), colNeededButNotExists.getInternalName()),
        List.of(colStatNeededAndExists, colStatNotNeededButExists),
        conf,
        0,
        List.of(colNeededButNotExists, colNeededAndExists, colNotNeededButExists, colNotNeededNotExists));

    assertEquals(3, allColumnStats.size());
    assertEquals(colStatNeededAndExists, allColumnStats.get(0));
    assertFalse(allColumnStats.get(0).isEstimated());
    assertEquals(colStatNotNeededButExists, allColumnStats.get(1));
    assertFalse(allColumnStats.get(1).isEstimated());
    assertEquals(colNeededButNotExists.getInternalName(), allColumnStats.get(2).getColumnName());
    assertTrue(allColumnStats.get(2).isEstimated());
  }

  @Test
  void testEstimateStatsForMissingColsReturnOnlyColumnsWithExistingStatsWhenNoNeededColumn() {
    HiveConf conf = new HiveConf();

    ColumnInfo colNotNeededButExists = new ColumnInfo("notNeededButExists", TypeInfoFactory.intTypeInfo, "t", false);
    ColumnInfo colNotNeededNotExists = new ColumnInfo("notNeededNotExists", TypeInfoFactory.intTypeInfo, "t", false);

    ColStatistics colStatNotNeededButExists = new ColStatistics();
    colStatNotNeededButExists.setColumnName(colNotNeededButExists.getInternalName());

    List<ColStatistics> allColumnStats = StatsUtils.estimateStatsForMissingCols(
        Collections.emptyList(),
        List.of(colStatNotNeededButExists),
        conf,
        0,
        List.of(colNotNeededButExists, colNotNeededNotExists));

    assertEquals(1, allColumnStats.size());
    assertEquals(allColumnStats.getFirst(), colStatNotNeededButExists);
  }

}
