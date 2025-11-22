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

package org.apache.hadoop.hive.ql.ddl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.stream.Stream;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class TestShowUtils {

  @ParameterizedTest(name = "{0} - {1}")
  @MethodSource("longStatsTestData")
  public void testExtractColumnValues_LongStats(String typeName, String scenarioName,
      Long lowValue, Long highValue, String expectedMin, String expectedMax) {
    FieldSchema column = new FieldSchema("test_col", typeName, null);

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

    String[] result = ShowUtils.extractColumnValues(column, true, cso, false);

    assertNotNull(result, "Result array should not be null");
    assertEquals("test_col", result[0], "Column name mismatch");
    assertEquals(typeName, result[1], "Column type mismatch");
    assertEquals(expectedMin, result[2], "Min value mismatch");
    assertEquals(expectedMax, result[3], "Max value mismatch");
    assertEquals("10", result[4], "NumNulls mismatch");
    assertEquals("100", result[5], "NumDVs mismatch");
  }

  @ParameterizedTest(name = "{0} - {1}")
  @MethodSource("doubleStatsTestData")
  public void testExtractColumnValues_DoubleStats(String typeName, String scenarioName,
      Double lowValue, Double highValue, String expectedMin, String expectedMax) {
    FieldSchema column = new FieldSchema("test_col", typeName, null);

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

    String[] result = ShowUtils.extractColumnValues(column, true, cso, false);

    assertNotNull(result, "Result array should not be null");
    assertEquals("test_col", result[0], "Column name mismatch");
    assertEquals(typeName, result[1], "Column type mismatch");
    assertEquals(expectedMin, result[2], "Min value mismatch");
    assertEquals(expectedMax, result[3], "Max value mismatch");
    assertEquals("10", result[4], "NumNulls mismatch");
    assertEquals("100", result[5], "NumDVs mismatch");
  }

  static Stream<Arguments> longStatsTestData() {
    return Stream.of(
      // {typeName, scenarioName, lowValue, highValue, expectedMin, expectedMax}
      Arguments.of(serdeConstants.TINYINT_TYPE_NAME, "BothValuesSet", 1L, 1000L, "1", "1000"),
      Arguments.of(serdeConstants.TINYINT_TYPE_NAME, "NoValuesSet", null, null, "", ""),
      Arguments.of(serdeConstants.TINYINT_TYPE_NAME, "OnlyLowValueSet", 100L, null, "100", ""),
      Arguments.of(serdeConstants.TINYINT_TYPE_NAME, "OnlyHighValueSet", null, 1000L, "", "1000"),
      Arguments.of(serdeConstants.TINYINT_TYPE_NAME, "NegativeHighValueOnly", null, -5L, "", "-5"),
      Arguments.of(serdeConstants.SMALLINT_TYPE_NAME, "BothValuesSet", 1L, 1000L, "1", "1000"),
      Arguments.of(serdeConstants.SMALLINT_TYPE_NAME, "NoValuesSet", null, null, "", ""),
      Arguments.of(serdeConstants.SMALLINT_TYPE_NAME, "OnlyLowValueSet", 100L, null, "100", ""),
      Arguments.of(serdeConstants.SMALLINT_TYPE_NAME, "OnlyHighValueSet", null, 1000L, "", "1000"),
      Arguments.of(serdeConstants.SMALLINT_TYPE_NAME, "NegativeHighValueOnly", null, -5L, "", "-5"),
      Arguments.of(serdeConstants.INT_TYPE_NAME, "BothValuesSet", 1L, 1000L, "1", "1000"),
      Arguments.of(serdeConstants.INT_TYPE_NAME, "NoValuesSet", null, null, "", ""),
      Arguments.of(serdeConstants.INT_TYPE_NAME, "OnlyLowValueSet", 100L, null, "100", ""),
      Arguments.of(serdeConstants.INT_TYPE_NAME, "OnlyHighValueSet", null, 1000L, "", "1000"),
      Arguments.of(serdeConstants.INT_TYPE_NAME, "NegativeHighValueOnly", null, -5L, "", "-5"),
      Arguments.of(serdeConstants.BIGINT_TYPE_NAME, "BothValuesSet", 1L, 1000L, "1", "1000"),
      Arguments.of(serdeConstants.BIGINT_TYPE_NAME, "NoValuesSet", null, null, "", ""),
      Arguments.of(serdeConstants.BIGINT_TYPE_NAME, "OnlyLowValueSet", 100L, null, "100", ""),
      Arguments.of(serdeConstants.BIGINT_TYPE_NAME, "OnlyHighValueSet", null, 1000L, "", "1000"),
      Arguments.of(serdeConstants.BIGINT_TYPE_NAME, "NegativeHighValueOnly", null, -5L, "", "-5")
    );
  }

  static Stream<Arguments> doubleStatsTestData() {
    return Stream.of(
      // {typeName, scenarioName, lowValue, highValue, expectedMin, expectedMax}
      Arguments.of(serdeConstants.FLOAT_TYPE_NAME, "BothValuesSet", 1.5, 1000.5, "1.5", "1000.5"),
      Arguments.of(serdeConstants.FLOAT_TYPE_NAME, "NoValuesSet", null, null, "", ""),
      Arguments.of(serdeConstants.FLOAT_TYPE_NAME, "OnlyLowValueSet", 100.5, null, "100.5", ""),
      Arguments.of(serdeConstants.FLOAT_TYPE_NAME, "OnlyHighValueSet", null, 1000.5, "", "1000.5"),
      Arguments.of(serdeConstants.FLOAT_TYPE_NAME, "NegativeHighValueOnly", null, -5.5, "", "-5.5"),
      Arguments.of(serdeConstants.DOUBLE_TYPE_NAME, "BothValuesSet", 1.5, 1000.5, "1.5", "1000.5"),
      Arguments.of(serdeConstants.DOUBLE_TYPE_NAME, "NoValuesSet", null, null, "", ""),
      Arguments.of(serdeConstants.DOUBLE_TYPE_NAME, "OnlyLowValueSet", 100.5, null, "100.5", ""),
      Arguments.of(serdeConstants.DOUBLE_TYPE_NAME, "OnlyHighValueSet", null, 1000.5, "", "1000.5"),
      Arguments.of(serdeConstants.DOUBLE_TYPE_NAME, "NegativeHighValueOnly", null, -5.5, "", "-5.5")
    );
  }

}
