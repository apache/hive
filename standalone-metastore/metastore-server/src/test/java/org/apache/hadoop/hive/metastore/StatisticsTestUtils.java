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
package org.apache.hadoop.hive.metastore;

import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.hadoop.hive.common.histogram.kll.KllUtils;
import org.apache.hadoop.hive.common.ndv.fm.FMSketch;
import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.TimestampColumnStatsData;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.ColStatsObjWithSourceInfo;
import org.junit.Assert;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Collections;

/**
 * Provides utility methods for writing test around statistics.
 * WARNING: Due to shading and relocation from the "hive-exec" module of the KllFloatSketch class,
 * it is not safe to use this class outside the "standalone-metastore/metastore-server" module.
 */
public class StatisticsTestUtils {

  public static final String HIVE_ENGINE = "hive";

  private StatisticsTestUtils() {
    throw new AssertionError("Suppress default constructor for non instantiation");
  }

  /**
   * Creates a {@link ColStatsObjWithSourceInfo} object for a given table, partition and column information,
   * using the given statistics data.
   * @param data the column statistics data
   * @param tbl the target table for stats
   * @param column the target column for stats
   * @param partName the target partition for stats
   * @return column statistics objects with source info.
   */
  public static ColStatsObjWithSourceInfo createStatsWithInfo(ColumnStatisticsData data, Table tbl,
      FieldSchema column, String partName) {
    ColumnStatisticsObj statObj = new ColumnStatisticsObj(column.getName(), column.getType(), data);
    return new ColStatsObjWithSourceInfo(statObj, tbl.getCatName(), tbl.getDbName(), column.getName(), partName);
  }

  /**
   * Creates a {@link ColumnStatistics} for a given table, partition and column information,
   * using the given statistics data.
   * @param data the column statistics data
   * @param tbl the target table for stats
   * @param column the target column for stats
   * @param partName the target partition for stats
   * @return column statistics object.
   */
  public static ColumnStatistics createColumnStatistics(ColumnStatisticsData data, Table tbl,
      FieldSchema column, String partName) {
    ColumnStatisticsObj statObj = new ColumnStatisticsObj(column.getName(), column.getType(), data);
    ColumnStatistics colStats = new ColumnStatistics();
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, tbl.getDbName(), tbl.getTableName());
    statsDesc.setPartName(partName);
    colStats.setStatsDesc(statsDesc);
    colStats.setStatsObj(Collections.singletonList(statObj));
    colStats.setEngine(HIVE_ENGINE);
    return colStats;
  }

  /**
   * Creates an FM sketch object initialized with the given values.
   * @param values the values to be added
   * @return an FM sketch initialized with the given values.
   */
  public static FMSketch createFMSketch(long... values) {
    FMSketch fm = new FMSketch(1);
    for (long value : values) {
      fm.addToEstimator(value);
    }
    return fm;
  }

  /**
   * Creates an FM sketch object initialized with the given values.
   * @param values the values to be added
   * @return an FM sketch initialized with the given values.
   */
  public static FMSketch createFMSketch(String... values) {
    FMSketch fm = new FMSketch(1);
    for (String value : values) {
      fm.addToEstimator(value);
    }
    return fm;
  }

  /**
   * Creates an HLL object initialized with the given values.
   * @param values the values to be added
   * @return an HLL object initialized with the given values.
   */
  public static HyperLogLog createHll(long... values) {
    HyperLogLog hll = HyperLogLog.builder().build();
    for (long value : values) {
      hll.addLong(value);
    }
    return hll;
  }

  /**
   * Creates an HLL object initialized with the given values.
   * @param values the values to be added
   * @return an HLL object initialized with the given values.
   */
  public static HyperLogLog createHll(String... values) {
    HyperLogLog hll = HyperLogLog.builder().build();
    for (String value : values) {
      hll.addBytes(value.getBytes());
    }
    return hll;
  }

  /**
   * Creates an HLL object initialized with the given values.
   * @param values the values to be added
   * @return an HLL object initialized with the given values.
   */
  public static HyperLogLog createHll(double... values) {
    HyperLogLog hll = HyperLogLog.builder().build();
    Arrays.stream(values).forEach(hll::addDouble);
    return hll;
  }

  /**
   * Creates a KLL object initialized with the given values.
   * @param values the values to be added
   * @return a KLL object initialized with the given values.
   */
  public static KllFloatsSketch createKll(float... values) {
    KllFloatsSketch kll = new KllFloatsSketch();
    for (float value : values) {
      kll.update(value);
    }
    return kll;
  }

  /**
   * Creates a KLL object initialized with the given values.
   * @param values the values to be added
   * @return a KLL object initialized with the given values.
   */
  public static KllFloatsSketch createKll(double... values) {
    KllFloatsSketch kll = new KllFloatsSketch();
    for (double value : values) {
      kll.update(Double.valueOf(value).floatValue());
    }
    return kll;
  }

  /**
   * Creates a KLL object initialized with the given values.
   * @param values the values to be added
   * @return a KLL object initialized with the given values.
   */
  public static KllFloatsSketch createKll(long... values) {
    KllFloatsSketch kll = new KllFloatsSketch();
    for (long value : values) {
      kll.update(value);
    }
    return kll;
  }

  /**
   * Checks if expected and computed statistics data are equal.
   * @param expected expected statistics data
   * @param computed computed statistics data
   */
  public static void assertEqualStatistics(ColumnStatisticsData expected, ColumnStatisticsData computed) {
    if (expected.getSetField() != computed.getSetField()) {
      throw new IllegalArgumentException("Expected data is of type " + expected.getSetField()
          + " while computed data is of type " + computed.getSetField());
    }

    Class<?> dataClass;
    switch (expected.getSetField()) {
    case DATE_STATS:
      dataClass = DateColumnStatsData.class;
      break;
    case LONG_STATS:
      dataClass = LongColumnStatsData.class;
      break;
    case DOUBLE_STATS:
      dataClass = DoubleColumnStatsData.class;
      break;
    case DECIMAL_STATS:
      dataClass = DecimalColumnStatsData.class;
      break;
    case TIMESTAMP_STATS:
      dataClass = TimestampColumnStatsData.class;
      break;
    default:
      // it's an unsupported class for KLL, no special treatment needed
      Assert.assertEquals(expected, computed);
      return;
    }
    assertEqualStatistics(expected, computed, dataClass);
  }

  private static <X> void assertEqualStatistics(
      ColumnStatisticsData expected, ColumnStatisticsData computed, Class<X> clazz) {
    try {
      final Object computedStats = computed.getFieldValue(computed.getSetField());
      final Object expectedStats = expected.getFieldValue(computed.getSetField());
      final boolean computedHasHistograms = (boolean) clazz.getMethod("isSetHistogram").invoke(computedStats);
      final boolean expectedHasHistograms = (boolean) clazz.getMethod("isSetHistogram").invoke(expectedStats);

      if (computedHasHistograms && expectedHasHistograms) {
        // KLL data sketches serialization depends on the insertion order, we can only safely compare their string
        // representation, we replace the serialized version and restore it back after comparison to not alter the input
        byte[] expectedHistogram = (byte[]) clazz.getMethod("getHistogram").invoke(expectedStats);
        byte[] computedHistogram = (byte[]) clazz.getMethod("getHistogram").invoke(computedStats);

        Assert.assertEquals(KllUtils.deserializeKll(expectedHistogram).toString(),
            KllUtils.deserializeKll(computedHistogram).toString());

        clazz.getMethod("setHistogram", byte[].class).invoke(expectedStats, computedHistogram);
        Assert.assertEquals(expected, computed);
        clazz.getMethod("setHistogram", byte[].class).invoke(expectedStats, expectedHistogram);
      } else {
        Assert.assertEquals(expected, computed);
      }
    } catch(NoSuchMethodException | IllegalAccessException | InvocationTargetException e){
      throw new RuntimeException("Reflection error", e);
    }
  }
}
