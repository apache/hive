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

import org.apache.hadoop.hive.common.ndv.fm.FMSketch;
import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreServerUtils.ColStatsObjWithSourceInfo;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hadoop.hive.metastore.Warehouse.DEFAULT_CATALOG_NAME;

public class StatisticsTestUtils {

  private static final String HIVE_ENGINE = "hive";

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
}
