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
   * Creates column statistics for a given table and partition.
   * @param data the statistics data
   * @param tbl the target table
   * @param column the target column
   * @param partName the target partition
   * @return column statistics for a given table and partition.
   */
  public static ColumnStatistics createColStats(ColumnStatisticsData data, Table tbl, FieldSchema column, String partName) {
    ColumnStatisticsObj statObj = new ColumnStatisticsObj(column.getName(), column.getType(), data);
    ColumnStatistics colStats = new ColumnStatistics();
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, tbl.getDbName(), tbl.getTableName());
    statsDesc.setPartName(partName);
    colStats.setStatsDesc(statsDesc);
    colStats.setStatsObj(Collections.singletonList(statObj));
    colStats.setEngine(HIVE_ENGINE);
    return colStats;
  }

  public static ColStatsObjWithSourceInfo createStatsWithInfo(ColumnStatisticsData data, Table tbl,
      FieldSchema column, String partName) {
    ColumnStatisticsObj statObj = new ColumnStatisticsObj(column.getName(), column.getType(), data);
    ColumnStatistics colStats = new ColumnStatistics();
    ColumnStatisticsDesc statsDesc = new ColumnStatisticsDesc(true, tbl.getDbName(), tbl.getTableName());
    statsDesc.setPartName(partName);
    colStats.setStatsDesc(statsDesc);
    colStats.setStatsObj(Collections.singletonList(statObj));
    colStats.setEngine(HIVE_ENGINE);
    return new ColStatsObjWithSourceInfo(statObj, tbl.getCatName(), tbl.getDbName(), column.getName(), partName);
  }

  /**
   * Creates a list of {@link ColStatsObjWithSourceInfo} for a given table, its partitions and using the given stats.
   * @param table the table the statistics information relates to
   * @param partitionNames the partition names of the given table
   * @param stats the statistics for the table partitions
   * @param indexes the indexes to select for which
   * @return a list of {@link ColStatsObjWithSourceInfo} for a given table, its partitions and using the given stats.
   */
  public static List<ColStatsObjWithSourceInfo> createColStatsObjWithSourceInfoList(
      Table table, List<String> partitionNames, List<ColumnStatistics> stats, List<Integer> indexes) {

    if (partitionNames.size() != stats.size()) {
      throw new IllegalArgumentException("partitionNames and stats lists must have the same length, found "
          + partitionNames.size() + " and " + stats.size() + ", respectively");
    }

    if (indexes.size() > partitionNames.size()) {
      throw new IllegalArgumentException("indexes list length can't be greater than the stats list length, found "
          + indexes.size() + " and " + stats.size() + ", respectively");
    }

    return indexes.stream()
        .map(i -> new ColStatsObjWithSourceInfo(stats.get(i).getStatsObj().get(0),
            DEFAULT_CATALOG_NAME, table.getDbName(), table.getTableName(), partitionNames.get(i)))
        .collect(Collectors.toList());
  }

  /**
   * Creates a list of {@link ColStatsObjWithSourceInfo} for a given table, its partitions and using the given stats.
   * @param table the table the statistics information relates to
   * @param partitionNames the partition names of the given table
   * @param stats the statistics for the table partitions
   * @return a list of {@link ColStatsObjWithSourceInfo} for a given table, its partitions and using the given stats.
   */
  public static List<ColStatsObjWithSourceInfo> createColStatsObjWithSourceInfoList(
      Table table, List<String> partitionNames, List<ColumnStatistics> stats) {

    List<Integer> indexes = IntStream.range(0, stats.size())
        .boxed()
        .collect(Collectors.toList());

    return createColStatsObjWithSourceInfoList(table, partitionNames, stats, indexes);
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
