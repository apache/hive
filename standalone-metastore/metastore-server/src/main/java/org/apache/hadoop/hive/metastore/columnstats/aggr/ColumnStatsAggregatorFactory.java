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

package org.apache.hadoop.hive.metastore.columnstats.aggr;

import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData._Fields;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.columnstats.cache.DateColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DecimalColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DoubleColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.LongColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.StringColumnStatsDataInspector;

public class ColumnStatsAggregatorFactory {

  private ColumnStatsAggregatorFactory() {
  }

  public static ColumnStatsAggregator getColumnStatsAggregator(_Fields type,
      boolean useDensityFunctionForNDVEstimation, double ndvTuner) {
    ColumnStatsAggregator agg;
    switch (type) {
    case BOOLEAN_STATS:
      agg = new BooleanColumnStatsAggregator();
      break;
    case LONG_STATS:
      agg = new LongColumnStatsAggregator();
      break;
    case DATE_STATS:
      agg = new DateColumnStatsAggregator();
      break;
    case DOUBLE_STATS:
      agg = new DoubleColumnStatsAggregator();
      break;
    case STRING_STATS:
      agg = new StringColumnStatsAggregator();
      break;
    case BINARY_STATS:
      agg = new BinaryColumnStatsAggregator();
      break;
    case DECIMAL_STATS:
      agg = new DecimalColumnStatsAggregator();
      break;
    default:
      throw new RuntimeException("Woh, bad.  Unknown stats type " + type.toString());
    }
    agg.useDensityFunctionForNDVEstimation = useDensityFunctionForNDVEstimation;
    agg.ndvTuner = ndvTuner;
    return agg;
  }

  public static ColumnStatisticsObj newColumnStaticsObj(String colName, String colType, _Fields type) {
    ColumnStatisticsObj cso = new ColumnStatisticsObj();
    ColumnStatisticsData csd = new ColumnStatisticsData();
    cso.setColName(colName);
    cso.setColType(colType);
    switch (type) {
    case BOOLEAN_STATS:
      csd.setBooleanStats(new BooleanColumnStatsData());
      break;

    case LONG_STATS:
      csd.setLongStats(new LongColumnStatsDataInspector());
      break;

    case DATE_STATS:
      csd.setDateStats(new DateColumnStatsDataInspector());
      break;

    case DOUBLE_STATS:
      csd.setDoubleStats(new DoubleColumnStatsDataInspector());
      break;

    case STRING_STATS:
      csd.setStringStats(new StringColumnStatsDataInspector());
      break;

    case BINARY_STATS:
      csd.setBinaryStats(new BinaryColumnStatsData());
      break;

    case DECIMAL_STATS:
      csd.setDecimalStats(new DecimalColumnStatsDataInspector());
      break;

    default:
      throw new RuntimeException("Woh, bad.  Unknown stats type!");
    }

    cso.setStatsData(csd);
    return cso;
  }

}
