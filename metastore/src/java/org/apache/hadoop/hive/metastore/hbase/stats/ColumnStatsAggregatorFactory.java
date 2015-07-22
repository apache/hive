/**
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

package org.apache.hadoop.hive.metastore.hbase.stats;

import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData._Fields;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;

public class ColumnStatsAggregatorFactory {

  private ColumnStatsAggregatorFactory() {
  }

  public static ColumnStatsAggregator getColumnStatsAggregator(_Fields type) {
    switch (type) {
    case BOOLEAN_STATS:
      return new BooleanColumnStatsAggregator();
    case LONG_STATS:
      return new LongColumnStatsAggregator();
    case DOUBLE_STATS:
      return new DoubleColumnStatsAggregator();
    case STRING_STATS:
      return new StringColumnStatsAggregator();
    case BINARY_STATS:
      return new BinaryColumnStatsAggregator();
    case DECIMAL_STATS:
      return new DecimalColumnStatsAggregator();
    default:
      throw new RuntimeException("Woh, bad.  Unknown stats type " + type.toString());
    }
  }

  public static ColumnStatisticsObj newColumnStaticsObj(String colName, _Fields type) {
    ColumnStatisticsObj cso = new ColumnStatisticsObj();
    ColumnStatisticsData csd = new ColumnStatisticsData();
    cso.setColName(colName);
    switch (type) {
    case BOOLEAN_STATS:
      csd.setBooleanStats(new BooleanColumnStatsData());
      cso.setColType("boolean");
      break;

    case LONG_STATS:
      csd.setLongStats(new LongColumnStatsData());
      cso.setColType("long");
      break;

    case DOUBLE_STATS:
      csd.setDoubleStats(new DoubleColumnStatsData());
      cso.setColType("double");
      break;

    case STRING_STATS:
      csd.setStringStats(new StringColumnStatsData());
      cso.setColType("string");
      break;

    case BINARY_STATS:
      csd.setBinaryStats(new BinaryColumnStatsData());
      cso.setColType("binary");
      break;

    case DECIMAL_STATS:
      csd.setDecimalStats(new DecimalColumnStatsData());
      cso.setColType("decimal");
      break;

    default:
      throw new RuntimeException("Woh, bad.  Unknown stats type!");
    }

    cso.setStatsData(csd);
    return cso;
  }

}
