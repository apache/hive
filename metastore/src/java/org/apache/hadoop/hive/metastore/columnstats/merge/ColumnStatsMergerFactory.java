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

package org.apache.hadoop.hive.metastore.columnstats.merge;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.common.ndv.NumDistinctValueEstimatorFactory;
import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData._Fields;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;

public class ColumnStatsMergerFactory {

  private ColumnStatsMergerFactory() {
  }

  public static ColumnStatsMerger getColumnStatsMerger(ColumnStatisticsObj statsObjNew,
      ColumnStatisticsObj statsObjOld) {
    ColumnStatsMerger agg;
    _Fields typeNew = statsObjNew.getStatsData().getSetField();
    _Fields typeOld = statsObjOld.getStatsData().getSetField();
    // make sure that they have the same type
    typeNew = typeNew == typeOld ? typeNew : null;
    switch (typeNew) {
    case BOOLEAN_STATS:
      agg = new BooleanColumnStatsMerger();
      break;
    case LONG_STATS: {
      agg = new LongColumnStatsMerger();
      break;
    }
    case DOUBLE_STATS: {
      agg = new DoubleColumnStatsMerger();
      break;
    }
    case STRING_STATS: {
      agg = new StringColumnStatsMerger();
      break;
    }
    case BINARY_STATS:
      agg = new BinaryColumnStatsMerger();
      break;
    case DECIMAL_STATS: {
      agg = new DecimalColumnStatsMerger();
      break;
    }
    case DATE_STATS: {
      agg = new DateColumnStatsMerger();
      break;
    }
    default:
      throw new IllegalArgumentException("Unknown stats type " + typeNew.toString());
    }
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
      csd.setLongStats(new LongColumnStatsData());
      break;

    case DOUBLE_STATS:
      csd.setDoubleStats(new DoubleColumnStatsData());
      break;

    case STRING_STATS:
      csd.setStringStats(new StringColumnStatsData());
      break;

    case BINARY_STATS:
      csd.setBinaryStats(new BinaryColumnStatsData());
      break;

    case DECIMAL_STATS:
      csd.setDecimalStats(new DecimalColumnStatsData());
      break;

    case DATE_STATS:
      csd.setDateStats(new DateColumnStatsData());
      break;

    default:
      throw new IllegalArgumentException("Unknown stats type");
    }

    cso.setStatsData(csd);
    return cso;
  }

}
