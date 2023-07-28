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

package org.apache.hadoop.hive.metastore.columnstats;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.columnstats.cache.DateColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DecimalColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.DoubleColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.LongColumnStatsDataInspector;
import org.apache.hadoop.hive.metastore.columnstats.cache.StringColumnStatsDataInspector;

/**
 * Utils class for columnstats package.
 */
public final class ColumnsStatsUtils {

  private ColumnsStatsUtils(){}

  /**
   * Convertes to DateColumnStatsDataInspector if it's a DateColumnStatsData.
   * @param cso ColumnStatisticsObj
   * @return DateColumnStatsDataInspector
   */
  public static DateColumnStatsDataInspector dateInspectorFromStats(ColumnStatisticsObj cso) {
    DateColumnStatsDataInspector dateColumnStats;
    if (cso.getStatsData().getDateStats() instanceof DateColumnStatsDataInspector) {
      dateColumnStats =
          (DateColumnStatsDataInspector)(cso.getStatsData().getDateStats());
    } else {
      dateColumnStats = new DateColumnStatsDataInspector(cso.getStatsData().getDateStats());
    }
    return dateColumnStats;
  }

  /**
   * Convertes to StringColumnStatsDataInspector
   * if it's a StringColumnStatsData.
   * @param cso ColumnStatisticsObj
   * @return StringColumnStatsDataInspector
   */
  public static StringColumnStatsDataInspector stringInspectorFromStats(ColumnStatisticsObj cso) {
    StringColumnStatsDataInspector columnStats;
    if (cso.getStatsData().getStringStats() instanceof StringColumnStatsDataInspector) {
      columnStats =
          (StringColumnStatsDataInspector)(cso.getStatsData().getStringStats());
    } else {
      columnStats = new StringColumnStatsDataInspector(cso.getStatsData().getStringStats());
    }
    return columnStats;
  }

  /**
   * Convertes to LongColumnStatsDataInspector if it's a LongColumnStatsData.
   * @param cso ColumnStatisticsObj
   * @return LongColumnStatsDataInspector
   */
  public static LongColumnStatsDataInspector longInspectorFromStats(ColumnStatisticsObj cso) {
    LongColumnStatsDataInspector columnStats;
    if (cso.getStatsData().getLongStats() instanceof LongColumnStatsDataInspector) {
      columnStats =
          (LongColumnStatsDataInspector)(cso.getStatsData().getLongStats());
    } else {
      columnStats = new LongColumnStatsDataInspector(cso.getStatsData().getLongStats());
    }
    return columnStats;
  }

  /**
   * Convertes to DoubleColumnStatsDataInspector
   * if it's a DoubleColumnStatsData.
   * @param cso ColumnStatisticsObj
   * @return DoubleColumnStatsDataInspector
   */
  public static DoubleColumnStatsDataInspector doubleInspectorFromStats(ColumnStatisticsObj cso) {
    DoubleColumnStatsDataInspector columnStats;
    if (cso.getStatsData().getDoubleStats() instanceof DoubleColumnStatsDataInspector) {
      columnStats =
          (DoubleColumnStatsDataInspector)(cso.getStatsData().getDoubleStats());
    } else {
      columnStats = new DoubleColumnStatsDataInspector(cso.getStatsData().getDoubleStats());
    }
    return columnStats;
  }

  /**
   * Convertes to DecimalColumnStatsDataInspector
   * if it's a DecimalColumnStatsData.
   * @param cso ColumnStatisticsObj
   * @return DecimalColumnStatsDataInspector
   */
  public static DecimalColumnStatsDataInspector decimalInspectorFromStats(ColumnStatisticsObj cso) {
    DecimalColumnStatsDataInspector columnStats;
    if (cso.getStatsData().getDecimalStats() instanceof DecimalColumnStatsDataInspector) {
      columnStats =
          (DecimalColumnStatsDataInspector)(cso.getStatsData().getDecimalStats());
    } else {
      columnStats = new DecimalColumnStatsDataInspector(cso.getStatsData().getDecimalStats());
    }
    return columnStats;
  }
}
