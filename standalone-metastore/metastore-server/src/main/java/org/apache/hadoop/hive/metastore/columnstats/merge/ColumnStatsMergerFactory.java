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

package org.apache.hadoop.hive.metastore.columnstats.merge;

import java.util.Objects;

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
import org.apache.hadoop.hive.metastore.columnstats.cache.TimestampColumnStatsDataInspector;

import com.google.common.base.Preconditions;

public class ColumnStatsMergerFactory {

  private ColumnStatsMergerFactory() {
  }

  /**
   * Get a statistics merger to merge the given statistics object.
   *
   * @param statsObjNew A statistics object to merger
   * @param statsObjOld A statistics object to merger
   * @return A ColumnStatsMerger object that can process the requested type
   * @throws IllegalArgumentException if the column statistics objects are of
   *           two different types or if they are of an unknown type
   * @throws NullPointerException if statistics object is {@code null}
   */
  public static ColumnStatsMerger<?> getColumnStatsMerger(final ColumnStatisticsObj statsObjNew,
      final ColumnStatisticsObj statsObjOld) {
    Objects.requireNonNull(statsObjNew, "Column 1 statistics cannot be null");
    Objects.requireNonNull(statsObjOld, "Column 2 statistics cannot be null");

    final _Fields typeNew = statsObjNew.getStatsData().getSetField();
    final _Fields typeOld = statsObjOld.getStatsData().getSetField();

    Preconditions.checkArgument(typeNew == typeOld, "The column types must match: [" + typeNew + "::" + typeOld + "]");

    switch (typeNew) {
    case BOOLEAN_STATS:
      return new BooleanColumnStatsMerger();
    case LONG_STATS:
      return new LongColumnStatsMerger();
    case DOUBLE_STATS:
      return new DoubleColumnStatsMerger();
    case STRING_STATS:
      return new StringColumnStatsMerger();
    case BINARY_STATS:
      return new BinaryColumnStatsMerger();
    case DECIMAL_STATS:
      return new DecimalColumnStatsMerger();
    case DATE_STATS:
      return new DateColumnStatsMerger();
    case TIMESTAMP_STATS:
      return new TimestampColumnStatsMerger();
    default:
      throw new IllegalArgumentException("Unknown stats type: " + statsObjNew.getStatsData().getSetField());
    }
  }

  public static ColumnStatisticsObj newColumnStaticsObj(final String colName, final String colType,
      final _Fields type) {
    final ColumnStatisticsObj cso = new ColumnStatisticsObj();
    final ColumnStatisticsData csd = new ColumnStatisticsData();

    Objects.requireNonNull(colName, "Column name cannot be null");
    Objects.requireNonNull(colType, "Column type cannot be null");
    Objects.requireNonNull(type, "Field type cannot be null");

    switch (type) {
    case BOOLEAN_STATS:
      csd.setBooleanStats(new BooleanColumnStatsData());
      break;

    case LONG_STATS:
      csd.setLongStats(new LongColumnStatsDataInspector());
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

    case DATE_STATS:
      csd.setDateStats(new DateColumnStatsDataInspector());
      break;

    case TIMESTAMP_STATS:
      csd.setTimestampStats(new TimestampColumnStatsDataInspector());
      break;

    default:
      throw new IllegalArgumentException("Unknown stats type: " + type);
    }

    cso.setColName(colName);
    cso.setColType(colType);
    cso.setStatsData(csd);

    return cso;
  }

}
