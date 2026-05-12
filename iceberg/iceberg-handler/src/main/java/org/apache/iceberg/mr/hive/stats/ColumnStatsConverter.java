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

package org.apache.iceberg.mr.hive.stats;

import java.nio.ByteBuffer;
import java.util.stream.Collectors;
import org.apache.hadoop.hive.metastore.api.ColumnStatistics;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsDesc;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;

public final class ColumnStatsConverter {
  private static final HiveBooleanConverter BOOLEAN = new HiveBooleanConverter();
  private static final HiveLongConverter LONG = new HiveLongConverter();
  private static final HiveDoubleConverter DOUBLE = new HiveDoubleConverter();
  private static final HiveStringConverter STRING = new HiveStringConverter();
  private static final HiveBinaryConverter BINARY = new HiveBinaryConverter();
  private static final HiveDecimalConverter DECIMAL = new HiveDecimalConverter();
  private static final HiveDateConverter DATE = new HiveDateConverter();
  private static final HiveTimestampConverter TIMESTAMP = new HiveTimestampConverter();

  private ColumnStatsConverter() {

  }

  public static HiveColumnStatistics fromThrift(ColumnStatistics columnStatistics) {
    if (columnStatistics == null) {
      return null;
    }

    ColumnStatisticsDesc tDesc = columnStatistics.getStatsDesc();
    return new HiveColumnStatistics(
        new HiveColumnStatisticsDesc(tDesc.isIsTblLevel(), tDesc.getDbName(), tDesc.getTableName(),
            tDesc.getPartName(), tDesc.isSetLastAnalyzed() ? tDesc.getLastAnalyzed() : null, tDesc.getCatName()),
        columnStatistics.getStatsObj().stream().map(ColumnStatsConverter::fromThrift).collect(Collectors.toList()),
        columnStatistics.isSetIsStatsCompliant() ? columnStatistics.isIsStatsCompliant() : null,
        columnStatistics.getEngine()
    );
  }

  public static HiveColumnStatisticsObj fromThrift(ColumnStatisticsObj columnStatisticsObj) {
    return new HiveColumnStatisticsObj(
        columnStatisticsObj.getColName(),
        columnStatisticsObj.getColType(),
        fromThriftData(columnStatisticsObj.getStatsData()));
  }

  private static HiveColumnStatisticsData fromThriftData(ColumnStatisticsData columnStatisticsData) {
    if (columnStatisticsData.isSetBooleanStats()) {
      return BOOLEAN.fromThrift(columnStatisticsData.getBooleanStats());
    }

    if (columnStatisticsData.isSetLongStats()) {
      return LONG.fromThrift(columnStatisticsData.getLongStats());
    }

    if (columnStatisticsData.isSetDoubleStats()) {
      return DOUBLE.fromThrift(columnStatisticsData.getDoubleStats());
    }

    if (columnStatisticsData.isSetStringStats()) {
      return STRING.fromThrift(columnStatisticsData.getStringStats());
    }

    if (columnStatisticsData.isSetBinaryStats()) {
      return BINARY.fromThrift(columnStatisticsData.getBinaryStats());
    }

    if (columnStatisticsData.isSetDecimalStats()) {
      return DECIMAL.fromThrift(columnStatisticsData.getDecimalStats());
    }

    if (columnStatisticsData.isSetDateStats()) {
      return DATE.fromThrift(columnStatisticsData.getDateStats());
    }

    if (columnStatisticsData.isSetTimestampStats()) {
      return TIMESTAMP.fromThrift(columnStatisticsData.getTimestampStats());
    }

    throw new UnsupportedOperationException("Unsupported Hive Stat Type");
  }

  public static ColumnStatistics toThrift(HiveColumnStatistics record) {
    if (record == null) {
      return null;
    }

    ColumnStatistics result = new ColumnStatistics();

    if (record.statsDesc() != null) {
      result.setStatsDesc(toThrift(record.statsDesc()));
    }

    if (record.statsObj() != null) {
      result.setStatsObj(record.statsObj().stream()
          .map(ColumnStatsConverter::toThrift)
          .collect(java.util.stream.Collectors.toList()));
    }

    if (record.isStatsCompliant() != null) {
      result.setIsStatsCompliant(record.isStatsCompliant());
    }

    result.setEngine(record.engine());

    return result;
  }

  private static ColumnStatisticsDesc toThrift(HiveColumnStatisticsDesc columnStatisticsDesc) {
    var result = new ColumnStatisticsDesc(
        columnStatisticsDesc.isTblLevel(),
        columnStatisticsDesc.dbName(),
        columnStatisticsDesc.tableName());

    if (columnStatisticsDesc.partName() != null) {
      result.setPartName(columnStatisticsDesc.partName());
    }
    if (columnStatisticsDesc.lastAnalyzed() != null) {
      result.setLastAnalyzed(columnStatisticsDesc.lastAnalyzed());
    }
    if (columnStatisticsDesc.catName() != null) {
      result.setCatName(columnStatisticsDesc.catName());
    }

    return result;
  }

  public static ColumnStatisticsObj toThrift(HiveColumnStatisticsObj columnStatisticsObj) {
    return new ColumnStatisticsObj(
        columnStatisticsObj.colName(),
        columnStatisticsObj.colType(),
        toThrift(columnStatisticsObj.statsData())
    );
  }

  private static ColumnStatisticsData toThrift(HiveColumnStatisticsData columnStatisticsData) {
    ColumnStatisticsData result = new ColumnStatisticsData();
    switch (columnStatisticsData) {
      case HiveBooleanStats statsValue -> result.setBooleanStats(BOOLEAN.toThrift(statsValue));
      case HiveLongStats statsValue -> result.setLongStats(LONG.toThrift(statsValue));
      case HiveDoubleStats statsValue -> result.setDoubleStats(DOUBLE.toThrift(statsValue));
      case HiveStringStats statsValue -> result.setStringStats(STRING.toThrift(statsValue));
      case HiveBinaryStats statsValue -> result.setBinaryStats(BINARY.toThrift(statsValue));
      case HiveDecimalStats statsValue -> result.setDecimalStats(DECIMAL.toThrift(statsValue));
      case HiveDateStats statsValue -> result.setDateStats(DATE.toThrift(statsValue));
      case HiveTimestampStats statsValue -> result.setTimestampStats(TIMESTAMP.toThrift(statsValue));
    }
    return result;
  }

  public static byte[] toBytes(ByteBuffer byteBuffer) {
    if (byteBuffer == null) {
      return null;
    }

    byte[] arr = new byte[byteBuffer.remaining()];
    byteBuffer.duplicate().get(arr);
    return arr;
  }
}
