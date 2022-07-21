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

package org.apache.hadoop.hive.ql.ddl.table.info.desc.formatter;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.metadata.CheckConstraint;
import org.apache.hadoop.hive.ql.metadata.DefaultConstraint;
import org.apache.hadoop.hive.ql.metadata.ForeignKeyInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.NotNullConstraint;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.PrimaryKeyInfo;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.UniqueConstraint;
import org.apache.hadoop.hive.ql.metadata.formatting.MapBuilder;
import org.apache.hadoop.hive.ql.parse.TransformSpec;

import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Formats DESC TABLE results to json format.
 */
public class JsonDescTableFormatter extends DescTableFormatter {
  private static final String COLUMN_NAME = "name";
  private static final String COLUMN_TYPE = "type";
  private static final String COLUMN_COMMENT = "comment";
  private static final String COLUMN_MIN = "min";
  private static final String COLUMN_MAX = "max";
  private static final String COLUMN_NUM_NULLS = "numNulls";
  private static final String COLUMN_NUM_TRUES = "numTrues";
  private static final String COLUMN_NUM_FALSES = "numFalses";
  private static final String COLUMN_DISTINCT_COUNT = "distinctCount";
  private static final String COLUMN_AVG_LENGTH = "avgColLen";
  private static final String COLUMN_MAX_LENGTH = "maxColLen";

  @Override
  public void describeTable(HiveConf conf, DataOutputStream out, String columnPath, String tableName, Table table,
      Partition partition, List<FieldSchema> columns, boolean isFormatted, boolean isExtended, boolean isOutputPadded,
      List<ColumnStatisticsObj> columnStats) throws HiveException {
    MapBuilder builder = MapBuilder.create();
    builder.put("columns", createColumnsInfo(columns, columnStats));

    if (isExtended) {
      addExtendedInfo(table, partition, builder);
    }

    ShowUtils.asJson(out, builder.build());
  }

  public static List<Map<String, Object>> createColumnsInfo(List<FieldSchema> columns,
      List<ColumnStatisticsObj> columnStatisticsList) {
    List<Map<String, Object>> columnsInfo = new ArrayList<>(columns.size());
    for (FieldSchema column : columns) {
      ColumnStatisticsData statistics = getStatistics(column, columnStatisticsList);
      columnsInfo.add(createColumnInfo(column, statistics));
    }
    return columnsInfo;
  }

  private static ColumnStatisticsData getStatistics(FieldSchema column,
      List<ColumnStatisticsObj> columnStatisticsList) {
    for (ColumnStatisticsObj columnStatistics : columnStatisticsList) {
      if (column.getName().equals(columnStatistics.getColName())) {
        return columnStatistics.getStatsData();
      }
    }

    return null;
  }

  private static Map<String, Object> createColumnInfo(FieldSchema column, ColumnStatisticsData statistics) {
    Map<String, Object> result = MapBuilder.create()
        .put(COLUMN_NAME, column.getName())
        .put(COLUMN_TYPE, column.getType())
        .put(COLUMN_COMMENT, column.getComment())
        .build();

    if (statistics != null) {
      if (statistics.isSetBinaryStats()) {
        addBinaryStats(statistics, result);
      } else if (statistics.isSetStringStats()) {
        addStringStats(statistics, result);
      } else if (statistics.isSetBooleanStats()) {
        addBooleansStats(statistics, result);
      } else if (statistics.isSetDecimalStats()) {
        addDecimalStats(statistics, result);
      } else if (statistics.isSetDoubleStats()) {
        addDoubleStats(statistics, result);
      } else if (statistics.isSetLongStats()) {
        addLongStats(statistics, result);
      } else if (statistics.isSetDateStats()) {
        addDateStats(statistics, result);
      } else if (statistics.isSetTimestampStats()) {
        addTimeStampStats(statistics, result);
      }
    }

    return result;
  }

  private static void addBinaryStats(ColumnStatisticsData statistics, Map<String, Object> result) {
    if (statistics.getBinaryStats().isSetNumNulls()) {
      result.put(COLUMN_NUM_NULLS, statistics.getBinaryStats().getNumNulls());
    }
    if (statistics.getBinaryStats().isSetAvgColLen()) {
      result.put(COLUMN_AVG_LENGTH, statistics.getBinaryStats().getAvgColLen());
    }
    if (statistics.getBinaryStats().isSetMaxColLen()) {
      result.put(COLUMN_MAX_LENGTH, statistics.getBinaryStats().getMaxColLen());
    }
  }

  private static void addStringStats(ColumnStatisticsData statistics, Map<String, Object> result) {
    if (statistics.getStringStats().isSetNumNulls()) {
      result.put(COLUMN_NUM_NULLS, statistics.getStringStats().getNumNulls());
    }
    if (statistics.getStringStats().isSetNumDVs()) {
      result.put(COLUMN_DISTINCT_COUNT, statistics.getStringStats().getNumDVs());
    }
    if (statistics.getStringStats().isSetAvgColLen()) {
      result.put(COLUMN_AVG_LENGTH, statistics.getStringStats().getAvgColLen());
    }
    if (statistics.getStringStats().isSetMaxColLen()) {
      result.put(COLUMN_MAX_LENGTH, statistics.getStringStats().getMaxColLen());
    }
  }

  private static void addBooleansStats(ColumnStatisticsData statistics, Map<String, Object> result) {
    if (statistics.getBooleanStats().isSetNumNulls()) {
      result.put(COLUMN_NUM_NULLS, statistics.getBooleanStats().getNumNulls());
    }
    if (statistics.getBooleanStats().isSetNumTrues()) {
      result.put(COLUMN_NUM_TRUES, statistics.getBooleanStats().getNumTrues());
    }
    if (statistics.getBooleanStats().isSetNumFalses()) {
      result.put(COLUMN_NUM_FALSES, statistics.getBooleanStats().getNumFalses());
    }
  }

  private static void addDecimalStats(ColumnStatisticsData statistics, Map<String, Object> result) {
    if (statistics.getDecimalStats().isSetLowValue()) {
      result.put(COLUMN_MIN, ShowUtils.convertToString(statistics.getDecimalStats().getLowValue()));
    }
    if (statistics.getDecimalStats().isSetHighValue()) {
      result.put(COLUMN_MAX, ShowUtils.convertToString(statistics.getDecimalStats().getHighValue()));
    }
    if (statistics.getDecimalStats().isSetNumNulls()) {
      result.put(COLUMN_NUM_NULLS, statistics.getDecimalStats().getNumNulls());
    }
    if (statistics.getDecimalStats().isSetNumDVs()) {
      result.put(COLUMN_DISTINCT_COUNT, statistics.getDecimalStats().getNumDVs());
    }
  }

  private static void addDoubleStats(ColumnStatisticsData statistics, Map<String, Object> result) {
    if (statistics.getDoubleStats().isSetLowValue()) {
      result.put(COLUMN_MIN, statistics.getDoubleStats().getLowValue());
    }
    if (statistics.getDoubleStats().isSetHighValue()) {
      result.put(COLUMN_MAX, statistics.getDoubleStats().getHighValue());
    }
    if (statistics.getDoubleStats().isSetNumNulls()) {
      result.put(COLUMN_NUM_NULLS, statistics.getDoubleStats().getNumNulls());
    }
    if (statistics.getDoubleStats().isSetNumDVs()) {
      result.put(COLUMN_DISTINCT_COUNT, statistics.getDoubleStats().getNumDVs());
    }
  }

  private static void addLongStats(ColumnStatisticsData statistics, Map<String, Object> result) {
    if (statistics.getLongStats().isSetLowValue()) {
      result.put(COLUMN_MIN, statistics.getLongStats().getLowValue());
    }
    if (statistics.getLongStats().isSetHighValue()) {
      result.put(COLUMN_MAX, statistics.getLongStats().getHighValue());
    }
    if (statistics.getLongStats().isSetNumNulls()) {
      result.put(COLUMN_NUM_NULLS, statistics.getLongStats().getNumNulls());
    }
    if (statistics.getLongStats().isSetNumDVs()) {
      result.put(COLUMN_DISTINCT_COUNT, statistics.getLongStats().getNumDVs());
    }
  }

  private static void addDateStats(ColumnStatisticsData statistics, Map<String, Object> result) {
    if (statistics.getDateStats().isSetLowValue()) {
      result.put(COLUMN_MIN, ShowUtils.convertToString(statistics.getDateStats().getLowValue()));
    }
    if (statistics.getDateStats().isSetHighValue()) {
      result.put(COLUMN_MAX, ShowUtils.convertToString(statistics.getDateStats().getHighValue()));
    }
    if (statistics.getDateStats().isSetNumNulls()) {
      result.put(COLUMN_NUM_NULLS, statistics.getDateStats().getNumNulls());
    }
    if (statistics.getDateStats().isSetNumDVs()) {
      result.put(COLUMN_DISTINCT_COUNT, statistics.getDateStats().getNumDVs());
    }
  }

  private static void addTimeStampStats(ColumnStatisticsData statistics, Map<String, Object> result) {
    if (statistics.getTimestampStats().isSetLowValue()) {
      result.put(COLUMN_MIN, ShowUtils.convertToString(statistics.getTimestampStats().getLowValue()));
    }
    if (statistics.getTimestampStats().isSetHighValue()) {
      result.put(COLUMN_MAX, ShowUtils.convertToString(statistics.getTimestampStats().getHighValue()));
    }
    if (statistics.getTimestampStats().isSetNumNulls()) {
      result.put(COLUMN_NUM_NULLS, statistics.getTimestampStats().getNumNulls());
    }
    if (statistics.getTimestampStats().isSetNumDVs()) {
      result.put(COLUMN_DISTINCT_COUNT, statistics.getTimestampStats().getNumDVs());
    }
  }

  private void addExtendedInfo(Table table, Partition partition, MapBuilder builder) {
    if (partition != null) {
      builder.put("partitionInfo", partition.getTPartition());
    } else {
      builder.put("tableInfo", table.getTTable());
    }
    if (table.isNonNative() && table.getStorageHandler() != null &&
        table.getStorageHandler().supportsPartitionTransform()) {
      List<TransformSpec> specs = table.getStorageHandler().getPartitionTransformSpec(table);
      if (!specs.isEmpty()) {
        builder.put("partitionSpecInfo", specs.stream().map(s -> {
          Map<String, String> result = new LinkedHashMap<>();
          result.put("column_name", s.getColumnName());
          result.put("transform_type", s.getTransformType().name());
          if (s.getTransformParam().isPresent()) {
            result.put("transform_param", String.valueOf(s.getTransformParam().get()));
          }
          return result;
        }).collect(Collectors.toList()));
      }
    }
    if (PrimaryKeyInfo.isNotEmpty(table.getPrimaryKeyInfo())) {
      builder.put("primaryKeyInfo", table.getPrimaryKeyInfo());
    }
    if (ForeignKeyInfo.isNotEmpty(table.getForeignKeyInfo())) {
      builder.put("foreignKeyInfo", table.getForeignKeyInfo());
    }
    if (UniqueConstraint.isNotEmpty(table.getUniqueKeyInfo())) {
      builder.put("uniqueConstraintInfo", table.getUniqueKeyInfo());
    }
    if (NotNullConstraint.isNotEmpty(table.getNotNullConstraint())) {
      builder.put("notNullConstraintInfo", table.getNotNullConstraint());
    }
    if (DefaultConstraint.isNotEmpty(table.getDefaultConstraint())) {
      builder.put("defaultConstraintInfo", table.getDefaultConstraint());
    }
    if (CheckConstraint.isNotEmpty(table.getCheckConstraint())) {
      builder.put("checkConstraintInfo", table.getCheckConstraint());
    }
    if (table.getStorageHandlerInfo() != null) {
      builder.put("storageHandlerInfo", table.getStorageHandlerInfo().toString());
    }
  }
}
