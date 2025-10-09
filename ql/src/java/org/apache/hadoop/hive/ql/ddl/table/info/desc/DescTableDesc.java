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

package org.apache.hadoop.hive.ql.ddl.table.info.desc;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;

import com.google.common.collect.ImmutableList;

/**
 * DDL task description for DESC table_name commands.
 */
@Explain(displayName = "Describe Table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class DescTableDesc implements DDLDesc, Serializable {
  private static final long serialVersionUID = 1L;

  public static final String SCHEMA = "col_name,data_type,comment#string:string:string";
  public static final String COLUMN_STATISTICS_SCHEMA = "column_property,value#string:string";
  public static final String PARTITION_TRANSFORM_SPEC_SCHEMA = "col_name,transform_type#string:string";
  private static final List<String> PURE_STATISTICS_HEADERS = ImmutableList.of(
      "min", "max", "num_nulls", "distinct_count", "avg_col_len", "max_col_len", "num_trues",
      "num_falses", "bit_vector"
  );
  // keeping this around for backward compatibility, use "getColumnStatisticsHeaders" instead
  @SuppressWarnings("unused")
  public static final List<String> COLUMN_STATISTICS_HEADERS = ImmutableList.<String>builder()
      .add("col_name")
      .add("data_type")
      .addAll(PURE_STATISTICS_HEADERS)
      .add("comment")
      .build();

  private final String resFile;
  private final TableName tableName;
  private final Map<String, String> partitionSpec;
  private final String columnPath;
  private final boolean isExtended;
  private final boolean isFormatted;

  public DescTableDesc(Path resFile, TableName tableName, Map<String, String> partitionSpec, String columnPath,
      boolean isExtended, boolean isFormatted) {
    this.resFile = resFile.toString();
    this.tableName = tableName;
    this.partitionSpec = partitionSpec;
    this.columnPath = columnPath;
    this.isExtended = isExtended;
    this.isFormatted = isFormatted;
  }

  @Explain(displayName = "result file", explainLevels = { Level.EXTENDED })
  public String getResFile() {
    return resFile;
  }

  @Explain(displayName = "table", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getDbTableName() {
    return tableName.getNotEmptyDbTable();
  }

  public TableName getTableName() {
    return tableName;
  }

  @Explain(displayName = "partition", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public Map<String, String> getPartitionSpec() {
    return partitionSpec;
  }

  public String getColumnPath() {
    return columnPath;
  }

  @Explain(displayName = "extended", displayOnlyOnTrue = true,
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isExtended() {
    return isExtended;
  }

  @Explain(displayName = "formatted", displayOnlyOnTrue = true,
      explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public boolean isFormatted() {
    return isFormatted;
  }

  public static List<String> getColumnStatisticsHeaders(boolean histogramEnabled) {
    ImmutableList.Builder<String> builder = ImmutableList.<String>builder()
        .add("col_name")
        .add("data_type")
        .addAll(PURE_STATISTICS_HEADERS);

    if (histogramEnabled) {
      builder.add("histogram");
    }

    builder.add("comment");
    return builder.build();
  }
}
