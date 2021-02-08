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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.ddl.ShowUtils;
import org.apache.hadoop.hive.ql.ddl.ShowUtils.TextMetaDataTable;
import org.apache.hadoop.hive.ql.ddl.table.info.desc.DescTableDesc;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.metadata.CheckConstraint;
import org.apache.hadoop.hive.ql.metadata.CheckConstraint.CheckConstraintCol;
import org.apache.hadoop.hive.ql.metadata.DefaultConstraint;
import org.apache.hadoop.hive.ql.metadata.DefaultConstraint.DefaultConstraintCol;
import org.apache.hadoop.hive.ql.metadata.ForeignKeyInfo;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.NotNullConstraint;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.PrimaryKeyInfo;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.metadata.UniqueConstraint;
import org.apache.hadoop.hive.ql.metadata.ForeignKeyInfo.ForeignKeyCol;
import org.apache.hadoop.hive.ql.metadata.UniqueConstraint.UniqueConstraintCol;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hive.common.util.HiveStringUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.TABLE_IS_CTAS;
import static org.apache.hadoop.hive.ql.ddl.ShowUtils.ALIGNMENT;
import static org.apache.hadoop.hive.ql.ddl.ShowUtils.DEFAULT_STRINGBUILDER_SIZE;
import static org.apache.hadoop.hive.ql.ddl.ShowUtils.FIELD_DELIM;
import static org.apache.hadoop.hive.ql.ddl.ShowUtils.LINE_DELIM;
import static org.apache.hadoop.hive.ql.ddl.ShowUtils.formatOutput;

/**
 * Formats DESC TABLE results to text format.
 */
class TextDescTableFormatter extends DescTableFormatter {
  @Override
  public void describeTable(HiveConf conf, DataOutputStream out, String columnPath, String tableName, Table table,
      Partition partition, List<FieldSchema> columns, boolean isFormatted, boolean isExtended, boolean isOutputPadded,
      List<ColumnStatisticsObj> columnStats) throws HiveException {
    try {
      addStatsData(out, columnPath, columns, isFormatted, columnStats, isOutputPadded);
      addPartitionData(out, conf, columnPath, table, isFormatted, isOutputPadded);

      if (columnPath == null) {
        if (isFormatted) {
          addFormattedTableData(out, table, partition, isOutputPadded);
        }

        if (isExtended) {
          out.write(Utilities.newLineCode);
          addExtendedTableData(out, table, partition);
          addExtendedConstraintData(out, table);
          addExtendedStorageData(out, table);
        }
      }
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  private void addStatsData(DataOutputStream out, String columnPath, List<FieldSchema> columns, boolean isFormatted,
      List<ColumnStatisticsObj> columnStats, boolean isOutputPadded) throws IOException {
    String statsData = "";
    
    TextMetaDataTable metaDataTable = new TextMetaDataTable();
    boolean needColStats = isFormatted && columnPath != null;
    if (needColStats) {
      metaDataTable.addRow(DescTableDesc.COLUMN_STATISTICS_HEADERS.toArray(new String[0]));
    } else if (isFormatted && !SessionState.get().isHiveServerQuery()) {
      statsData += "# ";
      metaDataTable.addRow(DescTableDesc.SCHEMA.split("#")[0].split(","));
    }
    for (FieldSchema column : columns) {
      metaDataTable.addRow(ShowUtils.extractColumnValues(column, needColStats,
          getColumnStatisticsObject(column.getName(), column.getType(), columnStats)));
    }
    if (needColStats) {
      metaDataTable.transpose();
    }
    statsData += metaDataTable.renderTable(isOutputPadded);
    out.write(statsData.getBytes(StandardCharsets.UTF_8));
  }

  private ColumnStatisticsObj getColumnStatisticsObject(String columnName, String columnType,
      List<ColumnStatisticsObj> columnStats) {
    if (CollectionUtils.isNotEmpty(columnStats)) {
      for (ColumnStatisticsObj columnStat : columnStats) {
        if (columnStat.getColName().equalsIgnoreCase(columnName) &&
            columnStat.getColType().equalsIgnoreCase(columnType)) {
          return columnStat;
        }
      }
    }
    return null;
  }

  private void addPartitionData(DataOutputStream out, HiveConf conf, String columnPath, Table table,
      boolean isFormatted, boolean isOutputPadded) throws IOException {
    String partitionData = "";
    if (columnPath == null) {
      List<FieldSchema> partitionColumns = table.isPartitioned() ? table.getPartCols() : null;
      if (CollectionUtils.isNotEmpty(partitionColumns) &&
          conf.getBoolVar(ConfVars.HIVE_DISPLAY_PARTITION_COLUMNS_SEPARATELY)) {
        TextMetaDataTable metaDataTable = new TextMetaDataTable();
        partitionData += LINE_DELIM + "# Partition Information" + LINE_DELIM + "# ";
        metaDataTable.addRow(DescTableDesc.SCHEMA.split("#")[0].split(","));
        for (FieldSchema partitionColumn : partitionColumns) {
          metaDataTable.addRow(ShowUtils.extractColumnValues(partitionColumn, false, null));
        }
        partitionData += metaDataTable.renderTable(isOutputPadded);
      }
    } else {
      String statsState = table.getParameters().get(StatsSetupConst.COLUMN_STATS_ACCURATE);
      if (table.getParameters() != null && statsState != null) {
        StringBuilder stringBuilder = new StringBuilder();
        formatOutput(StatsSetupConst.COLUMN_STATS_ACCURATE,
            isFormatted ? StringEscapeUtils.escapeJava(statsState) : HiveStringUtils.escapeJava(statsState),
            stringBuilder, isOutputPadded);
        partitionData += stringBuilder.toString();
      }
    }
    out.write(partitionData.getBytes(StandardCharsets.UTF_8));
  }

  private void addFormattedTableData(DataOutputStream out, Table table, Partition partition, boolean isOutputPadded)
      throws IOException, UnsupportedEncodingException {
    String formattedTableInfo = null;
    if (partition != null) {
      formattedTableInfo = getPartitionInformation(partition);
    } else {
      formattedTableInfo = getTableInformation(table, isOutputPadded);
    }

    if (table.getTableConstraintsInfo().isTableConstraintsInfoNotEmpty()) {
      formattedTableInfo += getConstraintsInformation(table);
    }
    out.write(formattedTableInfo.getBytes(StandardCharsets.UTF_8));
  }

  private String getTableInformation(Table table, boolean isOutputPadded) {
    StringBuilder tableInfo = new StringBuilder(DEFAULT_STRINGBUILDER_SIZE);
    tableInfo.append(LINE_DELIM).append("# Detailed Table Information").append(LINE_DELIM);
    getTableMetaDataInformation(tableInfo, table, isOutputPadded);

    tableInfo.append(LINE_DELIM).append("# Storage Information").append(LINE_DELIM);
    getStorageDescriptorInfo(tableInfo, table.getTTable().getSd());

    if (table.isView() || table.isMaterializedView()) {
      String viewInfoTitle = "# " + (table.isView() ? "" : "Materialized ") + "View Information";
      tableInfo.append(LINE_DELIM).append(viewInfoTitle).append(LINE_DELIM);
      getViewInfo(tableInfo, table);
    }

    return tableInfo.toString();
  }

  private String getPartitionInformation(Partition partition) {
    StringBuilder tableInfo = new StringBuilder(DEFAULT_STRINGBUILDER_SIZE);
    tableInfo.append(LINE_DELIM).append("# Detailed Partition Information").append(LINE_DELIM);
    getPartitionMetaDataInformation(tableInfo, partition);

    if (partition.getTable().getTableType() != TableType.VIRTUAL_VIEW) {
      tableInfo.append(LINE_DELIM).append("# Storage Information").append(LINE_DELIM);
      getStorageDescriptorInfo(tableInfo, partition.getTPartition().getSd());
    }

    return tableInfo.toString();
  }

  private void getViewInfo(StringBuilder tableInfo, Table table) {
    formatOutput("Original Query:", table.getViewOriginalText(), tableInfo);
    formatOutput("Expanded Query:", table.getViewExpandedText(), tableInfo);
    if (table.isMaterializedView()) {
      formatOutput("Rewrite Enabled:", table.isRewriteEnabled() ? "Yes" : "No", tableInfo);
      formatOutput("Outdated for Rewriting:", table.isOutdatedForRewriting() == null ? "Unknown"
          : table.isOutdatedForRewriting() ? "Yes" : "No", tableInfo);
    }
  }

  private void getStorageDescriptorInfo(StringBuilder tableInfo, StorageDescriptor storageDesc) {
    formatOutput("SerDe Library:", storageDesc.getSerdeInfo().getSerializationLib(), tableInfo);
    formatOutput("InputFormat:", storageDesc.getInputFormat(), tableInfo);
    formatOutput("OutputFormat:", storageDesc.getOutputFormat(), tableInfo);
    formatOutput("Compressed:", storageDesc.isCompressed() ? "Yes" : "No", tableInfo);
    formatOutput("Num Buckets:", String.valueOf(storageDesc.getNumBuckets()), tableInfo);
    formatOutput("Bucket Columns:", storageDesc.getBucketCols().toString(), tableInfo);
    formatOutput("Sort Columns:", storageDesc.getSortCols().toString(), tableInfo);

    if (storageDesc.isStoredAsSubDirectories()) {
      formatOutput("Stored As SubDirectories:", "Yes", tableInfo);
    }

    if (storageDesc.getSkewedInfo() != null) {
      if (CollectionUtils.isNotEmpty(storageDesc.getSkewedInfo().getSkewedColNames())) {
        List<String> skewedCoumnNames =
            storageDesc.getSkewedInfo().getSkewedColNames().stream()
              .sorted()
              .collect(Collectors.toList());
        formatOutput("Skewed Columns:", skewedCoumnNames.toString(), tableInfo);
      }

      if (CollectionUtils.isNotEmpty(storageDesc.getSkewedInfo().getSkewedColValues())) {
        List<List<String>> skewedColumnValues =
            storageDesc.getSkewedInfo().getSkewedColValues().stream()
              .sorted(new VectorComparator<String>())
              .collect(Collectors.toList());
        formatOutput("Skewed Values:", skewedColumnValues.toString(), tableInfo);
      }

      Map<List<String>, String> skewedColMap = new TreeMap<>(new VectorComparator<String>());
      skewedColMap.putAll(storageDesc.getSkewedInfo().getSkewedColValueLocationMaps());
      if (MapUtils.isNotEmpty(skewedColMap)) {
        formatOutput("Skewed Value to Path:", skewedColMap.toString(), tableInfo);
        Map<List<String>, String> truncatedSkewedColMap =
            new TreeMap<List<String>, String>(new VectorComparator<String>());
        // walk through existing map to truncate path so that test won't mask it then we can verify location is right
        Set<Entry<List<String>, String>> entries = skewedColMap.entrySet();
        for (Entry<List<String>, String> entry : entries) {
          truncatedSkewedColMap.put(entry.getKey(), PlanUtils.removePrefixFromWarehouseConfig(entry.getValue()));
        }
        formatOutput("Skewed Value to Truncated Path:", truncatedSkewedColMap.toString(), tableInfo);
      }
    }

    if (storageDesc.getSerdeInfo().getParametersSize() > 0) {
      tableInfo.append("Storage Desc Params:").append(LINE_DELIM);
      displayAllParameters(storageDesc.getSerdeInfo().getParameters(), tableInfo);
    }
  }

  private void getTableMetaDataInformation(StringBuilder tableInfo, Table table, boolean isOutputPadded) {
    formatOutput("Database:", table.getDbName(), tableInfo);
    formatOutput("OwnerType:", (table.getOwnerType() != null) ? table.getOwnerType().name() : "null", tableInfo);
    formatOutput("Owner:", table.getOwner(), tableInfo);
    formatOutput("CreateTime:", formatDate(table.getTTable().getCreateTime()), tableInfo);
    formatOutput("LastAccessTime:", formatDate(table.getTTable().getLastAccessTime()), tableInfo);
    formatOutput("Retention:", Integer.toString(table.getRetention()), tableInfo);
    
    if (!table.isView()) {
      formatOutput("Location:", table.getDataLocation().toString(), tableInfo);
    }
    formatOutput("Table Type:", table.getTableType().name(), tableInfo);

    if (table.getParameters().size() > 0) {
      tableInfo.append("Table Parameters:").append(LINE_DELIM);
      displayAllParameters(table.getParameters(), tableInfo, false, isOutputPadded);
    }
  }

  private void getPartitionMetaDataInformation(StringBuilder tableInfo, Partition partition) {
    formatOutput("Partition Value:", partition.getValues().toString(), tableInfo);
    formatOutput("Database:", partition.getTPartition().getDbName(), tableInfo);
    formatOutput("Table:", partition.getTable().getTableName(), tableInfo);
    formatOutput("CreateTime:", formatDate(partition.getTPartition().getCreateTime()), tableInfo);
    formatOutput("LastAccessTime:", formatDate(partition.getTPartition().getLastAccessTime()), tableInfo);
    formatOutput("Location:", partition.getLocation(), tableInfo);

    if (partition.getTPartition().getParameters().size() > 0) {
      tableInfo.append("Partition Parameters:").append(LINE_DELIM);
      displayAllParameters(partition.getTPartition().getParameters(), tableInfo);
    }
  }

  private class VectorComparator<T extends Comparable<T>>  implements Comparator<List<T>>{
    @Override
    public int compare(List<T> listA, List<T> listB) {
      for (int i = 0; i < listA.size() && i < listB.size(); i++) {
        T valA = listA.get(i);
        T valB = listB.get(i);
        if (valA != null) {
          int ret = valA.compareTo(valB);
          if (ret != 0) {
            return ret;
          }
        } else {
          if (valB != null) {
            return -1;
          }
        }
      }
      return Integer.compare(listA.size(), listB.size());
    }
  }

  private String formatDate(long timeInSeconds) {
    if (timeInSeconds != 0) {
      Date date = new Date(timeInSeconds * 1000);
      return date.toString();
    }
    return "UNKNOWN";
  }

  private void displayAllParameters(Map<String, String> params, StringBuilder tableInfo) {
    displayAllParameters(params, tableInfo, true, false);
  }

  private void displayAllParameters(Map<String, String> params, StringBuilder tableInfo, boolean escapeUnicode,
      boolean isOutputPadded) {
    List<String> keys = new ArrayList<String>(params.keySet());
    Collections.sort(keys);
    for (String key : keys) {
      String value = params.get(key);
      if (TABLE_IS_CTAS.equals(key)) {
        continue;
      }
      if (key.equals(StatsSetupConst.NUM_ERASURE_CODED_FILES)) {
        if ("0".equals(value)) {
          continue;
        }
      }
      tableInfo.append(FIELD_DELIM); // Ensures all params are indented.
      formatOutput(key, escapeUnicode ? StringEscapeUtils.escapeJava(value) : HiveStringUtils.escapeJava(value),
          tableInfo, isOutputPadded);
    }
  }

  private String getConstraintsInformation(Table table) {
    StringBuilder constraintsInfo = new StringBuilder(DEFAULT_STRINGBUILDER_SIZE);

    constraintsInfo.append(LINE_DELIM).append("# Constraints").append(LINE_DELIM);
    if (PrimaryKeyInfo.isPrimaryKeyInfoNotEmpty(table.getPrimaryKeyInfo())) {
      constraintsInfo.append(LINE_DELIM).append("# Primary Key").append(LINE_DELIM);
      getPrimaryKeyInformation(constraintsInfo, table.getPrimaryKeyInfo());
    }
    if (ForeignKeyInfo.isForeignKeyInfoNotEmpty(table.getForeignKeyInfo())) {
      constraintsInfo.append(LINE_DELIM).append("# Foreign Keys").append(LINE_DELIM);
      getForeignKeysInformation(constraintsInfo, table.getForeignKeyInfo());
    }
    if (UniqueConstraint.isUniqueConstraintNotEmpty(table.getUniqueKeyInfo())) {
      constraintsInfo.append(LINE_DELIM).append("# Unique Constraints").append(LINE_DELIM);
      getUniqueConstraintsInformation(constraintsInfo, table.getUniqueKeyInfo());
    }
    if (NotNullConstraint.isNotNullConstraintNotEmpty(table.getNotNullConstraint())) {
      constraintsInfo.append(LINE_DELIM).append("# Not Null Constraints").append(LINE_DELIM);
      getNotNullConstraintsInformation(constraintsInfo, table.getNotNullConstraint());
    }
    if (DefaultConstraint.isCheckConstraintNotEmpty(table.getDefaultConstraint())) {
      constraintsInfo.append(LINE_DELIM).append("# Default Constraints").append(LINE_DELIM);
      getDefaultConstraintsInformation(constraintsInfo, table.getDefaultConstraint());
    }
    if (CheckConstraint.isCheckConstraintNotEmpty(table.getCheckConstraint())) {
      constraintsInfo.append(LINE_DELIM).append("# Check Constraints").append(LINE_DELIM);
      getCheckConstraintsInformation(constraintsInfo, table.getCheckConstraint());
    }
    return constraintsInfo.toString();
  }

  private void getPrimaryKeyInformation(StringBuilder constraintsInfo, PrimaryKeyInfo constraint) {
    formatOutput("Table:", constraint.getDatabaseName() + "." + constraint.getTableName(), constraintsInfo);
    formatOutput("Constraint Name:", constraint.getConstraintName(), constraintsInfo);
    Map<Integer, String> columnNames = constraint.getColNames();
    String title = "Column Name:";
    for (String columnName : columnNames.values()) {
      constraintsInfo.append(String.format("%-" + ALIGNMENT + "s", title) + FIELD_DELIM);
      formatOutput(new String[] {columnName}, constraintsInfo);
    }
  }

  private void getForeignKeysInformation(StringBuilder constraintsInfo, ForeignKeyInfo constraint) {
    formatOutput("Table:", constraint.getChildDatabaseName() + "." + constraint.getChildTableName(), constraintsInfo);
    Map<String, List<ForeignKeyCol>> foreignKeys = constraint.getForeignKeys();
    if (MapUtils.isNotEmpty(foreignKeys)) {
      for (Map.Entry<String, List<ForeignKeyCol>> entry : foreignKeys.entrySet()) {
        getForeignKeyRelInformation(constraintsInfo, entry.getKey(), entry.getValue());
      }
    }
  }

  private void getForeignKeyRelInformation(StringBuilder constraintsInfo, String constraintName,
      List<ForeignKeyCol> columns) {
    formatOutput("Constraint Name:", constraintName, constraintsInfo);
    if (CollectionUtils.isNotEmpty(columns)) {
      for (ForeignKeyCol column : columns) {
        String[] fields = new String[3];
        fields[0] = "Parent Column Name:" +
            column.parentDatabaseName + "."+ column.parentTableName + "." + column.parentColName;
        fields[1] = "Column Name:" + column.childColName;
        fields[2] = "Key Sequence:" + column.position;
        formatOutput(fields, constraintsInfo);
      }
    }
    constraintsInfo.append(LINE_DELIM);
  }

  private void getUniqueConstraintsInformation(StringBuilder constraintsInfo, UniqueConstraint constraint) {
    formatOutput("Table:", constraint.getDatabaseName() + "." + constraint.getTableName(), constraintsInfo);
    Map<String, List<UniqueConstraintCol>> uniqueConstraints = constraint.getUniqueConstraints();
    if (MapUtils.isNotEmpty(uniqueConstraints)) {
      for (Map.Entry<String, List<UniqueConstraintCol>> entry : uniqueConstraints.entrySet()) {
        getUniqueConstraintRelInformation(constraintsInfo, entry.getKey(), entry.getValue());
      }
    }
  }

  private void getUniqueConstraintRelInformation(StringBuilder constraintsInfo, String constraintName,
      List<UniqueConstraintCol> columns) {
    formatOutput("Constraint Name:", constraintName, constraintsInfo);
    if (CollectionUtils.isNotEmpty(columns)) {
      for (UniqueConstraintCol column : columns) {
        String[] fields = new String[2];
        fields[0] = "Column Name:" + column.colName;
        fields[1] = "Key Sequence:" + column.position;
        formatOutput(fields, constraintsInfo);
      }
    }
    constraintsInfo.append(LINE_DELIM);
  }

  private void getNotNullConstraintsInformation(StringBuilder constraintsInfo, NotNullConstraint constraint) {
    formatOutput("Table:", constraint.getDatabaseName() + "." + constraint.getTableName(), constraintsInfo);
    Map<String, String> notNullConstraints = constraint.getNotNullConstraints();
    if (MapUtils.isNotEmpty(notNullConstraints)) {
      for (Map.Entry<String, String> entry : notNullConstraints.entrySet()) {
        formatOutput("Constraint Name:", entry.getKey(), constraintsInfo);
        formatOutput("Column Name:", entry.getValue(), constraintsInfo);
        constraintsInfo.append(LINE_DELIM);
      }
    }
  }

  private void getDefaultConstraintsInformation(StringBuilder constraintsInfo, DefaultConstraint constraint) {
    formatOutput("Table:", constraint.getDatabaseName() + "." + constraint.getTableName(), constraintsInfo);
    Map<String, List<DefaultConstraintCol>> defaultConstraints = constraint.getDefaultConstraints();
    if (MapUtils.isNotEmpty(defaultConstraints)) {
      for (Map.Entry<String, List<DefaultConstraintCol>> entry : defaultConstraints.entrySet()) {
        getDefaultConstraintRelInformation(constraintsInfo, entry.getKey(), entry.getValue());
      }
    }
  }

  private void getDefaultConstraintRelInformation(StringBuilder constraintsInfo, String constraintName,
      List<DefaultConstraintCol> columns) {
    formatOutput("Constraint Name:", constraintName, constraintsInfo);
    if (CollectionUtils.isNotEmpty(columns)) {
      for (DefaultConstraintCol column : columns) {
        String[] fields = new String[2];
        fields[0] = "Column Name:" + column.colName;
        fields[1] = "Default Value:" + column.defaultVal;
        formatOutput(fields, constraintsInfo);
      }
    }
    constraintsInfo.append(LINE_DELIM);
  }

  private void getCheckConstraintsInformation(StringBuilder constraintsInfo, CheckConstraint constraint) {
    formatOutput("Table:", constraint.getDatabaseName() + "." + constraint.getTableName(), constraintsInfo);
    Map<String, List<CheckConstraintCol>> checkConstraints = constraint.getCheckConstraints();
    if (MapUtils.isNotEmpty(checkConstraints)) {
      for (Map.Entry<String, List<CheckConstraintCol>> entry : checkConstraints.entrySet()) {
        getCheckConstraintRelInformation(constraintsInfo, entry.getKey(), entry.getValue());
      }
    }
  }

  private void getCheckConstraintRelInformation(StringBuilder constraintsInfo, String constraintName,
      List<CheckConstraintCol> columns) {
    formatOutput("Constraint Name:", constraintName, constraintsInfo);
    if (CollectionUtils.isNotEmpty(columns)) {
      for (CheckConstraintCol column : columns) {
        String[] fields = new String[2];
        fields[0] = "Column Name:" + column.colName;
        fields[1] = "Check Value:" + column.checkExpression;
        formatOutput(fields, constraintsInfo);
      }
    }
    constraintsInfo.append(LINE_DELIM);
  }

  private void addExtendedTableData(DataOutputStream out, Table table, Partition partition) throws IOException {
    if (partition != null) {
      out.write(("Detailed Partition Information").getBytes(StandardCharsets.UTF_8));
      out.write(Utilities.tabCode);
      out.write(partition.getTPartition().toString().getBytes(StandardCharsets.UTF_8));
      out.write(Utilities.tabCode);
      out.write(Utilities.newLineCode); // comment column is empty
    } else {
      out.write(("Detailed Table Information").getBytes(StandardCharsets.UTF_8));
      out.write(Utilities.tabCode);
      String tableDesc = HiveStringUtils.escapeJava(table.getTTable().toString());
      out.write(tableDesc.getBytes(StandardCharsets.UTF_8));
      out.write(Utilities.tabCode);
      out.write(Utilities.newLineCode); // comment column is empty
    }
  }

  private void addExtendedConstraintData(DataOutputStream out, Table table)
      throws IOException, UnsupportedEncodingException {
    if (table.getTableConstraintsInfo().isTableConstraintsInfoNotEmpty()) {
      out.write(("Constraints").getBytes(StandardCharsets.UTF_8));
      out.write(Utilities.tabCode);
      if (PrimaryKeyInfo.isPrimaryKeyInfoNotEmpty(table.getPrimaryKeyInfo())) {
        out.write(table.getPrimaryKeyInfo().toString().getBytes(StandardCharsets.UTF_8));
        out.write(Utilities.newLineCode);
      }
      if (ForeignKeyInfo.isForeignKeyInfoNotEmpty(table.getForeignKeyInfo())) {
        out.write(table.getForeignKeyInfo().toString().getBytes(StandardCharsets.UTF_8));
        out.write(Utilities.newLineCode);
      }
      if (UniqueConstraint.isUniqueConstraintNotEmpty(table.getUniqueKeyInfo())) {
        out.write(table.getUniqueKeyInfo().toString().getBytes(StandardCharsets.UTF_8));
        out.write(Utilities.newLineCode);
      }
      if (NotNullConstraint.isNotNullConstraintNotEmpty(table.getNotNullConstraint())) {
        out.write(table.getNotNullConstraint().toString().getBytes(StandardCharsets.UTF_8));
        out.write(Utilities.newLineCode);
      }
      if (DefaultConstraint.isCheckConstraintNotEmpty(table.getDefaultConstraint())) {
        out.write(table.getDefaultConstraint().toString().getBytes(StandardCharsets.UTF_8));
        out.write(Utilities.newLineCode);
      }
      if (CheckConstraint.isCheckConstraintNotEmpty(table.getCheckConstraint())) {
        out.write(table.getCheckConstraint().toString().getBytes(StandardCharsets.UTF_8));
        out.write(Utilities.newLineCode);
      }
    }
  }

  private void addExtendedStorageData(DataOutputStream out, Table table)
      throws IOException, UnsupportedEncodingException {
    if (table.getStorageHandlerInfo() != null) {
      out.write(("StorageHandlerInfo").getBytes(StandardCharsets.UTF_8));
      out.write(Utilities.newLineCode);
      out.write(table.getStorageHandlerInfo().formatAsText().getBytes(StandardCharsets.UTF_8));
      out.write(Utilities.newLineCode);
    }
  }
}
