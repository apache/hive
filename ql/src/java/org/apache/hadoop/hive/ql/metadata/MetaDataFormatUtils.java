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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.metadata;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.plan.DescTableDesc;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * This class provides methods to format table information.
 *
 */
public final class MetaDataFormatUtils {

  public static final String FIELD_DELIM = "\t";
  public static final String LINE_DELIM = "\n";

  private static final int DEFAULT_STRINGBUILDER_SIZE = 2048;
  private static final int ALIGNMENT = 20;

  private MetaDataFormatUtils() {
  }

  public static String getAllColumnsInformation(Table table) {

    StringBuilder columnInformation = new StringBuilder(DEFAULT_STRINGBUILDER_SIZE);
    formatColumnsHeader(columnInformation);
    formatAllFields(columnInformation, table.getCols());

    // Partitions
    if (table.isPartitioned()) {
      columnInformation.append(LINE_DELIM).append("# Partition Information")
          .append(LINE_DELIM);
      formatColumnsHeader(columnInformation);
      formatAllFields(columnInformation, table.getPartCols());
    }
    return columnInformation.toString();
  }

  private static void formatColumnsHeader(StringBuilder columnInformation) {
    formatOutput(getColumnsHeader(), columnInformation);
    columnInformation.append(LINE_DELIM);
  }

  public static String getAllColumnsInformation(List<FieldSchema> cols) {
    StringBuilder columnInformation = new StringBuilder(DEFAULT_STRINGBUILDER_SIZE);
    formatColumnsHeader(columnInformation);
    formatAllFields(columnInformation, cols);
    return columnInformation.toString();
  }

  private static void formatAllFields(StringBuilder tableInfo, List<FieldSchema> cols) {
    for (FieldSchema col : cols) {
      formatFieldSchemas(tableInfo, col);
    }
  }

  public static String getPartitionInformation(Partition part) {
    StringBuilder tableInfo = new StringBuilder(DEFAULT_STRINGBUILDER_SIZE);

    // Table Metadata
    tableInfo.append("# Detailed Partition Information").append(LINE_DELIM);
    getPartitionMetaDataInformation(tableInfo, part);

    // Storage information.
    tableInfo.append(LINE_DELIM).append("# Storage Information").append(LINE_DELIM);
    getStorageDescriptorInfo(tableInfo, part.getTPartition().getSd());

    return tableInfo.toString();
  }

  public static String getTableInformation(Table table) {
    StringBuilder tableInfo = new StringBuilder(DEFAULT_STRINGBUILDER_SIZE);

    // Table Metadata
    tableInfo.append("# Detailed Table Information").append(LINE_DELIM);
    getTableMetaDataInformation(tableInfo, table);

    // Storage information.
    tableInfo.append(LINE_DELIM).append("# Storage Information").append(LINE_DELIM);
    getStorageDescriptorInfo(tableInfo, table.getTTable().getSd());

    if (table.isView()) {
      tableInfo.append(LINE_DELIM).append("# View Information").append(LINE_DELIM);
      getViewInfo(tableInfo, table);
    }

    return tableInfo.toString();
  }

  private static void getViewInfo(StringBuilder tableInfo, Table tbl) {
    formatOutput("View Original Text:", tbl.getViewOriginalText(), tableInfo);
    formatOutput("View Expanded Text:", tbl.getViewExpandedText(), tableInfo);
  }

  private static void getStorageDescriptorInfo(StringBuilder tableInfo,
                                               StorageDescriptor storageDesc) {

    formatOutput("SerDe Library:", storageDesc.getSerdeInfo().getSerializationLib(), tableInfo);
    formatOutput("InputFormat:", storageDesc.getInputFormat(), tableInfo);
    formatOutput("OutputFormat:", storageDesc.getOutputFormat(), tableInfo);
    formatOutput("Compressed:", storageDesc.isCompressed() ? "Yes" : "No", tableInfo);
    formatOutput("Num Buckets:", String.valueOf(storageDesc.getNumBuckets()), tableInfo);
    formatOutput("Bucket Columns:", storageDesc.getBucketCols().toString(), tableInfo);
    formatOutput("Sort Columns:", storageDesc.getSortCols().toString(), tableInfo);

    if (storageDesc.getSerdeInfo().getParametersSize() > 0) {
      tableInfo.append("Storage Desc Params:").append(LINE_DELIM);
      displayAllParameters(storageDesc.getSerdeInfo().getParameters(), tableInfo);
    }
  }

  private static void getTableMetaDataInformation(StringBuilder tableInfo, Table  tbl) {
    formatOutput("Database:", tbl.getDbName(), tableInfo);
    formatOutput("Owner:", tbl.getOwner(), tableInfo);
    formatOutput("CreateTime:", formatDate(tbl.getTTable().getCreateTime()), tableInfo);
    formatOutput("LastAccessTime:", formatDate(tbl.getTTable().getLastAccessTime()), tableInfo);
    String protectMode = tbl.getProtectMode().toString();
    formatOutput("Protect Mode:", protectMode == null ? "None" : protectMode, tableInfo);
    formatOutput("Retention:", Integer.toString(tbl.getRetention()), tableInfo);
    if (!tbl.isView()) {
      formatOutput("Location:", tbl.getDataLocation().toString(), tableInfo);
    }
    formatOutput("Table Type:", tbl.getTableType().name(), tableInfo);

    if (tbl.getParameters().size() > 0) {
      tableInfo.append("Table Parameters:").append(LINE_DELIM);
      displayAllParameters(tbl.getParameters(), tableInfo);
    }
  }

  private static void getPartitionMetaDataInformation(StringBuilder tableInfo, Partition part) {
    formatOutput("Partition Value:", part.getValues().toString(), tableInfo);
    formatOutput("Database:", part.getTPartition().getDbName(), tableInfo);
    formatOutput("Table:", part.getTable().getTableName(), tableInfo);
    formatOutput("CreateTime:", formatDate(part.getTPartition().getCreateTime()), tableInfo);
    formatOutput("LastAccessTime:", formatDate(part.getTPartition().getLastAccessTime()),
        tableInfo);
    String protectMode = part.getProtectMode().toString();
    formatOutput("Protect Mode:", protectMode == null ? "None" : protectMode, tableInfo);
    formatOutput("Location:", part.getLocation(), tableInfo);

    if (part.getTPartition().getParameters().size() > 0) {
      tableInfo.append("Partition Parameters:").append(LINE_DELIM);
      displayAllParameters(part.getTPartition().getParameters(), tableInfo);
    }
  }

  private static void displayAllParameters(Map<String, String> params, StringBuilder tableInfo) {
    for (Map.Entry<String, String> parameter: params.entrySet()) {
      tableInfo.append(FIELD_DELIM); // Ensures all params are indented.
      formatOutput(parameter.getKey(), StringEscapeUtils.escapeJava(parameter.getValue()),
          tableInfo);
    }
  }

  private static void formatFieldSchemas(StringBuilder tableInfo, FieldSchema col) {
    String comment = col.getComment() != null ? col.getComment() : "None";
    formatOutput(col.getName(), col.getType(), comment, tableInfo);
  }

  private static String formatDate(long timeInSeconds) {
    Date date = new Date(timeInSeconds * 1000);
    return date.toString();
  }

  private static void formatOutput(String[] fields, StringBuilder tableInfo) {
    for (String field : fields) {
      tableInfo.append(String.format("%-" + ALIGNMENT + "s", field)).append(FIELD_DELIM);
    }
    tableInfo.append(LINE_DELIM);
  }

  private static void formatOutput(String name, String value,
                                   StringBuilder tableInfo) {
    tableInfo.append(String.format("%-" + ALIGNMENT + "s", name)).append(FIELD_DELIM);
    tableInfo.append(String.format("%-" + ALIGNMENT + "s", value)).append(LINE_DELIM);
  }

  private static void formatOutput(String col1, String col2, String col3,
                                   StringBuilder tableInfo) {
    tableInfo.append(String.format("%-" + ALIGNMENT + "s", col1)).append(FIELD_DELIM);
    tableInfo.append(String.format("%-" + ALIGNMENT + "s", col2)).append(FIELD_DELIM);
    tableInfo.append(String.format("%-" + ALIGNMENT + "s", col3)).append(LINE_DELIM);
  }

  public static String[] getColumnsHeader() {
    return DescTableDesc.getSchema().split("#")[0].split(",");
  }
}
