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

package org.apache.hadoop.hive.ql.metadata.formatting;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.index.HiveIndex;
import org.apache.hadoop.hive.ql.index.HiveIndex.IndexType;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.DescTableDesc;
import org.apache.hadoop.hive.ql.plan.ShowIndexesDesc;


/**
 * This class provides methods to format table and index information.
 *
 */
public final class MetaDataFormatUtils {

  public static final String FIELD_DELIM = "\t";
  public static final String LINE_DELIM = "\n";

  private static final int DEFAULT_STRINGBUILDER_SIZE = 2048;
  private static final int ALIGNMENT = 20;

  private MetaDataFormatUtils() {
  }

  private static void formatColumnsHeader(StringBuilder columnInformation) {
    columnInformation.append("# "); // Easy for shell scripts to ignore
    formatOutput(getColumnsHeader(), columnInformation);
    columnInformation.append(LINE_DELIM);
  }

  public static String getAllColumnsInformation(List<FieldSchema> cols) {
    StringBuilder columnInformation = new StringBuilder(DEFAULT_STRINGBUILDER_SIZE);
    formatColumnsHeader(columnInformation);
    formatAllFields(columnInformation, cols);
    return columnInformation.toString();
  }

  public static String getAllColumnsInformation(List<FieldSchema> cols, List<FieldSchema> partCols) {
    StringBuilder columnInformation = new StringBuilder(DEFAULT_STRINGBUILDER_SIZE);
    formatColumnsHeader(columnInformation);
    formatAllFields(columnInformation, cols);

    if ((partCols != null) && (!partCols.isEmpty())) {
      columnInformation.append(LINE_DELIM).append("# Partition Information")
        .append(LINE_DELIM);
      formatColumnsHeader(columnInformation);
      formatAllFields(columnInformation, partCols);
    }

    return columnInformation.toString();
  }

  private static void formatAllFields(StringBuilder tableInfo, List<FieldSchema> cols) {
    for (FieldSchema col : cols) {
      formatFieldSchemas(tableInfo, col);
    }
  }


  public static String getAllColumnsInformation(Index index) {
    StringBuilder indexInfo = new StringBuilder(DEFAULT_STRINGBUILDER_SIZE);

    List<String> indexColumns = new ArrayList<String>();

    indexColumns.add(index.getIndexName());
    indexColumns.add(index.getOrigTableName());

    // index key names
    List<FieldSchema> indexKeys = index.getSd().getCols();
    StringBuilder keyString = new StringBuilder();
    boolean first = true;
    for (FieldSchema key : indexKeys)
    {
      if (!first)
      {
        keyString.append(", ");
      }
      keyString.append(key.getName());
      first = false;
    }

    indexColumns.add(keyString.toString());

    indexColumns.add(index.getIndexTableName());

    // index type
    String indexHandlerClass = index.getIndexHandlerClass();
    IndexType indexType = HiveIndex.getIndexTypeByClassName(indexHandlerClass);
    indexColumns.add(indexType.getName());

    indexColumns.add(index.getParameters().get("comment"));

    formatOutput(indexColumns.toArray(new String[0]), indexInfo);

    return indexInfo.toString();
}

  /*
    Displaying columns unformatted for backward compatibility.
   */
  public static String displayColsUnformatted(List<FieldSchema> cols) {
    StringBuilder colBuffer = new StringBuilder(DEFAULT_STRINGBUILDER_SIZE);
    for (FieldSchema col : cols) {
      colBuffer.append(col.getName());
      colBuffer.append(FIELD_DELIM);
      colBuffer.append(col.getType());
      colBuffer.append(FIELD_DELIM);
      colBuffer.append(col.getComment() == null ? "" : col.getComment());
      colBuffer.append(LINE_DELIM);
    }
    return colBuffer.toString();
  }

  public static String getPartitionInformation(Partition part) {
    StringBuilder tableInfo = new StringBuilder(DEFAULT_STRINGBUILDER_SIZE);

    // Table Metadata
    tableInfo.append(LINE_DELIM).append("# Detailed Partition Information").append(LINE_DELIM);
    getPartitionMetaDataInformation(tableInfo, part);

    // Storage information.
    if (part.getTable().getTableType() != TableType.VIRTUAL_VIEW) {
      tableInfo.append(LINE_DELIM).append("# Storage Information").append(LINE_DELIM);
      getStorageDescriptorInfo(tableInfo, part.getTPartition().getSd());
    }

    return tableInfo.toString();
  }

  public static String getTableInformation(Table table) {
    StringBuilder tableInfo = new StringBuilder(DEFAULT_STRINGBUILDER_SIZE);

    // Table Metadata
    tableInfo.append(LINE_DELIM).append("# Detailed Table Information").append(LINE_DELIM);
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

    if (null != storageDesc.getSkewedInfo()) {
      List<String> skewedColNames = storageDesc.getSkewedInfo().getSkewedColNames();
      if ((skewedColNames != null) && (skewedColNames.size() > 0)) {
        formatOutput("Skewed Columns:", skewedColNames.toString(), tableInfo);
      }

      List<List<String>> skewedColValues = storageDesc.getSkewedInfo().getSkewedColValues();
      if ((skewedColValues != null) && (skewedColValues.size() > 0)) {
        formatOutput("Skewed Values:", skewedColValues.toString(), tableInfo);
      }

      Map<List<String>, String> skewedColMap = storageDesc.getSkewedInfo()
          .getSkewedColValueLocationMaps();
      if ((skewedColMap!=null) && (skewedColMap.size() > 0)) {
        formatOutput("Skewed Value to Location Mapping:", skewedColMap.toString(),
            tableInfo);
      }
    }

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
    List<String> keys = new ArrayList<String>(params.keySet());
    Collections.sort(keys);
    for (String key : keys) {
      tableInfo.append(FIELD_DELIM); // Ensures all params are indented.
      formatOutput(key, StringEscapeUtils.escapeJava(params.get(key)), tableInfo);
    }
  }

  private static void formatFieldSchemas(StringBuilder tableInfo, FieldSchema col) {
    String comment = col.getComment() != null ? col.getComment() : "None";
    formatOutput(col.getName(), col.getType(), comment, tableInfo);
  }

  private static String formatDate(long timeInSeconds) {
    if (timeInSeconds != 0) {
      Date date = new Date(timeInSeconds * 1000);
      return date.toString();
    }
    return "UNKNOWN";
  }

  private static void formatOutput(String[] fields, StringBuilder tableInfo) {
    for (String field : fields) {
      if (field == null) {
        tableInfo.append(FIELD_DELIM);
        continue;
      }
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

  public static String getIndexColumnsHeader() {
    StringBuilder indexCols = new StringBuilder(DEFAULT_STRINGBUILDER_SIZE);
    formatOutput(ShowIndexesDesc.getSchema().split("#")[0].split(","), indexCols);
    return indexCols.toString();
  }
}
