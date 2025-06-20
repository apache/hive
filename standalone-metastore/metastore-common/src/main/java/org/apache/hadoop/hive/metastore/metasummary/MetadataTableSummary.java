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
package org.apache.hadoop.hive.metastore.metasummary;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MetadataTableSummary {
  private String catalogName;
  private String dbName;
  private String tblName;
  private String owner;
  private int colCount;
  private int partitionColumnCount;
  private int partitionCount;
  private long totalSize;
  private long numRows;
  private long numFiles;
  private String tableType;
  private String fileFormat;
  private String compressionType;
  private int arrayColumnCount;
  private int structColumnCount;
  private int mapColumnCount;
  private Map<String, Object> summary;
  private transient boolean dropped = false;
  private transient long tableId;

  public MetadataTableSummary(String catalogName,
       String db, String tableName, String owner) {
    this.catalogName = catalogName;
    this.owner = owner;
    this.dbName = db;
    this.tblName = tableName;
  }

  public MetadataTableSummary() {
  }

  public void columnSummary(int columnCount,
      int arrayColCount, int structColCount, int mapColCount) {
    this.colCount = columnCount;
    this.arrayColumnCount = arrayColCount;
    this.structColumnCount = structColCount;
    this.mapColumnCount = mapColCount;
  }

  public void tableFormatSummary(String tblType, String compression, String fileFormat) {
    this.tableType = tblType;
    this.compressionType = compression;
    this.fileFormat = fileFormat;
  }

  public String getOwner() {
    return owner;
  }

  public void setOwner(String owner) {
    this. owner = owner;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public String getTblName() {
    return tblName;
  }

  public void setTblName(String tblName) {
    this.tblName = tblName;
  }

  public int getColCount() {
    return colCount;
  }

  public void setColCount(int colCount) {
    this.colCount = colCount;
  }

  public int getPartitionColumnCount() {
    return partitionColumnCount;
  }

  public void setPartitionColumnCount(int partitionColumnCount) {
    this.partitionColumnCount = partitionColumnCount;
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public void setPartitionCount(int partitionCount) {
    this.partitionCount = partitionCount;
  }

  public long getTotalSize() {
    return totalSize;
  }

  public void setTotalSize(long totalSize) {
    this.totalSize = totalSize;
  }

  public long getNumRows() {
    return numRows;
  }

  public void setNumRows(long numRows) {
    this.numRows = numRows;
  }

  public long getNumFiles() {
    return numFiles;
  }

  public void setNumFiles(long numFiles) {
    this.numFiles = numFiles;
  }

  public String getTableType() {
    return tableType;
  }

  public void setTableType(String tableType) {
    this.tableType = tableType;
  }

  public String getFileFormat() {
    return fileFormat;
  }

  public void setFileFormat(String fileFormat) {
    this.fileFormat = fileFormat;
  }

  public String getCompressionType() {
    return compressionType;
  }

  public void setCompressionType(String compressionType) {
    this.compressionType = compressionType;
  }

  public void setArrayColumnCount(int arrayColumnCount) {
    this.arrayColumnCount = arrayColumnCount;
  }

  public int getArrayColumnCount() {
    return arrayColumnCount;
  }

  public void setStructColumnCount(int structColumnCount) {
    this.structColumnCount = structColumnCount;
  }

  public int getStructColumnCount() {
    return structColumnCount;
  }

  public void setMapColumnCount(int mapColumnCount) {
    this.mapColumnCount = mapColumnCount;
  }

  public int getMapColumnCount() {
    return mapColumnCount;
  }

  public Map<String, Object> getExtraSummary() {
    return summary;
  }

  public MetadataTableSummary addExtra(String key, SummaryMapBuilder builder) {
    if (summary == null) {
      summary = new HashMap<>();
    }
    summary.put(key, builder.build());
    return this;
  }

  public void addExtra(SummaryMapBuilder builder) {
    if (summary == null) {
      summary = new HashMap<>();
    }
    summary.putAll(builder.build());
  }

  @Override
  public String toString() {
    return "TableSummary {" + "owner='" + owner + '\'' + ", db_name='" + dbName + '\'' + ", table_name='" + tblName +
        '\'' + ", column_count=" + colCount + ", partition_count=" + partitionCount + ", table_type='" + tableType + '\'' +
        ", file_format='" + fileFormat + '\'' + ", compression_type='" + compressionType + ", numRows=" + numRows +
        ", numFiles=" + numFiles + ", size_bytes=" + totalSize + ", partition_column_count=" + partitionColumnCount +
        ", array_column_count=" + arrayColumnCount + ", struct_column_count=" + structColumnCount + ", map_column_count=" + mapColumnCount + '}';
  }

  public String toCSV(List<String> fields) {
    StringBuilder csv = new StringBuilder().append(dbName).append(",").append(tblName).append(",")
        .append(owner).append(",").append(colCount).append(",").append(partitionCount).append(",").append(tableType).append(",")
        .append(fileFormat).append(",").append(compressionType).append(",").append(numRows).append(",")
        .append(numFiles).append(",").append(totalSize).append(",").append(partitionColumnCount).append(",")
        .append(arrayColumnCount).append(",").append(structColumnCount).append(",").append(mapColumnCount).append(",");
    if (!fields.isEmpty()) {
      csv.append(fields.stream().map(key -> {
        Object value = summary != null ? summary.get(key) : null;
        if (value == null) {
          return "";
        }
        return String.valueOf(value);
      }).collect(Collectors.joining(",")));
      csv.append(",");
    }
    return csv.toString();
  }

  public boolean isDropped() {
    return dropped;
  }

  public void setDropped(boolean dropped) {
    this.dropped = dropped;
  }

  public String getCatalogName() {
    return catalogName;
  }

  public void setCatalogName(String catalogName) {
    this.catalogName = catalogName;
  }

  public long getTableId() {
    return tableId;
  }

  public void setTableId(long tableId) {
    this.tableId = tableId;
  }
}