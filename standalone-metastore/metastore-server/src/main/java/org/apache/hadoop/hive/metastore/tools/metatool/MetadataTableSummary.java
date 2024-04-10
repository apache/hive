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
package org.apache.hadoop.hive.metastore.tools.metatool;

import java.math.BigInteger;

public class MetadataTableSummary {
  private String ctlgName;
  private String dbName;
  private String tblName;
  private int colCount;
  private int partitionColumnCount;
  private int partitionCount;
  private BigInteger totalSize;
  private BigInteger sizeNumRows;
  private BigInteger sizeNumFiles;
  private String tableType;
  private String fileFormat;
  private String compressionType;
  private int arrayColumnCount;
  private int structColumnCount;
  private int mapColumnCount;


  public MetadataTableSummary(String ctlgName, String dbName, String tblName, int colCount,
                        int partitionColumnCount, int partitionCount, BigInteger totalSize, BigInteger sizeNumRows,
                        BigInteger sizeNumFiles, String tableType, String fileFormat, String compressionType,
                        int arrayColumnCount, int structColumnCount, int mapColumnCount) {
    this.ctlgName = ctlgName;
    this.dbName = dbName;
    this.tblName = tblName;
    this.colCount = colCount;
    this.partitionColumnCount = partitionColumnCount;
    this.partitionCount = partitionCount;
    this.totalSize = totalSize;
    this.sizeNumRows = sizeNumRows;
    this.sizeNumFiles = sizeNumFiles;
    this.tableType = tableType;
    this.fileFormat = fileFormat;
    this.compressionType = compressionType;
    this.arrayColumnCount = arrayColumnCount;
    this.structColumnCount = structColumnCount;
    this.mapColumnCount = mapColumnCount;
  }

    public MetadataTableSummary() {  }

    public String getCtlgName() {
        return ctlgName;
    }

    public void setCtlgName(String ctlgName) {
        this.ctlgName = ctlgName;
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

    public BigInteger getTotalSize() {
        return totalSize;
    }

    public void setTotalSize(BigInteger totalSize) {
        this.totalSize = totalSize;
    }

    public BigInteger getSizeNumRows() {
        return sizeNumRows;
    }

    public void setSizeNumRows(BigInteger sizeNumRows) {
        this.sizeNumRows = sizeNumRows;
    }

    public BigInteger getSizeNumFiles() {
        return sizeNumFiles;
    }

    public void setSizeNumFiles(BigInteger sizeNumFiles) {
        this.sizeNumFiles = sizeNumFiles;
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




    @Override
    public String toString() {
      return "TableSummary {" +
             "cat_name='" + ctlgName + '\'' +
             ", db_name='" + dbName + '\'' +
             ", table_name='" + tblName + '\'' +
             ", column_count=" + colCount +
             ", partition_count=" + partitionCount +
             ", table_type='" + tableType + '\'' +
             ", file_format='" + fileFormat + '\'' +
             ", compression_type='" + compressionType  +
             ", size_numRows=" + sizeNumRows +
             ", size_numFiles=" + sizeNumFiles +
             ", size_bytes=" + totalSize +
             ", partition_column_count=" + partitionColumnCount +
             ", array_column_count=" + arrayColumnCount +
             ", struct_column_count=" + structColumnCount +
             ", map_column_count=" + mapColumnCount +
             '}';
    }

    public String toCSV() {
      return new StringBuilder()
        .append(ctlgName).append(",")
        .append(dbName).append(",")
        .append(tblName).append(",")
        .append(colCount).append(",")
        .append(partitionCount).append(",")
        .append(tableType).append(",")
        .append(fileFormat).append(",")
        .append(compressionType).append(",")
        .append(sizeNumRows).append(",")
        .append(sizeNumFiles).append(",")
        .append(totalSize).append(",")
        .append(partitionColumnCount).append(",")
        .append(arrayColumnCount).append(",")
        .append(structColumnCount).append(",")
        .append(mapColumnCount).append(",")
        .toString();
    }
}