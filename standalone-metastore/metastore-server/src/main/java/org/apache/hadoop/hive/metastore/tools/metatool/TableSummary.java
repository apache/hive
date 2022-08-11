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

public class TableSummary{
    String cat_name;
    String db_name;
    String table_name;
    int column_count;
    int partition_column_count;
    int partition_count;
    BigInteger size_bytes;
    BigInteger size_numRows;
    BigInteger size_numFiles;
    String table_type;
    String file_format;
    String compression_type;


    public TableSummary(String cat_name, String db_name, String table_name, int column_count,
                        int partition_column_count, int partition_count, BigInteger size_bytes, BigInteger size_numRows,
                        BigInteger size_numFiles, String table_type, String file_format, String compression_type) {
        this.cat_name = cat_name;
        this.db_name = db_name;
        this.table_name = table_name;
        this.column_count = column_count;
        this.partition_column_count = partition_column_count;
        this.partition_count = partition_count;
        this.size_bytes = size_bytes;
        this.size_numRows = size_numRows;
        this.size_numFiles = size_numFiles;
        this.table_type = table_type;
        this.file_format = file_format;
        this.compression_type = compression_type;
    }

    public TableSummary() {

    }

    public int getPartition_count() {
        return partition_count;
    }

    public void setPartition_count(int partition_count) {
        this.partition_count = partition_count;
    }

    public String getTable_name() {
        return table_name;
    }

    public void setTable_name(String table_name) {
        this.table_name = table_name;
    }

    public String getDb_name() {
        return db_name;
    }

    public void setDb_name(String db_name) {
        this.db_name = db_name;
    }

    public String getCat_name() {
        return cat_name;
    }

    public void setCat_name(String cat_name) {
        this.cat_name = cat_name;
    }

    public int getColumn_count() {
        return column_count;
    }

    public void setColumn_count(int column_count) {
        this.column_count = column_count;
    }

    public int getPartition_column_count() {
        return partition_column_count;
    }

    public void setPartition_column_count(int partition_column_count) {
        this.partition_column_count = partition_column_count;
    }

    public BigInteger getSize_bytes() {
        return size_bytes;
    }

    public void setSize_bytes(BigInteger size_bytes) {
        this.size_bytes = size_bytes;
    }

    public BigInteger getSize_numRows() {
        return size_numRows;
    }

    public void setSize_numRows(BigInteger size_numRows) {
        this.size_numRows = size_numRows;
    }

    public BigInteger getSize_numFiles() {
        return size_numFiles;
    }

    public void setSize_numFiles(BigInteger size_numFiles) {
        this.size_numFiles = size_numFiles;
    }

    public String getTable_type() {
        return table_type;
    }

    public void setTable_type(String table_type) {
        this.table_type = table_type;
    }

    public String getFile_format() {
        return file_format;
    }

    public void setFile_format(String file_format) {
        this.file_format = file_format;
    }

    public String getCompression_type() {
        return compression_type;
    }

    public void setCompression_type(String compression_type) {
        this.compression_type = compression_type;
    }

    @Override
    public String toString() {
        return "TableSummary{" +
                "cat_name='" + cat_name + '\'' +
                ", db_name='" + db_name + '\'' +
                ", table_name='" + table_name + '\'' +
                ", column_count=" + column_count +
                ", partition_column_count=" + partition_column_count +
                ", partition_count=" + partition_count +
                ", size_bytes=" + size_bytes +
                ", size_numRows=" + size_numRows +
                ", size_numFiles=" + size_numFiles +
                ", table_type='" + table_type + '\'' +
                ", file_format='" + file_format + '\'' +
                ", compression_type='" + compression_type + '\'' +
                '}';
    }
}
