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

/**
 *
 */
package org.apache.hadoop.hive.metastore.model;

import java.nio.ByteBuffer;


/**
 *
 * MTableColumnStatistics - Represents Hive's Column Statistics Description. The fields in this
 * class with the exception of table are persisted in the metastore. In case of table, tbl_id is
 * persisted in its place.
 *
 */
public class MTableColumnStatistics {

  private MTable table;
  private String dbName;
  private String tableName;
  private String colName;
  private String colType;

  private byte[] lowValue;
  private byte[] highValue;
  private long numNulls;
  private long numDVs;
  private double avgColLen;
  private long maxColLen;
  private long numTrues;
  private long numFalses;
  private long lastAnalyzed;

  public MTableColumnStatistics() {}

  public MTable getTable() {
    return table;
  }

  public void setTable(MTable table) {
    this.table = table;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String getColName() {
    return colName;
  }

  public void setColName(String colName) {
    this.colName = colName;
  }

  public String getColType() {
    return colType;
  }

  public void setColType(String colType) {
    this.colType = colType;
  }

  public byte[] getLowValue() {
    return lowValue;
  }

  public long getLowValueAsLong() {
    ByteBuffer byteBuf = ByteBuffer.wrap(lowValue);
    return byteBuf.getLong();
  }

  public double getLowValueAsDouble() {
    ByteBuffer byteBuf = ByteBuffer.wrap(lowValue);
    return byteBuf.getDouble();
  }

  public byte[] getHighValue() {
    return highValue;
  }

  public long getHighValueAsLong() {
    ByteBuffer byteBuf = ByteBuffer.wrap(highValue);
    return byteBuf.getLong();
  }

  public double getHighValueAsDouble() {
    ByteBuffer byteBuf = ByteBuffer.wrap(highValue);
    return byteBuf.getDouble();
  }

  public void setHighValue(byte[] b) {
    this.highValue = b;
  }

  public void setLowValue(byte[] b) {
    this.lowValue = b;
  }

  public long getNumNulls() {
    return numNulls;
  }


  public void setNumNulls(long numNulls) {
    this.numNulls = numNulls;
  }

  public long getNumDVs() {
    return numDVs;
  }

  public void setNumDVs(long numDVs) {
    this.numDVs = numDVs;
  }

  public double getAvgColLen() {
    return avgColLen;
  }

  public void setAvgColLen(double avgColLen) {
    this.avgColLen = avgColLen;
  }

  public long getMaxColLen() {
    return maxColLen;
  }

  public void setMaxColLen(long maxColLen) {
    this.maxColLen = maxColLen;
  }

  public long getNumTrues() {
    return numTrues;
  }

  public void setNumTrues(long numTrues) {
    this.numTrues = numTrues;
  }

  public long getNumFalses() {
    return numFalses;
  }

  public void setNumFalses(long numFalses) {
    this.numFalses = numFalses;
  }

  public long getLastAnalyzed() {
    return lastAnalyzed;
  }

  public void setLastAnalyzed(long lastAnalyzed) {
    this.lastAnalyzed = lastAnalyzed;
  }

  public String getDbName() {
    return dbName;
  }

  public void setDbName(String dbName) {
    this.dbName = dbName;
  }

  public void setBooleanStats(long numTrues, long numFalses, long numNulls) {
    this.numTrues = numTrues;
    this.numFalses = numFalses;
    this.numNulls = numNulls;
  }

  public void setLongStats(long numNulls, long numNDVs, long lowValue, long highValue) {
    this.numNulls = numNulls;
    this.numDVs = numNDVs;
    byte[] bytes = ByteBuffer.allocate(Long.SIZE/8).putLong(lowValue).array();
    this.lowValue = bytes;
    bytes = ByteBuffer.allocate(Long.SIZE/8).putLong(highValue).array();
    this.highValue = bytes;
  }

  public void setDoubleStats(long numNulls, long numNDVs, double lowValue, double highValue) {
    this.numNulls = numNulls;
    this.numDVs = numNDVs;
    byte[] bytes = ByteBuffer.allocate(Double.SIZE/8).putDouble(lowValue).array();
    this.lowValue = bytes;
    bytes = ByteBuffer.allocate(Double.SIZE/8).putDouble(highValue).array();
    this.highValue = bytes;
  }

  public void setStringStats(long numNulls, long numNDVs, long maxColLen, double avgColLen) {
    this.numNulls = numNulls;
    this.numDVs = numNDVs;
    this.maxColLen = maxColLen;
    this.avgColLen = avgColLen;
  }

  public void setBinaryStats(long numNulls, long maxColLen, double avgColLen) {
    this.numNulls = numNulls;
    this.maxColLen = maxColLen;
    this.avgColLen = avgColLen;
  }
}
