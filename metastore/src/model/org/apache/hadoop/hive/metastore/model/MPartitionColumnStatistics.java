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



/**
 *
 * MPartitionColumnStatistics - Represents Hive's partiton level Column Statistics Description.
 * The fields in this class with the exception of partition are persisted in the metastore.
 * In case of partition, part_id is persisted in its place.
 *
 */
public class MPartitionColumnStatistics {

  private MPartition partition;

  private String dbName;
  private String tableName;
  private String partitionName;
  private String colName;
  private String colType;

  private long longLowValue;
  private long longHighValue;
  private double doubleLowValue;
  private double doubleHighValue;
  private String decimalLowValue;
  private String decimalHighValue;
  private long numNulls;
  private long numDVs;
  private double avgColLen;
  private long maxColLen;
  private long numTrues;
  private long numFalses;
  private long lastAnalyzed;

  public MPartitionColumnStatistics() {}

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

  public MPartition getPartition() {
    return partition;
  }

  public void setPartition(MPartition partition) {
    this.partition = partition;
  }

  public String getPartitionName() {
    return partitionName;
  }

  public void setPartitionName(String partitionName) {
    this.partitionName = partitionName;
  }

  public String getColType() {
    return colType;
  }

  public void setColType(String colType) {
    this.colType = colType;
  }

  public void setBooleanStats(long numTrues, long numFalses, long numNulls) {
    this.numTrues = numTrues;
    this.numFalses = numFalses;
    this.numNulls = numNulls;
  }

  public void setLongStats(long numNulls, long numNDVs, long lowValue, long highValue) {
    this.numNulls = numNulls;
    this.numDVs = numNDVs;
    this.longLowValue = lowValue;
    this.longHighValue = highValue;
  }

  public void setDoubleStats(long numNulls, long numNDVs, double lowValue, double highValue) {
    this.numNulls = numNulls;
    this.numDVs = numNDVs;
    this.doubleLowValue = lowValue;
    this.doubleHighValue = highValue;
  }

  public void setDecimalStats(
      long numNulls, long numNDVs, String lowValue, String highValue) {
    this.numNulls = numNulls;
    this.numDVs = numNDVs;
    this.decimalLowValue = lowValue;
    this.decimalHighValue = highValue;
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
  public long getLongLowValue() {
    return longLowValue;
  }

  public void setLongLowValue(long longLowValue) {
    this.longLowValue = longLowValue;
  }

  public long getLongHighValue() {
    return longHighValue;
  }

  public void setLongHighValue(long longHighValue) {
    this.longHighValue = longHighValue;
  }

  public double getDoubleLowValue() {
    return doubleLowValue;
  }

  public void setDoubleLowValue(double doubleLowValue) {
    this.doubleLowValue = doubleLowValue;
  }

  public double getDoubleHighValue() {
    return doubleHighValue;
  }

  public void setDoubleHighValue(double doubleHighValue) {
    this.doubleHighValue = doubleHighValue;
  }

  public String getDecimalLowValue() {
    return decimalLowValue;
  }

  public void setDecimalLowValue(String decimalLowValue) {
    this.decimalLowValue = decimalLowValue;
  }

  public String getDecimalHighValue() {
    return decimalHighValue;
  }

  public void setDecimalHighValue(String decimalHighValue) {
    this.decimalHighValue = decimalHighValue;
  }
}
