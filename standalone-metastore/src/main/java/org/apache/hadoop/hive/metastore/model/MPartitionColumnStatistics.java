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

  private Long longLowValue;
  private Long longHighValue;
  private Double doubleLowValue;
  private Double doubleHighValue;
  private String decimalLowValue;
  private String decimalHighValue;
  private Long numNulls;
  private Long numDVs;
  private byte[] bitVector;
  private Double avgColLen;
  private Long maxColLen;
  private Long numTrues;
  private Long numFalses;
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

  public Long getNumNulls() {
    return numNulls;
  }

  public void setNumNulls(long numNulls) {
    this.numNulls = numNulls;
  }

  public Long getNumDVs() {
    return numDVs;
  }

  public void setNumDVs(long numDVs) {
    this.numDVs = numDVs;
  }

  public Double getAvgColLen() {
    return avgColLen;
  }

  public void setAvgColLen(double avgColLen) {
    this.avgColLen = avgColLen;
  }

  public Long getMaxColLen() {
    return maxColLen;
  }

  public void setMaxColLen(long maxColLen) {
    this.maxColLen = maxColLen;
  }

  public Long getNumTrues() {
    return numTrues;
  }

  public void setNumTrues(long numTrues) {
    this.numTrues = numTrues;
  }

  public Long getNumFalses() {
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

  public void setBooleanStats(Long numTrues, Long numFalses, Long numNulls) {
    this.numTrues = numTrues;
    this.numFalses = numFalses;
    this.numNulls = numNulls;
  }

  public void setLongStats(Long numNulls, Long numNDVs, byte[] bitVector, Long lowValue, Long highValue) {
    this.numNulls = numNulls;
    this.numDVs = numNDVs;
    this.bitVector = bitVector;
    this.longLowValue = lowValue;
    this.longHighValue = highValue;
  }

  public void setDoubleStats(Long numNulls, Long numNDVs, byte[] bitVector, Double lowValue, Double highValue) {
    this.numNulls = numNulls;
    this.numDVs = numNDVs;
    this.bitVector = bitVector;
    this.doubleLowValue = lowValue;
    this.doubleHighValue = highValue;
  }

  public void setDecimalStats(
      Long numNulls, Long numNDVs, byte[] bitVector, String lowValue, String highValue) {
    this.numNulls = numNulls;
    this.numDVs = numNDVs;
    this.bitVector = bitVector;
    this.decimalLowValue = lowValue;
    this.decimalHighValue = highValue;
  }

  public void setStringStats(Long numNulls, Long numNDVs, byte[] bitVector, Long maxColLen, Double avgColLen) {
    this.numNulls = numNulls;
    this.numDVs = numNDVs;
    this.bitVector = bitVector;
    this.maxColLen = maxColLen;
    this.avgColLen = avgColLen;
  }

  public void setBinaryStats(Long numNulls, Long maxColLen, Double avgColLen) {
    this.numNulls = numNulls;
    this.maxColLen = maxColLen;
    this.avgColLen = avgColLen;
  }

  public void setDateStats(Long numNulls, Long numNDVs, byte[] bitVector, Long lowValue, Long highValue) {
    this.numNulls = numNulls;
    this.numDVs = numNDVs;
    this.bitVector = bitVector;
    this.longLowValue = lowValue;
    this.longHighValue = highValue;
  }

  public Long getLongLowValue() {
    return longLowValue;
  }

  public void setLongLowValue(Long longLowValue) {
    this.longLowValue = longLowValue;
  }

  public Long getLongHighValue() {
    return longHighValue;
  }

  public void setLongHighValue(Long longHighValue) {
    this.longHighValue = longHighValue;
  }

  public Double getDoubleLowValue() {
    return doubleLowValue;
  }

  public void setDoubleLowValue(Double doubleLowValue) {
    this.doubleLowValue = doubleLowValue;
  }

  public Double getDoubleHighValue() {
    return doubleHighValue;
  }

  public void setDoubleHighValue(Double doubleHighValue) {
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

  public byte[] getBitVector() {
    return bitVector;
  }

  public void setBitVector(byte[] bitVector) {
    this.bitVector = bitVector;
  }
}
