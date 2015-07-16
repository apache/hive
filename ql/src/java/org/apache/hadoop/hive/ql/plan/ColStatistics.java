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

package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.hive.ql.stats.StatsUtils;


public class ColStatistics {

  private String colName;
  private String colType;
  private long countDistint;
  private long numNulls;
  private double avgColLen;
  private long numTrues;
  private long numFalses;
  private Range range;
  private boolean isPrimaryKey;

  public ColStatistics(String colName, String colType) {
    this.setColumnName(colName);
    this.setColumnType(colType);
    this.setPrimaryKey(false);
  }

  public ColStatistics() {
    this(null, null);
  }

  public String getColumnName() {
    return colName;
  }

  public void setColumnName(String colName) {
    this.colName = colName;
  }

  public String getColumnType() {
    return colType;
  }

  public void setColumnType(String colType) {
    this.colType = colType;
  }

  public long getCountDistint() {
    return countDistint;
  }

  public void setCountDistint(long countDistint) {
    this.countDistint = countDistint;
  }

  public long getNumNulls() {
    return numNulls;
  }

  public void setNumNulls(long numNulls) {
    this.numNulls = numNulls;
  }

  public double getAvgColLen() {
    return avgColLen;
  }

  public void setAvgColLen(double avgColLen) {
    this.avgColLen = avgColLen;
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

  public Range getRange() {
    return range;
  }

  public void setRange(Number minVal, Number maxVal) {
    this.range = new Range(minVal, maxVal);
  }

  public void setRange(Range r) {
    this.range = r;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(" colName: ");
    sb.append(colName);
    sb.append(" colType: ");
    sb.append(colType);
    sb.append(" countDistincts: ");
    sb.append(countDistint);
    sb.append(" numNulls: ");
    sb.append(numNulls);
    sb.append(" avgColLen: ");
    sb.append(avgColLen);
    sb.append(" numTrues: ");
    sb.append(numTrues);
    sb.append(" numFalses: ");
    sb.append(numFalses);
    if (range != null) {
      sb.append(" ");
      sb.append(range);
    }
    sb.append(" isPrimaryKey: ");
    sb.append(isPrimaryKey);
    return sb.toString();
  }

  @Override
  public ColStatistics clone() throws CloneNotSupportedException {
    ColStatistics clone = new ColStatistics(colName, colType);
    clone.setAvgColLen(avgColLen);
    clone.setCountDistint(countDistint);
    clone.setNumNulls(numNulls);
    clone.setNumTrues(numTrues);
    clone.setNumFalses(numFalses);
    clone.setPrimaryKey(isPrimaryKey);
    if (range != null ) {
      clone.setRange(range.clone());
    }
    return clone;
  }

  public boolean isPrimaryKey() {
    return isPrimaryKey;
  }

  public void setPrimaryKey(boolean isPrimaryKey) {
    this.isPrimaryKey = isPrimaryKey;
  }

  public static class Range {
    public final Number minValue;
    public final Number maxValue;

    public Range(Number minValue, Number maxValue) {
      super();
      this.minValue = minValue;
      this.maxValue = maxValue;
    }

    @Override
    public Range clone() {
      return new Range(minValue, maxValue);
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("Range: [");
      sb.append(" min: ");
      sb.append(minValue);
      sb.append(" max: ");
      sb.append(maxValue);
      sb.append(" ]");
      return sb.toString();
    }
  }

}
