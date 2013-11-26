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

  private String tabAlias;
  private String colName;
  private String colType;
  private String fqColName;
  private long countDistint;
  private long numNulls;
  private double avgColLen;
  private long numTrues;
  private long numFalses;

  public ColStatistics(String tabAlias, String colName, String colType) {
    this.setTableAlias(tabAlias);
    this.setColumnName(colName);
    this.setColumnType(colType);
    this.setFullyQualifiedColName(StatsUtils.getFullyQualifiedColumnName(tabAlias, colName));
  }

  public ColStatistics() {
    this(null, null, null);
  }

  public String getColumnName() {
    return colName;
  }

  public void setColumnName(String colName) {
    this.colName = colName;
    this.fqColName = StatsUtils.getFullyQualifiedColumnName(tabAlias, colName);
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

  public String getFullyQualifiedColName() {
    return fqColName;
  }

  public void setFullyQualifiedColName(String fqColName) {
    this.fqColName = fqColName;
  }

  public String getTableAlias() {
    return tabAlias;
  }

  public void setTableAlias(String tabName) {
    this.tabAlias = tabName;
    this.fqColName = StatsUtils.getFullyQualifiedColumnName(tabName, colName);
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


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(" fqColName: ");
    sb.append(fqColName);
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
    return sb.toString();
  }

  @Override
  public ColStatistics clone() throws CloneNotSupportedException {
    ColStatistics clone = new ColStatistics(tabAlias, colName, colType);
    clone.setFullyQualifiedColName(fqColName);
    clone.setAvgColLen(avgColLen);
    clone.setCountDistint(countDistint);
    clone.setNumNulls(numNulls);
    clone.setNumTrues(numTrues);
    clone.setNumFalses(numFalses);
    return clone;
  }

}
