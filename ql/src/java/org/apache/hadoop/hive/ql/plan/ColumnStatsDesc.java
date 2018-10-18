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
package org.apache.hadoop.hive.ql.plan;

import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.hive.ql.plan.Explain.Level;

/**
 * Contains the information needed to persist column level statistics
 */
public class ColumnStatsDesc extends DDLDesc implements Serializable, Cloneable {

  private static final long serialVersionUID = 1L;
  private FetchWork fWork;

  private boolean isTblLevel;
  private int numBitVector;
  private boolean needMerge;
  private String tableName;
  private List<String> colName;
  private List<String> colType;


  public ColumnStatsDesc(String tableName, List<String> colName,
      List<String> colType, boolean isTblLevel, int numBitVector, FetchWork fWork1) {
    this.tableName = tableName;
    this.colName = colName;
    this.colType = colType;
    this.isTblLevel = isTblLevel;
    this.numBitVector = numBitVector;
    this.needMerge = this.numBitVector != 0;
    this.fWork = fWork1;
  }

  @Explain(displayName = "Table")
  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  @Explain(displayName = "Is Table Level Stats", explainLevels = { Level.EXTENDED })
  public boolean isTblLevel() {
    return isTblLevel;
  }

  public void setTblLevel(boolean isTblLevel) {
    this.isTblLevel = isTblLevel;
  }

  @Explain(displayName = "Columns")
  public List<String> getColName() {
    return colName;
  }

  public void setColName(List<String> colName) {
    this.colName = colName;
  }

  @Explain(displayName = "Column Types")
  public List<String> getColType() {
    return colType;
  }

  public void setColType(List<String> colType) {
    this.colType = colType;
  }

  public int getNumBitVector() {
    return numBitVector;
  }

  public void setNumBitVector(int numBitVector) {
    this.numBitVector = numBitVector;
  }

  public boolean isNeedMerge() {
    return needMerge;
  }


  public FetchWork getFWork() {
    return fWork;
  }

}
