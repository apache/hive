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

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.stats.StatsUtils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Statistics. Describes the output of an operator in terms of size, rows, etc
 * based on estimates.
 */
@SuppressWarnings("serial")
public class Statistics implements Serializable {

  public enum State {
    COMPLETE, PARTIAL, NONE
  }

  private long numRows;
  private long dataSize;
  private State basicStatsState;
  private Map<String, ColStatistics> columnStats;
  private State columnStatsState;

  public Statistics() {
    this(0, 0);
  }

  public Statistics(long nr, long ds) {
    this.setNumRows(nr);
    this.setDataSize(ds);
    this.basicStatsState = State.NONE;
    this.columnStats = null;
    this.columnStatsState = State.NONE;
  }

  public long getNumRows() {
    return numRows;
  }

  public void setNumRows(long numRows) {
    this.numRows = numRows;
    updateBasicStatsState();
  }

  public long getDataSize() {
    return dataSize;
  }

  public void setDataSize(long dataSize) {
    this.dataSize = dataSize;
    updateBasicStatsState();
  }

  private void updateBasicStatsState() {
    if (numRows <= 0 && dataSize <= 0) {
      this.basicStatsState = State.NONE;
    } else if (numRows <= 0 || dataSize <= 0) {
      this.basicStatsState = State.PARTIAL;
    } else {
      this.basicStatsState = State.COMPLETE;
    }
  }

  public State getBasicStatsState() {
    return basicStatsState;
  }

  public void setBasicStatsState(State basicStatsState) {
    this.basicStatsState = basicStatsState;
  }

  public State getColumnStatsState() {
    return columnStatsState;
  }

  public void setColumnStatsState(State columnStatsState) {
    this.columnStatsState = columnStatsState;
  }

  @Override
  @Explain(displayName = "")
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(" numRows: ");
    sb.append(numRows);
    sb.append(" dataSize: ");
    sb.append(dataSize);
    sb.append(" basicStatsState: ");
    sb.append(basicStatsState);
    sb.append(" colStatsState: ");
    sb.append(columnStatsState);
    return sb.toString();
  }

  public String extendedToString() {
    StringBuilder sb = new StringBuilder();
    sb.append(" numRows: ");
    sb.append(numRows);
    sb.append(" dataSize: ");
    sb.append(dataSize);
    sb.append(" basicStatsState: ");
    sb.append(basicStatsState);
    sb.append(" colStatsState: ");
    sb.append(columnStatsState);
    sb.append(" colStats: ");
    sb.append(columnStats);
    return sb.toString();
  }

  @Override
  public Statistics clone() throws CloneNotSupportedException {
    Statistics clone = new Statistics(numRows, dataSize);
    clone.setBasicStatsState(basicStatsState);
    clone.setColumnStatsState(columnStatsState);
    if (columnStats != null) {
      Map<String, ColStatistics> cloneColStats = Maps.newHashMap();
      for (Map.Entry<String, ColStatistics> entry : columnStats.entrySet()) {
        cloneColStats.put(entry.getKey(), entry.getValue().clone());
      }
      clone.setColumnStats(cloneColStats);
    }
    return clone;
  }

  public void addToNumRows(long nr) {
    numRows += nr;
    updateBasicStatsState();
  }

  public void addToDataSize(long rds) {
    dataSize += rds;
    updateBasicStatsState();
  }

  public void setColumnStats(Map<String, ColStatistics> colStats) {
    this.columnStats = colStats;
  }

  public void setColumnStats(List<ColStatistics> colStats) {
    columnStats = Maps.newHashMap();
    addToColumnStats(colStats);
  }

  public void addToColumnStats(List<ColStatistics> colStats) {

    if (columnStats == null) {
      columnStats = Maps.newHashMap();
    }

    if (colStats != null) {
      for (ColStatistics cs : colStats) {
        ColStatistics updatedCS = null;
        if (cs != null) {

          String key = cs.getFullyQualifiedColName();
          // if column statistics for a column is already found then merge the statistics
          if (columnStats.containsKey(key) && columnStats.get(key) != null) {
            updatedCS = columnStats.get(key);
            updatedCS.setAvgColLen(Math.max(updatedCS.getAvgColLen(), cs.getAvgColLen()));
            updatedCS.setNumNulls(updatedCS.getNumNulls() + cs.getNumNulls());
            updatedCS.setCountDistint(Math.max(updatedCS.getCountDistint(), cs.getCountDistint()));
            columnStats.put(key, updatedCS);
          } else {
            columnStats.put(key, cs);
          }
        }
      }
    }
  }

  //                  newState
  //                  -----------------------------------------
  // columnStatsState | COMPLETE          PARTIAL      NONE    |
  //                  |________________________________________|
  //         COMPLETE | COMPLETE          PARTIAL      PARTIAL |
  //          PARTIAL | PARTIAL           PARTIAL      PARTIAL |
  //             NONE | COMPLETE          PARTIAL      NONE    |
  //                  -----------------------------------------
  public void updateColumnStatsState(State newState) {
    if (newState.equals(State.PARTIAL)) {
      columnStatsState = State.PARTIAL;
    }

    if (newState.equals(State.NONE)) {
      if (columnStatsState.equals(State.NONE)) {
        columnStatsState = State.NONE;
      } else {
        columnStatsState = State.PARTIAL;
      }
    }

    if (newState.equals(State.COMPLETE)) {
      if (columnStatsState.equals(State.PARTIAL)) {
        columnStatsState = State.PARTIAL;
      } else {
        columnStatsState = State.COMPLETE;
      }
    }
  }

  public long getAvgRowSize() {
    if (numRows != 0) {
      return dataSize / numRows;
    }

    return dataSize;
  }

  public ColStatistics getColumnStatisticsFromFQColName(String fqColName) {
    if (columnStats != null) {
      return columnStats.get(fqColName);
    }
    return null;
  }

  public ColStatistics getColumnStatisticsFromColName(String colName) {
    for (ColStatistics cs : columnStats.values()) {
      if (cs.getColumnName().equalsIgnoreCase(colName)) {
        return cs;
      }
    }

    return null;
  }

  public ColStatistics getColumnStatisticsForColumn(String tabAlias, String colName) {
    String fqColName = StatsUtils.getFullyQualifiedColumnName(tabAlias, colName);
    return getColumnStatisticsFromFQColName(fqColName);
  }

  public List<ColStatistics> getColumnStats() {
    if (columnStats != null) {
      return Lists.newArrayList(columnStats.values());
    }
    return null;
  }

}
