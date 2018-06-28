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
import java.util.Map;

import org.apache.hadoop.hive.ql.plan.Explain.Level;
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
    NONE, PARTIAL, COMPLETE;

    public boolean morePreciseThan(State other) {
      return ordinal() >= other.ordinal();
    }

    public State merge(State otherState) {
      if (this == otherState) {
        return this;
      }
      return PARTIAL;
    }
  }

  private long numRows;
  private long runTimeNumRows;
  private long dataSize;
  private State basicStatsState;
  private Map<String, ColStatistics> columnStats;
  private State columnStatsState;
  private boolean runtimeStats;

  public Statistics() {
    this(0, 0);
  }

  public Statistics(long nr, long ds) {
    numRows = nr;
    dataSize = ds;
    runTimeNumRows = -1;
    columnStats = null;
    columnStatsState = State.NONE;

    updateBasicStatsState();
  }

  public long getNumRows() {
    return numRows;
  }

  public void setNumRows(long numRows) {
    this.numRows = numRows;
    if (dataSize == 0) {
      updateBasicStatsState();
    }
  }

  public long getDataSize() {
    return dataSize;
  }

  public void setDataSize(long dataSize) {
    this.dataSize = dataSize;
    if (dataSize == 0) {
      updateBasicStatsState();
    }
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
    updateBasicStatsState();
    if (this.basicStatsState.morePreciseThan(basicStatsState)) {
      this.basicStatsState = basicStatsState;
    }
  }

  public State getColumnStatsState() {
    return columnStatsState;
  }

  public void setColumnStatsState(State columnStatsState) {
    this.columnStatsState = columnStatsState;
  }

  @Override
  @Explain(displayName = "Statistics")
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (runtimeStats) {
      sb.append("(RUNTIME) ");
    }
    sb.append("Num rows: ");
    sb.append(numRows);
    if (runTimeNumRows >= 0) {
      sb.append("/" + runTimeNumRows);
    }
    sb.append(" Data size: ");
    sb.append(dataSize);
    sb.append(" Basic stats: ");
    sb.append(basicStatsState);
    sb.append(" Column stats: ");
    sb.append(columnStatsState);
    return sb.toString();
  }

  @Explain(displayName = "Statistics", explainLevels = { Level.USER })
  public String toUserLevelExplainString() {
    StringBuilder sb = new StringBuilder();
    if (runtimeStats) {
      sb.append("runtime: ");
    }
    sb.append("rows=");
    sb.append(numRows);
    if (runTimeNumRows >= 0) {
      sb.append("/" + runTimeNumRows);
    }
    sb.append(" width=");
    // just to be safe about numRows
    if (numRows != 0) {
      sb.append(dataSize / numRows);
    } else {
      sb.append("-1");
    }
    return sb.toString();
  }

  public String extendedToString() {
    StringBuilder sb = new StringBuilder();
    if (runtimeStats) {
      sb.append(" (runtime) ");
    }
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
  public Statistics clone() {
    Statistics clone = new Statistics(numRows, dataSize);
    clone.setRunTimeNumRows(runTimeNumRows);
    clone.setBasicStatsState(basicStatsState);
    clone.setColumnStatsState(columnStatsState);
    if (columnStats != null) {
      Map<String, ColStatistics> cloneColStats = Maps.newHashMap();
      for (Map.Entry<String, ColStatistics> entry : columnStats.entrySet()) {
        cloneColStats.put(entry.getKey(), entry.getValue().clone());
      }
      clone.setColumnStats(cloneColStats);
    }
    // TODO: this boolean flag is set only by RS stats annotation at this point
    //clone.setRuntimeStats(runtimeStats);
    return clone;
  }

  public void addBasicStats(Statistics stats) {
    dataSize += stats.dataSize;
    numRows += stats.numRows;
    basicStatsState = inferColumnStatsState(basicStatsState, stats.basicStatsState);
  }

  @Deprecated
  public void addToDataSize(long rds) {
    dataSize += rds;
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

          String key = cs.getColumnName();
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

  public void updateColumnStatsState(State newState) {
    this.columnStatsState = inferColumnStatsState(columnStatsState, newState);
  }

  //                  newState
  //                  -----------------------------------------
  // columnStatsState | COMPLETE          PARTIAL      NONE    |
  //                  |________________________________________|
  //         COMPLETE | COMPLETE          PARTIAL      PARTIAL |
  //          PARTIAL | PARTIAL           PARTIAL      PARTIAL |
  //             NONE | COMPLETE          PARTIAL      NONE    |
  //                  -----------------------------------------
  public static State inferColumnStatsState(State prevState, State newState) {
    if (newState.equals(State.PARTIAL)) {
      return State.PARTIAL;
    }

    if (newState.equals(State.NONE)) {
      if (prevState.equals(State.NONE)) {
        return State.NONE;
      } else {
        return State.PARTIAL;
      }
    }

    if (newState.equals(State.COMPLETE)) {
      if (prevState.equals(State.PARTIAL)) {
        return State.PARTIAL;
      } else {
        return State.COMPLETE;
      }
    }

    return prevState;
  }

  public long getAvgRowSize() {
    if (numRows != 0) {
      return dataSize / numRows;
    }

    return dataSize;
  }

  public ColStatistics getColumnStatisticsFromColName(String colName) {
    if (columnStats == null) {
      return null;
    }
    for (ColStatistics cs : columnStats.values()) {
      if (cs.getColumnName().equalsIgnoreCase(colName)) {
        return cs;
      }
    }
    return null;
  }

  public List<ColStatistics> getColumnStats() {
    if (columnStats != null) {
      return Lists.newArrayList(columnStats.values());
    }
    return null;
  }

  public long getRunTimeNumRows() {
    return runTimeNumRows;
  }

  public void setRunTimeNumRows(long runTimeNumRows) {
    this.runTimeNumRows = runTimeNumRows;
  }

  public Statistics scaleToRowCount(long newRowCount, boolean downScaleOnly) {
    Statistics ret = clone();
    if (numRows == 0) {
      return ret;
    }
    if (downScaleOnly && newRowCount >= numRows) {
      return ret;
    }
    // FIXME: using real scaling by new/old ration might yield better results?
    ret.numRows = newRowCount;
    ret.dataSize = StatsUtils.safeMult(getAvgRowSize(), newRowCount);
    return ret;
  }

  public boolean isRuntimeStats() {
    return runtimeStats;
  }

  public void setRuntimeStats(final boolean runtimeStats) {
    this.runtimeStats = runtimeStats;
  }
}
