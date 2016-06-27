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

import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.tableSpec;

/**
 * ConditionalStats.
 *
 */
@Explain(displayName = "Stats-Aggr Operator")
public class StatsWork implements Serializable {
  private static final long serialVersionUID = 1L;

  private tableSpec tableSpecs;         // source table spec -- for TableScanOperator
  private LoadTableDesc loadTableDesc;  // same as MoveWork.loadTableDesc -- for FileSinkOperator
  private LoadFileDesc loadFileDesc;    // same as MoveWork.loadFileDesc -- for FileSinkOperator
  private String aggKey;                // aggregation key prefix
  private boolean statsReliable;        // are stats completely reliable

  // If stats aggregator is not present, clear the current aggregator stats.
  // For eg. if a merge is being performed, stats already collected by aggregator (numrows etc.)
  // are still valid. However, if a load file is being performed, the old stats collected by
  // aggregator are not valid. It might be a good idea to clear them instead of leaving wrong
  // and old stats.
  private boolean clearAggregatorStats = false;

  private boolean noStatsAggregator = false;

  private boolean isNoScanAnalyzeCommand = false;

  private boolean isPartialScanAnalyzeCommand = false;

  // sourceTask for TS is not changed (currently) but that of FS might be changed
  // by various optimizers (auto.convert.join, for example)
  // so this is set by DriverContext in runtime
  private transient Task sourceTask;

  public StatsWork() {
  }

  public StatsWork(tableSpec tableSpecs) {
    this.tableSpecs = tableSpecs;
  }

  public StatsWork(LoadTableDesc loadTableDesc) {
    this.loadTableDesc = loadTableDesc;
  }

  public StatsWork(LoadFileDesc loadFileDesc) {
    this.loadFileDesc = loadFileDesc;
  }

  public StatsWork(boolean statsReliable) {
    this.statsReliable = statsReliable;
  }

  public tableSpec getTableSpecs() {
    return tableSpecs;
  }

  public LoadTableDesc getLoadTableDesc() {
    return loadTableDesc;
  }

  public LoadFileDesc getLoadFileDesc() {
    return loadFileDesc;
  }

  public void setAggKey(String aggK) {
    aggKey = aggK;
  }

  @Explain(displayName = "Stats Aggregation Key Prefix", normalExplain = false)
  public String getAggKey() {
    return aggKey;
  }

  public boolean getNoStatsAggregator() {
    return noStatsAggregator;
  }

  public void setNoStatsAggregator(boolean noStatsAggregator) {
    this.noStatsAggregator = noStatsAggregator;
  }

  public boolean isStatsReliable() {
    return statsReliable;
  }

  public void setStatsReliable(boolean statsReliable) {
    this.statsReliable = statsReliable;
  }

  public boolean isClearAggregatorStats() {
    return clearAggregatorStats;
  }

  public void setClearAggregatorStats(boolean clearAggregatorStats) {
    this.clearAggregatorStats = clearAggregatorStats;
  }

  /**
   * @return the isNoScanAnalyzeCommand
   */
  public boolean isNoScanAnalyzeCommand() {
    return isNoScanAnalyzeCommand;
  }

  /**
   * @param isNoScanAnalyzeCommand the isNoScanAnalyzeCommand to set
   */
  public void setNoScanAnalyzeCommand(boolean isNoScanAnalyzeCommand) {
    this.isNoScanAnalyzeCommand = isNoScanAnalyzeCommand;
  }

  /**
   * @return the isPartialScanAnalyzeCommand
   */
  public boolean isPartialScanAnalyzeCommand() {
    return isPartialScanAnalyzeCommand;
  }

  /**
   * @param isPartialScanAnalyzeCommand the isPartialScanAnalyzeCommand to set
   */
  public void setPartialScanAnalyzeCommand(boolean isPartialScanAnalyzeCommand) {
    this.isPartialScanAnalyzeCommand = isPartialScanAnalyzeCommand;
  }

  public Task getSourceTask() {
    return sourceTask;
  }

  public void setSourceTask(Task sourceTask) {
    this.sourceTask = sourceTask;
  }
}
