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

import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.TableSpec;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.LoadTableDesc.LoadFileType;


/**
 * ConditionalStats.
 *
 */
public class BasicStatsWork implements Serializable {
  private static final long serialVersionUID = 1L;

  private TableSpec tableSpecs;         // source table spec -- for TableScanOperator
  private LoadTableDesc loadTableDesc;  // same as MoveWork.loadTableDesc -- for FileSinkOperator
  private LoadFileDesc loadFileDesc;    // same as MoveWork.loadFileDesc -- for FileSinkOperator
  private String aggKey;                // aggregation key prefix
  private boolean statsReliable;        // are stats completely reliable

  // If stats aggregator is not present, clear the current aggregator stats.
  // For eg. if a merge is being performed, stats already collected by aggregator (numrows etc.)
  // are still valid. However, if a load file is being performed, the old stats collected by
  // aggregator are not valid. It might be a good idea to clear them instead of leaving wrong
  // and old stats.
  // Since HIVE-12661, we maintain the old stats (although may be wrong) for CBO
  // purpose. We use a flag COLUMN_STATS_ACCURATE to
  // show the accuracy of the stats.

  private boolean clearAggregatorStats = false;

  private boolean noStatsAggregator = false;

  private boolean isNoScanAnalyzeCommand = false;

  // sourceTask for TS is not changed (currently) but that of FS might be changed
  // by various optimizers (auto.convert.join, for example)
  // so this is set by DriverContext in runtime
  private transient Task sourceTask;

  private boolean isFollowedByColStats = false;

  // used by FS based stats collector
  private String statsTmpDir;

  public BasicStatsWork() {
  }

  public BasicStatsWork(TableSpec tableSpecs) {
    this.tableSpecs = tableSpecs;
  }

  public BasicStatsWork(LoadTableDesc loadTableDesc) {
    this.loadTableDesc = loadTableDesc;
  }

  public BasicStatsWork(LoadFileDesc loadFileDesc) {
    this.loadFileDesc = loadFileDesc;
  }

  public TableSpec getTableSpecs() {
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

  @Explain(displayName = "Stats Aggregation Key Prefix", explainLevels = { Level.EXTENDED })
  public String getAggKey() {
    return aggKey;
  }

  public String getStatsTmpDir() {
    return statsTmpDir;
  }

  public void setStatsTmpDir(String statsTmpDir) {
    this.statsTmpDir = statsTmpDir;
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

  public Task getSourceTask() {
    return sourceTask;
  }

  public void setSourceTask(Task sourceTask) {
    this.sourceTask = sourceTask;
  }

  public boolean isFollowedByColStats1() {
    return isFollowedByColStats;
  }

  public void setFollowedByColStats1(boolean isFollowedByColStats) {
    this.isFollowedByColStats = isFollowedByColStats;
  }

  public boolean isExplicitAnalyze() {
    // ANALYZE TABLE
    return (getTableSpecs() != null);
  }
  public boolean isTargetRewritten() {
    // ANALYZE TABLE
    if (isExplicitAnalyze()) {
      return true;
    }
    // INSERT OVERWRITE
    if (getLoadTableDesc() != null && getLoadTableDesc().getLoadFileType() == LoadFileType.REPLACE_ALL) {
      return true;
    }
    // CREATE TABLE ... AS
    if (getLoadFileDesc() != null && getLoadFileDesc().getCtasCreateTableDesc() != null) {
      return true;
    }
    // CREATE MV ... AS
    // ALTER MV ... REBUILD
    if (getLoadFileDesc() != null && getLoadFileDesc().getCreateViewDesc() != null) {
      return true;
    }
    return false;
  }

  public String getTableName() {
    BasicStatsWork work = this;
    if (work.getLoadTableDesc() != null) {
      return work.getLoadTableDesc().getTable().getTableName();
    } else if (work.getTableSpecs() != null) {
      return work.getTableSpecs().tableName;
    } else if (getLoadFileDesc().getCtasCreateTableDesc() != null) {
      return getLoadFileDesc().getCtasCreateTableDesc().getTableName();
    } else {
      return getLoadFileDesc().getCreateViewDesc().getViewName();
    }
  }

}
