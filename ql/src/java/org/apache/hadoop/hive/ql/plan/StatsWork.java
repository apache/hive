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
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.session.SessionState;

/**
 * Stats Work, may include basic stats work and column stats desc
 *
 */
@Explain(displayName = "Stats Work", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class StatsWork implements Serializable {

  private static final long serialVersionUID = 1L;
  // this is for basic stats
  private BasicStatsWork basicStatsWork;
  private BasicStatsNoJobWork basicStatsNoJobWork;
  private ColumnStatsDesc colStats;
  private static final int LIMIT = -1;

  private String currentDatabase;
  private boolean statsReliable;
  private Table table;
  private boolean truncate;
  private boolean footerScan;
  private Set<Partition> partitions = new HashSet<>();

  public StatsWork(Table table, BasicStatsWork basicStatsWork, HiveConf hconf) {
    super();
    this.table = table;
    this.basicStatsWork = basicStatsWork;
    this.currentDatabase = SessionState.get().getCurrentDatabase();
    statsReliable = hconf.getBoolVar(ConfVars.HIVE_STATS_RELIABLE);
    basicStatsWork.setStatsReliable(statsReliable);
  }

  public StatsWork(Table table, HiveConf hconf) {
    super();
    this.table = table;
    this.currentDatabase = SessionState.get().getCurrentDatabase();
    statsReliable = hconf.getBoolVar(ConfVars.HIVE_STATS_RELIABLE);
  }

  @Override
  public String toString() {
    return String.format("StatWork; fetch: %s", getfWork());
  }

  FetchWork getfWork() {
    return colStats == null ? null : colStats.getFWork();
  }

  @Explain(displayName = "Column Stats Desc")
  public ColumnStatsDesc getColStats() {
    return colStats;
  }

  public void setColStats(ColumnStatsDesc colStats) {
    this.colStats = colStats;
  }

  // unused / unknown reason
  @Deprecated
  public static int getLimit() {
    return LIMIT;
  }

  @Explain(displayName = "Basic Stats Work", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public BasicStatsWork getBasicStatsWork() {
    return basicStatsWork;
  }

  // only explain uses it
  @Explain(displayName = "Basic Stats NoJob Work", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public BasicStatsNoJobWork getBasicStatsNoJobWork() {
    return basicStatsNoJobWork;
  }

  public void setSourceTask(Task<?> sourceTask) {
    basicStatsWork.setSourceTask(sourceTask);
  }

  public String getCurrentDatabaseName() {
    return currentDatabase;
  }

  public boolean hasColStats() {
    return colStats != null;
  }

  public Table getTable() {
    return table;
  }

  public void collectStatsFromAggregator(IStatsGatherDesc conf) {
    // AggKey in StatsWork is used for stats aggregation while StatsAggPrefix
    // in FileSinkDesc is used for stats publishing. They should be consistent.
    basicStatsWork.setAggKey(conf.getStatsAggPrefix());
    basicStatsWork.setStatsTmpDir(conf.getTmpStatsDir());
    basicStatsWork.setStatsReliable(statsReliable);
  }

  public void truncateExisting(boolean truncate) {
    this.truncate = truncate;
  }


  public void setFooterScan() {
    basicStatsNoJobWork = new BasicStatsNoJobWork(table.getTableSpec());
    basicStatsNoJobWork.setStatsReliable(getStatsReliable());
    footerScan = true;
  }

  public void addInputPartitions(Set<Partition> partitions) {
    this.partitions.addAll(partitions);
  }

  public Set<Partition> getPartitions() {
    return partitions;
  }

  public boolean isFooterScan() {
    return footerScan;
  }

  public boolean getStatsReliable() {
    return statsReliable;
  }

  public String getFullTableName() {
    return table.getDbName() + "." + table.getTableName();
  }

  public Task getSourceTask() {
    return basicStatsWork == null ? null : basicStatsWork.getSourceTask();
  }

  public String getAggKey() {
    return basicStatsWork.getAggKey();
  }

  public boolean isAggregating() {
    return basicStatsWork != null && basicStatsWork.getAggKey() != null;
  }
}
