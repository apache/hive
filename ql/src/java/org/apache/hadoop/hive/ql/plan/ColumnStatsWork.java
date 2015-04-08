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

import org.apache.hadoop.hive.ql.exec.ListSinkOperator;
import org.apache.hadoop.hive.ql.plan.Explain.Level;


/**
 * ColumnStats Work.
 *
 */
@Explain(displayName = "Column Stats Work", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
public class ColumnStatsWork implements Serializable {
  private static final long serialVersionUID = 1L;
  private FetchWork fWork;
  private ColumnStatsDesc colStats;
  private static final int LIMIT = -1;


  public ColumnStatsWork() {
  }

  public ColumnStatsWork(FetchWork work, ColumnStatsDesc colStats) {
    this.fWork = work;
    this.setColStats(colStats);
  }

  @Override
  public String toString() {
    String ret;
    ret = fWork.toString();
    return ret;
  }

  public FetchWork getfWork() {
    return fWork;
  }

  public void setfWork(FetchWork fWork) {
    this.fWork = fWork;
  }

  @Explain(displayName = "Column Stats Desc")
  public ColumnStatsDesc getColStats() {
    return colStats;
  }

  public void setColStats(ColumnStatsDesc colStats) {
    this.colStats = colStats;
  }

  public ListSinkOperator getSink() {
    return fWork.getSink();
  }

  public void initializeForFetch() {
    fWork.initializeForFetch();
  }

  public int getLeastNumRows() {
    return fWork.getLeastNumRows();
  }

  public static int getLimit() {
    return LIMIT;
  }

}
