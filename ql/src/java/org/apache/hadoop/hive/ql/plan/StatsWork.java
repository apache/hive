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

import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer.tableSpec;

/**
 * ConditionalStats.
 *
 */
@Explain(displayName = "Stats-Aggr Operator")
public class StatsWork implements Serializable {
  private static final long serialVersionUID = 1L;

  private tableSpec tableSpecs;        // source table spec -- for TableScanOperator
  private LoadTableDesc loadTableDesc; // same as MoveWork.loadTableDesc -- for FileSinkOperator
  private String aggKey;               // aggregation key prefix

  public StatsWork() {
  }

  public StatsWork(tableSpec tableSpecs) {
    this.tableSpecs = tableSpecs;
  }

  public StatsWork(LoadTableDesc loadTableDesc) {
    this.loadTableDesc = loadTableDesc;
  }

  public tableSpec getTableSpecs() {
    return tableSpecs;
  }

  public LoadTableDesc getLoadTableDesc() {
    return loadTableDesc;
  }

  public void setAggKey(String aggK) {
    aggKey = aggK;
  }

  @Explain(displayName = "Stats Aggregation Key Prefix", normalExplain = false)
  public String getAggKey() {
    return aggKey;
  }

}
