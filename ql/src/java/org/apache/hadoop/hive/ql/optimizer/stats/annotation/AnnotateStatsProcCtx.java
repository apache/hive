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

package org.apache.hadoop.hive.ql.optimizer.stats.annotation;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;

public class AnnotateStatsProcCtx implements NodeProcessorCtx {

  private ParseContext pctx;
  private HiveConf conf;
  private boolean uniformWithinRange;
  private Statistics andExprStats;
  private Set<String> affectedColumns;


  public AnnotateStatsProcCtx(ParseContext pctx) {
    this.pctx = pctx;
    if(pctx != null) {
      this.conf = pctx.getConf();
      this.uniformWithinRange = HiveConf.getBoolVar(this.conf,
          HiveConf.ConfVars.HIVE_STATS_RANGE_SELECTIVITY_UNIFORM_DISTRIBUTION);
    } else {
      this.conf = null;
      this.uniformWithinRange = false;
    }
    this.andExprStats = null;
    this.affectedColumns = new HashSet<>();
  }

  public HiveConf getConf() {
    return conf;
  }

  public boolean isUniformWithinRange() {
    return uniformWithinRange;
  }

  public ParseContext getParseContext() {
    return pctx;
  }

  public Statistics getAndExprStats() {
    return andExprStats;
  }

  public void setAndExprStats(Statistics andExprStats) {
    this.andExprStats = andExprStats;
  }

  public void clearAffectedColumns() {
    affectedColumns.clear();
  }

  public void addAffectedColumn(ExprNodeColumnDesc column) {
    affectedColumns.add(column.getColumn());
  }

  public void addAffectedColumns(Set<String> columns) {
    affectedColumns.addAll(columns);
  }

  public Set<String> getAffectedColumns() {
    return affectedColumns;
  }

}
