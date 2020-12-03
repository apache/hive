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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc.Mode;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.Statistics.State;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * SetHashGroupByMinReduction determines the min reduction to perform
 * a hash aggregation for a group by.
 */
public class SetHashGroupByMinReduction implements SemanticNodeProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(SetHashGroupByMinReduction.class.getName());

  @SuppressWarnings("unchecked")
  @Override
  public Object process(Node nd, Stack<Node> stack,
                        NodeProcessorCtx procContext, Object... nodeOutputs)
      throws SemanticException {

    GroupByOperator groupByOperator = (GroupByOperator) nd;
    GroupByDesc desc = groupByOperator.getConf();

    if (desc.getMode() != Mode.HASH || groupByOperator.getStatistics().getBasicStatsState() != State.COMPLETE
        || groupByOperator.getStatistics().getColumnStatsState() != State.COMPLETE) {
      return null;
    }

    // compute product of distinct values of grouping columns
    List<ColStatistics> colStats = new ArrayList<>();
    for (int i = 0; i < desc.getKeys().size(); i++) {
      ColumnInfo ci = groupByOperator.getSchema().getSignature().get(i);
      colStats.add(
          groupByOperator.getStatistics().getColumnStatisticsFromColName(ci.getInternalName()));
    }
    Statistics parentStats = groupByOperator.getParentOperators().get(0).getStatistics();
    long ndvProduct = StatsUtils.computeNDVGroupingColumns(
        colStats, parentStats, true);
    // if ndvProduct is 0 then column stats state must be partial and we are missing
    if (ndvProduct == 0) {
      return null;
    }

    long numRows = parentStats.getNumRows();
    if (ndvProduct > numRows) {
      ndvProduct = numRows;
    }

    // change the min reduction for hash group by
    float defaultMinReductionHashAggrFactor = desc.getMinReductionHashAggr();
    float defaultMinReductionHashAggrFactorLowerBound = desc.getMinReductionHashAggrLowerBound();
    float minReductionHashAggrFactor = 1f - ((float) ndvProduct / numRows);
    if (minReductionHashAggrFactor < defaultMinReductionHashAggrFactorLowerBound) {
      minReductionHashAggrFactor = defaultMinReductionHashAggrFactorLowerBound;
    }
    if (minReductionHashAggrFactor < defaultMinReductionHashAggrFactor) {
      desc.setMinReductionHashAggr(minReductionHashAggrFactor);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Minimum reduction for hash group by operator {} set to {}", groupByOperator, minReductionHashAggrFactor);
      }
    }

    return null;
  }

}
