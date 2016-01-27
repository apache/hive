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
package org.apache.hadoop.hive.ql.optimizer.calcite.cost;

import java.util.Set;

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

import com.google.common.collect.ImmutableList;

/**
 * Cost model interface.
 */
public abstract class HiveCostModel {

  private static final Logger LOG = LoggerFactory.getLogger(HiveCostModel.class);

  private final Set<JoinAlgorithm> joinAlgorithms;


  public HiveCostModel(Set<JoinAlgorithm> joinAlgorithms) {
    this.joinAlgorithms = joinAlgorithms;
  }

  public abstract RelOptCost getDefaultCost();

  public abstract RelOptCost getAggregateCost(HiveAggregate aggregate);

  public abstract RelOptCost getScanCost(HiveTableScan ts);

  public RelOptCost getJoinCost(HiveJoin join) {
    // Select algorithm with min cost
    JoinAlgorithm joinAlgorithm = null;
    RelOptCost minJoinCost = null;

    if (LOG.isTraceEnabled()) {
      LOG.trace("Join algorithm selection for:\n" + RelOptUtil.toString(join));
    }

    for (JoinAlgorithm possibleAlgorithm : this.joinAlgorithms) {
      if (!possibleAlgorithm.isExecutable(join)) {
        continue;
      }
      RelOptCost joinCost = possibleAlgorithm.getCost(join);
      if (LOG.isTraceEnabled()) {
        LOG.trace(possibleAlgorithm + " cost: " + joinCost);
      }
      if (minJoinCost == null || joinCost.isLt(minJoinCost) ) {
        joinAlgorithm = possibleAlgorithm;
        minJoinCost = joinCost;
      }
    }

    if (LOG.isTraceEnabled()) {
      LOG.trace(joinAlgorithm + " selected");
    }

    join.setJoinAlgorithm(joinAlgorithm);
    join.setJoinCost(minJoinCost);

    return minJoinCost;
  }

  /**
   * Interface for join algorithm.
   */
  public interface JoinAlgorithm {
    public String toString();
    public boolean isExecutable(HiveJoin join);
    public RelOptCost getCost(HiveJoin join);
    public ImmutableList<RelCollation> getCollation(HiveJoin join);
    public RelDistribution getDistribution(HiveJoin join);
    public Double getMemory(HiveJoin join);
    public Double getCumulativeMemoryWithinPhaseSplit(HiveJoin join);
    public Boolean isPhaseTransition(HiveJoin join);
    public Integer getSplitCount(HiveJoin join);
  }

}
