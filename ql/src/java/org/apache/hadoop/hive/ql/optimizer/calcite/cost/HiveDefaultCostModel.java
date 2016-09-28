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

import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

/**
 * Default implementation of the cost model.
 * Currently used by MR and Spark execution engines.
 */
public class HiveDefaultCostModel extends HiveCostModel {
  
  private static HiveDefaultCostModel INSTANCE;

  synchronized public static HiveDefaultCostModel getCostModel() {
    if (INSTANCE == null) {
      INSTANCE = new HiveDefaultCostModel();
    }

    return INSTANCE;
  }

  private HiveDefaultCostModel() {
    super(Sets.newHashSet(DefaultJoinAlgorithm.INSTANCE));
  }

  @Override
  public RelOptCost getDefaultCost() {
    return HiveCost.FACTORY.makeZeroCost();
  }

  @Override
  public RelOptCost getScanCost(HiveTableScan ts) {
    return HiveCost.FACTORY.makeZeroCost();
  }

  @Override
  public RelOptCost getAggregateCost(HiveAggregate aggregate) {
    return HiveCost.FACTORY.makeZeroCost();
  }

  /**
   * Default join algorithm. Cost is based on cardinality.
   */
  public static class DefaultJoinAlgorithm implements JoinAlgorithm {

    public static final JoinAlgorithm INSTANCE = new DefaultJoinAlgorithm();
    private static final String ALGORITHM_NAME = "none";


    @Override
    public String toString() {
      return ALGORITHM_NAME;
    }

    @Override
    public boolean isExecutable(HiveJoin join) {
      return true;
    }

    @Override
    public RelOptCost getCost(HiveJoin join) {
      RelMetadataQuery mq = RelMetadataQuery.instance();
      double leftRCount = mq.getRowCount(join.getLeft());
      double rightRCount = mq.getRowCount(join.getRight());
      return HiveCost.FACTORY.makeCost(leftRCount + rightRCount, 0.0, 0.0);
    }

    @Override
    public ImmutableList<RelCollation> getCollation(HiveJoin join) {
      return ImmutableList.of();
    }

    @Override
    public RelDistribution getDistribution(HiveJoin join) {
      return RelDistributions.SINGLETON;
    }

    @Override
    public Double getMemory(HiveJoin join) {
      return null;
    }

    @Override
    public Double getCumulativeMemoryWithinPhaseSplit(HiveJoin join) {
      return null;
    }

    @Override
    public Boolean isPhaseTransition(HiveJoin join) {
      return false;
    }

    @Override
    public Integer getSplitCount(HiveJoin join) {
      return 1;
    }
  }

}
