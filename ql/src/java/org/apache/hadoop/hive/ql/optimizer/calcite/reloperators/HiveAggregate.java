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
package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories.AggregateFactory;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveCost;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveCostUtil;

import com.google.common.collect.ImmutableList;

public class HiveAggregate extends Aggregate implements HiveRelNode {

  public static final HiveAggRelFactory HIVE_AGGR_REL_FACTORY = new HiveAggRelFactory();

  // Whether input is already sorted
  private boolean bucketedInput;

  public HiveAggregate(RelOptCluster cluster, RelTraitSet traitSet, RelNode child,
      boolean indicator, ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) throws InvalidRelException {
    super(cluster, TraitsUtil.getDefaultTraitSet(cluster), child, indicator, groupSet,
            groupSets, aggCalls);
    this.bucketedInput = false;
  }

  @Override
  public Aggregate copy(RelTraitSet traitSet, RelNode input,
          boolean indicator, ImmutableBitSet groupSet,
          List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    try {
      return new HiveAggregate(getCluster(), traitSet, input, indicator, groupSet,
              groupSets, aggCalls);
    } catch (InvalidRelException e) {
      // Semantic error not possible. Must be a bug. Convert to
      // internal error.
      throw new AssertionError(e);
    }
  }

  @Override
  public void implement(Implementor implementor) {
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    // Check whether input is in correct order
    checkInputCorrectBucketing();
    if (this.bucketedInput) {
      return HiveCost.FACTORY.makeZeroCost();
    } else {
      // 1. Sum of input cardinalities
      final Double rCount = RelMetadataQuery.getRowCount(this.getInput());
      if (rCount == null) {
        return null;
      }
      // 2. CPU cost = sorting cost
      final double cpuCost = HiveCostUtil.computeSortCPUCost(rCount);
      // 3. IO cost = cost of writing intermediary results to local FS +
      //              cost of reading from local FS for transferring to GBy +
      //              cost of transferring map outputs to GBy operator
      final Double rAverageSize = RelMetadataQuery.getAverageRowSize(this.getInput());
      if (rAverageSize == null) {
        return null;
      }
      final double ioCost = HiveCostUtil.computeSortIOCost(new Pair<Double,Double>(rCount,rAverageSize));
      // 4. Result
      return HiveCost.FACTORY.makeCost(rCount, cpuCost, ioCost);
    }
  }

  private void checkInputCorrectBucketing() {
    this.bucketedInput = RelMetadataQuery.distribution(this.getInput()).getKeys().
            containsAll(this.getGroupSet().asList());
  }

  @Override
  public double getRows() {
    return RelMetadataQuery.getDistinctRowCount(this, groupSet, getCluster().getRexBuilder()
        .makeLiteral(true));
  }

  private static class HiveAggRelFactory implements AggregateFactory {

    @Override
    public RelNode createAggregate(RelNode child, boolean indicator,
            ImmutableBitSet groupSet, ImmutableList<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
      try {
        return new HiveAggregate(child.getCluster(), child.getTraitSet(), child, indicator,
                groupSet, groupSets, aggCalls);
      } catch (InvalidRelException e) {
          throw new RuntimeException(e);
      }
    }
  }
}
