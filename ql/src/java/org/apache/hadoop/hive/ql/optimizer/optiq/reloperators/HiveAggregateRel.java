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
package org.apache.hadoop.hive.ql.optimizer.optiq.reloperators;

import java.util.BitSet;
import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.optiq.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveCost;
import org.eigenbase.rel.AggregateCall;
import org.eigenbase.rel.AggregateRelBase;
import org.eigenbase.rel.InvalidRelException;
import org.eigenbase.rel.RelFactories.AggregateFactory;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.metadata.RelMetadataQuery;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;

public class HiveAggregateRel extends AggregateRelBase implements HiveRel {

  public static final HiveAggRelFactory HIVE_AGGR_REL_FACTORY = new HiveAggRelFactory();

  public HiveAggregateRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child,
      BitSet groupSet, List<AggregateCall> aggCalls) throws InvalidRelException {
    super(cluster, TraitsUtil.getDefaultTraitSet(cluster), child, groupSet, aggCalls);
  }

  @Override
  public AggregateRelBase copy(RelTraitSet traitSet, RelNode input, BitSet groupSet,
      List<AggregateCall> aggCalls) {
    try {
      return new HiveAggregateRel(getCluster(), traitSet, input, groupSet, aggCalls);
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
    return HiveCost.FACTORY.makeZeroCost();
  }

  @Override
  public double getRows() {
    return RelMetadataQuery.getDistinctRowCount(this, groupSet, getCluster().getRexBuilder()
        .makeLiteral(true));
  }

  private static class HiveAggRelFactory implements AggregateFactory {

    @Override
    public RelNode createAggregate(RelNode child, BitSet groupSet,
        List<AggregateCall> aggCalls) {
      try {
        return new HiveAggregateRel(child.getCluster(), child.getTraitSet(), child, groupSet, aggCalls);
      } catch (InvalidRelException e) {
          throw new RuntimeException(e);
      }
    }
  }
}
