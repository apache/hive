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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories.SemiJoinFactory;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;

public class HiveSemiJoin extends SemiJoin implements HiveRelNode {

  public static final SemiJoinFactory HIVE_SEMIJOIN_FACTORY = new HiveSemiJoinFactoryImpl();

  public HiveSemiJoin(RelOptCluster cluster,
          RelTraitSet traitSet,
          RelNode left,
          RelNode right,
          RexNode condition,
          ImmutableIntList leftKeys,
          ImmutableIntList rightKeys) {
    super(cluster, traitSet, left, right, condition, leftKeys, rightKeys);
  }

  @Override
  public SemiJoin copy(RelTraitSet traitSet, RexNode condition,
          RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
    return new HiveSemiJoin(getCluster(), traitSet, left, right, condition,
            joinInfo.leftKeys, joinInfo.rightKeys);
  }

  @Override
  public void implement(Implementor implementor) {
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return RelMetadataQuery.getNonCumulativeCost(this);
  }

  /**
   * Implementation of {@link SemiJoinFactory} that returns
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin}
   * .
   */
  private static class HiveSemiJoinFactoryImpl implements SemiJoinFactory {
    @Override
    public RelNode createSemiJoin(RelNode left, RelNode right,
            RexNode condition) {
      final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
      final RelOptCluster cluster = left.getCluster();
      return new HiveSemiJoin(cluster, left.getTraitSet(), left, right, condition,
          joinInfo.leftKeys, joinInfo.rightKeys);
    }
  }
}
