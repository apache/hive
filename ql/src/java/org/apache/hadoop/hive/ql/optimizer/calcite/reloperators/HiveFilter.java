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
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories.FilterFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.cost.HiveCost;

public class HiveFilter extends Filter implements HiveRelNode {

  public static final FilterFactory DEFAULT_FILTER_FACTORY = new HiveFilterFactoryImpl();

  public HiveFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(cluster, TraitsUtil.getDefaultTraitSet(cluster), child, condition);
  }

  @Override
  public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    assert traitSet.containsIfApplicable(HiveRelNode.CONVENTION);
    return new HiveFilter(getCluster(), traitSet, input, getCondition());
  }

  @Override
  public void implement(Implementor implementor) {
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return HiveCost.FACTORY.makeZeroCost();
  }

  /**
   * Implementation of {@link FilterFactory} that returns
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter}
   * .
   */
  private static class HiveFilterFactoryImpl implements FilterFactory {
    @Override
    public RelNode createFilter(RelNode child, RexNode condition) {
      RelOptCluster cluster = child.getCluster();
      HiveFilter filter = new HiveFilter(cluster, TraitsUtil.getDefaultTraitSet(cluster), child, condition);
      return filter;
    }
  }
}
