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

import org.apache.hadoop.hive.ql.optimizer.optiq.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveCost;
import org.eigenbase.rel.FilterRelBase;
import org.eigenbase.rel.RelFactories.FilterFactory;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

public class HiveFilterRel extends FilterRelBase implements HiveRel {

  public static final FilterFactory DEFAULT_FILTER_FACTORY = new HiveFilterFactoryImpl();

  public HiveFilterRel(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
    super(cluster, TraitsUtil.getDefaultTraitSet(cluster), child, condition);
  }

  @Override
  public FilterRelBase copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
    assert traitSet.containsIfApplicable(HiveRel.CONVENTION);
    return new HiveFilterRel(getCluster(), traitSet, input, getCondition());
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
   * {@link org.apache.hadoop.hive.ql.optimizer.optiq.reloperators.HiveFilterRel}
   * .
   */
  private static class HiveFilterFactoryImpl implements FilterFactory {
    @Override
    public RelNode createFilter(RelNode child, RexNode condition) {
      RelOptCluster cluster = child.getCluster();
      HiveFilterRel filter = new HiveFilterRel(cluster, TraitsUtil.getDefaultTraitSet(cluster), child, condition);
      return filter;
    }
  }
}
