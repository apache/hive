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

import java.util.List;

import org.apache.hadoop.hive.ql.optimizer.optiq.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.optiq.cost.HiveCost;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.SingleRel;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.relopt.RelOptCost;
import org.eigenbase.relopt.RelOptPlanner;
import org.eigenbase.relopt.RelTraitSet;
import org.eigenbase.rex.RexNode;

public class HiveLimitRel extends SingleRel implements HiveRel {
  private final RexNode offset;
  private final RexNode fetch;

  HiveLimitRel(RelOptCluster cluster, RelTraitSet traitSet, RelNode child, RexNode offset,
      RexNode fetch) {
    super(cluster, TraitsUtil.getDefaultTraitSet(cluster), child);
    this.offset = offset;
    this.fetch = fetch;
    assert getConvention() instanceof HiveRel;
    assert getConvention() == child.getConvention();
  }

  @Override
  public HiveLimitRel copy(RelTraitSet traitSet, List<RelNode> newInputs) {
    return new HiveLimitRel(getCluster(), traitSet, sole(newInputs), offset, fetch);
  }

  public void implement(Implementor implementor) {
  }

  @Override
  public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return HiveCost.FACTORY.makeZeroCost();
  }
}
