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
package org.apache.hadoop.hive.ql.optimizer.calcite.reloperators;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveRulesRegistry;

import java.util.ArrayList;
import java.util.List;

public class HiveAntiJoin extends Join implements HiveRelNode {

  // The joinFilter holds the residual filter which is used during post processing.
  // These are the join conditions that are not part of the join key.
  private final RexNode joinFilter;

  public static HiveAntiJoin getAntiJoin(
          RelOptCluster cluster,
          RelTraitSet traitSet,
          RelNode left,
          RelNode right,
          RexNode condition) {
    try {
      return new HiveAntiJoin(cluster, traitSet, left, right, condition);
    } catch (CalciteSemanticException e) {
      throw new RuntimeException(e);
    }
  }

  protected HiveAntiJoin(RelOptCluster cluster,
                         RelTraitSet traitSet,
                         RelNode left,
                         RelNode right,
                         RexNode condition) throws CalciteSemanticException {
    super(cluster, traitSet, left, right, condition, JoinRelType.ANTI, Sets.newHashSet());
    final List<RelDataTypeField> systemFieldList = ImmutableList.of();
    List<List<RexNode>> joinKeyExprs = new ArrayList<List<RexNode>>();
    List<Integer> filterNulls = new ArrayList<Integer>();
    for (int i=0; i<this.getInputs().size(); i++) {
      joinKeyExprs.add(new ArrayList<>());
    }
    this.joinFilter = HiveRelOptUtil.splitHiveJoinCondition(systemFieldList, this.getInputs(),
            this.getCondition(), joinKeyExprs, filterNulls, null);
  }

  public RexNode getJoinFilter() {
    return joinFilter;
  }

  @Override
  public HiveAntiJoin copy(RelTraitSet traitSet, RexNode condition,
                           RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    try {
      HiveAntiJoin antiJoin = new HiveAntiJoin(getCluster(), traitSet, left, right, condition);
      // If available, copy state to registry for optimization rules
      HiveRulesRegistry registry = antiJoin.getCluster().getPlanner().getContext().unwrap(HiveRulesRegistry.class);
      if (registry != null) {
        registry.copyPushedPredicates(this, antiJoin);
      }
      return antiJoin;
    } catch (CalciteSemanticException e) {
      // Semantic error not possible. Must be a bug. Convert to
      // internal error.
      throw new AssertionError(e);
    }
  }
}
