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

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveRulesRegistry;

import com.google.common.collect.ImmutableList;

public class HiveSemiJoin extends SemiJoin implements HiveRelNode {

  private final RexNode joinFilter;


  public static HiveSemiJoin getSemiJoin(
          RelOptCluster cluster,
          RelTraitSet traitSet,
          RelNode left,
          RelNode right,
          RexNode condition,
          ImmutableIntList leftKeys,
          ImmutableIntList rightKeys) {
    try {
      HiveSemiJoin semiJoin = new HiveSemiJoin(cluster, traitSet, left, right,
              condition, leftKeys, rightKeys);
      return semiJoin;
    } catch (InvalidRelException | CalciteSemanticException e) {
      throw new RuntimeException(e);
    }
  }

  protected HiveSemiJoin(RelOptCluster cluster,
          RelTraitSet traitSet,
          RelNode left,
          RelNode right,
          RexNode condition,
          ImmutableIntList leftKeys,
          ImmutableIntList rightKeys) throws InvalidRelException, CalciteSemanticException {
    super(cluster, traitSet, left, right, condition, leftKeys, rightKeys);
    final List<RelDataTypeField> systemFieldList = ImmutableList.of();
    List<List<RexNode>> joinKeyExprs = new ArrayList<List<RexNode>>();
    List<Integer> filterNulls = new ArrayList<Integer>();
    for (int i=0; i<this.getInputs().size(); i++) {
      joinKeyExprs.add(new ArrayList<RexNode>());
    }
    this.joinFilter = HiveRelOptUtil.splitHiveJoinCondition(systemFieldList, this.getInputs(),
            this.getCondition(), joinKeyExprs, filterNulls, null);
  }

  public RexNode getJoinFilter() {
    return joinFilter;
  }

  @Override
  public SemiJoin copy(RelTraitSet traitSet, RexNode condition,
          RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    try {
      final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
      HiveSemiJoin semijoin = new HiveSemiJoin(getCluster(), traitSet, left, right, condition,
              joinInfo.leftKeys, joinInfo.rightKeys);
      // If available, copy state to registry for optimization rules
      HiveRulesRegistry registry = semijoin.getCluster().getPlanner().getContext().unwrap(HiveRulesRegistry.class);
      if (registry != null) {
        registry.copyPushedPredicates(this, semijoin);
      }
      return semijoin;
    } catch (InvalidRelException | CalciteSemanticException e) {
      // Semantic error not possible. Must be a bug. Convert to
      // internal error.
      throw new AssertionError(e);
    }
  }

  @Override
  public void implement(Implementor implementor) {
  }

}
