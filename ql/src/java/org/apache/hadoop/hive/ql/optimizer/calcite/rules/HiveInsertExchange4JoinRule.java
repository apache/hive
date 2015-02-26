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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalExchange;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinLeafPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelDistribution;

/** Not an optimization rule.
 * Rule to aid in translation from Calcite tree -> Hive tree.
 * Transforms :
 *   Left     Right                  Left                    Right
 *       \   /           ->             \                   /
 *       Join                          HashExchange       HashExchange
 *                                             \         /
 *                                                 Join
 */
public class HiveInsertExchange4JoinRule extends RelOptRule {
  
  protected static transient final Log LOG = LogFactory
      .getLog(HiveInsertExchange4JoinRule.class);

  public HiveInsertExchange4JoinRule() {

    // match join with exactly 2 inputs
    super(RelOptRule.operand(Join.class,
        operand(RelNode.class, any()),
        operand(RelNode.class, any())));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Join join = call.rel(0);
    
    if (call.rel(1) instanceof LogicalExchange &&
        call.rel(2) instanceof LogicalExchange) {
      return;
    }

    JoinPredicateInfo joinPredInfo =
        HiveCalciteUtil.JoinPredicateInfo.constructJoinPredicateInfo(join);

    // get key columns from inputs. Those are the columns on which we will distribute on.
    List<Integer> joinLeftKeyPositions = new ArrayList<Integer>();
    List<Integer> joinRightKeyPositions = new ArrayList<Integer>();
    for (int i = 0; i < joinPredInfo.getEquiJoinPredicateElements().size(); i++) {
      JoinLeafPredicateInfo joinLeafPredInfo = joinPredInfo.
          getEquiJoinPredicateElements().get(i);
      joinLeftKeyPositions.addAll(joinLeafPredInfo.getProjsFromLeftPartOfJoinKeysInChildSchema());
      joinRightKeyPositions.addAll(joinLeafPredInfo.getProjsFromRightPartOfJoinKeysInChildSchema());
    }

    LogicalExchange left = LogicalExchange.create(join.getLeft(), new HiveRelDistribution(RelDistribution.Type.HASH_DISTRIBUTED, joinLeftKeyPositions));
    LogicalExchange right = LogicalExchange.create(join.getRight(), new HiveRelDistribution(RelDistribution.Type.HASH_DISTRIBUTED, joinRightKeyPositions));

    Join newJoin = join.copy(join.getTraitSet(), join.getCondition(),
        left, right, join.getJoinType(), join.isSemiJoinDone());

    call.getPlanner().onCopy(join, newJoin);
    
    call.transformTo(newJoin);
  }

}
