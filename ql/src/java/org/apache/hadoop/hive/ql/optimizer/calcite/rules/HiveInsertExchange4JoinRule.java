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
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinLeafPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelCollation;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelDistribution;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortExchange;

import com.google.common.collect.ImmutableList;

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

  /** Rule that creates Exchange operators under a MultiJoin operator. */
  public static final HiveInsertExchange4JoinRule EXCHANGE_BELOW_MULTIJOIN =
      new HiveInsertExchange4JoinRule(MultiJoin.class);

  /** Rule that creates Exchange operators under a Join operator. */
  public static final HiveInsertExchange4JoinRule EXCHANGE_BELOW_JOIN =
      new HiveInsertExchange4JoinRule(Join.class);

  public HiveInsertExchange4JoinRule(Class<? extends RelNode> clazz) {
    // match multijoin or join
    super(RelOptRule.operand(clazz, any()));
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    JoinPredicateInfo joinPredInfo;
    if (call.rel(0) instanceof MultiJoin) {
      MultiJoin multiJoin = call.rel(0);
      joinPredInfo =  HiveCalciteUtil.JoinPredicateInfo.constructJoinPredicateInfo(multiJoin);
    } else if (call.rel(0) instanceof Join) {
      Join join = call.rel(0);
      joinPredInfo =  HiveCalciteUtil.JoinPredicateInfo.constructJoinPredicateInfo(join);
    } else {
      return;
    }

    for (RelNode child : call.rel(0).getInputs()) {
      if (((HepRelVertex)child).getCurrentRel() instanceof Exchange) {
        return;
      }
    }

    // get key columns from inputs. Those are the columns on which we will distribute on.
    // It is also the columns we will sort on.
    List<RelNode> newInputs = new ArrayList<RelNode>();
    for (int i=0; i<call.rel(0).getInputs().size(); i++) {
      List<Integer> joinKeyPositions = new ArrayList<Integer>();
      ImmutableList.Builder<RexNode> keyListBuilder = new ImmutableList.Builder<RexNode>();
      ImmutableList.Builder<RelFieldCollation> collationListBuilder =
              new ImmutableList.Builder<RelFieldCollation>();
      for (int j = 0; j < joinPredInfo.getEquiJoinPredicateElements().size(); j++) {
        JoinLeafPredicateInfo joinLeafPredInfo = joinPredInfo.
            getEquiJoinPredicateElements().get(j);
        for (int pos : joinLeafPredInfo.getProjsJoinKeysInChildSchema(i)) {
          if (!joinKeyPositions.contains(pos)) {
            joinKeyPositions.add(pos);
            collationListBuilder.add(new RelFieldCollation(pos));
            keyListBuilder.add(joinLeafPredInfo.getJoinKeyExprs(i).get(0));
          }
        }
      }
      HiveSortExchange exchange = HiveSortExchange.create(call.rel(0).getInput(i),
              new HiveRelDistribution(RelDistribution.Type.HASH_DISTRIBUTED, joinKeyPositions),
              new HiveRelCollation(collationListBuilder.build()),
              keyListBuilder.build());
      newInputs.add(exchange);
    }

    RelNode newOp;
    if (call.rel(0) instanceof MultiJoin) {
      MultiJoin multiJoin = call.rel(0);
      newOp = multiJoin.copy(multiJoin.getTraitSet(), newInputs);
    } else if (call.rel(0) instanceof Join) {
      Join join = call.rel(0);
      newOp = join.copy(join.getTraitSet(), join.getCondition(),
          newInputs.get(0), newInputs.get(1), join.getJoinType(),
          join.isSemiJoinDone());
    } else {
      return;
    }

    call.getPlanner().onCopy(call.rel(0), newOp);

    call.transformTo(newOp);
  }

}
