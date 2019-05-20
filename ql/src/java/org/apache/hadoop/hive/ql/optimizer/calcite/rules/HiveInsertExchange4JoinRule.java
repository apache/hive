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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinLeafPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelCollation;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelDistribution;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveMultiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortExchange;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

/** Not an optimization rule.
 * Rule to aid in translation from Calcite tree -&gt; Hive tree.
 * Transforms :
 *   Left     Right                  Left                    Right
 *       \   /           -&gt;             \                   /
 *       Join                          HashExchange       HashExchange
 *                                             \         /
 *                                                 Join
 */
public class HiveInsertExchange4JoinRule extends RelOptRule {

  protected static transient final Logger LOG = LoggerFactory
      .getLogger(HiveInsertExchange4JoinRule.class);

  /** Rule that creates Exchange operators under a MultiJoin operator. */
  public static final HiveInsertExchange4JoinRule EXCHANGE_BELOW_MULTIJOIN =
      new HiveInsertExchange4JoinRule(HiveMultiJoin.class);

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
    if (call.rel(0) instanceof HiveMultiJoin) {
      HiveMultiJoin multiJoin = call.rel(0);
      joinPredInfo = multiJoin.getJoinPredicateInfo();
    } else if (call.rel(0) instanceof HiveJoin) {
      HiveJoin hiveJoin = call.rel(0);
      joinPredInfo = hiveJoin.getJoinPredicateInfo();
    } else if (call.rel(0) instanceof Join) {
      Join join = call.rel(0);
      try {
        joinPredInfo =  HiveCalciteUtil.JoinPredicateInfo.constructJoinPredicateInfo(join);
      } catch (CalciteSemanticException e) {
        throw new RuntimeException(e);
      }
    } else {
      return;
    }

    for (RelNode child : call.rel(0).getInputs()) {
      if (((HepRelVertex)child).getCurrentRel() instanceof Exchange) {
        return;
      }
    }

    // Get key columns from inputs. Those are the columns on which we will distribute on.
    // It is also the columns we will sort on.
    List<RelNode> newInputs = new ArrayList<RelNode>();
    for (int i=0; i<call.rel(0).getInputs().size(); i++) {
      List<Integer> joinKeyPositions = new ArrayList<Integer>();
      ImmutableList.Builder<RexNode> joinExprsBuilder = new ImmutableList.Builder<RexNode>();
      Set<String> keySet = Sets.newHashSet();
      ImmutableList.Builder<RelFieldCollation> collationListBuilder =
              new ImmutableList.Builder<RelFieldCollation>();
      for (int j = 0; j < joinPredInfo.getEquiJoinPredicateElements().size(); j++) {
        JoinLeafPredicateInfo joinLeafPredInfo = joinPredInfo.
            getEquiJoinPredicateElements().get(j);
        for (RexNode joinExprNode : joinLeafPredInfo.getJoinExprs(i)) {
          if (keySet.add(joinExprNode.toString())) {
            joinExprsBuilder.add(joinExprNode);
          }
        }
        for (int pos : joinLeafPredInfo.getProjsJoinKeysInChildSchema(i)) {
          if (!joinKeyPositions.contains(pos)) {
            joinKeyPositions.add(pos);
            collationListBuilder.add(new RelFieldCollation(pos));
          }
        }
      }
      HiveSortExchange exchange = HiveSortExchange.create(call.rel(0).getInput(i),
              new HiveRelDistribution(RelDistribution.Type.HASH_DISTRIBUTED, joinKeyPositions),
              new HiveRelCollation(collationListBuilder.build()),
              joinExprsBuilder.build());
      newInputs.add(exchange);
    }

    RelNode newOp;
    if (call.rel(0) instanceof HiveMultiJoin) {
      HiveMultiJoin multiJoin = call.rel(0);
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
