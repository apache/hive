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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories.FilterFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinLeafPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil.JoinPredicateInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.SqlFunctionConverter;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import com.google.common.collect.ImmutableList;

public final class HiveJoinAddNotNullRule extends RelOptRule {

  private static final String NOT_NULL_FUNC_NAME = "isnotnull";
  
  /** The singleton. */
  public static final HiveJoinAddNotNullRule INSTANCE =
      new HiveJoinAddNotNullRule(HiveFilter.DEFAULT_FILTER_FACTORY);

  private final FilterFactory filterFactory;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an HiveJoinAddNotNullRule.
   */
  public HiveJoinAddNotNullRule(FilterFactory filterFactory) {
    super(operand(Join.class,
              operand(RelNode.class, any()),
              operand(RelNode.class, any())));
    this.filterFactory = filterFactory;
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);
    RelNode leftInput = call.rel(1);
    RelNode rightInput = call.rel(2);

    if (join.getJoinType() != JoinRelType.INNER) {
      return;
    }

    if (join.getCondition().isAlwaysTrue()) {
      return;
    }

    JoinPredicateInfo joinPredInfo =
            HiveCalciteUtil.JoinPredicateInfo.constructJoinPredicateInfo(join);

    Set<Integer> joinLeftKeyPositions = new HashSet<Integer>();
    Set<Integer> joinRightKeyPositions = new HashSet<Integer>();
    for (int i = 0; i < joinPredInfo.getEquiJoinPredicateElements().size(); i++) {
      JoinLeafPredicateInfo joinLeafPredInfo = joinPredInfo.
              getEquiJoinPredicateElements().get(i);
      joinLeftKeyPositions.addAll(joinLeafPredInfo.getProjsFromLeftPartOfJoinKeysInChildSchema());
      joinRightKeyPositions.addAll(joinLeafPredInfo.getProjsFromRightPartOfJoinKeysInChildSchema());
    }

    // Build not null conditions
    final RelOptCluster cluster = join.getCluster();
    final RexBuilder rexBuilder = join.getCluster().getRexBuilder();

    final Map<String,RexNode> newLeftConditions = getNotNullConditions(cluster,
            rexBuilder, leftInput, joinLeftKeyPositions);
    final Map<String,RexNode> newRightConditions = getNotNullConditions(cluster,
            rexBuilder, rightInput, joinRightKeyPositions);

    // Nothing will be added to the expression
    if (newLeftConditions == null && newRightConditions == null) {
      return;
    }

    if (newLeftConditions != null) {
      if (leftInput instanceof HiveFilter) {
        leftInput = leftInput.getInput(0);
      }
      leftInput = createHiveFilterConjunctiveCondition(filterFactory, rexBuilder,
              leftInput, newLeftConditions.values());
    }
    if (newRightConditions != null) {
      if (rightInput instanceof HiveFilter) {
        rightInput = rightInput.getInput(0);
      }
      rightInput = createHiveFilterConjunctiveCondition(filterFactory, rexBuilder,
              rightInput, newRightConditions.values());
    }

    Join newJoin = join.copy(join.getTraitSet(), join.getCondition(),
            leftInput, rightInput, join.getJoinType(), join.isSemiJoinDone());

    call.getPlanner().onCopy(join, newJoin);

    call.transformTo(newJoin);
  }
  
  private static Map<String,RexNode> getNotNullConditions(RelOptCluster cluster,
          RexBuilder rexBuilder, RelNode input, Set<Integer> inputKeyPositions) {

    boolean added = false;

    final RelDataType returnType = cluster.getTypeFactory().
            createSqlType(SqlTypeName.BOOLEAN);

    final Map<String,RexNode> newConditions;
    if (input instanceof HiveFilter) {
      newConditions = splitCondition(((HiveFilter) input).getCondition());
    }
    else {
      newConditions = new HashMap<String,RexNode>();
    }
    for (int pos : inputKeyPositions) {
      try {
        RelDataType keyType = input.getRowType().getFieldList().get(pos).getType();
        // Nothing to do if key cannot be null
        if (!keyType.isNullable()) {
          continue;
        }
        SqlOperator funcCall = SqlFunctionConverter.getCalciteOperator(NOT_NULL_FUNC_NAME,
                FunctionRegistry.getFunctionInfo(NOT_NULL_FUNC_NAME).getGenericUDF(),
                ImmutableList.of(keyType), returnType);
        RexNode cond = rexBuilder.makeCall(funcCall, rexBuilder.makeInputRef(input, pos));
        String digest = cond.toString();
        if (!newConditions.containsKey(digest)) {
          newConditions.put(digest,cond);
          added = true;
        }
      } catch (SemanticException e) {
        throw new AssertionError(e.getMessage());
      }
    }
    // Nothing will be added to the expression
    if (!added) {
      return null;
    }
    return newConditions;
  }
  
  private static Map<String,RexNode> splitCondition(RexNode condition) {
    Map<String,RexNode> newConditions = new HashMap<String,RexNode>();
    if (condition.getKind() == SqlKind.AND) {
      for (RexNode node : ((RexCall) condition).getOperands()) {
        newConditions.put(node.toString(), node);
      }
    }
    else {
      newConditions.put(condition.toString(), condition);
    }
    return newConditions;
  }
  
  private static RelNode createHiveFilterConjunctiveCondition(FilterFactory filterFactory,
          RexBuilder rexBuilder, RelNode input, Collection<RexNode> conditions) {
    final RexNode newCondition = RexUtil.composeConjunction(rexBuilder, conditions, false);
    return filterFactory.createFilter(input, newCondition);
  }
}