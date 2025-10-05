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

import org.apache.calcite.plan.CommonRelSubExprRule;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.hadoop.hive.ql.optimizer.calcite.CommonTableExpressionRegistry;

import com.google.common.collect.Multimap;

import java.util.function.Predicate;

/**
 * Rule for saving relational expressions that appear more than once in a query tree to the planner context.
 */
public final class CommonRelSubExprRegisterRule extends CommonRelSubExprRule {
  public static final CommonRelSubExprRegisterRule JOIN = new CommonRelSubExprRegisterRule(
      o -> o.operand(Join.class).predicate(j -> JoinRelType.INNER == j.getJoinType()).anyInputs());
  public static final CommonRelSubExprRegisterRule AGGREGATE =
      new CommonRelSubExprRegisterRule(o -> o.operand(Aggregate.class).anyInputs());
  public static final CommonRelSubExprRegisterRule FILTER =
      new CommonRelSubExprRegisterRule(o -> o.operand(Filter.class).anyInputs());
  public static final CommonRelSubExprRegisterRule PROJECT = new CommonRelSubExprRegisterRule(
      o -> o.operand(Project.class).predicate(new InterestingRelNodePredicate()).anyInputs());

  private CommonRelSubExprRegisterRule(OperandTransform operands) {
    super(new CommonRelSubExprRegisterRule.Config().withOperandSupplier(operands).as(Config.class));
  }

  @Override
  public void onMatch(final RelOptRuleCall call) {
    CommonTableExpressionRegistry r = call.getPlanner().getContext().unwrap(CommonTableExpressionRegistry.class);
    r.add(call.rel(0));
  }

  /**
   * A predicate determining if a relational expression is interesting.
   *
   * The notion of interesting is loosely defined on purpose since it may change as the API evolves. At the moment an
   * expression is considered interesting if it contains at least one of the following RelNode types:
   * <ul>
   *   <li>{@link Join}</li>
   *   <li>{@link Aggregate}</li>
   *   <li>{@link Filter}</li>
   * </ul>
   */
  private static final class InterestingRelNodePredicate implements Predicate<RelNode> {
    @Override
    public boolean test(final RelNode rel) {
      RelMetadataQuery mq = rel.getCluster().getMetadataQuery();
      Multimap<Class<? extends RelNode>, RelNode> types = mq.getNodeTypes(rel);
      if (types == null) {
        return false;
      }
      return types.keySet().stream().anyMatch(
          t -> Join.class.isAssignableFrom(t) || Aggregate.class.isAssignableFrom(t) || Filter.class.isAssignableFrom(
              t));
    }
  }

  private static class Config extends HiveRuleConfig implements CommonRelSubExprRule.Config {
    @Override
    public RelOptRule toRule() {
      return new CommonRelSubExprRegisterRule(this.operandSupplier());
    }
  }
}
