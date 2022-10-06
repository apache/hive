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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil;

public final class HiveFilterJoinRule {

  private static final java.util.function.Predicate<Join> IS_REWRITABLE_JOIN =
      j -> HiveRelOptUtil.isRewritablePKFKJoin(j, j.getLeft(), j.getRight(),
          j.getCluster().getMetadataQuery()).rewritable;

  private static final java.util.function.Predicate<Join> IS_DETERMINISTIC_JOIN =
      j -> RelOptUtil.conjunctions(j.getCondition()).stream().allMatch(HiveCalciteUtil::isDeterministic);

  /**
   * Rule that tries to push filter expressions into a join condition and into
   * the inputs of the join, iff the join is a column appending
   * non-filtering join.
   */
  // @formatter:off
  public static final RelOptRule FILTER_ON_NON_FILTERING_JOIN =
      FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig.DEFAULT
          .withOperandSupplier(
              b0 -> b0.operand(Filter.class).predicate(f -> HiveCalciteUtil.isDeterministic(f.getCondition())).oneInput(
                  b1 -> b1.operand(Join.class).predicate(IS_REWRITABLE_JOIN).anyInputs()))
          .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
          .withDescription("HiveFilterNonFilteringJoinMergeRule")
          .toRule();
  // @formatter:on

  // @formatter:off
  public static final RelOptRule FILTER_ON_JOIN =
      FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig.DEFAULT
          .withOperandSupplier(
              b0 -> b0.operand(Filter.class).predicate(f -> HiveCalciteUtil.isDeterministic(f.getCondition())).oneInput(
                  b1 -> b1.operand(Join.class).anyInputs()))
          .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
          .withDescription("HiveFilterJoinMergeRule")
          .toRule();
  // @formatter:on

  // @formatter:off
  public static final RelOptRule JOIN =
      FilterJoinRule.JoinConditionPushRule.JoinConditionPushRuleConfig.DEFAULT
          .withOperandSupplier(b0 -> b0.operand(Join.class).predicate(IS_DETERMINISTIC_JOIN).anyInputs())
          .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
          .withDescription("HiveFilterJoinTransposeRule")
          .toRule();
  // @formatter:on

}

// End PushFilterPastJoinRule.java

