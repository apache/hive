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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAntiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnion;

/**
 * This class provides access to Calcite's {@link PruneEmptyRules}.
 * The instances of the rules use {@link org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelBuilder}.
 */
public class HiveRemoveEmptySingleRules extends PruneEmptyRules {

  public static final RelOptRule PROJECT_INSTANCE =
      new RemoveEmptySingleRuleConfig()
                  .withOperandFor(HiveProject.class, project -> true)
                  .withDescription("HivePruneEmptyProject")
                  .toRule();

  public static final RelOptRule FILTER_INSTANCE =
      new RemoveEmptySingleRuleConfig()
                  .withOperandFor(HiveFilter.class, singleRel -> true)
                  .withDescription("HivePruneEmptyFilter")
                  .toRule();

  public static final RelOptRule JOIN_LEFT_INSTANCE = getJoinLeftInstance(HiveJoin.class);
  public static final RelOptRule SEMI_JOIN_LEFT_INSTANCE = getJoinLeftInstance(HiveSemiJoin.class);

  private static <R extends RelNode> RelOptRule getJoinLeftInstance(Class<R> clazz) {
    return new JoinLeftEmptyRuleConfig()
            .withOperandSupplier(b0 ->
                    b0.operand(clazz).inputs(
                            b1 -> b1.operand(Values.class)
                                    .predicate(Values::isEmpty).noInputs(),
                            b2 -> b2.operand(RelNode.class).anyInputs()))
            .withDescription("HivePruneEmptyJoin(left)")
            .toRule();
  }

  public static final RelOptRule JOIN_RIGHT_INSTANCE = getJoinRightInstance(HiveJoin.class);
  public static final RelOptRule ANTI_JOIN_RIGHT_INSTANCE = getJoinRightInstance(HiveAntiJoin.class);
  public static final RelOptRule SEMI_JOIN_RIGHT_INSTANCE = getJoinRightInstance(HiveSemiJoin.class);

  private static <R extends RelNode> RelOptRule getJoinRightInstance(Class<R> clazz) {
    return new JoinRightEmptyRuleConfig()
            .withOperandSupplier(b0 ->
                    b0.operand(clazz).inputs(
                            b1 -> b1.operand(RelNode.class).anyInputs(),
                            b2 -> b2.operand(Values.class).predicate(Values::isEmpty)
                                    .noInputs()))
            .withDescription("HivePruneEmptyJoin(right)")
            .toRule();
  }

  public static final RelOptRule CORRELATE_RIGHT_INSTANCE = new CorrelateRightEmptyRuleConfig()
      .withOperandSupplier(b0 ->
          b0.operand(Correlate.class).inputs(
              b1 -> b1.operand(RelNode.class).anyInputs(),
              b2 -> b2.operand(Values.class).predicate(Values::isEmpty).noInputs()))
      .withDescription("PruneEmptyCorrelate(right)")
      .toRule();
  public static final RelOptRule CORRELATE_LEFT_INSTANCE = new CorrelateLeftEmptyRuleConfig()
      .withOperandSupplier(b0 ->
          b0.operand(Correlate.class).inputs(
              b1 -> b1.operand(Values.class).predicate(Values::isEmpty).noInputs(),
              b2 -> b2.operand(RelNode.class).anyInputs()))
      .withDescription("PruneEmptyCorrelate(left)")
      .toRule();

  public static final RelOptRule SORT_INSTANCE =
          new RemoveEmptySingleRuleConfig()
                  .withOperandFor(HiveSortLimit.class, singleRel -> true)
                  .withDescription("HivePruneEmptySort")
                  .toRule();

  public static final RelOptRule SORT_FETCH_ZERO_INSTANCE =
          new SortFetchZeroRuleConfig()
                  .withOperandSupplier(b -> b.operand(HiveSortLimit.class).anyInputs())
                  .withDescription("HivePruneSortLimit0")
                  .toRule();

  public static final RelOptRule AGGREGATE_INSTANCE =
          new RemoveEmptySingleRuleConfig()
                  .withOperandFor(HiveAggregate.class, Aggregate::isNotGrandTotal)
                  .withDescription("HivePruneEmptyAggregate")
                  .toRule();

  public static final RelOptRule UNION_INSTANCE =
          new UnionEmptyPruneRuleConfig()
                  .withOperandSupplier(b0 ->
                          b0.operand(HiveUnion.class).unorderedInputs(b1 ->
                                  b1.operand(Values.class)
                                          .predicate(Values::isEmpty).noInputs()))
                  .withDescription("HivePruneEmptyUnionBranch")
                  .toRule();

  private static final class RemoveEmptySingleRuleConfig extends HiveRuleConfig
      implements RemoveEmptySingleRule.RemoveEmptySingleRuleConfig {
  }

  private static final class SortFetchZeroRuleConfig extends HiveRuleConfig
      implements PruneEmptyRules.SortFetchZeroRuleConfig {
  }

  private static final class UnionEmptyPruneRuleConfig extends HiveRuleConfig
      implements PruneEmptyRules.UnionEmptyPruneRuleConfig {
  }

  private static final class JoinLeftEmptyRuleConfig extends HiveRuleConfig
      implements PruneEmptyRules.JoinLeftEmptyRuleConfig {
  }

  private static final class JoinRightEmptyRuleConfig extends HiveRuleConfig
      implements PruneEmptyRules.JoinRightEmptyRuleConfig {
  }

  private static final class CorrelateLeftEmptyRuleConfig extends HiveRuleConfig
      implements PruneEmptyRules.CorrelateLeftEmptyRuleConfig {
  }

  private static final class CorrelateRightEmptyRuleConfig extends HiveRuleConfig
      implements PruneEmptyRules.CorrelateRightEmptyRuleConfig {
  }

}
