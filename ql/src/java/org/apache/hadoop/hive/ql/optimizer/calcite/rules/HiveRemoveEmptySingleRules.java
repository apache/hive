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
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSortLimit;

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

  public static final RelOptRule JOIN_LEFT_INSTANCE = PruneEmptyRules.JoinLeftEmptyRuleConfig.DEFAULT
      .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
      .toRule();

  public static final RelOptRule JOIN_RIGHT_INSTANCE = PruneEmptyRules.JoinRightEmptyRuleConfig.DEFAULT
      .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
      .toRule();

  public static final RelOptRule CORRELATE_RIGHT_INSTANCE = PruneEmptyRules.CorrelateRightEmptyRuleConfig.DEFAULT
      .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
      .toRule();

  public static final RelOptRule CORRELATE_LEFT_INSTANCE = PruneEmptyRules.CorrelateLeftEmptyRuleConfig.DEFAULT
      .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
      .toRule();

  public static final RelOptRule SORT_INSTANCE =
          new RemoveEmptySingleRuleConfig()
                  .withOperandFor(HiveSortLimit.class, singleRel -> true)
                  .withDescription("HivePruneEmptySort")
                  .toRule();

  public static final RelOptRule SORT_FETCH_ZERO_INSTANCE =
          PruneEmptyRules.SortFetchZeroRuleConfig.DEFAULT
                  .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
                  .withOperandSupplier(b -> b.operand(HiveSortLimit.class).anyInputs())
                  .withDescription("HivePruneSortLimit0")
                  .toRule();

  public static final RelOptRule AGGREGATE_INSTANCE =
          new RemoveEmptySingleRuleConfig()
                  .withOperandFor(HiveAggregate.class, Aggregate::isNotGrandTotal)
                  .withDescription("HivePruneEmptyAggregate")
                  .toRule();

  public static final RelOptRule UNION_INSTANCE =
      PruneEmptyRules.UnionEmptyPruneRuleConfig.DEFAULT
          .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
          .withDescription("HivePruneEmptyUnionBranch")
          .toRule();

  private static final class RemoveEmptySingleRuleConfig extends HiveRuleConfig
      implements RemoveEmptySingleRule.RemoveEmptySingleRuleConfig {
  }
}
