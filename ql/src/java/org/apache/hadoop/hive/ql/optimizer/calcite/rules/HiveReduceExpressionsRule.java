/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;

/**
 * Collection of planner rules that apply various simplifying transformations on
 * RexNode trees. Currently, there are two transformations:
 *
 * <ul>
 * <li>Constant reduction, which evaluates constant subtrees, replacing them
 * with a corresponding RexLiteral
 * <li>Removal of redundant casts, which occurs when the argument into the cast
 * is the same as the type of the resulting cast expression
 * </ul>
 */
public final class HiveReduceExpressionsRule {

  private HiveReduceExpressionsRule() {
    throw new IllegalStateException("Instantiation not allowed");
  }

  /**
   * Singleton rule that reduces constants inside a
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter}.
   */
  public static final RelOptRule FILTER_INSTANCE =
      ReduceExpressionsRule.FilterReduceExpressionsRule.Config.DEFAULT
          .withOperandFor(HiveFilter.class)
          .withMatchNullability(false)
          .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
          .as(ReduceExpressionsRule.FilterReduceExpressionsRule.Config.class)
          .toRule();

  /**
   * Singleton rule that reduces constants inside a
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject}.
   */
  public static final RelOptRule PROJECT_INSTANCE =
      ReduceExpressionsRule.ProjectReduceExpressionsRule.Config.DEFAULT
          .withOperandFor(HiveProject.class)
          .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
          .as(ReduceExpressionsRule.ProjectReduceExpressionsRule.Config.class)
          .toRule();

  /**
   * Singleton rule that reduces constants inside a
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin}.
   */
  public static final RelOptRule JOIN_INSTANCE =
      ReduceExpressionsRule.JoinReduceExpressionsRule.Config.DEFAULT
          .withOperandFor(HiveJoin.class)
          .withMatchNullability(false)
          .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
          .as(ReduceExpressionsRule.JoinReduceExpressionsRule.Config.class)
          .toRule();

  /**
   * Singleton rule that reduces constants inside a
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin}.
   */
  public static final RelOptRule SEMIJOIN_INSTANCE =
      ReduceExpressionsRule.JoinReduceExpressionsRule.Config.DEFAULT
          .withOperandFor(HiveSemiJoin.class)
          .withMatchNullability(false)
          .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
          .as(ReduceExpressionsRule.JoinReduceExpressionsRule.Config.class)
          .toRule();

}

// End HiveReduceExpressionsRule.java
