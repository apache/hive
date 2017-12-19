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
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJdbcConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;

//Copied from JoinExtractFilterRule, need to refactor
public final class MyJoinExtractFilterRule extends RelOptRule {
  //~ Static fields/initializers ---------------------------------------------


  //~ Constructors -----------------------------------------------------------

  /**
   * Creates an JoinExtractFilterRule.
   */
  public MyJoinExtractFilterRule() {
    super(operand(HiveJoin.class,
        operand(HiveJdbcConverter.class, any()),
        operand(HiveJdbcConverter.class, any())));
  }
  
  @Override
  public boolean matches(RelOptRuleCall call) {
    final Join join = call.rel(0);
    return MyAbstractSplitFilter.canSplitFilter(join.getCondition());
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final Join join = call.rel(0);

    if (join.getJoinType() != JoinRelType.INNER) {
      return;
    }

    if (join.getCondition().isAlwaysTrue()) {
      return;
    }

    if (!join.getSystemFieldList().isEmpty()) {
      // FIXME Enable this rule for joins with system fields
      return;
    }

    // NOTE jvs 14-Mar-2006:  See JoinCommuteRule for why we
    // preserve attribute semiJoinDone here.

    RelNode cartesianJoinRel =
        join.copy(
            join.getTraitSet(),
            join.getCluster().getRexBuilder().makeLiteral(true),
            join.getLeft(),
            join.getRight(),
            join.getJoinType(),
            join.isSemiJoinDone());

    final RelFactories.FilterFactory factory =
        HiveRelFactories.HIVE_FILTER_FACTORY;
    RelNode filterRel =
        factory.createFilter(cartesianJoinRel, join.getCondition());

    call.transformTo(filterRel);
  }
}

// End JoinExtractFilterRule.java
