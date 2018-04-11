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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;

/**
 * This rule will perform a rewriting to prepare the plan for incremental
 * view maintenance in case there is no aggregation operator, so we can
 * avoid the INSERT OVERWRITE and use a INSERT statement instead.
 * In particular, it removes the union branch that reads the old data from
 * the materialization, and keeps the branch that will read the new data.
 */
public class HiveNoAggregateIncrementalRewritingRule extends RelOptRule {

  public static final HiveNoAggregateIncrementalRewritingRule INSTANCE =
      new HiveNoAggregateIncrementalRewritingRule();

  private HiveNoAggregateIncrementalRewritingRule() {
    super(operand(Union.class, any()),
        HiveRelFactories.HIVE_BUILDER, "HiveNoAggregateIncrementalRewritingRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Union union = call.rel(0);
    // First branch is query, second branch is MV
    RelNode newNode = call.builder()
        .push(union.getInput(0))
        .convert(union.getRowType(), false)
        .build();
    call.transformTo(newNode);
  }

}
