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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveGroupingID;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;

/**
 * Planner rule that recognizes a {@link HiveAggregate}
 * on top of a {@link HiveProject} and if possible
 * aggregate through the project or removes the project.
 *
 * <p>This is only possible when the grouping expressions and arguments to
 * the aggregate functions are field references (i.e. not expressions).
 *
 * <p>In some cases, this rule has the effect of trimming: the aggregate will
 * use fewer columns than the project did.
 */
public class HiveAggregateProjectMergeRule extends AggregateProjectMergeRule {
  public static final HiveAggregateProjectMergeRule INSTANCE =
      new HiveAggregateProjectMergeRule();

  /** Private constructor. */
  private HiveAggregateProjectMergeRule() {
    super(HiveAggregate.class, HiveProject.class, HiveRelFactories.HIVE_BUILDER);
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final Aggregate aggregate = call.rel(0);
    // Rule cannot be applied if there are GroupingId because it will change the
    // value as the position will be changed.
    for (AggregateCall aggCall : aggregate.getAggCallList()) {
      if (aggCall.getAggregation().equals(HiveGroupingID.INSTANCE)) {
        return false;
      }
    }
    return super.matches(call);
  }

}

// End HiveAggregateProjectMergeRule.java
