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

import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.Bug;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;

/**
 * Mostly a copy of {@link org.apache.calcite.rel.rules.FilterMergeRule}.
 * However, it flattens the predicate before creating the new filter.
 */
public class HiveFilterMergeRule extends RelOptRule {

  public static final HiveFilterMergeRule INSTANCE =
      new HiveFilterMergeRule();

  /** Private constructor. */
  private HiveFilterMergeRule() {
    super(operand(HiveFilter.class,
        operand(HiveFilter.class, any())),
        HiveRelFactories.HIVE_BUILDER, null);
    if (Bug.CALCITE_3982_FIXED) {
      throw new AssertionError("Remove logic in HiveFilterMergeRule when [CALCITE-3982] "
          + "has been fixed and use directly Calcite's FilterMergeRule instead.");
    }
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final HiveFilter topFilter = call.rel(0);
    final HiveFilter bottomFilter = call.rel(1);

    RexBuilder rexBuilder = topFilter.getCluster().getRexBuilder();
    RexProgram bottomProgram = createProgram(bottomFilter);
    RexProgram topProgram = createProgram(topFilter);

    RexProgram mergedProgram =
        RexProgramBuilder.mergePrograms(
            topProgram,
            bottomProgram,
            rexBuilder);

    RexNode newCondition = expandLocalRef(rexBuilder,
        mergedProgram.getCondition(), mergedProgram.getExprList());

    final RelBuilder relBuilder = call.builder();
    relBuilder.push(bottomFilter.getInput())
        .filter(newCondition);

    call.transformTo(relBuilder.build());
  }

  /**
   * Creates a RexProgram corresponding to a LogicalFilter
   *
   * @param filterRel the LogicalFilter
   * @return created RexProgram
   */
  private RexProgram createProgram(Filter filterRel) {
    RexProgramBuilder programBuilder =
        new RexProgramBuilder(
            filterRel.getRowType(),
            filterRel.getCluster().getRexBuilder());
    programBuilder.addIdentity();
    programBuilder.addCondition(filterRel.getCondition());
    return programBuilder.getProgram();
  }

  private RexNode expandLocalRef(RexBuilder rexBuilder,
      RexLocalRef ref, List<RexNode> exprs) {
    return ref.accept(new ExpansionShuttle(rexBuilder, exprs));
  }

  private static class ExpansionShuttle extends RexShuttle {
    private final RexBuilder rexBuilder;
    private final List<RexNode> exprs;

    ExpansionShuttle(RexBuilder rexBuilder, List<RexNode> exprs) {
      this.rexBuilder = rexBuilder;
      this.exprs = exprs;
    }

    @Override
    public RexNode visitLocalRef(RexLocalRef localRef) {
      RexNode tree = this.exprs.get(localRef.getIndex());
      return RexUtil.flatten(rexBuilder, tree.accept(this));
    }
  }
}
