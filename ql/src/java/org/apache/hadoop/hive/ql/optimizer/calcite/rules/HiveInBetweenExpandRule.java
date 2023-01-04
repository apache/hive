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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.RexNodeConverter;

/**
 * This class contains rules to rewrite IN/BETWEEN clauses into their
 * corresponding AND/OR versions.
 * It is the counterpart to {@link HivePointLookupOptimizerRule}.
 */
public class HiveInBetweenExpandRule {

  public static final FilterRule FILTER_INSTANCE = new FilterRule();
  public static final JoinRule JOIN_INSTANCE = new JoinRule();
  public static final ProjectRule PROJECT_INSTANCE = new ProjectRule();

  /** Rule adapter to apply the transformation to Filter conditions. */
  private static class FilterRule extends RelOptRule {

    FilterRule() {
      super(operand(Filter.class, any()), HiveRelFactories.HIVE_BUILDER,
          "HiveInBetweenExpandRule(FilterRule)");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Filter filter = call.rel(0);
      RexInBetweenExpander expander = new RexInBetweenExpander(
          filter.getCluster().getRexBuilder());
      RexNode condition = expander.apply(filter.getCondition());

      if (!expander.modified) {
        return;
      }

      RelNode newFilter = filter.copy(filter.getTraitSet(),
          filter.getInput(), condition);

      call.transformTo(newFilter);
    }
  }

  /** Rule adapter to apply the transformation to Join conditions. */
  private static class JoinRule extends RelOptRule {

    JoinRule() {
      super(operand(Join.class, any()), HiveRelFactories.HIVE_BUILDER,
          "HiveInBetweenExpandRule(JoinRule)");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Join join = call.rel(0);
      RexInBetweenExpander expander = new RexInBetweenExpander(
          join.getCluster().getRexBuilder());
      RexNode condition = expander.apply(join.getCondition());

      if (!expander.modified) {
        return;
      }

      RelNode newJoin = join.copy(join.getTraitSet(),
          condition,
          join.getLeft(),
          join.getRight(),
          join.getJoinType(),
          join.isSemiJoinDone());

      call.transformTo(newJoin);
    }
  }

  /** Rule adapter to apply the transformation to Project expressions. */
  private static class ProjectRule extends RelOptRule {

    ProjectRule() {
      super(operand(Project.class, any()), HiveRelFactories.HIVE_BUILDER,
          "HiveInBetweenExpandRule(ProjectRule)");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Project project = call.rel(0);
      RexInBetweenExpander expander = new RexInBetweenExpander(
          project.getCluster().getRexBuilder());
      List<RexNode> newProjects = new ArrayList<>();
      for (RexNode expr : project.getProjects()) {
        newProjects.add(expander.apply(expr));
      }

      if (!expander.modified) {
        return;
      }

      Project newProject = project.copy(project.getTraitSet(),
          project.getInput(), newProjects, project.getRowType());

      call.transformTo(newProject);
    }
  }


  /**
   * Class that transforms IN/BETWEEN clauses in an expression.
   * If any call is modified, the modified flag will be set to
   * true after its execution.
   */
  private static final class RexInBetweenExpander extends RexShuttle {

    private final RexBuilder rexBuilder;
    private boolean modified;

    private RexInBetweenExpander(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
      this.modified = false;
    }

    @Override
    public RexNode visitCall(final RexCall call) {
      switch (call.getKind()) {
      case AND: {
        boolean[] update = {false};
        List<RexNode> newOperands = visitList(call.operands, update);
        if (update[0]) {
          return RexUtil.composeConjunction(rexBuilder, newOperands);
        }
        return call;
      }
      case OR: {
        boolean[] update = {false};
        List<RexNode> newOperands = visitList(call.operands, update);
        if (update[0]) {
          return RexUtil.composeDisjunction(rexBuilder, newOperands);
        }
        return call;
      }
      case IN: {
        List<RexNode> newOperands = RexNodeConverter.transformInToOrOperands(
            call.getOperands(), rexBuilder);
        if (newOperands == null) {
          // We could not execute transformation, return expression
          return call;
        }
        modified = true;
        if (newOperands.size() > 1) {
          return rexBuilder.makeCall(SqlStdOperatorTable.OR, newOperands);
        }
        return newOperands.get(0);
      }
      case BETWEEN: {
        List<RexNode> newOperands = RexNodeConverter.rewriteBetweenChildren(
            call.getOperands(), rexBuilder);
        modified = true;
        if (call.getOperands().get(0).isAlwaysTrue()) {
          return rexBuilder.makeCall(SqlStdOperatorTable.OR, newOperands);
        }
        return rexBuilder.makeCall(SqlStdOperatorTable.AND, newOperands);
      }
      default:
        return super.visitCall(call);
      }
    }

  }

  private HiveInBetweenExpandRule() {
    // Utility class, defeat instantiation
  }

}
