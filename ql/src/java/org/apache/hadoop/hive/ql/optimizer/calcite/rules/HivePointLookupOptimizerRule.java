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
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil.transformOrToInAndInequalityToBetween;

/**
 * This optimization attempts to identify and close expanded INs and BETWEENs
 *
 * Basically:
 * <pre>
 * (c) IN ( v1, v2, ...) &lt;=&gt; c1=v1 || c1=v2 || ...
 * </pre>
 * If c is struct; then c=v1 is a group of anded equations.
 *
 * Similarly
 * <pre>
 * v1 &lt;= c1 and c1 &lt;= v2
 * </pre>
 * is rewritten to <p>c1 between v1 and v2</p>
 */
public abstract class HivePointLookupOptimizerRule extends RelOptRule {

  /** Rule adapter to apply the transformation to Filter conditions. */
  public static class FilterCondition extends HivePointLookupOptimizerRule {
    public FilterCondition (int minNumORClauses) {
      super(operand(Filter.class, any()), minNumORClauses,
          "HivePointLookupOptimizerRule(FilterCondition)");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Filter filter = call.rel(0);
      final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
      final RexNode condition = RexUtil.pullFactors(rexBuilder, filter.getCondition());

      RexNode newCondition = transformOrToInAndInequalityToBetween(rexBuilder, condition, minNumORClauses);

      // If we could not transform anything, we bail out
      if (newCondition.toString().equals(condition.toString())) {
        return;
      }
      RelNode newNode = filter.copy(filter.getTraitSet(), filter.getInput(), newCondition);

      call.transformTo(newNode);
    }
  }

  /** Rule adapter to apply the transformation to Join conditions. */
  public static class JoinCondition extends HivePointLookupOptimizerRule {
    public JoinCondition (int minNumORClauses) {
      super(operand(Join.class, any()), minNumORClauses,
          "HivePointLookupOptimizerRule(JoinCondition)");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Join join = call.rel(0);
      final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
      final RexNode condition = RexUtil.pullFactors(rexBuilder, join.getCondition());

      RexNode newCondition = transformOrToInAndInequalityToBetween(rexBuilder, condition, minNumORClauses);

      // If we could not transform anything, we bail out
      if (newCondition.toString().equals(condition.toString())) {
        return;
      }

      RelNode newNode = join.copy(join.getTraitSet(),
          newCondition,
          join.getLeft(),
          join.getRight(),
          join.getJoinType(),
          join.isSemiJoinDone());

      call.transformTo(newNode);
    }
  }

  /** Rule adapter to apply the transformation to Projections. */
  public static class ProjectionExpressions extends HivePointLookupOptimizerRule {
    public ProjectionExpressions(int minNumORClauses) {
      super(operand(Project.class, any()), minNumORClauses,
          "HivePointLookupOptimizerRule(ProjectionExpressions)");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Project project = call.rel(0);
      boolean changed = false;
      final RexBuilder rexBuilder = project.getCluster().getRexBuilder();
      List<RexNode> newProjects = new ArrayList<>();
      for (RexNode oldNode : project.getProjects()) {
        RexNode newNode = transformOrToInAndInequalityToBetween(rexBuilder, oldNode, minNumORClauses);
        if (!newNode.toString().equals(oldNode.toString())) {
          changed = true;
          newProjects.add(newNode);
        } else {
          newProjects.add(oldNode);
        }
      }
      if (!changed) {
        return;
      }
      Project newProject = project.copy(project.getTraitSet(), project.getInput(), newProjects,
          project.getRowType());
      call.transformTo(newProject);
    }

  }

  protected static final Logger LOG = LoggerFactory.getLogger(HivePointLookupOptimizerRule.class);

  // Minimum number of OR clauses needed to transform into IN clauses
  protected final int minNumORClauses;

  protected HivePointLookupOptimizerRule(
      RelOptRuleOperand operand, int minNumORClauses, String description) {
    super(operand, description);
    this.minNumORClauses = minNumORClauses;
  }
}
