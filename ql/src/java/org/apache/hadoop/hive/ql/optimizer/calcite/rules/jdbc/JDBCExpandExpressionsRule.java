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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.jdbc;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcFilter;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcJoin;
import org.apache.calcite.adapter.jdbc.JdbcRules.JdbcProject;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.RexNodeConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JDBCExpandExpressionsRule that rewrites IN clauses over struct operators
 * into OR/AND expressions.
 */

public abstract class JDBCExpandExpressionsRule extends RelOptRule {
  private static final Logger LOG = LoggerFactory.getLogger(JDBCExpandExpressionsRule.class);

  public static final JDBCExpandExpressionsRule.FilterCondition FILTER_INSTANCE =
      new JDBCExpandExpressionsRule.FilterCondition();
  public static final JDBCExpandExpressionsRule.JoinCondition JOIN_INSTANCE =
      new JDBCExpandExpressionsRule.JoinCondition();
  public static final JDBCExpandExpressionsRule.ProjectionExpressions PROJECT_INSTANCE =
      new JDBCExpandExpressionsRule.ProjectionExpressions();


  private JDBCExpandExpressionsRule(RelOptRuleOperand operand, String description) {
    super(operand, description);
  }

  /** Rule adapter to apply the transformation to Filter conditions. */
  private static class FilterCondition extends JDBCExpandExpressionsRule {

    private FilterCondition() {
      super(operand(JdbcFilter.class, any()), "JDBCExpandExpressionsRule(FilterCondition)");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      LOG.debug("JDBCExpandExpressionsRule.FilterCondition has been called");

      final JdbcFilter filter = call.rel(0);
      final RexNode condition = filter.getCondition();

      RexNode newCondition = analyzeRexNode(
          filter.getCluster().getRexBuilder(), condition);

      // If we could not transform anything, we bail out
      if (newCondition.toString().equals(condition.toString())) {
        return;
      }

      RelNode newNode = filter.copy(filter.getTraitSet(), filter.getInput(), newCondition);
      call.transformTo(newNode);
    }
  }

  /** Rule adapter to apply the transformation to Join conditions. */
  private static class JoinCondition extends JDBCExpandExpressionsRule {

    private JoinCondition () {
      super(operand(JdbcJoin.class, any()), "JDBCExpandExpressionsRule(JoinCondition)");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      LOG.debug("JDBCExpandExpressionsRule.JoinCondition has been called");

      final Join join = call.rel(0);
      final RexNode condition = RexUtil.pullFactors(
          join.getCluster().getRexBuilder(), join.getCondition());

      RexNode newCondition = analyzeRexNode(
          join.getCluster().getRexBuilder(), condition);

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
  private static class ProjectionExpressions extends JDBCExpandExpressionsRule {

    private ProjectionExpressions() {
      super(operand(JdbcProject.class, any()), "JDBCExpandExpressionsRule(ProjectionExpressions)");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      LOG.debug("JDBCExpandExpressionsRule.ProjectionExpressions has been called");

      final Project project = call.rel(0);
      final RexBuilder rexBuilder = project.getCluster().getRexBuilder();
      boolean changed = false;
      List<RexNode> newProjects = new ArrayList<>();
      for (RexNode oldNode : project.getProjects()) {
        RexNode newNode = analyzeRexNode(rexBuilder, oldNode);
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

      Project newProject = project.copy(
          project.getTraitSet(),
          project.getInput(),
          newProjects,
          project.getRowType());
      call.transformTo(newProject);
    }

  }

  RexNode analyzeRexNode(RexBuilder rexBuilder, RexNode condition) {
    RexTransformIntoOrAndClause transformIntoInClause = new RexTransformIntoOrAndClause(rexBuilder);
    RexNode newCondition = transformIntoInClause.apply(condition);
    return newCondition;
  }

  /**
   * Transforms IN clauses into OR/AND clauses, when possible.
   */
  protected static class RexTransformIntoOrAndClause extends RexShuttle {
    private final RexBuilder rexBuilder;

    RexTransformIntoOrAndClause(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall inputCall) {
      RexNode node = super.visitCall(inputCall);
      if (node instanceof RexCall) {
        RexCall call = (RexCall) node;
        switch (call.getKind()) {
          case IN:
            return transformIntoOrAndClause(rexBuilder, call);
          default:
            break;
        }
      }
      return node;
    }

    private RexNode transformIntoOrAndClause(RexBuilder rexBuilder, RexCall expression) {
      assert expression.getKind() == SqlKind.IN;

      if (expression.getOperands().get(0).getKind() != SqlKind.ROW) {
        // Nothing to do, return expression
        return expression;
      }

      final List<RexNode> disjuncts = RexNodeConverter.transformInToOrOperands(
          expression.getOperands(), rexBuilder);
      if (disjuncts == null) {
        // We could not execute transformation, return expression
        return expression;
      }

      if (disjuncts.size() > 1) {
        return rexBuilder.makeCall(SqlStdOperatorTable.OR, disjuncts);
      } else {
        return disjuncts.get(0);
      }
    }

  }


}
