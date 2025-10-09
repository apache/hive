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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * This class implements the main algorithm of rewriting an MV maintenance plan to incremental.
 * Subclasses are represents Calcite rules which performs a rewriting to prepare the plan for incremental
 * view maintenance in case there exist aggregation operator, so we can
 * avoid the INSERT OVERWRITE and use a MERGE statement instead.
 *
 * See subclasses for examples:
 *   {@link HiveAggregateInsertIncrementalRewritingRule},
 *   {@link HiveAggregateInsertDeleteIncrementalRewritingRule}
 *
 * @param <T> Should be a class that wraps the right input of the top right outer join: the union branch
 *           which represents the plan which produces the delta records since the last view rebuild.
 */
public abstract class HiveAggregateIncrementalRewritingRuleBase<
        T extends HiveAggregateIncrementalRewritingRuleBase.IncrementalComputePlan>
        extends RelOptRule {

  private final int aggregateIndex;

  protected HiveAggregateIncrementalRewritingRuleBase(RelOptRuleOperand operand,
                                                      RelBuilderFactory relBuilderFactory, String description,
                                                      int aggregateIndex) {
    super(operand, relBuilderFactory, description);
    this.aggregateIndex = aggregateIndex;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Aggregate agg = call.rel(aggregateIndex);
    final Union union = call.rel(1);
    final RelBuilder relBuilder = call.builder();
    final RexBuilder rexBuilder = relBuilder.getRexBuilder();

    // 1) First branch is query, second branch is MV
    RelNode joinLeftInput = union.getInput(1);
    final T joinRightInput = createJoinRightInput(call);
    if (joinRightInput == null) {
      return;
    }

    // 2) Introduce a Project on top of MV scan having all columns from the view plus a boolean literal which indicates
    // whether the row with the key values coming from the joinRightInput exists in the view:
    // - true means exist
    // - null means not exists
    // Project also needed to encapsulate the view scan by a subquery -> this is required by
    // CalcitePlanner.fixUpASTAggregateInsertIncrementalRebuild
    // CalcitePlanner.fixUpASTAggregateInsertDeleteIncrementalRebuild
    List<RexNode> mvCols = new ArrayList<>(joinLeftInput.getRowType().getFieldCount());
    for (int i = 0; i < joinLeftInput.getRowType().getFieldCount(); ++i) {
      mvCols.add(rexBuilder.makeInputRef(
              joinLeftInput.getRowType().getFieldList().get(i).getType(), i));
    }
    mvCols.add(rexBuilder.makeLiteral(true));

    joinLeftInput = relBuilder
            .push(joinLeftInput)
            .project(mvCols)
            .build();

    // 3) Build conditions for join and start adding
    // expressions for project operator
    List<RexNode> projExprs = new ArrayList<>();
    List<RexNode> joinConjs = new ArrayList<>();
    int groupCount = agg.getGroupCount();
    int totalCount = agg.getGroupCount() + agg.getAggCallList().size();
    for (int leftPos = 0, rightPos = totalCount + 1;
         leftPos < groupCount; leftPos++, rightPos++) {
      RexNode leftRef = rexBuilder.makeInputRef(
          joinLeftInput.getRowType().getFieldList().get(leftPos).getType(), leftPos);
      RexNode rightRef = rexBuilder.makeInputRef(
          joinRightInput.rightInput.getRowType().getFieldList().get(leftPos).getType(), rightPos);
      projExprs.add(rightRef);

      RexNode nsEqExpr = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
              ImmutableList.of(leftRef, rightRef));
      joinConjs.add(nsEqExpr);
    }

    // 4) Create join node
    RexNode joinCond = RexUtil.composeConjunction(rexBuilder, joinConjs);
    RelNode join = relBuilder
            .push(joinLeftInput)
            .push(joinRightInput.rightInput)
            .join(JoinRelType.RIGHT, joinCond)
            .build();

    // 5) Add the expressions that correspond to the aggregation
    // functions
    for (int i = 0, leftPos = groupCount, rightPos = totalCount + 1 + groupCount;
         leftPos < totalCount; i++, leftPos++, rightPos++) {
      // case when source.s is null and mv2.s is null then null
      // case when source.s IS null then mv2.s
      // case when mv2.s IS null then source.s
      // else source.s + mv2.s end
      RexNode leftRef = rexBuilder.makeInputRef(
          joinLeftInput.getRowType().getFieldList().get(leftPos).getType(), leftPos);
      RexNode rightRef = rexBuilder.makeInputRef(
          joinRightInput.rightInput.getRowType().getFieldList().get(leftPos).getType(), rightPos);
      // Generate SQLOperator for merging the aggregations
      SqlAggFunction aggCall = agg.getAggCallList().get(i).getAggregation();
      RexNode elseReturn = createAggregateNode(aggCall, leftRef, rightRef, rexBuilder);
      // According to SQL standard (and Hive) Aggregate functions eliminates null values however operators used in
      // elseReturn expressions returns null if one of their operands is null
      // hence we need a null check of both operands.
      // Note: If both are null, we will fall into branch    WHEN leftNull THEN rightRef
      RexNode leftNull = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, leftRef);
      RexNode rightNull = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, rightRef);
      RexNode caseExpression = rexBuilder.makeCall(SqlStdOperatorTable.CASE,
              leftNull, rightRef,
              rightNull, leftRef,
              elseReturn);
      RexNode cast = rexBuilder.makeCast(
              call.rel(0).getRowType().getFieldList().get(projExprs.size()).getType(), caseExpression);
      projExprs.add(cast);
    }

    int flagIndex = joinLeftInput.getRowType().getFieldCount() - 1;
    RexNode flagNode = rexBuilder.makeInputRef(
            join.getRowType().getFieldList().get(flagIndex).getType(), flagIndex);

    // 6) Build plan
    RelNode newNode = relBuilder
        .push(join)
        .filter(createFilterCondition(joinRightInput, flagNode, projExprs, relBuilder))
        .project(projExprs)
        .build();
    call.transformTo(newNode);
  }

  protected abstract T createJoinRightInput(RelOptRuleCall call);

  protected static class IncrementalComputePlan {
    protected final RelNode rightInput;

    public IncrementalComputePlan(RelNode rightInput) {
      this.rightInput = rightInput;
    }
  }

  protected RexNode createAggregateNode(
          SqlAggFunction aggCall, RexNode leftRef, RexNode rightRef, RexBuilder rexBuilder) {
    switch (aggCall.getKind()) {
      case SUM:
      case SUM0:
      case COUNT:
        // SUM and COUNT are rolled up as SUM, hence SUM represents both here
        return rexBuilder.makeCall(SqlStdOperatorTable.PLUS,
                ImmutableList.of(rightRef, leftRef));
      default:
        throw new AssertionError("Found an aggregation that could not be"
                + " recognized: " + aggCall);
    }
  }

  protected abstract RexNode createFilterCondition(T rightInput, RexNode flagNode, List<RexNode> projExprs, RelBuilder relBuilder);
}
