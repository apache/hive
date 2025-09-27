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

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RexImplicationChecker;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexExecutor;
import org.apache.calcite.rex.RexExecutorImpl;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAntiJoin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Planner rule that converts a join plus filter to anti join.
 */
public class HiveAntiSemiJoinRule extends RelOptRule {
  protected static final Logger LOG = LoggerFactory.getLogger(HiveAntiSemiJoinRule.class);
  public static final HiveAntiSemiJoinRule INSTANCE = new HiveAntiSemiJoinRule();

  //    HiveProject(fld=[$0])
  //      HiveFilter(condition=[IS NULL($1)])
  //        HiveJoin(condition=[=($0, $1)], joinType=[left], algorithm=[none], cost=[not available])
  //
  // TO
  //
  //    HiveProject(fld_tbl=[$0])
  //      HiveAntiJoin(condition=[=($0, $1)], joinType=[anti])
  //
  public HiveAntiSemiJoinRule() {
    super(operand(Project.class, operand(Filter.class, operand(Join.class, RelOptRule.any()))),
            "HiveJoinWithFilterToAntiJoinRule:filter");
  }

  // is null filter over a left join.
  public void onMatch(final RelOptRuleCall call) {
    final Project project = call.rel(0);
    final Filter filter = call.rel(1);
    final Join join = call.rel(2);
    perform(call, project, filter, join);
  }

  protected void perform(RelOptRuleCall call, Project project, Filter filter, Join join) {
    LOG.debug("Start Matching HiveAntiJoinRule");

    //TODO : Need to support this scenario.
    //https://issues.apache.org/jira/browse/HIVE-23991
    if (join.getCondition().isAlwaysTrue()) {
      return;
    }

    //We support conversion from left outer join only.
    if (join.getJoinType() != JoinRelType.LEFT) {
      return;
    }

    assert (filter != null);

    ImmutableBitSet rhsFields = HiveCalciteUtil.getRightSideBitset(join);
    Optional<List<RexNode>> optFilterList = getResidualFilterNodes(filter, join, rhsFields);
    if (optFilterList.isEmpty()) {
      return;
    }
    List<RexNode> filterList = optFilterList.get();

    // If any projection is there from right side, then we can not convert to anti join.
    ImmutableBitSet projectedFields = RelOptUtil.InputFinder.bits(project.getProjects(), null);
    boolean projectionUsesRHS = projectedFields.intersects(rhsFields);
    if (projectionUsesRHS) {
      return;
    }

    // if one of the operand from join condition is not from right table then no need to convert to anti join.
    if (HiveCalciteUtil.checkIfJoinConditionOnlyUsesLeftOperands(join)) {
      return;
    }

    LOG.debug("Matched HiveAntiJoinRule");

    // Build anti join with same left, right child and condition as original left outer join.
    Join anti = HiveAntiJoin.getAntiJoin(join.getLeft().getCluster(), join.getLeft().getTraitSet(),
            join.getLeft(), join.getRight(), join.getCondition());
    RelNode newProject;
    if (filterList.isEmpty()) {
      newProject = project.copy(project.getTraitSet(), anti, project.getProjects(), project.getRowType());
    } else {
      // Collate the filter condition using AND as the filter was decomposed based
      // on AND condition (RelOptUtil.conjunctions).
      RexNode condition = filterList.size() == 1 ? filterList.get(0) :
              join.getCluster().getRexBuilder().makeCall(SqlStdOperatorTable.AND, filterList);
      Filter newFilter = filter.copy(filter.getTraitSet(), anti, condition);
      newProject = project.copy(project.getTraitSet(), newFilter, project.getProjects(), project.getRowType());
    }
    call.transformTo(newProject);
  }

  /**
   * Extracts the non-null filter conditions from given filter node.
   *
   * @param filter    The filter condition to be checked.
   * @param join      Join node whose right side has to be searched.
   * @param rhsFields
   * @return null : Anti join condition is not matched for filter.
   *     Empty list : No residual filter conditions present.
   *     Valid list containing the filter to be applied after join.
   */
  private Optional<List<RexNode>> getResidualFilterNodes(Filter filter, Join join, ImmutableBitSet rhsFields) {
    // 1. If null filter is not present from right side then we can not convert to anti join.
    // 2. If any non-null filter is present from right side, we can not convert it to anti join.
    // 3. Keep other filters which needs to be executed after join.
    // 4. The filter conditions are decomposed on AND conditions only.
    //TODO If some conditions like (fld1 is null or fld2 is null) present, it will not be considered for conversion.
    //https://issues.apache.org/jira/browse/HIVE-23992
    List<RexNode> aboveFilters = RelOptUtil.conjunctions(filter.getCondition());
    boolean hasNullFilterOnRightSide = false;
    List<RexNode> filterList = new ArrayList<>();
    final ImmutableBitSet notNullColumnsFromRightSide = getNotNullColumnsFromRightSide(join);

    for (RexNode filterNode : aboveFilters) {
      final ImmutableBitSet usedFields = RelOptUtil.InputFinder.bits(filterNode);
      boolean usesFieldFromRHS = usedFields.intersects(rhsFields);

      if(!usesFieldFromRHS) {
        // Only LHS fields or constants, so the filterNode is part of the residual filter
        filterList.add(filterNode);
        continue;
      }

      // In the following we check for filter nodes that let us deduce that
      // "an (originally) not-null column of RHS IS NULL because the LHS row will not be matched"

      if(filterNode.getKind() != SqlKind.IS_NULL) {
        return Optional.empty();
      }

      boolean usesRHSFieldsOnly = rhsFields.contains(usedFields);
      if (!usesRHSFieldsOnly) {
        // If there is a mix between LHS and RHS fields, don't convert to anti-join
        return Optional.empty();
      }

      // Null filter from right side table can be removed and it is a pre-condition for anti join conversion.
      RexNode arg = ((RexCall) filterNode).getOperands().get(0);
      if (isStrong(arg, notNullColumnsFromRightSide)) {
        hasNullFilterOnRightSide = true;
      } else if(!isStrong(arg, rhsFields)) {
        // if all RHS fields are null and the IS NULL is still not fulfilled, bail out
        return Optional.empty();
      }
    }

    if (!hasNullFilterOnRightSide) {
      return Optional.empty();
    }
    return Optional.of(filterList);
  }

  private ImmutableBitSet getNotNullColumnsFromRightSide(RelNode joinRel) {
    // we need to shift the indices of the second child to the right
    int shift = (joinRel.getInput(0)).getRowType().getFieldCount();
    ImmutableBitSet rhsNotnullColumns = deduceNotNullColumns(joinRel.getInput(1));
    return rhsNotnullColumns.shift(shift);
  }

  /**
   * Deduce which columns of the <code>relNode</code> are definitively NOT NULL.
   */
  private ImmutableBitSet deduceNotNullColumns(RelNode relNode) {
    // adapted from org.apache.calcite.plan.RelOptUtil.containsNullableFields
    RelOptCluster cluster = relNode.getCluster();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    ImmutableBitSet.Builder result = ImmutableBitSet.builder();
    ImmutableBitSet.Builder candidatesBuilder = ImmutableBitSet.builder();
    List<RelDataTypeField> fieldList = relNode.getRowType().getFieldList();
    for (int i=0; i<fieldList.size(); i++) {
      if (fieldList.get(i).getType().isNullable()) {
        candidatesBuilder.set(i);
      }
      else {
        result.set(i);
      }
    }
    ImmutableBitSet candidates = candidatesBuilder.build();
    if (candidates.isEmpty()) {
      // All columns are declared NOT NULL, no need to change
      return result.build();
    }
    final RexExecutor executor = cluster.getPlanner().getExecutor();
    if (!(executor instanceof RexExecutorImpl)) {
      // Cannot proceed without an executor.
      return result.build();
    }

    final RexImplicationChecker checker =
        new RexImplicationChecker(rexBuilder, executor, relNode.getRowType());
    final RelOptPredicateList predicates = mq.getPulledUpPredicates(relNode);

    ImmutableList<RexNode> preds = predicates.pulledUpPredicates;
    final List<RexNode> antecedent = new ArrayList<>(preds);
    final RexNode first = RexUtil.composeConjunction(rexBuilder, antecedent);
    for (int c : candidates) {
      RelDataTypeField field = fieldList.get(c);
      final RexNode second = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL,
          rexBuilder.makeInputRef(field.getType(), field.getIndex()));
      // Suppose we have EMP(empno INT NOT NULL, mgr INT),
      // and predicates [empno > 0, mgr > 0].
      // We make first: "empno > 0 AND mgr > 0"
      // and second: "mgr IS NOT NULL"
      // and ask whether first implies second.
      // It does, so we have no nullable columns.
      if(checker.implies(first, second)) {
        result.set(c);
      }
    }
    return result.build();
  }

  private boolean isStrong(RexNode rexNode, ImmutableBitSet rightSideBitset) {
    try {
      rexNode.accept(new RexVisitorImpl<Void>(true) {
        @Override
        public Void visitCall(RexCall call) {
          if (call.getKind() == SqlKind.CAST) {
            throw Util.FoundOne.NULL;
          }
          return super.visitCall(call);
        }
      });
    } catch (Util.FoundOne e) {
      // Hive's CAST might introduce NULL for NOT NULL fields
      return false;
    }
    return Strong.isNull(rexNode, rightSideBitset);
  }
}
