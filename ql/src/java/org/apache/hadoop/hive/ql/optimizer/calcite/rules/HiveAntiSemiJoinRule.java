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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAntiJoin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

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

    List<RexNode> filterList = getResidualFilterNodes(filter, join);
    if (filterList == null) {
      return;
    }

    // If any projection is there from right side, then we can not convert to anti join.
    boolean hasProjection = HiveCalciteUtil.hasAnyExpressionFromRightSide(join, project.getProjects());
    if (hasProjection) {
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
   * @param filter The filter condition to be checked.
   * @param join Join node whose right side has to be searched.
   * @return null : Anti join condition is not matched for filter.
   *         Empty list : No residual filter conditions present.
   *         Valid list containing the filter to be applied after join.
   */
  private List<RexNode> getResidualFilterNodes(Filter filter, Join join) {
    // 1. If null filter is not present from right side then we can not convert to anti join.
    // 2. If any non-null filter is present from right side, we can not convert it to anti join.
    // 3. Keep other filters which needs to be executed after join.
    // 4. The filter conditions are decomposed on AND conditions only.
    //TODO If some conditions like (fld1 is null or fld2 is null) present, it will not be considered for conversion.
    //https://issues.apache.org/jira/browse/HIVE-23992
    List<RexNode> aboveFilters = RelOptUtil.conjunctions(filter.getCondition());
    boolean hasNullFilterOnRightSide = false;
    List<RexNode> filterList = new ArrayList<>();
    for (RexNode filterNode : aboveFilters) {
      if (filterNode.getKind() == SqlKind.IS_NULL) {
        // Null filter from right side table can be removed and its a pre-condition for anti join conversion.
        if (HiveCalciteUtil.hasAllExpressionsFromRightSide(join, Collections.singletonList(filterNode))
            && isStrong(((RexCall) filterNode).getOperands().get(0))) {
          hasNullFilterOnRightSide = true;
        } else {
          filterList.add(filterNode);
        }
      } else {
        if (HiveCalciteUtil.hasAnyExpressionFromRightSide(join, Collections.singletonList(filterNode))) {
          // If some non null condition is present from right side, we can not convert the join to anti join as
          // anti join does not project the fields from right side.
          return null;
        } else {
          filterList.add(filterNode);
        }
      }
    }

    if (!hasNullFilterOnRightSide) {
      return null;
    }
    return filterList;
  }

  private boolean isStrong(RexNode rexNode) {
    AtomicBoolean hasCast = new AtomicBoolean(false);
    rexNode.accept(new RexVisitorImpl<Void>(true) {
      @Override
      public Void visitCall(RexCall call) {
        if (call.getKind() == SqlKind.CAST) {
          hasCast.set(true);
        }
        return super.visitCall(call);
      }
    });
    return !hasCast.get() && Strong.isStrong(rexNode);
  }
}
