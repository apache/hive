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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAntiJoin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
    if (join.getCondition().isAlwaysTrue()) {
      return;
    }

    //We support conversion from left outer join only.
    if (join.getJoinType() != JoinRelType.LEFT) {
      return;
    }

    assert (filter != null);

    // If null filter is not present from right side then we can not convert to anti join.
    List<RexNode> aboveFilters = RelOptUtil.conjunctions(filter.getCondition());
    Stream<RexNode> nullFilters = aboveFilters.stream().filter(filterNode -> filterNode.getKind() == SqlKind.IS_NULL);
    boolean hasNullFilter = HiveCalciteUtil.hasAnyExpressionFromRightSide(join, nullFilters.collect(Collectors.toList()));
    if (!hasNullFilter) {
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
    RelNode newProject = project.copy(project.getTraitSet(), anti, project.getProjects(), project.getRowType());
    call.transformTo(newProject);
  }
}
