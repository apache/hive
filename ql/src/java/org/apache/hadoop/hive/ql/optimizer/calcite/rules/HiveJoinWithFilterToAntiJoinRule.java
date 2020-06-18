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
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableBitSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Planner rule that converts a join plus filter to anti join.
 */
public class HiveJoinWithFilterToAntiJoinRule extends RelOptRule {
  protected static final Logger LOG = LoggerFactory.getLogger(HiveJoinWithFilterToAntiJoinRule.class);
  public static final HiveJoinWithFilterToAntiJoinRule INSTANCE = new HiveJoinWithFilterToAntiJoinRule();

  //    HiveProject(fld=[$0])
  //      HiveFilter(condition=[IS NULL($1)])
  //        HiveJoin(condition=[=($0, $1)], joinType=[left], algorithm=[none], cost=[not available])
  //
  // TO
  //
  //    HiveProject(fld_tbl=[$0])
  //      HiveAntiJoin(condition=[=($0, $1)], joinType=[anti])
  //
  public HiveJoinWithFilterToAntiJoinRule() {
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
    LOG.debug("Matched HiveAntiJoinRule");

    assert (filter != null);

    //We support conversion from left outer join only.
    if (join.getJoinType() != JoinRelType.LEFT) {
      return;
    }

    List<RexNode> aboveFilters = RelOptUtil.conjunctions(filter.getCondition());
    boolean hasIsNull = false;

    // Get all filter condition and check if any of them is a "is null" kind.
    for (RexNode filterNode : aboveFilters) {
      if (filterNode.getKind() == SqlKind.IS_NULL &&
              isFilterFromRightSide(join, filterNode, join.getJoinType())) {
        hasIsNull = true;
        break;
      }
    }

    // Is null should be on a key from right side of the join.
    if (!hasIsNull) {
      return;
    }

    // Build anti join with same left, right child and condition as original left outer join.
    Join anti = join.copy(join.getTraitSet(), join.getCondition(),
            join.getLeft(), join.getRight(), JoinRelType.ANTI, false);

    //TODO : Do we really need it
    call.getPlanner().onCopy(join, anti);

    RelNode newProject = getNewProjectNode(project, anti);
    if (newProject != null) {
      call.getPlanner().onCopy(project, newProject);
      call.transformTo(newProject);
    }
  }

  protected RelNode getNewProjectNode(Project oldProject, Join newJoin) {
    List<RelDataTypeField> newJoinFiledList = newJoin.getRowType().getFieldList();
    List<RexNode> newProjectExpr = new ArrayList<>();
    for (RexNode field : oldProject.getProjects()) {
      if (!(field instanceof  RexInputRef)) {
        return null;
      }
      int idx = ((RexInputRef)field).getIndex();
      if (idx > newJoinFiledList.size()) {
        LOG.debug(" Project filed " + ((RexInputRef) field).getName() +
                " is from right side of join. Can not convert to anti join.");
        return null;
      }

      final RexInputRef ref = newJoin.getCluster().getRexBuilder()
              .makeInputRef(field.getType(), idx);
      newProjectExpr.add(ref);
    }
    return oldProject.copy(oldProject.getTraitSet(), newJoin, newProjectExpr, oldProject.getRowType());
  }

  private boolean isFilterFromRightSide(RelNode joinRel, RexNode filter, JoinRelType joinType) {
    List<RelDataTypeField> joinFields = joinRel.getRowType().getFieldList();
    int nTotalFields = joinFields.size();

    List<RelDataTypeField> leftFields = (joinRel.getInputs().get(0)).getRowType().getFieldList();
    int nFieldsLeft = leftFields.size();
    List<RelDataTypeField> rightFields = (joinRel.getInputs().get(1)).getRowType().getFieldList();
    int nFieldsRight = rightFields.size();
    assert nTotalFields == (!joinType.projectsRight() ? nFieldsLeft : nFieldsLeft + nFieldsRight);

    ImmutableBitSet rightBitmap = ImmutableBitSet.range(nFieldsLeft, nTotalFields);
    RelOptUtil.InputFinder inputFinder = RelOptUtil.InputFinder.analyze(filter);
    ImmutableBitSet inputBits = inputFinder.inputBitSet.build();
    return rightBitmap.contains(inputBits);
  }
}
