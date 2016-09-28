/**
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;

public class HiveFilterProjectTransposeRule extends FilterProjectTransposeRule {

  public static final HiveFilterProjectTransposeRule INSTANCE_DETERMINISTIC_WINDOWING =
          new HiveFilterProjectTransposeRule(Filter.class, HiveProject.class,
                  HiveRelFactories.HIVE_BUILDER, true, true);

  public static final HiveFilterProjectTransposeRule INSTANCE_DETERMINISTIC =
          new HiveFilterProjectTransposeRule(Filter.class, HiveProject.class,
                  HiveRelFactories.HIVE_BUILDER, true, false);

  public static final HiveFilterProjectTransposeRule INSTANCE =
          new HiveFilterProjectTransposeRule(Filter.class, HiveProject.class,
                  HiveRelFactories.HIVE_BUILDER, false, false);

  private final boolean onlyDeterministic;

  private final boolean pushThroughWindowing;

  private HiveFilterProjectTransposeRule(Class<? extends Filter> filterClass,
      Class<? extends Project> projectClass, RelBuilderFactory relBuilderFactory,
      boolean onlyDeterministic,boolean pushThroughWindowing) {
    super(filterClass, projectClass, false, false, relBuilderFactory);
    this.onlyDeterministic = onlyDeterministic;
    this.pushThroughWindowing = pushThroughWindowing;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final Filter filterRel = call.rel(0);
    RexNode condition = filterRel.getCondition();
    if (this.onlyDeterministic && !HiveCalciteUtil.isDeterministic(condition)) {
      return false;
    }

    return super.matches(call);
  }

  public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final Project origproject = call.rel(1);
    RexNode filterCondToPushBelowProj = filter.getCondition();
    RexNode unPushedFilCondAboveProj = null;

    if (RexUtil.containsCorrelation(filterCondToPushBelowProj)) {
      // If there is a correlation condition anywhere in the filter, don't
      // push this filter past project since in some cases it can prevent a
      // Correlate from being de-correlated.
      return;
    }

    if (RexOver.containsOver(origproject.getProjects(), null)) {
      RexNode origFilterCond = filterCondToPushBelowProj;
      filterCondToPushBelowProj = null;
      if (pushThroughWindowing) {
        Set<Integer> commonPartitionKeys = getCommonPartitionCols(origproject.getProjects());
        List<RexNode> newPartKeyFilConds = new ArrayList<RexNode>();
        List<RexNode> unpushedFilConds = new ArrayList<RexNode>();

        // TODO:
        // 1) Handle compound partition keys (partition by k1+k2)
        // 2) When multiple window clauses are present in same select Even if
        // Predicate can not pushed past all of them, we might still able to
        // push
        // it below some of them.
        // Ex: select * from (select key, value, avg(c_int) over (partition by
        // key), sum(c_float) over(partition by value) from t1)t1 where value <
        // 10
        // --> select * from (select key, value, avg(c_int) over (partition by
        // key) from (select key, value, sum(c_float) over(partition by value)
        // from t1 where value < 10)t1)t2
        if (!commonPartitionKeys.isEmpty()) {
          for (RexNode ce : RelOptUtil.conjunctions(origFilterCond)) {
            RexNode newCondition = RelOptUtil.pushPastProject(ce, origproject);
            if (HiveCalciteUtil.isDeterministicFuncWithSingleInputRef(newCondition,
                commonPartitionKeys)) {
              newPartKeyFilConds.add(newCondition);
            } else {
              unpushedFilConds.add(ce);
            }
          }

          if (!newPartKeyFilConds.isEmpty()) {
            filterCondToPushBelowProj = RexUtil.composeConjunction(filter.getCluster().getRexBuilder(),
                    newPartKeyFilConds, true);
          }
          if (!unpushedFilConds.isEmpty()) {
            unPushedFilCondAboveProj = RexUtil.composeConjunction(filter.getCluster().getRexBuilder(),
                    unpushedFilConds, true);
          }
        }
      }
    }

    if (filterCondToPushBelowProj != null) {
      RelNode newProjRel = getNewProject(filterCondToPushBelowProj, unPushedFilCondAboveProj, origproject, filter.getCluster()
          .getTypeFactory(), call.builder());
      call.transformTo(newProjRel);
    }
  }

  private static RelNode getNewProject(RexNode filterCondToPushBelowProj, RexNode unPushedFilCondAboveProj, Project oldProj,
      RelDataTypeFactory typeFactory, RelBuilder relBuilder) {

    // convert the filter to one that references the child of the project
    RexNode newPushedCondition = RelOptUtil.pushPastProject(filterCondToPushBelowProj, oldProj);

    // Remove cast of BOOLEAN NOT NULL to BOOLEAN or vice versa. Filter accepts
    // nullable and not-nullable conditions, but a CAST might get in the way of
    // other rewrites.
    if (RexUtil.isNullabilityCast(typeFactory, newPushedCondition)) {
      newPushedCondition = ((RexCall) newPushedCondition).getOperands().get(0);
    }

    RelNode newPushedFilterRel = relBuilder.push(oldProj.getInput()).filter(newPushedCondition).build();

    RelNode newProjRel = relBuilder.push(newPushedFilterRel)
        .project(oldProj.getProjects(), oldProj.getRowType().getFieldNames()).build();

    if (unPushedFilCondAboveProj != null) {
      // Remove cast of BOOLEAN NOT NULL to BOOLEAN or vice versa. Filter accepts
      // nullable and not-nullable conditions, but a CAST might get in the way of
      // other rewrites.
      if (RexUtil.isNullabilityCast(typeFactory, newPushedCondition)) {
        unPushedFilCondAboveProj = ((RexCall) unPushedFilCondAboveProj).getOperands().get(0);
      }
      newProjRel = relBuilder.push(newProjRel).filter(unPushedFilCondAboveProj).build();
    }

    return newProjRel;
  }

  private static Set<Integer> getCommonPartitionCols(List<RexNode> projections) {
    RexOver overClause;
    boolean firstOverClause = true;
    Set<Integer> commonPartitionKeys = new HashSet<Integer>();

    for (RexNode expr : projections) {
      if (expr instanceof RexOver) {
        overClause = (RexOver) expr;

        if (firstOverClause) {
          firstOverClause = false;
          commonPartitionKeys.addAll(getPartitionCols(overClause.getWindow().partitionKeys));
        } else {
          commonPartitionKeys.retainAll(getPartitionCols(overClause.getWindow().partitionKeys));
        }
      }
    }

    return commonPartitionKeys;
  }

  private static List<Integer> getPartitionCols(List<RexNode> partitionKeys) {
    List<Integer> pCols = new ArrayList<Integer>();
    for (RexNode key : partitionKeys) {
      if (key instanceof RexInputRef) {
        pCols.add(((RexInputRef) key).getIndex());
      }
    }
    return pCols;
  }
}
