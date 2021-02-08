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

import org.apache.calcite.adapter.druid.DruidQuery;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.RelFactories.FilterFactory;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;

import java.util.Collections;

//TODO: Remove this once Calcite FilterProjectTransposeRule can take rule operand
public class HiveFilterProjectTSTransposeRule extends RelOptRule {

  public final static HiveFilterProjectTSTransposeRule INSTANCE =
      new HiveFilterProjectTSTransposeRule(
          Filter.class, HiveRelFactories.HIVE_FILTER_FACTORY, HiveProject.class,
          HiveRelFactories.HIVE_PROJECT_FACTORY, TableScan.class);

  public final static  HiveFilterProjectTSTransposeRule INSTANCE_DRUID =
      new HiveFilterProjectTSTransposeRule(
          Filter.class, HiveRelFactories.HIVE_FILTER_FACTORY, HiveProject.class,
          HiveRelFactories.HIVE_PROJECT_FACTORY, DruidQuery.class);

  private final RelFactories.FilterFactory  filterFactory;
  private final RelFactories.ProjectFactory projectFactory;

  private HiveFilterProjectTSTransposeRule(Class<? extends Filter> filterClass,
      FilterFactory filterFactory, Class<? extends Project> projectClass,
      ProjectFactory projectFactory, Class<? extends RelNode> tsClass) {
    super(operand(filterClass, operand(projectClass, operand(tsClass, none()))));
    this.filterFactory = filterFactory;
    this.projectFactory = projectFactory;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    final HiveProject projRel = call.rel(1);

    // Assumption:
    // 1. This will be run last after PP, Col Pruning in the PreJoinOrder
    // optimizations.
    // 2. If ProjectRel is not synthetic then PPD would have already pushed
    // relevant pieces down and hence no point in running PPD again.
    // 3. For synthetic Projects we don't care about non deterministic UDFs
    if (!projRel.isSynthetic()) {
      return false;
    }

    return super.matches(call);
  }

  // ~ Methods ----------------------------------------------------------------

  // implement RelOptRule
  public void onMatch(RelOptRuleCall call) {
    final Filter filter = call.rel(0);
    final Project project = call.rel(1);

    if (RexOver.containsOver(project.getProjects(), null)) {
      // In general a filter cannot be pushed below a windowing calculation.
      // Applying the filter before the aggregation function changes
      // the results of the windowing invocation.
      //
      // When the filter is on the PARTITION BY expression of the OVER clause
      // it can be pushed down. For now we don't support this.
      return;
    }

    if (RexUtil.containsCorrelation(filter.getCondition())) {
      // If there is a correlation condition anywhere in the filter, don't
      // push this filter past project since in some cases it can prevent a
      // Correlate from being de-correlated.
      return;
    }

    // convert the filter to one that references the child of the project
    RexNode newCondition = RelOptUtil.pushPastProject(filter.getCondition(), project);

    // Remove cast of BOOLEAN NOT NULL to BOOLEAN or vice versa. Filter accepts
    // nullable and not-nullable conditions, but a CAST might get in the way of
    // other rewrites.
    final RelDataTypeFactory typeFactory = filter.getCluster().getTypeFactory();
    if (RexUtil.isNullabilityCast(typeFactory, newCondition)) {
      newCondition = ((RexCall) newCondition).getOperands().get(0);
    }

    RelNode newFilterRel = filterFactory == null ? filter.copy(filter.getTraitSet(),
        project.getInput(), newCondition) : filterFactory.createFilter(project.getInput(),
        newCondition);

    RelNode newProjRel = projectFactory == null ? project.copy(project.getTraitSet(), newFilterRel,
        project.getProjects(), project.getRowType()) : projectFactory.createProject(newFilterRel,
        Collections.emptyList(), project.getProjects(), project.getRowType().getFieldNames());

    call.transformTo(newProjRel);
  }
}
