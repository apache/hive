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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import java.util.Collections;
import java.util.List;

import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;

import com.google.common.collect.ImmutableList;

/**
 * Planner rule that replaces (if possible)
 * a {@link org.apache.calcite.rel.core.Project}
 * on a {@link org.apache.calcite.rel.core.Filter}
 * on a {@link org.apache.calcite.rel.core.TableScan}
 * to use a Materialized View.
 */
public class HiveMaterializedViewFilterScanRule extends RelOptRule {

  public static final HiveMaterializedViewFilterScanRule INSTANCE =
      new HiveMaterializedViewFilterScanRule(HiveRelFactories.HIVE_BUILDER);


  //~ Constructors -----------------------------------------------------------

  /** Creates a HiveMaterializedViewFilterScanRule. */
  protected HiveMaterializedViewFilterScanRule(RelBuilderFactory relBuilderFactory) {
    super(operand(Project.class, operand(Filter.class, operand(TableScan.class, null, none()))),
            relBuilderFactory, "MaterializedViewFilterScanRule");
  }

  //~ Methods ----------------------------------------------------------------

  public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final Filter filter = call.rel(1);
    final TableScan scan = call.rel(2);
    apply(call, project, filter, scan);
  }

  protected void apply(RelOptRuleCall call, Project project, Filter filter, TableScan scan) {
    RelOptPlanner planner = call.getPlanner();
    List<RelOptMaterialization> materializations =
        (planner instanceof VolcanoPlanner)
            ? ((VolcanoPlanner) planner).getMaterializations()
            : ImmutableList.<RelOptMaterialization>of();
    if (!materializations.isEmpty()) {
      RelNode root = project.copy(project.getTraitSet(), Collections.singletonList(
          filter.copy(filter.getTraitSet(), Collections.singletonList(
              (RelNode) scan))));
      // Costing is done in transformTo(), so we call it repeatedly with all applicable
      // materialized views and cheapest one will be picked
      List<RelOptMaterialization> applicableMaterializations =
          VolcanoPlanner.getApplicableMaterializations(root, materializations);
      for (RelOptMaterialization materialization : applicableMaterializations) {
        List<RelNode> subs = new MaterializedViewSubstitutionVisitor(
            materialization.queryRel, root, relBuilderFactory).go(materialization.tableRel);
        for (RelNode s : subs) {
          call.transformTo(s);
        }
      }
    }
  }

}
