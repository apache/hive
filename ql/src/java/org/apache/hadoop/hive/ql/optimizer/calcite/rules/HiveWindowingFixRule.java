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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;

/**
 * Rule to fix windowing issue when it is done over
 * aggregation columns (more info in HIVE-10627).
 *
 * This rule is applied as a post-processing step after
 * optimization by Calcite in order to add columns
 * that may be pruned by RelFieldTrimmer, but are
 * still needed due to the concrete implementation of
 * Windowing processing in Hive.
 */
public class HiveWindowingFixRule extends RelOptRule {

  public static final HiveWindowingFixRule INSTANCE = new HiveWindowingFixRule();

  private final ProjectFactory projectFactory;


  private HiveWindowingFixRule() {
    super(
        operand(Project.class,
            operand(Aggregate.class, any())));
    this.projectFactory = HiveProject.DEFAULT_PROJECT_FACTORY;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Project project = call.rel(0);
    Aggregate aggregate = call.rel(1);

    // 1. We go over the expressions in the project operator
    //    and we separate the windowing nodes that are result
    //    of an aggregate expression from the rest of nodes
    final int groupingFields = aggregate.getGroupCount() + aggregate.getIndicatorCount();
    Set<String> projectExprsDigest = new HashSet<String>();
    Map<String, RexNode> windowingExprsDigestToNodes = new HashMap<String,RexNode>();
    for (RexNode r : project.getChildExps()) {
      if (r instanceof RexOver) {
        RexOver rexOverNode = (RexOver) r;
        // Operands
        for (RexNode operand : rexOverNode.getOperands()) {
          if (operand instanceof RexInputRef &&
                  ((RexInputRef)operand).getIndex() >= groupingFields) {
            windowingExprsDigestToNodes.put(operand.toString(), operand);
          }
        }
        // Partition keys
        for (RexNode partitionKey : rexOverNode.getWindow().partitionKeys) {
          if (partitionKey instanceof RexInputRef &&
                  ((RexInputRef)partitionKey).getIndex() >= groupingFields) {
            windowingExprsDigestToNodes.put(partitionKey.toString(), partitionKey);
          }
        }
        // Order keys
        for (RexFieldCollation orderKey : rexOverNode.getWindow().orderKeys) {
          if (orderKey.left instanceof RexInputRef &&
                  ((RexInputRef)orderKey.left).getIndex() >= groupingFields) {
            windowingExprsDigestToNodes.put(orderKey.left.toString(), orderKey.left);
          }
        }
      } else {
        projectExprsDigest.add(r.toString());
      }
    }

    // 2. We check whether there is a column needed by the
    //    windowing operation that is missing in the
    //    project expressions. For instance, if the windowing
    //    operation is over an aggregation column, Hive expects
    //    that column to be in the Select clause of the query.
    //    The idea is that if there is a column missing, we will
    //    replace the old project operator by two new project
    //    operators:
    //    - a project operator containing the original columns
    //      of the project operator plus all the columns that were
    //      missing
    //    - a project on top of the previous one, that will take
    //      out the columns that were missing and were added by the
    //      previous project

    // These data structures are needed to create the new project
    // operator (below)
    final List<RexNode> belowProjectExprs = new ArrayList<RexNode>();
    final List<String> belowProjectColumnNames = new ArrayList<String>();

    // This data structure is needed to create the new project
    // operator (top)
    final List<RexNode> topProjectExprs = new ArrayList<RexNode>();

    final int projectCount = project.getChildExps().size();
    for (int i = 0; i < projectCount; i++) {
      belowProjectExprs.add(project.getChildExps().get(i));
      belowProjectColumnNames.add(project.getRowType().getFieldNames().get(i));
      topProjectExprs.add(RexInputRef.of(i, project.getRowType()));
    }
    boolean windowingFix = false;
    for (Entry<String, RexNode> windowingExpr : windowingExprsDigestToNodes.entrySet()) {
      if (!projectExprsDigest.contains(windowingExpr.getKey())) {
        windowingFix = true;
        belowProjectExprs.add(windowingExpr.getValue());
        int colIndex = 0;
        String alias = "window_col_" + colIndex;
        while (belowProjectColumnNames.contains(alias)) {
          alias = "window_col_" + (colIndex++);
        }
        belowProjectColumnNames.add(alias);
      }
    }

    if (!windowingFix) {
      // We do not need to do anything, we bail out
      return;
    }

    // 3. We need to fix it, we create the two replacement project
    //    operators
    RelNode newProjectRel = projectFactory.createProject(
        aggregate, belowProjectExprs, belowProjectColumnNames);
    RelNode newTopProjectRel = projectFactory.createProject(
        newProjectRel, topProjectExprs, project.getRowType().getFieldNames());

    call.transformTo(newTopProjectRel);
  }

}
