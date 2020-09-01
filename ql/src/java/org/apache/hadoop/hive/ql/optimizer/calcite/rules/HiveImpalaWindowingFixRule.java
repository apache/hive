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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories.ProjectFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;

/**
 * Rule to push functions within analytical expressions
 * to a new Project operator below the original one.
 *
 * The main reason to do this is that our analytical expression
 * generation logic relies on Impala's analytical planner to
 * generate the plan. We need to extract the mappings after
 * the planner analyzes the expression, and without having only
 * references, it is not possible.
 *
 * After applying this rule, there will be two Project operators:
 * - The functions within the analytical expressions will be in
 * the Project operator below, which will be translated by FENG.
 * - Then the Project with the analytical expression will be above,
 * however it will not contain the functions anymore, just
 * references to the Project below.
 * Thus, it will all work properly.
 */
public class HiveImpalaWindowingFixRule extends RelOptRule {

  public static final HiveImpalaWindowingFixRule INSTANCE =
      new HiveImpalaWindowingFixRule();

  private final ProjectFactory projectFactory;


  private HiveImpalaWindowingFixRule() {
    super(operand(Project.class, any()));
    this.projectFactory = HiveRelFactories.HIVE_PROJECT_FACTORY;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    final Project project = call.rel(0);
    final RelNode input = project.getInput();
    final RexBuilder rexBuilder = project.getCluster().getRexBuilder();

    // 1. Gather all functions within over clause
    FunctionWithinRexOverCollector collector = new FunctionWithinRexOverCollector();
    for (RexNode r : project.getChildExps()) {
      r.accept(collector);
    }
    // If there are none, bail out
    if (collector.getFunctionExprs().isEmpty()) {
      return;
    }

    // 2. Add all refs and function refs to projections
    List<RexNode> bottomExprs = new ArrayList<>();
    List<String> bottomFieldNames = new ArrayList<>();
    Map<RexNode, RexNode> mapping = new HashMap<>();
    for (RexInputRef r : collector.getInputRefExprs()) {
      RexInputRef inputRef = rexBuilder.makeInputRef(
          r.getType(), bottomExprs.size());
      bottomExprs.add(r);
      bottomFieldNames.add(input.getRowType().getFieldNames().get(r.getIndex()));
      mapping.put(r, inputRef);
    }
    int colIndex = 0;
    for (RexNode r : collector.getFunctionExprs()) {
      RexInputRef inputRef = rexBuilder.makeInputRef(
          r.getType(), bottomExprs.size());
      bottomExprs.add(r);
      bottomFieldNames.add("$func_" + (colIndex++));
      mapping.put(r, inputRef);
    }

    // 3. Create bottom project
    RelNode bottomProject = projectFactory.createProject(
        project.getInput(), bottomExprs, bottomFieldNames);

    // 4. Replace expressions within over clause in top project
    FunctionWithinRexOverReplacer replacer =
        new FunctionWithinRexOverReplacer(mapping);
    List<RexNode> topExprs = new ArrayList<>();
    for (RexNode r : project.getChildExps()) {
      topExprs.add(replacer.apply(r));
    }
    RelNode topProject = projectFactory.createProject(
        bottomProject, topExprs, project.getRowType().getFieldNames());

    call.transformTo(topProject);
  }


  /**
   * Traverses an expressions and finds all functions that are part
   * of analytical expressions (RexOver clauses).
   */
  private static class FunctionWithinRexOverCollector extends RexVisitorImpl<Void> {

    private final Set<RexInputRef> inputRefExprs;
    private final Set<RexNode> functionExprs;

    private FunctionWithinRexOverCollector() {
      super(true);
      inputRefExprs = new LinkedHashSet<>();
      functionExprs = new LinkedHashSet<>();
    }

    @Override
    public Void visitInputRef(RexInputRef inputRef) {
      inputRefExprs.add(inputRef);
      return null;
    }

    @Override
    public Void visitOver(RexOver over) {
      // Operands
      for (RexNode operand : over.getOperands()) {
        if (operand instanceof RexCall) {
          functionExprs.add(operand);
        }
      }
      // Partition keys
      for (RexNode partitionKey : over.getWindow().partitionKeys) {
        if (partitionKey instanceof RexCall) {
          functionExprs.add(partitionKey);
        }
      }
      // Order keys
      for (RexFieldCollation orderKey : over.getWindow().orderKeys) {
        if (orderKey.left instanceof RexCall) {
          functionExprs.add(orderKey.left);
        }
      }
      return super.visitOver(over);
    }

    private Set<RexInputRef> getInputRefExprs() {
      return inputRefExprs;
    }

    private Set<RexNode> getFunctionExprs() {
      return functionExprs;
    }
  }

  private static class FunctionWithinRexOverReplacer extends RexShuttle {

    private final Map<RexNode, RexNode> mapping;

    private FunctionWithinRexOverReplacer(Map<RexNode, RexNode> mapping) {
      this.mapping = mapping;
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
      RexNode mappedCall = mapping.get(inputRef);
      return Objects.requireNonNull(mappedCall);
    }

    @Override
    public RexNode visitCall(RexCall call) {
      RexNode mappedCall = mapping.get(call);
      return mappedCall != null ? mappedCall : super.visitCall(call);
    }
  }

}
