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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPermuteInputsShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.CommonRelSubExprRegisterRule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Suggester for join common table expressions that appear more than once in the query plan.
 */
public class CommonTableExpressionJoinSuggester implements CommonTableExpressionSuggester {
  private static final RelOptRule JOIN_PROJECT_TRANSPOSE_RULE = JoinProjectTransposeRule.Config.DEFAULT
          .withOperandSupplier(j -> j.operand(Join.class).inputs(
              l -> l.operand(Project.class).anyInputs(), 
              r -> r.operand(Project.class).anyInputs()))
          .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
          .toRule();
  private static final RelOptRule PROJECT_FILTER_TRANSPOSE_RULE = ProjectFilterTransposeRule.Config.DEFAULT
          .withOperandFor(Project.class, Filter.class)
          .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
          .toRule();
  private static final RelOptRule JOIN_FILTER_TRANSPOSE_RULE = new JoinFilterTransposeRule();
  private static final RelOptRule JOIN_CTE = CommonRelSubExprRegisterRule.JOIN;

  @Override
  public List<RelNode> suggest(final RelNode input, final Configuration configuration) {
    List<RelOptRule> rules =
        Arrays.asList(JOIN_FILTER_TRANSPOSE_RULE, JOIN_PROJECT_TRANSPOSE_RULE, PROJECT_FILTER_TRANSPOSE_RULE, JOIN_CTE);
    CommonTableExpressionRegistry localRegistry = new CommonTableExpressionRegistry();
    HepPlanner planner =
        new HepPlanner(new HepProgramBuilder().addRuleCollection(rules).build(), Contexts.of(localRegistry));
    planner.setRoot(input);
    planner.findBestExp();
    return localRegistry.entries().collect(Collectors.toList());
  }

  /**
   * Pushes a join below a filter.
   */
  private static final class JoinFilterTransposeRule extends RelOptRule {
    public JoinFilterTransposeRule() {
      super(operand(Join.class, operand(RelNode.class, any()), operand(RelNode.class, any())),
          HiveRelFactories.HIVE_BUILDER, "JoinFilterTransposeRule");
    }

    @Override
    public boolean matches(final RelOptRuleCall call) {
      Join j = call.rel(0);
      RelNode l = call.rel(1);
      RelNode r = call.rel(2);
      return JoinRelType.INNER.equals(j.getJoinType()) && (l instanceof Filter || r instanceof Filter);
    }

    @Override
    public void onMatch(final RelOptRuleCall call) {
      Join j = call.rel(0);
      RelNode l = call.rel(1);
      RelNode r = call.rel(2);
      RelBuilder b = call.builder();
      b.push(j);
      List<RexNode> filterPredicates = new ArrayList<>();
      int nFieldsLeft = l.getRowType().getFieldList().size();
      int nFieldsRight = r.getRowType().getFieldList().size();

      if (l instanceof Filter) {
        Filter lF = (Filter) l;
        b.push(lF.getInput());
        Mappings.TargetMapping leftMapping = Mappings.createShiftMapping(nFieldsLeft, 0, 0, nFieldsLeft);
        filterPredicates.add(lF.getCondition().accept(new RexPermuteInputsShuttle(leftMapping, l)));
      } else {
        b.push(l);
      }
      if (r instanceof Filter) {
        Filter rF = (Filter) r;
        b.push(rF.getInput());
        Mappings.TargetMapping rightMapping =
            Mappings.createShiftMapping(nFieldsLeft + nFieldsRight, nFieldsLeft, 0, nFieldsRight);
        filterPredicates.add(rF.getCondition().accept(new RexPermuteInputsShuttle(rightMapping, r)));
      } else {
        b.push(r);
      }
      b.join(j.getJoinType(), j.getCondition());
      b.filter(filterPredicates);
      call.transformTo(b.build());
    }
  }
}
