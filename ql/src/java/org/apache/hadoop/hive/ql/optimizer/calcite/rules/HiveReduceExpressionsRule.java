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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAggregate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Collection of planner rules that apply various simplifying transformations on
 * RexNode trees. Currently, there are two transformations:
 *
 * <ul>
 * <li>Constant reduction, which evaluates constant subtrees, replacing them
 * with a corresponding RexLiteral
 * <li>Removal of redundant casts, which occurs when the argument into the cast
 * is the same as the type of the resulting cast expression
 * </ul>
 */
public final class HiveReduceExpressionsRule {

  private HiveReduceExpressionsRule() {
    throw new IllegalStateException("Instantiation not allowed");
  }

  /**
   * Singleton rule that reduces constants inside a
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter}.
   */
  public static final RelOptRule FILTER_INSTANCE =
      ReduceExpressionsRule.FilterReduceExpressionsRule.Config.DEFAULT
          .withOperandFor(HiveFilter.class)
          .withMatchNullability(false)
          .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
          .as(ReduceExpressionsRule.FilterReduceExpressionsRule.Config.class)
          .toRule();

  /**
   * Singleton rule that reduces constants inside a
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject}.
   */
  public static final RelOptRule PROJECT_INSTANCE =
      HiveProjectReduceExpressionsRule.Config.DEFAULT
          .withOperandFor(HiveProject.class)
          .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
          .as(HiveProjectReduceExpressionsRule.Config.class)
          .toRule();

  /**
   * Singleton rule that reduces constants inside a
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin}.
   */
  public static final RelOptRule JOIN_INSTANCE =
      ReduceExpressionsRule.JoinReduceExpressionsRule.Config.DEFAULT
          .withOperandFor(HiveJoin.class)
          .withMatchNullability(false)
          .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
          .as(ReduceExpressionsRule.JoinReduceExpressionsRule.Config.class)
          .toRule();

  /**
   * Singleton rule that reduces constants inside a
   * {@link org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin}.
   */
  public static final RelOptRule SEMIJOIN_INSTANCE =
      ReduceExpressionsRule.JoinReduceExpressionsRule.Config.DEFAULT
          .withOperandFor(HiveSemiJoin.class)
          .withMatchNullability(false)
          .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
          .as(ReduceExpressionsRule.JoinReduceExpressionsRule.Config.class)
          .toRule();

  public static class HiveProjectReduceExpressionsRule extends ReduceExpressionsRule.ProjectReduceExpressionsRule {
    protected HiveProjectReduceExpressionsRule(ProjectReduceExpressionsRule.Config config) {
      super(config);
    }

    private boolean hasGroupingSets(Project project) {
      RelNode input = project.getInput();
      if (input instanceof HepRelVertex) {
        HepRelVertex hepInput = (HepRelVertex) input;
        if (hepInput.getCurrentRel() instanceof HiveAggregate) {
          HiveAggregate aggregate = (HiveAggregate) hepInput.getCurrentRel();
          return aggregate.getGroupType() != Aggregate.Group.SIMPLE;
        }
      }

      return false;
    }

    @Override public void onMatch(RelOptRuleCall call) {
      final Project project = call.rel(0);
      final RelMetadataQuery mq = call.getMetadataQuery();
      final RelOptPredicateList predicates = mq.getPulledUpPredicates(project.getInput());
      final List<RexNode> expList;
      if (hasGroupingSets(project)) {
        expList = project.getProjects().stream().filter(v -> !(v instanceof RexCall)).collect(Collectors.toList());
      } else {
        expList = Lists.newArrayList(project.getProjects());
      }

      if (reduceExpressions(project, expList, predicates, false, config.matchNullability())) {
        assert !project.getProjects().equals(expList) : "Reduced expressions should be different from original expressions";
        call.transformTo(
            call.builder().push(project.getInput()).project(expList, project.getRowType().getFieldNames()).build()
        );

        // New plan is absolutely better than old plan.
        call.getPlanner().prune(project);
      }
    }

    public interface Config extends ProjectReduceExpressionsRule.Config {
      HiveProjectReduceExpressionsRule.Config DEFAULT = EMPTY.as(HiveProjectReduceExpressionsRule.Config.class)
          .withMatchNullability(true)
          .withOperandFor(LogicalProject.class)
          .withDescription("HiveProjectReduceExpressionsRule(Project)")
          .as(HiveProjectReduceExpressionsRule.Config.class);

      @Override default HiveProjectReduceExpressionsRule toRule() {
        return new HiveProjectReduceExpressionsRule(this);
      }
    }
  }
}

// End HiveReduceExpressionsRule.java
