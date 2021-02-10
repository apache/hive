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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.materialize.MaterializedViewProjectFilterRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewOnlyFilterRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewProjectJoinRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewOnlyJoinRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewProjectAggregateRule;
import org.apache.calcite.rel.rules.materialize.MaterializedViewOnlyAggregateRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveFilterProjectTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveJoinProjectTransposeRule;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HiveProjectMergeRule;

import java.util.List;

/**
 * Enable join and aggregate materialized view rewriting
 */
public class HiveMaterializedViewRule {

  /**
   * This PROGRAM will be executed when there is a partial rewriting
   * (using union operator) to pull up the projection expressions
   * on top of the input that executes the modified query. The goal
   * of the program is to expose all available expressions below
   * the root of the plan.
   */
  private static final HepProgram PROGRAM = new HepProgramBuilder()
      .addRuleInstance(HiveHepExtractRelNodeRule.INSTANCE)
      .addRuleInstance(HiveVolcanoExtractRelNodeRule.INSTANCE)
      .addRuleInstance(HiveTableScanProjectInsert.INSTANCE)
      .addRuleCollection(
          ImmutableList.of(
              HiveFilterProjectTransposeRule.INSTANCE,
              HiveJoinProjectTransposeRule.BOTH_PROJECT,
              HiveJoinProjectTransposeRule.LEFT_PROJECT,
              HiveJoinProjectTransposeRule.RIGHT_PROJECT,
              HiveProjectMergeRule.INSTANCE))
      .addRuleInstance(ProjectRemoveRule.Config.DEFAULT.toRule())
      .addRuleInstance(HiveRootJoinProjectInsert.INSTANCE)
      .build();

  public static final MaterializedViewProjectFilterRule INSTANCE_PROJECT_FILTER =
    (MaterializedViewProjectFilterRule) MaterializedViewProjectFilterRule.Config.DEFAULT
      .withGenerateUnionRewriting(true)
      .withFastBailOut(false)
      .withUnionRewritingPullProgram(PROGRAM)
      .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
      .toRule();

  public static final MaterializedViewOnlyFilterRule INSTANCE_FILTER =
    (MaterializedViewOnlyFilterRule) MaterializedViewOnlyFilterRule.Config.DEFAULT
      .withGenerateUnionRewriting(true)
      .withFastBailOut(false)
      .withUnionRewritingPullProgram(PROGRAM)
      .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
      .toRule();

  public static final MaterializedViewProjectJoinRule INSTANCE_PROJECT_JOIN =
    (MaterializedViewProjectJoinRule) MaterializedViewProjectJoinRule.Config.DEFAULT
      .withGenerateUnionRewriting(true)
      .withFastBailOut(false)
      .withUnionRewritingPullProgram(PROGRAM)
      .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
      .toRule();

  public static final MaterializedViewOnlyJoinRule INSTANCE_JOIN =
    (MaterializedViewOnlyJoinRule) MaterializedViewOnlyJoinRule.Config.DEFAULT
      .withGenerateUnionRewriting(true)
      .withFastBailOut(false)
      .withUnionRewritingPullProgram(PROGRAM)
      .withRelBuilderFactory(HiveRelFactories.HIVE_BUILDER)
      .toRule();

  public static final HiveMaterializedViewProjectAggregateRule INSTANCE_PROJECT_AGGREGATE =
      new HiveMaterializedViewProjectAggregateRule(HiveRelFactories.HIVE_BUILDER,
          true, PROGRAM);

  public static final HiveMaterializedViewOnlyAggregateRule INSTANCE_AGGREGATE =
      new HiveMaterializedViewOnlyAggregateRule(HiveRelFactories.HIVE_BUILDER,
          true, PROGRAM);

  public static final RelOptRule[] MATERIALIZED_VIEW_REWRITING_RULES =
      new RelOptRule[] {
          HiveMaterializedViewRule.INSTANCE_PROJECT_FILTER,
          HiveMaterializedViewRule.INSTANCE_FILTER,
          HiveMaterializedViewRule.INSTANCE_PROJECT_JOIN,
          HiveMaterializedViewRule.INSTANCE_JOIN,
          HiveMaterializedViewRule.INSTANCE_PROJECT_AGGREGATE,
          HiveMaterializedViewRule.INSTANCE_AGGREGATE };


  protected static class HiveMaterializedViewProjectAggregateRule extends MaterializedViewProjectAggregateRule {
    public HiveMaterializedViewProjectAggregateRule(
        RelBuilderFactory relBuilderFactory, boolean generateUnionRewriting, HepProgram unionRewritingPullProgram) {
      super(relBuilderFactory, generateUnionRewriting, unionRewritingPullProgram);
    }

    @Override
    protected SqlFunction getFloorSqlFunction(TimeUnitRange flag) {
      return HiveRelBuilder.getFloorSqlFunction(flag);
    }

    @Override
    public SqlAggFunction getRollup(SqlAggFunction aggregation) {
      return HiveRelBuilder.getRollup(aggregation);
    }
  }

  protected static class HiveMaterializedViewOnlyAggregateRule extends MaterializedViewOnlyAggregateRule {
    public HiveMaterializedViewOnlyAggregateRule(
        RelBuilderFactory relBuilderFactory, boolean generateUnionRewriting, HepProgram unionRewritingPullProgram) {
      super(relBuilderFactory, generateUnionRewriting, unionRewritingPullProgram);
    }

    @Override
    protected SqlFunction getFloorSqlFunction(TimeUnitRange flag) {
      return HiveRelBuilder.getFloorSqlFunction(flag);
    }

    @Override
    public SqlAggFunction getRollup(SqlAggFunction aggregation) {
      return HiveRelBuilder.getRollup(aggregation);
    }
  }

  /**
   * This rule is used within the PROGRAM that rewrites the query for
   * partial rewritings. Its goal is to extract the RelNode from the
   * HepRelVertex node so the rest of the rules in the PROGRAM can be
   * applied correctly.
   */
  private static class HiveHepExtractRelNodeRule extends RelOptRule {

    private static final HiveHepExtractRelNodeRule INSTANCE =
        new HiveHepExtractRelNodeRule();

    private HiveHepExtractRelNodeRule() {
      super(operand(HepRelVertex.class, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final HepRelVertex rel = call.rel(0);
      call.transformTo(rel.getCurrentRel());
    }
  }

  /**
   * This rule is used within the PROGRAM that rewrites the query for
   * partial rewritings. Its goal is to extract the RelNode from the
   * RelSubset node so the rest of the rules in the PROGRAM can be
   * applied correctly.
   */
  private static class HiveVolcanoExtractRelNodeRule extends RelOptRule {

    private static final HiveVolcanoExtractRelNodeRule INSTANCE =
        new HiveVolcanoExtractRelNodeRule();

    private HiveVolcanoExtractRelNodeRule() {
      super(operand(RelSubset.class, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final RelSubset rel = call.rel(0);
      call.transformTo(Util.first(rel.getBest(), rel.getOriginal()));
    }
  }

  /**
   * This rule inserts an identity Project operator on top of a TableScan.
   * The rule is useful to pull-up the projection expressions during partial
   * rewriting using Union operator, as we would like to have all those
   * expressions available at the top of the input to insert Filter conditions
   * if needed.
   */
  private static class HiveTableScanProjectInsert extends RelOptRule {

    private static final HiveTableScanProjectInsert INSTANCE =
        new HiveTableScanProjectInsert();

    private HiveTableScanProjectInsert() {
      super(operand(Filter.class, operand(TableScan.class, any())),
          HiveRelFactories.HIVE_BUILDER, "HiveTableScanProjectInsert");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Filter fil = call.rel(0);
      final TableScan rel = call.rel(1);
      // Add identity
      RelBuilder relBuilder = call.builder();
      relBuilder.push(rel);
      List<RexNode> identityFields = relBuilder.fields(
          ImmutableBitSet.range(0, rel.getRowType().getFieldCount()).asList());
      RelNode newRel = relBuilder
          .project(identityFields, ImmutableList.of(), true)
          .build();
      call.transformTo(fil.copy(fil.getTraitSet(), ImmutableList.of(newRel)));
    }

  }

  /**
   * This rule adds a Project operator on top of the root operator if it is a join.
   * This is important to meet the requirements set by the rewriting rule with
   * respect to the plan returned by the input program.
   */
  private static class HiveRootJoinProjectInsert extends RelOptRule {

    private static final HiveRootJoinProjectInsert INSTANCE =
        new HiveRootJoinProjectInsert();

    private HiveRootJoinProjectInsert() {
      super(operand(Join.class, any()),
          HiveRelFactories.HIVE_BUILDER, "HiveRootJoinProjectInsert");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      final Join join = call.rel(0);
      final HepRelVertex root = (HepRelVertex) call.getPlanner().getRoot();
      if (root.getCurrentRel() != join) {
        // Bail out
        return;
      }
      // The join is the root, but we should always end up with a Project operator
      // on top. We will add it.
      RelBuilder relBuilder = call.builder();
      relBuilder.push(join);
      List<RexNode> identityFields = relBuilder.fields(
          ImmutableBitSet.range(0, join.getRowType().getFieldCount()).asList());
      relBuilder.project(identityFields, ImmutableList.of(), true);
      call.transformTo(relBuilder.build());
    }

  }
}
