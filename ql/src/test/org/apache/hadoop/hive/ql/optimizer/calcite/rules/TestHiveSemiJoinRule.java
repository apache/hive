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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.assertPlans;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.buildPlanner;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.PlanFixture;

@RunWith(MockitoJUnitRunner.class)
public class TestHiveSemiJoinRule {

  private PlanFixture fixture(RelOptRule rule) {
    return new PlanFixture(buildPlanner(Collections.singletonList(rule)))
        .registerTable("t1", TestHiveAntiSemiJoinRule.T1Record.class)
        .registerTable("t2", TestHiveAntiSemiJoinRule.T2Record.class);
  }

  @Test
  public void testProjectLeftJoinWithAggregateOnLeftIsNotRewritten() {
    PlanFixture fixture = fixture(HiveSemiJoinRule.INSTANCE_PROJECT_SWAPPED);
    RelBuilder builder = buildJoinPlan(fixture, JoinRelType.LEFT);
    RelNode plan = builder.project(builder.field("t2id")).build();

    assertUnchanged(fixture, plan);
  }

  @Test
  public void testAggregateLeftJoinWithAggregateOnLeftIsNotRewritten() {
    PlanFixture fixture = fixture(HiveSemiJoinRule.INSTANCE_AGGREGATE_SWAPPED);
    RelBuilder builder = buildJoinPlan(fixture, JoinRelType.LEFT);
    RelNode plan = builder
        .aggregate(builder.groupKey(), builder.aggregateCall(SqlStdOperatorTable.COUNT))
        .build();

    assertUnchanged(fixture, plan);
  }

  @Test
  public void testProjectOuterJoinWithAggregateOnLeftIsNotRewritten() {
    for (JoinRelType joinType : new JoinRelType[] {JoinRelType.RIGHT, JoinRelType.FULL}) {
      PlanFixture fixture = fixture(HiveSemiJoinRule.INSTANCE_PROJECT_SWAPPED);
      RelBuilder builder = buildJoinPlan(fixture, joinType);
      RelNode plan = builder.project(builder.field("t2id")).build();

      assertUnchanged(fixture, plan);
    }
  }

  @Test
  public void testAggregateOuterJoinWithAggregateOnLeftIsNotRewritten() {
    for (JoinRelType joinType : new JoinRelType[] {JoinRelType.RIGHT, JoinRelType.FULL}) {
      PlanFixture fixture = fixture(HiveSemiJoinRule.INSTANCE_AGGREGATE_SWAPPED);
      RelBuilder builder = buildJoinPlan(fixture, joinType);
      RelNode plan = builder
          .aggregate(builder.groupKey(), builder.aggregateCall(SqlStdOperatorTable.COUNT))
          .build();

      assertUnchanged(fixture, plan);
    }
  }

  @Test
  public void testProjectOuterJoinWithAggregateOnRightIsNotRewritten() {
    for (JoinRelType joinType : new JoinRelType[] {JoinRelType.RIGHT, JoinRelType.FULL}) {
      PlanFixture fixture = fixture(HiveSemiJoinRule.INSTANCE_PROJECT);
      RelBuilder builder = buildJoinPlanWithAggregateOnRight(fixture, joinType);
      RelNode plan = builder.project(builder.field("t1id")).build();

      assertUnchanged(fixture, plan);
    }
  }

  @Test
  public void testAggregateOuterJoinWithAggregateOnRightIsNotRewritten() {
    for (JoinRelType joinType : new JoinRelType[] {JoinRelType.RIGHT, JoinRelType.FULL}) {
      PlanFixture fixture = fixture(HiveSemiJoinRule.INSTANCE_AGGREGATE);
      RelBuilder builder = buildJoinPlanWithAggregateOnRight(fixture, joinType);
      RelNode plan = builder
          .aggregate(builder.groupKey(), builder.aggregateCall(SqlStdOperatorTable.COUNT))
          .build();

      assertUnchanged(fixture, plan);
    }
  }

  @Test
  public void testProjectInnerJoinWithAggregateOnLeftIsRewritten() {
    PlanFixture fixture = fixture(HiveSemiJoinRule.INSTANCE_PROJECT_SWAPPED);
    RelBuilder builder = buildJoinPlan(fixture, JoinRelType.INNER);
    RelNode plan = builder.project(builder.field("t2id")).build();

    String expectedPrePlan = "HiveProject(t2id=[$1])\n"
        + "  HiveJoin(condition=[=($0, $1)], joinType=[inner], algorithm=[none], cost=[not available])\n"
        + "    HiveAggregate(group=[{0}])\n"
        + "      LogicalTableScan(table=[[t1]])\n"
        + "    LogicalTableScan(table=[[t2]])\n";

    String expectedPostPlan = "HiveSemiJoin(condition=[=($0, $1)], joinType=[semi])\n"
        + "  HiveProject(t2id=[$0])\n"
        + "    LogicalTableScan(table=[[t2]])\n"
        + "  HiveProject(t1id=[$0])\n"
        + "    LogicalTableScan(table=[[t1]])\n";

    assertPlans(fixture.getPlanner(), plan, expectedPrePlan, expectedPostPlan);
  }

  private RelBuilder buildJoinPlan(PlanFixture fixture, JoinRelType joinType) {
    RelBuilder builder = fixture.createRelBuilder();
    return builder
        .scan("t1")
        .aggregate(builder.groupKey(0))
        .scan("t2")
        .join(joinType, builder.equals(
            builder.field(2, 0, "t1id"),
            builder.field(2, 1, "t2id")));
  }

  private RelBuilder buildJoinPlanWithAggregateOnRight(PlanFixture fixture, JoinRelType joinType) {
    RelBuilder builder = fixture.createRelBuilder();
    return builder
        .scan("t1")
        .scan("t2")
        .aggregate(builder.groupKey(0))
        .join(joinType, builder.equals(
            builder.field(2, 0, "t1id"),
            builder.field(2, 1, "t2id")));
  }

  private void assertUnchanged(PlanFixture fixture, RelNode plan) {
    String expectedPlan = RelOptUtil.toString(plan);
    assertPlans(fixture.getPlanner(), plan, expectedPlan, expectedPlan);
  }

  // Record classes are shared with TestHiveAntiSemiJoinRule.
}
