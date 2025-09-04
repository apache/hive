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

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.*;

@RunWith(MockitoJUnitRunner.class)
public class TestHiveAntiSemiJoinRule {

  PlanFixture fixture() {
    RelOptPlanner planner = buildPlanner(Collections.singletonList(HiveAntiSemiJoinRule.INSTANCE));
    return new PlanFixture(planner).registerTable("t1", T1Record.class).registerTable("t2", T2Record.class);
  }

  @Test
  public void testFilterOnNullableColumn() {
    PlanFixture fixture = fixture();
    RelBuilder b = fixture.createRelBuilder();

    // @formatter:off
    RelNode plan = b
        .scan("t1")
        .scan("t2")
        .join(JoinRelType.LEFT, b.equals(
            b.field(2, 0, "t1nullable"),
            b.field(2, 1, "t2id")))
        .filter(b.isNull(b.field("t2nullable")))
        .project(b.field("t1id"))
        .build();

    String expectedPlan = "HiveProject(t1id=[$0])\n"
                        + "  HiveFilter(condition=[IS NULL($5)])\n"
                        + "    HiveJoin(condition=[=($2, $3)], joinType=[left], algorithm=[none], cost=[not available])\n"
                        + "      LogicalTableScan(table=[[t1]])\n"
                        + "      LogicalTableScan(table=[[t2]])\n";
    // @formatter:on

    assertPlans(fixture.getPlanner(), plan, expectedPlan, expectedPlan);
  }

  @Test
  public void testFilterIsNullFromBothSides() {
    PlanFixture fixture = fixture();

    RelNode plan;
    try (Hook.Closeable ignore = Hook.REL_BUILDER_SIMPLIFY.addThread(Hook.propertyJ(false))) {
      RelBuilder b = fixture.createRelBuilder();
      // @formatter:off
      plan = b.scan("t1")
              .scan("t2")
              .join(JoinRelType.LEFT, b.equals(b.field(2, 0, "t1nullable"), b.field(2, 1, "t2id")))
              .filter(b.isNull(b.call(SqlStdOperatorTable.PLUS, b.field("t2nullable"), b.field("t1nullable"))))
              .project(b.field("t1id")).build();
      // @formatter:on
    }

    // @formatter:off
    String expectedPlan = "HiveProject(t1id=[$0])\n"
                        + "  HiveFilter(condition=[IS NULL(+($5, $2))])\n"
                        + "    HiveJoin(condition=[=($2, $3)], joinType=[left], algorithm=[none], cost=[not available])\n"
                        + "      LogicalTableScan(table=[[t1]])\n"
                        + "      LogicalTableScan(table=[[t2]])\n";
    // @formatter:on

    assertPlans(fixture.getPlanner(), plan, expectedPlan, expectedPlan);
  }

  @Test
  public void testFilterOnNotNullColumn() {
    PlanFixture fixture = fixture();
    RelBuilder b = fixture.createRelBuilder();

    // @formatter:off
    RelNode plan = b
        .scan("t1")
        .scan("t2")
        .join(JoinRelType.LEFT, b.equals(
            b.field(2, 0, "t1nullable"),
            b.field(2, 1, "t2id")))
        .filter(b.isNull(b.field("t2notnull")))
        .project(b.field("t1id"))
        .build();

    String prePlan = "HiveProject(t1id=[$0])\n"
                   + "  HiveFilter(condition=[IS NULL($4)])\n"
                   + "    HiveJoin(condition=[=($2, $3)], joinType=[left], algorithm=[none], cost=[not available])\n"
                   + "      LogicalTableScan(table=[[t1]])\n"
                   + "      LogicalTableScan(table=[[t2]])\n";

    String postPlan = "HiveProject(t1id=[$0])\n"
                   + "  HiveAntiJoin(condition=[=($2, $3)], joinType=[anti])\n"
                   + "    LogicalTableScan(table=[[t1]])\n"
                   + "    LogicalTableScan(table=[[t2]])\n";
    // @formatter:on

    assertPlans(fixture.getPlanner(), plan, prePlan, postPlan);
  }

  @Test
  public void testFilterOnNullAndNotNullColumn() {
    PlanFixture fixture = fixture();
    RelBuilder b = fixture.createRelBuilder();

    // @formatter:off
    RelNode plan = b
        .scan("t1")
        .scan("t2")
        .join(JoinRelType.LEFT, b.equals(
            b.field(2, 0, "t1nullable"),
            b.field(2, 1, "t2id")))
        .filter(b.and(b.isNull(b.field("t2notnull")), b.isNull((b.field("t2nullable")))))
        .project(b.field("t1id"))
        .build();

    String prePlan = "HiveProject(t1id=[$0])\n"
        + "  HiveFilter(condition=[AND(IS NULL($4), IS NULL($5))])\n"
        + "    HiveJoin(condition=[=($2, $3)], joinType=[left], algorithm=[none], cost=[not available])\n"
        + "      LogicalTableScan(table=[[t1]])\n"
        + "      LogicalTableScan(table=[[t2]])\n";

    String postPlan = "HiveProject(t1id=[$0])\n"
        + "  HiveAntiJoin(condition=[=($2, $3)], joinType=[anti])\n"
        + "    LogicalTableScan(table=[[t1]])\n"
        + "    LogicalTableScan(table=[[t2]])\n";
    // @formatter:on

    assertPlans(fixture.getPlanner(), plan, prePlan, postPlan);
  }

  static class T1Record {
    public int t1id;
    public int t1notnull;
    public Integer t1nullable;
  }

  static class T2Record {
    public int t2id;
    public int t2notnull;
    public Integer t2nullable;
  }
}
