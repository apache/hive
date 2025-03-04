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

import org.apache.calcite.plan.AbstractRelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;

import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.and;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.assertPlans;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.buildPlanner;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.buildRelBuilder;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.gt;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.gteq;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.lt;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.lteq;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.neq;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.or;

@RunWith(MockitoJUnitRunner.class)
public class TestHiveReduceSearchComplexityRule {
  @Mock
  private RelOptSchema schemaMock;
  @Mock
  RelOptHiveTable tableMock;
  @Mock
  Table hiveTableMDMock;
  
  private AbstractRelOptPlanner planner;
  private RelBuilder relBuilder;

  @Before
  public void before() {
    planner = buildPlanner(
        Arrays.asList(HiveReduceSearchComplexityRule.FILTER, HiveReduceSearchComplexityRule.PROJECT)
    );
    relBuilder = buildRelBuilder(planner, schemaMock, tableMock, hiveTableMDMock, TestRuleHelper.MyRecord.class);
  }

  @Test
  public void testFilterConditionWithSearchOnSingleColumn() {
    RelNode plan = relBuilder
        .scan("t")
        .filter(
            and(relBuilder,
                neq(relBuilder, "f1", 1),
                neq(relBuilder, "f1", 2))
        )
        .build();
    
    String expectedPrePlan = 
        "HiveFilter(condition=[SEARCH($0, Sarg[(-∞..1), (1..2), (2..+∞)])])\n" + 
        "  LogicalTableScan(table=[[]])\n";
    String expectedPostPlan = 
        "HiveFilter(condition=[NOT(SEARCH($0, Sarg[1, 2]))])\n" +
        "  LogicalTableScan(table=[[]])\n";
    
    assertPlans(planner, plan, expectedPrePlan, expectedPostPlan);
  }
  
  @Test
  public void testFilterConditionWithSearchOnMultipleColumns() {
    RelNode plan = relBuilder
        .scan("t")
        .filter(
            or(relBuilder,
                and(relBuilder,
                    neq(relBuilder, "f1", 1),
                    neq(relBuilder, "f1", 2)),
                and(relBuilder,
                    neq(relBuilder, "f2", 1),
                    neq(relBuilder, "f2", 2))
            )
        )
        .build();
    
    String expectedPrePlan = 
        "HiveFilter(condition=[OR(SEARCH($0, Sarg[(-∞..1), (1..2), (2..+∞)]), SEARCH($1, Sarg[(-∞..1), (1..2), (2..+∞)]))])\n" +
        "  LogicalTableScan(table=[[]])\n";
    String expectedPostPlan = 
        "HiveFilter(condition=[OR(NOT(SEARCH($0, Sarg[1, 2])), NOT(SEARCH($1, Sarg[1, 2])))])\n" +
        "  LogicalTableScan(table=[[]])\n";

    assertPlans(planner, plan, expectedPrePlan, expectedPostPlan);
  }
  
  @Test
  public void testFilterConditionWithNotBetween() {
    RelNode plan = relBuilder
        .scan("t")
        .filter(
            or(relBuilder,
                lt(relBuilder, "f1", 10),
                gt(relBuilder, "f1", 20)
            )
        )
        .build();

    String expectedPrePlan = 
        "HiveFilter(condition=[SEARCH($0, Sarg[(-∞..10), (20..+∞)])])\n" +
        "  LogicalTableScan(table=[[]])\n";
    String expectedPostPlan = 
        "HiveFilter(condition=[NOT(SEARCH($0, Sarg[[10..20]]))])\n" +
        "  LogicalTableScan(table=[[]])\n";

    assertPlans(planner, plan, expectedPrePlan, expectedPostPlan);
  }
  
  @Test
  public void testFilterConditionWithBetweenAndNotBetween() {
    RelNode plan = relBuilder
        .scan("t")
        .filter(
            or(relBuilder,
                or(relBuilder,
                    lt(relBuilder, "f1", 10),
                    gt(relBuilder, "f1", 20)
                ),
                and(relBuilder,
                    gteq(relBuilder, "f2", 30),
                    lteq(relBuilder, "f2", 40)
                )
            )
        )
        .build();

    String expectedPrePlan =
        "HiveFilter(condition=[OR(SEARCH($0, Sarg[(-∞..10), (20..+∞)]), SEARCH($1, Sarg[[30..40]]))])\n" +
        "  LogicalTableScan(table=[[]])\n";
    String expectedPostPlan =
        "HiveFilter(condition=[OR(NOT(SEARCH($0, Sarg[[10..20]])), SEARCH($1, Sarg[[30..40]]))])\n" +
        "  LogicalTableScan(table=[[]])\n";

    assertPlans(planner, plan, expectedPrePlan, expectedPostPlan);
  }
  
  @Test
  public void testFilterConditionWhenNegationIsBetterBasedOnCountOfClosedRanges() {
    RelNode plan = relBuilder
        .scan("t")
        .filter(
            or(relBuilder,
                lt(relBuilder, "f1", 30),
                and(relBuilder,
                    gt(relBuilder, "f1", 40),
                    lt(relBuilder, "f1", 50)
                )
            )
        )
        .build();

    String expectedPrePlan =
        "HiveFilter(condition=[SEARCH($0, Sarg[(-∞..30), (40..50)])])\n" +
        "  LogicalTableScan(table=[[]])\n";
    String expectedPostPlan =
        "HiveFilter(condition=[NOT(SEARCH($0, Sarg[[30..40], [50..+∞)]))])\n" +
        "  LogicalTableScan(table=[[]])\n";

    assertPlans(planner, plan, expectedPrePlan, expectedPostPlan);
  }

  @Test
  public void testProjectWithSearchOnSingleColumn() {
    RelNode plan = relBuilder
        .scan("t")
        .project(
            and(relBuilder,
                neq(relBuilder, "f1", 1),
                neq(relBuilder, "f1", 2))
        )
        .build();

    String expectedPrePlan =
        "HiveProject($f0=[SEARCH($0, Sarg[(-∞..1), (1..2), (2..+∞)])])\n" +
        "  LogicalTableScan(table=[[]])\n";
    String expectedPostPlan =
        "HiveProject($f0=[NOT(SEARCH($0, Sarg[1, 2]))])\n" +
        "  LogicalTableScan(table=[[]])\n";

    assertPlans(planner, plan, expectedPrePlan, expectedPostPlan);
  }

  @Test
  public void testProjectWithSearchOnMultipleColumns() {
    RelNode plan = relBuilder
        .scan("t")
        .project(
            or(relBuilder,
                and(relBuilder,
                    neq(relBuilder, "f1", 1),
                    neq(relBuilder, "f1", 2)),
                and(relBuilder,
                    neq(relBuilder, "f2", 1),
                    neq(relBuilder, "f2", 2))
            )
        )
        .build();

    String expectedPrePlan =
        "HiveProject($f0=[OR(SEARCH($0, Sarg[(-∞..1), (1..2), (2..+∞)]), SEARCH($1, Sarg[(-∞..1), (1..2), (2..+∞)]))])\n" +
        "  LogicalTableScan(table=[[]])\n";
    String expectedPostPlan =
        "HiveProject($f0=[OR(NOT(SEARCH($0, Sarg[1, 2])), NOT(SEARCH($1, Sarg[1, 2])))])\n" +
        "  LogicalTableScan(table=[[]])\n";

    assertPlans(planner, plan, expectedPrePlan, expectedPostPlan);
  }

  @Test
  public void testProjectWithNotBetween() {
    RelNode plan = relBuilder
        .scan("t")
        .project(
            or(relBuilder,
                lt(relBuilder, "f1", 10),
                gt(relBuilder, "f1", 20)
            )
        )
        .build();

    String expectedPrePlan =
        "HiveProject($f0=[SEARCH($0, Sarg[(-∞..10), (20..+∞)])])\n" +
        "  LogicalTableScan(table=[[]])\n";
    String expectedPostPlan =
        "HiveProject($f0=[NOT(SEARCH($0, Sarg[[10..20]]))])\n" +
        "  LogicalTableScan(table=[[]])\n";

    assertPlans(planner, plan, expectedPrePlan, expectedPostPlan);
  }

  @Test
  public void testProjectWithBetweenAndNotBetween() {
    RelNode plan = relBuilder
        .scan("t")
        .project(
            or(relBuilder,
                or(relBuilder,
                    lt(relBuilder, "f1", 10),
                    gt(relBuilder, "f1", 20)
                ),
                and(relBuilder,
                    gteq(relBuilder, "f2", 30),
                    lteq(relBuilder, "f2", 40)
                )
            )
        )
        .build();

    String expectedPrePlan =
        "HiveProject($f0=[OR(SEARCH($0, Sarg[(-∞..10), (20..+∞)]), SEARCH($1, Sarg[[30..40]]))])\n" +
        "  LogicalTableScan(table=[[]])\n";
    String expectedPostPlan =
        "HiveProject($f0=[OR(NOT(SEARCH($0, Sarg[[10..20]])), SEARCH($1, Sarg[[30..40]]))])\n" +
        "  LogicalTableScan(table=[[]])\n";

    assertPlans(planner, plan, expectedPrePlan, expectedPostPlan);
  }

  @Test
  public void testProjectWhenNegationIsBetterBasedOnCountOfClosedRanges() {
    RelNode plan = relBuilder
        .scan("t")
        .project(
            or(relBuilder,
                lt(relBuilder, "f1", 30),
                and(relBuilder,
                    gt(relBuilder, "f1", 40),
                    lt(relBuilder, "f1", 50)
                )
            )
        )
        .build();

    String expectedPrePlan =
        "HiveProject($f0=[SEARCH($0, Sarg[(-∞..30), (40..50)])])\n" +
        "  LogicalTableScan(table=[[]])\n";
    String expectedPostPlan =
        "HiveProject($f0=[NOT(SEARCH($0, Sarg[[30..40], [50..+∞)]))])\n" +
        "  LogicalTableScan(table=[[]])\n";

    assertPlans(planner, plan, expectedPrePlan, expectedPostPlan);
  }
}
