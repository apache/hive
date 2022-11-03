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
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.buildPlanner;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.buildRelBuilder;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.and;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.eq;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.or;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class TestHivePointLookupOptimizerRule {

  @Mock
  private RelOptSchema schemaMock;
  @Mock
  RelOptHiveTable tableMock;
  @Mock
  Table hiveTableMDMock;

  private AbstractRelOptPlanner planner;
  private RelBuilder relBuilder;

  @SuppressWarnings("unused")
  private static class MyRecord {
    public int f1;
    public int f2;
    public int f3;
    public double f4;
  }

  @Before
  public void before() {
    planner = buildPlanner(Collections.singletonList(new HivePointLookupOptimizerRule.FilterCondition(2)));
    relBuilder = buildRelBuilder(planner, schemaMock, tableMock, hiveTableMDMock, MyRecord.class);
  }

  @Test
  public void testSimpleCase() {

    // @formatter:off
    final RelNode basePlan = relBuilder
          .scan("t")
          .filter(
              and(relBuilder,
                or(relBuilder,
                    eq(relBuilder, "f1",1),
                    eq(relBuilder, "f1",2)
                    ),
                or(relBuilder,
                    eq(relBuilder, "f2",3),
                    eq(relBuilder, "f2",4)
                    )
                )
              )
          .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    assertEquals("AND(IN($0, 1, 2), IN($1, 3, 4))", condition.toString());
  }

  @Test
  public void testInExprsMergedSingleOverlap() {

    // @formatter:off
    final RelNode basePlan = relBuilder
        .scan("t")
        .filter(
            and(relBuilder,
                or(relBuilder,
                    eq(relBuilder,"f1",1),
                    eq(relBuilder,"f1",2)
                ),
                or(relBuilder,
                    eq(relBuilder,"f1",1),
                    eq(relBuilder,"f1",3)
                )
            )
        )
        .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    assertEquals("=($0, 1)", condition.toString());
  }

  @Test
  public void testInExprsAndEqualsMerged() {

    // @formatter:off
    final RelNode basePlan = relBuilder
        .scan("t")
        .filter(
            and(relBuilder,
                or(relBuilder,
                    eq(relBuilder,"f1",1),
                    eq(relBuilder,"f1",2)
                ),
                or(relBuilder,
                    eq(relBuilder,"f1",1),
                    eq(relBuilder,"f1",3)
                ),
                eq(relBuilder,"f1",1)
            )
        )
        .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    assertEquals("=($0, 1)", condition.toString());
  }

  @Test
  public void testInExprsMergedMultipleOverlap() {

    // @formatter:off
    final RelNode basePlan = relBuilder
        .scan("t")
        .filter(
            and(relBuilder,
                or(relBuilder,
                    eq(relBuilder,"f1",1),
                    eq(relBuilder,"f1",2),
                    eq(relBuilder,"f1",4),
                    eq(relBuilder,"f1",3)
                ),
                or(relBuilder,
                    eq(relBuilder,"f1",5),
                    eq(relBuilder,"f1",1),
                    eq(relBuilder,"f1",2),
                    eq(relBuilder,"f1",3)
                )
            )
        )
        .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    assertEquals("IN($0, 1, 2, 3)", condition.toString());
  }

  @Test
  public void testCaseWithConstantsOfDifferentType() {

    // @formatter:off
    final RelNode basePlan = relBuilder
        .scan("t")
        .filter(
            and(relBuilder,
                or(relBuilder,
                    eq(relBuilder,"f1",1),
                    eq(relBuilder,"f1",2)
                ),
                eq(relBuilder,"f1", 1.0),
                or(relBuilder,
                    eq(relBuilder,"f4",3.0),
                    eq(relBuilder,"f4",4.1)
                )
            )
        )
        .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    // ideally the result would be AND(=($0, 1), IN($3, 3.0E0:DOUBLE, 4.1E0:DOUBLE)), but we
    // don't try to compare constants of different type for the same column, even if comparable
    assertEquals("AND(IN($0, 1, 2), =($0, 1.0E0:DOUBLE), IN($3, 3.0E0:DOUBLE, 4.1E0:DOUBLE))",
        condition.toString());
  }

  @Test
  public void testCaseInAndEqualsWithConstantsOfDifferentType() {

    // @formatter:off
    final RelNode basePlan = relBuilder
        .scan("t")
        .filter(
            and(relBuilder,
                or(relBuilder,
                    eq(relBuilder,"f1",1),
                    eq(relBuilder,"f1",2)
                ),
                eq(relBuilder,"f1",1),
                or(relBuilder,
                    eq(relBuilder,"f4",3.0),
                    eq(relBuilder,"f4",4.1)
                ),
                eq(relBuilder,"f4",4.1)
            )
        )
        .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    assertEquals("AND(=($0, 1), =($3, 4.1E0:DOUBLE))", condition.toString());
  }

  @Test
  public void testSimpleStructCase() {

    // @formatter:off
    final RelNode basePlan = relBuilder
          .scan("t")
          .filter(
              or(relBuilder,
                  and(relBuilder,
                      eq(relBuilder,"f1",1), eq(relBuilder,"f2",1)),
                  and(relBuilder,
                      eq(relBuilder,"f1",2), eq(relBuilder,"f2",2))
                  )
              )
          .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    assertEquals("IN(ROW($0, $1), ROW(1, 1), ROW(2, 2))", condition.toString());
  }

  /** Despite the fact that f2=99 is there...the extraction should happen */
  @Test
  public void testObscuredSimple() {

    // @formatter:off
    final RelNode basePlan = relBuilder
          .scan("t")
          .filter(
              or(relBuilder,
                  eq(relBuilder,"f2",99),
                  eq(relBuilder,"f1",1),
                  eq(relBuilder,"f1",2)
                  )
              )
          .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    System.out.println(condition);
    assertEquals("OR(IN($0, 1, 2), =($1, 99))", condition.toString());
  }

  /** Despite that extraction happen at a higher level; nested parts should also be handled */
  @Test
  public void testRecursionIsNotObstructed() {

    // @formatter:off
    final RelNode basePlan = relBuilder
          .scan("t")
          .filter(
              and(relBuilder,
                or(relBuilder,
                    eq(relBuilder,"f1",1),
                    eq(relBuilder,"f1",2)
                    )
                ,
                or(relBuilder,
                    and(relBuilder,
                        or(relBuilder,
                            eq(relBuilder,"f2",1), eq(relBuilder,"f2",2)),
                        or(relBuilder,
                            eq(relBuilder,"f3",1), eq(relBuilder,"f3",2))
                        ),
                    and(relBuilder,
                        or(relBuilder,
                            eq(relBuilder,"f2",3),eq(relBuilder,"f2",4)),
                        or(relBuilder,
                            eq(relBuilder,"f3",3),eq(relBuilder,"f3",4))
                        )
                )
              ))
          .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    System.out.println(condition);
    assertEquals("AND(IN($0, 1, 2), OR(AND(IN($1, 1, 2), IN($2, 1, 2)), "
            + "AND(IN($1, 3, 4), IN($2, 3, 4))))",
        condition.toString());
  }

}
