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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import org.apache.calcite.plan.AbstractRelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.DateString;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.Date;

import static org.apache.hadoop.hive.ql.optimizer.calcite.TestHelper.and;
import static org.apache.hadoop.hive.ql.optimizer.calcite.TestHelper.between;
import static org.apache.hadoop.hive.ql.optimizer.calcite.TestHelper.buildPlanner;
import static org.apache.hadoop.hive.ql.optimizer.calcite.TestHelper.buildRelBuilder;
import static org.apache.hadoop.hive.ql.optimizer.calcite.TestHelper.eq;
import static org.apache.hadoop.hive.ql.optimizer.calcite.TestHelper.gt;
import static org.apache.hadoop.hive.ql.optimizer.calcite.TestHelper.gteq;
import static org.apache.hadoop.hive.ql.optimizer.calcite.TestHelper.in;
import static org.apache.hadoop.hive.ql.optimizer.calcite.TestHelper.isNotNull;
import static org.apache.hadoop.hive.ql.optimizer.calcite.TestHelper.isNull;
import static org.apache.hadoop.hive.ql.optimizer.calcite.TestHelper.lt;
import static org.apache.hadoop.hive.ql.optimizer.calcite.TestHelper.lteq;
import static org.apache.hadoop.hive.ql.optimizer.calcite.TestHelper.neq;
import static org.apache.hadoop.hive.ql.optimizer.calcite.TestHelper.notBetween;
import static org.apache.hadoop.hive.ql.optimizer.calcite.TestHelper.notIn;
import static org.apache.hadoop.hive.ql.optimizer.calcite.TestHelper.or;
import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class TestSearchOperator {
  @Mock
  private RelOptSchema schemaMock;
  @Mock
  RelOptHiveTable tableMock;
  @Mock
  Table hiveTableMDMock;

  private AbstractRelOptPlanner planner;
  private RelBuilder relBuilder;

  static class TestRecord {
    public Integer intCol;
    public Double doubleCol;
    public Long longCol;
    public String strCol;
    public Date dateCol;
  }

  @Before
  public void before() {
    planner = buildPlanner(Collections.emptyList());
    relBuilder = buildRelBuilder(planner, schemaMock, tableMock, hiveTableMDMock, TestRecord.class);
  }
  
  @Test
  public void testIn() {
    RelNode plan = relBuilder
        .scan("t")
        .filter(
            and(relBuilder,
                in(relBuilder, "intCol", 1, 2),
                in(relBuilder, "doubleCol", 1.0D, 2.0D),
                in(relBuilder, "longCol", Integer.MAX_VALUE + 1L, Integer.MAX_VALUE + 2L),
                in(relBuilder, "strCol", "one", "two"),
                in(relBuilder, "dateCol", new DateString(2025, 1, 1), 
                    new DateString(2025, 2, 1))
            )
        )
        .build();

    planner.setRoot(plan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    String expected = "AND(SEARCH($0, Sarg[1, 2])," +
        " SEARCH($1, Sarg[1.0E0:DOUBLE, 2.0E0:DOUBLE]:DOUBLE)," +
        " SEARCH($2, Sarg[2147483648L:BIGINT, 2147483649L:BIGINT]:BIGINT)," +
        " SEARCH($3, Sarg['one', 'two']:CHAR(3))," +
        " SEARCH($4, Sarg[2025-01-01, 2025-02-01]))";
    assertEquals(expected, condition.toString());
  }

  @Test
  public void testNotIn() {
    RelNode plan = relBuilder
        .scan("t")
        .filter(
            and(relBuilder,
                notIn(relBuilder, "intCol", 1, 2),
                notIn(relBuilder, "doubleCol", 1.0D, 2.0D),
                notIn(relBuilder, "longCol", Integer.MAX_VALUE + 1L, Integer.MAX_VALUE + 2L),
                notIn(relBuilder, "strCol", "one", "two"),
                notIn(relBuilder, "dateCol", new DateString(2025, 1, 1),
                    new DateString(2025, 2, 1))
            )
        )
        .build();

    planner.setRoot(plan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    String expected = "AND(SEARCH($0, Sarg[(-∞..1), (1..2), (2..+∞)])," +
        " SEARCH($1, Sarg[(-∞..1.0E0:DOUBLE), (1.0E0:DOUBLE..2.0E0:DOUBLE), (2.0E0:DOUBLE..+∞)]:DOUBLE)," +
        " SEARCH($2, Sarg[(-∞..2147483648L:BIGINT), (2147483648L:BIGINT..2147483649L:BIGINT), (2147483649L:BIGINT..+∞)]:BIGINT)," +
        " SEARCH($3, Sarg[(-∞..'one'), ('one'..'two'), ('two'..+∞)]:CHAR(3))," +
        " SEARCH($4, Sarg[(-∞..2025-01-01), (2025-01-01..2025-02-01), (2025-02-01..+∞)]))";
    assertEquals(expected, condition.toString());
  }

  @Test
  public void testBetween() {
    RelNode plan = relBuilder
        .scan("t")
        .filter(
            and(relBuilder,
                between(relBuilder, "intCol", 1, 2),
                between(relBuilder, "doubleCol", 1.0D, 2.0D),
                between(relBuilder, "longCol", Integer.MAX_VALUE + 1L, Integer.MAX_VALUE + 2L),
                between(relBuilder, "strCol", "one", "two"),
                between(relBuilder, "dateCol", new DateString(2025, 1, 1),
                    new DateString(2025, 2, 1))
            )
        )
        .build();

    planner.setRoot(plan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    String expected = "AND(SEARCH($0, Sarg[[1..2]])," +
        " SEARCH($1, Sarg[[1.0E0:DOUBLE..2.0E0:DOUBLE]]:DOUBLE)," +
        " SEARCH($2, Sarg[[2147483648L:BIGINT..2147483649L:BIGINT]]:BIGINT)," +
        " SEARCH($3, Sarg[['one'..'two']]:CHAR(3))," +
        " SEARCH($4, Sarg[[2025-01-01..2025-02-01]]))";
    assertEquals(expected, condition.toString());
  }

  @Test
  public void testNotBetween() {
    RelNode plan = relBuilder
        .scan("t")
        .filter(
            and(relBuilder,
                notBetween(relBuilder, "intCol", 1, 2),
                notBetween(relBuilder, "doubleCol", 1.0D, 2.0D),
                notBetween(relBuilder, "longCol", Integer.MAX_VALUE + 1L, Integer.MAX_VALUE + 2L),
                notBetween(relBuilder, "strCol", "one", "two"),
                notBetween(relBuilder, "dateCol", new DateString(2025, 1, 1),
                    new DateString(2025, 2, 1))
            )
        )
        .build();

    planner.setRoot(plan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    String expected = "AND(SEARCH($0, Sarg[(-∞..1), (2..+∞)])," +
        " SEARCH($1, Sarg[(-∞..1.0E0:DOUBLE), (2.0E0:DOUBLE..+∞)]:DOUBLE)," +
        " SEARCH($2, Sarg[(-∞..2147483648L:BIGINT), (2147483649L:BIGINT..+∞)]:BIGINT)," +
        " SEARCH($3, Sarg[(-∞..'one'), ('two'..+∞)]:CHAR(3))," +
        " SEARCH($4, Sarg[(-∞..2025-01-01), (2025-02-01..+∞)]))";
    assertEquals(expected, condition.toString());
  }

  @Test
  public void testExpressions() {
    RelNode plan = relBuilder
        .scan("t")
        .filter(
            or(relBuilder,
                and(relBuilder, lt(relBuilder, "intCol", 10), neq(relBuilder, "intCol", 5)),
                and(relBuilder, gteq(relBuilder, "doubleCol", 20.5D),
                    neq(relBuilder, "doubleCol", 25.56D)),
                or(relBuilder, eq(relBuilder, "longCol", Integer.MAX_VALUE + 1L),
                    gt(relBuilder, "longCol", Integer.MAX_VALUE + 10L)),
                and(relBuilder, lteq(relBuilder, "strCol", "one"),
                    gt(relBuilder, "strCol", "five")),
                or(relBuilder, eq(relBuilder, "dateCol", new DateString(2025, 1, 1)),
                    gt(relBuilder, "dateCol", new DateString(2025, 2, 1)))
            )
        )
        .build();

    planner.setRoot(plan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    String expected = "OR(SEARCH($0, Sarg[(-∞..5), (5..10)])," +
        " SEARCH($1, Sarg[[2.05E1:DOUBLE..2.556E1:DOUBLE), (2.556E1:DOUBLE..+∞)]:DOUBLE)," +
        " SEARCH($2, Sarg[2147483648L:BIGINT, (2147483657L:BIGINT..+∞)]:BIGINT)," +
        " SEARCH($3, Sarg[('five'..'one':CHAR(4)]]:CHAR(4))," +
        " SEARCH($4, Sarg[2025-01-01, (2025-02-01..+∞)]))";
    assertEquals(expected, condition.toString());
  }

  @Test
  public void testIsNull() {
    RelNode plan = relBuilder
        .scan("t")
        .filter(
            or(relBuilder,
                or(relBuilder, in(relBuilder, "intCol", 10, 20), isNull(relBuilder, "intCol")),
                or(relBuilder, in(relBuilder, "doubleCol", 20.1D, 30.5D), isNull(relBuilder, "doubleCol")),
                or(relBuilder, in(relBuilder, "longCol", Integer.MAX_VALUE + 1L, Integer.MAX_VALUE + 10L), 
                    isNull(relBuilder, "longCol")),
                or(relBuilder, in(relBuilder, "strCol", "one", "eight"), 
                    isNull(relBuilder, "strCol")),
                or(relBuilder, in(relBuilder, "dateCol", new DateString(2025, 1, 1),
                    new DateString(2025, 2, 1)),
                    isNull(relBuilder, "dateCol"))
            )
        )
        .build();

    planner.setRoot(plan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    String expected = "OR(SEARCH($0, Sarg[10, 20; NULL AS TRUE])," +
        " SEARCH($1, Sarg[2.01E1:DOUBLE, 3.05E1:DOUBLE; NULL AS TRUE]:DOUBLE)," +
        " SEARCH($2, Sarg[2147483648L:BIGINT, 2147483657L:BIGINT; NULL AS TRUE]:BIGINT)," +
        " SEARCH($3, Sarg['eight', 'one':CHAR(5); NULL AS TRUE]:CHAR(5))," +
        " SEARCH($4, Sarg[2025-01-01, 2025-02-01; NULL AS TRUE]))";
    assertEquals(expected, condition.toString());
  }

  @Test
  public void testIsNotNull() {
    RelNode plan = relBuilder
        .scan("t")
        .filter(
            or(relBuilder,
                and(relBuilder, in(relBuilder, "intCol", 10, 20),
                    isNotNull(relBuilder, "intCol")),
                and(relBuilder, in(relBuilder, "doubleCol", 20.1D, 30.5D), 
                    isNotNull(relBuilder, "doubleCol")),
                and(relBuilder, in(relBuilder, "longCol", Integer.MAX_VALUE + 1L, Integer.MAX_VALUE + 10L),
                    isNotNull(relBuilder, "longCol")),
                and(relBuilder, in(relBuilder, "strCol", "one", "eight"),
                    isNotNull(relBuilder, "strCol")),
                and(relBuilder, in(relBuilder, "dateCol", new DateString(2025, 1, 1),
                        new DateString(2025, 2, 1)),
                    isNotNull(relBuilder, "dateCol"))
            )
        )
        .build();

    planner.setRoot(plan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    String expected = "OR(SEARCH($0, Sarg[10, 20; NULL AS FALSE])," +
        " SEARCH($1, Sarg[2.01E1:DOUBLE, 3.05E1:DOUBLE; NULL AS FALSE]:DOUBLE)," +
        " SEARCH($2, Sarg[2147483648L:BIGINT, 2147483657L:BIGINT; NULL AS FALSE]:BIGINT)," +
        " SEARCH($3, Sarg['eight', 'one':CHAR(5); NULL AS FALSE]:CHAR(5))," +
        " SEARCH($4, Sarg[2025-01-01, 2025-02-01; NULL AS FALSE]))";
    assertEquals(expected, condition.toString());
  }
}
