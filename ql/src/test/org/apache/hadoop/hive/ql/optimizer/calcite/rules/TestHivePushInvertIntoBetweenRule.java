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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveBetween;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class TestHivePushInvertIntoBetweenRule {

  @Mock
  private RelOptSchema schemaMock;
  @Mock
  RelOptHiveTable tableMock;
  @Mock
  Table hiveTableMDMock;

  private HepPlanner planner;
  private RelBuilder builder;

  @SuppressWarnings("unused")
  private static class MyRecord {
    public int f1;
    public int f2;
    public int f3;
  }

  @Before
  public void before() {
    HepProgramBuilder programBuilder = new HepProgramBuilder();
    programBuilder.addRuleInstance(HiveDruidPushInvertIntoBetweenRule.INSTANCE);

    planner = new HepPlanner(programBuilder.build());

    JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final RelOptCluster optCluster = RelOptCluster.create(planner, rexBuilder);
    RelDataType rowTypeMock = typeFactory.createStructType(MyRecord.class);
    Mockito.doReturn(rowTypeMock).when(tableMock).getRowType();
    Mockito.doReturn(tableMock).when(schemaMock).getTableForMember(Matchers.any());
    Mockito.doReturn(hiveTableMDMock).when(tableMock).getHiveTableMD();

    builder = HiveRelFactories.HIVE_BUILDER.create(optCluster, schemaMock);

  }

  public RexNode or(RexNode... args) {
    return builder.call(SqlStdOperatorTable.OR, args);
  }

  public RexNode between(boolean invert, String field, int value1, int value2) {
    return builder.call(HiveBetween.INSTANCE,
        builder.literal(invert), builder.field(field), builder.literal(value1), builder.literal(value2));
  }

  public RexNode and(RexNode... args) {
    return builder.call(SqlStdOperatorTable.AND, args);
  }

  public RexNode not(RexNode... args) {
    return builder.call(SqlStdOperatorTable.NOT, args);
  }

  public RexNode gt(String field, int value) {
    return builder.call(SqlStdOperatorTable.GREATER_THAN,
        builder.field(field), builder.literal(value));
  }

  public RexNode lt(String field, int value) {
    return builder.call(SqlStdOperatorTable.LESS_THAN,
        builder.field(field), builder.literal(value));
  }

  @Test
  public void testSimpleCase() {
    // @formatter:off
    final RelNode basePlan = builder
          .scan("t")
          .filter(
                  not(
              between( false,"f1", 1,10)
                  )
          )
          .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    assertEquals("BETWEEN(true, $0, 1, 10)", condition.toString());
  }

  @Test
  public void testSimpleAlreadyInvertCase() {

    // @formatter:off
    final RelNode basePlan = builder
            .scan("t")
            .filter(
                    not(
                            between( true,"f1", 1,10)
                    )
            )
            .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    assertEquals("BETWEEN(false, $0, 1, 10)", condition.toString());
  }

  @Test
  public void testBetweenWithOrCase() {

    // @formatter:off
    final RelNode basePlan = builder
            .scan("t")
            .filter(
                    or(
                      not(between( false,"f1", 1,10)),
                      not(between( true,"f2", 2,20))
                    )
            )
            .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    assertEquals("OR(BETWEEN(true, $0, 1, 10), BETWEEN(false, $1, 2, 20))", condition.toString());
  }

  @Test
  public void testBetweenAbsentCase() {

    // @formatter:off
    final RelNode basePlan = builder
            .scan("t")
            .filter(
                    and(
                    between( true,"f1", 1,10),
                    between( false,"f1", 1,10)
                    )
            )
            .build();
    // @formatter:on

    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();

    HiveFilter filter = (HiveFilter) optimizedRelNode;
    RexNode condition = filter.getCondition();
    assertEquals("AND(BETWEEN(true, $0, 1, 10), BETWEEN(false, $0, 1, 10))", condition.toString());
  }

}
