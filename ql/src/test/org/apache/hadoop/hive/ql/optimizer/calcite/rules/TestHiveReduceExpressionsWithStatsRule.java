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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import com.google.common.collect.Lists;

@RunWith(MockitoJUnitRunner.class)
public class TestHiveReduceExpressionsWithStatsRule {

  @Mock
  private RelOptSchema schemaMock;
  @Mock
  RelOptHiveTable tableMock;
  @Mock
  Table hiveTableMDMock;

  Map<String, String> tableParams = new HashMap<>();

  private HepPlanner planner;
  private RelBuilder builder;
  private ColStatistics statObj;

  private static class MyRecord {
    public int _int;
    public String _str;
  }

  @Before
  public void before() {
    HepProgramBuilder programBuilder = new HepProgramBuilder();
    programBuilder.addRuleInstance(HiveReduceExpressionsWithStatsRule.INSTANCE);

    planner = new HepPlanner(programBuilder.build());

    JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final RelOptCluster optCluster = RelOptCluster.create(planner, rexBuilder);
    RelDataType rowTypeMock = typeFactory.createStructType(MyRecord.class);
    Mockito.doReturn(rowTypeMock).when(tableMock).getRowType();
    LogicalTableScan tableScan = LogicalTableScan.create(optCluster, tableMock, Collections.emptyList());
    doReturn(tableScan).when(tableMock).toRel(any());
    Mockito.doReturn(tableMock).when(schemaMock).getTableForMember(any());
    statObj = new ColStatistics("_int", "int");
    Mockito.doReturn(Lists.newArrayList(statObj)).when(tableMock).getColStat(anyList(), eq(false));
    Mockito.doReturn(hiveTableMDMock).when(tableMock).getHiveTableMD();
    Mockito.doReturn(tableParams).when(hiveTableMDMock).getParameters();

    builder = HiveRelFactories.HIVE_BUILDER.create(optCluster, schemaMock);

    StatsSetupConst.setStatsStateForCreateTable(tableParams, Lists.newArrayList("_int"), StatsSetupConst.TRUE);
    tableParams.put(StatsSetupConst.ROW_COUNT, "3");

  }

  @Test
  public void testGreaterThan_Below() {

    // @formatter:off
    final RelNode basePlan = builder
          .scan("t")
          .filter(
              builder.call(SqlStdOperatorTable.GREATER_THAN,
                    builder.field("_int"), builder.literal(0)
                    )
              )
          .build();
    // @formatter:on

    statObj.setRange(100, 200);
    planner.setRoot(basePlan);
    RelNode optimizedRelNode = planner.findBestExp();
    final String expected = "HiveFilter(condition=[true])\n" +
      "  LogicalTableScan(table=[[]])\n";
    assertEquals("missing literal", expected, RelOptUtil.toString(optimizedRelNode));
  }

  @Test
  public void testIsNull_zero() {

    // @formatter:off
    final RelNode basePlan = builder
          .scan("t")
          .filter(
              builder.call(SqlStdOperatorTable.IS_NULL,
                    builder.field("_str")
                    )
              )
          .build();
    // @formatter:on

    statObj.setNumNulls(0);
    planner.setRoot(basePlan);
    System.out.println(RelOptUtil.toString(basePlan));
    RelNode optimizedRelNode = planner.findBestExp();
    System.out.println(RelOptUtil.toString(optimizedRelNode));
    final String expected = "HiveFilter(condition=[false])\n" +
      "  LogicalTableScan(table=[[]])\n";
    assertEquals("missing literal", expected, RelOptUtil.toString(optimizedRelNode));
  }

  @Test
  public void testIsNull_one() {

    // @formatter:off
    final RelNode basePlan = builder
          .scan("t")
          .filter(
              builder.call(SqlStdOperatorTable.IS_NULL,
                    builder.field("_str")
                    )
              )
          .build();
    // @formatter:on

    statObj.setNumNulls(1);
    planner.setRoot(basePlan);
    System.out.println(RelOptUtil.toString(basePlan));
    RelNode optimizedRelNode = planner.findBestExp();
    System.out.println(RelOptUtil.toString(optimizedRelNode));
    final String expected = "HiveFilter(condition=[IS NULL($1)])\n" +
      "  LogicalTableScan(table=[[]])\n";
    assertEquals("should not be a literal", expected, RelOptUtil.toString(optimizedRelNode));
  }

  @Test
  public void testIsNull_all() {

    // @formatter:off
    final RelNode basePlan = builder
          .scan("t")
          .filter(
              builder.call(SqlStdOperatorTable.IS_NULL,
                    builder.field("_str")
                    )
              )
          .build();
    // @formatter:on

    statObj.setNumNulls(3);
    planner.setRoot(basePlan);
    System.out.println(RelOptUtil.toString(basePlan));
    RelNode optimizedRelNode = planner.findBestExp();
    System.out.println(RelOptUtil.toString(optimizedRelNode));
    final String expected = "HiveFilter(condition=[true])\n" +
      "  LogicalTableScan(table=[[]])\n";
    assertEquals("missing literal", expected, RelOptUtil.toString(optimizedRelNode));
  }

  @Test
  public void testIsNotNull() {

    // @formatter:off
    final RelNode basePlan = builder
          .scan("t")
          .filter(
              builder.call(SqlStdOperatorTable.IS_NOT_NULL,
                    builder.field("_str")
                    )
              )
          .build();
    // @formatter:on

    statObj.setNumNulls(0);
    planner.setRoot(basePlan);
    System.out.println(RelOptUtil.toString(basePlan));
    RelNode optimizedRelNode = planner.findBestExp();
    System.out.println(RelOptUtil.toString(optimizedRelNode));
    final String expected = "HiveFilter(condition=[true])\n" +
      "  LogicalTableScan(table=[[]])\n";
    assertEquals("missing literal", expected, RelOptUtil.toString(optimizedRelNode));
  }

}
