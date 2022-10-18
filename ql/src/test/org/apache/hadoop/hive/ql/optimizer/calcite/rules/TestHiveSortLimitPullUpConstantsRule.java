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
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;

@RunWith(MockitoJUnitRunner.class)
public class TestHiveSortLimitPullUpConstantsRule {

  private final static JavaTypeFactoryImpl JAVA_TYPE_FACTORY = new JavaTypeFactoryImpl();

  @Mock
  private RelOptSchema schemaMock;
  @Mock
  RelOptHiveTable tableMock;
  @Mock
  Table hiveTableMDMock;

  private HepPlanner planner;
  private RelBuilder rexBuilder;

  private static class MyRecordWithNullableField {
    public Integer f1;
    public int f2;
    public double f3;
  }

  private static class MyRecord {
    public int f1;
    public int f2;
    public double f3;
  }

  public void before(Class<?> clazz) {
    HepProgramBuilder programBuilder = new HepProgramBuilder();
    programBuilder.addRuleInstance(HiveSortPullUpConstantsRule.SORT_LIMIT_INSTANCE);
    programBuilder.addRuleInstance(HiveProjectMergeRule.INSTANCE);

    planner = new HepPlanner(programBuilder.build());

    RexBuilder rexBuilder = new RexBuilder(JAVA_TYPE_FACTORY);
    final RelOptCluster optCluster = RelOptCluster.create(planner, rexBuilder);
    RelDataType rowTypeMock = JAVA_TYPE_FACTORY.createStructType(clazz);
    doReturn(rowTypeMock).when(tableMock).getRowType();
    LogicalTableScan tableScan = LogicalTableScan.create(optCluster, tableMock, Collections.emptyList());
    doReturn(tableScan).when(tableMock).toRel(ArgumentMatchers.any());
    doReturn(tableMock).when(schemaMock).getTableForMember(any());
    lenient().doReturn(hiveTableMDMock).when(tableMock).getHiveTableMD();

    this.rexBuilder = HiveRelFactories.HIVE_BUILDER.create(optCluster, schemaMock);
  }

  public RexNode eq(String field, Number value) {
    return rexBuilder.call(SqlStdOperatorTable.EQUALS,
        rexBuilder.field(field), rexBuilder.literal(value));
  }

  private void test(RelNode plan, String expectedPrePlan, String expectedPostPlan) {
    planner.setRoot(plan);
    RelNode optimizedRelNode = planner.findBestExp();
    assertEquals("Original plans do not match", expectedPrePlan, RelOptUtil.toString(plan));
    assertEquals("Optimized plans do not match", expectedPostPlan, RelOptUtil.toString(optimizedRelNode));
  }

  @org.junit.Test
  public void testNonNullableFields() {
    before(MyRecord.class);

    final RelNode plan = rexBuilder
        .scan("t")
        .filter(eq("f1",1))
        .sort(rexBuilder.field("f1"), rexBuilder.field("f2"))
        .project(rexBuilder.field("f1"), rexBuilder.field("f2"))
        .build();

    String prePlan = "HiveProject(f1=[$0], f2=[$1])\n"
                   + "  HiveSortLimit(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])\n"
                   + "    HiveFilter(condition=[=($0, 1)])\n"
                   + "      LogicalTableScan(table=[[]])\n";

    String postPlan = "HiveProject(f1=[1], f2=[$0])\n"
                    + "  HiveSortLimit(sort0=[$0], dir0=[ASC])\n"
                    + "    HiveProject(f2=[$1], f3=[$2])\n"
                    + "      HiveFilter(condition=[=($0, 1)])\n"
                    + "        LogicalTableScan(table=[[]])\n";

    test(plan, prePlan, postPlan);
  }

  @org.junit.Test
  public void testNullableFields() {
    before(MyRecordWithNullableField.class);

    final RelNode plan = rexBuilder
        .scan("t")
        .filter(eq("f1",1))
        .sort(rexBuilder.field("f1"), rexBuilder.field("f2"))
        .project(rexBuilder.field("f1"), rexBuilder.field("f2"))
        .build();

    String prePlan = "HiveProject(f1=[$0], f2=[$1])\n"
                   + "  HiveSortLimit(sort0=[$0], sort1=[$1], dir0=[ASC], dir1=[ASC])\n"
                   + "    HiveFilter(condition=[=($0, 1)])\n"
                   + "      LogicalTableScan(table=[[]])\n";

    String postPlan = "HiveProject(f1=[CAST(1):JavaType(class java.lang.Integer)], f2=[$0])\n"
                    + "  HiveSortLimit(sort0=[$0], dir0=[ASC])\n"
                    + "    HiveProject(f2=[$1], f3=[$2])\n"
                    + "      HiveFilter(condition=[=($0, 1)])\n"
                    + "        LogicalTableScan(table=[[]])\n";

    test(plan, prePlan, postPlan);
  }
}
