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
import org.apache.calcite.plan.AbstractRelOptPlanner;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collection;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;

public class TestRuleHelper {

  protected final static JavaTypeFactoryImpl JAVA_TYPE_FACTORY = new JavaTypeFactoryImpl();

  static class MyRecordWithNullableField {
    public Integer f1;
    public int f2;
    public double f3;
  }

  static class MyRecord {
    public int f1;
    public int f2;
    public double f3;
  }

  public static AbstractRelOptPlanner buildPlanner(Collection<RelOptRule> rules) {
    HepProgramBuilder programBuilder = new HepProgramBuilder();
    rules.forEach(programBuilder::addRuleInstance);
    return new HepPlanner(programBuilder.build());
  }

  public static RelBuilder buildRelBuilder(AbstractRelOptPlanner planner,
      RelOptSchema schemaMock, RelOptHiveTable tableMock, Table hiveTableMock, Class<?> clazz) {

    RexBuilder rexBuilder = new RexBuilder(JAVA_TYPE_FACTORY);
    final RelOptCluster optCluster = RelOptCluster.create(planner, rexBuilder);
    RelDataType rowTypeMock = JAVA_TYPE_FACTORY.createStructType(clazz);
    doReturn(rowTypeMock).when(tableMock).getRowType();
    LogicalTableScan tableScan = LogicalTableScan.create(optCluster, tableMock, Collections.emptyList());
    doReturn(tableScan).when(tableMock).toRel(ArgumentMatchers.any());
    doReturn(tableMock).when(schemaMock).getTableForMember(any());
    lenient().doReturn(hiveTableMock).when(tableMock).getHiveTableMD();

    return HiveRelFactories.HIVE_BUILDER.create(optCluster, schemaMock);
  }

  static RexNode eq(RelBuilder relBuilder, String field, Number value) {
    return relBuilder.call(SqlStdOperatorTable.EQUALS,
        relBuilder.field(field), relBuilder.literal(value));
  }

  static RexNode or(RelBuilder relBuilder, RexNode... args) {
    return relBuilder.call(SqlStdOperatorTable.OR, args);
  }

  static RexNode and(RelBuilder relBuilder, RexNode... args) {
    return relBuilder.call(SqlStdOperatorTable.AND, args);
  }

  static void assertPlans(AbstractRelOptPlanner planner, RelNode plan, String expectedPrePlan, String expectedPostPlan) {
    planner.setRoot(plan);
    RelNode optimizedRelNode = planner.findBestExp();
    assertEquals("Original plans do not match", expectedPrePlan, RelOptUtil.toString(plan));
    assertEquals("Optimized plans do not match", expectedPostPlan, RelOptUtil.toString(optimizedRelNode));
  }
}
