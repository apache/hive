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

package org.apache.hadoop.hive.ql.optimizer.signature;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HivePointLookupOptimizerRule;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFConcat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

@RunWith(MockitoJUnitRunner.class)
public class TestRelSignature {

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
    programBuilder.addRuleInstance(new HivePointLookupOptimizerRule.FilterCondition(2));

    planner = new HepPlanner(programBuilder.build());

    JavaTypeFactoryImpl typeFactory = new JavaTypeFactoryImpl();
    RexBuilder rexBuilder = new RexBuilder(typeFactory);
    final RelOptCluster optCluster = RelOptCluster.create(planner, rexBuilder);
    RelDataType rowTypeMock = typeFactory.createStructType(MyRecord.class);
    LogicalTableScan tableScan = LogicalTableScan.create(optCluster, tableMock, Collections.emptyList());
    doReturn(tableScan).when(tableMock).toRel(any());
    doReturn(rowTypeMock).when(tableMock).getRowType();
    doReturn(tableMock).when(schemaMock).getTableForMember(any());
    lenient().doReturn(hiveTableMDMock).when(tableMock).getHiveTableMD();

    builder = HiveRelFactories.HIVE_BUILDER.create(optCluster, schemaMock);
  }

  @Deprecated

  GenericUDF udf = new GenericUDFConcat();
  @Deprecated

  CompilationOpContext cCtx = new CompilationOpContext();

  public RexNode eq(String field, int value) {
    return builder.call(SqlStdOperatorTable.EQUALS,
        builder.field(field), builder.literal(value));
  }

  @Test
  public void testFilterOpEquals() {

    RelNode r7 = builder.scan("t").filter(eq("f1", 7)).build();
    RelNode r8 = builder.scan("t").filter(eq("f1", 8)).build();
    RelNode r7b = builder.scan("t").filter(eq("f1", 7)).build();

    checkEquals(r7, r7b);
    checkNotEquals(r7, r8);
  }

  public static void checkEquals(RelNode r7, RelNode r7b) {

    RelTreeSignature s1 = RelTreeSignature.of(r7);
    RelTreeSignature s2 = RelTreeSignature.of(r7b);

    assertEquals(s1.hashCode(), s2.hashCode());
    assertEquals(s1, s2);
  }

  public static void checkNotEquals(RelNode r7, RelNode r8) {
    RelTreeSignature s1 = RelTreeSignature.of(r7);
    RelTreeSignature s2 = RelTreeSignature.of(r8);

    assertNotEquals(s1.hashCode(), s2.hashCode());
    assertNotEquals(s1, s2);
  }

  private Operator<? extends OperatorDesc> getFilterOp(int constVal) {

    ExprNodeDesc pred = new ExprNodeConstantDesc(constVal);
    FilterDesc fd = new FilterDesc(pred, true);
    Operator<? extends OperatorDesc> op = OperatorFactory.get(cCtx, fd);
    return op;
  }
}
