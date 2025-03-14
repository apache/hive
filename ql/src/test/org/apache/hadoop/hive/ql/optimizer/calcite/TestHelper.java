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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.AbstractRelOptPlanner;
import org.apache.calcite.plan.RelOptCluster;
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
import org.mockito.ArgumentMatchers;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.lenient;

public class TestHelper {

  protected final static JavaTypeFactoryImpl JAVA_TYPE_FACTORY = new JavaTypeFactoryImpl();

  public static class MyRecordWithNullableField {
    public Integer f1;
    public int f2;
    public double f3;
  }

  public static class MyRecord {
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

  public static RexNode eq(RelBuilder relBuilder, String field, Object value) {
    return relBuilder.call(SqlStdOperatorTable.EQUALS,
        relBuilder.field(field), relBuilder.literal(value));
  }

  public static RexNode neq(RelBuilder relBuilder, String field, Object value) {
    return relBuilder.call(SqlStdOperatorTable.NOT_EQUALS,
        relBuilder.field(field), relBuilder.literal(value));
  }

  public static RexNode lt(RelBuilder relBuilder, String field, Object value) {
    return relBuilder.call(SqlStdOperatorTable.LESS_THAN,
        relBuilder.field(field), relBuilder.literal(value));
  }

  public static RexNode gt(RelBuilder relBuilder, String field, Object value) {
    return relBuilder.call(SqlStdOperatorTable.GREATER_THAN,
        relBuilder.field(field), relBuilder.literal(value));
  }

  public static RexNode lteq(RelBuilder relBuilder, String field, Object value) {
    return relBuilder.call(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
        relBuilder.field(field), relBuilder.literal(value));
  }

  public static RexNode gteq(RelBuilder relBuilder, String field, Object value) {
    return relBuilder.call(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
        relBuilder.field(field), relBuilder.literal(value));
  }

  public static RexNode or(RelBuilder relBuilder, RexNode... args) {
    return relBuilder.call(SqlStdOperatorTable.OR, args);
  }

  public static RexNode and(RelBuilder relBuilder, RexNode... args) {
    return relBuilder.call(SqlStdOperatorTable.AND, args);
  }
  
  public static RexNode in(RelBuilder relBuilder, String field, Object... values) {
    return relBuilder.call(SqlStdOperatorTable.OR,
        Arrays.stream(values).map(v -> eq(relBuilder, field, v)).collect(Collectors.toList()));
  }

  public static RexNode notIn(RelBuilder relBuilder, String field, Object... values) {
    return relBuilder.call(SqlStdOperatorTable.AND,
        Arrays.stream(values).map(v -> neq(relBuilder, field, v)).collect(Collectors.toList()));
  }

  public static <R extends Comparable<R>> RexNode between(RelBuilder relBuilder, String field, R value1, R value2) {
    assert value1.compareTo(value2) < 0 : 
        MessageFormat.format("value1 should be smaller than value2. value1={0}, value2={1}", value1, value2);
    return and(relBuilder,
        gteq(relBuilder, field, value1),
        lteq(relBuilder, field, value2)
    );
  }

  public static <R extends Comparable<R>> RexNode notBetween(
      RelBuilder relBuilder, String field, R value1, R value2) {
    assert value1.compareTo(value2) < 0 :
        MessageFormat.format("value1 should be smaller than value2. value1={0}, value2={1}", value1, value2);
    return or(relBuilder,
        lt(relBuilder, field, value1),
        gt(relBuilder, field, value2)
    );
  }
  
  public static RexNode isNull(RelBuilder relBuilder, String field) {
    return relBuilder.isNull(relBuilder.field(field));
  }

  public static RexNode isNotNull(RelBuilder relBuilder, String field) {
    return relBuilder.isNotNull(relBuilder.field(field));
  }

  public static void assertPlans(AbstractRelOptPlanner planner, RelNode plan, String expectedPrePlan, String expectedPostPlan) {
    planner.setRoot(plan);
    RelNode optimizedRelNode = planner.findBestExp();
    assertEquals("Original plans do not match", expectedPrePlan, RelOptUtil.toString(plan));
    assertEquals("Optimized plans do not match", expectedPostPlan, RelOptUtil.toString(optimizedRelNode));
  }
}
