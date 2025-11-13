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
import org.apache.calcite.plan.*;
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
import org.mockito.ArgumentMatchers;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

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

  public static class HiveTableMock {
    final Class<?> recordClass;
    final RelOptHiveTable tableMock;
    final Table hiveTableMock;
    final List<String> name;

    public HiveTableMock(List<String> name, Class<?> recordClass, PlanFixture mockBuilder) {
      this.name = List.copyOf(name);
      this.recordClass = recordClass;
      tableMock = mock(RelOptHiveTable.class);
      hiveTableMock = mock(Table.class);

      RelDataType rowTypeMock = JAVA_TYPE_FACTORY.createStructType(recordClass);
      doReturn(rowTypeMock).when(tableMock).getRowType();

      LogicalTableScan tableScan = LogicalTableScan.create(mockBuilder.optCluster, tableMock, Collections.emptyList());
      doReturn(tableScan).when(tableMock).toRel(ArgumentMatchers.any());

      doReturn(this.name).when(tableMock).getQualifiedName();

      lenient().doReturn(hiveTableMock).when(tableMock).getHiveTableMD();
    }
  }

  /**
   * A fixture for creating plans with <code>HiveRelNode</code>s.
   */
  public static class PlanFixture {
    final RelOptCluster optCluster;

    final Map<List<String>, HiveTableMock> tables = new HashMap<>();

    Class<?> defaultRecordClass;

    public PlanFixture(RelOptPlanner planner) {
      RexBuilder rexBuilder = new RexBuilder(JAVA_TYPE_FACTORY);
      optCluster = RelOptCluster.create(planner, rexBuilder);
    }

    /**
     * Register a table in the schema, using the attributes of the class as columns.
     */
    public PlanFixture registerTable(String name, Class<?> recordClass) {
      return registerTable(List.of(name), recordClass);
    }

    /**
     * Similar to {@link #registerTable(String, Class)}, but with a qualified name.
     * <p>
     * See {@link RelOptTable#getQualifiedName()}.
     */
    public PlanFixture registerTable(List<String> name, Class<?> recordClass) {
      name = List.copyOf(name);
      tables.put(name, new HiveTableMock(name, recordClass, this));
      return this;
    }

    /**
     * Allows to use any table names when scanning.
     * <p>
     * The scanned table will provide the attributes of the class as columns.
     */
    public PlanFixture setDefaultRecordClass(Class<?> recordClass) {
      this.defaultRecordClass = recordClass;
      return this;
    }

    public RelOptPlanner getPlanner() {
      return optCluster.getPlanner();
    }

    public RelBuilder createRelBuilder() {
      final RelOptSchema schemaMock;
      schemaMock = mock(RelOptSchema.class);
      // create a copy that we can modify in our method
      Map<List<String>, HiveTableMock> tableMap = new HashMap<>(tables);

      when(schemaMock.getTableForMember(any())).thenAnswer(i -> {
        List<String> tableName = i.getArgument(0);
        HiveTableMock hiveTableMock = tableMap.get(tableName);
        if(hiveTableMock == null) {
          Objects.requireNonNull(defaultRecordClass, "Table " + tableName + " was not registered with the mock, and no default table provided");
          hiveTableMock = new HiveTableMock(tableName, defaultRecordClass, this);
          tableMap.put(tableName, hiveTableMock);
        }
        return hiveTableMock.tableMock;
      } );

      return HiveRelFactories.HIVE_BUILDER.create(optCluster, schemaMock);
    }
  }

  public static RelBuilder buildRelBuilder(RelOptPlanner planner,
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

  static void assertPlans(RelOptPlanner planner, RelNode plan, String expectedPrePlan, String expectedPostPlan) {
    planner.setRoot(plan);
    RelNode optimizedRelNode = planner.findBestExp();
    assertEquals("Original plans do not match", expectedPrePlan, RelOptUtil.toString(plan));
    assertEquals("Optimized plans do not match", expectedPostPlan, RelOptUtil.toString(optimizedRelNode));
  }
}
