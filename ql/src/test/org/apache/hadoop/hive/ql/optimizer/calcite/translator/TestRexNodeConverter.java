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

package org.apache.hadoop.hive.ql.optimizer.calcite.translator;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class TestRexNodeConverter {

  private static final String CASE_FUNC_TEST = "case";
  private static final RexBuilder REX_BUILDER = new RexBuilder(
      new JavaTypeFactoryImpl(new HiveTypeSystemImpl()));
  private static final RelDataTypeFactory TYPE_FACTORY = REX_BUILDER.getTypeFactory();

  private static RelDataType smallIntegerType;
  private static RelDataType integerType;
  @SuppressWarnings("FieldCanBeLocal")
  private static RelDataType nullableSmallIntegerType;

  private static RexNode varChar34;
  private static RexNode varChar35;
  private static RexNode varCharNull;

  private static RelOptCluster relOptCluster;
  private static RelBuilder relBuilder;
  private static RelDataType tableType;

  @Mock
  private RelOptSchema schemaMock;
  @Mock
  private RelOptHiveTable tableMock;

  private LogicalTableScan tableScan;

  @BeforeClass
  public static void beforeClass() {
    smallIntegerType = TYPE_FACTORY.createSqlType(SqlTypeName.SMALLINT);
    integerType = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    nullableSmallIntegerType = TYPE_FACTORY.createTypeWithNullability(smallIntegerType, true);

    RelDataType varcharType = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR, 20);
    varChar34 = REX_BUILDER.makeLiteral("34", varcharType, true);
    varChar35 = REX_BUILDER.makeLiteral("35", varcharType, true);
    varCharNull = REX_BUILDER.makeLiteral(null, varcharType, true);

    tableType = TYPE_FACTORY.createStructType(
        ImmutableList.of(smallIntegerType, nullableSmallIntegerType),
        ImmutableList.of("f1", "f2")
    );

    RelOptPlanner planner = CalcitePlanner.createPlanner(new HiveConf());
    relOptCluster = RelOptCluster.create(planner, REX_BUILDER);
  }

  @Before
  public void before() {
    doReturn(tableType).when(tableMock).getRowType();
    tableScan = LogicalTableScan.create(relOptCluster, tableMock, Collections.emptyList());
    relBuilder = HiveRelFactories.HIVE_BUILDER.create(relOptCluster, schemaMock);
  }

  @Test public void testRewriteCaseChildren() throws SemanticException {
    RelNode scan = relBuilder.push(tableScan).build();
    RexNode inputRef = REX_BUILDER.makeInputRef(scan, 0);

    List<RexNode> childrenNodeList = ImmutableList.of(
        inputRef,
        REX_BUILDER.makeLiteral(1, integerType, true),
        varChar34,
        REX_BUILDER.makeLiteral(6, integerType, true),
        varChar35);

    List<RexNode> expected = ImmutableList.of(
        REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
            inputRef, REX_BUILDER.makeLiteral(1, smallIntegerType, true)),
        varChar34,
        REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
            inputRef, REX_BUILDER.makeLiteral(6, smallIntegerType, true)),
        varChar35,
        varCharNull);

    List<RexNode> computed = RexNodeConverter.rewriteCaseChildren(
        CASE_FUNC_TEST, childrenNodeList, REX_BUILDER);

    Assert.assertEquals(expected, computed);
  }

  @Test public void testRewriteCaseChildrenNullChild() throws SemanticException {
    RelNode scan = relBuilder.push(tableScan).build();
    RexNode inputRef = REX_BUILDER.makeInputRef(scan, 0);

    List<RexNode> childrenNodeList = ImmutableList.of(
        inputRef,
        REX_BUILDER.makeLiteral(1, integerType, true),
        varChar34,
        REX_BUILDER.makeLiteral(null, integerType, true),
        varChar35);

    List<RexNode> expected = ImmutableList.of(
        REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
            inputRef, REX_BUILDER.makeLiteral(1, smallIntegerType, true)),
        varChar34,
        REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
            inputRef, REX_BUILDER.makeLiteral(null, smallIntegerType, true)),
        varChar35,
        varCharNull);

    List<RexNode> computed = RexNodeConverter.rewriteCaseChildren(
        CASE_FUNC_TEST, childrenNodeList, REX_BUILDER);

    Assert.assertEquals(expected, computed);
  }

  @Test public void testRewriteCaseChildrenNullChildAndNullableType() throws SemanticException {
    RelNode scan = relBuilder.push(tableScan).build();
    RexNode inputRef = REX_BUILDER.makeInputRef(scan, 1);

    List<RexNode> childrenNodeList = ImmutableList.of(
        inputRef,
        REX_BUILDER.makeLiteral(1, integerType, true),
        varChar34,
        REX_BUILDER.makeLiteral(null, integerType, true),
        varChar35);

    List<RexNode> expected = ImmutableList.of(
        REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
            inputRef, REX_BUILDER.makeLiteral(1, smallIntegerType, true)),
        varChar34,
        REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
            inputRef, REX_BUILDER.makeLiteral(null, smallIntegerType, true)),
        varChar35,
        varCharNull);

    List<RexNode> computed = RexNodeConverter.rewriteCaseChildren(
        CASE_FUNC_TEST, childrenNodeList, REX_BUILDER);

    Assert.assertEquals(expected, computed);
  }
}
