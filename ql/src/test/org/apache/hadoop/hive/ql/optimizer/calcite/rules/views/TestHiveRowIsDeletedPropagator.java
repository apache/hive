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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@RunWith(MockitoJUnitRunner.class)
public class TestHiveRowIsDeletedPropagator extends TestRuleBase {
  @Test
  public void testJoining3TablesAndAllChanged() {
    RelNode ts1 = createTS(t1NativeMock, "t1");
    RelNode ts2 = createTS(t2NativeMock, "t2");
    RelNode ts3 = createTS(t3NativeMock, "t3");

    RelBuilder relBuilder = HiveRelFactories.HIVE_BUILDER.create(relOptCluster, null);

    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0),
        REX_BUILDER.makeInputRef(ts2.getRowType().getFieldList().get(0).getType(), 5));
    RelNode join1 = relBuilder
        .push(ts1)
        .filter(REX_BUILDER.makeCall(SqlStdOperatorTable.IS_NOT_NULL, REX_BUILDER.makeInputRef(ts1, 0)))
        .push(ts2)
        .filter(REX_BUILDER.makeCall(SqlStdOperatorTable.IS_NOT_NULL, REX_BUILDER.makeInputRef(ts2, 0)))
        .join(JoinRelType.INNER, joinCondition)
        .build();

    RexNode joinCondition2 = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(ts3.getRowType().getFieldList().get(0).getType(), 10),
        REX_BUILDER.makeInputRef(join1.getRowType().getFieldList().get(5).getType(), 5));

    RelDataType bigIntType = relBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT);

    RexNode writeIdFilter = REX_BUILDER.makeCall(SqlStdOperatorTable.OR,
        REX_BUILDER.makeCall(SqlStdOperatorTable.LESS_THAN, REX_BUILDER.makeLiteral(1, bigIntType, false), rowIdFieldAccess(ts1, 3)),
        REX_BUILDER.makeCall(SqlStdOperatorTable.LESS_THAN, REX_BUILDER.makeLiteral(1, bigIntType, false), rowIdFieldAccess(ts2, 8)),
        REX_BUILDER.makeCall(SqlStdOperatorTable.LESS_THAN, REX_BUILDER.makeLiteral(1, bigIntType, false), rowIdFieldAccess(ts3, 13)));

    RelNode root = relBuilder
        .push(join1)
        .push(ts3)
        .filter(REX_BUILDER.makeCall(SqlStdOperatorTable.IS_NOT_NULL, REX_BUILDER.makeInputRef(ts3, 0)))
        .join(JoinRelType.INNER, joinCondition2)
        .filter(writeIdFilter)
        .build();

    HiveRowIsDeletedPropagator propagator = new HiveRowIsDeletedPropagator(relBuilder);
    RelNode newRoot = propagator.propagate(root);

    String dump = RelOptUtil.toString(newRoot);
    assertThat(dump, is(EXPECTED_testJoining3TablesAndAllChanged));
  }

  private static final String EXPECTED_testJoining3TablesAndAllChanged =
      "HiveFilter(condition=[OR(<(1, $3.writeId), <(1, $8.writeId), <(1, $13.writeId))])\n" +
      "  HiveFilter(condition=[OR(NOT($15), NOT($16))])\n" +
      "    HiveProject(a=[$0], b=[$1], c=[$2], ROW__ID=[$3], ROW__IS__DELETED=[$4], d=[$5], e=[$6], f=[$7], ROW__ID0=[$8], ROW__IS__DELETED0=[$9], g=[$12], h=[$13], i=[$14], ROW__ID1=[$15], ROW__IS__DELETED1=[$16], _any_deleted=[OR($10, $17)], _any_inserted=[OR($11, $18)])\n" +
      "      HiveJoin(condition=[=($12, $5)], joinType=[inner], algorithm=[none], cost=[not available])\n" +
      "        HiveFilter(condition=[OR(NOT($10), NOT($11))])\n" +
      "          HiveProject(a=[$0], b=[$1], c=[$2], ROW__ID=[$3], ROW__IS__DELETED=[$4], d=[$7], e=[$8], f=[$9], ROW__ID0=[$10], ROW__IS__DELETED0=[$11], _any_deleted=[OR($5, $12)], _any_inserted=[OR($6, $13)])\n" +
      "            HiveJoin(condition=[=($0, $7)], joinType=[inner], algorithm=[none], cost=[not available])\n" +
      "              HiveFilter(condition=[IS NOT NULL($0)])\n" +
      "                HiveProject(a=[$0], b=[$1], c=[$2], ROW__ID=[$3], ROW__IS__DELETED=[$4], _deleted=[AND($4, <(1, $3.writeId))], _inserted=[AND(<(1, $3.writeId), NOT($4))])\n" +
      "                  HiveTableScan(table=[[default, t1]], table:alias=[t1])\n" +
      "              HiveFilter(condition=[IS NOT NULL($0)])\n" +
      "                HiveProject(d=[$0], e=[$1], f=[$2], ROW__ID=[$3], ROW__IS__DELETED=[$4], _deleted=[AND($4, <(1, $3.writeId))], _inserted=[AND(<(1, $3.writeId), NOT($4))])\n" +
      "                  HiveTableScan(table=[[default, t2]], table:alias=[t2])\n" +
      "        HiveFilter(condition=[IS NOT NULL($0)])\n" +
      "          HiveProject(g=[$0], h=[$1], i=[$2], ROW__ID=[$3], ROW__IS__DELETED=[$4], _deleted=[AND($4, <(1, $3.writeId))], _inserted=[AND(<(1, $3.writeId), NOT($4))])\n" +
      "            HiveTableScan(table=[[]], table:alias=[t3])\n";

  private RexNode rowIdFieldAccess(RelNode tableScan, int posInTarget) {
    int rowIDPos = tableScan.getTable().getRowType().getField(
        VirtualColumn.ROWID.getName(), false, false).getIndex();
    return REX_BUILDER.makeFieldAccess(REX_BUILDER.makeInputRef(
        tableScan.getTable().getRowType().getFieldList().get(rowIDPos).getType(), posInTarget), 0);
  }
}