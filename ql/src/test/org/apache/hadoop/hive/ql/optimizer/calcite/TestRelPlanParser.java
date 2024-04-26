/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.optimizer.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.TestRuleBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestRelPlanParser extends TestRuleBase {

  @Mock
  private RelOptHiveTableFactory relOptHiveTableFactory;
  private RelNode ts1;
  private RelNode ts2;

  @Override
  public void setup() {
    super.setup();
    when(relOptHiveTableFactory.createRelOptHiveTable(eq("t1"), any(), any(), any(), any(), any()))
        .thenReturn(t1NativeMock);
    when(relOptHiveTableFactory.createRelOptHiveTable(eq("t2"), any(), any(), any(), any(), any()))
        .thenReturn(t2NativeMock);
    ts1 = createTS(t1NativeMock, "t1");
    ts2 = createTS(t2NativeMock, "t2");
  }

  @Test
  public void testSimpleJoin() throws IOException {
    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0),
        REX_BUILDER.makeInputRef(ts2.getRowType().getFieldList().get(0).getType(), 5));

    /*
     * HiveProject(a=[$0], b=[$1], d=[$5])
     *   HiveJoin(condition=[=($0, $5)], joinType=[inner], algorithm=[none], cost=[not available])
     *     HiveFilter(condition=[IS NOT NULL($0)])
     *       HiveTableScan(table=[[default, t1]], table:alias=[t1])
     *     HiveFilter(condition=[IS NOT NULL($0)])
     *       HiveTableScan(table=[[default, t2]], table:alias=[t2])
     */
    RelNode planToSerialize = REL_BUILDER
        .push(ts1)
        .filter(REX_BUILDER.makeCall(SqlStdOperatorTable.IS_NOT_NULL, REX_BUILDER.makeInputRef(ts1, 0)))
        .push(ts2)
        .filter(REX_BUILDER.makeCall(SqlStdOperatorTable.IS_NOT_NULL, REX_BUILDER.makeInputRef(ts2, 0)))
        .join(JoinRelType.INNER, joinCondition)
        .project(
            REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0),
            REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(1).getType(), 1),
            REX_BUILDER.makeInputRef(ts2.getRowType().getFieldList().get(0).getType(), ts1.getRowType().getFieldCount()))
        .build();

    serializeDeserializeAndAssertEquals(planToSerialize);

    verify(relOptHiveTableFactory, atLeastOnce())
        .createRelOptHiveTable(eq("t1"), any(), any(), any(), any(), any());
    verify(relOptHiveTableFactory, atLeastOnce())
        .createRelOptHiveTable(eq("t2"), any(), any(), any(), any(), any());
  }

  @Test
  public void testAggregate() throws IOException {
    /*
     * HiveAggregate(group=[{0, 1}], cs=[COUNT()], agg#1=[MIN($2)], agg#2=[SUM($2)])
     *   HiveTableScan(table=[[default, t1]], table:alias=[t1])
     */
    RelNode planToSerialize = REL_BUILDER
        .push(ts1)
        .aggregate(
            REL_BUILDER.groupKey(0, 1),
            REL_BUILDER.countStar("cs"),
            REL_BUILDER.min(REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(2).getType(), 2)),
            REL_BUILDER.sum(REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(2).getType(), 2)))
        .build();

    serializeDeserializeAndAssertEquals(planToSerialize);
  }

  @Test
  public void testAntiJoin() throws IOException {
    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0),
        REX_BUILDER.makeInputRef(ts2.getRowType().getFieldList().get(0).getType(), 5));

    /*
     * HiveAntiJoin(condition=[=($0, $5)], joinType=[anti])
     *   HiveTableScan(table=[[default, t1]], table:alias=[t1])
     *   HiveTableScan(table=[[default, t2]], table:alias=[t2])
     */
    RelNode planToSerialize = REL_BUILDER
        .push(ts1)
        .push(ts2)
        .join(JoinRelType.ANTI, joinCondition)
        .build();

    serializeDeserializeAndAssertEquals(planToSerialize);
  }

  @Test
  public void testSortExchange() throws IOException {
    /*
     * HiveSortExchange(distribution=[any], collation=[[0]])
     *   HiveFilter(condition=[IS NOT NULL($0)])
     *     HiveTableScan(table=[[default, t1]], table:alias=[t1])
     */
    RelNode planToSerialize = REL_BUILDER
        .push(ts1)
        .filter(REX_BUILDER.makeCall(SqlStdOperatorTable.IS_NOT_NULL, REX_BUILDER.makeInputRef(ts1, 0)))
        .sortExchange(HiveRelDistribution.ANY, RelCollations.of(0))
        .build();

    serializeDeserializeAndAssertEquals(planToSerialize);
  }

  @Test
  public void testSortLimit() throws IOException {
    /*
     * HiveSortLimit(sort0=[$0], dir0=[ASC], fetch=[10])
     *   HiveProject(a=[$0], b=[$1])
     *     HiveTableScan(table=[[default, t1]], table:alias=[t1])
     */
    RelNode planToSerialize = REL_BUILDER
        .push(ts1)
        .project(
            REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0),
            REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(1).getType(), 1))
        .sortLimit(-1, 10,
            REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0))
        .build();

    serializeDeserializeAndAssertEquals(planToSerialize);
  }

  @Test
  public void testUnion() throws IOException {
    /*
     * HiveUnion(all=[true])
     *   HiveTableScan(table=[[default, t1]], table:alias=[t1])
     *   HiveTableScan(table=[[default, t2]], table:alias=[t2])
     */
    RelNode planToSerialize = REL_BUILDER
        .push(ts1)
        .push(ts2)
        .union(true)
        .build();

    serializeDeserializeAndAssertEquals(planToSerialize);
  }

  @Test
  public void testValues() throws IOException {
    List<RexLiteral> values = ImmutableList.of(
        REX_BUILDER.makeExactLiteral(BigDecimal.ONE),
        REX_BUILDER.makeExactLiteral(BigDecimal.TEN)
    );

    List<RelDataType> schema = ImmutableList.of(
        TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER),
        TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER)
    );

    /*
     * HiveValues(tuples=[[{ 1, 10 }]])
     */
    RelNode planToSerialize = REL_BUILDER
        .values(ImmutableList.of(values), TYPE_FACTORY.createStructType(schema, ImmutableList.of("a", "b")))
        .build();

    serializeDeserializeAndAssertEquals(planToSerialize);
  }

  private void serializeDeserializeAndAssertEquals(RelNode plan) throws IOException {
    String planJson = HiveRelOptUtil.asJSONObjectString(plan, false);
    RelPlanParser parser = new RelPlanParser(relOptCluster, relOptHiveTableFactory, new HashMap<>());
    RelNode parsedPlan = parser.parse(planJson);

    assertEquals(RelOptUtil.toString(plan), RelOptUtil.toString(parsedPlan));
  }
}