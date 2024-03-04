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

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class TestMaterializedViewIncrementalRewritingRelVisitor extends TestRuleBase {

  @Test
  public void testIncrementalRebuildIsNotAvailableWhenPlanHasUnsupportedOperator() {
    RelNode ts1 = createTS(t1NativeMock, "t1");

    RelNode mvQueryPlan = REL_BUILDER
        .push(ts1)
        .sort(1) // Order by is not supported
        .build();

    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    assertThat(visitor.go(mvQueryPlan).getIncrementalRebuildMode(), is(IncrementalRebuildMode.NOT_AVAILABLE));
  }

  @Test
  public void testIncrementalRebuildIsInsertOnlyWhenPlanHasTSOnNonNativeTable() {
    RelNode ts1 = createT2IcebergTS();

    RelNode mvQueryPlan = REL_BUILDER
        .push(ts1)
        .build();

    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    assertThat(visitor.go(mvQueryPlan).getIncrementalRebuildMode(), is(IncrementalRebuildMode.NOT_AVAILABLE));
  }

  @Test
  public void testIncrementalRebuildIsInsertOnlyWhenPlanHasTSOnNonNativeTableSupportsSnapshots() {
    doReturn(true).when(table2storageHandler).areSnapshotsSupported();
    RelNode ts1 = createT2IcebergTS();

    RelNode mvQueryPlan = REL_BUILDER
        .push(ts1)
        .build();

    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    assertThat(visitor.go(mvQueryPlan).getIncrementalRebuildMode(), is(IncrementalRebuildMode.INSERT_ONLY));
  }

  @Test
  public void testIncrementalRebuildIsInsertOnlyWhenPlanHasFilter() {
    RelNode ts1 = createTS(t1NativeMock, "t1");

    RelNode mvQueryPlan = REL_BUILDER
        .push(ts1)
        .filter(REX_BUILDER.makeCall(SqlStdOperatorTable.IS_NOT_NULL, REX_BUILDER.makeInputRef(ts1, 0)))
        .build();

    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    MaterializedViewIncrementalRewritingRelVisitor.Result result = visitor.go(mvQueryPlan);

    assertThat(result.getIncrementalRebuildMode(), is(IncrementalRebuildMode.INSERT_ONLY));
    assertThat(result.containsAggregate(), is(false));
  }

  @Test
  public void testIncrementalRebuildIsInsertOnlyWhenPlanHasInnerJoin() {
    RelNode ts1 = createTS(t1NativeMock, "t1");
    RelNode ts2 = createTS(t2NativeMock, "t2");

    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0),
        REX_BUILDER.makeInputRef(ts2.getRowType().getFieldList().get(0).getType(), 5));

    RelNode mvQueryPlan = REL_BUILDER
        .push(ts1)
        .push(ts2)
        .join(JoinRelType.INNER, joinCondition)
        .build();

    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    assertThat(visitor.go(mvQueryPlan).getIncrementalRebuildMode(), is(IncrementalRebuildMode.INSERT_ONLY));
  }

  @Test
  public void testIncrementalRebuildIsNotAvailableWhenPlanHasJoinOtherThanInner() {
    RelNode ts1 = createTS(t1NativeMock, "t1");
    RelNode ts2 = createTS(t2NativeMock, "t2");

    RexNode joinCondition = REX_BUILDER.makeCall(SqlStdOperatorTable.EQUALS,
        REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0),
        REX_BUILDER.makeInputRef(ts2.getRowType().getFieldList().get(0).getType(), 5));

    RelNode mvQueryPlan = REL_BUILDER
        .push(ts1)
        .push(ts2)
        .join(JoinRelType.LEFT, joinCondition)
        .build();

    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    assertThat(visitor.go(mvQueryPlan).getIncrementalRebuildMode(), is(IncrementalRebuildMode.NOT_AVAILABLE));
  }

  @Test
  public void testIncrementalRebuildIsInsertOnlyWhenPlanHasAggregate() {
    RelNode ts1 = createTS(t1NativeMock, "t1");

    RelNode mvQueryPlan = REL_BUILDER
        .push(ts1)
        .aggregate(
            REL_BUILDER.groupKey(0),
            REL_BUILDER.aggregateCall(
                SqlStdOperatorTable.SUM,
                REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0)))
        .build();

    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    MaterializedViewIncrementalRewritingRelVisitor.Result result = visitor.go(mvQueryPlan);

    assertThat(result.getIncrementalRebuildMode(), is(IncrementalRebuildMode.INSERT_ONLY));
    assertThat(result.containsAggregate(), is(true));
    assertThat(result.getCountStarIndex(), is(-1));
  }

  @Test
  public void testIncrementalRebuildIsAvailableWhenPlanHasAggregateAndCountStar() {
    RelNode ts1 = createTS(t1NativeMock, "t1");

    RelNode mvQueryPlan = REL_BUILDER
        .push(ts1)
        .aggregate(
            REL_BUILDER.groupKey(0),
            REL_BUILDER.aggregateCall(
                SqlStdOperatorTable.SUM,
                REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0)),
            REL_BUILDER.aggregateCall(SqlStdOperatorTable.COUNT))
        .build();

    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    MaterializedViewIncrementalRewritingRelVisitor.Result result = visitor.go(mvQueryPlan);

    assertThat(result.getIncrementalRebuildMode(), is(IncrementalRebuildMode.AVAILABLE));
    assertThat(result.containsAggregate(), is(true));
    assertThat(result.getCountStarIndex(), is(1));
  }

  @Test
  public void testIncrementalRebuildIsNotAvailableWhenPlanHasAggregateAvg() {
    RelNode ts1 = createTS(t1NativeMock, "t1");

    RelNode mvQueryPlan = REL_BUILDER
        .push(ts1)
        .aggregate(
            REL_BUILDER.groupKey(0),
            REL_BUILDER.aggregateCall(
                SqlStdOperatorTable.AVG,
                REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0)))
        .build();

    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    assertThat(visitor.go(mvQueryPlan).getIncrementalRebuildMode(), is(IncrementalRebuildMode.NOT_AVAILABLE));
  }

  @Test
  public void testIncrementalRebuildIsInsertOnlyWhenPlanHasAggregateAvgCountSumOnTheSameColumn() {
    RelNode ts1 = createTS(t1NativeMock, "t1");

    RexInputRef rexInputRef = REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0);
    RelNode mvQueryPlan = REL_BUILDER
        .push(ts1)
        .aggregate(
            REL_BUILDER.groupKey(0),
            REL_BUILDER.aggregateCall(SqlStdOperatorTable.COUNT, rexInputRef),
            REL_BUILDER.aggregateCall(SqlStdOperatorTable.SUM, rexInputRef),
            REL_BUILDER.aggregateCall(SqlStdOperatorTable.AVG, rexInputRef))
        .build();

    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    assertThat(visitor.go(mvQueryPlan).getIncrementalRebuildMode(), is(IncrementalRebuildMode.INSERT_ONLY));
  }

  @Test
  public void testIncrementalRebuildIsNotAvailableWhenPlanHasAggregateAvgCountSumButOnDifferentColumns() {
    RelNode ts1 = createTS(t1NativeMock, "t1");

    RexInputRef rexInputRef = REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0);
    RexInputRef rexInputRef2 = REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 1);
    RelNode mvQueryPlan = REL_BUILDER
        .push(ts1)
        .aggregate(
            REL_BUILDER.groupKey(0),
            REL_BUILDER.aggregateCall(SqlStdOperatorTable.COUNT, rexInputRef),
            REL_BUILDER.aggregateCall(SqlStdOperatorTable.SUM, rexInputRef2),
            REL_BUILDER.aggregateCall(SqlStdOperatorTable.AVG, rexInputRef))
        .build();

    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    assertThat(visitor.go(mvQueryPlan).getIncrementalRebuildMode(), is(IncrementalRebuildMode.NOT_AVAILABLE));
  }

  @Test
  public void testIncrementalRebuildNotAvailableWhenPlanHasNotSupportedAggregate() {
    RelNode ts1 = createTS(t1NativeMock, "t1");

    RexInputRef rexInputRef = REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0);
    RelNode mvQueryPlan = REL_BUILDER
        .push(ts1)
        .aggregate(
            REL_BUILDER.groupKey(0),
            REL_BUILDER.aggregateCall(SqlStdOperatorTable.STDDEV, rexInputRef),
            REL_BUILDER.aggregateCall(SqlStdOperatorTable.SUM, rexInputRef),
            REL_BUILDER.aggregateCall(SqlStdOperatorTable.COUNT, rexInputRef))
        .build();

    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    assertThat(visitor.go(mvQueryPlan).getIncrementalRebuildMode(), is(IncrementalRebuildMode.NOT_AVAILABLE));
  }

  @Test
  public void testIncrementalRebuildNotAvailableWhenPlanHasBothSupportedAndNotSupportedAggregate() {
    RelNode ts1 = createTS(t1NativeMock, "t1");

    RexInputRef rexInputRef = REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0);
    RelNode mvQueryPlan = REL_BUILDER
        .push(ts1)
        .aggregate(
            REL_BUILDER.groupKey(0),
            REL_BUILDER.aggregateCall(SqlStdOperatorTable.STDDEV, rexInputRef),
            REL_BUILDER.aggregateCall(SqlStdOperatorTable.MIN, rexInputRef))
        .build();

    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    assertThat(visitor.go(mvQueryPlan).getIncrementalRebuildMode(), is(IncrementalRebuildMode.NOT_AVAILABLE));
  }

  @Test
  public void testIncrementalRebuildIsInsertOnlyWhenPlanHasBothSupportedAndInsertOnlySupportAggregate() {
    RelNode ts1 = createTS(t1NativeMock, "t1");

    RexInputRef rexInputRef = REX_BUILDER.makeInputRef(ts1.getRowType().getFieldList().get(0).getType(), 0);
    RelNode mvQueryPlan = REL_BUILDER
        .push(ts1)
        .aggregate(
            REL_BUILDER.groupKey(0),
            REL_BUILDER.aggregateCall(SqlStdOperatorTable.MIN, rexInputRef),
            REL_BUILDER.aggregateCall(SqlStdOperatorTable.COUNT))
        .build();

    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    assertThat(visitor.go(mvQueryPlan).getIncrementalRebuildMode(), is(IncrementalRebuildMode.INSERT_ONLY));
  }

  @Test
  public void testIncrementalRebuildIsNotAvailableWhenPlanHasCountDistinct() {
    RelNode ts1 = createTS(t1NativeMock, "t1");

    RelDataType countRetType =
        TYPE_FACTORY.createTypeWithNullability(TYPE_FACTORY.createSqlType(SqlTypeName.BIGINT), false);

    AggregateCall aggregateCall = AggregateCall.create(SqlStdOperatorTable.COUNT, true, false, false,
        Collections.emptyList(), -1, RelCollations.EMPTY, countRetType, null);
//    AggregateCall aggregateCall = AggregateCall.create(SqlStdOperatorTable.COUNT, true, false, false,
//        Collections.singletonList(0), -1, RelCollations.EMPTY, countRetType, null);
    RelNode mvQueryPlan = REL_BUILDER
        .push(ts1)
        .aggregate(REL_BUILDER.groupKey(0), Collections.singletonList(aggregateCall))
        .build();

    MaterializedViewIncrementalRewritingRelVisitor visitor = new MaterializedViewIncrementalRewritingRelVisitor();
    assertThat(visitor.go(mvQueryPlan).getIncrementalRebuildMode(), is(IncrementalRebuildMode.NOT_AVAILABLE));
  }
}
