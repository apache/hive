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

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.views.TestRuleBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.HashMap;

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

  @Override
  public void setup() {
    super.setup();
    when(relOptHiveTableFactory.createRelOptHiveTable(eq("t1"), any(), any(), any(), any(), any()))
        .thenReturn(t1NativeMock);
    when(relOptHiveTableFactory.createRelOptHiveTable(eq("t2"), any(), any(), any(), any(), any()))
        .thenReturn(t2NativeMock);
  }

  @Test
  public void testSimpleJoin() throws IOException {
    RelNode ts1 = createTS(t1NativeMock, "t1");
    RelNode ts2 = createTS(t2NativeMock, "t2");

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

    String planJson = HiveRelOptUtil.asJSONObjectString(planToSerialize, false);

    RelPlanParser parser = new RelPlanParser(relOptCluster, relOptHiveTableFactory, new HashMap<>());
    RelNode parsedPlan = parser.parse(planJson);

    verify(relOptHiveTableFactory, atLeastOnce())
        .createRelOptHiveTable(eq("t1"), any(), any(), any(), any(), any());
    verify(relOptHiveTableFactory, atLeastOnce())
        .createRelOptHiveTable(eq("t2"), any(), any(), any(), any(), any());
    assertEquals(RelOptUtil.toString(planToSerialize), RelOptUtil.toString(parsedPlan));
  }
}