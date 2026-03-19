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

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Holder;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRexExecutorImpl;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.buildPlanner;
import static org.junit.Assert.*;

public class TestHiveRelDecorrelator {

  TestRuleHelper.PlanFixture fixture() {
    RelOptPlanner planner = buildPlanner(Collections.singletonList(HiveAntiSemiJoinRule.INSTANCE));
    planner.setExecutor(new HiveRexExecutorImpl());
    return new TestRuleHelper.PlanFixture(planner)
            .registerTable("t1", T1Record.class)
            .registerTable("t2", T2Record.class);
  }


  @Test
  public void testDecorrelateCorrelateIsRemovedWhenPlanHasEmptyValues() {
    RelBuilder relBuilder = fixture().createRelBuilder();
    Holder<RexCorrelVariable> v = Holder.empty();
    RelNode base = relBuilder
            .scan("t1")
            .empty()
            .scan("t1")
            .variable(v)
            .scan("t2")
            .filter(relBuilder.call(SqlStdOperatorTable.EQUALS, relBuilder.getRexBuilder().makeFieldAccess(v.get(), 0), relBuilder.literal(10)))
            .correlate(JoinRelType.SEMI, v.get().id, relBuilder.field(2, 0, "t1id"))
            .union(false)
            .build();

    Assert.assertTrue("Plan before decorrelation does not contain correlate: \n" + RelOptUtil.toString(base),
            RelOptUtil.toString(base).contains("Correlate"));
    Assert.assertTrue("Plan before decorrelation does not contain Values: \n" + RelOptUtil.toString(base),
            RelOptUtil.toString(base).contains("Values"));

    RelNode decorrelatedPlan = HiveRelDecorrelator.decorrelateQuery(base);

    Assert.assertFalse("Plan after decorrelation still has correlate: \n" + RelOptUtil.toString(decorrelatedPlan),
            RelOptUtil.toString(decorrelatedPlan).contains("Correlate"));
    Assert.assertTrue("Plan after decorrelation does not contain Values: \n" + RelOptUtil.toString(decorrelatedPlan),
            RelOptUtil.toString(decorrelatedPlan).contains("Values"));
  }

  static class T1Record {
    public int t1id;
    public int t1AnyCol;
  }

  static class T2Record {
    public int t2id;
    public int t2AnyCol;
  }
}