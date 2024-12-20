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

import org.apache.calcite.plan.AbstractRelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.assertPlans;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.buildPlanner;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.buildRelBuilder;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.MyRecord;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.MyRecordWithNullableField;
import static org.apache.hadoop.hive.ql.optimizer.calcite.rules.TestRuleHelper.eq;

@RunWith(MockitoJUnitRunner.class)
public class TestHiveUnionPullUpConstantsRule {

  @Mock
  private RelOptSchema schemaMock;
  @Mock
  RelOptHiveTable tableMock;
  @Mock
  Table hiveTableMDMock;

  private AbstractRelOptPlanner planner;
  private RelBuilder relBuilder;

  public void before(Class<?> clazz) {
    planner = buildPlanner(Collections.singletonList(HiveUnionPullUpConstantsRule.INSTANCE));
    relBuilder = buildRelBuilder(planner, schemaMock, tableMock, hiveTableMDMock, clazz);
  }

  @Test
  public void testNonNullableFields() {
    before(MyRecord.class);

    final RelNode plan = relBuilder
        .scan("t")
        .filter(eq(relBuilder, "f1",1))
        .project(relBuilder.field("f1"), relBuilder.field("f2"))
        .scan("t")
        .filter(eq(relBuilder, "f1",1))
        .project(relBuilder.field("f1"), relBuilder.field("f2"))
        .union(true)
        .build();

    String prePlan = "HiveUnion(all=[true])\n"
                   + "  HiveProject(f1=[$0], f2=[$1])\n"
                   + "    HiveFilter(condition=[=($0, 1)])\n"
                   + "      LogicalTableScan(table=[[]])\n"
                   + "  HiveProject(f1=[$0], f2=[$1])\n"
                   + "    HiveFilter(condition=[=($0, 1)])\n"
                   + "      LogicalTableScan(table=[[]])\n";

    String postPlan = "HiveProject(f1=[1], f2=[$0])\n"
                    + "  HiveUnion(all=[true])\n"
                    + "    HiveProject(f2=[$1])\n"
                    + "      HiveProject(f1=[$0], f2=[$1])\n"
                    + "        HiveFilter(condition=[=($0, 1)])\n"
                    + "          LogicalTableScan(table=[[]])\n"
                    + "    HiveProject(f2=[$1])\n"
                    + "      HiveProject(f1=[$0], f2=[$1])\n"
                    + "        HiveFilter(condition=[=($0, 1)])\n"
                    + "          LogicalTableScan(table=[[]])\n";

    assertPlans(planner, plan, prePlan, postPlan);
  }

  @Test
  public void testNullableFields() {
    before(MyRecordWithNullableField.class);

    final RelNode plan = relBuilder
        .scan("t")
        .filter(eq(relBuilder, "f1",1))
        .project(relBuilder.field("f1"), relBuilder.field("f2"))
        .scan("t")
        .filter(eq(relBuilder, "f1",1))
        .project(relBuilder.field("f1"), relBuilder.field("f2"))
        .union(false)
        .build();

    String prePlan = "HiveUnion(all=[true])\n"
                   + "  HiveProject(f1=[$0], f2=[$1])\n"
                   + "    HiveFilter(condition=[=($0, 1)])\n"
                   + "      LogicalTableScan(table=[[]])\n"
                   + "  HiveProject(f1=[$0], f2=[$1])\n"
                   + "    HiveFilter(condition=[=($0, 1)])\n"
                   + "      LogicalTableScan(table=[[]])\n";

    String postPlan = "HiveProject(f1=[CAST(1):JavaType(class java.lang.Integer)], f2=[$0])\n"
                    + "  HiveUnion(all=[true])\n"
                    + "    HiveProject(f2=[$1])\n"
                    + "      HiveProject(f1=[$0], f2=[$1])\n"
                    + "        HiveFilter(condition=[=($0, 1)])\n"
                    + "          LogicalTableScan(table=[[]])\n"
                    + "    HiveProject(f2=[$1])\n"
                    + "      HiveProject(f1=[$0], f2=[$1])\n"
                    + "        HiveFilter(condition=[=($0, 1)])\n"
                    + "          LogicalTableScan(table=[[]])\n";

    assertPlans(planner, plan, prePlan, postPlan);
  }
}
