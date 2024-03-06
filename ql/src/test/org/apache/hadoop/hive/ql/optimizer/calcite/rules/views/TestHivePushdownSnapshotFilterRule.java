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

package org.apache.hadoop.hive.ql.optimizer.calcite.rules.views;

import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

@RunWith(MockitoJUnitRunner.class)
public class TestHivePushdownSnapshotFilterRule extends TestRuleBase {

  @Mock
  private RelOptSchema schemaMock;

  @Test
  public void testFilterIsRemovedAndVersionIntervalFromIsSetWhenFilterHasSnapshotIdPredicate() {
    RelNode tableScan = createNonNativeTSSupportingSnapshots();

    RelBuilder relBuilder = HiveRelFactories.HIVE_BUILDER.create(relOptCluster, schemaMock);
    RelNode root = relBuilder.push(tableScan)
        .filter(REX_BUILDER.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            REX_BUILDER.makeInputRef(HiveAugmentSnapshotMaterializationRule.snapshotIdType(TYPE_FACTORY), 3),
            REX_BUILDER.makeLiteral(42, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), false)))
        .build();

    System.out.println(RelOptUtil.toString(root));

    RelNode newRoot = HiveMaterializedViewUtils.applyRule(root, HivePushdownSnapshotFilterRule.INSTANCE);

    assertThat(newRoot, instanceOf(HiveTableScan.class));
    HiveTableScan newScan = (HiveTableScan) newRoot;
    RelOptHiveTable optHiveTable = (RelOptHiveTable) newScan.getTable();
    assertThat(optHiveTable.getHiveTableMD().getVersionIntervalFrom(), is("42"));
  }

  @Test
  public void testFilterLeftIntactWhenItDoesNotHaveSnapshotIdPredicate() {
    RelNode tableScan = createNonNativeTS();

    RelBuilder relBuilder = HiveRelFactories.HIVE_BUILDER.create(relOptCluster, schemaMock);
    RelNode root = relBuilder.push(tableScan)
        .filter(REX_BUILDER.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
            REX_BUILDER.makeInputRef(TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), 1),
            REX_BUILDER.makeLiteral(42, TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER), false)))
        .build();

    System.out.println(RelOptUtil.toString(root));

    RelNode newRoot = HiveMaterializedViewUtils.applyRule(root, HivePushdownSnapshotFilterRule.INSTANCE);

    assertThat(newRoot.getDigest(), is(root.getDigest()));
  }
}
