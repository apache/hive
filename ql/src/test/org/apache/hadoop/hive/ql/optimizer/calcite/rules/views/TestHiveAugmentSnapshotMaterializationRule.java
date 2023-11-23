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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.common.type.SnapshotContext;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class TestHiveAugmentSnapshotMaterializationRule extends TestRuleBase {

  @Test
  public void testWhenSnapshotAndTableAreEmptyNoFilterAdded() {
    RelNode tableScan = createTS();
    RelOptRule rule = HiveAugmentSnapshotMaterializationRule.with(Collections.emptyMap());

    RelNode newRoot = HiveMaterializedViewUtils.applyRule(tableScan, rule);

    assertThat(newRoot, is(tableScan));
  }

  @Test
  public void testWhenNoSnapshotButTableHasNewDataAFilterWithDefaultSnapshotIDAdded() {
    doReturn(new SnapshotContext(42)).when(table2storageHandler).getCurrentSnapshotContext(table2);
    RelNode tableScan = createTS();
    RelOptRule rule = HiveAugmentSnapshotMaterializationRule.with(Collections.emptyMap());

    RelNode newRoot = HiveMaterializedViewUtils.applyRule(tableScan, rule);

    assertThat(newRoot, instanceOf(HiveFilter.class));
    HiveFilter filter = (HiveFilter) newRoot;
    assertThat(filter.getCondition().toString(), is("<=($3, null)"));
  }

  @Test
  public void testWhenMVAndTableCurrentSnapshotAreTheSameNoFilterAdded() {
    doReturn(new SnapshotContext(42)).when(table2storageHandler).getCurrentSnapshotContext(table2);
    RelNode tableScan = createTS();
    Map<String, SnapshotContext> mvSnapshot = new HashMap<>();
    mvSnapshot.put(table2.getFullyQualifiedName(), new SnapshotContext(42));
    RelOptRule rule = HiveAugmentSnapshotMaterializationRule.with(mvSnapshot);

    RelNode newRoot = HiveMaterializedViewUtils.applyRule(tableScan, rule);

    assertThat(newRoot, is(tableScan));
  }

  @Test
  public void testWhenMVSnapshotIsDifferentThanTableCurrentSnapshotHasNewDataAFilterWithMVSnapshotIdAdded() {
    doReturn(new SnapshotContext(10)).when(table2storageHandler).getCurrentSnapshotContext(table2);
    RelNode tableScan = createTS();
    Map<String, SnapshotContext> mvSnapshot = new HashMap<>();
    mvSnapshot.put(table2.getFullyQualifiedName(), new SnapshotContext(42));
    RelOptRule rule = HiveAugmentSnapshotMaterializationRule.with(mvSnapshot);

    RelNode newRoot = HiveMaterializedViewUtils.applyRule(tableScan, rule);

    assertThat(newRoot, instanceOf(HiveFilter.class));
    HiveFilter filter = (HiveFilter) newRoot;
    assertThat(filter.getCondition().toString(), is("<=($3, 42)"));
  }
}