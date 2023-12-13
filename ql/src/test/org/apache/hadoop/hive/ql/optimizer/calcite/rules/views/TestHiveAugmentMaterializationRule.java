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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.RelNode;
import org.apache.hadoop.hive.common.ValidReaderWriteIdList;
import org.apache.hadoop.hive.common.ValidTxnWriteIdList;
import org.apache.hadoop.hive.common.ValidWriteIdList;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.BitSet;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.mockito.Mockito.doReturn;

@RunWith(MockitoJUnitRunner.class)
public class TestHiveAugmentMaterializationRule extends TestRuleBase {
  @Test
  public void testFilterIsCreatedInTopOfTSWhenTableHasChangesSinceSavedSnapshot() {
    RelNode tableScan = createTS(t1NativeMock, "t1");

    ValidTxnWriteIdList current = new ValidTxnWriteIdList(10L);
    ValidWriteIdList validWriteIdList = new ValidReaderWriteIdList("default.t1", new long[0], new BitSet(), 10L);
    current.addTableValidWriteIdList(validWriteIdList);

    ValidTxnWriteIdList mv = new ValidTxnWriteIdList(5L);
    validWriteIdList = new ValidReaderWriteIdList("default.t1", new long[] {4, 6}, new BitSet(), 5L);
    mv.addTableValidWriteIdList(validWriteIdList);

    RelOptRule rule = new HiveAugmentMaterializationRule(REX_BUILDER, current, mv);

    RelNode newRoot = HiveMaterializedViewUtils.applyRule(tableScan, rule);

    assertThat(newRoot, instanceOf(HiveFilter.class));
    HiveFilter filter = (HiveFilter) newRoot;
    assertThat(filter.getCondition().toString(), is("AND(<=($3.writeId, 5), <>($3.writeId, 4), <>($3.writeId, 6))"));
  }

}