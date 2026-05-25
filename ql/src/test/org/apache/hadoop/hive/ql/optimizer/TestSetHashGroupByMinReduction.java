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

package org.apache.hadoop.hive.ql.optimizer;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc.Mode;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.Statistics.State;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

class TestSetHashGroupByMinReduction {

  @Test
  void testProcessReturnsNullWhenNdvProductIsUnknown() throws SemanticException {
    GroupByOperator op = setupCompleteHashGroupBy(0.5f, 0.1f);
    GroupByDesc desc = op.getConf();

    try (MockedStatic<StatsUtils> stub = mockStatic(StatsUtils.class)) {
      stub.when(() -> StatsUtils.computeNDVGroupingColumns(any(), any(), eq(true)))
          .thenReturn(-1L);

      Object result = new SetHashGroupByMinReduction().process(op, null, null);

      assertNull(result, "Unknown ndvProduct (-1) makes process() return null");
      verify(desc, never()).setMinReductionHashAggr(anyFloat());
    }
  }

  @Test
  void testProcessProceedsWhenNdvProductIsVerifiedZero() throws SemanticException {
    // HIVE-29625 disambiguation: ndvProduct == 0 (verified) now distinct from ndvProduct < 0 (unknown).
    // Verified-zero means there are zero distinct group keys -> maximum reduction (factor = 1.0).
    GroupByOperator op = setupCompleteHashGroupBy(0.99f, 0.5f);
    GroupByDesc desc = op.getConf();

    try (MockedStatic<StatsUtils> stub = mockStatic(StatsUtils.class)) {
      stub.when(() -> StatsUtils.computeNDVGroupingColumns(any(), any(), eq(true)))
          .thenReturn(0L);

      Object result = new SetHashGroupByMinReduction().process(op, null, null);

      assertNull(result, "process() always returns null sentinel");
      // 1f - (0 / numRows) = 1.0; capped by lowerBound 0.5 -> stays 1.0; less than default 0.99? No, 1.0 > 0.99.
      // So setMinReductionHashAggr is NOT called (newFactor not strictly less than default).
      verify(desc, never()).setMinReductionHashAggr(anyFloat());
    }
  }

  @Test
  void testProcessProceedsWhenNdvProductIsKnownPositive() throws SemanticException {
    // numRows=1000, ndvProduct=100 -> factor = 1 - 100/1000 = 0.9
    // default = 0.99 -> 0.9 < 0.99 -> setMinReductionHashAggr(0.9)
    GroupByOperator op = setupCompleteHashGroupBy(0.99f, 0.1f);
    GroupByDesc desc = op.getConf();

    try (MockedStatic<StatsUtils> stub = mockStatic(StatsUtils.class)) {
      stub.when(() -> StatsUtils.computeNDVGroupingColumns(any(), any(), eq(true)))
          .thenReturn(100L);

      new SetHashGroupByMinReduction().process(op, null, null);

      verify(desc, atLeastOnce()).setMinReductionHashAggr(anyFloat());
    }
  }

  @Test
  void testProcessReturnsNullWhenModeNotHash() throws SemanticException {
    GroupByOperator op = setupCompleteHashGroupBy(0.99f, 0.5f);
    when(op.getConf().getMode()).thenReturn(Mode.MERGEPARTIAL);

    Object result = new SetHashGroupByMinReduction().process(op, null, null);

    assertNull(result, "Non-HASH mode -> early return null");
    verify(op.getConf(), never()).setMinReductionHashAggr(anyFloat());
  }

  @Test
  void testProcessReturnsNullWhenBasicStatsIncomplete() throws SemanticException {
    GroupByOperator op = setupCompleteHashGroupBy(0.99f, 0.5f);
    when(op.getStatistics().getBasicStatsState()).thenReturn(State.PARTIAL);

    Object result = new SetHashGroupByMinReduction().process(op, null, null);

    assertNull(result, "Incomplete basic stats -> early return null");
    verify(op.getConf(), never()).setMinReductionHashAggr(anyFloat());
  }

  @Test
  void testProcessReturnsNullWhenColumnStatsIncomplete() throws SemanticException {
    GroupByOperator op = setupCompleteHashGroupBy(0.99f, 0.5f);
    when(op.getStatistics().getColumnStatsState()).thenReturn(State.PARTIAL);

    Object result = new SetHashGroupByMinReduction().process(op, null, null);

    assertNull(result, "Incomplete column stats -> early return null");
    verify(op.getConf(), never()).setMinReductionHashAggr(anyFloat());
  }

  /**
   * Build a GroupByOperator that passes all the early-return gates, with empty keys
   * so the colStats loop is a no-op (the inputs to computeNDVGroupingColumns are
   * controlled directly via mockStatic in each test).
   */
  private static GroupByOperator setupCompleteHashGroupBy(
      float defaultMinReduction, float defaultMinReductionLowerBound) {
    GroupByOperator op = mock(GroupByOperator.class);
    GroupByDesc desc = mock(GroupByDesc.class);
    Statistics stats = mock(Statistics.class);
    Operator<?> parent = mock(Operator.class);
    Statistics parentStats = mock(Statistics.class);
    RowSchema schema = mock(RowSchema.class);

    when(op.getConf()).thenReturn(desc);
    when(op.getStatistics()).thenReturn(stats);
    when(op.getSchema()).thenReturn(schema);
    when(schema.getSignature()).thenReturn(Collections.emptyList());

    when(desc.getMode()).thenReturn(Mode.HASH);
    when(desc.getKeys()).thenReturn(Collections.emptyList());
    when(desc.getMinReductionHashAggr()).thenReturn(defaultMinReduction);
    when(desc.getMinReductionHashAggrLowerBound()).thenReturn(defaultMinReductionLowerBound);

    when(stats.getBasicStatsState()).thenReturn(State.COMPLETE);
    when(stats.getColumnStatsState()).thenReturn(State.COMPLETE);

    when(parent.getStatistics()).thenReturn(parentStats);
    when(parentStats.getNumRows()).thenReturn(1000L);

    when(op.getParentOperators()).thenReturn(Arrays.asList(parent));

    return op;
  }
}
