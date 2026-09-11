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
import java.util.function.Consumer;
import java.util.stream.Stream;

import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc.Mode;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.Statistics.State;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

class TestSetHashGroupByMinReduction {

  // Default-reduction tuple used across all tests. Picked so the known-positive case
  // (ndvProduct=100, numRows=1000) produces a factor (0.9) strictly below the default
  // (0.99), triggering setMinReductionHashAggr.
  private static final float DEFAULT_MIN_REDUCTION = 0.99f;
  private static final float DEFAULT_MIN_REDUCTION_LOWER_BOUND = 0.1f;

  @ParameterizedTest(name = "{0}")
  @MethodSource("ndvProductCases")
  void testProcessByNdvProduct(String name, long ndvProduct, boolean expectSetCall)
      throws SemanticException {
    GroupByOperator op = setupCompleteHashGroupBy();
    GroupByDesc desc = op.getConf();

    try (MockedStatic<StatsUtils> stub = mockStatic(StatsUtils.class)) {
      stub.when(() -> StatsUtils.computeNDVGroupingColumns(any(), any(), eq(true)))
          .thenReturn(ndvProduct);

      Object result = new SetHashGroupByMinReduction().process(op, null, null);

      assertNull(result, "process() always returns null sentinel");
      verify(desc, expectSetCall ? atLeastOnce() : never()).setMinReductionHashAggr(anyFloat());
    }
  }

  private static Stream<Arguments> ndvProductCases() {
    return Stream.of(
        Arguments.of("unknownNDVEarlyReturns",        -1L,  false),
        Arguments.of("verifiedZeroFactorTooHigh",      0L,  false),
        Arguments.of("knownPositiveBelowDefault",    100L,  true)
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("earlyReturnGateCases")
  void testProcessEarlyReturnsOnUnsupportedState(String name, Consumer<GroupByOperator> flipGate)
      throws SemanticException {
    GroupByOperator op = setupCompleteHashGroupBy();
    flipGate.accept(op);

    Object result = new SetHashGroupByMinReduction().process(op, null, null);

    assertNull(result);
    verify(op.getConf(), never()).setMinReductionHashAggr(anyFloat());
  }

  private static Stream<Arguments> earlyReturnGateCases() {
    return Stream.of(
        Arguments.of("modeNotHash",
            (Consumer<GroupByOperator>) op ->
                when(op.getConf().getMode()).thenReturn(Mode.MERGEPARTIAL)),
        Arguments.of("basicStatsIncomplete",
            (Consumer<GroupByOperator>) op ->
                when(op.getStatistics().getBasicStatsState()).thenReturn(State.PARTIAL)),
        Arguments.of("columnStatsIncomplete",
            (Consumer<GroupByOperator>) op ->
                when(op.getStatistics().getColumnStatsState()).thenReturn(State.PARTIAL))
    );
  }

  // Passes all early-return gates; empty keys make the colStats loop a no-op.
  private static GroupByOperator setupCompleteHashGroupBy() {
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
    when(desc.getMinReductionHashAggr()).thenReturn(DEFAULT_MIN_REDUCTION);
    when(desc.getMinReductionHashAggrLowerBound()).thenReturn(DEFAULT_MIN_REDUCTION_LOWER_BOUND);

    when(stats.getBasicStatsState()).thenReturn(State.COMPLETE);
    when(stats.getColumnStatsState()).thenReturn(State.COMPLETE);

    when(parent.getStatistics()).thenReturn(parentStats);
    when(parentStats.getNumRows()).thenReturn(1000L);

    when(op.getParentOperators()).thenReturn(Arrays.asList(parent));

    return op;
  }
}
