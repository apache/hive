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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.junit.jupiter.api.Test;

class TestConvertJoinMapJoin {

  @Test
  void crossProductByteFallback_allowsWhenOnlineSizeWithinBudget() {
    ConvertJoinMapJoin converter = new ConvertJoinMapJoin();
    converter.hashTableLoadFactor = 0.75f;
    Statistics stats = new Statistics(2L, 500L, 0L, 0L);
    assertTrue(
        converter.crossProductBuildSideWithinBroadcastBudgetAfterRowCheck(stats, 1L, 10_000_000L));
  }

  @Test
  void crossProductByteFallback_rejectsWhenOnlineSizeExceedsBudget() {
    ConvertJoinMapJoin converter = new ConvertJoinMapJoin();
    converter.hashTableLoadFactor = 0.75f;
    Statistics stats = new Statistics(50_000L, 50_000_000L, 0L, 0L);
    assertFalse(
        converter.crossProductBuildSideWithinBroadcastBudgetAfterRowCheck(stats, 1L, 10_000_000L));
  }

  @Test
  void crossProductByteFallback_rejectsWhenBudgetTooSmallForEstimatedSize() {
    ConvertJoinMapJoin converter = new ConvertJoinMapJoin();
    converter.hashTableLoadFactor = 0.75f;
    Statistics stats = new Statistics(2L, 500L, 0L, 0L);
    assertFalse(converter.crossProductBuildSideWithinBroadcastBudgetAfterRowCheck(stats, 1L, 1L));
  }

  /**
   * NDV-driven filter selectivity can estimate a tiny lookup at ~2 rows / a few hundred bytes
   * onlineDataSize when {@code hive.xprod.mapjoin.small.table.rows=1}. The row-only gate would
   * reject the broadcast even though the build side is well below the noconditionaltask byte
   * budget. The byte fallback must still admit map-join in that shape.
   */
  @Test
  void crossProductByteFallback_twoRowsTinyOnlineSize() {
    ConvertJoinMapJoin converter = new ConvertJoinMapJoin();
    converter.hashTableLoadFactor = 0.75f;
    final long ndvDrivenRowEstimate = 2L;
    final long tinyDataSizeBytes = 296L;
    Statistics stats = new Statistics(ndvDrivenRowEstimate, tinyDataSizeBytes, 0L, 0L);
    final long xprodRowThreshold = 1L;
    final long noconditionalBudgetBytes = 10_000_000L;
    assertTrue(converter.crossProductBuildSideWithinBroadcastBudgetAfterRowCheck(
        stats, xprodRowThreshold, noconditionalBudgetBytes));
  }

  /**
   * Same small row estimate (2) as the previous case, but with bytes large enough that the build
   * side exceeds the broadcast budget — the byte fallback must reject so the row-only cap still
   * bites.
   */
  @Test
  void crossProductByteFallback_rejectsTwoRowsWhenEstimatedPayloadExceedsBudget() {
    ConvertJoinMapJoin converter = new ConvertJoinMapJoin();
    converter.hashTableLoadFactor = 0.75f;
    Statistics stats = new Statistics(2L, 50_000_000L, 0L, 0L);
    assertFalse(converter.crossProductBuildSideWithinBroadcastBudgetAfterRowCheck(
        stats, 1L, 10_000_000L));
  }

  @Test
  void testComputeOnlineDataSizeGenericLargeDataSize() {
    ConvertJoinMapJoin converter = new ConvertJoinMapJoin();
    converter.hashTableLoadFactor = 0.75f;
    Statistics stats = new Statistics(1000L, Long.MAX_VALUE, 0L, 0L);

    long result = converter.computeOnlineDataSizeGeneric(stats, 10L, 8L);

    assertTrue(result >= 0, "Result should not be negative due to overflow");
  }

  @Test
  void testComputeOnlineDataSizeGenericLargeNumRowsWithOverhead() {
    ConvertJoinMapJoin converter = new ConvertJoinMapJoin();
    converter.hashTableLoadFactor = 0.75f;
    Statistics stats = new Statistics(Long.MAX_VALUE / 2, 1000L, 0L, 0L);

    long result = converter.computeOnlineDataSizeGeneric(stats, Long.MAX_VALUE / 4, Long.MAX_VALUE / 4);

    assertTrue(result >= 0, "Result should not be negative due to overflow");
    assertEquals(Long.MAX_VALUE, result, "Result should saturate at Long.MAX_VALUE");
  }

  @Test
  void testComputeOnlineDataSizeGenericNumNullsLargerThanNumRows() {
    ConvertJoinMapJoin converter = new ConvertJoinMapJoin();
    converter.hashTableLoadFactor = 0.75f;
    Statistics stats = new Statistics(100L, 10000L, 0L, 0L);
    List<ColStatistics> colStats = new ArrayList<>();
    ColStatistics cs = new ColStatistics("col1", "string");
    cs.setNumNulls(Long.MAX_VALUE);
    colStats.add(cs);
    stats.setColumnStats(colStats);

    long result = converter.computeOnlineDataSizeGeneric(stats, 10L, 8L);

    assertTrue(result >= 0, "Result should not be negative due to underflow in nonNullCount");
  }

  @Test
  void testComputeOnlineDataSizeGenericSmallDataSizeLargeAdjustment() {
    ConvertJoinMapJoin converter = new ConvertJoinMapJoin();
    converter.hashTableLoadFactor = 0.75f;
    Statistics stats = new Statistics(1000000L, 100L, 0L, 0L);
    List<ColStatistics> colStats = new ArrayList<>();
    ColStatistics cs = new ColStatistics("col1", "string");
    cs.setNumNulls(0L);
    colStats.add(cs);
    stats.setColumnStats(colStats);

    long result = converter.computeOnlineDataSizeGeneric(stats, 10L, 8L);

    assertTrue(result >= 0, "Result should not be negative when adjustment > dataSize");
  }

  @Test
  void testComputeOnlineDataSizeGenericAllExtremeValues() {
    ConvertJoinMapJoin converter = new ConvertJoinMapJoin();
    converter.hashTableLoadFactor = 0.75f;
    Statistics stats = new Statistics(Long.MAX_VALUE, Long.MAX_VALUE, 0L, 0L);
    List<ColStatistics> colStats = new ArrayList<>();
    ColStatistics cs = new ColStatistics("col1", "string");
    cs.setNumNulls(Long.MAX_VALUE);
    colStats.add(cs);
    stats.setColumnStats(colStats);

    long result = converter.computeOnlineDataSizeGeneric(stats, Long.MAX_VALUE, Long.MAX_VALUE);

    assertTrue(result >= 0, "Result should not be negative with extreme values");
    assertEquals(Long.MAX_VALUE, result, "Result should saturate at Long.MAX_VALUE");
  }

  @Test
  void testComputeCumulativeCardinalityWithParentsOverflow() {
    Operator<?> parent1 = createMockOperatorWithStats(Long.MAX_VALUE / 2);
    when(parent1.getParentOperators()).thenReturn(Collections.emptyList());
    Operator<?> parent2 = createMockOperatorWithStats(Long.MAX_VALUE / 2);
    when(parent2.getParentOperators()).thenReturn(Collections.emptyList());
    Operator<?> mockOp = createMockOperatorWithStats(Long.MAX_VALUE / 2);
    when(mockOp.getParentOperators()).thenReturn(Arrays.asList(parent1, parent2));

    Long result = invokeComputeCumulativeCardinality(mockOp);

    assertNotNull(result, "Result should not be null");
    assertTrue(result >= 0, "Result should not be negative due to overflow");
    assertEquals(Long.MAX_VALUE, result.longValue(), "Result should saturate at Long.MAX_VALUE");
  }

  @Test
  void testComputeCumulativeCardinalityDeepTreeOverflow() {
    Operator<?> leaf = createMockOperatorWithStats(Long.MAX_VALUE / 2);
    when(leaf.getParentOperators()).thenReturn(Collections.emptyList());
    Operator<?> mid1 = createMockOperatorWithStats(Long.MAX_VALUE / 2);
    when(mid1.getParentOperators()).thenReturn(Collections.singletonList(leaf));
    Operator<?> mid2 = createMockOperatorWithStats(Long.MAX_VALUE / 2);
    when(mid2.getParentOperators()).thenReturn(Collections.singletonList(mid1));
    Operator<?> root = createMockOperatorWithStats(Long.MAX_VALUE / 2);
    when(root.getParentOperators()).thenReturn(Collections.singletonList(mid2));

    Long result = invokeComputeCumulativeCardinality(root);

    assertNotNull(result, "Result should not be null");
    assertTrue(result >= 0, "Result should not be negative due to overflow");
    assertEquals(Long.MAX_VALUE, result.longValue(), "Result should saturate at Long.MAX_VALUE");
  }

  @SuppressWarnings("unchecked")
  private Operator<?> createMockOperatorWithStats(long numRows) {
    Operator<?> mockOp = mock(Operator.class);
    Statistics stats = new Statistics(numRows, numRows * 100, 0L, 0L);
    when(mockOp.getStatistics()).thenReturn(stats);
    return mockOp;
  }

  private Long invokeComputeCumulativeCardinality(Operator<?> op) {
    try {
      Method method = ConvertJoinMapJoin.class.getDeclaredMethod(
          "computeCumulativeCardinality", Operator.class);
      method.setAccessible(true);
      return (Long) method.invoke(null, op);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
