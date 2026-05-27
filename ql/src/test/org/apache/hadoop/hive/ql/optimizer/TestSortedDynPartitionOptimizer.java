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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

class TestSortedDynPartitionOptimizer {

  @ParameterizedTest(name = "{0}")
  @MethodSource("computePartCardinalityColumnCases")
  void testComputePartCardinalityColumnBranch(
      String scenarioName, long[] ndvs, boolean firstStatNull, long expected) {
    SortedDynPartitionOptimizer.SortedDynamicPartitionProc proc = newProc(null);

    Statistics tStats = mock(Statistics.class);
    Operator<?> fsParent = mock(FileSinkOperator.class);
    RowSchema schema = mock(RowSchema.class);
    when(fsParent.getSchema()).thenReturn(schema);

    ColStatistics[] colStats = buildColStats(ndvs, firstStatNull, "p");
    List<ColumnInfo> sig = new ArrayList<>();
    List<Integer> partitionPos = new ArrayList<>();
    for (int i = 0; i < ndvs.length; i++) {
      String colName = "p" + i;
      ColumnInfo ci = mock(ColumnInfo.class);
      when(ci.getInternalName()).thenReturn(colName);
      sig.add(ci);
      when(tStats.getColumnStatisticsFromColName(colName)).thenReturn(colStats[i]);
      partitionPos.add(i);
    }
    when(schema.getSignature()).thenReturn(sig);

    long result = proc.computePartCardinality(
        partitionPos, Collections.emptyList(), tStats, fsParent, new ArrayList<>());

    assertEquals(expected, result, scenarioName);
  }

  private static Stream<Arguments> computePartCardinalityColumnCases() {
    return Stream.of(
        // All known positive NDVs - product computed
        Arguments.of("twoPositiveColumns",          new long[] {10L, 5L},     false, 50L),
        Arguments.of("singlePositiveColumn",        new long[] {42L},          false, 42L),
        Arguments.of("threeColumnsCompound",        new long[] {3L, 4L, 5L},   false, 60L),
        // HIVE-29625: NDV<0 is unknown - returns -1
        Arguments.of("unknownNDVShortCircuits",     new long[] {10L, -1L, 5L}, false, -1L),
        Arguments.of("firstUnknownShortCircuits",   new long[] {-1L, 10L},     false, -1L),
        Arguments.of("singleUnknownColumn",         new long[] {-1L},          false, -1L),
        // Verified zero NDV (HIVE-29625 disambiguation) - falls through to multiplication
        Arguments.of("verifiedZeroProducesZero",    new long[] {10L, 0L, 5L},  false, 0L),
        Arguments.of("singleVerifiedZero",          new long[] {0L},           false, 0L),
        // Missing stats (partStats == null) - returns -1
        Arguments.of("nullStatsShortCircuits",      new long[] {0L, 10L},      true,  -1L)
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("computePartCardinalityExprCases")
  void testComputePartCardinalityCustomExprBranch(
      String scenarioName, long[] ndvs, boolean firstStatNull, long expected) {
    HiveConf conf = new HiveConf();
    ParseContext parseCtx = mock(ParseContext.class);
    when(parseCtx.getConf()).thenReturn(conf);
    SortedDynPartitionOptimizer.SortedDynamicPartitionProc proc = newProc(parseCtx);

    Statistics tStats = mock(Statistics.class);
    Operator<?> fsParent = mock(FileSinkOperator.class);
    ArrayList<ExprNodeDesc> allRSCols = new ArrayList<>();
    List<Function<List<ExprNodeDesc>, ExprNodeDesc>> exprs = new ArrayList<>();
    List<ExprNodeDesc> resolvedExprs = new ArrayList<>();
    for (int i = 0; i < ndvs.length; i++) {
      ExprNodeDesc resolved = mock(ExprNodeDesc.class);
      resolvedExprs.add(resolved);
      exprs.add(cols -> resolved);
    }

    ColStatistics[] colStats = buildColStats(ndvs, firstStatNull, "e");
    try (MockedStatic<StatsUtils> stub = mockStatic(StatsUtils.class)) {
      for (int i = 0; i < ndvs.length; i++) {
        final int idx = i;
        stub.when(() -> StatsUtils.getColStatisticsFromExpression(eq(conf), eq(tStats), eq(resolvedExprs.get(idx))))
            .thenReturn(colStats[idx]);
      }

      long result = proc.computePartCardinality(
          Collections.emptyList(), exprs, tStats, fsParent, allRSCols);

      assertEquals(expected, result, scenarioName);
    }
  }

  private static Stream<Arguments> computePartCardinalityExprCases() {
    return Stream.of(
        Arguments.of("singleKnownExpr",             new long[] {7L},          false, 7L),
        Arguments.of("twoKnownExprsMultiply",       new long[] {3L, 4L},      false, 12L),
        // HIVE-29625: NDV<0 from expression stats short-circuits
        Arguments.of("unknownExprStatsShortCircuit", new long[] {5L, -1L},    false, -1L),
        Arguments.of("firstExprUnknown",            new long[] {-1L, 5L},     false, -1L),
        // Verified zero from expression stats - falls through to multiplication
        Arguments.of("verifiedZeroExprProducesZero", new long[] {5L, 0L},     false, 0L),
        // Null expression stats (StatsUtils returned null) - returns -1
        Arguments.of("nullExprStatsShortCircuits",   new long[] {0L, 5L},     true,  -1L)
    );
  }

  @Test
  void testComputePartCardinalityBothEmptyReturnsZero() {
    SortedDynPartitionOptimizer.SortedDynamicPartitionProc proc = newProc(null);
    long result = proc.computePartCardinality(
        Collections.emptyList(), Collections.emptyList(),
        mock(Statistics.class), mock(FileSinkOperator.class), new ArrayList<>());
    assertEquals(0L, result, "Both partitionPos and customPartitionExprs empty -> 0");
  }

  private static SortedDynPartitionOptimizer.SortedDynamicPartitionProc newProc(ParseContext parseCtx) {
    SortedDynPartitionOptimizer outer = new SortedDynPartitionOptimizer();
    return outer.new SortedDynamicPartitionProc(parseCtx);
  }

  /**
   * Builds one ColStatistics per ndvs entry; the first entry is null when firstStatNull
   * is true (used to simulate "missing stats" for either the partition-column or the
   * custom-expression branch).
   */
  private static ColStatistics[] buildColStats(long[] ndvs, boolean firstStatNull, String prefix) {
    ColStatistics[] result = new ColStatistics[ndvs.length];
    for (int i = 0; i < ndvs.length; i++) {
      if (i == 0 && firstStatNull) {
        result[i] = null;
      } else {
        ColStatistics cs = new ColStatistics(prefix + i, "int");
        cs.setCountDistint(ndvs[i]);
        result[i] = cs;
      }
    }
    return result;
  }
}
