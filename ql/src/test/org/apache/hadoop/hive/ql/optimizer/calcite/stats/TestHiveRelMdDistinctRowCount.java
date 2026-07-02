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

package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestHiveRelMdDistinctRowCount {

  @ParameterizedTest(name = "{0}")
  @MethodSource("getDistinctRowCountCases")
  void testGetDistinctRowCountHiveTableScan(
      String scenarioName, long[] ndvs, double rowCount, double expected) {
    HiveTableScan htRel = mock(HiveTableScan.class);
    RelMetadataQuery mq = mock(RelMetadataQuery.class);

    when(htRel.getColStat(any())).thenReturn(buildColStats(ndvs));
    when(mq.getRowCount(htRel)).thenReturn(rowCount);

    HiveRelMdDistinctRowCount provider = new HiveRelMdDistinctRowCount();
    Double result = provider.getDistinctRowCount(htRel, mq, ImmutableBitSet.of(0), null);

    assertEquals(expected, result);
  }

  private static Stream<Arguments> getDistinctRowCountCases() {
    return Stream.of(
        // All positive, product fits under row count
        Arguments.of("allPositiveProductUnderRowCount", new long[] {10L, 5L}, 1000.0, 50.0),
        Arguments.of("singlePositive",                  new long[] {42L},     1000.0, 42.0),
        // Product exceeds row count -> capped
        Arguments.of("productCappedAtRowCount",         new long[] {2000L, 50L}, 1000.0, 1000.0),
        Arguments.of("singlePositiveExceedsRowCount",   new long[] {5000L},    1000.0, 1000.0),
        // Verified-zero NDV in any column triggers the <=0 early-exit
        Arguments.of("verifiedZeroInAnyColumn",         new long[] {10L, 0L, 5L}, 1000.0, 0.0),
        Arguments.of("verifiedZeroFirstShortCircuits",  new long[] {0L, 10L},    1000.0, 0.0),
        Arguments.of("verifiedZeroAlone",               new long[] {0L},         1000.0, 0.0),
        // Unknown NDV (-1) in any column triggers the <=0 early-exit (HIVE-29625)
        Arguments.of("unknownInAnyColumn",              new long[] {10L, -1L, 5L}, 1000.0, 0.0),
        Arguments.of("unknownFirstShortCircuits",       new long[] {-1L, 10L},   1000.0, 0.0),
        Arguments.of("unknownAlone",                    new long[] {-1L},        1000.0, 0.0),
        // Mixed verified-zero and unknown: both produce 0.0 regardless of order
        Arguments.of("unknownThenVerifiedZero",         new long[] {-1L, 0L},    1000.0, 0.0),
        Arguments.of("verifiedZeroThenUnknown",         new long[] {0L, -1L},    1000.0, 0.0),
        // Empty column list - loop doesn't execute, fall through to Math.min(1.0, rowCount)
        Arguments.of("emptyColStatsFallsThroughTo1",    new long[] {},           1000.0, 1.0),
        Arguments.of("emptyColStatsCappedByLowRowCount", new long[] {},          0.5,    0.5)
    );
  }

  private static List<ColStatistics> buildColStats(long[] ndvs) {
    List<ColStatistics> stats = new ArrayList<>();
    for (int i = 0; i < ndvs.length; i++) {
      ColStatistics cs = new ColStatistics("c" + i, "int");
      cs.setCountDistint(ndvs[i]);
      stats.add(cs);
    }
    return stats;
  }
}
