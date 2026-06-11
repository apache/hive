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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.parse.GenTezProcContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.stats.StatsUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

class TestReduceSinkMapJoinProc {

  // A null ndv row represents StatsUtils.getColStatisticsFromExpression returning null.
  @ParameterizedTest(name = "{0}")
  @MethodSource("keyCountFromNdvCases")
  void testProcessReduceSinkToHashJoinKeyCountFromNdv(
      String scenarioName, Long ndv, long parentRows, long expectedKeyCount) throws Exception {
    invokeAndAssertKeyCount(ndv == null ? null : buildColStat(ndv), parentRows, expectedKeyCount);
  }

  private static Stream<Arguments> keyCountFromNdvCases() {
    // Behavior recap:
    //   Initial keyCount = parentRows = stats.getNumRows()
    //   For each key col:
    //     if cs == null or cs.getCountDistint() < 0 -> maxKeyCount = MAX_VALUE; break    <-- HIVE-29625 change
    //     else maxKeyCount *= cs.getCountDistint()
    //   keyCount = min(maxKeyCount, keyCount)
    //   if keyCount == 0 -> keyCount = 1
    //   joinConf.getParentKeyCounts().put(pos, keyCount)   [only if keyCount != MAX_VALUE]
    return Stream.of(
        // NDV > 0 and below parentRows -> keyCount = NDV
        Arguments.of("knownPositiveBelowParent",       10L,    1000L, 10L),
        // NDV > 0 but above parentRows -> capped at parentRows
        Arguments.of("knownPositiveAboveParent",       5000L,  1000L, 1000L),
        // NDV = 0 (verified zero under HIVE-29625) -> maxKeyCount = 0 -> keyCount = 0 -> bumped to 1
        Arguments.of("verifiedZeroBumpsToOne",         0L,     1000L, 1L),
        // NDV = -1 (unknown under HIVE-29625) -> maxKeyCount = MAX_VALUE -> keyCount = parentRows
        Arguments.of("unknownFallsBackToParent",       -1L,    1000L, 1000L),
        // cs == null (no derivable stat) -> shares the MAX_VALUE fallback path
        Arguments.of("nullColStatsFallsBackToParent",  null,   1000L, 1000L)
    );
  }

  // Shared harness: build GenTezProcContext + mocked operators, run the method, read the keyCount put().
  private static void invokeAndAssertKeyCount(
      ColStatistics csForKey, long parentRows, long expectedKeyCount) throws Exception {

    // ---- Operator chain mocks ----
    ReduceSinkOperator parentRS = mock(ReduceSinkOperator.class);
    MapJoinOperator mapJoinOp = mock(MapJoinOperator.class);
    ReduceSinkDesc rsConf = mock(ReduceSinkDesc.class);
    MapJoinDesc joinConf = mock(MapJoinDesc.class);
    Statistics rsStats = mock(Statistics.class);
    ExprNodeDesc keyExpr = mock(ExprNodeDesc.class);
    BaseWork parentWork = mock(BaseWork.class);

    when(parentRS.getConf()).thenReturn(rsConf);
    when(parentRS.getStatistics()).thenReturn(rsStats);
    when(parentRS.getCompilationOpContext()).thenReturn(new CompilationOpContext());
    Map<String, ExprNodeDesc> columnExprMap = new HashMap<>();
    columnExprMap.put(Utilities.ReduceField.KEY.toString() + ".k0", keyExpr);
    when(parentRS.getColumnExprMap()).thenReturn(columnExprMap);
    Operator<?> upstreamParent = mock(Operator.class);
    when(upstreamParent.getSchema()).thenReturn(new RowSchema(Collections.emptyList()));
    when(parentRS.getParentOperators()).thenReturn(Arrays.asList(upstreamParent));
    List<Operator<? extends OperatorDesc>> childOps = new ArrayList<>();
    childOps.add(mapJoinOp);
    when(parentRS.getChildOperators()).thenReturn(childOps);

    when(mapJoinOp.getConf()).thenReturn(joinConf);

    when(rsConf.getOutputKeyColumnNames()).thenReturn(Arrays.asList("k0"));

    when(joinConf.isBucketMapJoin()).thenReturn(false);
    when(joinConf.isDynamicPartitionHashJoin()).thenReturn(false);
    Map<Integer, Long> parentKeyCounts = new LinkedHashMap<>();
    when(joinConf.getParentKeyCounts()).thenReturn(parentKeyCounts);
    when(joinConf.getParentToInput()).thenReturn(new LinkedHashMap<>());
    when(joinConf.getParentDataSizes()).thenReturn(new LinkedHashMap<>());
    Map<Byte, List<ExprNodeDesc>> keyExprMap = new HashMap<>();
    keyExprMap.put((byte) 0, Collections.emptyList());
    when(joinConf.getKeys()).thenReturn(keyExprMap);

    when(rsStats.getNumRows()).thenReturn(parentRows);
    when(rsStats.getDataSize()).thenReturn(8000L);

    when(parentWork.getName()).thenReturn("parent_work");

    // ---- Real GenTezProcContext (constructor sets up all the maps for us) ----
    HiveConf conf = new HiveConf();
    ParseContext parseCtx = mock(ParseContext.class);
    Context ctx = mock(Context.class);
    when(parseCtx.getContext()).thenReturn(ctx);
    when(ctx.getSequencer()).thenReturn(new AtomicInteger());
    GenTezProcContext context = new GenTezProcContext(
        conf, parseCtx, Collections.emptyList(), new ArrayList<>(),
        Collections.emptySet(), Collections.emptySet());

    context.childToWorkMap.put(parentRS, Arrays.asList(parentWork));
    context.mapJoinParentMap.put(mapJoinOp, Arrays.asList(parentRS));

    // ---- Stub StatsUtils.getColStatisticsFromExpression to return our chosen colStat ----
    try (MockedStatic<StatsUtils> stub = mockStatic(StatsUtils.class)) {
      stub.when(() -> StatsUtils.getColStatisticsFromExpression(
              any(HiveConf.class), any(Statistics.class), any(ExprNodeDesc.class)))
          .thenReturn(csForKey);

      ReduceSinkMapJoinProc.processReduceSinkToHashJoin(parentRS, mapJoinOp, context);
    }

    Long actual = parentKeyCounts.get(0);
    assertEquals(expectedKeyCount, actual.longValue());
  }

  private static ColStatistics buildColStat(long ndv) {
    ColStatistics cs = new ColStatistics("k0", "int");
    cs.setCountDistint(ndv);
    return cs;
  }
}
