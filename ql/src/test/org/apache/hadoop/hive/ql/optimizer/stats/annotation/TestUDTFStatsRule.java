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
package org.apache.hadoop.hive.ql.optimizer.stats.annotation;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.UDTFOperator;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.UDTFDesc;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.apache.hadoop.hive.ql.plan.mapper.StatsSource;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TestUDTFStatsRule {

  @Test
  void testUdtfDoesNotInheritParentColumnStats() throws Exception {
    HiveConf conf = new HiveConf();
    conf.setFloatVar(HiveConf.ConfVars.HIVE_STATS_UDTF_FACTOR, 2.0f);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_QUERY_REEXECUTION_ENABLED, false);

    PlanMapper planMapper = mock(PlanMapper.class);
    StatsSource statsSource = mock(StatsSource.class);
    Context context = mock(Context.class);
    when(context.getConf()).thenReturn(conf);
    when(context.getPlanMapper()).thenReturn(planMapper);
    when(context.getStatsSource()).thenReturn(statsSource);
    ParseContext pctx = mock(ParseContext.class);
    when(pctx.getConf()).thenReturn(conf);
    when(pctx.getContext()).thenReturn(context);
    AnnotateStatsProcCtx aspCtx = new AnnotateStatsProcCtx(pctx);

    Statistics parentStats = new Statistics(100L, 1000L, 0L, 0L);
    parentStats.setBasicStatsState(Statistics.State.COMPLETE);
    parentStats.setColumnStatsState(Statistics.State.COMPLETE);
    ColStatistics parentCol = new ColStatistics("_col0", "array<string>");
    parentCol.setCountDistint(50L);
    parentCol.setNumNulls(5L);
    parentCol.setAvgColLen(200.0);
    parentStats.addToColumnStats(Collections.singletonList(parentCol));

    SelectOperator selectOp = mock(SelectOperator.class);
    when(selectOp.getStatistics()).thenReturn(parentStats);

    ColumnInfo ci0 = new ColumnInfo("val", TypeInfoFactory.stringTypeInfo, "", false);
    ColumnInfo ci1 = new ColumnInfo("pos", TypeInfoFactory.intTypeInfo, "", false);
    RowSchema outputSchema = new RowSchema(Arrays.asList(ci0, ci1));

    UDTFOperator udtfOp = mock(UDTFOperator.class);
    UDTFDesc udtfDesc = mock(UDTFDesc.class);
    when(udtfOp.getConf()).thenReturn(udtfDesc);
    when(udtfOp.getSchema()).thenReturn(outputSchema);
    List<Operator<? extends OperatorDesc>> parents = new ArrayList<>();
    parents.add(selectOp);
    when(udtfOp.getParentOperators()).thenReturn(parents);

    final Statistics[] capturedStats = new Statistics[1];
    doAnswer(invocation -> {
      capturedStats[0] = invocation.getArgument(0);
      return null;
    }).when(udtfOp).setStatistics(any(Statistics.class));

    StatsRulesProcFactory.UDTFStatsRule rule = new StatsRulesProcFactory.UDTFStatsRule();
    rule.process(udtfOp, new Stack<>(), aspCtx);

    assertNotNull(capturedStats[0], "Statistics should have been set on UDTF operator");

    assertEquals(200L, capturedStats[0].getNumRows(),
        "Row count should be parentRows * udtfFactor = 100 * 2.0 = 200");

    List<ColStatistics> colStats = capturedStats[0].getColumnStats();
    assertNotNull(colStats);
    assertEquals(2, colStats.size(), "Should have 2 output columns matching UDTF schema");

    ColStatistics valStats = colStats.get(0);
    assertEquals("val", valStats.getColumnName());
    assertEquals("string", valStats.getColumnType());
    assertEquals(0L, valStats.getCountDistint(), "NDV should be 0 (unknown)");
    assertEquals(-1L, valStats.getNumNulls(), "numNulls should be -1 (unknown)");
    assertTrue(valStats.isEstimated());

    ColStatistics posStats = colStats.get(1);
    assertEquals("pos", posStats.getColumnName());
    assertEquals("int", posStats.getColumnType());
    assertEquals(0L, posStats.getCountDistint(), "NDV should be 0 (unknown)");
    assertEquals(-1L, posStats.getNumNulls(), "numNulls should be -1 (unknown)");
    assertTrue(posStats.isEstimated());

    // Parent's column stats must NOT leak into UDTF output
    for (ColStatistics cs : colStats) {
      assertTrue(cs.getCountDistint() != 50L,
          "Parent NDV must not leak into UDTF output column " + cs.getColumnName());
    }

    assertEquals(Statistics.State.COMPLETE, capturedStats[0].getBasicStatsState());
  }

  @Test
  void testUdtfWithNullParentStats() throws Exception {
    HiveConf conf = new HiveConf();
    PlanMapper planMapper = mock(PlanMapper.class);
    StatsSource statsSource = mock(StatsSource.class);
    Context context = mock(Context.class);
    when(context.getConf()).thenReturn(conf);
    when(context.getPlanMapper()).thenReturn(planMapper);
    when(context.getStatsSource()).thenReturn(statsSource);
    ParseContext pctx = mock(ParseContext.class);
    when(pctx.getConf()).thenReturn(conf);
    when(pctx.getContext()).thenReturn(context);
    AnnotateStatsProcCtx aspCtx = new AnnotateStatsProcCtx(pctx);

    SelectOperator selectOp = mock(SelectOperator.class);
    when(selectOp.getStatistics()).thenReturn(null);

    UDTFOperator udtfOp = mock(UDTFOperator.class);
    UDTFDesc udtfDesc = mock(UDTFDesc.class);
    when(udtfOp.getConf()).thenReturn(udtfDesc);
    List<Operator<? extends OperatorDesc>> parents = new ArrayList<>();
    parents.add(selectOp);
    when(udtfOp.getParentOperators()).thenReturn(parents);

    final Statistics[] capturedStats = new Statistics[1];
    doAnswer(invocation -> {
      capturedStats[0] = invocation.getArgument(0);
      return null;
    }).when(udtfOp).setStatistics(any(Statistics.class));

    StatsRulesProcFactory.UDTFStatsRule rule = new StatsRulesProcFactory.UDTFStatsRule();
    rule.process(udtfOp, new Stack<>(), aspCtx);

    // When parent has no stats, UDTF should not set stats either
    assertEquals(null, capturedStats[0],
        "When parent stats are null, UDTF should not produce stats");
  }
}
