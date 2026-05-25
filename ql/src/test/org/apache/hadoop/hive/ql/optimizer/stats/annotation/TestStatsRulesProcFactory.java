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

import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.hadoop.hive.common.ndv.fm.FMSketch;
import org.apache.hadoop.hive.common.ndv.hll.HyperLogLog;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.metastore.StatisticsTestUtils;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.tez.DagUtils;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.yarn.api.records.Resource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.apache.hadoop.hive.ql.optimizer.stats.annotation.StatsRulesProcFactory.FilterStatsRule.extractFloatFromLiteralValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestStatsRulesProcFactory {

  private final static String COL_NAME = "col1";
  private final static ExprNodeDesc COL_EXPR = new  ExprNodeColumnDesc(
      TypeInfoFactory.intTypeInfo, COL_NAME, "table", false);

  private final static AnnotateStatsProcCtx STATS_PROC_CTX = new AnnotateStatsProcCtx(null);

  private final static long[] VALUES = { 1L, 2L, 2L, 2L, 2L, 2L, 2L, 2L, 3L, 4L, 5L, 6L, 7L };

  @Test
  public void testComparisonRowCountZeroNonNullValues() throws SemanticException {
    long numNulls = 2;
    long[] values = {};
    Statistics stats = createStatistics(values, numNulls);

    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(3)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(0, numRows);
  }

  @Test
  public void testComparisonRowCountInvalidKll() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    stats.getColumnStats().get(0).setHistogram(null);

    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(3)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    // when no KLL it defaults to 1/3 of the number of rows
    assertEquals((VALUES.length + numNulls) / 3, numRows);

    // empty KLL array is not valid either
    stats.getColumnStats().get(0).setHistogram(new byte[0]);
    numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    // when no KLL it defaults to 1/3 of the number of rows
    assertEquals((VALUES.length + numNulls) / 3, numRows);
  }

  @Test
  public void testEvaluateInExprWithUnknownNDVAppliesHalfFactor() throws SemanticException {
    // HIVE-29625: when the column's NDV is unknown (-1), the IN filter takes
    // factor *= 0.5 and continues (rather than the old behavior of treating
    // dvs==0 as unknown). currNumRows=13, factor=0.5, inFactor=1.0 (default).
    Statistics stats = createStatistics(VALUES, 0);
    stats.getColumnStats().get(0).setCountDistint(-1); // force unknown NDV

    AnnotateStatsProcCtx ctx = spy(new AnnotateStatsProcCtx(null));
    when(ctx.getConf()).thenReturn(new HiveConf());

    ExprNodeDesc inExpr = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFIn(),
        Arrays.asList(COL_EXPR, createExprNodeConstantDesc(1), createExprNodeConstantDesc(2)));

    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, inExpr, ctx, Arrays.asList(COL_NAME), null, VALUES.length);

    assertEquals(Math.round(VALUES.length * 0.5), numRows);
  }

  @Test
  public void testEvaluateEqualWithUnknownNDVUsesHalfRows() throws SemanticException {
    // HIVE-29625: col = const where col.NDV=-1 (unknown) falls back to numRows/2.
    // VALUES.length=13, expected = 13/2 = 6 (long division).
    Statistics stats = createStatistics(VALUES, 0);
    stats.getColumnStats().get(0).setCountDistint(-1);

    AnnotateStatsProcCtx ctx = spy(new AnnotateStatsProcCtx(null));
    when(ctx.getConf()).thenReturn(new HiveConf());

    ExprNodeDesc eqExpr = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPEqual(),
        Arrays.asList(COL_EXPR, createExprNodeConstantDesc(1)));

    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, eqExpr, ctx, Arrays.asList(COL_NAME), null, VALUES.length);

    assertEquals(6, numRows);
  }

  @Test
  public void testEvaluateEqualWithVerifiedZeroNDVReturnsZero() throws SemanticException {
    // HIVE-29625: col = const where col.NDV=0 (verified zero) returns 0 rows.
    Statistics stats = createStatistics(VALUES, 0);
    stats.getColumnStats().get(0).setCountDistint(0);

    AnnotateStatsProcCtx ctx = spy(new AnnotateStatsProcCtx(null));
    when(ctx.getConf()).thenReturn(new HiveConf());

    ExprNodeDesc eqExpr = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPEqual(),
        Arrays.asList(COL_EXPR, createExprNodeConstantDesc(1)));

    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, eqExpr, ctx, Arrays.asList(COL_NAME), null, VALUES.length);

    assertEquals(0, numRows);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("groupByFinalCases")
  public void testGroupByStatsRuleFinalCardinality(String name, long keyNdv, long expectedRows) throws SemanticException {
    assertGroupByFinalCardinality(keyNdv, expectedRows);
  }

  private static Stream<Arguments> groupByFinalCases() {
    return Stream.of(
        Arguments.of("ndvUnknownAppliesFallback",            -1L, 500L),
        Arguments.of("ndvVerifiedZeroFlowsThroughClampedToOne", 0L,  1L),
        Arguments.of("ndvKnownUsesProduct",                  10L,  10L)
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("groupByHashCases")
  public void testCheckMapSideAggregationHashCardinality(String name, long keyNdv, long expectedRows) throws SemanticException {
    assertGroupByHashCardinality(keyNdv, expectedRows);
  }

  private static Stream<Arguments> groupByHashCases() {
    return Stream.of(
        Arguments.of("ndvUnknownFallsBackToHalfParent",  -1L, 500L),
        Arguments.of("ndvKnownUsesProduct",             100L, 100L)
    );
  }

  private void assertGroupByHashCardinality(long keyNdv, long expectedRows) throws SemanticException {
    Statistics parentStats = new Statistics(1000, 8000, 0, 0);
    parentStats.setBasicStatsState(Statistics.State.COMPLETE);
    parentStats.setColumnStatsState(Statistics.State.COMPLETE);
    ColStatistics keyCol = new ColStatistics("k", "int");
    keyCol.setCountDistint(keyNdv);
    keyCol.setNumNulls(0);
    parentStats.setColumnStats(Collections.singletonList(keyCol));

    @SuppressWarnings("unchecked")
    Operator<? extends OperatorDesc> parent = mock(Operator.class);
    when(parent.getStatistics()).thenReturn(parentStats);
    when(parent.getParentOperators()).thenReturn(Collections.emptyList());

    GroupByDesc gbyDesc = mock(GroupByDesc.class);
    when(gbyDesc.getMode()).thenReturn(GroupByDesc.Mode.HASH);
    when(gbyDesc.getAggregators()).thenReturn(Collections.emptyList());
    when(gbyDesc.isGroupingSetsPresent()).thenReturn(false);
    ExprNodeColumnDesc keyExpr = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "k", "table", false);
    when(gbyDesc.getKeys()).thenReturn(Collections.singletonList(keyExpr));

    GroupByOperator gop = mock(GroupByOperator.class);
    when(gop.getParentOperators()).thenReturn(Collections.singletonList(parent));
    when(gop.getConf()).thenReturn(gbyDesc);
    Map<String, ExprNodeDesc> colExprMap = new HashMap<>();
    colExprMap.put("_col0", keyExpr);
    when(gop.getColumnExprMap()).thenReturn(colExprMap);
    RowSchema rs = mock(RowSchema.class);
    ColumnInfo colInfo = new ColumnInfo("_col0", TypeInfoFactory.intTypeInfo, "table", false);
    when(rs.getSignature()).thenReturn(Collections.singletonList(colInfo));
    when(rs.getColumnInfo("_col0")).thenReturn(colInfo);
    when(gop.getSchema()).thenReturn(rs);

    Context context = mock(Context.class);
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_QUERY_REEXECUTION_ENABLED, false);
    when(context.getConf()).thenReturn(conf);
    ParseContext pctx = mock(ParseContext.class);
    when(pctx.getContext()).thenReturn(context);
    AnnotateStatsProcCtx ctx = spy(new AnnotateStatsProcCtx(null));
    when(ctx.getConf()).thenReturn(conf);
    when(ctx.getParseContext()).thenReturn(pctx);

    // checkMapSideAggregation calls DagUtils.getContainerResource(conf) to compute
    // the available hash-aggregation memory. Stub it to a generous 1024 MB so the
    // estimated hash table size stays well under the threshold and hashAgg is selected.
    try (MockedStatic<DagUtils> dagMock = mockStatic(DagUtils.class)) {
      Resource res = mock(Resource.class);
      when(res.getMemorySize()).thenReturn(1024L);
      dagMock.when(() -> DagUtils.getContainerResource(any())).thenReturn(res);

      new StatsRulesProcFactory.GroupByStatsRule().process(gop, null, ctx, (Object[]) null);
    }

    ArgumentCaptor<Statistics> captor = ArgumentCaptor.forClass(Statistics.class);
    verify(gop).setStatistics(captor.capture());
    assertEquals(expectedRows, captor.getValue().getNumRows());
  }

  private void assertGroupByFinalCardinality(long keyNdv, long expectedRows) throws SemanticException {
    Statistics parentStats = new Statistics(1000, 8000, 0, 0);
    parentStats.setBasicStatsState(Statistics.State.COMPLETE);
    parentStats.setColumnStatsState(Statistics.State.COMPLETE);
    ColStatistics keyCol = new ColStatistics("k", "int");
    keyCol.setCountDistint(keyNdv);
    keyCol.setNumNulls(0);
    parentStats.setColumnStats(Collections.singletonList(keyCol));

    @SuppressWarnings("unchecked")
    Operator<? extends OperatorDesc> parent = mock(Operator.class);
    when(parent.getStatistics()).thenReturn(parentStats);
    when(parent.getParentOperators()).thenReturn(Collections.emptyList());

    GroupByDesc gbyDesc = mock(GroupByDesc.class);
    when(gbyDesc.getMode()).thenReturn(GroupByDesc.Mode.FINAL);
    when(gbyDesc.getAggregators()).thenReturn(Collections.emptyList());
    when(gbyDesc.isGroupingSetsPresent()).thenReturn(false);
    ExprNodeColumnDesc keyExpr = new ExprNodeColumnDesc(TypeInfoFactory.intTypeInfo, "k", "table", false);
    when(gbyDesc.getKeys()).thenReturn(Collections.singletonList(keyExpr));

    GroupByOperator gop = mock(GroupByOperator.class);
    when(gop.getParentOperators()).thenReturn(Collections.singletonList(parent));
    when(gop.getConf()).thenReturn(gbyDesc);
    Map<String, ExprNodeDesc> colExprMap = new HashMap<>();
    colExprMap.put("_col0", keyExpr);
    when(gop.getColumnExprMap()).thenReturn(colExprMap);
    RowSchema rs = mock(RowSchema.class);
    ColumnInfo colInfo = new ColumnInfo("_col0", TypeInfoFactory.intTypeInfo, "table", false);
    when(rs.getSignature()).thenReturn(Collections.singletonList(colInfo));
    when(rs.getColumnInfo("_col0")).thenReturn(colInfo);
    when(gop.getSchema()).thenReturn(rs);

    Context context = mock(Context.class);
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_QUERY_REEXECUTION_ENABLED, false);
    when(context.getConf()).thenReturn(conf);
    ParseContext pctx = mock(ParseContext.class);
    when(pctx.getContext()).thenReturn(context);
    AnnotateStatsProcCtx ctx = spy(new AnnotateStatsProcCtx(null));
    when(ctx.getConf()).thenReturn(conf);
    when(ctx.getParseContext()).thenReturn(pctx);

    new StatsRulesProcFactory.GroupByStatsRule().process(gop, null, ctx, (Object[]) null);

    ArgumentCaptor<Statistics> captor = ArgumentCaptor.forClass(Statistics.class);
    verify(gop).setStatistics(captor.capture());
    assertEquals(expectedRows, captor.getValue().getNumRows());
  }

  @Test
  public void testEvaluateEqualWithKnownNDVUsesUniformDistribution() throws SemanticException {
    // Regression check: col = const where col.NDV=7 returns round(13/7)=2 rows.
    // VALUES has 7 distinct values, so createStatistics sets NDV=7.
    Statistics stats = createStatistics(VALUES, 0);

    AnnotateStatsProcCtx ctx = spy(new AnnotateStatsProcCtx(null));
    when(ctx.getConf()).thenReturn(new HiveConf());

    ExprNodeDesc eqExpr = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFOPEqual(),
        Arrays.asList(COL_EXPR, createExprNodeConstantDesc(1)));

    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, eqExpr, ctx, Arrays.asList(COL_NAME), null, VALUES.length);

    assertEquals(2, numRows);
  }

  @Test
  public void testEvaluateInExprWithVerifiedZeroNDVReturnsZero() throws SemanticException {
    // HIVE-29625: when the column's NDV is verified zero (0), the IN filter
    // sets factor=0 and breaks out of the loop, so no rows match.
    Statistics stats = createStatistics(VALUES, 0);
    stats.getColumnStats().get(0).setCountDistint(0); // force verified-zero NDV

    AnnotateStatsProcCtx ctx = spy(new AnnotateStatsProcCtx(null));
    when(ctx.getConf()).thenReturn(new HiveConf());

    ExprNodeDesc inExpr = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
        new GenericUDFIn(),
        Arrays.asList(COL_EXPR, createExprNodeConstantDesc(1), createExprNodeConstantDesc(2)));

    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, inExpr, ctx, Arrays.asList(COL_NAME), null, VALUES.length);

    assertEquals(0, numRows);
  }

  @Test
  public void testComparisonRowCountLessThan() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);

    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(3)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(8, numRows);
  }

  @Test
  public void testComparisonRowCountLessThanMin() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);

    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(1)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(0, numRows);
  }

  @Test
  public void testComparisonRowCountLessThanBelowMin() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);

    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(0)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(0, numRows);
  }

  @Test
  public void testComparisonRowCountLessThanMax() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);

    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(7)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(12, numRows);
  }

  @Test
  public void testComparisonRowCountLessThanAboveMax() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);

    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(8)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(13, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrLessThan() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(3)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(9, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrLessThanMin() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(1)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(1, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrLessThanBelowMin() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(0)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(0, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrLessThanMax() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(7)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(13, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrLessThanAboveMax() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(8)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(13, numRows);
  }

  @Test
  public void testComparisonRowCountGreaterThan() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(5)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(2, numRows);
  }

  @Test
  public void testComparisonRowCountGreaterThanMin() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(1)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(12, numRows);
  }

  @Test
  public void testComparisonRowCountGreaterThanBelowMin() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(0)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(13, numRows);
  }

  @Test
  public void testComparisonRowCountGreaterThanMax() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(7)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(0, numRows);
  }

  @Test
  public void testComparisonRowCountGreaterThanAboveMax() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(8)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(0, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrGreaterThan() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(5)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(3, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrGreaterThanMin() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(1)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(13, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrGreaterThanBelowMin() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(0)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(13, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrGreaterThanMax() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(7)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(1, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrGreaterThanBeyondMax() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(8)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(0, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrLessThanWhenMinEqualMax() throws SemanticException {
    long[] values = { 1L, 1L };
    long numNulls = 2;
    Statistics stats = createStatistics(values, numNulls);

    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(1)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, values.length + numNulls);

    assertEquals(2, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrGreaterThanWhenMinEqualMax() throws SemanticException {
    long[] values = { 1L, 1L };
    long numNulls = 2;
    Statistics stats = createStatistics(values, numNulls);

    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(1)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, values.length + numNulls);

    assertEquals(2, numRows);
  }

  @Test
  public void testBetween() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);

    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFBetween(), Arrays.asList(new ExprNodeConstantDesc(Boolean.FALSE),
            COL_EXPR, createExprNodeConstantDesc(3), createExprNodeConstantDesc(4)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(2, numRows);
  }

  @Test
  public void testLiteralExtraction() {
    final double DELTA = 1e-5;

    assertEquals((float) 100,
        extractFloatFromLiteralValue("int", "100"), DELTA);
    assertEquals((float) 1,
        extractFloatFromLiteralValue("smallint", "1"), DELTA);
    assertEquals((float) 1,
        extractFloatFromLiteralValue("tinyint", "1"), DELTA);
    assertEquals((float) 10000000000000L,
        extractFloatFromLiteralValue("bigint", "10000000000000"), DELTA);
    assertEquals((float) 15.2,
        extractFloatFromLiteralValue("decimal(5, 2)", "15.2"), DELTA);
    assertEquals(15.0002f,
        extractFloatFromLiteralValue("float", "15.0002"), DELTA);
    assertEquals((float) 1512.2222222d,
        extractFloatFromLiteralValue("double", "1512.2222222d"), DELTA);
    assertEquals((float) Date.valueOf("2022-01-05").toEpochSecond(),
        extractFloatFromLiteralValue("date", "2022-01-05"), DELTA);
    assertEquals((float) Date.valueOf("2022-1-5").toEpochSecond(),
        extractFloatFromLiteralValue("date", "2022-1-5"), DELTA);
    assertEquals((float) Timestamp.valueOf("2022-01-05 00:00:00").toEpochSecond(),
        extractFloatFromLiteralValue("timestamp", "2022-01-05 00:00:00"), DELTA);
    assertEquals((float) Timestamp.valueOf("2022-01-05 01:20:02").toEpochSecond(),
        extractFloatFromLiteralValue("timestamp", "2022-01-05 01:20:02"), DELTA);
    assertEquals((float) Timestamp.valueOf("2022-01-05 01:20:02").toEpochSecond(),
        extractFloatFromLiteralValue("timestamp", "2022-1-5 01:20:02"), DELTA);
  }

  @Test
  public void testLiteralExtractionFailures() {
    // make sure the correct exceptions are raised so that we can default to standard computation
    String[] types = {"int", "tinyint", "smallint", "bigint", "date", "timestamp", "float", "double"};
    for (String type : types) {
      // check we throw the correct exception when the boundary value parsing fails
      assertThrows(IllegalArgumentException.class, () -> extractFloatFromLiteralValue(type, "abc"));
      // check we throw the correct exception (NullPointerException for some, NumberFormatException for others,
      // so we use their common parent RuntimeException) when a null value provided
      assertThrows(RuntimeException.class, () -> extractFloatFromLiteralValue(type, null));
    }

    // check we throw the correct exception when an unsupported type is provided
    assertThrows(IllegalStateException.class,
        () -> extractFloatFromLiteralValue("typex", "abc"));
  }

  @Test
  public void testBetweenLeftLowerThanMin() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);

    ExprNodeDesc exprNodeDescLeq = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(4)));
    long numRowsLeq = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDescLeq, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    ExprNodeDesc exprNodeDescBetween = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFBetween(), Arrays.asList(new ExprNodeConstantDesc(Boolean.FALSE),
        COL_EXPR, createExprNodeConstantDesc(0), createExprNodeConstantDesc(4)));
    long numRowsBetween = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDescBetween, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(numRowsLeq, numRowsBetween);
    assertEquals(10, numRowsBetween);
  }

  @Test
  public void testBetweenLeftLowerThanMinRightHigherThanMax() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);

    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFBetween(), Arrays.asList(new ExprNodeConstantDesc(Boolean.FALSE),
        COL_EXPR, createExprNodeConstantDesc(0), createExprNodeConstantDesc(10)));
    long numRowsBetween = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(VALUES.length, numRowsBetween);
  }

  @Test
  public void testBetweenRightHigherThanMax() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);

    ExprNodeDesc exprNodeDescGeq = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(2)));
    long numRowsGeq = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDescGeq, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    ExprNodeDesc exprNodeDescBetween = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFBetween(), Arrays.asList(new ExprNodeConstantDesc(Boolean.FALSE),
        COL_EXPR, createExprNodeConstantDesc(2), createExprNodeConstantDesc(10)));
    long numRowsBetween = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDescBetween, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(numRowsGeq, numRowsBetween);
    assertEquals(12, numRowsBetween);
  }

  @Test
  public void testBetweenRightLowerThanMin() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);

    ExprNodeDesc exprNodeDescBetween = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFBetween(), Arrays.asList(new ExprNodeConstantDesc(Boolean.FALSE),
        COL_EXPR, createExprNodeConstantDesc(-1), createExprNodeConstantDesc(0)));
    long numRowsBetween = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDescBetween, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(0, numRowsBetween);
  }

  @Test
  public void testBetweenLeftHigherThanMax() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);

    ExprNodeDesc exprNodeDescBetween = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFBetween(), Arrays.asList(new ExprNodeConstantDesc(Boolean.FALSE),
        COL_EXPR, createExprNodeConstantDesc(10), createExprNodeConstantDesc(12)));
    long numRowsBetween = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDescBetween, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(0, numRowsBetween);
  }

  @Test
  public void testBetweenLeftEqualMax() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);

    ExprNodeDesc exprNodeDescBetween = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFBetween(), Arrays.asList(new ExprNodeConstantDesc(Boolean.FALSE),
        COL_EXPR, createExprNodeConstantDesc(3), createExprNodeConstantDesc(3)));
    long numRowsBetween = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDescBetween, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(7, numRowsBetween);
  }

  @Test
  public void testNotBetween() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);

    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFBetween(), Arrays.asList(new ExprNodeConstantDesc(Boolean.TRUE),
        COL_EXPR, createExprNodeConstantDesc(3), createExprNodeConstantDesc(4)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    ExprNodeDesc exprNodeDescLth = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(3)));
    long numRowsLth = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDescLth, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    ExprNodeDesc exprNodeDescGth = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(4)));
    long numRowsGth = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDescGth, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(11, numRows);
    assertEquals(numRows, numRowsLth + numRowsGth);
  }

  @Test
  public void testNotBetweenLowerThanMinHigherThanMax() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);

    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFBetween(), Arrays.asList(new ExprNodeConstantDesc(Boolean.TRUE),
        COL_EXPR, createExprNodeConstantDesc(0), createExprNodeConstantDesc(10)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(0, numRows);
  }

  @Test
  public void testNotBetweenLeftEqualsRight() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);

    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFBetween(), Arrays.asList(new ExprNodeConstantDesc(Boolean.TRUE),
        COL_EXPR, createExprNodeConstantDesc(3), createExprNodeConstantDesc(3)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(VALUES.length - 1, numRows);
  }

  @Test
  public void testNotBetweenRightLowerThanLeft() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);

    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFBetween(), Arrays.asList(new ExprNodeConstantDesc(Boolean.TRUE),
        COL_EXPR, createExprNodeConstantDesc(4), createExprNodeConstantDesc(3)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    assertEquals(VALUES.length, numRows);
  }

  private ExprNodeDesc createExprNodeConstantDesc(int value) {
    return new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, value);
  }

  private Statistics createStatistics(long[] values, long numNulls) {
    long numDVs = Arrays.stream(values).distinct().count();
    Statistics stats = new Statistics(values.length + numNulls, 100, 100, 100);

    HyperLogLog hll = StatisticsTestUtils.createHll(values);
    float[] val = new float[values.length];
    for (int i = 0; i < values.length; i++) {
      val[i] = values[i];
    }
    KllFloatsSketch kll = StatisticsTestUtils.createKll(val);
    ColStatistics colStatistics = createColStatistics(COL_NAME, "int", numNulls, numDVs, hll, kll);

    stats.addToColumnStats(Collections.singletonList(colStatistics));

    return stats;
  }

  private static ColStatistics createColStatistics(
      String colName, String colType, long numNulls, long numDVs, Object hll, KllFloatsSketch kll) {
    ColStatistics colStatistics = new ColStatistics(colName, colType);

    colStatistics.setNumNulls(numNulls);
    colStatistics.setCountDistint(numDVs);
    if (hll != null) {
      if (hll instanceof HyperLogLog) {
        colStatistics.setBitVectors(((HyperLogLog) hll).serialize());
      } else if (hll instanceof FMSketch) {
        colStatistics.setBitVectors(((FMSketch) hll).serialize());
      } else {
        throw new IllegalArgumentException("Unsupported HLL class: " + hll.getClass().getName());
      }
    }
    if (kll != null) {
      colStatistics.setHistogram(kll.toByteArray());
    }

    return colStatistics;
  }

  /**
   * Test that computeAggregateColumnMinMax properly handles numNulls=-1 (unknown).
   * With the fix, numNulls=-1 should be treated as 0, giving valuesCount = numRows.
   * Without the fix, valuesCount = numRows - (-1) = numRows + 1 (wrong).
   */
  @Test
  public void testComputeAggregateColumnMinMaxWithUnknownNumNulls() throws SemanticException {
    ColStatistics cs = new ColStatistics("_col0", "bigint");
    HiveConf conf = new HiveConf();

    // Create parent column stats with numNulls=-1 (unknown) and Range(1, 100)
    ColStatistics parentColStats = new ColStatistics("val", "int");
    parentColStats.setNumNulls(-1);  // unknown numNulls - this is what we're testing
    parentColStats.setCountDistint(100);
    parentColStats.setRange(1, 100);

    Statistics parentStats = new Statistics(100, 400, 400, 400);
    parentStats.addToColumnStats(Collections.singletonList(parentColStats));

    ExprNodeColumnDesc colExpr = new ExprNodeColumnDesc(
        TypeInfoFactory.intTypeInfo, "val", "t", false);

    AggregationDesc agg = new AggregationDesc();
    agg.setGenericUDAFName("count");
    agg.setParameters(Collections.singletonList(colExpr));
    agg.setDistinct(false);
    agg.setMode(GenericUDAFEvaluator.Mode.COMPLETE);

    StatsRulesProcFactory.GroupByStatsRule.computeAggregateColumnMinMax(
        cs, conf, agg, "bigint", parentStats);

    // Verify: With the fix, COUNT Range should be (0, 100)
    // numNulls=-1 is treated as 0, so valuesCount = 100 - 0 = 100
    // Without the fix, valuesCount = 100 - (-1) = 101 (WRONG)
    assertNotNull(cs.getRange(), "Range should be set on COUNT column");
    assertEquals(0L, ((Number) cs.getRange().minValue).longValue(), "COUNT min should be 0");
    assertEquals(100L, ((Number) cs.getRange().maxValue).longValue(),
        "COUNT max should be 100 (numRows), not 101");
  }

  @Test
  public void testComputeAggregateColumnMinMaxWithKnownNumNulls() throws SemanticException {
    ColStatistics cs = new ColStatistics("_col0", "bigint");
    HiveConf conf = new HiveConf();

    // Create parent column stats with numNulls=20 (known) and Range
    ColStatistics parentColStats = new ColStatistics("val", "int");
    parentColStats.setNumNulls(20);  // known numNulls
    parentColStats.setCountDistint(80);
    parentColStats.setRange(1, 100);

    Statistics parentStats = new Statistics(100, 400, 400, 400);
    parentStats.addToColumnStats(Collections.singletonList(parentColStats));

    ExprNodeColumnDesc colExpr = new ExprNodeColumnDesc(
        TypeInfoFactory.intTypeInfo, "val", "t", false);
    AggregationDesc agg = new AggregationDesc();
    agg.setGenericUDAFName("count");
    agg.setParameters(Collections.singletonList(colExpr));
    agg.setDistinct(false);
    agg.setMode(GenericUDAFEvaluator.Mode.COMPLETE);

    StatsRulesProcFactory.GroupByStatsRule.computeAggregateColumnMinMax(
        cs, conf, agg, "bigint", parentStats);

    // With known numNulls=20, valuesCount = 100 - 20 = 80
    assertNotNull(cs.getRange(), "Range should be set");
    assertEquals(0L, ((Number) cs.getRange().minValue).longValue());
    assertEquals(80L, ((Number) cs.getRange().maxValue).longValue(),
        "COUNT max should be 80 (numRows - numNulls)");
  }

  @Test
  public void testComputeAggregateColumnMinMaxDistinctWithUnknownNDVReturnsEarly() throws SemanticException {
    // HIVE-29625: for COUNT(DISTINCT col), valuesCount = parentCS.getCountDistint().
    // When that NDV is -1 (unknown), the new guard returns early to avoid building
    // a Range with a negative maxValue.
    ColStatistics cs = new ColStatistics("_col0", "bigint");
    HiveConf conf = new HiveConf();

    ColStatistics parentColStats = new ColStatistics("val", "int");
    parentColStats.setNumNulls(0);
    parentColStats.setCountDistint(-1);   // unknown NDV
    parentColStats.setRange(1, 100);

    Statistics parentStats = new Statistics(100, 400, 400, 400);
    parentStats.addToColumnStats(Collections.singletonList(parentColStats));

    ExprNodeColumnDesc colExpr = new ExprNodeColumnDesc(
        TypeInfoFactory.intTypeInfo, "val", "t", false);
    AggregationDesc agg = new AggregationDesc();
    agg.setGenericUDAFName("count");
    agg.setParameters(Collections.singletonList(colExpr));
    agg.setDistinct(true);
    agg.setMode(GenericUDAFEvaluator.Mode.COMPLETE);

    StatsRulesProcFactory.GroupByStatsRule.computeAggregateColumnMinMax(
        cs, conf, agg, "bigint", parentStats);

    assertNull(cs.getRange(), "Range should NOT be set when DISTINCT NDV is unknown");
  }

  @Test
  public void testComputeAggregateColumnMinMaxDistinctWithKnownNDVSetsRange() throws SemanticException {
    // Regression: COUNT(DISTINCT col) with known parentCS.NDV=50 sets Range(0, 50).
    ColStatistics cs = new ColStatistics("_col0", "bigint");
    HiveConf conf = new HiveConf();

    ColStatistics parentColStats = new ColStatistics("val", "int");
    parentColStats.setNumNulls(0);
    parentColStats.setCountDistint(50);
    parentColStats.setRange(1, 100);

    Statistics parentStats = new Statistics(100, 400, 400, 400);
    parentStats.addToColumnStats(Collections.singletonList(parentColStats));

    ExprNodeColumnDesc colExpr = new ExprNodeColumnDesc(
        TypeInfoFactory.intTypeInfo, "val", "t", false);
    AggregationDesc agg = new AggregationDesc();
    agg.setGenericUDAFName("count");
    agg.setParameters(Collections.singletonList(colExpr));
    agg.setDistinct(true);
    agg.setMode(GenericUDAFEvaluator.Mode.COMPLETE);

    StatsRulesProcFactory.GroupByStatsRule.computeAggregateColumnMinMax(
        cs, conf, agg, "bigint", parentStats);

    assertNotNull(cs.getRange(), "Range should be set when DISTINCT NDV is known");
    assertEquals(0L, ((Number) cs.getRange().minValue).longValue());
    assertEquals(50L, ((Number) cs.getRange().maxValue).longValue(),
        "COUNT DISTINCT max should equal the NDV (50)");
  }

  /**
   * Test that JoinStatsRule.updateNumNulls preserves unknown numNulls (-1).
   * With the fix, when numNulls is -1 (unknown), the method returns early without modification.
   * Without the fix, LEFT_OUTER_JOIN would calculate: newNumNulls = oldNumNulls + leftUnmatchedRows = -1 + 100 = 99
   */
  @Test
  public void testUpdateNumNullsPreservesUnknownNumNulls() {
    StatsRulesProcFactory.JoinStatsRule joinStatsRule = new StatsRulesProcFactory.JoinStatsRule();

    // Create ColStatistics with numNulls = -1 (unknown)
    ColStatistics colStats = new ColStatistics("non_join_col", "int");
    colStats.setNumNulls(-1);
    colStats.setCountDistint(100);

    // Create a mock JoinOperator with LEFT_OUTER_JOIN
    JoinCondDesc joinCond = mock(JoinCondDesc.class);
    when(joinCond.getType()).thenReturn(JoinDesc.LEFT_OUTER_JOIN);
    when(joinCond.getRight()).thenReturn(0);  // pos=0 will match getRight()

    JoinDesc joinDesc = mock(JoinDesc.class);
    when(joinDesc.getConds()).thenReturn(new JoinCondDesc[] {joinCond});
    when(joinDesc.getJoinKeys()).thenReturn(new ExprNodeDesc[][] {});

    @SuppressWarnings("unchecked")
    CommonJoinOperator<JoinDesc> mockJop = mock(CommonJoinOperator.class);
    when(mockJop.getConf()).thenReturn(joinDesc);

    // Call updateNumNulls with:
    // - leftUnmatchedRows=100 (without fix, this would be added to -1, giving 99)
    // - pos=0 (matches joinCond.getRight())
    // With the fix: should return early because numNulls is -1
    // Without fix: numNulls would become Math.min(1000, -1 + 100) = 99
    joinStatsRule.updateNumNulls(colStats, 100L, 100L, 1000L, 0L, mockJop);

    // Assert that numNulls is still -1 (unchanged)
    assertEquals(-1L, colStats.getNumNulls(),
        "Unknown numNulls (-1) should be preserved after updateNumNulls");
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("calculateUnmatchedRowsForOuterCases")
  public void testCalculateUnmatchedRowsForOuter(
      String name, long ndv, long distinctUnmatched, long expected) {
    assertCalculateUnmatchedRowsForOuter(ndv, distinctUnmatched, expected);
  }

  private static Stream<Arguments> calculateUnmatchedRowsForOuterCases() {
    return Stream.of(
        Arguments.of("distinctValUnknownReturnsInputRowCount",         -1L,  5L, 100L),
        Arguments.of("distinctValVerifiedZeroReturnsInputRowCount",     0L,  5L, 100L),
        Arguments.of("distinctUnmatchedUnknownReturnsInputRowCount",   10L, -1L, 100L),
        Arguments.of("distinctUnmatchedExceedsReturnsInputRowCount",   10L, 15L, 100L),
        Arguments.of("normalCaseDivides",                              10L,  2L,  20L)
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("computeRowCountAssumingInnerJoinCases")
  public void testComputeRowCountAssumingInnerJoin(String name, long denom, long expected) {
    assertComputeRowCountAssumingInnerJoin(denom, expected);
  }

  private static Stream<Arguments> computeRowCountAssumingInnerJoinCases() {
    return Stream.of(
        Arguments.of("denomPositiveDivides",         10L,   2000L),
        Arguments.of("denomZeroClampsToOne",          0L,  20000L),
        Arguments.of("denomNegativeClampsToOne",     -1L,  20000L)
    );
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("updateColStatsCases")
  public void testUpdateColStats(String name, long initialNdv, long expectedNdv) {
    ColStatistics cs = new ColStatistics("k", "int");
    cs.setCountDistint(initialNdv);
    cs.setNumNulls(0);
    Statistics stats = new Statistics(1000, 8000, 0, 0);
    stats.setColumnStats(Collections.singletonList(cs));

    Map<String, Byte> reversedExprs = new HashMap<>();
    reversedExprs.put("k", (byte) 0);
    JoinCondDesc joinCond = mock(JoinCondDesc.class);
    when(joinCond.getType()).thenReturn(JoinDesc.INNER_JOIN);
    JoinDesc joinDesc = mock(JoinDesc.class);
    when(joinDesc.getReversedExprs()).thenReturn(reversedExprs);
    when(joinDesc.getConds()).thenReturn(new JoinCondDesc[]{joinCond});
    when(joinDesc.getJoinKeys()).thenReturn(new ExprNodeDesc[][]{});
    @SuppressWarnings("unchecked")
    CommonJoinOperator<JoinDesc> jop = mock(CommonJoinOperator.class);
    when(jop.getConf()).thenReturn(joinDesc);
    RowSchema schema = mock(RowSchema.class);
    when(schema.getColumnNames()).thenReturn(Collections.singletonList("k"));
    when(schema.getSignature()).thenReturn(Collections.emptyList());
    when(jop.getSchema()).thenReturn(schema);
    Map<Integer, Long> rowCountParents = new HashMap<>();
    rowCountParents.put(0, 1000L);
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_STATS_JOIN_NDV_READJUSTMENT, false);

    new StatsRulesProcFactory.JoinStatsRule().updateColStats(
        conf, stats, 0L, 0L, 500L, jop, rowCountParents);

    assertEquals(expectedNdv, cs.getCountDistint());
  }

  private static Stream<Arguments> updateColStatsCases() {
    return Stream.of(
        Arguments.of("unknownNdvSkipsMath",     -1L,  -1L),
        Arguments.of("knownNdvScaledByRatio", 100L,  50L)
    );
  }

  private void assertComputeRowCountAssumingInnerJoin(long denom, long expected) {
    StatsRulesProcFactory.JoinStatsRule rule = new StatsRulesProcFactory.JoinStatsRule();
    long actual = rule.computeRowCountAssumingInnerJoin(Arrays.asList(100L, 200L), denom, null);
    assertEquals(expected, actual);
  }

  private void assertCalculateUnmatchedRowsForOuter(long ndv, long distinctUnmatched, long expected) {
    HiveConf conf = new HiveConf();
    ColStatistics cs = new ColStatistics("k", "int");
    cs.setCountDistint(ndv);
    cs.setNumNulls(0);
    Statistics stats = new Statistics(100, 400, 0, 0);
    stats.setColumnStats(Collections.singletonList(cs));

    StatsRulesProcFactory.JoinStatsRule rule = new StatsRulesProcFactory.JoinStatsRule();
    long actual = rule.calculateUnmatchedRowsForOuter(
        conf, 100L, Collections.singletonList("k"), stats, distinctUnmatched);

    assertEquals(expected, actual);
  }
}
