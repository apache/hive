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
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.mapper.PlanMapper;
import org.apache.hadoop.hive.ql.plan.mapper.StatsSource;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.stream.Stream;

import static org.apache.hadoop.hive.ql.optimizer.stats.annotation.StatsRulesProcFactory.FilterStatsRule.extractFloatFromLiteralValue;
import static org.apache.hadoop.hive.ql.optimizer.stats.annotation.StatsRulesProcFactory.JoinStatsRule.computeJoinFactorEstimate;
import static org.apache.hadoop.hive.ql.optimizer.stats.annotation.StatsRulesProcFactory.JoinStatsRule.hasZeroNdvJoinKey;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
  @MethodSource("joinFactorEstimateTestData")
  void testComputeJoinFactorEstimate(String scenario, long maxValue, int numParents, long expected) {
    HiveConf conf = new HiveConf();
    assertEquals(expected, computeJoinFactorEstimate(conf, maxValue, numParents));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("hasZeroNdvJoinKeyTestData")
  void testHasZeroNdvJoinKey(String scenario, Map<Integer, List<String>> joinKeys,
      Map<Integer, Statistics> joinStats, boolean expected) {
    assertEquals(expected, hasZeroNdvJoinKey(joinKeys, joinStats));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testJoinStatsRuleWithZeroNdv() throws SemanticException {
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
    AnnotateStatsProcCtx ctx = new AnnotateStatsProcCtx(pctx);

    Statistics leftStats = new Statistics(1000L, 10000L, 0L, 0L);
    leftStats.setBasicStatsState(Statistics.State.COMPLETE);
    leftStats.setColumnStatsState(Statistics.State.COMPLETE);
    ColStatistics leftColStats = new ColStatistics("KEY.key", "int");
    leftColStats.setCountDistint(0L);
    leftColStats.setNumNulls(0L);
    leftStats.addToColumnStats(Collections.singletonList(leftColStats));

    Statistics rightStats = new Statistics(500L, 5000L, 0L, 0L);
    rightStats.setBasicStatsState(Statistics.State.COMPLETE);
    rightStats.setColumnStatsState(Statistics.State.COMPLETE);
    ColStatistics rightColStats = new ColStatistics("KEY.key", "int");
    rightColStats.setCountDistint(100L);
    rightColStats.setNumNulls(0L);
    rightStats.addToColumnStats(Collections.singletonList(rightColStats));

    ReduceSinkOperator leftRsOp = mock(ReduceSinkOperator.class);
    ReduceSinkDesc leftRsDesc = mock(ReduceSinkDesc.class);
    when(leftRsOp.getStatistics()).thenReturn(leftStats);
    when(leftRsOp.getConf()).thenReturn(leftRsDesc);
    when(leftRsDesc.getOutputKeyColumnNames()).thenReturn(Arrays.asList("key"));

    ReduceSinkOperator rightRsOp = mock(ReduceSinkOperator.class);
    ReduceSinkDesc rightRsDesc = mock(ReduceSinkDesc.class);
    when(rightRsOp.getStatistics()).thenReturn(rightStats);
    when(rightRsOp.getConf()).thenReturn(rightRsDesc);
    when(rightRsDesc.getOutputKeyColumnNames()).thenReturn(Arrays.asList("key"));

    List<Operator<? extends OperatorDesc>> parents = new ArrayList<>();
    parents.add(leftRsOp);
    parents.add(rightRsOp);

    JoinOperator joinOp = mock(JoinOperator.class);
    JoinDesc joinDesc = mock(JoinDesc.class);
    JoinCondDesc joinCond = new JoinCondDesc(0, 1, JoinDesc.INNER_JOIN);
    when(joinOp.getParentOperators()).thenReturn(parents);
    when(joinOp.getConf()).thenReturn(joinDesc);
    when(joinDesc.getConds()).thenReturn(new JoinCondDesc[]{joinCond});

    RowSchema rowSchema = mock(RowSchema.class);
    ColumnInfo colInfo = new ColumnInfo("key", TypeInfoFactory.intTypeInfo, "", false);
    when(rowSchema.getSignature()).thenReturn(Arrays.asList(colInfo));
    when(joinOp.getSchema()).thenReturn(rowSchema);

    final Statistics[] capturedStats = new Statistics[1];
    doAnswer(invocation -> {
      capturedStats[0] = invocation.getArgument(0);
      return null;
    }).when(joinOp).setStatistics(any(Statistics.class));

    StatsRulesProcFactory.JoinStatsRule rule = new StatsRulesProcFactory.JoinStatsRule();
    rule.process(joinOp, new Stack<>(), ctx);

    assertNotNull(capturedStats[0], "Statistics should have been set on join operator");
    assertEquals(Statistics.State.COMPLETE, capturedStats[0].getColumnStatsState(),
        "Column stats state should be COMPLETE when using NDV=0 fallback");
    assertEquals(1100L, capturedStats[0].getNumRows(),
        "Row count should use joinFactor heuristic: max(1000,500) * 1.1 = 1100");
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

  static Stream<Arguments> joinFactorEstimateTestData() {
    return Stream.of(
        Arguments.of("SingleParent", 1000L, 1, 1100L),
        Arguments.of("TwoParents", 1000L, 2, 1100L),
        Arguments.of("ThreeParents", 1000L, 3, 2200L),
        Arguments.of("Overflow", Long.MAX_VALUE, 3, Long.MAX_VALUE)
    );
  }

  static Stream<Arguments> hasZeroNdvJoinKeyTestData() {
    // Helper to create Statistics with given row count
    java.util.function.Function<Long, Statistics> statsWithRows = rows -> {
      Statistics s = new Statistics(rows, 100L, 0L, 0L);
      s.setColumnStatsState(Statistics.State.COMPLETE);
      return s;
    };

    // Helper to create ColStatistics with given NDV
    java.util.function.BiConsumer<Statistics, Long> addColWithNdv = (stats, ndv) -> {
      ColStatistics cs = new ColStatistics("col", "int");
      cs.setCountDistint(ndv);
      stats.addToColumnStats(Collections.singletonList(cs));
    };

    // Empty joinKeys
    Map<Integer, List<String>> emptyKeys = new HashMap<>();
    Map<Integer, Statistics> emptyStats = new HashMap<>();

    // Single table with <=1 row (should skip)
    Map<Integer, List<String>> singleRowKeys = new HashMap<>();
    singleRowKeys.put(0, Arrays.asList("col"));
    Map<Integer, Statistics> singleRowStats = new HashMap<>();
    Statistics singleRowStat = statsWithRows.apply(1L);
    addColWithNdv.accept(singleRowStat, 0L);
    singleRowStats.put(0, singleRowStat);

    // Table with rows but no zero NDV
    Map<Integer, List<String>> noZeroNdvKeys = new HashMap<>();
    noZeroNdvKeys.put(0, Arrays.asList("col"));
    Map<Integer, Statistics> noZeroNdvStats = new HashMap<>();
    Statistics noZeroNdvStat = statsWithRows.apply(100L);
    addColWithNdv.accept(noZeroNdvStat, 50L);
    noZeroNdvStats.put(0, noZeroNdvStat);

    // Table with rows and zero NDV
    Map<Integer, List<String>> zeroNdvKeys = new HashMap<>();
    zeroNdvKeys.put(0, Arrays.asList("col"));
    Map<Integer, Statistics> zeroNdvStats = new HashMap<>();
    Statistics zeroNdvStat = statsWithRows.apply(100L);
    addColWithNdv.accept(zeroNdvStat, 0L);
    zeroNdvStats.put(0, zeroNdvStat);

    // Column not found (null ColStatistics)
    Map<Integer, List<String>> nullColKeys = new HashMap<>();
    nullColKeys.put(0, Arrays.asList("nonexistent"));
    Map<Integer, Statistics> nullColStats = new HashMap<>();
    nullColStats.put(0, statsWithRows.apply(100L));

    // Two tables: first has non-zero NDV, second has zero NDV
    Map<Integer, List<String>> mixedKeys = new HashMap<>();
    mixedKeys.put(0, Arrays.asList("col"));
    mixedKeys.put(1, Arrays.asList("col"));
    Map<Integer, Statistics> mixedStats = new HashMap<>();
    Statistics mixedStat0 = statsWithRows.apply(100L);
    addColWithNdv.accept(mixedStat0, 50L);
    mixedStats.put(0, mixedStat0);
    Statistics mixedStat1 = statsWithRows.apply(100L);
    addColWithNdv.accept(mixedStat1, 0L);
    mixedStats.put(1, mixedStat1);

    // Two tables: first has zero NDV, second has non-zero NDV
    Map<Integer, List<String>> firstZeroKeys = new HashMap<>();
    firstZeroKeys.put(0, Arrays.asList("col"));
    firstZeroKeys.put(1, Arrays.asList("col"));
    Map<Integer, Statistics> firstZeroStats = new HashMap<>();
    Statistics firstZeroStat0 = statsWithRows.apply(100L);
    addColWithNdv.accept(firstZeroStat0, 0L);
    firstZeroStats.put(0, firstZeroStat0);
    Statistics firstZeroStat1 = statsWithRows.apply(100L);
    addColWithNdv.accept(firstZeroStat1, 50L);
    firstZeroStats.put(1, firstZeroStat1);

    // Three tables: first two have non-zero NDV, third has zero NDV
    Map<Integer, List<String>> threeTableKeys = new HashMap<>();
    threeTableKeys.put(0, Arrays.asList("col"));
    threeTableKeys.put(1, Arrays.asList("col"));
    threeTableKeys.put(2, Arrays.asList("col"));
    Map<Integer, Statistics> threeTableStats = new HashMap<>();
    Statistics threeStat0 = statsWithRows.apply(100L);
    addColWithNdv.accept(threeStat0, 50L);
    threeTableStats.put(0, threeStat0);
    Statistics threeStat1 = statsWithRows.apply(100L);
    addColWithNdv.accept(threeStat1, 25L);
    threeTableStats.put(1, threeStat1);
    Statistics threeStat2 = statsWithRows.apply(100L);
    addColWithNdv.accept(threeStat2, 0L);
    threeTableStats.put(2, threeStat2);

    // Two tables: first has 1 row (skipped), second has zero NDV
    Map<Integer, List<String>> skipFirstKeys = new HashMap<>();
    skipFirstKeys.put(0, Arrays.asList("col"));
    skipFirstKeys.put(1, Arrays.asList("col"));
    Map<Integer, Statistics> skipFirstStats = new HashMap<>();
    Statistics skipStat0 = statsWithRows.apply(1L);
    addColWithNdv.accept(skipStat0, 0L);
    skipFirstStats.put(0, skipStat0);
    Statistics skipStat1 = statsWithRows.apply(100L);
    addColWithNdv.accept(skipStat1, 0L);
    skipFirstStats.put(1, skipStat1);

    return Stream.of(
        Arguments.of("EmptyJoinKeys", emptyKeys, emptyStats, false),
        Arguments.of("SingleRowTable", singleRowKeys, singleRowStats, false),
        Arguments.of("NoZeroNdv", noZeroNdvKeys, noZeroNdvStats, false),
        Arguments.of("HasZeroNdv", zeroNdvKeys, zeroNdvStats, true),
        Arguments.of("NullColStatistics", nullColKeys, nullColStats, false),
        Arguments.of("TwoTablesFirstHasZero", firstZeroKeys, firstZeroStats, true),
        Arguments.of("TwoTablesSecondHasZero", mixedKeys, mixedStats, true),
        Arguments.of("ThreeTablesThirdHasZero", threeTableKeys, threeTableStats, true),
        Arguments.of("FirstSkippedSecondHasZero", skipFirstKeys, skipFirstStats, true)
    );
  }
}
