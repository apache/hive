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
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;

import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.plan.JoinCondDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.apache.hadoop.hive.ql.optimizer.stats.annotation.StatsRulesProcFactory.FilterStatsRule.extractFloatFromLiteralValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

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
  public void testComputeAggregateColumnMinMaxWithUnknownNumNulls() throws Exception {
    // Get the private static method via reflection
    Class<?> groupByStatsRuleClass = null;
    for (Class<?> innerClass : StatsRulesProcFactory.class.getDeclaredClasses()) {
      if (innerClass.getSimpleName().equals("GroupByStatsRule")) {
        groupByStatsRuleClass = innerClass;
        break;
      }
    }
    assertNotNull("GroupByStatsRule class not found", groupByStatsRuleClass);

    Method method = groupByStatsRuleClass.getDeclaredMethod(
        "computeAggregateColumnMinMax",
        ColStatistics.class, HiveConf.class, AggregationDesc.class, String.class, Statistics.class);
    method.setAccessible(true);

    // Create output ColStatistics for the COUNT result
    ColStatistics cs = new ColStatistics("_col0", "bigint");

    // Create HiveConf
    HiveConf conf = new HiveConf();

    // Create parent column stats with numNulls=-1 (unknown) and Range(1, 100)
    ColStatistics parentColStats = new ColStatistics("val", "int");
    parentColStats.setNumNulls(-1);  // unknown numNulls - this is what we're testing
    parentColStats.setCountDistint(100);
    parentColStats.setRange(1, 100);

    // Create parent Statistics with 100 rows
    Statistics parentStats = new Statistics(100, 400, 400, 400);
    parentStats.addToColumnStats(Collections.singletonList(parentColStats));

    // Create ExprNodeColumnDesc for the "val" column
    ExprNodeColumnDesc colExpr = new ExprNodeColumnDesc(
        TypeInfoFactory.intTypeInfo, "val", "t", false);

    // Create AggregationDesc for COUNT(val) using no-arg constructor to avoid NPE
    AggregationDesc agg = new AggregationDesc();
    agg.setGenericUDAFName("count");
    agg.setParameters(Collections.singletonList(colExpr));
    agg.setDistinct(false);
    agg.setMode(GenericUDAFEvaluator.Mode.COMPLETE);

    // Call the method
    method.invoke(null, cs, conf, agg, "bigint", parentStats);

    // Verify: With the fix, COUNT Range should be (0, 100)
    // numNulls=-1 is treated as 0, so valuesCount = 100 - 0 = 100
    // Without the fix, valuesCount = 100 - (-1) = 101 (WRONG)
    assertNotNull("Range should be set on COUNT column", cs.getRange());
    assertEquals("COUNT min should be 0", 0L, ((Number) cs.getRange().minValue).longValue());
    assertEquals("COUNT max should be 100 (numRows), not 101",
        100L, ((Number) cs.getRange().maxValue).longValue());
  }

  @Test
  public void testComputeAggregateColumnMinMaxWithKnownNumNulls() throws Exception {
    // Get the private static method via reflection
    Class<?> groupByStatsRuleClass = null;
    for (Class<?> innerClass : StatsRulesProcFactory.class.getDeclaredClasses()) {
      if (innerClass.getSimpleName().equals("GroupByStatsRule")) {
        groupByStatsRuleClass = innerClass;
        break;
      }
    }
    Method method = groupByStatsRuleClass.getDeclaredMethod(
        "computeAggregateColumnMinMax",
        ColStatistics.class, HiveConf.class, AggregationDesc.class, String.class, Statistics.class);
    method.setAccessible(true);

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

    method.invoke(null, cs, conf, agg, "bigint", parentStats);

    // With known numNulls=20, valuesCount = 100 - 20 = 80
    assertNotNull("Range should be set", cs.getRange());
    assertEquals(0L, ((Number) cs.getRange().minValue).longValue());
    assertEquals("COUNT max should be 80 (numRows - numNulls)",
        80L, ((Number) cs.getRange().maxValue).longValue());
  }

  /**
   * Test that JoinStatsRule.updateNumNulls preserves unknown numNulls (-1).
   * With the fix, when numNulls is -1 (unknown), the method returns early without modification.
   * Without the fix, LEFT_OUTER_JOIN would calculate: newNumNulls = oldNumNulls + leftUnmatchedRows = -1 + 100 = 99
   */
  @Test
  public void testUpdateNumNullsPreservesUnknownNumNulls() throws Exception {
    // Get the private JoinStatsRule inner class
    Class<?> joinStatsRuleClass = null;
    for (Class<?> innerClass : StatsRulesProcFactory.class.getDeclaredClasses()) {
      if (innerClass.getSimpleName().equals("JoinStatsRule")) {
        joinStatsRuleClass = innerClass;
        break;
      }
    }
    assertNotNull("JoinStatsRule class not found", joinStatsRuleClass);

    // Create an instance of JoinStatsRule
    Constructor<?> ctor = joinStatsRuleClass.getDeclaredConstructor();
    ctor.setAccessible(true);
    Object joinStatsRule = ctor.newInstance();

    // Get the updateNumNulls method
    Method updateNumNulls = joinStatsRuleClass.getDeclaredMethod("updateNumNulls",
        ColStatistics.class, long.class, long.class, long.class, long.class,
        CommonJoinOperator.class);
    updateNumNulls.setAccessible(true);

    // Create ColStatistics with numNulls = -1 (unknown)
    // Use a column name that won't be a join key
    ColStatistics colStats = new ColStatistics("non_join_col", "int");
    colStats.setNumNulls(-1);
    colStats.setCountDistint(100);

    // Create a mock JoinOperator with LEFT_OUTER_JOIN
    JoinCondDesc joinCond = mock(JoinCondDesc.class);
    when(joinCond.getType()).thenReturn(JoinDesc.LEFT_OUTER_JOIN);
    when(joinCond.getRight()).thenReturn(0);  // pos=0 will match getRight()

    JoinDesc joinDesc = mock(JoinDesc.class);
    when(joinDesc.getConds()).thenReturn(new JoinCondDesc[] {joinCond});
    // Return empty join keys so our column won't be a join key
    when(joinDesc.getJoinKeys()).thenReturn(new ExprNodeDesc[][] {});

    @SuppressWarnings("unchecked")
    CommonJoinOperator<JoinDesc> mockJop = mock(CommonJoinOperator.class);
    when(mockJop.getConf()).thenReturn(joinDesc);

    // Call updateNumNulls with:
    // - leftUnmatchedRows=100 (without fix, this would be added to -1, giving 99)
    // - pos=0 (matches joinCond.getRight())
    // With the fix: should return early because numNulls is -1
    // Without fix: numNulls would become Math.min(1000, -1 + 100) = 99
    updateNumNulls.invoke(joinStatsRule, colStats, 100L, 100L, 1000L, 0L, mockJop);

    // Assert that numNulls is still -1 (unchanged)
    assertEquals("Unknown numNulls (-1) should be preserved after updateNumNulls",
        -1L, colStats.getNumNulls());
  }
}
