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
import org.apache.hadoop.hive.metastore.StatisticsTestUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualOrLessThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPGreaterThan;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPLessThan;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

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

    Assert.assertEquals(0, numRows);
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
    Assert.assertEquals((VALUES.length + numNulls) / 3, numRows);

    // empty KLL array is not valid either
    stats.getColumnStats().get(0).setHistogram(new byte[0]);
    numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    // when no KLL it defaults to 1/3 of the number of rows
    Assert.assertEquals((VALUES.length + numNulls) / 3, numRows);
  }

  @Test
  public void testComparisonRowCountLessThan() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);

    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(3)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    Assert.assertEquals(8, numRows);
  }

  @Test
  public void testComparisonRowCountLessThanMin() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);

    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(1)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    Assert.assertEquals(0, numRows);
  }

  @Test
  public void testComparisonRowCountLessThanBelowMin() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);

    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(0)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    Assert.assertEquals(0, numRows);
  }

  @Test
  public void testComparisonRowCountLessThanMax() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);

    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(7)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    Assert.assertEquals(12, numRows);
  }

  @Test
  public void testComparisonRowCountLessThanAboveMax() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);

    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(8)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    Assert.assertEquals(13, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrLessThan() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(3)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    Assert.assertEquals(9, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrLessThanMin() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(1)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    Assert.assertEquals(1, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrLessThanBelowMin() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(0)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    Assert.assertEquals(0, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrLessThanMax() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(7)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    Assert.assertEquals(13, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrLessThanAboveMax() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrLessThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(8)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    Assert.assertEquals(13, numRows);
  }

  @Test
  public void testComparisonRowCountGreaterThan() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(5)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    Assert.assertEquals(2, numRows);
  }

  @Test
  public void testComparisonRowCountGreaterThanMin() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(1)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    Assert.assertEquals(12, numRows);
  }

  @Test
  public void testComparisonRowCountGreaterThanBelowMin() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(0)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    Assert.assertEquals(13, numRows);
  }

  @Test
  public void testComparisonRowCountGreaterThanMax() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(7)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    Assert.assertEquals(0, numRows);
  }

  @Test
  public void testComparisonRowCountGreaterThanAboveMax() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(8)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    Assert.assertEquals(0, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrGreaterThan() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(5)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    Assert.assertEquals(3, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrGreaterThanMin() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(1)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    Assert.assertEquals(13, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrGreaterThanBelowMin() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(0)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    Assert.assertEquals(13, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrGreaterThanMax() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(7)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    Assert.assertEquals(1, numRows);
  }

  @Test
  public void testComparisonRowCountEqualOrGreaterThanBeyondMax() throws SemanticException {
    long numNulls = 2;
    Statistics stats = createStatistics(VALUES, numNulls);
    ExprNodeDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo,
        new GenericUDFOPEqualOrGreaterThan(), Arrays.asList(COL_EXPR, createExprNodeConstantDesc(8)));
    long numRows = new StatsRulesProcFactory.FilterStatsRule().evaluateExpression(
        stats, exprNodeDesc, STATS_PROC_CTX, Collections.emptyList(), null, VALUES.length + numNulls);

    Assert.assertEquals(0, numRows);
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

    Assert.assertEquals(2, numRows);
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

    Assert.assertEquals(2, numRows);
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
}
