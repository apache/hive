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

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.StatisticsTestUtils;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveBetween;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Collections;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.DOUBLE;
import static org.apache.calcite.sql.type.SqlTypeName.FLOAT;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.SMALLINT;
import static org.apache.calcite.sql.type.SqlTypeName.TINYINT;
import static org.apache.hadoop.hive.ql.optimizer.calcite.stats.FilterSelectivityEstimator.betweenSelectivity;
import static org.apache.hadoop.hive.ql.optimizer.calcite.stats.FilterSelectivityEstimator.greaterThanOrEqualSelectivity;
import static org.apache.hadoop.hive.ql.optimizer.calcite.stats.FilterSelectivityEstimator.greaterThanSelectivity;
import static org.apache.hadoop.hive.ql.optimizer.calcite.stats.FilterSelectivityEstimator.isHistogramAvailable;
import static org.apache.hadoop.hive.ql.optimizer.calcite.stats.FilterSelectivityEstimator.lessThanOrEqualSelectivity;
import static org.apache.hadoop.hive.ql.optimizer.calcite.stats.FilterSelectivityEstimator.lessThanSelectivity;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestFilterSelectivityEstimator {

  private static final SqlBinaryOperator GT = SqlStdOperatorTable.GREATER_THAN;
  private static final SqlBinaryOperator GE = SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
  private static final SqlBinaryOperator LT = SqlStdOperatorTable.LESS_THAN;
  private static final SqlBinaryOperator LE = SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
  private static final SqlOperator BETWEEN = HiveBetween.INSTANCE;

  private static final float[] VALUES = { 1, 2, 2, 2, 2, 2, 2, 2, 3, 4, 5, 6, 7 };
  private static final float[] VALUES2 = {
      // rounding for DECIMAL(3,1)
      // -99.95f and its two predecessors and successors
      -99.95001f, -99.950005f, -99.95f, -99.94999f, -99.94998f,
      // some values
      0f, 1f, 10f,
      // rounding for DECIMAL(3,1)
      // 99.95f and its two predecessors and successors
      99.94998f, 99.94999f, 99.95f, 99.950005f, 99.95001f,
      // 100f and its two predecessors and successors
      99.999985f, 99.99999f, 100f, 100.00001f, 100.000015f,
      // 100.05f and its two predecessors and successors
      100.04999f, 100.049995f, 100.05f, 100.05001f, 100.05002f,
      // some values
      1_000f, 10_000f, 100_000f, 1_000_000f, 10_000_000f };

  /**
   * Both dates and timestamps are converted to epoch seconds.
   * <p>
   * See {@link org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUnixTimeStamp#evaluate(GenericUDF.DeferredObject[])}.
   */
  private static final float[] VALUES_TIME = {
      timestamp("2020-11-01"), timestamp("2020-11-02"), timestamp("2020-11-03"), timestamp("2020-11-04"),
      timestamp("2020-11-05T11:23:45Z"), timestamp("2020-11-06"), timestamp("2020-11-07") };

  private static final KllFloatsSketch KLL = StatisticsTestUtils.createKll(VALUES);
  private static final KllFloatsSketch KLL2 = StatisticsTestUtils.createKll(VALUES2);
  private static final KllFloatsSketch KLL_TIME = StatisticsTestUtils.createKll(VALUES_TIME);
  private static final float DELTA = 1e-7f;
  private static final RexBuilder REX_BUILDER = new RexBuilder(new JavaTypeFactoryImpl(new HiveTypeSystemImpl()));
  private static final RelDataTypeFactory TYPE_FACTORY = REX_BUILDER.getTypeFactory();

  private static RelOptCluster relOptCluster;
  private static RexNode intMinus1;
  private static RexNode int0;
  private static RexNode int1;
  private static RexNode int2;
  private static RexNode int3;
  private static RexNode int4;
  private static RexNode int5;
  private static RexNode int7;
  private static RexNode int8;
  private static RexNode int10;
  private static RexNode int11;
  private static RelDataType tableType;
  private static RexNode inputRef0;
  private static RexNode boolFalse;
  private static RexNode boolTrue;

  @Mock
  private RelOptSchema schemaMock;
  @Mock
  private RelOptHiveTable tableMock;
  @Mock
  private RelMetadataQuery mq;

  private ColStatistics stats;
  private RelNode scan;
  private RexNode currentInputRef;
  private int currentValuesSize;

  @BeforeClass
  public static void beforeClass() {
    RelDataType integerType = TYPE_FACTORY.createSqlType(INTEGER);
    intMinus1 = REX_BUILDER.makeLiteral(-1, integerType, true);
    int0 = REX_BUILDER.makeLiteral(0, integerType, true);
    int1 = REX_BUILDER.makeLiteral(1, integerType, true);
    int2 = REX_BUILDER.makeLiteral(2, integerType, true);
    int3 = REX_BUILDER.makeLiteral(3, integerType, true);
    int4 = REX_BUILDER.makeLiteral(4, integerType, true);
    int5 = REX_BUILDER.makeLiteral(5, integerType, true);
    int7 = REX_BUILDER.makeLiteral(7, integerType, true);
    int8 = REX_BUILDER.makeLiteral(8, integerType, true);
    int10 = REX_BUILDER.makeLiteral(10, integerType, true);
    int11 = REX_BUILDER.makeLiteral(11, integerType, true);
    boolFalse = REX_BUILDER.makeLiteral(false, TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN), true);
    boolTrue = REX_BUILDER.makeLiteral(true, TYPE_FACTORY.createSqlType(SqlTypeName.BOOLEAN), true);
    RelDataTypeFactory.Builder b = new RelDataTypeFactory.Builder(TYPE_FACTORY);
    b.add("f_numeric", decimalType(38, 25));
    b.add("f_timestamp", SqlTypeName.TIMESTAMP);
    b.add("f_date", SqlTypeName.DATE).build();
    tableType = b.build();

    RelOptPlanner planner = CalcitePlanner.createPlanner(new HiveConf());
    relOptCluster = RelOptCluster.create(planner, REX_BUILDER);
  }

  private static ColStatistics.Range rangeOf(float[] values) {
    float min = Float.MAX_VALUE, max = -Float.MAX_VALUE;
    for (float v : values) {
      min = Math.min(min, v);
      max = Math.max(max, v);
    }
    return new ColStatistics.Range(min, max);
  }

  @Before
  public void before() {
    currentValuesSize = VALUES.length;
    doReturn(tableType).when(tableMock).getRowType();
    when(tableMock.getRowCount()).thenAnswer(a -> (double) currentValuesSize);

    RelBuilder relBuilder = HiveRelFactories.HIVE_BUILDER.create(relOptCluster, schemaMock);
    HiveTableScan tableScan =
        new HiveTableScan(relOptCluster, relOptCluster.traitSetOf(HiveRelNode.CONVENTION), tableMock, "table", null,
            false, false);
    scan = relBuilder.push(tableScan).build();
    inputRef0 = REX_BUILDER.makeInputRef(scan, 0);
    currentInputRef = inputRef0;

    stats = new ColStatistics();
    stats.setHistogram(KLL.toByteArray());
    stats.setRange(rangeOf(VALUES));
  }

  /**
   * Note: call this method only at the beginning of a test method.
   */
  private void useFieldWithValues(String fieldname, float[] values, KllFloatsSketch sketch) {
    currentValuesSize = values.length;
    stats.setHistogram(sketch.toByteArray());
    stats.setRange(rangeOf(values));
    int fieldIndex = scan.getRowType().getFieldNames().indexOf(fieldname);
    currentInputRef = REX_BUILDER.makeInputRef(scan, fieldIndex);
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(fieldIndex));
  }

  @Test
  public void testIsHistogramAvailableWhenAvailable() {
    ColStatistics colStatistics = new ColStatistics();
    colStatistics.setHistogram(KLL.toByteArray());
    Assert.assertTrue(isHistogramAvailable(colStatistics));
  }

  @Test
  public void testIsHistogramAvailableWhenNullStatistics() {
    Assert.assertFalse(isHistogramAvailable(null));
  }

  @Test
  public void testIsHistogramAvailableWhenNullHistogram() {
    Assert.assertFalse(isHistogramAvailable(new ColStatistics()));
  }

  @Test
  public void testIsHistogramAvailableWhenEmptyArray() {
    ColStatistics colStatistics = new ColStatistics();
    colStatistics.setHistogram(new byte[0]);
    Assert.assertFalse(isHistogramAvailable(colStatistics));
  }

  @Test
  public void testLessThanSelectivity() {
    Assert.assertEquals(0.6153846153846154, lessThanSelectivity(KLL, 3), DELTA);
  }

  @Test
  public void testLessThanSelectivityWhenLowerThanMin() {
    Assert.assertEquals(0, lessThanSelectivity(KLL, 0), DELTA);
  }

  @Test
  public void testLessThanSelectivityWhenHigherThanMax() {
    Assert.assertEquals(1, lessThanSelectivity(KLL, 10), DELTA);
  }

  @Test
  public void testLessThanOrEqualSelectivity() {
    Assert.assertEquals(0.6923076923076923, lessThanOrEqualSelectivity(KLL, 3), DELTA);
  }

  @Test
  public void testLessThanOrEqualSelectivityWhenLowerThanMin() {
    Assert.assertEquals(0, lessThanOrEqualSelectivity(KLL, 0), DELTA);
  }

  @Test
  public void testLessThanOrEqualSelectivityWhenHigherThanMax() {
    Assert.assertEquals(1, lessThanOrEqualSelectivity(KLL, 10), DELTA);
  }

  @Test
  public void testGreaterThanSelectivity() {
    Assert.assertEquals(0.3076923076923077, greaterThanSelectivity(KLL, 3), DELTA);
  }

  @Test
  public void testGreaterThanSelectivityWhenLowerThanMin() {
    Assert.assertEquals(1, greaterThanSelectivity(KLL, 0), DELTA);
  }

  @Test
  public void testGreaterThanSelectivityWhenHigherThanMax() {
    Assert.assertEquals(0, greaterThanSelectivity(KLL, 10), DELTA);
  }

  @Test
  public void testGreaterThanOrEqualSelectivity() {
    Assert.assertEquals(0.3846153846153846, greaterThanOrEqualSelectivity(KLL, 3), DELTA);
  }

  @Test
  public void testGreaterThanOrEqualSelectivityWhenLowerThanMin() {
    Assert.assertEquals(1, greaterThanOrEqualSelectivity(KLL, 0), DELTA);
  }

  @Test
  public void testGreaterThanOrEqualSelectivityWhenHigherThanMax() {
    Assert.assertEquals(0, greaterThanOrEqualSelectivity(KLL, 10), DELTA);
  }

  @Test
  public void testBetweenSelectivity() {
    Assert.assertEquals(0.6923076923076923, betweenSelectivity(KLL, 1, 3), DELTA);
  }

  @Test
  public void testBetweenSelectivityFromMinToMax() {
    Assert.assertEquals(1, betweenSelectivity(KLL, 1, 7), DELTA);
  }

  @Test
  public void testBetweenSelectivityFromLowerThanMinToHigherThanMax() {
    Assert.assertEquals(1, betweenSelectivity(KLL, 0, 8), DELTA);
  }

  @Test
  public void testBetweenSelectivityLeftLowerThanMin() {
    Assert.assertEquals(0.6923076923076923, betweenSelectivity(KLL, 0, 3), DELTA);
  }

  @Test
  public void testBetweenSelectivityRightLowerThanMin() {
    Assert.assertEquals(0, betweenSelectivity(KLL, -1, 0), DELTA);
  }

  @Test
  public void testBetweenSelectivityLeftHigherThanMax() {
    Assert.assertEquals(0, betweenSelectivity(KLL, 10, 11), DELTA);
  }

  @Test
  public void testBetweenSelectivityLeftLowerThanRight() {
    Assert.assertEquals(0, betweenSelectivity(KLL, 4, 2), DELTA);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testBetweenSelectivityLeftEqualsRight_KO() {
    betweenSelectivity(KLL, 2, 2);
  }

  @Test
  public void testComputeRangePredicateSelectivityWhenNoStats() {
    RexNode filter = REX_BUILDER.makeCall(SqlStdOperatorTable.LESS_THAN, inputRef0, int3);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    // defaults to 1/3 in the absence of stats
    Assert.assertEquals(0.3333333333333333, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityLessThan() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(SqlStdOperatorTable.LESS_THAN, inputRef0, int3);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(0.6153846153846154, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityLessThanWhenLowerThanMin() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(SqlStdOperatorTable.LESS_THAN, inputRef0, int0);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(0, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityLessThanWhenHigherThanMax() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(SqlStdOperatorTable.LESS_THAN, inputRef0, int10);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(1, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityLessThanOrEqual() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, inputRef0, int3);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(0.6923076923076923, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityLessThanOrEqualWhenLowerThanMin() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, inputRef0, int0);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(0, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityLessThanOrEqualWhenHigherThanMax() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, inputRef0, int10);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(1, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityGreaterThan() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(SqlStdOperatorTable.GREATER_THAN, inputRef0, int3);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(0.3076923076923077, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityGreaterThanWhenLowerThanMin() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(SqlStdOperatorTable.GREATER_THAN, inputRef0, int0);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(1, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityGreaterThanWhenHigherThanMax() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(SqlStdOperatorTable.GREATER_THAN, inputRef0, int10);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(0, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityGreaterThanOrEqual() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, inputRef0, int3);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(0.38461538461538464, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityGreaterThanOrEqualWhenLowerThanMin() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, inputRef0, int0);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(1, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityGreaterThanOrEqualWhenHigherThanMax() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, inputRef0, int10);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(0, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityBetween() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(HiveBetween.INSTANCE, boolFalse, inputRef0, int1, int3);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(0.6923076923076923, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityBetweenFromMinToMax() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(HiveBetween.INSTANCE, boolFalse, inputRef0, int1, int7);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(1, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityBetweenFromLowerThanMinToHigherThanMax() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(HiveBetween.INSTANCE, boolFalse, inputRef0, int0, int8);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(1, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityBetweenLeftLowerThanMin() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(HiveBetween.INSTANCE, boolFalse, inputRef0, int0, int3);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(0.6923076923076923, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityBetweenRightLowerThanMin() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(HiveBetween.INSTANCE, boolFalse, inputRef0, intMinus1, int0);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(0, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityBetweenLeftHigherThanMax() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(HiveBetween.INSTANCE, boolFalse, inputRef0, int10, int11);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(0, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityBetweenLeftLowerThanRight() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(HiveBetween.INSTANCE, boolFalse, inputRef0, int4, int2);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(0, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityBetweenLeftEqualsRight() {
    verify(tableMock, never()).getColStat(any());
    doReturn(10.0).when(mq).getDistinctRowCount(scan, ImmutableBitSet.of(0), REX_BUILDER.makeLiteral(true));
    RexNode filter = REX_BUILDER.makeCall(HiveBetween.INSTANCE, boolFalse, inputRef0, int3, int3);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    // this is what FilterSelectivityEstimator returns for a generic "function" based on NDV values, in this case 1 / 10
    Assert.assertEquals(0.1, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityNotBetween() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(HiveBetween.INSTANCE, boolTrue, inputRef0, int3, int5);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(0.7692307692307693, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityNotBetweenLowerThanMinHigherThanMax() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(HiveBetween.INSTANCE, boolTrue, inputRef0, int0, int10);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(0, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityNotBetweenRightLowerThanLeft() {
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(HiveBetween.INSTANCE, boolTrue, inputRef0, int5, int3);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(1, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityNotBetweenLeftEqualsRight() {
    verify(tableMock, never()).getColStat(any());
    RexNode filter = REX_BUILDER.makeCall(HiveBetween.INSTANCE, boolTrue, inputRef0, int3, int3);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(1, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityGreaterThanWithNULLS() {
    doReturn((double) 20).when(tableMock).getRowCount();
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(SqlStdOperatorTable.GREATER_THAN, inputRef0, int3);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(0.2, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityGreaterThanOrEqualWithNULLS() {
    doReturn((double) 20).when(tableMock).getRowCount();
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, inputRef0, int3);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(0.25, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityLessThanWithNULLS() {
    doReturn((double) 20).when(tableMock).getRowCount();
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(SqlStdOperatorTable.LESS_THAN, inputRef0, int3);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(0.4, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityLessThanOrEqualWithNULLS() {
    doReturn((double) 20).when(tableMock).getRowCount();
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, inputRef0, int3);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(0.45, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityBetweenWithNULLS() {
    doReturn((double) 20).when(tableMock).getRowCount();
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(HiveBetween.INSTANCE, boolFalse, inputRef0, int1, int3);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(0.45, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityNotBetweenWithNULLS() {
    doReturn((double) 20).when(tableMock).getRowCount();
    doReturn(Collections.singletonList(stats)).when(tableMock).getColStat(Collections.singletonList(0));
    RexNode filter = REX_BUILDER.makeCall(HiveBetween.INSTANCE, boolTrue, inputRef0, int1, int3);
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    // only the values 4, 5, 6, 7 fulfill the condition NOT BETWEEN 1 AND 3
    // (the NULL values do not fulfill the condition)
    Assert.assertEquals(0.2, estimator.estimateSelectivity(filter), DELTA);
  }

  @Test
  public void testComputeRangePredicateSelectivityWithCast() {
    useFieldWithValues("f_numeric", VALUES, KLL);
    checkSelectivity(3 / 13.f, ge(cast("f_numeric", TINYINT), int5));
    checkSelectivity(10 / 13.f, lt(cast("f_numeric", TINYINT), int5));
    checkSelectivity(2 / 13.f, gt(cast("f_numeric", TINYINT), int5));
    checkSelectivity(11 / 13.f, le(cast("f_numeric", TINYINT), int5));

    checkSelectivity(12 / 13f, ge(cast("f_numeric", TINYINT), int2));
    checkSelectivity(1 / 13f, lt(cast("f_numeric", TINYINT), int2));
    checkSelectivity(5 / 13f, gt(cast("f_numeric", TINYINT), int2));
    checkSelectivity(8 / 13f, le(cast("f_numeric", TINYINT), int2));

    // check some types
    checkSelectivity(3 / 13.f, ge(cast("f_numeric", INTEGER), int5));
    checkSelectivity(3 / 13.f, ge(cast("f_numeric", SMALLINT), int5));
    checkSelectivity(3 / 13.f, ge(cast("f_numeric", BIGINT), int5));
    checkSelectivity(3 / 13.f, ge(cast("f_numeric", FLOAT), int5));
    checkSelectivity(3 / 13.f, ge(cast("f_numeric", DOUBLE), int5));
  }

  @Test
  public void testComputeRangePredicateSelectivityWithCast2() {
    useFieldWithValues("f_numeric", VALUES2, KLL2);
    RelDataType decimal3s1 = decimalType(3, 1);
    checkSelectivity(4 / 28.f, ge(cast("f_numeric", decimal3s1), literalFloat(1)));

    // values from -99.94999 to 99.94999 (both inclusive)
    checkSelectivity(7 / 28.f, lt(cast("f_numeric", decimal3s1), literalFloat(100)));
    checkSelectivity(7 / 28.f, le(cast("f_numeric", decimal3s1), literalFloat(100)));
    checkSelectivity(0 / 28.f, gt(cast("f_numeric", decimal3s1), literalFloat(100)));
    checkSelectivity(0 / 28.f, ge(cast("f_numeric", decimal3s1), literalFloat(100)));

    RelDataType decimal4s1 = decimalType(4, 1);
    checkSelectivity(10 / 28.f, lt(cast("f_numeric", decimal4s1), literalFloat(100)));
    checkSelectivity(20 / 28.f, le(cast("f_numeric", decimal4s1), literalFloat(100)));
    checkSelectivity(3 / 28.f, gt(cast("f_numeric", decimal4s1), literalFloat(100)));
    checkSelectivity(13 / 28.f, ge(cast("f_numeric", decimal4s1), literalFloat(100)));

    RelDataType decimal2s1 = decimalType(2, 1);
    checkSelectivity(2 / 28.f, lt(cast("f_numeric", decimal2s1), literalFloat(100)));
    checkSelectivity(2 / 28.f, le(cast("f_numeric", decimal2s1), literalFloat(100)));
    checkSelectivity(0 / 28.f, gt(cast("f_numeric", decimal2s1), literalFloat(100)));
    checkSelectivity(0 / 28.f, ge(cast("f_numeric", decimal2s1), literalFloat(100)));

    // expected: 100_000f
    RelDataType decimal7s1 = decimalType(7, 1);
    checkSelectivity(1 / 28.f, gt(cast("f_numeric", decimal7s1), literalFloat(10000)));

    // expected: 10_000f, 100_000f, because CAST(1_000_000 AS DECIMAL(7,1)) = NULL, and similar for even larger values
    checkSelectivity(2 / 28.f, ge(cast("f_numeric", decimal7s1), literalFloat(9999)));
    checkSelectivity(2 / 28.f, ge(cast("f_numeric", decimal7s1), literalFloat(10000)));

    // expected: 100_000f
    checkSelectivity(1 / 28.f, gt(cast("f_numeric", decimal7s1), literalFloat(10000)));
    checkSelectivity(1 / 28.f, gt(cast("f_numeric", decimal7s1), literalFloat(10001)));

    // expected 1f, 10f, 99.94998f, 99.94999f
    checkSelectivity(4 / 28.f, ge(cast("f_numeric", decimal3s1), literalFloat(1)));
    checkSelectivity(3 / 28.f, gt(cast("f_numeric", decimal3s1), literalFloat(1)));
    // expected -99.94999f, -99.94998f, 0f, 1f
    checkSelectivity(4 / 28.f, le(cast("f_numeric", decimal3s1), literalFloat(1)));
    checkSelectivity(3 / 28.f, lt(cast("f_numeric", decimal3s1), literalFloat(1)));

    // the cast would apply a modulo operation to the values outside the range of the cast
    // so instead a default selectivity should be returned
    checkSelectivity(1 / 3.f, lt(cast("f_numeric", TINYINT), literalFloat(100)));
    checkSelectivity(1 / 3.f, lt(cast("f_numeric", TINYINT), literalFloat(100)));
  }

  private void checkTimeFieldOnMidnightTimestamps(RexNode field) {
    // note: use only values from VALUES_TIME that specify a date without hh:mm:ss!
    checkSelectivity(7 / 7.f, ge(field, literalTimestamp("2020-11-01")));
    checkSelectivity(5 / 7.f, ge(field, literalTimestamp("2020-11-03")));
    checkSelectivity(1 / 7.f, ge(field, literalTimestamp("2020-11-07")));

    checkSelectivity(6 / 7.f, gt(field, literalTimestamp("2020-11-01")));
    checkSelectivity(4 / 7.f, gt(field, literalTimestamp("2020-11-03")));
    checkSelectivity(0 / 7.f, gt(field, literalTimestamp("2020-11-07")));

    checkSelectivity(1 / 7.f, le(field, literalTimestamp("2020-11-01")));
    checkSelectivity(3 / 7.f, le(field, literalTimestamp("2020-11-03")));
    checkSelectivity(7 / 7.f, le(field, literalTimestamp("2020-11-07")));

    checkSelectivity(0 / 7.f, lt(field, literalTimestamp("2020-11-01")));
    checkSelectivity(2 / 7.f, lt(field, literalTimestamp("2020-11-03")));
    checkSelectivity(6 / 7.f, lt(field, literalTimestamp("2020-11-07")));
  }

  private void checkTimeFieldOnIntraDayTimestamps(RexNode field) {
    checkSelectivity(3 / 7.f, ge(field, literalTimestamp("2020-11-05T11:23:45Z")));
    checkSelectivity(2 / 7.f, gt(field, literalTimestamp("2020-11-05T11:23:45Z")));
    checkSelectivity(5 / 7.f, le(field, literalTimestamp("2020-11-05T11:23:45Z")));
    checkSelectivity(4 / 7.f, lt(field, literalTimestamp("2020-11-05T11:23:45Z")));
  }

  @Test
  public void testComputeRangePredicateSelectivityTimestamp() {
    useFieldWithValues("f_timestamp", VALUES_TIME, KLL_TIME);
    checkTimeFieldOnMidnightTimestamps(currentInputRef);
    checkTimeFieldOnIntraDayTimestamps(currentInputRef);
  }

  @Test
  public void testComputeRangePredicateSelectivityDate() {
    useFieldWithValues("f_date", VALUES_TIME, KLL_TIME);
    checkTimeFieldOnMidnightTimestamps(currentInputRef);

    // it does not make sense to compare with "2020-11-05T11:23:45Z",
    // as that value would not be stored as-is in a date column, but as "2020-11-05" instead
  }

  @Test
  public void testComputeRangePredicateSelectivityDateWithCast() {
    useFieldWithValues("f_date", VALUES_TIME, KLL_TIME);
    RexNode field1 = cast("f_date", SqlTypeName.DATE);
    checkTimeFieldOnMidnightTimestamps(field1);
    checkTimeFieldOnIntraDayTimestamps(field1);

    RexNode field2 = cast("f_date", SqlTypeName.TIMESTAMP);
    checkTimeFieldOnMidnightTimestamps(field2);
    checkTimeFieldOnIntraDayTimestamps(field2);
  }

  @Test
  public void testComputeRangePredicateSelectivityTimestampWithCast() {
    useFieldWithValues("f_timestamp", VALUES_TIME, KLL_TIME);
    checkTimeFieldOnMidnightTimestamps(cast("f_timestamp", SqlTypeName.DATE));
    checkTimeFieldOnMidnightTimestamps(cast("f_timestamp", SqlTypeName.TIMESTAMP));
  }

  @Test
  public void testComputeRangePredicateSelectivityBetweenWithCastDecimal2_1() {
    useFieldWithValues("f_numeric", VALUES2, KLL2);
    float total = VALUES2.length;
    float universe = 2; // the number of values that "survive" the cast
    RexNode cast = REX_BUILDER.makeCast(decimalType(2, 1), inputRef0);
    checkBetweenSelectivity(0, universe, total, cast, 100f, 1000f);
    checkBetweenSelectivity(1, universe, total, cast, 1f, 100f);
    checkBetweenSelectivity(0, universe, total, cast, 100f, 0f);
  }

  @Test
  public void testComputeRangePredicateSelectivityBetweenWithCastDecimal3_1() {
    useFieldWithValues("f_numeric", VALUES2, KLL2);
    float total = VALUES2.length;
    float universe = 7;
    RexNode cast = REX_BUILDER.makeCast(decimalType(3, 1), inputRef0);
    checkBetweenSelectivity(0, universe, total, cast, 100f, 1000f);
    checkBetweenSelectivity(4, universe, total, cast, 1f, 100f);
    checkBetweenSelectivity(0, universe, total, cast, 100f, 0f);
  }

  @Test
  public void testComputeRangePredicateSelectivityBetweenWithCastDecimal4_1() {
    useFieldWithValues("f_numeric", VALUES2, KLL2);
    float total = VALUES2.length;
    float universe = 23;
    RexNode cast = REX_BUILDER.makeCast(decimalType(4, 1), inputRef0);
    // the values between -999.94999... and 999.94999... (both inclusive) pass through the cast
    // the values between 99.95 and 100 are rounded up to 100, so they fulfill the BETWEEN
    checkBetweenSelectivity(13, universe, total, cast, 100, 1000);
    checkBetweenSelectivity(14, universe, total, cast, 1f, 100f);
    checkBetweenSelectivity(0, universe, total, cast, 100f, 0f);
  }

  @Test
  public void testComputeRangePredicateSelectivityBetweenWithCastDecimal7_1() {
    useFieldWithValues("f_numeric", VALUES2, KLL2);
    float total = VALUES2.length;
    float universe = 26;
    RexNode cast = REX_BUILDER.makeCast(decimalType(7, 1), inputRef0);
    checkBetweenSelectivity(14, universe, total, cast, 100, 1000);
    checkBetweenSelectivity(14, universe, total, cast, 1f, 100f);
    checkBetweenSelectivity(0, universe, total, cast, 100f, 0f);
  }

  private void checkSelectivity(float expectedSelectivity, RexNode filter) {
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    Assert.assertEquals(filter.toString(), expectedSelectivity, estimator.estimateSelectivity(filter), DELTA);

    // swap equation, e.g., col < 5 becomes 5 > col; selectivity stays the same
    RexCall call = (RexCall) filter;
    SqlOperator operator = ((RexCall) filter).getOperator();
    SqlOperator swappedOp;
    if (operator == LE) {
      swappedOp = GE;
    } else if (operator == LT) {
      swappedOp = GT;
    } else if (operator == GE) {
      swappedOp = LE;
    } else if (operator == GT) {
      swappedOp = LT;
    } else if (operator == BETWEEN) {
      // BETWEEN cannot be swapped
      return;
    } else {
      throw new UnsupportedOperationException();
    }
    RexNode swapped = REX_BUILDER.makeCall(swappedOp, call.getOperands().get(1), call.getOperands().get(0));
    Assert.assertEquals(filter.toString(), expectedSelectivity, estimator.estimateSelectivity(swapped), DELTA);
  }

  private void checkBetweenSelectivity(float expectedEntries, float universe, float total, RexNode value, float lower,
      float upper) {
    RexNode betweenFilter =
        REX_BUILDER.makeCall(HiveBetween.INSTANCE, boolFalse, value, literalFloat(lower), literalFloat(upper));
    FilterSelectivityEstimator estimator = new FilterSelectivityEstimator(scan, mq);
    String between = "BETWEEN " + lower + " AND " + upper;
    float expectedSelectivity = expectedEntries / total;
    String message = between + ": calcite filter " + betweenFilter.toString();
    Assert.assertEquals(message, expectedSelectivity, estimator.estimateSelectivity(betweenFilter), DELTA);

    // invert the filter to a NOT BETWEEN
    RexNode invBetween =
        REX_BUILDER.makeCall(HiveBetween.INSTANCE, boolTrue, value, literalFloat(lower), literalFloat(upper));
    String invMessage = "NOT " + between + ": calcite filter " + invBetween.toString();
    float invExpectedSelectivity = (universe - expectedEntries) / total;
    Assert.assertEquals(invMessage, invExpectedSelectivity, estimator.estimateSelectivity(invBetween), DELTA);
  }

  private RexNode cast(String fieldname, SqlTypeName typeName) {
    return cast(fieldname, type(typeName));
  }

  private RexNode cast(String fieldname, RelDataType type) {
    int fieldIndex = scan.getRowType().getFieldNames().indexOf(fieldname);
    RexNode column = REX_BUILDER.makeInputRef(scan, fieldIndex);
    return REX_BUILDER.makeCast(type, column);
  }

  private RexNode ge(RexNode expr, RexNode value) {
    return REX_BUILDER.makeCall(GE, expr, value);
  }

  private RexNode gt(RexNode expr, RexNode value) {
    return REX_BUILDER.makeCall(GT, expr, value);
  }

  private RexNode le(RexNode expr, RexNode value) {
    return REX_BUILDER.makeCall(LE, expr, value);
  }

  private RexNode lt(RexNode expr, RexNode value) {
    return REX_BUILDER.makeCall(LT, expr, value);
  }

  private static RelDataType type(SqlTypeName typeName) {
    return REX_BUILDER.getTypeFactory().createSqlType(typeName);
  }

  private static RelDataType decimalType(int precision, int scale) {
    return REX_BUILDER.getTypeFactory().createSqlType(SqlTypeName.DECIMAL, precision, scale);
  }

  private static RexLiteral literalTimestamp(String timestamp) {
    return REX_BUILDER.makeLiteral(timestampMillis(timestamp),
        REX_BUILDER.getTypeFactory().createSqlType(SqlTypeName.TIMESTAMP));
  }

  private RexNode literalFloat(float f) {
    return REX_BUILDER.makeLiteral(f, type(SqlTypeName.FLOAT));
  }

  private static long timestampMillis(String timestamp) {
    if (!timestamp.contains(":")) {
      return LocalDate.parse(timestamp).toEpochSecond(LocalTime.MIDNIGHT, ZoneOffset.UTC) * 1000;
    }
    return Instant.parse(timestamp).toEpochMilli();
  }

  private static long timestamp(String timestamp) {
    return timestampMillis(timestamp) / 1000;
  }
}
