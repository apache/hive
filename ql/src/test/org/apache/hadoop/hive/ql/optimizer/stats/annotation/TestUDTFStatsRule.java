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
import org.apache.hadoop.hive.ql.exec.UDTFOperator;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.UDTFDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Stack;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for UDTFStatsRule - specifically HIVE-29473 fix.
 *
 * The bug: UDTFStatsRule was cloning parent statistics which retained input column names
 * (e.g., _col0 for the array). This caused namespace collisions in LateralViewJoin when
 * the SELECT branch also had columns named _col0.
 *
 * The fix: UDTFStatsRule now creates new column statistics based on the UDTF's OUTPUT
 * schema (e.g., pos, val for posexplode) instead of cloning parent stats.
 */
public class TestUDTFStatsRule {

  /**
   * Tests that UDTF produces stats with output column names, not input column names.
   * This is the core fix for HIVE-29473.
   */
  @Test
  public void testOutputColumnNames() throws Exception {
    // Parent has input column "_col0" (the array being exploded)
    Operator<? extends OperatorDesc> parent = createMockParentOperator(
        Arrays.asList("_col0"), Arrays.asList(6L), 100, 5000);

    // UDTF output schema has "pos" and "val" (like posexplode)
    List<ColumnInfo> outputSignature = Arrays.asList(
        new ColumnInfo("pos", TypeInfoFactory.intTypeInfo, "", false),
        new ColumnInfo("val", TypeInfoFactory.stringTypeInfo, "", false));

    final Statistics[] capturedStats = new Statistics[1];
    UDTFOperator udtf = createMockUDTFOperator(parent, outputSignature, capturedStats);

    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.UDTFStatsRule rule = new StatsRulesProcFactory.UDTFStatsRule();
    rule.process(udtf, new Stack<>(), ctx);

    assertNotNull("Statistics should be set on UDTF", capturedStats[0]);

    // Key assertion: stats should have "pos" and "val", NOT "_col0"
    assertNull("Should NOT have stats for input column _col0",
        capturedStats[0].getColumnStatisticsFromColName("_col0"));
    assertNotNull("Should have stats for output column pos",
        capturedStats[0].getColumnStatisticsFromColName("pos"));
    assertNotNull("Should have stats for output column val",
        capturedStats[0].getColumnStatisticsFromColName("val"));
  }

  /**
   * Tests that output column stats have NDV=0 (unknown) since we can't
   * determine the actual distinct values without UDTF-specific logic.
   */
  @Test
  public void testOutputColumnNdvIsZero() throws Exception {
    Operator<? extends OperatorDesc> parent = createMockParentOperator(
        Arrays.asList("_col0"), Arrays.asList(100L), 50, 2500);

    List<ColumnInfo> outputSignature = Arrays.asList(
        new ColumnInfo("pos", TypeInfoFactory.intTypeInfo, "", false),
        new ColumnInfo("val", TypeInfoFactory.stringTypeInfo, "", false));

    final Statistics[] capturedStats = new Statistics[1];
    UDTFOperator udtf = createMockUDTFOperator(parent, outputSignature, capturedStats);

    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.UDTFStatsRule rule = new StatsRulesProcFactory.UDTFStatsRule();
    rule.process(udtf, new Stack<>(), ctx);

    assertNotNull("Statistics should be set", capturedStats[0]);

    ColStatistics posStats = capturedStats[0].getColumnStatisticsFromColName("pos");
    ColStatistics valStats = capturedStats[0].getColumnStatisticsFromColName("val");

    assertEquals("pos NDV should be 0 (unknown)", 0, posStats.getCountDistint());
    assertEquals("val NDV should be 0 (unknown)", 0, valStats.getCountDistint());
  }

  /**
   * Tests that output column stats have numNulls=-1 (unknown).
   */
  @Test
  public void testOutputColumnNumNullsIsUnknown() throws Exception {
    Operator<? extends OperatorDesc> parent = createMockParentOperator(
        Arrays.asList("_col0"), Arrays.asList(10L), 50, 2500);

    List<ColumnInfo> outputSignature = Arrays.asList(
        new ColumnInfo("col1", TypeInfoFactory.stringTypeInfo, "", false));

    final Statistics[] capturedStats = new Statistics[1];
    UDTFOperator udtf = createMockUDTFOperator(parent, outputSignature, capturedStats);

    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.UDTFStatsRule rule = new StatsRulesProcFactory.UDTFStatsRule();
    rule.process(udtf, new Stack<>(), ctx);

    assertNotNull("Statistics should be set", capturedStats[0]);

    ColStatistics colStats = capturedStats[0].getColumnStatisticsFromColName("col1");
    assertEquals("numNulls should be -1 (unknown)", -1, colStats.getNumNulls());
  }

  /**
   * Tests that row count is scaled by UDTF factor.
   */
  @Test
  public void testRowCountScaling() throws Exception {
    Operator<? extends OperatorDesc> parent = createMockParentOperator(
        Arrays.asList("_col0"), Arrays.asList(10L), 100, 5000);

    List<ColumnInfo> outputSignature = Arrays.asList(
        new ColumnInfo("val", TypeInfoFactory.stringTypeInfo, "", false));

    final Statistics[] capturedStats = new Statistics[1];
    UDTFOperator udtf = createMockUDTFOperator(parent, outputSignature, capturedStats);

    // Use factor of 2.0
    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx(2.0f);
    StatsRulesProcFactory.UDTFStatsRule rule = new StatsRulesProcFactory.UDTFStatsRule();
    rule.process(udtf, new Stack<>(), ctx);

    assertNotNull("Statistics should be set", capturedStats[0]);
    assertEquals("Row count should be scaled by factor", 200, capturedStats[0].getNumRows());
  }

  /**
   * Tests that data size is recalculated based on output column types,
   * not inherited from input (which could have inflated array sizes).
   */
  @Test
  public void testDataSizeBasedOnOutputColumns() throws Exception {
    // Parent has large data size (simulating array column with large avgColLen)
    Operator<? extends OperatorDesc> parent = createMockParentOperator(
        Arrays.asList("_col0"), Arrays.asList(10L), 100, 100000);

    // Output is just an int column - should have much smaller data size
    List<ColumnInfo> outputSignature = Arrays.asList(
        new ColumnInfo("pos", TypeInfoFactory.intTypeInfo, "", false));

    final Statistics[] capturedStats = new Statistics[1];
    UDTFOperator udtf = createMockUDTFOperator(parent, outputSignature, capturedStats);

    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.UDTFStatsRule rule = new StatsRulesProcFactory.UDTFStatsRule();
    rule.process(udtf, new Stack<>(), ctx);

    assertNotNull("Statistics should be set", capturedStats[0]);

    // Data size should be based on int column (4 bytes), not inherited 100000
    // 100 rows * 4 bytes = 400 bytes (approximately, may vary based on implementation)
    long dataSize = capturedStats[0].getDataSize();
    assert dataSize < 100000 : "Data size should be much smaller than parent's inflated size. Got: " + dataSize;
    assert dataSize > 0 : "Data size should be positive. Got: " + dataSize;
  }

  /**
   * Tests handling of single output column (like explode).
   */
  @Test
  public void testSingleOutputColumn() throws Exception {
    Operator<? extends OperatorDesc> parent = createMockParentOperator(
        Arrays.asList("arr"), Arrays.asList(50L), 100, 5000);

    List<ColumnInfo> outputSignature = Arrays.asList(
        new ColumnInfo("col", TypeInfoFactory.stringTypeInfo, "", false));

    final Statistics[] capturedStats = new Statistics[1];
    UDTFOperator udtf = createMockUDTFOperator(parent, outputSignature, capturedStats);

    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.UDTFStatsRule rule = new StatsRulesProcFactory.UDTFStatsRule();
    rule.process(udtf, new Stack<>(), ctx);

    assertNotNull("Statistics should be set", capturedStats[0]);
    assertEquals("Should have exactly 1 column stat", 1,
        capturedStats[0].getColumnStats().size());
    assertNotNull("Should have stats for 'col'",
        capturedStats[0].getColumnStatisticsFromColName("col"));
  }

  /**
   * Tests handling of multiple output columns (like posexplode).
   */
  @Test
  public void testMultipleOutputColumns() throws Exception {
    Operator<? extends OperatorDesc> parent = createMockParentOperator(
        Arrays.asList("arr"), Arrays.asList(50L), 100, 5000);

    List<ColumnInfo> outputSignature = Arrays.asList(
        new ColumnInfo("pos", TypeInfoFactory.intTypeInfo, "", false),
        new ColumnInfo("val", TypeInfoFactory.stringTypeInfo, "", false));

    final Statistics[] capturedStats = new Statistics[1];
    UDTFOperator udtf = createMockUDTFOperator(parent, outputSignature, capturedStats);

    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.UDTFStatsRule rule = new StatsRulesProcFactory.UDTFStatsRule();
    rule.process(udtf, new Stack<>(), ctx);

    assertNotNull("Statistics should be set", capturedStats[0]);
    assertEquals("Should have exactly 2 column stats", 2,
        capturedStats[0].getColumnStats().size());
    assertNotNull("Should have stats for 'pos'",
        capturedStats[0].getColumnStatisticsFromColName("pos"));
    assertNotNull("Should have stats for 'val'",
        capturedStats[0].getColumnStatisticsFromColName("val"));
  }

  /**
   * Tests that column stats state is inherited from parent.
   */
  @Test
  public void testColumnStatsStateInherited() throws Exception {
    Operator<? extends OperatorDesc> parent = createMockParentOperator(
        Arrays.asList("_col0"), Arrays.asList(10L), 100, 5000);

    List<ColumnInfo> outputSignature = Arrays.asList(
        new ColumnInfo("val", TypeInfoFactory.stringTypeInfo, "", false));

    final Statistics[] capturedStats = new Statistics[1];
    UDTFOperator udtf = createMockUDTFOperator(parent, outputSignature, capturedStats);

    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.UDTFStatsRule rule = new StatsRulesProcFactory.UDTFStatsRule();
    rule.process(udtf, new Stack<>(), ctx);

    assertNotNull("Statistics should be set", capturedStats[0]);
    assertEquals("Column stats state should be COMPLETE",
        Statistics.State.COMPLETE, capturedStats[0].getColumnStatsState());
  }

  private Operator<? extends OperatorDesc> createMockParentOperator(
      List<String> colNames, List<Long> ndvs, long numRows, long dataSize) {
    @SuppressWarnings("unchecked")
    Operator<? extends OperatorDesc> parent = mock(Operator.class);

    Statistics stats = new Statistics(numRows, dataSize, 0, 0);
    List<ColStatistics> colStatsList = new ArrayList<>();
    List<ColumnInfo> signature = new ArrayList<>();

    for (int i = 0; i < colNames.size(); i++) {
      String colName = colNames.get(i);
      ColStatistics colStats = new ColStatistics(colName, "string");
      colStats.setCountDistint(ndvs.get(i));
      colStats.setNumNulls(0);
      colStats.setAvgColLen(50);
      colStatsList.add(colStats);
      signature.add(new ColumnInfo(colName, TypeInfoFactory.stringTypeInfo, "", false));
    }

    stats.addToColumnStats(colStatsList);
    stats.setColumnStatsState(Statistics.State.COMPLETE);
    when(parent.getStatistics()).thenReturn(stats);
    when(parent.getSchema()).thenReturn(new RowSchema(signature));

    return parent;
  }

  @SuppressWarnings("unchecked")
  private UDTFOperator createMockUDTFOperator(
      Operator<? extends OperatorDesc> parent,
      List<ColumnInfo> outputSignature,
      Statistics[] capturedStats) {

    UDTFOperator udtf = mock(UDTFOperator.class);

    List<Operator<? extends OperatorDesc>> parents = new ArrayList<>();
    parents.add(parent);
    when(udtf.getParentOperators()).thenReturn(parents);

    when(udtf.getSchema()).thenReturn(new RowSchema(outputSignature));

    UDTFDesc desc = mock(UDTFDesc.class);
    when(udtf.getConf()).thenReturn(desc);

    doAnswer((Answer<Void>) invocation -> {
      capturedStats[0] = invocation.getArgument(0);
      return null;
    }).when(udtf).setStatistics(any(Statistics.class));

    return udtf;
  }

  private AnnotateStatsProcCtx createAnnotateStatsProcCtx() {
    return createAnnotateStatsProcCtx(1.0f);
  }

  private AnnotateStatsProcCtx createAnnotateStatsProcCtx(float udtfFactor) {
    HiveConf conf = new HiveConf();
    conf.setFloatVar(HiveConf.ConfVars.HIVE_STATS_UDTF_FACTOR, udtfFactor);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_QUERY_REEXECUTION_ENABLED, false);

    Context context = mock(Context.class);
    when(context.getConf()).thenReturn(conf);

    ParseContext pctx = mock(ParseContext.class);
    when(pctx.getConf()).thenReturn(conf);
    when(pctx.getContext()).thenReturn(context);

    return new AnnotateStatsProcCtx(pctx);
  }
}
