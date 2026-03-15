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
import org.apache.hadoop.hive.ql.exec.LateralViewJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.LateralViewJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for LateralViewJoinStatsRule - specifically HIVE-29473 fix.
 *
 * The bug: In nested lateral views, column name collisions (e.g., _col0 appearing
 * in both SELECT and UDTF branches) cause the UDTF column's NDV to overwrite
 * the SELECT column's NDV, leading to incorrect CBO estimates.
 */
public class TestLateralViewJoinStatsRule {

  /**
   * Tests that column stats from SELECT and UDTF branches are properly isolated
   * when processing LateralViewJoinOperator statistics.
   *
   * Scenario: SELECT branch has a column with NDV=2, UDTF branch has a column with NDV=6.
   * After stats annotation, the SELECT column should retain NDV=2.
   */
  @Test
  public void testColumnStatsIsolation() throws Exception {
    // SELECT parent: 1 column "_col0" with NDV=2
    Operator<? extends OperatorDesc> selectParent = createMockParentOperator(
        "_col0", 2, 100);

    // UDTF parent: 1 column "_col1" with NDV=6
    Operator<? extends OperatorDesc> udtfParent = createMockParentOperator(
        "_col1", 6, 100);

    // LVJ has 2 columns: _col0 (from select), _col1 (from udtf)
    List<ColumnInfo> lvjSignature = Arrays.asList(
        new ColumnInfo("_col0", TypeInfoFactory.stringTypeInfo, "", false),
        new ColumnInfo("_col1", TypeInfoFactory.stringTypeInfo, "", false));

    Map<String, ExprNodeDesc> colExprMap = new HashMap<>();
    colExprMap.put("_col0", new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "_col0", "", false));
    colExprMap.put("_col1", new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "_col1", "", false));

    // Capture the statistics set on the LVJ operator
    final Statistics[] capturedStats = new Statistics[1];
    LateralViewJoinOperator lvj = createMockLVJOperator(
        selectParent, udtfParent, lvjSignature, colExprMap, 1, capturedStats);

    // Run the stats rule
    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.LateralViewJoinStatsRule rule =
        new StatsRulesProcFactory.LateralViewJoinStatsRule();
    rule.process(lvj, new Stack<>(), ctx);

    // Verify results
    assertNotNull("Statistics should be set on LVJ", capturedStats[0]);

    ColStatistics selectColStats = capturedStats[0].getColumnStatisticsFromColName("_col0");
    assertNotNull("Should have stats for _col0", selectColStats);
    assertEquals("SELECT column _col0 should have NDV=2", 2, selectColStats.getCountDistint());

    ColStatistics udtfColStats = capturedStats[0].getColumnStatisticsFromColName("_col1");
    assertNotNull("Should have stats for _col1", udtfColStats);
    assertEquals("UDTF column _col1 should have NDV=6", 6, udtfColStats.getCountDistint());
  }

  /**
   * Tests the specific bug scenario from HIVE-29473: nested lateral views where
   * internal column names collide (both sides have _col0).
   *
   * Before fix: UDTF's _col0 NDV would overwrite SELECT's _col0 NDV.
   * After fix: Each branch's stats are isolated.
   */
  @Test
  public void testNestedLateralViewNameCollision() throws Exception {
    // Simulate nested LV scenario where both branches internally use _col0
    // SELECT parent: _col0 with NDV=2 (e.g., grouping column 'id')
    Operator<? extends OperatorDesc> selectParent = createMockParentOperator(
        "_col0", 2, 100);

    // UDTF parent: _col0 with NDV=6 (e.g., from explode of column with 6 distinct values)
    Operator<? extends OperatorDesc> udtfParent = createMockParentOperator(
        "_col0", 6, 100);

    // LVJ output schema: both columns exist but with different output names
    // In real scenario, LVJ renames to avoid collision, but the columnExprMap
    // still references the original _col0 from each parent
    List<ColumnInfo> lvjSignature = Arrays.asList(
        new ColumnInfo("_col0", TypeInfoFactory.stringTypeInfo, "", false),
        new ColumnInfo("_col1", TypeInfoFactory.stringTypeInfo, "", false));

    // columnExprMap: output _col0 -> select's _col0, output _col1 -> udtf's _col0
    Map<String, ExprNodeDesc> colExprMap = new HashMap<>();
    colExprMap.put("_col0", new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "_col0", "", false));
    colExprMap.put("_col1", new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "_col0", "", false));

    final Statistics[] capturedStats = new Statistics[1];
    LateralViewJoinOperator lvj = createMockLVJOperator(
        selectParent, udtfParent, lvjSignature, colExprMap, 1, capturedStats);

    // Run the stats rule
    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.LateralViewJoinStatsRule rule =
        new StatsRulesProcFactory.LateralViewJoinStatsRule();
    rule.process(lvj, new Stack<>(), ctx);

    // The key assertion: _col0 (from SELECT) should have NDV=2, NOT 6
    assertNotNull("Statistics should be set", capturedStats[0]);
    ColStatistics col0Stats = capturedStats[0].getColumnStatisticsFromColName("_col0");
    assertNotNull("Should have stats for _col0", col0Stats);

    // This is the bug fix verification - before fix, this would be 6 (contaminated by UDTF)
    assertEquals("SELECT's _col0 should retain NDV=2, not be overwritten by UDTF's NDV=6",
        2, col0Stats.getCountDistint());
  }

  /**
   * Tests that columns missing from columnExprMap are handled gracefully.
   * The fix has a null check: if (expr != null) - this tests that branch.
   */
  @Test
  public void testMissingColumnExprMapEntry() throws Exception {
    Operator<? extends OperatorDesc> selectParent = createMockParentOperator(
        "_col0", 2, 100);
    Operator<? extends OperatorDesc> udtfParent = createMockParentOperator(
        "_col1", 6, 100);

    List<ColumnInfo> lvjSignature = Arrays.asList(
        new ColumnInfo("_col0", TypeInfoFactory.stringTypeInfo, "", false),
        new ColumnInfo("_col1", TypeInfoFactory.stringTypeInfo, "", false),
        new ColumnInfo("_col2", TypeInfoFactory.stringTypeInfo, "", false));

    // Only provide expr for _col0 and _col1, _col2 is missing
    Map<String, ExprNodeDesc> colExprMap = new HashMap<>();
    colExprMap.put("_col0", new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "_col0", "", false));
    colExprMap.put("_col1", new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "_col1", "", false));

    final Statistics[] capturedStats = new Statistics[1];
    LateralViewJoinOperator lvj = createMockLVJOperator(
        selectParent, udtfParent, lvjSignature, colExprMap, 1, capturedStats);

    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.LateralViewJoinStatsRule rule =
        new StatsRulesProcFactory.LateralViewJoinStatsRule();
    rule.process(lvj, new Stack<>(), ctx);

    assertNotNull("Statistics should be set", capturedStats[0]);
    assertEquals("SELECT column should have NDV=2", 2,
        capturedStats[0].getColumnStatisticsFromColName("_col0").getCountDistint());
    assertEquals("UDTF column should have NDV=6", 6,
        capturedStats[0].getColumnStatisticsFromColName("_col1").getCountDistint());
  }

  /**
   * Tests multiple columns per branch with name collision on one of them.
   */
  @Test
  public void testMultipleColumnsWithPartialCollision() throws Exception {
    // SELECT has _col0 (NDV=2) and _col1 (NDV=10)
    Operator<? extends OperatorDesc> selectParent = createMockParentOperatorMultiCol(
        Arrays.asList("_col0", "_col1"), Arrays.asList(2L, 10L), 100);

    // UDTF has _col0 (NDV=50) - collides with SELECT's _col0
    Operator<? extends OperatorDesc> udtfParent = createMockParentOperator(
        "_col0", 50, 100);

    // LVJ output: _col0, _col1 from SELECT, _col2 from UDTF
    List<ColumnInfo> lvjSignature = Arrays.asList(
        new ColumnInfo("_col0", TypeInfoFactory.stringTypeInfo, "", false),
        new ColumnInfo("_col1", TypeInfoFactory.stringTypeInfo, "", false),
        new ColumnInfo("_col2", TypeInfoFactory.stringTypeInfo, "", false));

    Map<String, ExprNodeDesc> colExprMap = new HashMap<>();
    colExprMap.put("_col0", new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "_col0", "", false));
    colExprMap.put("_col1", new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "_col1", "", false));
    colExprMap.put("_col2", new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "_col0", "", false));

    final Statistics[] capturedStats = new Statistics[1];
    LateralViewJoinOperator lvj = createMockLVJOperator(
        selectParent, udtfParent, lvjSignature, colExprMap, 2, capturedStats);

    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.LateralViewJoinStatsRule rule =
        new StatsRulesProcFactory.LateralViewJoinStatsRule();
    rule.process(lvj, new Stack<>(), ctx);

    assertNotNull("Statistics should be set", capturedStats[0]);
    assertEquals("SELECT _col0 should retain NDV=2", 2,
        capturedStats[0].getColumnStatisticsFromColName("_col0").getCountDistint());
    assertEquals("SELECT _col1 should have NDV=10", 10,
        capturedStats[0].getColumnStatisticsFromColName("_col1").getCountDistint());
    assertEquals("UDTF _col2 should have NDV=50", 50,
        capturedStats[0].getColumnStatisticsFromColName("_col2").getCountDistint());
  }

  /**
   * Tests edge case: only SELECT columns (numSelColumns = signature.size()).
   */
  @Test
  public void testOnlySelectColumns() throws Exception {
    Operator<? extends OperatorDesc> selectParent = createMockParentOperator(
        "_col0", 5, 100);
    Operator<? extends OperatorDesc> udtfParent = createMockParentOperator(
        "_col1", 10, 100);

    // LVJ has only 1 column from SELECT
    List<ColumnInfo> lvjSignature = Arrays.asList(
        new ColumnInfo("_col0", TypeInfoFactory.stringTypeInfo, "", false));

    Map<String, ExprNodeDesc> colExprMap = new HashMap<>();
    colExprMap.put("_col0", new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "_col0", "", false));

    final Statistics[] capturedStats = new Statistics[1];
    LateralViewJoinOperator lvj = createMockLVJOperator(
        selectParent, udtfParent, lvjSignature, colExprMap, 1, capturedStats);

    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.LateralViewJoinStatsRule rule =
        new StatsRulesProcFactory.LateralViewJoinStatsRule();
    rule.process(lvj, new Stack<>(), ctx);

    assertNotNull("Statistics should be set", capturedStats[0]);
    assertEquals("SELECT column should have NDV=5", 5,
        capturedStats[0].getColumnStatisticsFromColName("_col0").getCountDistint());
  }

  /**
   * Tests edge case: only UDTF columns (numSelColumns = 0).
   */
  @Test
  public void testOnlyUdtfColumns() throws Exception {
    Operator<? extends OperatorDesc> selectParent = createMockParentOperator(
        "_col0", 5, 100);
    Operator<? extends OperatorDesc> udtfParent = createMockParentOperator(
        "_col1", 10, 100);

    // LVJ has only 1 column from UDTF
    List<ColumnInfo> lvjSignature = Arrays.asList(
        new ColumnInfo("_col1", TypeInfoFactory.stringTypeInfo, "", false));

    Map<String, ExprNodeDesc> colExprMap = new HashMap<>();
    colExprMap.put("_col1", new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "_col1", "", false));

    final Statistics[] capturedStats = new Statistics[1];
    LateralViewJoinOperator lvj = createMockLVJOperator(
        selectParent, udtfParent, lvjSignature, colExprMap, 0, capturedStats);

    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.LateralViewJoinStatsRule rule =
        new StatsRulesProcFactory.LateralViewJoinStatsRule();
    rule.process(lvj, new Stack<>(), ctx);

    assertNotNull("Statistics should be set", capturedStats[0]);
    assertEquals("UDTF column should have NDV=10", 10,
        capturedStats[0].getColumnStatisticsFromColName("_col1").getCountDistint());
  }

  private Operator<? extends OperatorDesc> createMockParentOperator(
      String colName, long ndv, long numRows) {
    return createMockParentOperatorMultiCol(Arrays.asList(colName), Arrays.asList(ndv), numRows);
  }

  private Operator<? extends OperatorDesc> createMockParentOperatorMultiCol(
      List<String> colNames, List<Long> ndvs, long numRows) {
    @SuppressWarnings("unchecked")
    Operator<? extends OperatorDesc> parent = mock(Operator.class);

    Statistics stats = new Statistics(numRows, numRows * 10, 0, 0);
    List<ColStatistics> colStatsList = new ArrayList<>();
    List<ColumnInfo> signature = new ArrayList<>();

    for (int i = 0; i < colNames.size(); i++) {
      String colName = colNames.get(i);
      ColStatistics colStats = new ColStatistics(colName, "string");
      colStats.setCountDistint(ndvs.get(i));
      colStats.setNumNulls(0);
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
  private LateralViewJoinOperator createMockLVJOperator(
      Operator<? extends OperatorDesc> selectParent,
      Operator<? extends OperatorDesc> udtfParent,
      List<ColumnInfo> signature,
      Map<String, ExprNodeDesc> colExprMap,
      int numSelColumns,
      Statistics[] capturedStats) {

    LateralViewJoinOperator lvj = mock(LateralViewJoinOperator.class);

    // Parent operators
    List<Operator<? extends OperatorDesc>> parents = new ArrayList<>();
    parents.add(selectParent);
    parents.add(udtfParent);
    when(lvj.getParentOperators()).thenReturn(parents);

    // Schema
    when(lvj.getSchema()).thenReturn(new RowSchema(signature));

    // Column expression map
    when(lvj.getColumnExprMap()).thenReturn(colExprMap);

    // LVJ descriptor with numSelColumns
    List<String> outputColNames = new ArrayList<>();
    for (ColumnInfo ci : signature) {
      outputColNames.add(ci.getInternalName());
    }
    LateralViewJoinDesc desc = new LateralViewJoinDesc(numSelColumns, outputColNames);
    when(lvj.getConf()).thenReturn(desc);

    // Capture setStatistics call
    doAnswer((Answer<Void>) invocation -> {
      capturedStats[0] = invocation.getArgument(0);
      return null;
    }).when(lvj).setStatistics(any(Statistics.class));

    return lvj;
  }

  private AnnotateStatsProcCtx createAnnotateStatsProcCtx() {
    HiveConf conf = new HiveConf();
    // Disable runtime stats to avoid NPE in applyRuntimeStats
    conf.setBoolVar(HiveConf.ConfVars.HIVE_QUERY_REEXECUTION_ENABLED, false);

    Context context = mock(Context.class);
    when(context.getConf()).thenReturn(conf);

    ParseContext pctx = mock(ParseContext.class);
    when(pctx.getConf()).thenReturn(conf);
    when(pctx.getContext()).thenReturn(context);

    return new AnnotateStatsProcCtx(pctx);
  }
}
