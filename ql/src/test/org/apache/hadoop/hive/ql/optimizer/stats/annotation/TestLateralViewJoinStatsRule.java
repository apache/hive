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
import org.apache.hadoop.hive.ql.exec.UDTFOperator;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.plan.ColStatistics;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.LateralViewJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.UDTFDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFJSONTuple;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFPosExplode;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
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
 * Tests for LateralViewJoinStatsRule - specifically the refineUdtfColStats() method.
 */
public class TestLateralViewJoinStatsRule {

  /**
   * Tests that posexplode pos column is refined with NDV=ceil(factor) and numNulls=0.
   */
  @Test
  public void testPosExplodePosColumnRefinement() throws Exception {
    // SELECT has 100 rows, UDTF has 200 rows (factor = 2.0), inputNdv = 10
    Statistics[] capturedStats = new Statistics[1];
    LateralViewJoinOperator lvj = createMockLVJ(
        new GenericUDTFPosExplode(),
        Arrays.asList("pos", "val"),
        Arrays.asList(TypeInfoFactory.intTypeInfo, TypeInfoFactory.stringTypeInfo),
        100, 200, 10, capturedStats);

    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.LateralViewJoinStatsRule rule =
        new StatsRulesProcFactory.LateralViewJoinStatsRule();
    rule.process(lvj, new Stack<>(), ctx);

    assertNotNull("Statistics should be set", capturedStats[0]);

    ColStatistics posStats = capturedStats[0].getColumnStatisticsFromColName("pos");
    assertNotNull("pos column stats should exist", posStats);
    assertEquals("pos NDV should be ceil(factor)=2", 2, posStats.getCountDistint());
    assertEquals("pos numNulls should be 0", 0, posStats.getNumNulls());
  }

  /**
   * Tests that posexplode val column is refined with NDV=min(numRows, inputNdv*factor).
   */
  @Test
  public void testPosExplodeValColumnRefinement() throws Exception {
    Statistics[] capturedStats = new Statistics[1];
    LateralViewJoinOperator lvj = createMockLVJ(
        new GenericUDTFPosExplode(),
        Arrays.asList("pos", "val"),
        Arrays.asList(TypeInfoFactory.intTypeInfo, TypeInfoFactory.stringTypeInfo),
        100, 200, 10, capturedStats);

    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.LateralViewJoinStatsRule rule =
        new StatsRulesProcFactory.LateralViewJoinStatsRule();
    rule.process(lvj, new Stack<>(), ctx);

    assertNotNull("Statistics should be set", capturedStats[0]);

    ColStatistics valStats = capturedStats[0].getColumnStatisticsFromColName("val");
    assertNotNull("val column stats should exist", valStats);
    // NDV = min(200, 10 * 2) = 20
    assertEquals("val NDV should be min(numRows, inputNdv*factor)", 20, valStats.getCountDistint());
  }

  /**
   * Tests that explode col is refined correctly.
   */
  @Test
  public void testExplodeRefinement() throws Exception {
    Statistics[] capturedStats = new Statistics[1];
    LateralViewJoinOperator lvj = createMockLVJ(
        new GenericUDTFExplode(),
        Arrays.asList("col"),
        Arrays.asList(TypeInfoFactory.stringTypeInfo),
        100, 300, 10, capturedStats);

    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.LateralViewJoinStatsRule rule =
        new StatsRulesProcFactory.LateralViewJoinStatsRule();
    rule.process(lvj, new Stack<>(), ctx);

    assertNotNull("Statistics should be set", capturedStats[0]);

    ColStatistics colStats = capturedStats[0].getColumnStatisticsFromColName("col");
    assertNotNull("col column stats should exist", colStats);
    // NDV = min(300, 10 * 3) = 30
    assertEquals("col NDV should be min(numRows, inputNdv*factor)", 30, colStats.getCountDistint());
  }

  /**
   * Tests that value column is NOT refined when inputNdv is 0.
   * The pos column should still be refined (it's independent of inputNdv).
   */
  @Test
  public void testNoValueRefinementWhenInputNdvIsZero() throws Exception {
    Statistics[] capturedStats = new Statistics[1];
    LateralViewJoinOperator lvj = createMockLVJ(
        new GenericUDTFPosExplode(),
        Arrays.asList("pos", "val"),
        Arrays.asList(TypeInfoFactory.intTypeInfo, TypeInfoFactory.stringTypeInfo),
        100, 200, 0, capturedStats);  // inputNdv = 0

    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.LateralViewJoinStatsRule rule =
        new StatsRulesProcFactory.LateralViewJoinStatsRule();
    rule.process(lvj, new Stack<>(), ctx);

    assertNotNull("Statistics should be set", capturedStats[0]);

    // pos should still be refined (independent of inputNdv)
    ColStatistics posStats = capturedStats[0].getColumnStatisticsFromColName("pos");
    assertEquals("pos NDV should still be refined", 2, posStats.getCountDistint());

    // val should NOT be refined (inputNdv = 0)
    ColStatistics valStats = capturedStats[0].getColumnStatisticsFromColName("val");
    assertEquals("val NDV should remain 0 (not refined)", 0, valStats.getCountDistint());
  }

  /**
   * Tests that non-explode UDTFs (e.g., json_tuple) are NOT refined.
   */
  @Test
  public void testNoRefinementForOtherUdtfs() throws Exception {
    Statistics[] capturedStats = new Statistics[1];
    LateralViewJoinOperator lvj = createMockLVJ(
        new GenericUDTFJSONTuple(),
        Arrays.asList("c0", "c1"),
        Arrays.asList(TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo),
        100, 200, 10, capturedStats);

    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.LateralViewJoinStatsRule rule =
        new StatsRulesProcFactory.LateralViewJoinStatsRule();
    rule.process(lvj, new Stack<>(), ctx);

    assertNotNull("Statistics should be set", capturedStats[0]);

    // Neither column should be refined for json_tuple
    ColStatistics c0Stats = capturedStats[0].getColumnStatisticsFromColName("c0");
    ColStatistics c1Stats = capturedStats[0].getColumnStatisticsFromColName("c1");
    assertEquals("c0 NDV should remain 0 (not refined)", 0, c0Stats.getCountDistint());
    assertEquals("c1 NDV should remain 0 (not refined)", 0, c1Stats.getCountDistint());
  }

  /**
   * Tests that already-refined stats (isEstimated=false) are NOT double-refined.
   */
  @Test
  public void testAlreadyRefinedStatsNotDoubleRefined() throws Exception {
    Statistics[] capturedStats = new Statistics[1];
    // Mark stats as not estimated (already refined)
    LateralViewJoinOperator lvj = createMockLVJWithExistingStats(
        new GenericUDTFPosExplode(),
        Arrays.asList("pos", "val"),
        Arrays.asList(TypeInfoFactory.intTypeInfo, TypeInfoFactory.stringTypeInfo),
        100, 200, 10,
        50,    // existing NDV for pos
        100,   // existing NDV for val
        false, // isEstimated = false
        capturedStats);

    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.LateralViewJoinStatsRule rule =
        new StatsRulesProcFactory.LateralViewJoinStatsRule();
    rule.process(lvj, new Stack<>(), ctx);

    assertNotNull("Statistics should be set", capturedStats[0]);

    // Stats should NOT be modified since isEstimated=false
    ColStatistics posStats = capturedStats[0].getColumnStatisticsFromColName("pos");
    ColStatistics valStats = capturedStats[0].getColumnStatisticsFromColName("val");
    assertEquals("pos NDV should remain unchanged", 50, posStats.getCountDistint());
    assertEquals("val NDV should remain unchanged", 100, valStats.getCountDistint());
  }

  /**
   * Tests that stats with non-zero countDistint are NOT refined.
   */
  @Test
  public void testStatsWithNonZeroNdvNotRefined() throws Exception {
    Statistics[] capturedStats = new Statistics[1];
    // Create stats with non-zero NDV but isEstimated=true
    LateralViewJoinOperator lvj = createMockLVJWithExistingStats(
        new GenericUDTFPosExplode(),
        Arrays.asList("pos", "val"),
        Arrays.asList(TypeInfoFactory.intTypeInfo, TypeInfoFactory.stringTypeInfo),
        100, 200, 10,
        5,    // existing NDV for pos (non-zero)
        15,   // existing NDV for val (non-zero)
        true, // isEstimated = true
        capturedStats);

    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.LateralViewJoinStatsRule rule =
        new StatsRulesProcFactory.LateralViewJoinStatsRule();
    rule.process(lvj, new Stack<>(), ctx);

    assertNotNull("Statistics should be set", capturedStats[0]);

    // Stats should NOT be modified since countDistint != 0
    ColStatistics posStats = capturedStats[0].getColumnStatisticsFromColName("pos");
    ColStatistics valStats = capturedStats[0].getColumnStatisticsFromColName("val");
    assertEquals("pos NDV should remain unchanged", 5, posStats.getCountDistint());
    assertEquals("val NDV should remain unchanged", 15, valStats.getCountDistint());
  }

  /**
   * Tests defensive identity mapping - pos column is identified by schema name, not position.
   */
  @Test
  public void testDefensiveIdentityMapping() throws Exception {
    Statistics[] capturedStats = new Statistics[1];
    // Use custom column names
    LateralViewJoinOperator lvj = createMockLVJ(
        new GenericUDTFPosExplode(),
        Arrays.asList("mypos", "myval"),
        Arrays.asList(TypeInfoFactory.intTypeInfo, TypeInfoFactory.stringTypeInfo),
        100, 200, 10, capturedStats);

    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.LateralViewJoinStatsRule rule =
        new StatsRulesProcFactory.LateralViewJoinStatsRule();
    rule.process(lvj, new Stack<>(), ctx);

    assertNotNull("Statistics should be set", capturedStats[0]);

    // mypos should be identified as the pos column and refined
    ColStatistics myposStats = capturedStats[0].getColumnStatisticsFromColName("mypos");
    assertNotNull("mypos column stats should exist", myposStats);
    assertEquals("mypos NDV should be refined to ceil(factor)=2", 2, myposStats.getCountDistint());
    assertEquals("mypos numNulls should be 0", 0, myposStats.getNumNulls());

    // myval should also be refined
    ColStatistics myvalStats = capturedStats[0].getColumnStatisticsFromColName("myval");
    assertEquals("myval NDV should be refined", 20, myvalStats.getCountDistint());
  }

  /**
   * Tests that NDV is capped at numRows when inputNdv*factor exceeds numRows.
   */
  @Test
  public void testNdvCappedAtNumRows() throws Exception {
    Statistics[] capturedStats = new Statistics[1];
    // Large inputNdv that would exceed numRows after multiplication
    LateralViewJoinOperator lvj = createMockLVJ(
        new GenericUDTFExplode(),
        Arrays.asList("col"),
        Arrays.asList(TypeInfoFactory.stringTypeInfo),
        100, 150, 1000, capturedStats);  // 1000 * 1.5 = 1500 > 150

    AnnotateStatsProcCtx ctx = createAnnotateStatsProcCtx();
    StatsRulesProcFactory.LateralViewJoinStatsRule rule =
        new StatsRulesProcFactory.LateralViewJoinStatsRule();
    rule.process(lvj, new Stack<>(), ctx);

    assertNotNull("Statistics should be set", capturedStats[0]);

    ColStatistics colStats = capturedStats[0].getColumnStatisticsFromColName("col");
    // NDV = min(150, 1000 * 2) = 150 (capped at numRows)
    assertEquals("NDV should be capped at numRows", 150, colStats.getCountDistint());
  }

  // ========== Helper methods ==========

  private LateralViewJoinOperator createMockLVJ(
      GenericUDTF genericUDTF,
      List<String> udtfColNames,
      List<TypeInfo> udtfColTypes,
      long selectNumRows,
      long udtfNumRows,
      long inputNdv,
      Statistics[] capturedStats) {
    return createMockLVJWithExistingStats(genericUDTF, udtfColNames, udtfColTypes,
        selectNumRows, udtfNumRows, inputNdv, 0, 0, true, capturedStats);
  }

  @SuppressWarnings("unchecked")
  private LateralViewJoinOperator createMockLVJWithExistingStats(
      GenericUDTF genericUDTF,
      List<String> udtfColNames,
      List<TypeInfo> udtfColTypes,
      long selectNumRows,
      long udtfNumRows,
      long inputNdv,
      long existingNdv0,
      long existingNdv1,
      boolean isEstimated,
      Statistics[] capturedStats) {

    // Create SELECT parent (index 0)
    Operator<? extends OperatorDesc> selectOp = mock(Operator.class);
    Statistics selectStats = new Statistics(selectNumRows, selectNumRows * 50, 0, 0);
    ColStatistics selectColStats = new ColStatistics("id", "string");
    selectColStats.setCountDistint(10);
    selectColStats.setNumNulls(0);
    selectColStats.setAvgColLen(10);
    selectStats.addToColumnStats(Arrays.asList(selectColStats));
    selectStats.setColumnStatsState(Statistics.State.COMPLETE);
    when(selectOp.getStatistics()).thenReturn(selectStats);

    // Create UDTF parent's parent (for inputNdv)
    Operator<? extends OperatorDesc> udtfParentParent = mock(Operator.class);
    Statistics udtfParentStats = new Statistics(selectNumRows, selectNumRows * 100, 0, 0);
    ColStatistics inputColStats = new ColStatistics("arr", "array<string>");
    inputColStats.setCountDistint(inputNdv);
    inputColStats.setNumNulls(0);
    inputColStats.setAvgColLen(100);
    udtfParentStats.addToColumnStats(Arrays.asList(inputColStats));
    udtfParentStats.setColumnStatsState(Statistics.State.COMPLETE);
    when(udtfParentParent.getStatistics()).thenReturn(udtfParentStats);

    // Create UDTF schema
    List<ColumnInfo> udtfSignature = new ArrayList<>();
    for (int i = 0; i < udtfColNames.size(); i++) {
      udtfSignature.add(new ColumnInfo(udtfColNames.get(i), udtfColTypes.get(i), "", false));
    }
    RowSchema udtfSchema = new RowSchema(udtfSignature);

    // Create UDTFDesc
    UDTFDesc udtfDesc = mock(UDTFDesc.class);
    when(udtfDesc.getGenericUDTF()).thenReturn(genericUDTF);

    // Create UDTF operator (index 1)
    UDTFOperator udtfOp = mock(UDTFOperator.class);

    // Create UDTF stats with placeholder or existing values
    Statistics udtfStats = new Statistics(udtfNumRows, udtfNumRows * 50, 0, 0);
    List<ColStatistics> udtfColStatsList = new ArrayList<>();
    for (int i = 0; i < udtfColNames.size(); i++) {
      ColStatistics cs = new ColStatistics(udtfColNames.get(i), udtfColTypes.get(i).getTypeName());
      if (i == 0 && existingNdv0 > 0) {
        cs.setCountDistint(existingNdv0);
      } else if (i == 1 && existingNdv1 > 0) {
        cs.setCountDistint(existingNdv1);
      } else {
        cs.setCountDistint(0);
      }
      cs.setNumNulls(-1);
      cs.setAvgColLen(10);
      cs.setIsEstimated(isEstimated);
      udtfColStatsList.add(cs);
    }
    udtfStats.addToColumnStats(udtfColStatsList);
    udtfStats.setColumnStatsState(Statistics.State.PARTIAL);

    when(udtfOp.getStatistics()).thenReturn(udtfStats);
    when(udtfOp.getConf()).thenReturn(udtfDesc);
    when(udtfOp.getSchema()).thenReturn(udtfSchema);
    when(udtfOp.getParentOperators()).thenReturn(Arrays.asList(udtfParentParent));

    // Create LVJ operator
    LateralViewJoinOperator lvj = mock(LateralViewJoinOperator.class);

    // Set up parents list with correct indices
    List<Operator<? extends OperatorDesc>> parents = new ArrayList<>();
    parents.add(selectOp);  // SELECT_TAG = 0
    parents.add(udtfOp);    // UDTF_TAG = 1
    when(lvj.getParentOperators()).thenReturn(parents);

    // Create LVJ schema (includes both SELECT and UDTF columns)
    List<ColumnInfo> lvjSignature = new ArrayList<>();
    lvjSignature.add(new ColumnInfo("id", TypeInfoFactory.stringTypeInfo, "", false));
    for (int i = 0; i < udtfColNames.size(); i++) {
      lvjSignature.add(new ColumnInfo(udtfColNames.get(i), udtfColTypes.get(i), "", false));
    }
    RowSchema lvjSchema = new RowSchema(lvjSignature);
    when(lvj.getSchema()).thenReturn(lvjSchema);

    // Create columnExprMap
    Map<String, ExprNodeDesc> columnExprMap = new HashMap<>();
    columnExprMap.put("id", new ExprNodeColumnDesc(TypeInfoFactory.stringTypeInfo, "id", "", false));
    for (int i = 0; i < udtfColNames.size(); i++) {
      columnExprMap.put(udtfColNames.get(i),
          new ExprNodeColumnDesc(udtfColTypes.get(i), udtfColNames.get(i), "", false));
    }
    when(lvj.getColumnExprMap()).thenReturn(columnExprMap);

    // Capture statistics
    doAnswer((Answer<Void>) invocation -> {
      capturedStats[0] = invocation.getArgument(0);
      return null;
    }).when(lvj).setStatistics(any(Statistics.class));

    // LVJ conf
    LateralViewJoinDesc lvjDesc = mock(LateralViewJoinDesc.class);
    when(lvj.getConf()).thenReturn(lvjDesc);

    return lvj;
  }

  private AnnotateStatsProcCtx createAnnotateStatsProcCtx() {
    HiveConf conf = new HiveConf();
    conf.setFloatVar(HiveConf.ConfVars.HIVE_STATS_UDTF_FACTOR, 1.0f);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_QUERY_REEXECUTION_ENABLED, false);

    Context context = mock(Context.class);
    when(context.getConf()).thenReturn(conf);

    ParseContext pctx = mock(ParseContext.class);
    when(pctx.getConf()).thenReturn(conf);
    when(pctx.getContext()).thenReturn(context);

    return new AnnotateStatsProcCtx(pctx);
  }
}
