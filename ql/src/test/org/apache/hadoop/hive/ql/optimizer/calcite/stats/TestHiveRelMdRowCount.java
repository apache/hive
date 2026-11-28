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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.optimizer.calcite.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelOptUtil.PKFKJoinInfo;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAntiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFilter;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveProject;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveRelNode;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveSemiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableScan;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.MockedStatic;

class TestHiveRelMdRowCount {

  private static final ImmutableBitSet KEY = ImmutableBitSet.of(0);

  private RexBuilder rexBuilder;
  private RelOptCluster cluster;
  private RelMetadataQuery mq;
  private HiveTableScan pkScan;
  private HiveFilter pkFilter;
  private HiveTableScan fkScan;

  @BeforeEach
  void setUp() {
    rexBuilder = new RexBuilder(new JavaTypeFactoryImpl(new HiveTypeSystemImpl()));
    cluster = RelOptCluster.create(CalcitePlanner.createPlanner(new HiveConf()), rexBuilder);
    mq = mock(RelMetadataQuery.class);
    pkScan = scan("pk");
    fkScan = scan("fk");
    RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, rexBuilder.makeInputRef(pkScan, 0),
        rexBuilder.makeExactLiteral(BigDecimal.ONE));
    pkFilter = new HiveFilter(cluster, pkScan.getTraitSet(), pkScan, filter);
    when(mq.getRowCount(pkScan)).thenReturn(1000.0);
    when(mq.getRowCount(pkFilter)).thenReturn(250.0);
    when(mq.getRowCount(fkScan)).thenReturn(1000.0);
    when(mq.areColumnsUnique(pkFilter, KEY)).thenReturn(true);
  }

  @ParameterizedTest(name = "constraints={0}, pkLeft={1}, type={2}, pkNDV={3}, fkNDV={4}, simple={6}")
  @MethodSource("pkfkCases")
  void analysisAndRowCountWithUnknownAndKnownNdv(boolean constraints, boolean pkLeft, JoinRelType type,
      Double pkNdv, Double fkNdv, double expectedScaling, boolean simple) {
    Analysis a = analyze(constraints, pkLeft, type, pkNdv, fkNdv, simple);
    assertEquals(pkLeft ? 1 : 0, a.info.fkSide);
    assertEquals(!simple || pkNdv == null ? -1 : pkNdv, a.info.pkInfo.distinctCount);
    assertEquals(!simple || fkNdv == null ? -1 : fkNdv, a.info.fkInfo.distinctCount);
    assertEquals(expectedScaling, a.info.ndvScalingFactor);
    assertEquals(simple && !constraints, a.info.isPKSideSimple);
    if (type == JoinRelType.INNER) {
      assertEquals(0.25, a.info.pkInfo.selectivity);
      double expectedSelectivity = expectedScaling < 0 ? 1 : Math.min(1, 0.25 * expectedScaling);
      assertEquals(expectedSelectivity, a.info.joinSelectivity());
      assertEquals(1000 * expectedSelectivity, a.rowCount);
    } else {
      // an FK-preserving outer join keeps every FK row; unknown NDVs reach that upper bound
      assertEquals(1.0, a.info.joinSelectivity());
      assertEquals(1000.0, a.rowCount);
    }
  }

  private static Stream<Arguments> pkfkCases() {
    List<Arguments> cases = new ArrayList<>();
    for (boolean constraints : List.of(false, true)) {
      for (boolean pkLeft : List.of(false, true)) {
        JoinRelType outer = pkLeft ? JoinRelType.RIGHT : JoinRelType.LEFT;
        for (Double[] ndvs : new Double[][] {{null, 80.0, -1.0}, {20.0, null, -1.0}, {null, null, -1.0}}) {
          cases.add(Arguments.of(constraints, pkLeft, JoinRelType.INNER, ndvs[0], ndvs[1], ndvs[2], true));
          cases.add(Arguments.of(constraints, pkLeft, outer, ndvs[0], ndvs[1], ndvs[2], true));
        }
        for (Double[] ndvs : new Double[][] {{20.0, 80.0, 0.25}, {80.0, 20.0, 4.0}, {200.0, 20.0, 10.0},
            {0.0, 80.0, 0.0}}) {
          cases.add(Arguments.of(constraints, pkLeft, JoinRelType.INNER, ndvs[0], ndvs[1], ndvs[2], true));
        }
        cases.add(Arguments.of(constraints, pkLeft, JoinRelType.INNER, 20.0, 80.0, 1.0, false));
      }
    }
    return cases.stream();
  }

  @ParameterizedTest(name = "constraints={0}, pkLeft={1}, pkNDV={2}, fkNDV={3}, simple={4}")
  @MethodSource("outerJoinKnownNdvCases")
  @Disabled("Pre-existing: FK-preserving outer joins apply the PK filter selectivity (follow-up, not HIVE-29625)")
  void fkPreservingOuterJoinKeepsEveryFkRow(boolean constraints, boolean pkLeft, Double pkNdv, Double fkNdv,
      boolean simple) {
    Analysis a = analyze(constraints, pkLeft, pkLeft ? JoinRelType.RIGHT : JoinRelType.LEFT, pkNdv, fkNdv, simple);
    assertEquals(1.0, a.info.pkInfo.selectivity);
    assertEquals(1000.0, a.rowCount);
  }

  private static Stream<Arguments> outerJoinKnownNdvCases() {
    List<Arguments> cases = new ArrayList<>();
    for (boolean constraints : List.of(false, true)) {
      for (boolean pkLeft : List.of(false, true)) {
        for (Double[] ndvs : new Double[][] {{20.0, 80.0}, {80.0, 20.0}, {200.0, 20.0}, {0.0, 80.0}}) {
          cases.add(Arguments.of(constraints, pkLeft, ndvs[0], ndvs[1], true));
        }
        cases.add(Arguments.of(constraints, pkLeft, 20.0, 80.0, false));
      }
    }
    return cases.stream();
  }

  private record Analysis(HiveRelMdRowCount.PKFKRelationInfo info, double rowCount) {
  }

  private Analysis analyze(boolean constraints, boolean pkLeft, JoinRelType type, Double pkNdv, Double fkNdv,
      boolean simple) {
    RelNode pkInput = simple ? pkFilter : HiveProject.create(cluster, pkFilter,
        List.of(rexBuilder.makeCall(SqlStdOperatorTable.UNARY_MINUS, rexBuilder.makeInputRef(pkFilter, 0))),
        pkFilter.getRowType(), List.of());
    when(mq.getRowCount(pkInput)).thenReturn(250.0);
    HiveJoin join = HiveJoin.getJoin(cluster, pkLeft ? pkInput : fkScan, pkLeft ? fkScan : pkInput,
        joinCondition(), type);
    when(mq.getDistinctRowCount(eq(pkInput), eq(KEY), any())).thenReturn(pkNdv);
    when(mq.getDistinctRowCount(eq(fkScan), eq(KEY), any())).thenReturn(fkNdv);
    PKFKJoinInfo notPkFk = PKFKJoinInfo.of(false, null, null);
    PKFKJoinInfo pkFk = PKFKJoinInfo.of(true, Pair.of(KEY, KEY), List.of());

    try (MockedStatic<EstimateUniqueKeys> keys = mockStatic(EstimateUniqueKeys.class, CALLS_REAL_METHODS);
         MockedStatic<HiveRelOptUtil> constraintsMetadata = mockStatic(HiveRelOptUtil.class, CALLS_REAL_METHODS)) {
      keys.when(() -> EstimateUniqueKeys.getUniqueKeys(pkInput)).thenReturn(Set.of(KEY));
      keys.when(() -> EstimateUniqueKeys.getUniqueKeys(fkScan)).thenReturn(Set.of());
      constraintsMetadata.when(() -> HiveRelOptUtil.extractPKFKJoin(eq(join), anyList(), eq(false), eq(mq)))
          .thenReturn(constraints && pkLeft ? pkFk : notPkFk);
      constraintsMetadata.when(() -> HiveRelOptUtil.extractPKFKJoin(eq(join), anyList(), eq(true), eq(mq)))
          .thenReturn(constraints && !pkLeft ? pkFk : notPkFk);

      HiveRelMdRowCount.PKFKRelationInfo info;
      if (constraints) {
        Pair<HiveRelMdRowCount.PKFKRelationInfo, RexNode> result =
            HiveRelMdRowCount.constraintsBasedAnalyzeJoinForPKFK(join, mq);
        assertNotNull(result);
        assertNull(result.right);
        info = result.left;
      } else {
        info = HiveRelMdRowCount.analyzeJoinForPKFK(join, mq);
      }
      assertNotNull(info);
      double rowCount = new HiveRelMdRowCount().getRowCount(join, mq);
      if (!simple) {
        verify(mq, never()).getDistinctRowCount(any(), any(), any());
      }
      return new Analysis(info, rowCount);
    }
  }

  @ParameterizedTest
  @EnumSource(value = JoinRelType.class, names = {"SEMI", "ANTI"})
  void semiAndAntiJoinsDoNotReadPkFkNdv(JoinRelType type) {
    Join join = type == JoinRelType.SEMI
        ? HiveSemiJoin.getSemiJoin(cluster, pkScan.getTraitSet(), pkFilter, fkScan, joinCondition())
        : HiveAntiJoin.getAntiJoin(cluster, pkScan.getTraitSet(), pkFilter, fkScan, joinCondition());
    assertNull(HiveRelMdRowCount.analyzeJoinForPKFK(join, mq));
    assertNull(HiveRelMdRowCount.constraintsBasedAnalyzeJoinForPKFK(join, mq));
    verify(mq, never()).getDistinctRowCount(any(), any(), any());
  }

  @Test
  void semiJoinRowCountIsCalciteEstimate() {
    Join join = HiveSemiJoin.getSemiJoin(cluster, fkScan.getTraitSet(), fkScan, pkFilter, fkJoinCondition());
    when(mq.getSelectivity(eq(fkScan), any())).thenReturn(0.2);
    assertEquals(200.0, new HiveRelMdRowCount().getRowCount((HiveSemiJoin) join, mq), 1e-9);
  }

  @Test
  @Disabled("Pre-existing: the anti-join row count equals the semi-join one (follow-up, not part of HIVE-29625)")
  void antiJoinRowCountKeepsUnmatchedRows() {
    Join join = HiveAntiJoin.getAntiJoin(cluster, fkScan.getTraitSet(), fkScan, pkFilter, fkJoinCondition());
    when(mq.getSelectivity(eq(fkScan), any())).thenReturn(0.2);
    assertEquals(800.0, new HiveRelMdRowCount().getRowCount((HiveAntiJoin) join, mq), 1e-9);
  }

  private RexNode fkJoinCondition() {
    return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(fkScan, 0),
        rexBuilder.makeInputRef(pkFilter.getRowType().getFieldList().get(0).getType(), 1));
  }

  private HiveTableScan scan(String name) {
    RelDataType rowType = rexBuilder.getTypeFactory().builder().add("k", SqlTypeName.INTEGER).build();
    RelOptHiveTable table = mock(RelOptHiveTable.class);
    when(table.getRowType()).thenReturn(rowType);
    when(table.getQualifiedName()).thenReturn(List.of("default", name));
    when(table.getHiveTableMD()).thenReturn(new Table("default", name));
    return new HiveTableScan(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION), table, name, null, false, false);
  }

  private RexNode joinCondition() {
    return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(pkScan, 0),
        rexBuilder.makeInputRef(fkScan.getRowType().getFieldList().get(0).getType(), 1));
  }
}
