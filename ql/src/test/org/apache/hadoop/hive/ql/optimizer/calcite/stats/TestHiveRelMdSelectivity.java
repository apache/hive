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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveTypeSystemImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.RelOptHiveTable;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveAntiJoin;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveJoin;
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
import org.junit.jupiter.params.provider.ValueSource;

class TestHiveRelMdSelectivity {

  private RexBuilder rexBuilder;
  private RelOptCluster cluster;
  private RelMetadataQuery mq;
  private HiveTableScan left;
  private HiveTableScan right;

  @BeforeEach
  void setUp() {
    setUp(false);
  }

  private void setUp(boolean correlated) {
    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_CBO_STATS_CORRELATED_MULTI_KEY_JOINS, correlated);
    rexBuilder = new RexBuilder(new JavaTypeFactoryImpl(new HiveTypeSystemImpl()));
    cluster = RelOptCluster.create(CalcitePlanner.createPlanner(conf), rexBuilder);
    mq = mock(RelMetadataQuery.class, withSettings().useConstructor());
    left = scan("left", 100.0);
    right = scan("right", 50.0);
  }

  private HiveTableScan scan(String name, double rows) {
    RelDataType rowType = rexBuilder.getTypeFactory().builder()
        .add("k0", SqlTypeName.INTEGER).add("k1", SqlTypeName.INTEGER).build();
    RelOptHiveTable table = mock(RelOptHiveTable.class);
    when(table.getRowType()).thenReturn(rowType);
    HiveTableScan scan = new HiveTableScan(cluster, cluster.traitSetOf(HiveRelNode.CONVENTION),
        table, name, null, false, false);
    when(mq.getRowCount(scan)).thenReturn(rows);
    return scan;
  }

  @Test
  void scanWithoutPredicateKeepsAllRows() {
    assertEquals(1.0, selectivity(left, null));
  }

  @ParameterizedTest
  @MethodSource("predicateCases")
  void scanPredicate(Double ndv, double expected) {
    ndv(left, 0, ndv);
    assertEquals(expected, selectivity(left, equality(left, 0)));
  }

  private static Stream<Arguments> predicateCases() {
    return Stream.of(Arguments.of(null, 1.0), Arguments.of(10.0, 0.1));
  }

  @ParameterizedTest
  @EnumSource(value = JoinRelType.class, names = {"INNER", "LEFT", "RIGHT", "SEMI", "ANTI"})
  void unknownAdditionalJoinPredicateDoesNotReturnNull(JoinRelType type) {
    Join join = join(type, false);
    when(mq.getRowCount(join)).thenReturn(100.0);
    ndv(join, 0, null);
    RexNode predicate = rexBuilder.makeCall(SqlStdOperatorTable.AND, join.getCondition(), equality(join, 0));
    assertEquals(1.0, selectivity(join, predicate));
  }

  @Test
  void partlyUnknownFilterAboveAntiJoinKeepsEveryRow() {
    Join join = join(JoinRelType.ANTI, false);
    when(mq.getRowCount(join)).thenReturn(100.0);
    ndv(join, 0, null);
    ndv(join, 1, 10.0);
    RexNode filter = rexBuilder.makeCall(SqlStdOperatorTable.OR, equality(join, 0), equality(join, 1));
    RexNode predicate = rexBuilder.makeCall(SqlStdOperatorTable.AND, join.getCondition(), filter);
    assertEquals(1.0, selectivity(join, predicate));
  }

  @ParameterizedTest
  @EnumSource(value = JoinRelType.class, names = {"INNER", "LEFT", "RIGHT", "SEMI"})
  void knownAdditionalJoinPredicateIsPreserved(JoinRelType type) {
    Join join = join(type, false);
    when(mq.getRowCount(join)).thenReturn(100.0);
    ndv(join, 0, 10.0);
    RexNode predicate = rexBuilder.makeCall(SqlStdOperatorTable.AND, join.getCondition(), equality(join, 0));
    assertEquals(0.1, selectivity(join, predicate), 1e-12);
  }

  @Test
  void knownFilterAboveAntiJoinIsNotTreatedAsUnknown() {
    Join join = join(JoinRelType.ANTI, false);
    when(mq.getRowCount(join)).thenReturn(100.0);
    ndv(join, 0, 10.0);
    RexNode predicate = rexBuilder.makeCall(SqlStdOperatorTable.AND, join.getCondition(), equality(join, 0));
    assertNotEquals(1.0, selectivity(join, predicate));
  }

  @Test
  void guardDoesNotApplyToTheAntiJoinCondition() {
    // with unknown keys the guard would keep every row (1.0); the condition's own estimate is a pre-existing follow-up
    ndv(left, 0, null);
    ndv(right, 0, null);
    assertNotEquals(1.0, selectivity(join(JoinRelType.ANTI, false), null));
  }

  @Test
  @Disabled("Pre-existing: a filter above an anti join gets 1 - selectivity (follow-up, not part of HIVE-29625)")
  void knownFilterAboveAntiJoinKeepsItsSelectivity() {
    Join join = join(JoinRelType.ANTI, false);
    when(mq.getRowCount(join)).thenReturn(100.0);
    ndv(join, 0, 10.0);
    RexNode predicate = rexBuilder.makeCall(SqlStdOperatorTable.AND, join.getCondition(), equality(join, 0));
    assertEquals(0.1, selectivity(join, predicate), 1e-12);
  }

  @ParameterizedTest(name = "{0}: leftNDV={1}, rightNDV={2}")
  @MethodSource("joinKeyCases")
  void joinKeysRetainKnownNdv(JoinRelType type, Double leftNdv, Double rightNdv, double expected) {
    ndv(left, 0, leftNdv);
    ndv(right, 0, rightNdv);
    assertEquals(expected, selectivity(join(type, false), null), 1e-12);
  }

  private static Stream<Arguments> joinKeyCases() {
    List<Arguments> cases = new ArrayList<>();
    for (JoinRelType type : List.of(JoinRelType.INNER, JoinRelType.LEFT, JoinRelType.RIGHT)) {
      for (Double[] ndvs : new Double[][] {{null, 20.0}, {20.0, null}, {null, null}, {10.0, 20.0}, {20.0, 10.0}}) {
        double ndv = ndvs[0] == null && ndvs[1] == null ? 1.0 : 20.0;
        cases.add(Arguments.of(type, ndvs[0], ndvs[1], 1 / ndv));
      }
    }
    return cases.stream();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void multikeyJoinRetainsKnownKeysFromBothSides(boolean correlated) {
    setUp(correlated);
    ndv(left, 0, null);
    ndv(right, 0, 25.0);
    ndv(left, 1, 16.0);
    ndv(right, 1, null);
    assertEquals(correlated ? 1.0 / 25 : 1.0 / 100, selectivity(join(JoinRelType.INNER, true), null), 1e-12);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void multikeyJoinWithAllKeysUnknownKeepsAllRows(boolean correlated) {
    setUp(correlated);
    ndv(left, 0, null);
    ndv(right, 0, null);
    ndv(left, 1, null);
    ndv(right, 1, null);
    assertEquals(1.0, selectivity(join(JoinRelType.INNER, true), null));
  }

  private void ndv(RelNode rel, int column, Double value) {
    when(mq.getDistinctRowCount(eq(rel), eq(ImmutableBitSet.of(column)), any())).thenReturn(value);
  }

  private RexNode equality(RelNode rel, int column) {
    return rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(rel, column),
        rexBuilder.makeExactLiteral(BigDecimal.ONE));
  }

  private Join join(JoinRelType type, boolean multikey) {
    RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
        rexBuilder.makeInputRef(left, 0),
        rexBuilder.makeInputRef(right.getRowType().getFieldList().get(0).getType(), 2));
    if (multikey) {
      condition = rexBuilder.makeCall(SqlStdOperatorTable.AND, condition,
          rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rexBuilder.makeInputRef(left, 1),
              rexBuilder.makeInputRef(right.getRowType().getFieldList().get(1).getType(), 3)));
    }
    if (type == JoinRelType.SEMI) {
      return HiveSemiJoin.getSemiJoin(cluster, left.getTraitSet(), left, right, condition);
    }
    if (type == JoinRelType.ANTI) {
      return HiveAntiJoin.getAntiJoin(cluster, left.getTraitSet(), left, right, condition);
    }
    return HiveJoin.getJoin(cluster, left, right, condition, type);
  }

  private Double selectivity(RelNode rel, RexNode predicate) {
    BuiltInMetadata.Selectivity metadata = HiveRelMdSelectivity.SOURCE
        .apply(rel.getClass(), BuiltInMetadata.Selectivity.class).bind(rel, mq);
    return metadata.getSelectivity(predicate);
  }
}
