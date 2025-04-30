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
package org.apache.hadoop.hive.ql.optimizer.calcite;

import com.google.common.collect.Range;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.RangeSets;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveBetween;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts a {@link Range} to a {@link RexNode} expression based on the range type as defined by the
 * {@link RangeSets.Consumer}.
 *
 * <table>
 * <tr><th>Range        <th>RexNode
 * <tr><td>{@code (a..b)}  <td>{@code x > a AND x < b}
 * <tr><td>{@code [a..b]}  <td>{@code x BETWEEN a AND b}
 * <tr><td>{@code [a..a]}  <td>{@code a}
 * <tr><td>{@code (a..b]}  <td>{@code x > a AND x <= b}
 * <tr><td>{@code [a..b)}  <td>{@code x >= a AND x < b}
 * <tr><td>{@code (a..+∞)} <td>{@code x > a}
 * <tr><td>{@code [a..+∞)} <td>{@code x >= a}
 * <tr><td>{@code (-∞..b)} <td>{@code x < b}
 * <tr><td>{@code (-∞..b]} <td>{@code x <= b}
 * <tr><td>{@code (-∞..+∞)}<td>{@code true}
 * </table>
 *
 * @param <C> type of the range
 */
class RangeConverter<C extends Comparable<C>> implements RangeSets.Consumer<C> {

  private final RexBuilder rexBuilder;
  private final RelDataType type;
  private final RexNode ref;
  final List<RexNode> inLiterals;
  final List<RexNode> nodes;

  public RangeConverter(RexBuilder rexBuilder, RelDataType type, RexNode ref) {
    this.rexBuilder = rexBuilder;
    this.type = type;
    this.ref = ref;
    this.inLiterals = new ArrayList<>();
    this.nodes = new ArrayList<>();
  }

  private RexNode op(SqlOperator op, C value) {
    return rexBuilder.makeCall(op, ref, rexBuilder.makeLiteral(value, type, true, true));
  }

  private RexNode and(RexNode... nodes) {
    return rexBuilder.makeCall(SqlStdOperatorTable.AND, nodes);
  }

  public void all() {
    nodes.add(rexBuilder.makeLiteral(true));
  }

  @Override
  public void atLeast(C lower) {
    nodes.add(op(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, lower));
  }

  @Override
  public void atMost(C upper) {
    nodes.add(op(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, upper));
  }

  @Override
  public void greaterThan(C lower) {
    nodes.add(op(SqlStdOperatorTable.GREATER_THAN, lower));
  }

  @Override
  public void lessThan(C upper) {
    nodes.add(op(SqlStdOperatorTable.LESS_THAN, upper));
  }

  @Override
  public void singleton(C value) {
    inLiterals.add(rexBuilder.makeLiteral(value, type, true, true));
  }

  @Override
  public void closed(C lower, C upper) {
    nodes.add(rexBuilder.makeCall(HiveBetween.INSTANCE,
        rexBuilder.makeLiteral(false),
        ref,
        rexBuilder.makeLiteral(lower, type, true, true),
        rexBuilder.makeLiteral(upper, type, true, true)));
  }

  @Override
  public void closedOpen(C lower, C upper) {
    nodes.add(and(op(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, lower), op(SqlStdOperatorTable.LESS_THAN, upper)));
  }

  @Override
  public void openClosed(C lower, C upper) {
    nodes.add(and(op(SqlStdOperatorTable.GREATER_THAN, lower), op(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, upper)));
  }

  @Override
  public void open(C lower, C upper) {
    nodes.add(and(op(SqlStdOperatorTable.GREATER_THAN, lower), op(SqlStdOperatorTable.LESS_THAN, upper)));
  }
}
