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

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.RangeSets;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveBetween;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

class RangeConverter<C extends Comparable<C>> implements RangeSets.Consumer<C> {

  protected final RexBuilder rexBuilder;
  protected final RelDataType type;
  protected final RexNode ref;
  protected final boolean negate;
  public final List<RexNode> inNodes;
  public final List<RexNode> nodes;

  public RangeConverter(RexBuilder rexBuilder, RelDataType type, RexNode ref, boolean negate) {
    this.rexBuilder = rexBuilder;
    this.type = type;
    this.ref = ref;
    this.inNodes = new ArrayList<>();
    this.nodes = new ArrayList<>();
    this.negate = negate;
  }

  private RexNode op(SqlOperator op, C value) {
    return rexBuilder.makeCall(op, ref, rexBuilder.makeLiteral(value, type, true, true));
  }

  private RexNode and(RexNode... nodes) {
    return rexBuilder.makeCall(SqlStdOperatorTable.AND, nodes);
  }

  private void addWithNegate(RexNode node) {
    RexCall call = (RexCall) node;
    if (negate) {
      if (call.isA(SqlKind.AND)) {
        call = (RexCall) rexBuilder.makeCall(SqlStdOperatorTable.OR,
            Arrays.asList(RexUtil.negate(rexBuilder, (RexCall) call.getOperands().get(0)),
                RexUtil.negate(rexBuilder, (RexCall) call.getOperands().get(1))));
      } else {
        call = (RexCall) RexUtil.negate(rexBuilder, call);
      }
    }
    assert call != null;
    nodes.add(call);
  }

  public void all() {
    nodes.add(rexBuilder.makeLiteral(!negate));
  }

  @Override
  public void atLeast(C lower) {
    addWithNegate(op(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, lower));
  }

  @Override
  public void atMost(C upper) {
    addWithNegate(op(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, upper));
  }

  @Override
  public void greaterThan(C lower) {
    addWithNegate(op(SqlStdOperatorTable.GREATER_THAN, lower));
  }

  @Override
  public void lessThan(C upper) {
    addWithNegate(op(SqlStdOperatorTable.LESS_THAN, upper));
  }

  @Override
  public void singleton(C value) {
    inNodes.add(rexBuilder.makeLiteral(value, type, true, true));
  }

  @Override
  public void closed(C lower, C upper) {
    // when `negate` is true, we want to create NOT BETWEEN, so we set `isNotBetween` to `negate` (true)
    nodes.add(makeHiveBetween(rexBuilder, negate, ref, type, lower, upper));
  }

  @Override
  public void closedOpen(C lower, C upper) {
    addWithNegate(and(op(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, lower), op(SqlStdOperatorTable.LESS_THAN, upper)));
  }

  @Override
  public void openClosed(C lower, C upper) {
    addWithNegate(and(op(SqlStdOperatorTable.GREATER_THAN, lower), op(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, upper)));
  }

  @Override
  public void open(C lower, C upper) {
    addWithNegate(and(op(SqlStdOperatorTable.GREATER_THAN, lower), op(SqlStdOperatorTable.LESS_THAN, upper)));
  }

  private RexNode makeHiveBetween(RexBuilder rexBuilder, boolean isNotBetween, RexNode operand, RelDataType type,
      Object lower, Object upper) {
    return rexBuilder.makeCall(HiveBetween.INSTANCE, rexBuilder.makeLiteral(isNotBetween), operand,
        rexBuilder.makeLiteral(lower, type, true, true), rexBuilder.makeLiteral(upper, type, true, true));
  }
}
