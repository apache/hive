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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUnknownAs;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.RangeSets;
import org.apache.calcite.util.Sarg;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveBetween;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveIn;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A class that transforms a call to the internal {@link SqlStdOperatorTable#SEARCH} operator into an equivalent
 * {@link RexNode} expression. The new expression comprises conjunctions/disjunctions involving the following operators:
 * <ul>
 *   <li>standard binary comparisons ({@code <,<=,>,>=,=,<>});</li>
 *   <li>{@code IS NULL}, {@code IS NOT NULL};</li>
 *   <li>{@link HiveIn}, {@link HiveBetween}.</li>
 * </ul>
 * Below you can find an example from the transformation of a SEARCH expression of INTEGER type.
 * <pre>{@code
 * Input: SEARCH($9, Sarg[1, 2, 3, [10..15], (20..+∞)])
 * Output: OR(IN($9, 1, 2, 3), BETWEEN(false, $9, 10, 15), >($9, 20))
 * }</pre>
 *
 * @param <C> type of the Sarg literal
 */
public class SearchTransformer<C extends Comparable<C>> {
  private final RexBuilder rexBuilder;
  private final RexNode ref;
  private final Sarg<C> sarg;
  protected final RelDataType type;

  public SearchTransformer(RexBuilder rexBuilder, RexCall search) {
    this.rexBuilder = rexBuilder;
    ref = search.getOperands().get(0);
    RexLiteral literal = (RexLiteral) search.operands.get(1);
    sarg = Objects.requireNonNull(literal.getValueAs(Sarg.class), "Sarg");
    type = literal.getType();
  }

  public RexNode transform() {
    PerfLogger perfLogger = SessionState.getPerfLogger();
    perfLogger.perfLogBegin(this.getClass().getName(), PerfLogger.SEARCH_TRANSFORMER);

    RangeConverter<C> consumer = new RangeConverter<>(rexBuilder, type, ref);
    RangeSets.forEach(sarg.rangeSet, consumer);

    List<RexNode> orList = new ArrayList<>();
    if (sarg.nullAs == RexUnknownAs.TRUE) {
      orList.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, ref));
    }
    switch (consumer.inLiterals.size()) {
    case 0:
      break;
    case 1:
      orList.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, ref, consumer.inLiterals.get(0)));
      break;
    default:
      List<RexNode> operands = new ArrayList<>(consumer.inLiterals.size() + 1);
      operands.add(ref);
      operands.addAll(consumer.inLiterals);
      orList.add(rexBuilder.makeCall(HiveIn.INSTANCE, operands));
    }
    orList.addAll(consumer.nodes);
    RexNode x = RexUtil.composeDisjunction(rexBuilder, orList);

    if (sarg.nullAs == RexUnknownAs.FALSE) {
      RexNode notNull = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, ref);
      x = RexUtil.composeConjunction(rexBuilder, Arrays.asList(notNull, x));
    }
    perfLogger.perfLogEnd(this.getClass().getName(), PerfLogger.SEARCH_TRANSFORMER);
    return x;
  }

  public static class Shuttle extends RexShuttle {
    private final RexBuilder rexBuilder;

    public Shuttle(final RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override public RexNode visitCall(RexCall call) {
      final boolean[] update = {false};
      final List<RexNode> clonedOperands;
      switch (call.getKind()) {
      // Flatten AND/OR operands.
      case OR:
        clonedOperands = visitList(call.operands, update);
        if (update[0]) {
          return RexUtil.composeDisjunction(rexBuilder, clonedOperands);
        } else {
          return call;
        }
      case AND:
        clonedOperands = visitList(call.operands, update);
        if (update[0]) {
          return RexUtil.composeConjunction(rexBuilder, clonedOperands);
        } else {
          return call;
        }
      case SEARCH:
        return new SearchTransformer<>(rexBuilder, call).transform();
      default:
        return super.visitCall(call);
      }
    }
  }
}

